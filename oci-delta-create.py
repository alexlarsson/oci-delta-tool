#!/usr/bin/env python3

import argparse
import json
import tarfile
import struct
import sys
import gzip
import io
from pathlib import Path
from typing import Dict, Set, BinaryIO

CHUNK_TYPE_DATA = 0x00
CHUNK_TYPE_OSTREE = 0x01

def parse_oci_image(image_path: str):
    """Parse OCI image and return index, layers, and blob members."""
    index_data = None
    layer_blobs = set()
    blob_members = {}

    with tarfile.open(image_path, 'r') as tar:
        try:
            index_member = tar.getmember('index.json')
            f = tar.extractfile(index_member)
            index_data = json.load(f)
        except KeyError:
            print(f"Error: No index.json found in {image_path}", file=sys.stderr)
            sys.exit(1)

        for member in tar.getmembers():
            if member.name.startswith('blobs/sha256/'):
                digest = member.name.split('/')[-1]
                blob_members[digest] = member

        for manifest_desc in index_data.get('manifests', []):
            manifest_digest = manifest_desc['digest'].split(':')[-1]
            if manifest_digest in blob_members:
                manifest_member = blob_members[manifest_digest]
                f = tar.extractfile(manifest_member)
                manifest = json.load(f)

                for layer in manifest.get('layers', []):
                    layer_digest = layer['digest'].split(':')[-1]
                    layer_blobs.add(layer_digest)

    return index_data, layer_blobs, blob_members

def find_ostree_objects_in_layer(tar: tarfile.TarFile, blob_digest: str, blob_members: Dict) -> Set[str]:
    """Find all ostree object files in a layer."""
    if blob_digest not in blob_members:
        return set()

    blob_member = blob_members[blob_digest]
    blob_file = tar.extractfile(blob_member)

    ostree_objects = set()
    try:
        with tarfile.open(fileobj=blob_file, mode='r:*') as layer_tar:
            for member in layer_tar.getmembers():
                if (member.name.startswith('sysroot/ostree/repo/objects/') and
                    member.name.endswith('.file') and member.isfile()):
                    ostree_objects.add(member.name)
    except Exception as e:
        print(f"Warning: Could not read layer {blob_digest}: {e}", file=sys.stderr)

    return ostree_objects

def extract_ostree_digest(filepath: str) -> str:
    """
    Extract ostree object digest from filepath.
    Example: sysroot/ostree/repo/objects/c8/552977...68.file -> c8552977...68
    """
    parts = filepath.split('/')
    if len(parts) >= 2:
        dir_name = parts[-2]
        file_name = parts[-1].replace('.file', '')
        return dir_name + file_name
    return None

def write_chunk(output: BinaryIO, chunk_type: int, data: bytes):
    """Write a chunk to the output stream."""
    output.write(struct.pack('B', chunk_type))
    output.write(struct.pack('>Q', len(data)))
    output.write(data)

def parse_tar_header(header_bytes: bytes) -> dict:
    """Parse a 512-byte tar header."""
    if len(header_bytes) != 512:
        return None

    if header_bytes == b'\x00' * 512:
        return {'type': 'zero'}

    try:
        name = header_bytes[0:100].rstrip(b'\x00').decode('utf-8', errors='ignore')
        size_str = header_bytes[124:136].rstrip(b'\x00 ').decode('ascii', errors='ignore')

        if not size_str:
            return {'type': 'zero'}

        size = int(size_str, 8)
        return {
            'type': 'file',
            'name': name,
            'size': size
        }
    except (ValueError, UnicodeDecodeError):
        return None

def chunk_layer(layer_stream: BinaryIO, reusable_ostree: Set[str]) -> bytes:
    """
    Convert an uncompressed tar stream to chunked format.

    Args:
        layer_stream: Uncompressed tar file stream
        reusable_ostree: Set of ostree object paths that exist in old image

    Returns:
        Chunked data as bytes
    """
    output = io.BytesIO()

    while True:
        header_bytes = layer_stream.read(512)
        if len(header_bytes) < 512:
            break

        header_info = parse_tar_header(header_bytes)

        if not header_info or header_info['type'] == 'zero':
            write_chunk(output, CHUNK_TYPE_DATA, header_bytes)
            if len(header_bytes) < 512:
                break
            continue

        write_chunk(output, CHUNK_TYPE_DATA, header_bytes)

        file_size = header_info['size']
        file_name = header_info['name']

        is_reusable_ostree = file_name in reusable_ostree

        if is_reusable_ostree:
            ostree_digest = extract_ostree_digest(file_name)
            if ostree_digest:
                digest_bytes = bytes.fromhex(ostree_digest)
                write_chunk(output, CHUNK_TYPE_OSTREE, digest_bytes)

                layer_stream.read(file_size)
            else:
                data = layer_stream.read(file_size)
                write_chunk(output, CHUNK_TYPE_DATA, data)
        else:
            data = layer_stream.read(file_size)
            if data:
                write_chunk(output, CHUNK_TYPE_DATA, data)

        padding_size = (512 - (file_size % 512)) % 512
        if padding_size > 0:
            padding = layer_stream.read(padding_size)
            if padding:
                write_chunk(output, CHUNK_TYPE_DATA, padding)

    return output.getvalue()

def create_delta(old_image: str, new_image: str, output_path: str):
    """Create a delta update file."""
    print(f"Creating delta from:")
    print(f"  Old: {old_image}")
    print(f"  New: {new_image}")
    print(f"  Output: {output_path}")
    print()

    print("Parsing images...")
    old_index, old_layers, old_blobs = parse_oci_image(old_image)
    new_index, new_layers, new_blobs = parse_oci_image(new_image)

    common_layers = old_layers & new_layers
    new_only_layers = new_layers - old_layers

    print(f"Common layers (will skip): {len(common_layers)}")
    print(f"New layers (will process): {len(new_only_layers)}")

    print("\nCollecting ostree objects from old image...")
    old_ostree_objects = set()
    with tarfile.open(old_image, 'r') as tar:
        for layer_digest in old_layers:
            ostree_files = find_ostree_objects_in_layer(tar, layer_digest, old_blobs)
            old_ostree_objects.update(ostree_files)

    print(f"Found {len(old_ostree_objects)} ostree objects in old image")

    print("\nCreating delta file...")
    with gzip.open(output_path, 'wb', compresslevel=6) as delta_tar_gz:
        with tarfile.open(fileobj=delta_tar_gz, mode='w') as delta_tar:
            with tarfile.open(new_image, 'r') as new_tar:
                print("  Copying index.json...")
                index_member = new_tar.getmember('index.json')
                delta_tar.addfile(index_member, new_tar.extractfile(index_member))

                try:
                    print("  Copying oci-layout...")
                    oci_layout_member = new_tar.getmember('oci-layout')
                    delta_tar.addfile(oci_layout_member, new_tar.extractfile(oci_layout_member))
                except KeyError:
                    print("  Warning: No oci-layout file found in source image")

                print("  Copying non-layer blobs...")
                non_layer_blobs = set(new_blobs.keys()) - new_layers
                for blob_digest in non_layer_blobs:
                    blob_member = new_blobs[blob_digest]
                    delta_tar.addfile(blob_member, new_tar.extractfile(blob_member))

                print("  Processing new layers...")
                for layer_digest in new_only_layers:
                    print(f"    Processing layer {layer_digest[:16]}...")

                    blob_member = new_blobs[layer_digest]

                    reusable_in_layer = find_ostree_objects_in_layer(new_tar, layer_digest, new_blobs) & old_ostree_objects

                    if reusable_in_layer:
                        print(f"      Found {len(reusable_in_layer)} reusable ostree objects - chunking")

                        blob_file = new_tar.extractfile(blob_member)
                        with gzip.GzipFile(fileobj=blob_file) as gz:
                            uncompressed_tar = io.BytesIO(gz.read())

                        chunked_data = chunk_layer(uncompressed_tar, reusable_in_layer)

                        chunked_member = tarfile.TarInfo(name=f'blobs/sha256/{layer_digest}')
                        chunked_member.size = len(chunked_data)
                        delta_tar.addfile(chunked_member, io.BytesIO(chunked_data))

                        print(f"      Original: {blob_member.size} bytes")
                        print(f"      Chunked: {len(chunked_data)} bytes")
                    else:
                        print(f"      No reusable ostree objects - copying original compressed blob")
                        delta_tar.addfile(blob_member, new_tar.extractfile(blob_member))

    delta_size = Path(output_path).stat().st_size
    new_size = Path(new_image).stat().st_size

    print(f"\nDelta file created successfully!")
    print(f"  New image size: {new_size:,} bytes")
    print(f"  Delta file size: {delta_size:,} bytes")
    print(f"  Savings: {new_size - delta_size:,} bytes ({(1 - delta_size/new_size)*100:.1f}%)")

def main():
    parser = argparse.ArgumentParser(
        description='Create delta update between two bootc OCI images'
    )
    parser.add_argument('old_image', help='Path to old OCI image tar file')
    parser.add_argument('new_image', help='Path to new OCI image tar file')
    parser.add_argument('output', help='Path for output delta file (.tar.gz)')

    args = parser.parse_args()

    if not Path(args.old_image).exists():
        print(f"Error: Old image not found: {args.old_image}", file=sys.stderr)
        sys.exit(1)

    if not Path(args.new_image).exists():
        print(f"Error: New image not found: {args.new_image}", file=sys.stderr)
        sys.exit(1)

    create_delta(args.old_image, args.new_image, args.output)

if __name__ == '__main__':
    main()
