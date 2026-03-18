#!/usr/bin/env python3

import argparse
import json
import tarfile
import struct
import sys
import gzip
import io
import os
import stat
from pathlib import Path

CHUNK_TYPE_DATA = 0x00
CHUNK_TYPE_OSTREE = 0x01

def read_chunk(stream):
    """Read a single chunk from the stream."""
    chunk_type_bytes = stream.read(1)
    if not chunk_type_bytes:
        return None

    chunk_type = struct.unpack('B', chunk_type_bytes)[0]
    size_bytes = stream.read(8)
    if len(size_bytes) < 8:
        return None

    size = struct.unpack('>Q', size_bytes)[0]
    data = stream.read(size)

    return {
        'type': chunk_type,
        'size': size,
        'data': data
    }

def reconstruct_layer(chunk_stream, ostree_root: Path = None):
    """
    Reconstruct a layer tar stream from chunks.

    Returns (tar_data, missing_objects)
    """
    output = io.BytesIO()
    missing_objects = []

    while True:
        chunk = read_chunk(chunk_stream)
        if not chunk:
            break

        if chunk['type'] == CHUNK_TYPE_DATA:
            output.write(chunk['data'])
        elif chunk['type'] == CHUNK_TYPE_OSTREE:
            digest = chunk['data'].hex()

            if not ostree_root:
                missing_objects.append(digest)
                print(f"Error: OSTREE chunk found but no ostree repo provided", file=sys.stderr)
                print(f"  Digest: {digest}", file=sys.stderr)
                continue

            ostree_path = ostree_root / 'objects' / digest[0:2] / f"{digest[2:]}.file"
            try:
                if ostree_path.exists():
                    current_mode = ostree_path.stat().st_mode
                    needs_chmod = not (current_mode & stat.S_IRUSR)

                    if needs_chmod:
                        os.chmod(ostree_path, current_mode | stat.S_IRUSR)

                    try:
                        with open(ostree_path, 'rb') as f:
                            output.write(f.read())
                    finally:
                        if needs_chmod:
                            os.chmod(ostree_path, current_mode)
                else:
                    missing_objects.append(str(ostree_path))
                    print(f"Error: Ostree object not found: {ostree_path}", file=sys.stderr)
            except (PermissionError, OSError) as e:
                missing_objects.append(f"{digest} ({e})")
                print(f"Error reading ostree object {digest}: {e}", file=sys.stderr)

    return output.getvalue(), missing_objects

def apply_delta(delta_path: str, output_path: str, ostree_repo: str = None):
    """Apply a delta file to create a standard OCI archive."""
    print(f"Applying delta:")
    print(f"  Delta: {delta_path}")
    print(f"  Output: {output_path}")
    if ostree_repo:
        print(f"  Ostree repo: {ostree_repo}")
    print()

    ostree_root = Path(ostree_repo) if ostree_repo else None
    if ostree_root and not ostree_root.exists():
        print(f"Warning: Ostree repo not found: {ostree_repo}", file=sys.stderr)
        ostree_root = None

    with gzip.open(delta_path, 'rb') as delta_gz:
        with tarfile.open(fileobj=delta_gz, mode='r') as delta_tar:
            # First, identify layer blobs from the manifest
            print("Reading delta structure...")

            try:
                index_member = delta_tar.getmember('index.json')
                index_data = json.load(delta_tar.extractfile(index_member))
            except KeyError:
                print("Error: No index.json found in delta", file=sys.stderr)
                sys.exit(1)

            # Find layer digests from manifest
            layer_digests = set()
            all_members = {}

            for member in delta_tar.getmembers():
                all_members[member.name] = member
                if member.name.startswith('blobs/sha256/') and not member.name.endswith('/'):
                    digest = member.name.split('/')[-1]

            # Read manifest to identify layers and preserve order
            manifest_list = []
            for manifest_desc in index_data.get('manifests', []):
                manifest_digest = manifest_desc['digest'].split(':')[-1]
                manifest_path = f'blobs/sha256/{manifest_digest}'
                if manifest_path in all_members:
                    manifest_member = all_members[manifest_path]
                    manifest_data = json.load(delta_tar.extractfile(manifest_member))
                    manifest_list.append((manifest_desc, manifest_data, manifest_digest))

                    for layer in manifest_data.get('layers', []):
                        layer_digest = layer['digest'].split(':')[-1]
                        layer_digests.add(layer_digest)

            print(f"Found {len(layer_digests)} layers in delta")
            print("\nCreating standard OCI archive...")

            # Track digest mapping for recompressed layers
            digest_mapping = {}  # old_digest -> new_digest
            blob_sizes = {}  # new_digest -> size

            # Create output tar file
            with tarfile.open(output_path, 'w') as output_tar:
                # Don't copy metadata files yet - we'll update them at the end

                if 'oci-layout' in all_members:
                    print("  Copying oci-layout...")
                    oci_layout_member = all_members['oci-layout']
                    output_tar.addfile(oci_layout_member, delta_tar.extractfile(oci_layout_member))

                # Process all blobs (layers and non-layer blobs)
                for member in delta_tar.getmembers():
                    if not member.name.startswith('blobs/sha256/') or member.name.endswith('/'):
                        continue

                    if member.name in ['index.json', 'oci-layout']:
                        continue

                    digest = member.name.split('/')[-1]

                    if digest in layer_digests:
                        # This is a layer - check if it's chunked or original compressed
                        blob_stream = delta_tar.extractfile(member)

                        # Peek at first 2 bytes to detect format
                        first_bytes = blob_stream.read(2)
                        blob_stream.seek(0)

                        # Gzip magic bytes are 0x1f 0x8b
                        is_gzipped = (len(first_bytes) >= 2 and
                                     first_bytes[0] == 0x1f and
                                     first_bytes[1] == 0x8b)

                        if is_gzipped:
                            # Original compressed layer - copy as-is with same digest
                            print(f"  Copying layer {digest[:16]}... (original compressed)")
                            blob_data = blob_stream.read()
                            layer_member = tarfile.TarInfo(name=member.name)
                            layer_member.size = len(blob_data)
                            output_tar.addfile(layer_member, io.BytesIO(blob_data))
                            blob_sizes[digest] = len(blob_data)
                            # No digest change - same as original
                        else:
                            # Chunked layer - reconstruct and compress
                            print(f"  Processing layer {digest[:16]}... (chunked)")

                            tar_data, missing = reconstruct_layer(blob_stream, ostree_root)

                            if missing:
                                print(f"    Warning: {len(missing)} missing ostree objects", file=sys.stderr)
                                if not ostree_root:
                                    print(f"    Provide --ostree-repo to resolve OSTREE chunks", file=sys.stderr)

                            # Compress the reconstructed tar data with reproducible settings
                            # Match podman/buildah compression: level 9, no filename, mtime=0
                            compressed = io.BytesIO()
                            with gzip.GzipFile(fileobj=compressed, mode='wb',
                                             compresslevel=9, mtime=0, filename='') as gz:
                                gz.write(tar_data)

                            compressed_data = compressed.getvalue()

                            # Compute new digest for compressed data
                            import hashlib
                            new_digest = hashlib.sha256(compressed_data).hexdigest()
                            digest_mapping[digest] = new_digest
                            blob_sizes[new_digest] = len(compressed_data)

                            print(f"    Reconstructed: {len(tar_data):,} bytes")
                            print(f"    Compressed: {len(compressed_data):,} bytes")
                            print(f"    Old digest: {digest[:16]}...")
                            print(f"    New digest: {new_digest[:16]}...")

                            # Create new tar member with new digest in name
                            layer_member = tarfile.TarInfo(name=f'blobs/sha256/{new_digest}')
                            layer_member.size = len(compressed_data)
                            output_tar.addfile(layer_member, io.BytesIO(compressed_data))
                    else:
                        # Non-layer blob (manifest, config) - we'll handle these after updating
                        pass

                # Now update and write manifests with new layer digests
                print("\n  Updating manifests and metadata...")
                import hashlib
                updated_manifests = []

                for manifest_desc, manifest_data, old_manifest_digest in manifest_list:
                    # Update layer digests in manifest
                    if 'layers' in manifest_data:
                        for layer in manifest_data['layers']:
                            old_layer_digest = layer['digest'].split(':')[-1]
                            if old_layer_digest in digest_mapping:
                                new_layer_digest = digest_mapping[old_layer_digest]

                                # Store original digest in annotations for bootc compatibility
                                if 'annotations' not in layer:
                                    layer['annotations'] = {}
                                layer['annotations']['org.containers.bootc.delta.original-digest'] = f"sha256:{old_layer_digest}"

                                # Update to new digest
                                layer['digest'] = f"sha256:{new_layer_digest}"
                                layer['size'] = blob_sizes[new_layer_digest]
                            elif old_layer_digest in blob_sizes:
                                # Not remapped, but we have size tracked
                                layer['size'] = blob_sizes[old_layer_digest]

                    # Serialize updated manifest
                    manifest_json = json.dumps(manifest_data, indent=3, separators=(',', ': '))
                    manifest_bytes = manifest_json.encode('utf-8')

                    # Compute new manifest digest
                    new_manifest_digest = hashlib.sha256(manifest_bytes).hexdigest()

                    # Write updated manifest
                    manifest_member = tarfile.TarInfo(name=f'blobs/sha256/{new_manifest_digest}')
                    manifest_member.size = len(manifest_bytes)
                    output_tar.addfile(manifest_member, io.BytesIO(manifest_bytes))

                    print(f"    Updated manifest: {old_manifest_digest[:16]}... -> {new_manifest_digest[:16]}...")

                    # Track for updating index
                    updated_manifests.append((manifest_desc, new_manifest_digest, len(manifest_bytes)))

                # Copy config and other non-layer, non-manifest blobs
                for member in delta_tar.getmembers():
                    if not member.name.startswith('blobs/sha256/') or member.name.endswith('/'):
                        continue
                    if member.name in ['index.json', 'oci-layout']:
                        continue

                    digest = member.name.split('/')[-1]

                    # Skip if it's a layer or manifest (already processed)
                    if digest in layer_digests:
                        continue

                    # Check if it's a manifest
                    is_manifest = False
                    for _, _, old_manifest_digest in manifest_list:
                        if digest == old_manifest_digest:
                            is_manifest = True
                            break

                    if not is_manifest:
                        # Config or other blob - copy as-is
                        output_tar.addfile(member, delta_tar.extractfile(member))

                # Update index.json with new manifest digests
                for manifest_desc, new_manifest_digest, manifest_size in updated_manifests:
                    manifest_desc['digest'] = f"sha256:{new_manifest_digest}"
                    manifest_desc['size'] = manifest_size

                # Write updated index.json
                index_json = json.dumps(index_data, indent=3, separators=(',', ': '))
                index_bytes = index_json.encode('utf-8')
                index_member_new = tarfile.TarInfo(name='index.json')
                index_member_new.size = len(index_bytes)
                output_tar.addfile(index_member_new, io.BytesIO(index_bytes))

                print(f"    Updated index.json")

    output_size = Path(output_path).stat().st_size
    delta_size = Path(delta_path).stat().st_size

    print(f"\nOCI archive created successfully!")
    print(f"  Delta size: {delta_size:,} bytes")
    print(f"  Output size: {output_size:,} bytes")
    print(f"  Expansion: {output_size - delta_size:,} bytes ({(output_size/delta_size - 1)*100:.1f}%)")

def main():
    parser = argparse.ArgumentParser(
        description='Apply a bootc OCI delta file to create a standard OCI archive'
    )
    parser.add_argument('delta_file', help='Path to delta file (.tar.gz)')
    parser.add_argument('output', help='Path for output OCI archive (.tar)')
    parser.add_argument('--ostree-repo', metavar='PATH',
                        help='Path to ostree repo for reconstructing OSTREE chunks (e.g., /sysroot/ostree/repo)')

    args = parser.parse_args()

    if not Path(args.delta_file).exists():
        print(f"Error: Delta file not found: {args.delta_file}", file=sys.stderr)
        sys.exit(1)

    apply_delta(args.delta_file, args.output, args.ostree_repo)

if __name__ == '__main__':
    main()
