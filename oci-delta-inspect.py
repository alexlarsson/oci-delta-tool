#!/usr/bin/env python3

import argparse
import json
import tarfile
import struct
import sys
import gzip
import hashlib
import io
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

def format_size(size_bytes):
    """Format size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def reconstruct_tar_from_chunks(chunk_stream, ostree_root: Path = None):
    """
    Reconstruct tar data from chunks.

    Returns (tar_data, has_ostree_refs, missing_objects)
    """
    import os
    import stat

    output = io.BytesIO()
    has_ostree_refs = False
    missing_objects = []

    while True:
        chunk = read_chunk(chunk_stream)
        if not chunk:
            break

        if chunk['type'] == CHUNK_TYPE_DATA:
            output.write(chunk['data'])
        elif chunk['type'] == CHUNK_TYPE_OSTREE:
            has_ostree_refs = True
            digest = chunk['data'].hex()

            if ostree_root:
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
                except (PermissionError, OSError) as e:
                    missing_objects.append(f"{digest} ({e})")
            else:
                missing_objects.append(digest)

    return output.getvalue(), has_ostree_refs, missing_objects

def get_config_and_diff_ids(tar, all_blobs, manifest_data):
    """Extract config and diff_ids from manifest."""
    config_digest = manifest_data.get('config', {}).get('digest', '').split(':')[-1]
    if config_digest not in all_blobs:
        return None, []

    config_member = all_blobs[config_digest]
    config_data = json.load(tar.extractfile(config_member))
    diff_ids = config_data.get('rootfs', {}).get('diff_ids', [])

    return config_digest, diff_ids

def inspect_delta(delta_path: str, verbose: bool = False, verify: bool = False, ostree_repo: str = None):
    """Inspect a delta file and show its structure."""
    print(f"Inspecting delta file: {delta_path}")
    print()

    ostree_root = Path(ostree_repo) if ostree_repo else None
    if verify and ostree_root and not ostree_root.exists():
        print(f"Warning: Ostree repo not found: {ostree_repo}", file=sys.stderr)
        ostree_root = None

    with gzip.open(delta_path, 'rb') as gz_file:
        with tarfile.open(fileobj=gz_file, mode='r') as tar:
            print("Delta file contents:")
            print("=" * 80)

            # Read index.json to find manifest
            try:
                index_member = tar.getmember('index.json')
                index_data = json.load(tar.extractfile(index_member))
            except KeyError:
                print("Error: No index.json found", file=sys.stderr)
                return

            # Find layer digests from manifest
            layer_digests = set()
            all_blobs = {}

            for member in tar.getmembers():
                if member.name.startswith('blobs/sha256/') and not member.name.endswith('/'):
                    digest = member.name.split('/')[-1]
                    all_blobs[digest] = member

            # Read manifest to identify layers and get diff_ids if verifying
            manifest_data = None
            diff_ids = []
            config_digest = None

            for manifest_desc in index_data.get('manifests', []):
                manifest_digest = manifest_desc['digest'].split(':')[-1]
                if manifest_digest in all_blobs:
                    manifest_member = all_blobs[manifest_digest]
                    manifest_data = json.load(tar.extractfile(manifest_member))

                    for layer in manifest_data.get('layers', []):
                        layer_digest = layer['digest'].split(':')[-1]
                        layer_digests.add(layer_digest)

                    if verify:
                        config_digest, diff_ids = get_config_and_diff_ids(tar, all_blobs, manifest_data)

            # Categorize blobs (preserve layer order from manifest)
            # Also build mapping of layer digest to diff_id index
            layer_blobs = []
            layer_to_diff_id_idx = {}
            other_blobs = []

            if manifest_data:
                for idx, layer in enumerate(manifest_data.get('layers', [])):
                    layer_digest = layer['digest'].split(':')[-1]
                    layer_to_diff_id_idx[layer_digest] = idx
                    if layer_digest in all_blobs:
                        layer_blobs.append(all_blobs[layer_digest])

            for digest, member in all_blobs.items():
                if digest not in layer_digests:
                    other_blobs.append(member)

            print(f"\nOCI metadata files:")
            print(f"  index.json ({format_size(index_member.size)})")

            try:
                oci_layout_member = tar.getmember('oci-layout')
                print(f"  oci-layout ({format_size(oci_layout_member.size)})")
            except KeyError:
                pass

            print(f"\nNon-layer blobs (manifests, configs):")
            for member in other_blobs:
                digest = member.name.split('/')[-1]
                print(f"  {digest[:16]}... ({format_size(member.size)})")

            print(f"\nLayer blobs: {len(layer_blobs)}")
            if verify and diff_ids:
                print(f"\nVerification mode enabled (config: {config_digest[:16]}...)")
                print(f"Expected diff_ids: {len(diff_ids)}")

            for idx, member in enumerate(layer_blobs):
                digest = member.name.split('/')[-1]
                print(f"\n  Layer {idx}: {digest[:16]}...")
                print(f"    Size: {format_size(member.size)}")

                f = tar.extractfile(member)

                # Peek at first 2 bytes to detect format
                first_bytes = f.read(2)
                f.seek(0)

                # Gzip magic bytes are 0x1f 0x8b
                is_gzipped = (len(first_bytes) >= 2 and
                             first_bytes[0] == 0x1f and
                             first_bytes[1] == 0x8b)

                if is_gzipped:
                    print(f"    Format: Original compressed (gzip)")
                else:
                    print(f"    Format: Chunked")

                    chunk_stats = {
                        'data_chunks': 0,
                        'ostree_chunks': 0,
                        'data_bytes': 0,
                        'ostree_bytes': 0
                    }

                    while True:
                        chunk = read_chunk(f)
                        if not chunk:
                            break

                        if chunk['type'] == CHUNK_TYPE_DATA:
                            chunk_stats['data_chunks'] += 1
                            chunk_stats['data_bytes'] += chunk['size']
                        elif chunk['type'] == CHUNK_TYPE_OSTREE:
                            chunk_stats['ostree_chunks'] += 1
                            chunk_stats['ostree_bytes'] += chunk['size']
                            if verbose:
                                ostree_digest = chunk['data'].hex()
                                print(f"      OSTREE ref: {ostree_digest}")

                    print(f"    DATA chunks: {chunk_stats['data_chunks']} ({format_size(chunk_stats['data_bytes'])})")
                    print(f"    OSTREE chunks: {chunk_stats['ostree_chunks']} ({format_size(chunk_stats['ostree_bytes'])})")

                    if chunk_stats['ostree_chunks'] > 0:
                        saved_estimate = chunk_stats['ostree_chunks'] * 512 * 1024
                        print(f"    Estimated ostree savings: ~{format_size(saved_estimate)}")

                if verify and diff_ids:
                    diff_id_idx = layer_to_diff_id_idx.get(digest)
                    if diff_id_idx is not None and diff_id_idx < len(diff_ids):
                        expected_diff_id = diff_ids[diff_id_idx].split(':')[-1] if ':' in diff_ids[diff_id_idx] else diff_ids[diff_id_idx]
                        print(f"    Expected diff_id[{diff_id_idx}]: {expected_diff_id[:16]}...")

                        f = tar.extractfile(member)

                        if is_gzipped:
                            # Original compressed - just uncompress and hash
                            tar_data = gzip.decompress(f.read())
                            computed_hash = hashlib.sha256(tar_data).hexdigest()
                            if computed_hash == expected_diff_id:
                                print(f"    ✅ Checksum verified: {computed_hash[:16]}...")
                            else:
                                print(f"    ❌ Checksum mismatch!")
                                print(f"       Expected: {expected_diff_id[:16]}...")
                                print(f"       Got:      {computed_hash[:16]}...")
                        else:
                            # Chunked - reconstruct then hash
                            tar_data, has_ostree, missing = reconstruct_tar_from_chunks(f, ostree_root)

                            if missing and not ostree_root:
                                print(f"    ⚠️  Cannot verify: contains {len(missing)} OSTREE refs, needs --ostree-repo")
                            elif missing:
                                print(f"    ❌ Cannot verify: {len(missing)} ostree objects missing from repo")
                                if verbose:
                                    for obj in missing[:5]:
                                        print(f"       Missing: {obj}")
                            else:
                                computed_hash = hashlib.sha256(tar_data).hexdigest()
                                if computed_hash == expected_diff_id:
                                    print(f"    ✅ Checksum verified: {computed_hash[:16]}...")
                                else:
                                    print(f"    ❌ Checksum mismatch!")
                                    print(f"       Expected: {expected_diff_id[:16]}...")
                                    print(f"       Got:      {computed_hash[:16]}...")

def main():
    parser = argparse.ArgumentParser(
        description='Inspect a bootc OCI delta file'
    )
    parser.add_argument('delta_file', help='Path to delta file (.tar.gz)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Show verbose output including ostree digests')
    parser.add_argument('--verify', action='store_true',
                        help='Verify layer checksums against config diff_ids')
    parser.add_argument('--ostree-repo', metavar='PATH',
                        help='Path to ostree repo for reconstructing OSTREE chunks (e.g., /sysroot/ostree/repo)')

    args = parser.parse_args()

    if not Path(args.delta_file).exists():
        print(f"Error: Delta file not found: {args.delta_file}", file=sys.stderr)
        sys.exit(1)

    inspect_delta(args.delta_file, args.verbose, args.verify, args.ostree_repo)

if __name__ == '__main__':
    main()
