#!/usr/bin/env python3

import argparse
import json
import tarfile
import hashlib
import sys
from pathlib import Path
from typing import Dict, Set, List, Tuple

def parse_oci_image(image_path: str) -> Tuple[Dict, Set[str], Dict[str, tarfile.TarInfo], Dict[str, str]]:
    """
    Parse an OCI image tar file and extract:
    - index.json content
    - set of blob digests (layer blobs)
    - mapping of blob digest to TarInfo for accessing blob data
    - mapping of diff_id (uncompressed digest) to compressed layer digest
    """
    index_data = None
    layer_blobs = set()
    blob_members = {}
    diff_id_to_digest = {}

    with tarfile.open(image_path, 'r') as tar:
        # Read index.json
        try:
            index_member = tar.getmember('index.json')
            f = tar.extractfile(index_member)
            index_data = json.load(f)
        except KeyError:
            print(f"Error: No index.json found in {image_path}", file=sys.stderr)
            sys.exit(1)

        # Collect all blob members
        for member in tar.getmembers():
            if member.name.startswith('blobs/sha256/'):
                digest = member.name.split('/')[-1]
                blob_members[digest] = member

        # Parse manifest to find layer blobs
        for manifest_desc in index_data.get('manifests', []):
            manifest_digest = manifest_desc['digest'].split(':')[-1]
            if manifest_digest in blob_members:
                manifest_member = blob_members[manifest_digest]
                f = tar.extractfile(manifest_member)
                manifest = json.load(f)

                # Get config to access diff_ids
                config_digest = manifest.get('config', {}).get('digest', '').split(':')[-1]
                if config_digest and config_digest in blob_members:
                    config_member = blob_members[config_digest]
                    config_f = tar.extractfile(config_member)
                    config_data = json.load(config_f)

                    # Get diff_ids (uncompressed layer digests)
                    diff_ids = config_data.get('rootfs', {}).get('diff_ids', [])
                    layers = manifest.get('layers', [])

                    # Map diff_id to compressed layer digest
                    for i, layer in enumerate(layers):
                        layer_digest = layer['digest'].split(':')[-1]
                        layer_blobs.add(layer_digest)

                        if i < len(diff_ids):
                            diff_id = diff_ids[i].split(':')[-1]
                            diff_id_to_digest[diff_id] = layer_digest
                else:
                    # Fallback if no config available
                    for layer in manifest.get('layers', []):
                        layer_digest = layer['digest'].split(':')[-1]
                        layer_blobs.add(layer_digest)

    return index_data, layer_blobs, blob_members, diff_id_to_digest

def list_layer_contents(tar: tarfile.TarFile, blob_digest: str, blob_members: Dict[str, tarfile.TarInfo]) -> List[str]:
    """
    List contents of a compressed layer blob.
    Layer blobs are gzipped tar files.
    """
    if blob_digest not in blob_members:
        return []

    blob_member = blob_members[blob_digest]
    blob_file = tar.extractfile(blob_member)

    contents = []
    try:
        with tarfile.open(fileobj=blob_file, mode='r:*') as layer_tar:
            for member in layer_tar.getmembers():
                contents.append(member.name)
    except Exception as e:
        print(f"Warning: Could not read layer {blob_digest}: {e}", file=sys.stderr)

    return contents

def find_ostree_objects(file_list: List[str]) -> Set[str]:
    """
    Find ostree object files in a file list.
    Pattern: sysroot/ostree/repo/objects/XX/YYYYYY....file
    """
    ostree_objects = set()
    for filepath in file_list:
        # Match pattern like sysroot/ostree/repo/objects/c8/552977359a0e4484f572b9bf94b79c0afee63852aa9491bc6fac5274b87168.file
        if filepath.startswith('sysroot/ostree/repo/objects/') and filepath.endswith('.file'):
            ostree_objects.add(filepath)
    return ostree_objects

def analyze_delta(old_image: str, new_image: str):
    """
    Analyze what can be optimized in a delta update from old_image to new_image.
    """
    print(f"Analyzing delta from:")
    print(f"  Old: {old_image}")
    print(f"  New: {new_image}")
    print()

    # Parse both images
    print("Parsing old image...")
    old_index, old_layers, old_blobs, old_diff_ids = parse_oci_image(old_image)

    print("Parsing new image...")
    new_index, new_layers, new_blobs, new_diff_ids = parse_oci_image(new_image)

    print()
    print("=" * 80)
    print("LAYER ANALYSIS")
    print("=" * 80)

    # Build reverse mapping for old image: diff_id -> layer_digest
    old_diff_id_set = set(old_diff_ids.keys())

    # Find layers that match by compressed digest
    common_layers = old_layers & new_layers

    # Find layers that match by diff_id but not by compressed digest (recompressed)
    recompressed_layers = set()
    for diff_id, new_layer_digest in new_diff_ids.items():
        if diff_id in old_diff_id_set and new_layer_digest not in common_layers:
            recompressed_layers.add(new_layer_digest)

    # Layers that are truly new (neither digest nor diff_id matches)
    new_only_layers = new_layers - common_layers - recompressed_layers
    old_only_layers = old_layers - new_layers

    print(f"\nLayers in old image: {len(old_layers)}")
    print(f"Layers in new image: {len(new_layers)}")
    print(f"Common layers (same digest, can skip): {len(common_layers)}")
    print(f"Recompressed layers (same content, different compression, can skip): {len(recompressed_layers)}")
    print(f"New layers (need to process): {len(new_only_layers)}")
    print(f"Old-only layers (removed): {len(old_only_layers)}")

    if common_layers:
        print("\nCommon layer digests (these can be completely skipped):")
        for digest in sorted(common_layers):
            print(f"  - {digest[:16]}...")

    if recompressed_layers:
        print("\nRecompressed layer digests (same content, different compression):")
        for digest in sorted(recompressed_layers):
            # Find the matching diff_id
            diff_id = None
            for did, dg in new_diff_ids.items():
                if dg == digest:
                    diff_id = did
                    break
            if diff_id:
                print(f"  - {digest[:16]}... (diff_id: {diff_id[:16]}...)")
            else:
                print(f"  - {digest[:16]}...")

    # Collect all ostree objects from old image
    print()
    print("=" * 80)
    print("OSTREE OBJECT ANALYSIS")
    print("=" * 80)

    print("\nScanning old image for ostree objects...")
    old_ostree_objects = set()

    with tarfile.open(old_image, 'r') as tar:
        for layer_digest in old_layers:
            contents = list_layer_contents(tar, layer_digest, old_blobs)
            ostree_files = find_ostree_objects(contents)
            old_ostree_objects.update(ostree_files)

    print(f"Found {len(old_ostree_objects)} ostree object files in old image")

    # Scan new-only layers for ostree objects that exist in old image
    print("\nScanning new layers for ostree objects that exist in old image...")

    total_ostree_in_new = 0
    reusable_ostree = 0

    with tarfile.open(new_image, 'r') as tar:
        for layer_digest in new_only_layers:
            print(f"\n  Layer {layer_digest[:16]}...")
            contents = list_layer_contents(tar, layer_digest, new_blobs)
            ostree_files = find_ostree_objects(contents)

            if ostree_files:
                total_ostree_in_new += len(ostree_files)
                reusable = ostree_files & old_ostree_objects
                reusable_ostree += len(reusable)

                print(f"    Total ostree objects: {len(ostree_files)}")
                print(f"    Reusable from old image: {len(reusable)}")

                if reusable and len(reusable) <= 10:
                    print(f"    Reusable objects:")
                    for obj in sorted(reusable):
                        print(f"      - {obj}")
                elif reusable:
                    print(f"    First 10 reusable objects:")
                    for obj in sorted(reusable)[:10]:
                        print(f"      - {obj}")

    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\nLayer optimization:")
    print(f"  - {len(common_layers)} identical layers can be skipped")
    print(f"  - {len(recompressed_layers)} recompressed layers can be skipped (same content)")
    print(f"  - {len(new_only_layers)} layers need to be processed")

    print(f"\nOstree object optimization:")
    print(f"  - {total_ostree_in_new} ostree objects in new layers")
    print(f"  - {reusable_ostree} can reuse content from old image (keep tar headers only)")
    print(f"  - {total_ostree_in_new - reusable_ostree} need full content")

    if total_ostree_in_new > 0:
        savings_pct = (reusable_ostree / total_ostree_in_new) * 100
        print(f"  - Potential ostree object reuse: {savings_pct:.1f}%")

def main():
    parser = argparse.ArgumentParser(
        description='Analyze delta opportunities between two bootc OCI images'
    )
    parser.add_argument('old_image', help='Path to old OCI image tar file')
    parser.add_argument('new_image', help='Path to new OCI image tar file')

    args = parser.parse_args()

    # Validate files exist
    if not Path(args.old_image).exists():
        print(f"Error: Old image not found: {args.old_image}", file=sys.stderr)
        sys.exit(1)

    if not Path(args.new_image).exists():
        print(f"Error: New image not found: {args.new_image}", file=sys.stderr)
        sys.exit(1)

    analyze_delta(args.old_image, args.new_image)

if __name__ == '__main__':
    main()
