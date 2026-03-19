#!/usr/bin/env python3

import argparse
import sys
import tarfile
from pathlib import Path

from oci_delta_common import parse_oci_image, find_ostree_objects_in_layer


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
    _, old_layers, old_blobs, old_diff_ids = parse_oci_image(old_image)

    print("Parsing new image...")
    _, new_layers, new_blobs, new_diff_ids = parse_oci_image(new_image)

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
    print(
        f"Recompressed layers (same content, different compression, can skip): {len(recompressed_layers)}"
    )
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

    with tarfile.open(old_image, "r") as tar:
        for layer_digest in old_layers:
            ostree_files = find_ostree_objects_in_layer(tar, layer_digest, old_blobs)
            old_ostree_objects.update(ostree_files)

    print(f"Found {len(old_ostree_objects)} ostree object files in old image")

    # Scan new-only layers for ostree objects that exist in old image
    print("\nScanning new layers for ostree objects that exist in old image...")

    total_ostree_in_new = 0
    reusable_ostree = 0

    with tarfile.open(new_image, "r") as tar:
        for layer_digest in new_only_layers:
            print(f"\n  Layer {layer_digest[:16]}...")
            ostree_files = find_ostree_objects_in_layer(tar, layer_digest, new_blobs)

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
    print(
        f"  - {len(recompressed_layers)} recompressed layers can be skipped (same content)"
    )
    print(f"  - {len(new_only_layers)} layers need to be processed")

    print(f"\nOstree object optimization:")
    print(f"  - {total_ostree_in_new} ostree objects in new layers")
    print(
        f"  - {reusable_ostree} can reuse content from old image (keep tar headers only)"
    )
    print(f"  - {total_ostree_in_new - reusable_ostree} need full content")

    if total_ostree_in_new > 0:
        savings_pct = (reusable_ostree / total_ostree_in_new) * 100
        print(f"  - Potential ostree object reuse: {savings_pct:.1f}%")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze delta opportunities between two bootc OCI images"
    )
    parser.add_argument("old_image", help="Path to old OCI image tar file")
    parser.add_argument("new_image", help="Path to new OCI image tar file")

    args = parser.parse_args()

    # Validate files exist
    if not Path(args.old_image).exists():
        print(f"Error: Old image not found: {args.old_image}", file=sys.stderr)
        sys.exit(1)

    if not Path(args.new_image).exists():
        print(f"Error: New image not found: {args.new_image}", file=sys.stderr)
        sys.exit(1)

    analyze_delta(args.old_image, args.new_image)


if __name__ == "__main__":
    main()
