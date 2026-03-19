#!/usr/bin/env python3

import argparse
import tarfile
import sys
import gzip
import io
from pathlib import Path

from oci_delta_common import (
    parse_oci_image,
    find_ostree_objects_in_layer,
    chunk_layer,
)


def create_delta(old_image: str, new_image: str, output_path: str):
    """Create a delta update file."""
    print(f"Creating delta from:")
    print(f"  Old: {old_image}")
    print(f"  New: {new_image}")
    print(f"  Output: {output_path}")
    print()

    print("Parsing images...")
    _, old_layers, old_blobs, old_diff_ids = parse_oci_image(old_image)
    _, new_layers, new_blobs, new_diff_ids = parse_oci_image(new_image)

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

    print(f"Common layers (same digest, will skip): {len(common_layers)}")
    print(
        f"Recompressed layers (same content, different compression, will skip): {len(recompressed_layers)}"
    )
    print(f"New layers (will process): {len(new_only_layers)}")

    print("\nCollecting ostree objects from old image...")
    old_ostree_objects = set()
    with tarfile.open(old_image, "r") as tar:
        for layer_digest in old_layers:
            ostree_files = find_ostree_objects_in_layer(tar, layer_digest, old_blobs)
            old_ostree_objects.update(ostree_files)

    print(f"Found {len(old_ostree_objects)} ostree objects in old image")

    print("\nCreating delta file...")
    with gzip.open(output_path, "wb", compresslevel=6) as delta_tar_gz:
        with tarfile.open(fileobj=delta_tar_gz, mode="w") as delta_tar:
            with tarfile.open(new_image, "r") as new_tar:
                print("  Copying index.json...")
                index_member = new_tar.getmember("index.json")
                delta_tar.addfile(index_member, new_tar.extractfile(index_member))

                try:
                    print("  Copying oci-layout...")
                    oci_layout_member = new_tar.getmember("oci-layout")
                    delta_tar.addfile(
                        oci_layout_member, new_tar.extractfile(oci_layout_member)
                    )
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

                    reusable_in_layer = (
                        find_ostree_objects_in_layer(new_tar, layer_digest, new_blobs)
                        & old_ostree_objects
                    )

                    if reusable_in_layer:
                        print(
                            f"      Found {len(reusable_in_layer)} reusable ostree objects - chunking"
                        )

                        blob_file = new_tar.extractfile(blob_member)
                        with gzip.GzipFile(fileobj=blob_file) as gz:
                            uncompressed_tar = io.BytesIO(gz.read())

                        chunked_data = chunk_layer(uncompressed_tar, reusable_in_layer)

                        chunked_member = tarfile.TarInfo(
                            name=f"blobs/sha256/{layer_digest}"
                        )
                        chunked_member.size = len(chunked_data)
                        delta_tar.addfile(chunked_member, io.BytesIO(chunked_data))

                        print(f"      Original: {blob_member.size} bytes")
                        print(f"      Chunked: {len(chunked_data)} bytes")
                    else:
                        print(
                            f"      No reusable ostree objects - copying original compressed blob"
                        )
                        delta_tar.addfile(blob_member, new_tar.extractfile(blob_member))

                # Note: Common and recompressed layers are not included in the delta
                # They can be reused from the old image without transfer

    delta_size = Path(output_path).stat().st_size
    new_size = Path(new_image).stat().st_size

    print(f"\nDelta file created successfully!")
    print(f"  New image size: {new_size:,} bytes")
    print(f"  Delta file size: {delta_size:,} bytes")
    print(
        f"  Savings: {new_size - delta_size:,} bytes ({(1 - delta_size/new_size)*100:.1f}%)"
    )
    print(f"\nLayer summary:")
    print(f"  Identical layers (skipped): {len(common_layers)}")
    print(f"  Recompressed layers (same content, skipped): {len(recompressed_layers)}")
    print(f"  New layers (included): {len(new_only_layers)}")


def main():
    parser = argparse.ArgumentParser(
        description="Create delta update between two bootc OCI images"
    )
    parser.add_argument("old_image", help="Path to old OCI image tar file")
    parser.add_argument("new_image", help="Path to new OCI image tar file")
    parser.add_argument("output", help="Path for output delta file (.tar.gz)")

    args = parser.parse_args()

    if not Path(args.old_image).exists():
        print(f"Error: Old image not found: {args.old_image}", file=sys.stderr)
        sys.exit(1)

    if not Path(args.new_image).exists():
        print(f"Error: New image not found: {args.new_image}", file=sys.stderr)
        sys.exit(1)

    create_delta(args.old_image, args.new_image, args.output)


if __name__ == "__main__":
    main()
