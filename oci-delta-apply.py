#!/usr/bin/env python3

import argparse
import hashlib
import json
import tarfile
import sys
import gzip
import io
import tempfile
import subprocess
from pathlib import Path


def apply_delta(delta_path: str, output_path: str, delta_source: str = "/"):
    """Apply a delta file to create a standard OCI archive."""
    print(f"Applying delta:")
    print(f"  Delta: {delta_path}")
    print(f"  Output: {output_path}")
    print(f"  Delta source: {delta_source}")
    print()

    source_root = Path(delta_source)
    if not source_root.exists():
        print(f"Warning: Delta source not found: {delta_source}", file=sys.stderr)

    with gzip.open(delta_path, "rb") as delta_gz:
        with tarfile.open(fileobj=delta_gz, mode="r") as delta_tar:
            # First, identify layer blobs from the manifest
            print("Reading delta structure...")

            try:
                index_member = delta_tar.getmember("index.json")
                index_data = json.load(delta_tar.extractfile(index_member))
            except KeyError:
                print("Error: No index.json found in delta", file=sys.stderr)
                sys.exit(1)

            # Find layer digests from manifest
            layer_digests = set()
            all_members = {}

            for member in delta_tar.getmembers():
                all_members[member.name] = member
                if member.name.startswith("blobs/sha256/") and not member.name.endswith(
                    "/"
                ):
                    digest = member.name.split("/")[-1]

            # Read manifest to identify layers and preserve order
            manifest_list = []
            layer_to_diff_id = {}  # layer_digest -> expected diff_id

            for manifest_desc in index_data.get("manifests", []):
                manifest_digest = manifest_desc["digest"].split(":")[-1]
                manifest_path = f"blobs/sha256/{manifest_digest}"
                if manifest_path in all_members:
                    manifest_member = all_members[manifest_path]
                    manifest_data = json.load(delta_tar.extractfile(manifest_member))
                    manifest_list.append(
                        (manifest_desc, manifest_data, manifest_digest)
                    )

                    for layer in manifest_data.get("layers", []):
                        layer_digest = layer["digest"].split(":")[-1]
                        layer_digests.add(layer_digest)

                    # Extract diff_ids from config for validation
                    config_digest = manifest_data.get("config", {}).get("digest", "").split(":")[-1]
                    if config_digest and f"blobs/sha256/{config_digest}" in all_members:
                        config_member = all_members[f"blobs/sha256/{config_digest}"]
                        config_data = json.load(delta_tar.extractfile(config_member))
                        diff_ids = config_data.get("rootfs", {}).get("diff_ids", [])

                        # Map layer digests to their expected diff_ids
                        layers = manifest_data.get("layers", [])
                        for i, layer in enumerate(layers):
                            if i < len(diff_ids):
                                layer_digest = layer["digest"].split(":")[-1]
                                expected_diff_id = diff_ids[i].split(":")[-1]
                                layer_to_diff_id[layer_digest] = expected_diff_id

            print(f"Found {len(layer_digests)} layers in delta")
            print("\nCreating standard OCI archive...")

            # Track digest mapping for recompressed layers
            digest_mapping = {}  # old_digest -> new_digest
            blob_sizes = {}  # new_digest -> size

            # Create output tar file
            with tarfile.open(output_path, "w") as output_tar:
                # Don't copy metadata files yet - we'll update them at the end

                if "oci-layout" in all_members:
                    print("  Copying oci-layout...")
                    oci_layout_member = all_members["oci-layout"]
                    output_tar.addfile(
                        oci_layout_member, delta_tar.extractfile(oci_layout_member)
                    )

                # Process all blobs (layers and non-layer blobs)
                for member in delta_tar.getmembers():
                    if not member.name.startswith(
                        "blobs/sha256/"
                    ) or member.name.endswith("/"):
                        continue

                    if member.name in ["index.json", "oci-layout"]:
                        continue

                    digest = member.name.split("/")[-1]

                    if digest in layer_digests:
                        # This is a layer - check if it's chunked or original compressed
                        blob_stream = delta_tar.extractfile(member)

                        # Peek at first 2 bytes to detect format
                        first_bytes = blob_stream.read(2)
                        blob_stream.seek(0)

                        # Gzip magic bytes are 0x1f 0x8b
                        is_gzipped = (
                            len(first_bytes) >= 2
                            and first_bytes[0] == 0x1F
                            and first_bytes[1] == 0x8B
                        )

                        if is_gzipped:
                            # Original compressed layer - copy as-is with same digest
                            print(
                                f"  Copying layer {digest[:16]}... (original compressed)"
                            )
                            blob_data = blob_stream.read()
                            layer_member = tarfile.TarInfo(name=member.name)
                            layer_member.size = len(blob_data)
                            output_tar.addfile(layer_member, io.BytesIO(blob_data))
                            blob_sizes[digest] = len(blob_data)
                            # No digest change - same as original
                        else:
                            # tar-diff layer - reconstruct using tar-patch
                            print(f"  Processing layer {digest[:16]}... (tar-diff)")

                            with tempfile.TemporaryDirectory() as tmpdir:
                                tmpdir_path = Path(tmpdir)

                                # Extract tar-diff to temp file
                                tar_diff_path = tmpdir_path / f"{digest[:16]}.tar-diff"
                                with open(tar_diff_path, "wb") as f:
                                    f.write(blob_stream.read())

                                # Run tar-patch to reconstruct
                                reconstructed_path = tmpdir_path / f"{digest[:16]}.tar"
                                tar_patch_cmd = [
                                    "tar-patch",
                                    str(tar_diff_path),
                                    str(source_root),
                                    str(reconstructed_path),
                                ]

                                try:
                                    subprocess.run(
                                        tar_patch_cmd, check=True, capture_output=True
                                    )
                                except subprocess.CalledProcessError as e:
                                    print(
                                        f"    Error: tar-patch failed: {e.stderr.decode()}",
                                        file=sys.stderr,
                                    )
                                    sys.exit(1)

                                # Read reconstructed tar
                                with open(reconstructed_path, "rb") as f:
                                    tar_data = f.read()

                                # Validate diff_id if we have the expected value
                                actual_diff_id = hashlib.sha256(tar_data).hexdigest()
                                if digest in layer_to_diff_id:
                                    expected_diff_id = layer_to_diff_id[digest]
                                    if actual_diff_id != expected_diff_id:
                                        print(
                                            f"    Error: diff_id mismatch!",
                                            file=sys.stderr,
                                        )
                                        print(
                                            f"      Expected: {expected_diff_id}",
                                            file=sys.stderr,
                                        )
                                        print(
                                            f"      Actual:   {actual_diff_id}",
                                            file=sys.stderr,
                                        )
                                        sys.exit(1)
                                    print(f"    Validated diff_id: {actual_diff_id[:16]}...")

                                # Compress the reconstructed tar data with reproducible settings
                                # Match podman/buildah compression: level 9, no filename, mtime=0
                                compressed = io.BytesIO()
                                with gzip.GzipFile(
                                    fileobj=compressed,
                                    mode="wb",
                                    compresslevel=9,
                                    mtime=0,
                                    filename="",
                                ) as gz:
                                    gz.write(tar_data)

                                compressed_data = compressed.getvalue()

                                # Compute new digest for compressed data
                                new_digest = hashlib.sha256(compressed_data).hexdigest()
                                digest_mapping[digest] = new_digest
                                blob_sizes[new_digest] = len(compressed_data)

                                print(f"    Reconstructed: {len(tar_data):,} bytes")
                                print(f"    Compressed: {len(compressed_data):,} bytes")
                                print(f"    Old digest: {digest[:16]}...")
                                print(f"    New digest: {new_digest[:16]}...")

                                # Create new tar member with new digest in name
                                layer_member = tarfile.TarInfo(
                                    name=f"blobs/sha256/{new_digest}"
                                )
                                layer_member.size = len(compressed_data)
                                output_tar.addfile(
                                    layer_member, io.BytesIO(compressed_data)
                                )
                    else:
                        # Non-layer blob (manifest, config) - we'll handle these after updating
                        pass

                # Now update and write manifests with new layer digests
                print("\n  Updating manifests and metadata...")
                updated_manifests = []

                for manifest_desc, manifest_data, old_manifest_digest in manifest_list:
                    # Update layer digests in manifest
                    if "layers" in manifest_data:
                        for layer in manifest_data["layers"]:
                            old_layer_digest = layer["digest"].split(":")[-1]
                            if old_layer_digest in digest_mapping:
                                new_layer_digest = digest_mapping[old_layer_digest]

                                # Store original digest in annotations for bootc compatibility
                                if "annotations" not in layer:
                                    layer["annotations"] = {}
                                layer["annotations"][
                                    "org.containers.bootc.delta.original-digest"
                                ] = f"sha256:{old_layer_digest}"

                                # Update to new digest
                                layer["digest"] = f"sha256:{new_layer_digest}"
                                layer["size"] = blob_sizes[new_layer_digest]
                            elif old_layer_digest in blob_sizes:
                                # Not remapped, but we have size tracked
                                layer["size"] = blob_sizes[old_layer_digest]

                    # Serialize updated manifest
                    manifest_json = json.dumps(
                        manifest_data, indent=3, separators=(",", ": ")
                    )
                    manifest_bytes = manifest_json.encode("utf-8")

                    # Compute new manifest digest
                    new_manifest_digest = hashlib.sha256(manifest_bytes).hexdigest()

                    # Write updated manifest
                    manifest_member = tarfile.TarInfo(
                        name=f"blobs/sha256/{new_manifest_digest}"
                    )
                    manifest_member.size = len(manifest_bytes)
                    output_tar.addfile(manifest_member, io.BytesIO(manifest_bytes))

                    print(
                        f"    Updated manifest: {old_manifest_digest[:16]}... -> {new_manifest_digest[:16]}..."
                    )

                    # Track for updating index
                    updated_manifests.append(
                        (manifest_desc, new_manifest_digest, len(manifest_bytes))
                    )

                # Copy config and other non-layer, non-manifest blobs
                for member in delta_tar.getmembers():
                    if not member.name.startswith(
                        "blobs/sha256/"
                    ) or member.name.endswith("/"):
                        continue
                    if member.name in ["index.json", "oci-layout"]:
                        continue

                    digest = member.name.split("/")[-1]

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
                for (
                    manifest_desc,
                    new_manifest_digest,
                    manifest_size,
                ) in updated_manifests:
                    manifest_desc["digest"] = f"sha256:{new_manifest_digest}"
                    manifest_desc["size"] = manifest_size

                # Write updated index.json
                index_json = json.dumps(index_data, indent=3, separators=(",", ": "))
                index_bytes = index_json.encode("utf-8")
                index_member_new = tarfile.TarInfo(name="index.json")
                index_member_new.size = len(index_bytes)
                output_tar.addfile(index_member_new, io.BytesIO(index_bytes))

                print(f"    Updated index.json")

    output_size = Path(output_path).stat().st_size
    delta_size = Path(delta_path).stat().st_size

    print(f"\nOCI archive created successfully!")
    print(f"  Delta size: {delta_size:,} bytes")
    print(f"  Output size: {output_size:,} bytes")
    print(
        f"  Expansion: {output_size - delta_size:,} bytes ({(output_size/delta_size - 1)*100:.1f}%)"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Apply a bootc OCI delta file to create a standard OCI archive"
    )
    parser.add_argument("delta_file", help="Path to delta file (.tar.gz)")
    parser.add_argument("output", help="Path for output OCI archive (.tar)")
    parser.add_argument(
        "--delta-source",
        metavar="PATH",
        default="/",
        help="Source directory for tar-patch delta reconstruction (default: /)",
    )

    args = parser.parse_args()

    if not Path(args.delta_file).exists():
        print(f"Error: Delta file not found: {args.delta_file}", file=sys.stderr)
        sys.exit(1)

    apply_delta(args.delta_file, args.output, args.delta_source)


if __name__ == "__main__":
    main()
