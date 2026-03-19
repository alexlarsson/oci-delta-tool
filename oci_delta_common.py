import json
import tarfile
import struct
import io
import os
import stat
from pathlib import Path
from typing import Dict, Set, Tuple, BinaryIO

CHUNK_TYPE_DATA = 0x00
CHUNK_TYPE_OSTREE = 0x01


def read_chunk(stream):
    """Read a single chunk from the stream."""
    chunk_type_bytes = stream.read(1)
    if not chunk_type_bytes:
        return None

    chunk_type = struct.unpack("B", chunk_type_bytes)[0]
    size_bytes = stream.read(8)
    if len(size_bytes) < 8:
        return None

    size = struct.unpack(">Q", size_bytes)[0]
    data = stream.read(size)

    return {"type": chunk_type, "size": size, "data": data}


def write_chunk(output: BinaryIO, chunk_type: int, data: bytes):
    """Write a chunk to the output stream."""
    output.write(struct.pack("B", chunk_type))
    output.write(struct.pack(">Q", len(data)))
    output.write(data)


def format_size(size_bytes):
    """Format size in human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def parse_oci_image(
    image_path: str,
) -> Tuple[Dict, Set[str], Dict[str, tarfile.TarInfo], Dict[str, str]]:
    """
    Parse an OCI image tar file and extract:
    - index.json content
    - set of blob digests (layer blobs)
    - mapping of blob digest to TarInfo for accessing blob data
    - mapping of diff_id (uncompressed digest) to compressed layer digest
    """
    import sys

    index_data = None
    layer_blobs = set()
    blob_members = {}
    diff_id_to_digest = {}

    with tarfile.open(image_path, "r") as tar:
        try:
            index_member = tar.getmember("index.json")
            f = tar.extractfile(index_member)
            index_data = json.load(f)
        except KeyError:
            print(f"Error: No index.json found in {image_path}", file=sys.stderr)
            sys.exit(1)

        for member in tar.getmembers():
            if member.name.startswith("blobs/sha256/"):
                digest = member.name.split("/")[-1]
                blob_members[digest] = member

        for manifest_desc in index_data.get("manifests", []):
            manifest_digest = manifest_desc["digest"].split(":")[-1]
            if manifest_digest in blob_members:
                manifest_member = blob_members[manifest_digest]
                f = tar.extractfile(manifest_member)
                manifest = json.load(f)

                config_digest = (
                    manifest.get("config", {}).get("digest", "").split(":")[-1]
                )
                if config_digest and config_digest in blob_members:
                    config_member = blob_members[config_digest]
                    config_f = tar.extractfile(config_member)
                    config_data = json.load(config_f)

                    diff_ids = config_data.get("rootfs", {}).get("diff_ids", [])
                    layers = manifest.get("layers", [])

                    for i, layer in enumerate(layers):
                        layer_digest = layer["digest"].split(":")[-1]
                        layer_blobs.add(layer_digest)

                        if i < len(diff_ids):
                            diff_id = diff_ids[i].split(":")[-1]
                            diff_id_to_digest[diff_id] = layer_digest
                else:
                    for layer in manifest.get("layers", []):
                        layer_digest = layer["digest"].split(":")[-1]
                        layer_blobs.add(layer_digest)

    return index_data, layer_blobs, blob_members, diff_id_to_digest


def get_config_and_diff_ids(tar, all_blobs, manifest_data):
    """Extract config and diff_ids from manifest."""
    config_digest = manifest_data.get("config", {}).get("digest", "").split(":")[-1]
    if config_digest not in all_blobs:
        return None, []

    config_member = all_blobs[config_digest]
    config_data = json.load(tar.extractfile(config_member))
    diff_ids = config_data.get("rootfs", {}).get("diff_ids", [])

    return config_digest, diff_ids


def extract_ostree_digest(filepath: str) -> str:
    """
    Extract ostree object digest from filepath.
    Example: sysroot/ostree/repo/objects/c8/552977...68.file -> c8552977...68
    """
    parts = filepath.split("/")
    if len(parts) >= 2:
        dir_name = parts[-2]
        file_name = parts[-1].replace(".file", "")
        return dir_name + file_name
    return None


def find_ostree_objects_in_layer(
    tar: tarfile.TarFile, blob_digest: str, blob_members: Dict
) -> Set[str]:
    """Find all ostree object files in a layer."""
    import sys

    if blob_digest not in blob_members:
        return set()

    blob_member = blob_members[blob_digest]
    blob_file = tar.extractfile(blob_member)

    ostree_objects = set()
    try:
        with tarfile.open(fileobj=blob_file, mode="r:*") as layer_tar:
            for member in layer_tar.getmembers():
                if (
                    member.name.startswith("sysroot/ostree/repo/objects/")
                    and member.name.endswith(".file")
                    and member.isfile()
                ):
                    ostree_objects.add(member.name)
    except Exception as e:
        print(f"Warning: Could not read layer {blob_digest}: {e}", file=sys.stderr)

    return ostree_objects


def parse_tar_header(header_bytes: bytes) -> dict:
    """Parse a 512-byte tar header."""
    if len(header_bytes) != 512:
        return None

    if header_bytes == b"\x00" * 512:
        return {"type": "zero"}

    try:
        name = header_bytes[0:100].rstrip(b"\x00").decode("utf-8", errors="ignore")
        size_str = (
            header_bytes[124:136].rstrip(b"\x00 ").decode("ascii", errors="ignore")
        )

        if not size_str:
            return {"type": "zero"}

        size = int(size_str, 8)
        return {"type": "file", "name": name, "size": size}
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

        if not header_info or header_info["type"] == "zero":
            write_chunk(output, CHUNK_TYPE_DATA, header_bytes)
            if len(header_bytes) < 512:
                break
            continue

        write_chunk(output, CHUNK_TYPE_DATA, header_bytes)

        file_size = header_info["size"]
        file_name = header_info["name"]

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


def reconstruct_layer(chunk_stream, ostree_root: Path = None):
    """
    Reconstruct a layer tar stream from chunks.

    Returns (tar_data, missing_objects)
    """
    import sys

    output = io.BytesIO()
    missing_objects = []

    while True:
        chunk = read_chunk(chunk_stream)
        if not chunk:
            break

        if chunk["type"] == CHUNK_TYPE_DATA:
            output.write(chunk["data"])
        elif chunk["type"] == CHUNK_TYPE_OSTREE:
            digest = chunk["data"].hex()

            if not ostree_root:
                missing_objects.append(digest)
                print(
                    f"Error: OSTREE chunk found but no ostree repo provided",
                    file=sys.stderr,
                )
                print(f"  Digest: {digest}", file=sys.stderr)
                continue

            ostree_path = ostree_root / "objects" / digest[0:2] / f"{digest[2:]}.file"
            try:
                if ostree_path.exists():
                    current_mode = ostree_path.stat().st_mode
                    needs_chmod = not current_mode & stat.S_IRUSR

                    if needs_chmod:
                        os.chmod(ostree_path, current_mode | stat.S_IRUSR)

                    try:
                        with open(ostree_path, "rb") as f:
                            output.write(f.read())
                    finally:
                        if needs_chmod:
                            os.chmod(ostree_path, current_mode)
                else:
                    missing_objects.append(str(ostree_path))
                    print(
                        f"Error: Ostree object not found: {ostree_path}",
                        file=sys.stderr,
                    )
            except (PermissionError, OSError) as e:
                missing_objects.append(f"{digest} ({e})")
                print(f"Error reading ostree object {digest}: {e}", file=sys.stderr)

    return output.getvalue(), missing_objects


def reconstruct_tar_from_chunks(chunk_stream, ostree_root: Path = None):
    """
    Reconstruct tar data from chunks (with has_ostree_refs flag).

    Returns (tar_data, has_ostree_refs, missing_objects)
    """
    output = io.BytesIO()
    has_ostree_refs = False
    missing_objects = []

    while True:
        chunk = read_chunk(chunk_stream)
        if not chunk:
            break

        if chunk["type"] == CHUNK_TYPE_DATA:
            output.write(chunk["data"])
        elif chunk["type"] == CHUNK_TYPE_OSTREE:
            has_ostree_refs = True
            digest = chunk["data"].hex()

            if ostree_root:
                ostree_path = (
                    ostree_root / "objects" / digest[0:2] / f"{digest[2:]}.file"
                )
                try:
                    if ostree_path.exists():
                        current_mode = ostree_path.stat().st_mode
                        needs_chmod = not current_mode & stat.S_IRUSR

                        if needs_chmod:
                            os.chmod(ostree_path, current_mode | stat.S_IRUSR)

                        try:
                            with open(ostree_path, "rb") as f:
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
