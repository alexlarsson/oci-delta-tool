# Delta File Chunk Format

## Overview

Delta files are compressed tar archives containing a modified OCI image structure where layer blobs use a chunked format to enable deduplication of ostree objects.

## Delta File Structure

```
delta.tar.gz                    # Compressed tar file
├── index.json                  # Same as source OCI image
├── oci-layout                  # Same as source OCI image
├── blobs/sha256/
│   ├── <manifest-digest>       # Manifest blob (same as source)
│   ├── <config-digest>         # Config blob (same as source)
│   └── <layer-digest>          # Chunked layer blob OR original compressed blob
```

## Reconstructed OCI Archive Structure

When a delta is applied, it creates a standard OCI archive:

```
output.tar                      # Uncompressed tar file
├── index.json                  # Updated with new manifest digest
├── oci-layout                  # Same as delta
├── blobs/sha256/
│   ├── <new-manifest-digest>   # Updated manifest with new layer digests
│   ├── <config-digest>         # Config blob (unchanged)
│   ├── <layer-digest>          # Original layer (if no OSTREE chunks)
│   └── <new-layer-digest>      # Recompressed layer (if had OSTREE chunks)
```

### Layer Digest Mapping

Layers that contain OSTREE chunks are reconstructed and recompressed during delta application. This changes their compressed blob digest. To maintain compatibility with future delta updates, the manifest includes an annotation on recompressed layers:

```json
{
  "digest": "sha256:<new-digest>",
  "size": 12345678,
  "annotations": {
    "org.containers.bootc.delta.original-digest": "sha256:<original-digest>"
  }
}
```

When bootc imports the image, it should create ostree refs for both the new digest and the original digest, pointing to the same content. This ensures future deltas referencing the original digest will find the layer in the local ostree repo.

## Chunk Format

Layer blobs in the delta file are uncompressed and use a simple binary chunk format:

```
[Chunk Type: 1 byte][Size: 8 bytes][Data: variable]
```

### Chunk Types

- `0x00` (DATA): Regular data chunk containing literal bytes
- `0x01` (OSTREE): Reference to an ostree object

### Chunk Structure

#### DATA Chunk
```
Type: 0x00
Size: uint64 (big-endian) - number of data bytes
Data: raw bytes
```

#### OSTREE Chunk
```
Type: 0x01
Size: uint64 (big-endian) - 32 (SHA256 digest size)
Data: 32 bytes of binary SHA256 digest
```

## Layer Reconstruction

To reconstruct a layer blob:

1. Read chunks sequentially from the chunked layer blob
2. For DATA chunks: write the data bytes to the output tar stream
3. For OSTREE chunks:
   - Read the digest string
   - Construct path: `/ostree/repo/objects/{digest[0:2]}/{digest[2:]}.file`
   - Read the file content from the old bootc installation
   - Write the content to the output tar stream
4. Continue until all chunks are processed

## Space Savings

The delta format provides savings through:

1. **Layer deduplication**: Identical layers between old and new are omitted entirely
2. **Ostree object deduplication**: File content for ostree objects present in the old image is replaced with small digest references
3. **Outer compression**: The entire delta tar file is gzip-compressed

## Example

For an ostree object file in the tar stream:
```
Original layer (compressed): ~500 MB
Uncompressed tar stream with ostree object (1 MB file):
  [tar header: 512 bytes][file data: 1,048,576 bytes][padding: 0 bytes]

Chunked format:
  [DATA chunk: 512 byte header]
  [OSTREE chunk: 32 byte binary digest instead of 1,048,576 bytes]

Savings per 1 MB ostree object: ~1,048,544 bytes
```
