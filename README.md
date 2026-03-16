# OCI Delta Tool

Tools for creating and analyzing delta updates for bootc OCI container images.

## Tools

### oci-delta-analyze.py

Analyzes the delta optimization opportunities between two OCI images.

**Usage:**
```bash
./oci-delta-analyze.py old-image.tar new-image.tar
```

**Output:**
- Layer-level analysis (common vs new layers)
- Ostree object analysis with size information
- Potential space savings statistics

### oci-delta-create.py

Creates a delta update file between two OCI images.

**Usage:**
```bash
./oci-delta-create.py old-image.tar new-image.tar delta-output.tar.gz
```

**Output:**
- Delta file in compressed tar format with chunked layer blobs
- Statistics on compression savings

### oci-delta-inspect.py

Inspects a delta file to show its structure and chunk statistics.

**Usage:**
```bash
./oci-delta-inspect.py delta-output.tar.gz
./oci-delta-inspect.py delta-output.tar.gz -v  # Verbose mode
./oci-delta-inspect.py delta-output.tar.gz --verify --ostree-repo /sysroot/ostree/repo
```

**Output:**
- File structure of the delta archive
- Per-layer chunk statistics (DATA vs OSTREE chunks)
- Estimated space savings
- Checksum verification (with --verify)

### oci-delta-apply.py

Applies a delta file to reconstruct a standard OCI archive with gzip-compressed layers.

**Usage:**
```bash
./oci-delta-apply.py delta-output.tar.gz output-image.tar --ostree-repo /sysroot/ostree/repo
```

**Output:**
- Standard OCI archive (.tar) with gzip-compressed layers
- Reconstructed from chunked format
- OSTREE chunks resolved from ostree repository
- Layers without OSTREE chunks preserve original digests
- Layers with OSTREE chunks get new digests with annotation pointing to original
- Updated manifest and index.json with correct layer references

## How It Works

1. **Layer Deduplication**: Layers present in both old and new images are completely omitted from the delta

2. **Ostree Object Deduplication**: For new layers, ostree object files that exist in the old image have their content replaced with digest references

3. **Chunked Format**: Layer blobs use a binary chunk format (see CHUNK_FORMAT.md) with:
   - DATA chunks for regular content
   - OSTREE chunks for ostree object references

4. **Compression**: The entire delta file is gzip-compressed

## Requirements

- Python 3.6+
- Standard library only (no external dependencies)

## Example Workflow

```bash
# Analyze potential savings
./oci-delta-analyze.py fedora-40.tar fedora-41.tar

# Create delta file
./oci-delta-create.py fedora-40.tar fedora-41.tar fedora-40-41-delta.tar.gz

# Inspect the delta file
./oci-delta-inspect.py fedora-40-41-delta.tar.gz

# Verify delta checksums
./oci-delta-inspect.py fedora-40-41-delta.tar.gz --verify --ostree-repo /sysroot/ostree/repo

# Apply delta to reconstruct OCI archive
./oci-delta-apply.py fedora-40-41-delta.tar.gz fedora-41-reconstructed.tar --ostree-repo /sysroot/ostree/repo
```
