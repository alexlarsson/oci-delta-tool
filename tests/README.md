# OCI Delta Tool Tests

This directory contains automated tests for the OCI delta tool suite.

## Test Structure

### Dockerfiles

- **Dockerfile.base** - Basic minimal image with a couple files
- **Dockerfile.updated** - Updated version with modified and new files
- **Dockerfile.with-ostree** - Image containing simulated ostree objects
- **Dockerfile.with-ostree-updated** - Updated ostree image with reusable and new objects
- **Dockerfile.empty** - Minimal image for edge case testing

## Running Tests

```bash
# Run all tests
just test

# Or run directly
./tests/test_runner.py

# Run specific tests only
just test test_basic_delta_creation test_delta_inspection

# Run with arguments
./tests/test_runner.py test_ostree_objects test_roundtrip

# Keep build directory for debugging
./tests/test_runner.py --no-cleanup

# Show help
./tests/test_runner.py --help
```

### Available Tests

- `test_basic_delta_creation` - Creates delta between two images
- `test_delta_inspection` - Validates inspection output
- `test_delta_apply` - Reconstructs image from delta
- `test_delta_analyze` - Validates analysis output
- `test_ostree_objects` - Tests ostree object chunking
- `test_empty_image` - Edge case with minimal images
- `test_verify_checksums` - Validates layer checksums
- `test_validation_detects_corruption` - Ensures validation catches corrupted files
- `test_identical_images` - Delta between identical images

## Requirements

- Python 3
- podman (for building and managing test images)
- Standard Unix tools (tar, file, grep)

## Test Output

Tests produce colored output:
- **Yellow** [TEST] - Test starting
- **Green** [PASS] - Test passed
- **Red** [FAIL] - Test failed
- White [INFO] - Informational messages

The test suite creates a temporary `tests/build/` directory that is cleaned up after each run.
