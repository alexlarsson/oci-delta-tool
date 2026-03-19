#!/usr/bin/env python3

import argparse
import subprocess
import sys
import tarfile
from pathlib import Path
from typing import Set, Callable, List, Optional
import shutil


class Colors:
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    NC = "\033[0m"


class TestStats:
    def __init__(self):
        self.total = 0
        self.passed = 0
        self.failed = 0


class ImageSpec:
    def __init__(self, dockerfile: str, tag: str, output: str):
        self.dockerfile = dockerfile
        self.tag = tag
        self.output = output


IMAGES = {
    "base": ImageSpec("Dockerfile.base", "test-base", "base.tar"),
    "updated": ImageSpec("Dockerfile.updated", "test-updated", "updated.tar"),
    "ostree-base": ImageSpec(
        "Dockerfile.with-ostree", "test-ostree-base", "ostree-base.tar"
    ),
    "ostree-updated": ImageSpec(
        "Dockerfile.with-ostree-updated", "test-ostree-updated", "ostree-updated.tar"
    ),
    "empty": ImageSpec("Dockerfile.empty", "test-empty", "empty.tar"),
}


class TestContext:
    def __init__(self, project_dir: Path, tests_dir: Path, build_dir: Path):
        self.project_dir = project_dir
        self.tests_dir = tests_dir
        self.build_dir = build_dir
        self.stats = TestStats()

    def image_path(self, image_name: str) -> Path:
        return self.build_dir / IMAGES[image_name].output

    def create_delta(
        self, old_image: Path, new_image: Path, output: Path, capture: bool = False
    ) -> subprocess.CompletedProcess:
        """Run oci-delta-create.py"""
        cmd = [
            "python3",
            str(self.project_dir / "oci-delta-create.py"),
            str(old_image),
            str(new_image),
            str(output),
        ]
        return run_command(cmd, capture=capture)

    def inspect_delta(
        self,
        delta: Path,
        verify: bool = False,
        verbose: bool = False,
        ostree_repo: Optional[str] = None,
        capture: bool = False,
    ) -> subprocess.CompletedProcess:
        """Run oci-delta-inspect.py"""
        cmd = ["python3", str(self.project_dir / "oci-delta-inspect.py")]
        if verbose:
            cmd.append("-v")
        if verify:
            cmd.append("--verify")
        if ostree_repo:
            cmd.extend(["--ostree-repo", ostree_repo])
        cmd.append(str(delta))
        return run_command(cmd, capture=capture)

    def apply_delta(
        self,
        delta: Path,
        output: Path,
        ostree_repo: Optional[str] = None,
        capture: bool = False,
    ) -> subprocess.CompletedProcess:
        """Run oci-delta-apply.py"""
        cmd = [
            "python3",
            str(self.project_dir / "oci-delta-apply.py"),
            str(delta),
            str(output),
        ]
        if ostree_repo:
            cmd.extend(["--ostree-repo", ostree_repo])
        return run_command(cmd, capture=capture)

    def analyze_delta(
        self, old_image: Path, new_image: Path, capture: bool = False
    ) -> subprocess.CompletedProcess:
        """Run oci-delta-analyze.py"""
        cmd = [
            "python3",
            str(self.project_dir / "oci-delta-analyze.py"),
            str(old_image),
            str(new_image),
        ]
        return run_command(cmd, capture=capture)


def requires_images(*image_names: str):
    """Decorator to specify which images a test needs."""

    def decorator(func: Callable) -> Callable:
        func.required_images = set(image_names)
        return func

    return decorator


def log_test(msg: str):
    print(f"{Colors.YELLOW}[TEST]{Colors.NC} {msg}")


def log_pass(msg: str):
    print(f"{Colors.GREEN}[PASS]{Colors.NC} {msg}")


def log_fail(msg: str):
    print(f"{Colors.RED}[FAIL]{Colors.NC} {msg}")


def log_info(msg: str):
    print(f"[INFO] {msg}")


def build_image(spec: ImageSpec, tests_dir: Path, build_dir: Path) -> bool:
    """Build and save a single image."""
    log_info(f"Building image: {spec.tag}")

    dockerfile = tests_dir / spec.dockerfile
    output = build_dir / spec.output

    build_cmd = [
        "podman",
        "build",
        "-t",
        spec.tag,
        "-f",
        str(dockerfile),
        str(tests_dir),
    ]
    result = subprocess.run(build_cmd, capture_output=True, text=True)

    if result.returncode != 0:
        log_fail(f"Failed to build image {spec.tag}")
        print(result.stderr, file=sys.stderr)
        return False

    log_info(f"Saving image to: {output}")
    save_cmd = [
        "podman",
        "save",
        "--format",
        "oci-archive",
        spec.tag,
        "-o",
        str(output),
    ]
    result = subprocess.run(save_cmd, capture_output=True, text=True)

    if result.returncode != 0:
        log_fail(f"Failed to save image {spec.tag}")
        print(result.stderr, file=sys.stderr)
        return False

    return True


def build_required_images(required: Set[str], tests_dir: Path, build_dir: Path) -> bool:
    """Build all required images."""
    if not required:
        return True

    log_info(f"Building {len(required)} required images...")

    for image_name in sorted(required):
        if image_name not in IMAGES:
            log_fail(f"Unknown image: {image_name}")
            return False

        spec = IMAGES[image_name]
        if not build_image(spec, tests_dir, build_dir):
            return False

    print()
    return True


def verify_tar_structure(
    tar_file: Path, test_name: str, allow_missing_blobs: bool = False
) -> bool:
    """
    Verify OCI tar structure with full validation.

    Args:
        tar_file: Path to tar file to validate
        test_name: Name of test for error messages
        allow_missing_blobs: If True, layer blobs are optional (for delta files)

    Returns:
        True if valid, False otherwise
    """
    import json
    import hashlib
    import gzip
    import io

    try:
        with tarfile.open(tar_file, "r") as tar:
            members = {m.name: m for m in tar.getmembers()}
            member_names = set(members.keys())

            # Check for required files
            if "index.json" not in member_names:
                log_fail(f"{test_name}: Missing index.json")
                return False

            if "oci-layout" not in member_names:
                log_fail(f"{test_name}: Missing oci-layout")
                return False

            # Read index.json
            index_member = members["index.json"]
            index_data = json.load(tar.extractfile(index_member))

            if "manifests" not in index_data:
                log_fail(f"{test_name}: index.json missing 'manifests' field")
                return False

            # Validate each manifest
            for manifest_desc in index_data.get("manifests", []):
                manifest_digest = manifest_desc.get("digest", "")
                if not manifest_digest.startswith("sha256:"):
                    log_fail(
                        f"{test_name}: Invalid manifest digest format: {manifest_digest}"
                    )
                    return False

                manifest_hash = manifest_digest.split(":", 1)[1]
                manifest_path = f"blobs/sha256/{manifest_hash}"

                if manifest_path not in member_names:
                    if allow_missing_blobs:
                        continue
                    log_fail(f"{test_name}: Manifest blob not found: {manifest_path}")
                    return False

                # Verify manifest digest
                manifest_member = members[manifest_path]
                manifest_content = tar.extractfile(manifest_member).read()
                computed_digest = hashlib.sha256(manifest_content).hexdigest()

                if computed_digest != manifest_hash:
                    log_fail(f"{test_name}: Manifest digest mismatch")
                    log_fail(f"  Expected: {manifest_hash}")
                    log_fail(f"  Got:      {computed_digest}")
                    return False

                # Parse manifest
                manifest_data = json.loads(manifest_content)

                # Validate config blob
                config_desc = manifest_data.get("config", {})
                config_digest = config_desc.get("digest", "")

                if not config_digest.startswith("sha256:"):
                    log_fail(
                        f"{test_name}: Invalid config digest format: {config_digest}"
                    )
                    return False

                config_hash = config_digest.split(":", 1)[1]
                config_path = f"blobs/sha256/{config_hash}"

                config_data = None
                if config_path in member_names:
                    # Verify config digest
                    config_member = members[config_path]
                    config_content = tar.extractfile(config_member).read()
                    computed_digest = hashlib.sha256(config_content).hexdigest()

                    if computed_digest != config_hash:
                        log_fail(f"{test_name}: Config digest mismatch")
                        log_fail(f"  Expected: {config_hash}")
                        log_fail(f"  Got:      {computed_digest}")
                        return False

                    config_data = json.loads(config_content)
                elif not allow_missing_blobs:
                    log_fail(f"{test_name}: Config blob not found: {config_path}")
                    return False

                # Get diff_ids from config
                diff_ids = []
                if config_data:
                    diff_ids = config_data.get("rootfs", {}).get("diff_ids", [])

                # Validate layer blobs
                layers = manifest_data.get("layers", [])
                for idx, layer in enumerate(layers):
                    layer_digest = layer.get("digest", "")

                    if not layer_digest.startswith("sha256:"):
                        log_fail(
                            f"{test_name}: Invalid layer digest format: {layer_digest}"
                        )
                        return False

                    layer_hash = layer_digest.split(":", 1)[1]
                    layer_path = f"blobs/sha256/{layer_hash}"

                    if layer_path not in member_names:
                        if allow_missing_blobs:
                            continue
                        log_fail(f"{test_name}: Layer blob not found: {layer_path}")
                        return False

                    # Verify layer compressed digest
                    layer_member = members[layer_path]
                    layer_content = tar.extractfile(layer_member).read()
                    computed_digest = hashlib.sha256(layer_content).hexdigest()

                    if computed_digest != layer_hash:
                        log_fail(f"{test_name}: Layer {idx} compressed digest mismatch")
                        log_fail(f"  Expected: {layer_hash}")
                        log_fail(f"  Got:      {computed_digest}")
                        return False

                    # Verify layer size matches manifest
                    layer_size = layer.get("size", 0)
                    if layer_size > 0 and len(layer_content) != layer_size:
                        log_fail(f"{test_name}: Layer {idx} size mismatch")
                        log_fail(f"  Expected: {layer_size}")
                        log_fail(f"  Got:      {len(layer_content)}")
                        return False

                    # Verify uncompressed digest if we have diff_ids
                    if idx < len(diff_ids):
                        expected_diff_id = diff_ids[idx]
                        if expected_diff_id.startswith("sha256:"):
                            expected_diff_id = expected_diff_id.split(":", 1)[1]

                        # Decompress and compute uncompressed digest
                        try:
                            # Check if it's gzipped by magic bytes
                            if (
                                len(layer_content) >= 2
                                and layer_content[0:2] == b"\x1f\x8b"
                            ):
                                uncompressed = gzip.decompress(layer_content)
                            else:
                                # Not gzipped (might be chunked format in delta)
                                # For now, skip uncompressed validation for non-gzip
                                continue

                            computed_diff_id = hashlib.sha256(uncompressed).hexdigest()

                            if computed_diff_id != expected_diff_id:
                                log_fail(
                                    f"{test_name}: Layer {idx} uncompressed digest mismatch"
                                )
                                log_fail(f"  Expected diff_id: {expected_diff_id}")
                                log_fail(f"  Computed:         {computed_diff_id}")
                                return False
                        except Exception as e:
                            log_fail(
                                f"{test_name}: Failed to decompress layer {idx}: {e}"
                            )
                            return False

        return True
    except Exception as e:
        log_fail(f"{test_name}: Failed to verify tar structure: {e}")
        import traceback

        traceback.print_exc()
        return False


def run_command(cmd: List[str], capture: bool = False) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    if capture:
        return subprocess.run(cmd, capture_output=True, text=True)
    else:
        return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


@requires_images("base", "updated")
def test_basic_delta_creation(ctx: TestContext) -> bool:
    """Test basic delta creation."""
    log_test("Basic delta creation")

    old_image = ctx.image_path("base")
    new_image = ctx.image_path("updated")
    delta_file = ctx.build_dir / "delta.tar.gz"

    result = ctx.create_delta(old_image, new_image, delta_file)

    if result.returncode != 0:
        log_fail("Delta creation failed")
        return False

    if not delta_file.exists():
        log_fail("Delta file was not created")
        return False

    if not verify_tar_structure(delta_file, "Basic delta", allow_missing_blobs=True):
        return False

    log_pass("Basic delta creation")
    return True


@requires_images("base", "updated")
def test_delta_inspection(ctx: TestContext) -> bool:
    """Test delta inspection."""
    log_test("Delta inspection")

    old_image = ctx.image_path("base")
    new_image = ctx.image_path("updated")
    delta_file = ctx.build_dir / "delta.tar.gz"

    ctx.create_delta(old_image, new_image, delta_file)

    output_file = ctx.build_dir / "inspect_output.txt"
    result = ctx.inspect_delta(delta_file, capture=True)

    if result.returncode != 0:
        log_fail("Delta inspection failed")
        return False

    output_file.write_text(result.stdout)

    if "Delta file contents" not in result.stdout:
        log_fail("Inspection output missing expected content")
        return False

    log_pass("Delta inspection")
    return True


@requires_images("base", "updated")
def test_delta_apply(ctx: TestContext) -> bool:
    """Test delta application."""
    log_test("Delta application")

    old_image = ctx.image_path("base")
    new_image = ctx.image_path("updated")
    delta_file = ctx.build_dir / "delta.tar.gz"
    reconstructed = ctx.build_dir / "reconstructed.tar"

    ctx.create_delta(old_image, new_image, delta_file)

    result = ctx.apply_delta(delta_file, reconstructed)

    if result.returncode != 0:
        log_fail("Delta application failed")
        return False

    if not reconstructed.exists():
        log_fail("Reconstructed image was not created")
        return False

    # The reconstructed image has missing blobs for the shared layers
    if not verify_tar_structure(
        reconstructed, "Reconstructed image", allow_missing_blobs=True
    ):
        return False

    log_pass("Delta application")
    return True


@requires_images("base", "updated")
def test_delta_analyze(ctx: TestContext) -> bool:
    """Test delta analysis."""
    log_test("Delta analysis")

    old_image = ctx.image_path("base")
    new_image = ctx.image_path("updated")

    result = ctx.analyze_delta(old_image, new_image, capture=True)

    if result.returncode != 0:
        log_fail("Delta analysis failed")
        return False

    output_file = ctx.build_dir / "analyze_output.txt"
    output_file.write_text(result.stdout)

    if "LAYER ANALYSIS" not in result.stdout:
        log_fail("Analysis output missing expected content")
        return False

    if "OSTREE OBJECT ANALYSIS" not in result.stdout:
        log_fail("Analysis output missing ostree analysis")
        return False

    log_pass("Delta analysis")
    return True


@requires_images("ostree-base", "ostree-updated")
def test_ostree_objects(ctx: TestContext) -> bool:
    """Test delta with ostree objects."""
    log_test("Delta with ostree objects")

    old_image = ctx.image_path("ostree-base")
    new_image = ctx.image_path("ostree-updated")
    delta_file = ctx.build_dir / "ostree-delta.tar.gz"

    result = ctx.create_delta(old_image, new_image, delta_file)

    if result.returncode != 0:
        log_fail("Delta creation with ostree failed")
        return False

    result = ctx.inspect_delta(delta_file, capture=True)

    output_file = ctx.build_dir / "ostree_inspect.txt"
    output_file.write_text(result.stdout)

    if "OSTREE chunks:" in result.stdout:
        log_info("Found OSTREE chunks in delta (good!)")

    log_pass("Delta with ostree objects")
    return True


@requires_images("empty", "base")
def test_empty_image(ctx: TestContext) -> bool:
    """Test delta with empty/minimal image."""
    log_test("Delta with empty/minimal image")

    old_image = ctx.image_path("empty")
    new_image = ctx.image_path("base")
    delta_file = ctx.build_dir / "empty-delta.tar.gz"

    result = ctx.create_delta(old_image, new_image, delta_file)

    if result.returncode != 0:
        log_fail("Delta creation with empty image failed")
        return False

    log_pass("Delta with empty/minimal image")
    return True


@requires_images("base", "updated")
def test_verify_checksums(ctx: TestContext) -> bool:
    """Test checksum verification."""
    log_test("Checksum verification")

    old_image = ctx.image_path("base")
    new_image = ctx.image_path("updated")
    delta_file = ctx.build_dir / "delta-verify.tar.gz"

    ctx.create_delta(old_image, new_image, delta_file)

    result = ctx.inspect_delta(delta_file, verify=True, capture=True)

    if result.returncode != 0:
        log_fail("Checksum verification failed")
        return False

    output_file = ctx.build_dir / "verify_output.txt"
    output_file.write_text(result.stdout)

    if "Checksum mismatch" in result.stdout:
        log_fail("Found checksum mismatches")
        print(result.stdout)
        return False

    log_pass("Checksum verification")
    return True


@requires_images("base", "updated")
def test_validation_detects_corruption(ctx: TestContext) -> bool:
    """Test that validation detects corrupted archives."""
    log_test("Validation detects corruption")

    import json
    import gzip

    old_image = ctx.image_path("base")
    new_image = ctx.image_path("updated")
    delta_file = ctx.build_dir / "corruption-delta.tar.gz"
    corrupted_file = ctx.build_dir / "corrupted-delta.tar.gz"

    # Create a normal delta first
    ctx.create_delta(old_image, new_image, delta_file)

    # Extract, corrupt index.json, and repackage
    extract_dir = ctx.build_dir / "corrupt-extract"
    extract_dir.mkdir(exist_ok=True)

    with gzip.open(delta_file, "rb") as gz_in:
        with tarfile.open(fileobj=gz_in, mode="r") as tar:
            tar.extractall(extract_dir)

    # Corrupt the index.json by changing manifest digest
    index_path = extract_dir / "index.json"
    index_data = json.loads(index_path.read_text())

    if index_data.get("manifests"):
        # Change the digest to something invalid
        original_digest = index_data["manifests"][0]["digest"]
        index_data["manifests"][0][
            "digest"
        ] = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        index_path.write_text(json.dumps(index_data))

    # Repackage
    with gzip.open(corrupted_file, "wb") as gz_out:
        with tarfile.open(fileobj=gz_out, mode="w") as tar:
            tar.add(extract_dir, arcname=".")

    # Validation should fail on corrupted file (suppress expected error output)
    import sys
    import io

    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.stdout = io.StringIO()
    sys.stderr = io.StringIO()

    validation_passed = verify_tar_structure(
        corrupted_file, "Corrupted delta", allow_missing_blobs=True
    )

    sys.stdout = old_stdout
    sys.stderr = old_stderr

    if validation_passed:
        log_fail("Validation did not detect corrupted delta file")
        return False

    log_info("Successfully detected corrupted delta file")

    # But original should still pass
    if not verify_tar_structure(delta_file, "Original delta", allow_missing_blobs=True):
        log_fail("Original delta failed validation")
        return False

    log_pass("Validation detects corruption")
    return True


@requires_images("base")
def test_identical_images(ctx: TestContext) -> bool:
    """Test delta between identical images."""
    log_test("Delta between identical images")

    image1 = ctx.image_path("base")
    image2 = ctx.image_path("base")
    delta_file = ctx.build_dir / "identical-delta.tar.gz"

    result = ctx.create_delta(image1, image2, delta_file, capture=True)

    if result.returncode != 0:
        log_fail("Delta creation for identical images failed")
        return False

    output_file = ctx.build_dir / "identical_output.txt"
    output_file.write_text(result.stdout)

    if "New layers (included): 0" in result.stdout:
        log_info("Correctly identified no new layers for identical images")

    log_pass("Delta between identical images")
    return True


ALL_TESTS = [
    test_basic_delta_creation,
    test_delta_inspection,
    test_delta_apply,
    test_delta_analyze,
    test_ostree_objects,
    test_empty_image,
    test_verify_checksums,
    test_validation_detects_corruption,
    test_identical_images,
]


def discover_tests(test_names: Optional[List[str]] = None) -> List[Callable]:
    """Discover tests to run based on test names."""
    if not test_names:
        return ALL_TESTS

    available = {t.__name__: t for t in ALL_TESTS}
    selected = []

    for name in test_names:
        if name not in available:
            log_fail(f"Unknown test: {name}")
            print(f"Available tests: {', '.join(sorted(available.keys()))}")
            sys.exit(1)
        selected.append(available[name])

    return selected


def collect_required_images(tests: List[Callable]) -> Set[str]:
    """Collect all images required by the given tests."""
    required = set()
    for test in tests:
        if hasattr(test, "required_images"):
            required.update(test.required_images)
    return required


def setup(build_dir: Path):
    """Setup test environment."""
    log_info("Setting up test environment")
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True)


def cleanup(build_dir: Path):
    """Cleanup test environment."""
    if build_dir.exists():
        log_info("Cleaning up build directory")
        shutil.rmtree(build_dir)


def print_results(stats: TestStats):
    """Print test results."""
    print()
    print("=" * 50)
    print("Test Results")
    print("=" * 50)
    print(f"Total:  {stats.total}")
    print(f"Passed: {Colors.GREEN}{stats.passed}{Colors.NC}")
    print(f"Failed: {Colors.RED}{stats.failed}{Colors.NC}")
    print("=" * 50)

    if stats.failed == 0:
        print(f"{Colors.GREEN}All tests passed!{Colors.NC}")
    else:
        print(f"{Colors.RED}Some tests failed!{Colors.NC}")


def main():
    parser = argparse.ArgumentParser(description="Run OCI delta tool tests")
    parser.add_argument("tests", nargs="*", help="Specific tests to run (default: all)")
    parser.add_argument(
        "--no-cleanup", action="store_true", help="Keep build directory after tests"
    )
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    project_dir = script_dir.parent
    tests_dir = script_dir
    build_dir = tests_dir / "build"

    ctx = TestContext(project_dir, tests_dir, build_dir)

    tests = discover_tests(args.tests if args.tests else None)
    required_images = collect_required_images(tests)

    setup(build_dir)

    try:
        if not build_required_images(required_images, tests_dir, build_dir):
            log_fail("Failed to build required images")
            return 1

        log_info("Starting test suite")
        print()

        for test in tests:
            ctx.stats.total += 1
            try:
                if test(ctx):
                    ctx.stats.passed += 1
                else:
                    ctx.stats.failed += 1
            except Exception as e:
                log_fail(f"{test.__name__} raised exception: {e}")
                import traceback

                traceback.print_exc()
                ctx.stats.failed += 1

        print_results(ctx.stats)

        return 0 if ctx.stats.failed == 0 else 1

    finally:
        if not args.no_cleanup:
            cleanup(build_dir)


if __name__ == "__main__":
    sys.exit(main())
