# OCI Delta Tool - Just commands

default:
    @just --list

test *TESTS:
    ./tests/test_runner.py {{TESTS}}

clean:
    rm -rf tests/build
    find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
    rm -f *.tar *.tar.gz *.oci-archive 2>/dev/null || true

lint:
    python3 -m pylint oci-delta-*.py oci_delta_common.py 2>/dev/null || echo "Install pylint to run linting: pip install pylint"

format:
    python3 -m black oci-delta-*.py oci_delta_common.py 2>/dev/null || echo "Install black to run formatting: pip install black"

check:
    python3 -m py_compile oci-delta-*.py oci_delta_common.py

help:
    @echo "OCI Delta Tool - Available commands:"
    @echo ""
    @echo "  just test              - Run all tests"
    @echo "  just test TEST1 TEST2  - Run specific tests"
    @echo "  just clean             - Remove build artifacts"
    @echo "  just lint              - Run linter (requires pylint)"
    @echo "  just format            - Format code (requires black)"
    @echo "  just check             - Check Python syntax"
