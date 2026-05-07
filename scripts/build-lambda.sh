#!/usr/bin/env bash
# Builds a deployable Lambda asset for a given lambda directory.
# Usage: scripts/build-lambda.sh <lambda-name>
# Example: scripts/build-lambda.sh ingestion
#
# Output: lambdas/<name>/.build/  (zip-ready directory: handler.py + deps)
#
# Cross-platform builds:
#   By default we install with --platform manylinux2014_aarch64 so wheels are
#   compatible with our Lambda's ARM64 runtime even when this script is run
#   on macOS (different OS + arch). pip will fall back to platform-agnostic
#   wheels for pure-Python packages.
#
# Shared code:
#   If lambdas/shared/ exists, its modules are copied under `lambdas/shared/`
#   inside the build dir, and a top-level `lambdas/__init__.py` shim is added
#   so `from lambdas.shared import deviation` works at runtime — same import
#   path the lambda's tests use.

set -euo pipefail

LAMBDA_NAME="${1:-}"
if [[ -z "$LAMBDA_NAME" ]]; then
  echo "Usage: $0 <lambda-name>" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="$REPO_ROOT/lambdas/$LAMBDA_NAME"
BUILD_DIR="$SRC_DIR/.build"
SHARED_DIR="$REPO_ROOT/lambdas/shared"

# Lambda's runtime: Python 3.12 on ARM64 (Graviton). pip needs explicit hints
# when building from a host with a different OS/arch.
LAMBDA_PYTHON_VERSION="${LAMBDA_PYTHON_VERSION:-3.12}"
LAMBDA_PLATFORM="${LAMBDA_PLATFORM:-manylinux2014_aarch64}"

if [[ ! -d "$SRC_DIR" ]]; then
  echo "Lambda directory not found: $SRC_DIR" >&2
  exit 1
fi

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# Install third-party deps with platform-pinned wheels. --only-binary=:all:
# forces pip to fail loudly if no compatible wheel exists, instead of
# silently building from source for the wrong arch.
if [[ -f "$SRC_DIR/requirements.txt" ]]; then
  # Skip pip install entirely when requirements.txt has only comments —
  # otherwise pip emits a warning and adds nothing.
  if grep -Eq '^[^#[:space:]]' "$SRC_DIR/requirements.txt"; then
    pip3 install \
      --quiet \
      --target "$BUILD_DIR" \
      --requirement "$SRC_DIR/requirements.txt" \
      --platform "$LAMBDA_PLATFORM" \
      --python-version "$LAMBDA_PYTHON_VERSION" \
      --only-binary=:all: \
      --implementation cp
  fi
fi

# Copy handler and any sibling Python modules (skip tests, build artifacts).
find "$SRC_DIR" -maxdepth 1 -type f -name '*.py' -exec cp {} "$BUILD_DIR/" \;

# Stamp shared code into the asset under lambdas/shared/ so handlers can
# `from lambdas.shared import deviation, gtfs_static`. We also drop two
# __init__.py files to make `lambdas` and `lambdas.shared` resolvable inside
# Lambda's runtime.
if [[ -d "$SHARED_DIR" ]]; then
  mkdir -p "$BUILD_DIR/lambdas/shared"
  : > "$BUILD_DIR/lambdas/__init__.py"
  find "$SHARED_DIR" -maxdepth 1 -type f -name '*.py' \
    -exec cp {} "$BUILD_DIR/lambdas/shared/" \;
fi

echo "Built $LAMBDA_NAME → $BUILD_DIR"
