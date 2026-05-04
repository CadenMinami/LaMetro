#!/usr/bin/env bash
# Builds a deployable Lambda asset for a given lambda directory.
# Usage: scripts/build-lambda.sh <lambda-name>
# Example: scripts/build-lambda.sh ingestion
#
# Output: lambdas/<name>/.build/  (zip-ready directory: handler.py + deps)
#
# We bundle deps + handler into one asset rather than using a Layer so the
# CDK Code.fromAsset() points at a single directory. Phase 2+ may switch to
# Layers if multiple Lambdas share heavy deps.

set -euo pipefail

LAMBDA_NAME="${1:-}"
if [[ -z "$LAMBDA_NAME" ]]; then
  echo "Usage: $0 <lambda-name>" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="$REPO_ROOT/lambdas/$LAMBDA_NAME"
BUILD_DIR="$SRC_DIR/.build"

if [[ ! -d "$SRC_DIR" ]]; then
  echo "Lambda directory not found: $SRC_DIR" >&2
  exit 1
fi

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

if [[ -f "$SRC_DIR/requirements.txt" ]]; then
  pip3 install \
    --quiet \
    --target "$BUILD_DIR" \
    --requirement "$SRC_DIR/requirements.txt"
fi

# Copy handler and any local modules (skip tests, build artifacts)
find "$SRC_DIR" -maxdepth 1 -type f -name '*.py' -exec cp {} "$BUILD_DIR/" \;

echo "Built $LAMBDA_NAME → $BUILD_DIR"
