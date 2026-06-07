#!/usr/bin/env bash
set -euo pipefail

OUTPUT_DIR="${1:-dist/glue}"
PACKAGE_PATH="${OUTPUT_DIR}/inventory_streaming_libs.zip"

mkdir -p "${OUTPUT_DIR}"
rm -f "${PACKAGE_PATH}"

zip -r "${PACKAGE_PATH}" spark aws \
  -x "*/__pycache__/*" \
  -x "*.pyc"

echo "${PACKAGE_PATH}"
