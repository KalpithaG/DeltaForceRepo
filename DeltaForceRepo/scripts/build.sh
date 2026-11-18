#!/usr/bin/env bash
# Sync the editable config into the package, then build the wheel into dist/.
set -euo pipefail
cd "$(dirname "$0")/.."
cp resources/*.csv src/deltaforce_dq/default_config/
python -m build --wheel
ls -1 dist/
