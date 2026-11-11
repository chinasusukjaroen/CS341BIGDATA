#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
download_air4thai 2026 'incoming/pm25/2026.xlsx'
