#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
download_air4thai 2024 'incoming/pm25/year=2024/2024.xlsx'
