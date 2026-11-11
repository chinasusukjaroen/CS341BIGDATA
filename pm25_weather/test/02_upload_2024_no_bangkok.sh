#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
upload_local '2024_No_Bangkok.xlsx' 'incoming/pm25/year=2024/2024_No_Bangkok.xlsx'
