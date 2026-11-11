
#!/usr/bin/env bash
set -euo pipefail

# หาตำแหน่งของ common.sh และ Root ของโปรเจกต์
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

# โหลด .env จาก Root ของโปรเจกต์
if [[ -f "$PROJECT_ROOT/.env" ]]; then
  set -a
  source "$PROJECT_ROOT/.env"
  set +a
fi

# ใช้ค่า Default หากไม่มีการกำหนดใน .env
AWS_REGION="${AWS_REGION:-us-east-1}"
S3_BUCKET="${S3_BUCKET:-pm25-weather}"

# ตรวจสอบว่ามี AWS CLI
require_tools() {
  command -v aws >/dev/null || {
    echo 'ERROR: aws CLI not found' >&2
    exit 1
  }
}

# Upload ไฟล์ Excel ที่อยู่ในโฟลเดอร์เดียวกับ Script
upload_local() {
  local filename="$1"
  local key="$2"

  require_tools

  local filepath="${SCRIPT_DIR}/${filename}"

  [[ -f "$filepath" ]] || {
    echo "ERROR: File not found: $filepath" >&2
    exit 1
  }

  echo "Uploading: $filepath"

  aws s3 cp \
    "$filepath" \
    "s3://${S3_BUCKET}/${key}" \
    --region "$AWS_REGION"

  printf 'source_key: %s\n' "$key"
}

# Download Excel จาก Air4Thai แล้ว Upload ไป S3
download_air4thai() {
  local year="$1"
  local key="$2"

  local url="https://air4thai.net/daily_download/PM2.5/${year}.xlsx"

  require_tools

  command -v curl >/dev/null || {
    echo 'ERROR: curl not found' >&2
    exit 1
  }

  command -v od >/dev/null || {
    echo 'ERROR: od not found' >&2
    exit 1
  }

  local tmp
  local http_status
  local signature

  tmp="$(mktemp)"

  # ลบไฟล์ชั่วคราวเมื่อ Script จบการทำงาน
  trap "rm -f -- '$tmp'" EXIT

  echo "Downloading: $url"

  http_status="$(
    curl -k -L -sS \
      --retry 2 \
      --connect-timeout 20 \
      --max-time 120 \
      -A 'Mozilla/5.0' \
      -o "$tmp" \
      -w '%{http_code}' \
      "$url"
  )"

  echo "HTTP Status: $http_status"

  if [[ "$http_status" != "200" ]]; then
    echo "ERROR: HTTP $http_status" >&2
    exit 1
  fi

  # ตรวจสอบว่าไฟล์ขึ้นต้นด้วย PK (ZIP/XLSX)
  signature="$(
    od -An -tx1 -N2 "$tmp" | tr -d ' \n'
  )"

  if [[ "$signature" != "504b" ]]; then
    echo "ERROR: Response is not a valid XLSX/ZIP (missing PK signature)" >&2
    exit 1
  fi

  echo "Downloaded size: $(wc -c < "$tmp" | tr -d ' ') bytes"

  # Upload ขึ้น S3
  aws s3 cp \
    "$tmp" \
    "s3://${S3_BUCKET}/${key}" \
    --region "$AWS_REGION" \
    --content-type 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

  printf 'source_key: %s\n' "$key"

  echo "SUCCESS: Download and upload completed"
}
