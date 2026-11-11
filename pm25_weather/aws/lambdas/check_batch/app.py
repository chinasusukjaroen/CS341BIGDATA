
import json
import os
import re
import uuid

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")


def invalid_input(event, reason):
    return {
        **event,
        "check": {
            "status": "INVALID_INPUT",
            "reason": reason
        }
    }


def handler(event, context):
    if not isinstance(event, dict):
        return {
            "check": {
                "status": "INVALID_INPUT",
                "reason": "Input must be a JSON object"
            }
        }

    bucket = event.get("bucket") or os.environ.get("DATA_BUCKET")
    key = event.get("source_key", "")

    # 1. Validate input
    if not isinstance(bucket, str) or not bucket.strip():
        return invalid_input(event, "bucket is required")

    if not isinstance(key, str) or not key:
        return invalid_input(event, "source_key is required")

    # Accept any CSV/XLSX filename in the year folder; keep ValidateBatch's format rules.
    match = re.fullmatch(r"incoming/pm25/year=(20\d{2})/([^/]+\.(?:csv|xlsx))", key, re.I)

    if not match:
        return invalid_input(
            event,
            "Invalid source_key format or year mismatch"
        )

    year = match.group(1)

    marker = (
        f"curated/pm25_weather/year={year}/_SUCCESS.json"
    )

    # 2. Check source object
    try:
        source = s3.head_object(
            Bucket=bucket,
            Key=key
        )

    except ClientError as exc:
        code = exc.response["Error"]["Code"]

        if code in ("404", "NoSuchKey", "NotFound"):
            return invalid_input(
                event,
                "Source file does not exist in S3"
            )

        # AWS errors go to PipelineFailed
        raise

    etag = source["ETag"].strip('"')

    # 3. Check success marker
    same = False

    try:
        obj = s3.get_object(
            Bucket=bucket,
            Key=marker
        )

        saved = json.loads(obj["Body"].read())

        same = (
            saved.get("source_key") == key
            and saved.get("source_etag") == etag
            and saved.get("status") == "SUCCESS"
        )

    except ClientError as exc:
        code = exc.response["Error"]["Code"]

        if code not in ("NoSuchKey", "404", "NotFound"):
            raise

    # 4. Generate run_id only for new processing
    run_id = str(uuid.uuid4()) if not same else None

    return {
        **event,
        "bucket": bucket,
        "batch_id": year,
        "source_etag": etag,
        "run_id": run_id,
        "check": {
            "status": (
                "ALREADY_PROCESSED"
                if same
                else "NOT_PROCESSED"
            ),
            "marker_key": marker
        }
    }


