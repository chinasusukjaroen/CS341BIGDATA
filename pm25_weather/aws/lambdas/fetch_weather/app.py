"""AWS Lambda: enrich Air4Thai PM2.5 station-days with Open-Meteo ERA5 daily weather."""
import csv
import io
import json
import os
import ssl
import certifi
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import boto3
from botocore.exceptions import ClientError

S3 = boto3.client("s3")
AIR4THAI_URL = "https://air4thai.pcd.go.th/services/getNewAQI_JSON.php"
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
METADATA_KEY = "metadata/station_coordinates.csv"
FIELDS = [
    "temperature_2m_mean", "weather_code", "wind_speed_10m_max",
    "wind_direction_10m_dominant", "precipitation_sum",
    "precipitation_hours", "relative_humidity_2m_mean",
]
METADATA_FIELDS = ["station_id", "latitude", "longitude", "name_th", "station_type", "status", "last_checked", "last_updated"]
BKK = timezone(timedelta(hours=7))


def read_csv(bucket, key):
    obj = S3.get_object(Bucket=bucket, Key=key)
    return list(csv.DictReader(io.StringIO(obj["Body"].read().decode("utf-8-sig"))))


def write_csv(bucket, key, fields, rows):
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    S3.put_object(Bucket=bucket, Key=key, Body=stream.getvalue().encode("utf-8-sig"), ContentType="text/csv; charset=utf-8")


def write_json(bucket, key, data):
    S3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"), ContentType="application/json")


def http_json(url, attempts=3, timeout=20, verify_ssl=True):
    error = None

    if verify_ssl:
        context = ssl.create_default_context(cafile=certifi.where())
    else:
        context = ssl._create_unverified_context()

    for attempt in range(attempts):
        started = time.monotonic()

        try:
            request = Request(
                url,
                headers={
                    "User-Agent": "CS341-PM25-Weather-Pipeline/1.0",
                    "Accept": "application/json"
                }
            )

            print(
                f"[HTTP] START host={request.host} attempt={attempt + 1}",
                flush=True
            )

            with urlopen(request, timeout=timeout, context=context) as response:
                result = json.load(response)

            print(
                f"[HTTP] SUCCESS host={request.host} "
                f"elapsed={time.monotonic() - started:.2f}s",
                flush=True
            )

            return result

        except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
            error = exc

            print(
                f"[HTTP] FAILED host={request.host} "
                f"attempt={attempt + 1} "
                f"elapsed={time.monotonic() - started:.2f}s "
                f"error={exc}",
                flush=True
            )

            if attempt < attempts - 1:
                time.sleep(2 ** attempt)

    raise RuntimeError(f"HTTP request failed: {error}")


def valid_coords(row):
    try:
        lat = float(row.get("latitude", ""))
        lon = float(row.get("longitude", ""))
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return lat, lon
    except (TypeError, ValueError):
        pass
    return None


def load_cache(bucket):
    try:
        rows = read_csv(bucket, METADATA_KEY)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404", "NotFound"):
            return {}
        raise
    cache = {}
    for row in rows:
        station_id = (row.get("station_id") or "").strip().upper()
        if station_id:
            row["station_id"] = station_id
            cache[station_id] = row
    return cache


def is_fresh(row, today, ttl_days):
    try:
        checked = date.fromisoformat(row.get("last_checked", ""))
        return timedelta(0) <= (today - checked) < timedelta(days=ttl_days)
    except (TypeError, ValueError):
        return False


def refresh_cache(bucket, cache, required_ids, today, ttl_days):
    needs_refresh = sorted(sid for sid in required_ids if sid not in cache or not is_fresh(cache[sid], today, ttl_days))
    summary = {"refresh_required": needs_refresh, "api_called": False, "api_error": None, "updated_ids": [], "coordinate_changes": [], "unresolved_ids": []}
    if not needs_refresh:
        return cache, summary
    summary["api_called"] = True
    try:
        payload = http_json(AIR4THAI_URL, verify_ssl=False)
        stations = payload.get("stations") if isinstance(payload, dict) else None
        if not isinstance(stations, list) or not stations:
            raise ValueError("Air4Thai response has no stations[]")
        lookup = {}
        for station in stations:
            if isinstance(station, dict):
                sid = str(station.get("stationID") or "").strip().upper()
                if sid in required_ids:
                    lookup[sid] = station
        for sid in needs_refresh:
            old = cache.get(sid, {})
            station = lookup.get(sid)
            new = dict(old)
            new["station_id"] = sid
            new["last_checked"] = today.isoformat()
            if station:
                candidate = {"latitude": station.get("lat"), "longitude": station.get("long")}
                coords = valid_coords(candidate)
                if coords:
                    prior = valid_coords(old)
                    if prior and prior != coords:
                        summary["coordinate_changes"].append({"station_id": sid, "previous": list(prior), "current": list(coords)})
                    new.update({
                        "latitude": str(coords[0]), "longitude": str(coords[1]),
                        "name_th": station.get("nameTH") or old.get("name_th", ""),
                        "station_type": station.get("stationType") or old.get("station_type", ""),
                        "status": "api_valid", "last_updated": today.isoformat(),
                    })
                elif not valid_coords(old):
                    new["status"] = "missing"
            elif not valid_coords(old):
                new["status"] = "missing"
            if not valid_coords(new):
                summary["unresolved_ids"].append(sid)
            cache[sid] = new
            summary["updated_ids"].append(sid)
        # Merge/upsert: preserve all existing station IDs, including those not in this batch.
        write_csv(bucket, METADATA_KEY, METADATA_FIELDS, [cache[sid] for sid in sorted(cache)])
    except Exception as exc:
        summary["api_error"] = str(exc)
        # Existing coordinates remain available even when the metadata API fails.
    return cache, summary


def fetch_weather(lat, lon, start_date, end_date):
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": start_date, "end_date": end_date,
        "daily": ",".join(FIELDS), "timezone": "Asia/Bangkok", "models": "era5",
    }
    url = OPEN_METEO_URL + "?" + urlencode(params)
    response = http_json(url)
    daily = response.get("daily") if isinstance(response, dict) else None
    if not isinstance(daily, dict) or not isinstance(daily.get("time"), list):
        raise ValueError(f"Open-Meteo response missing daily.time: {str(response)[:300]}")
    return daily, url


def handler(event, context):
    bucket = event.get("bucket") or os.environ.get("DATA_BUCKET")
    batch_id = str(event.get("batch_id") or "")
    run_id = str(event.get("run_id") or "")
    validation = event.get("validation") or {}

    if not bucket or len(batch_id) != 4 or not batch_id.isdigit():
        raise ValueError(
            "bucket and four-digit batch_id are required"
        )

    if not run_id:
        raise ValueError("run_id is required")

    if (
        validation.get("status") != "PASSED"
        or not validation.get("raw_key")
    ):
        raise ValueError(
            "validation.status=PASSED and "
            "validation.raw_key are required"
        )

    pm25_key = validation["raw_key"]

    expected_pm25_key = (
        f"raw/pm25/year={batch_id}/"
        f"run_id={run_id}/pm25_{batch_id}.csv"
    )

    if pm25_key != expected_pm25_key:
        raise ValueError(
            "PM2.5 raw_key does not match run_id"
        )

    # NEW: run_id folder for weather CSV.
    weather_key = (
        f"raw/weather/year={batch_id}/"
        f"run_id={run_id}/weather_{batch_id}.csv"
    )

    report_key = (
        f"report/validation/year={batch_id}/weather_fetch.json"
    )

    rows = read_csv(bucket, pm25_key)

    if not rows:
        raise ValueError("PM2.5 CSV is empty")

    station_dates = defaultdict(set)

    for row in rows:
        sid = (row.get("station_id") or "").strip().upper()
        date_text = (row.get("date") or "").strip()

        if not sid or not date_text:
            raise ValueError(
                "PM2.5 CSV contains missing station_id/date"
            )

        day = date.fromisoformat(date_text)

        if day.year != int(batch_id):
            raise ValueError(
                f"Date {day} outside batch year {batch_id}"
            )

        station_dates[sid].add(day)

    today = datetime.now(BKK).date()

    ttl_days = int(
        os.environ.get("METADATA_TTL_DAYS", "30")
    )

    cache, metadata = refresh_cache(
        bucket,
        load_cache(bucket),
        set(station_dates),
        today,
        ttl_days
    )

    archive_cutoff = today - timedelta(
        days=int(os.environ.get("ARCHIVE_LAG_DAYS", "5"))
    )

    fetched = []
    skipped = []
    failed = {}
    weather_rows = []
    requests = []

    for sid in sorted(station_dates):
        coords = valid_coords(cache.get(sid, {}))

        if coords is None:
            skipped.append({
                "station_id": sid,
                "reason": "MISSING_COORDINATES"
            })
            continue

        days = sorted(
            day
            for day in station_dates[sid]
            if day <= archive_cutoff
        )

        if not days:
            skipped.append({
                "station_id": sid,
                "reason": "ARCHIVE_DATE_NOT_READY"
            })
            continue

        start_date = days[0].isoformat()
        end_date = days[-1].isoformat()

        try:
            daily, url = fetch_weather(
                coords[0],
                coords[1],
                start_date,
                end_date
            )

            requests.append({
                "station_id": sid,
                "latitude": coords[0],
                "longitude": coords[1],
                "start_date": start_date,
                "end_date": end_date,
                "url": url
            })

            date_index = {
                day: i
                for i, day in enumerate(daily["time"])
            }

            station_rows = []

            for day in days:
                idx = date_index.get(day.isoformat())

                if idx is None:
                    continue

                item = {
                    "station_id": sid,
                    "date": day.isoformat()
                }

                for field in FIELDS:
                    values = daily.get(field) or []

                    item[field] = (
                        values[idx]
                        if idx < len(values)
                        else None
                    )

                station_rows.append(item)

            if len(station_rows) != len(days):
                raise ValueError(
                    "Missing weather dates: "
                    f"got {len(station_rows)} of {len(days)}"
                )

            weather_rows.extend(station_rows)
            fetched.append(sid)

        except Exception as exc:
            failed[sid] = str(exc)

    weather_rows.sort(
        key=lambda row: (
            row["station_id"],
            row["date"]
        )
    )

    # Preserve original validation policy.
    status = (
        "PASSED"
        if weather_rows and not failed
        else "FAILED"
    )

    if weather_rows:
        write_csv(
            bucket,
            weather_key,
            ["station_id", "date"] + FIELDS,
            weather_rows
        )

    report = {
        "status": status,
        "batch_id": batch_id,
        "run_id": run_id,
        "pm25_key": pm25_key,
        "metadata_key": METADATA_KEY,
        "metadata_refresh": metadata,
        "weather_key": (
            weather_key if weather_rows else None
        ),
        "stations_in_pm25": len(station_dates),
        "stations_fetched": fetched,
        "stations_skipped": skipped,
        "stations_failed": failed,
        "weather_rows": len(weather_rows),
        "archive_cutoff": archive_cutoff.isoformat(),
        "station_requests": requests,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

    write_json(bucket, report_key, report)

    return {
        **event,
        "fetch": {
            "status": status,
            "weather_key": (
                weather_key if weather_rows else None
            ),
            "report_key": report_key,
            "stations_fetched": len(fetched),
            "stations_skipped": skipped,
            "stations_failed": list(failed),
            "weather_rows": len(weather_rows),
            "metadata_api_called": metadata["api_called"],
            "metadata_api_error": metadata["api_error"]
        }
    }
