"""Validate fetched daily ERA5 weather against the cleaned PM2.5 station-days."""
import csv
import io
import json
import math
import os
from collections import Counter, defaultdict
from datetime import date, datetime, timezone

import boto3

S3 = boto3.client('s3')
WEATHER_FIELDS = [
    'temperature_2m_mean', 'weather_code', 'wind_speed_10m_max',
    'wind_direction_10m_dominant', 'precipitation_sum',
    'precipitation_hours', 'relative_humidity_2m_mean',
]
REQUIRED = ['station_id', 'date'] + WEATHER_FIELDS
# Wide but physically reasonable validation limits, not climatological filters.
BOUNDS = {
    'temperature_2m_mean': (-90, 65),
    'weather_code': (0, 99),
    'wind_speed_10m_max': (0, 150),
    'wind_direction_10m_dominant': (0, 360),
    'precipitation_sum': (0, 2000),
    'precipitation_hours': (0, 24),
    'relative_humidity_2m_mean': (0, 100),
}
MAX_ISSUES = 50


def load_csv(bucket, key):
    body = S3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(body))
    if not reader.fieldnames:
        raise ValueError(f'CSV has no header: {key}')
    return reader.fieldnames, list(reader)


def save_json(bucket, key, value):
    S3.put_object(Bucket=bucket, Key=key,
                  Body=json.dumps(value, ensure_ascii=False, indent=2).encode('utf-8'),
                  ContentType='application/json')


def handler(event, context):
    bucket = event.get('bucket') or os.environ.get('DATA_BUCKET')
    batch_id = str(event.get('batch_id') or '')
    run_id = str(event.get('run_id') or '')
    validation = event.get('validation') or {}
    fetch = event.get('fetch') or {}
    if not bucket or len(batch_id) != 4 or not batch_id.isdigit():
        raise ValueError('bucket and four-digit batch_id are required')
    if not run_id:
        raise ValueError('run_id is required')
    if validation.get('status') != 'PASSED' or not validation.get('raw_key'):
        raise ValueError('validation.status=PASSED and validation.raw_key are required')
    if fetch.get('status') != 'PASSED' or not fetch.get('weather_key'):
        raise ValueError('fetch.status=PASSED and fetch.weather_key are required')

    pm_key = validation['raw_key']
    weather_key = fetch['weather_key']
    expected_pm_key = (
        f'raw/pm25/year={batch_id}/run_id={run_id}/pm25_{batch_id}.csv'
    )
    expected_weather_key = (
        f'raw/weather/year={batch_id}/run_id={run_id}/weather_{batch_id}.csv'
    )
    if pm_key != expected_pm_key:
        raise ValueError('PM2.5 raw_key does not match run_id')
    if weather_key != expected_weather_key:
        raise ValueError('Weather key does not match run_id')
    report_key = f'report/validation/year={batch_id}/weather_validation.json'
    issues = []
    errors = Counter()
    warnings = Counter()

    def issue(kind, detail, fatal=True):
        (errors if fatal else warnings)[kind] += 1
        if len(issues) < MAX_ISSUES:
            issues.append({'severity': 'ERROR' if fatal else 'WARNING', 'type': kind, **detail})

    pm_fields, pm_rows = load_csv(bucket, pm_key)
    if not {'station_id', 'date'}.issubset(pm_fields):
        raise ValueError('PM2.5 CSV requires station_id,date')
    weather_fields, weather_rows = load_csv(bucket, weather_key)
    missing_columns = sorted(set(REQUIRED) - set(weather_fields))
    if missing_columns:
        issue('MISSING_COLUMNS', {'columns': missing_columns})

    pm_keys = set()
    pm_by_station = defaultdict(set)
    for idx, row in enumerate(pm_rows, 2):
        sid = (row.get('station_id') or '').strip().upper()
        dt = (row.get('date') or '').strip()
        try:
            d = date.fromisoformat(dt)
            if not sid or d.year != int(batch_id):
                raise ValueError('invalid station or batch year')
        except ValueError:
            issue('INVALID_PM_KEY', {'line': idx, 'station_id': sid, 'date': dt})
            continue
        key = (sid, dt)
        if key in pm_keys:
            issue('DUPLICATE_PM_KEY', {'line': idx, 'station_id': sid, 'date': dt})
        pm_keys.add(key)
        pm_by_station[sid].add(dt)

    seen = set()
    weather_by_station = Counter()
    missing_value_counts = Counter()
    for idx, row in enumerate(weather_rows, 2):
        sid = (row.get('station_id') or '').strip().upper()
        dt = (row.get('date') or '').strip()
        try:
            d = date.fromisoformat(dt)
            if not sid or d.year != int(batch_id):
                raise ValueError('invalid station or batch year')
        except ValueError:
            issue('INVALID_WEATHER_KEY', {'line': idx, 'station_id': sid, 'date': dt})
            continue
        key = (sid, dt)
        if key in seen:
            issue('DUPLICATE_WEATHER_KEY', {'line': idx, 'station_id': sid, 'date': dt})
        seen.add(key)
        weather_by_station[sid] += 1
        if key not in pm_keys:
            issue('WEATHER_KEY_NOT_IN_PM', {'line': idx, 'station_id': sid, 'date': dt})
        for field in WEATHER_FIELDS:
            value = row.get(field)
            if value is None or not str(value).strip() or str(value).strip().lower() in ('null', 'none', 'nan'):
                missing_value_counts[field] += 1
                continue
            try:
                number = float(value)
                lo, hi = BOUNDS[field]
                if not math.isfinite(number) or not lo <= number <= hi:
                    raise ValueError('outside bounds')
                if field == 'weather_code' and not number.is_integer():
                    raise ValueError('weather code not integer')
            except (TypeError, ValueError):
                issue('INVALID_WEATHER_VALUE', {'line': idx, 'field': field, 'value': str(value)[:80]})

    uncovered = sorted(pm_keys - seen)
    skipped_ids = {str(item.get('station_id', '')).upper() for item in fetch.get('stations_skipped', []) if isinstance(item, dict)}
    unexplained = [key for key in uncovered if key[0] not in skipped_ids]
    if unexplained:
        issue('UNEXPLAINED_MISSING_WEATHER', {'count': len(unexplained), 'examples': [list(k) for k in unexplained[:10]]})
    if uncovered:
        issue('PM_DAYS_WITHOUT_WEATHER', {'count': len(uncovered), 'station_ids': sorted({s for s, _ in uncovered})}, fatal=False)
    if not weather_rows:
        issue('EMPTY_WEATHER', {'weather_key': weather_key})
    if missing_value_counts:
        issue('NULL_WEATHER_VALUES', {'counts': dict(missing_value_counts)}, fatal=False)

    status = 'PASSED' if not errors else 'FAILED'
    report = {
        'status': status, 'batch_id': batch_id, 'run_id': run_id, 'pm25_key': pm_key,
        'weather_key': weather_key, 'pm25_rows': len(pm_rows),
        'pm25_unique_station_days': len(pm_keys), 'weather_rows': len(weather_rows),
        'weather_unique_station_days': len(seen),
        'weather_station_count': len(weather_by_station),
        'weather_rows_by_station': dict(sorted(weather_by_station.items())),
        'pm25_days_without_weather': len(uncovered),
        'pm25_days_without_weather_by_station': dict(sorted(Counter(s for s, _ in uncovered).items())),
        'skipped_station_ids': sorted(skipped_ids),
        'missing_weather_values': dict(missing_value_counts),
        'error_counts': dict(errors), 'warning_counts': dict(warnings),
        'issue_examples': issues, 'generated_at': datetime.now(timezone.utc).isoformat(),
    }
    save_json(bucket, report_key, report)
    return {**event, 'weather_validation': {
        'status': status, 'report_key': report_key,
        'pm25_rows': len(pm_rows), 'weather_rows': len(weather_rows),
        'missing_weather_days': len(uncovered), 'error_counts': dict(errors),
        'warning_counts': dict(warnings),
    }}


