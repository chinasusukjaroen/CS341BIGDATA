"""AWS Lambda transform_batch (Python 3.12, app.handler): no pandas/pyarrow.

Python validates and LEFT JOINs two raw CSVs; Athena CTAS writes Snappy Parquet
under a run-scoped STAGING prefix. Does NOT publish to curated.
Requires boto3 (bundled with AWS Lambda), Glue/Athena/S3 permissions.
"""
import csv
import io
import json
import math
import os
import re
import time
import uuid
from datetime import date, datetime, timezone

import boto3

s3 = boto3.client('s3')
glue = boto3.client('glue')
athena = boto3.client('athena')

WEATHER_FIELDS = [
    'temperature_2m_mean', 'weather_code', 'wind_speed_10m_max',
    'wind_direction_10m_dominant', 'precipitation_sum',
    'precipitation_hours', 'relative_humidity_2m_mean',
]
OUTPUT_FIELDS = ['station_id', 'date', 'pm25'] + WEATHER_FIELDS + ['weather_description', 'lat', 'lon']
FLOAT_FIELDS = ['pm25'] + [x for x in WEATHER_FIELDS if x != 'weather_code']


def description(code):
    if code is None:
        return None
    if 0 <= code <= 3:
        return 'Clear sky / Cloudy'
    if code in (45, 48):
        return 'Fog'
    if 51 <= code <= 57:
        return 'Drizzle'
    if 61 <= code <= 67:
        return 'Rain'
    if 71 <= code <= 77:
        return 'Snow'
    if 80 <= code <= 86:
        return 'Showers'
    if 95 <= code <= 99:
        return 'Thunderstorm'
    return 'Unknown'


def parse_date(value, label, index):
    try:
        parsed = date.fromisoformat((value or '').strip())
        if parsed.isoformat() != (value or '').strip():
            raise ValueError('noncanonical date')
        return parsed
    except ValueError as exc:
        raise ValueError(f'{label} row {index}: invalid date {value!r}, expected YYYY-MM-DD') from exc


def parse_number(value, label, index, field, required=False):
    raw = (value or '').strip()
    if not raw:
        if required:
            raise ValueError(f'{label} row {index}: missing {field}')
        return None
    try:
        number = float(raw)
    except ValueError as exc:
        raise ValueError(f'{label} row {index}: invalid numeric {field}={raw!r}') from exc
    if not math.isfinite(number):
        raise ValueError(f'{label} row {index}: nonfinite {field}')
    if field == 'weather_code':
        if not number.is_integer() or not (-2147483648 <= number <= 2147483647):
            raise ValueError(f'{label} row {index}: weather_code must be INT32')
        return int(number)
    if field == 'pm25' and number < 0:
        raise ValueError(f'{label} row {index}: negative pm25')
    return number


def read_rows(bucket, key, label, required_fields, year):
    response = s3.get_object(Bucket=bucket, Key=key)
    data = response['Body'].read().decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(data, newline=''))
    if not reader.fieldnames:
        raise ValueError(f'{label}: empty CSV/header')
    missing = sorted(set(required_fields) - set(reader.fieldnames))
    if missing:
        raise ValueError(f'{label}: missing columns {missing}')
    records = {}
    for index, row in enumerate(reader, start=2):
        if None in row:
            raise ValueError(f'{label} row {index}: too many columns')
        station = (row.get('station_id') or '').strip()
        if not station:
            raise ValueError(f'{label} row {index}: empty station_id')
        day = parse_date(row.get('date'), label, index)
        if day.year != int(year):
            raise ValueError(f'{label} row {index}: date outside year={year}')
        pair = (station, day.isoformat())
        if pair in records:
            raise ValueError(f'{label}: duplicate station_id/date {pair}')
        fields = ['pm25'] if label == 'PM25' else WEATHER_FIELDS
        values = {field: parse_number(row.get(field), label, index, field, required=(field == 'pm25')) for field in fields}
        records[pair] = values
    if label == 'PM25' and not records:
        raise ValueError('PM25: no rows')
    return records



def read_coordinates(bucket, key):
    """Read station coordinates; allow common header aliases, never drop PM25 rows."""
    response = s3.get_object(Bucket=bucket, Key=key)
    data = response['Body'].read().decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(data, newline=''))
    if not reader.fieldnames:
        raise ValueError(f'Coordinates: empty CSV/header at {key}')
    names = {name.strip().lower(): name for name in reader.fieldnames}

    def column(*aliases):
        for alias in aliases:
            if alias in names:
                return names[alias]
        return None

    station_col = column('station_id', 'stationid', 'station_code')
    lat_col = column('lat', 'latitude')
    lon_col = column('lon', 'lng', 'longitude', 'long')
    if not all((station_col, lat_col, lon_col)):
        raise ValueError(f'Coordinates: expected station_id, lat, lon columns (or aliases); found {reader.fieldnames}')

    coordinates = {}
    for index, row in enumerate(reader, start=2):
        if None in row:
            raise ValueError(f'Coordinates row {index}: too many columns')
        station = (row.get(station_col) or '').strip()
        if not station:
            raise ValueError(f'Coordinates row {index}: missing station_id')
        if station in coordinates:
            raise ValueError(f'Coordinates: duplicate station_id {station!r}')
        lat = parse_number(row.get(lat_col), 'Coordinates', index, 'lat')
        lon = parse_number(row.get(lon_col), 'Coordinates', index, 'lon')
        if (lat is None) != (lon is None):
            raise ValueError(f'Coordinates row {index}: lat and lon must both be set or both empty')
        if lat is not None and not (-90 <= lat <= 90 and -180 <= lon <= 180):
            raise ValueError(f'Coordinates row {index}: coordinates out of range')
        coordinates[station] = {'lat': lat, 'lon': lon}
    return coordinates


def put_csv(bucket, key, records):
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=OUTPUT_FIELDS, lineterminator='\n')
    writer.writeheader()
    writer.writerows(records)
    s3.put_object(Bucket=bucket, Key=key, Body=stream.getvalue().encode('utf-8'), ContentType='text/csv')


def create_source_table(database, name, bucket, input_prefix):
    glue.create_table(
        DatabaseName=database,
        TableInput={
            'Name': name, 'TableType': 'EXTERNAL_TABLE',
            'Parameters': {'EXTERNAL': 'TRUE', 'skip.header.line.count': '1', 'classification': 'csv'},
            'StorageDescriptor': {
                'Columns': [{'Name': col, 'Type': 'string'} for col in OUTPUT_FIELDS],
                'Location': f's3://{bucket}/{input_prefix}',
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                    'Parameters': {'separatorChar': ',', 'quoteChar': '"', 'escapeChar': '\\'},
                },
            },
        },
    )


def execute_athena(query, database, bucket, context):
    params = {
        'QueryString': query,
        'QueryExecutionContext': {'Database': database},
        'ResultConfiguration': {'OutputLocation': os.environ.get('ATHENA_RESULTS_S3', f's3://{bucket}/athena-results/')},
    }
    if os.environ.get('ATHENA_WORKGROUP'):
        params['WorkGroup'] = os.environ['ATHENA_WORKGROUP']
    qid = athena.start_query_execution(**params)['QueryExecutionId']
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']
        state = status['State']
        if state == 'SUCCEEDED':
            return qid
        if state in ('FAILED', 'CANCELLED'):
            raise RuntimeError(f'Athena query {qid} {state}: {status.get("StateChangeReason", "unknown")}')
        if context is not None and context.get_remaining_time_in_millis() < 12000:
            athena.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f'Athena query {qid} exceeded remaining Lambda time')
        time.sleep(2)


def list_keys(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    return [obj['Key'] for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
            for obj in page.get('Contents', [])]


def delete_prefix(bucket, prefix):
    keys = list_keys(bucket, prefix)
    for start in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': k} for k in keys[start:start+1000]]})


def handler(event, context):
    validation = event.get('validation') or {}
    fetch = event.get('fetch') or {}
    check = event.get('check') or {}
    bucket = event.get('bucket') or os.environ.get('BUCKET_NAME', 'pm25-weather')
    year = str(event.get('year') or event.get('batch_id') or check.get('year') or '')
    run_id = str(event.get('run_id') or check.get('run_id') or '')
    pm_key = validation.get('raw_key')
    weather_key = fetch.get('weather_key')

    if not re.fullmatch(r'20\d{2}', year):
        raise ValueError('Expected year or batch_id = YYYY')
    try:
        uuid.UUID(run_id)
    except (ValueError, AttributeError) as exc:
        raise ValueError('Expected run_id as UUID, generated by check_batch') from exc
    if validation.get('status') not in (None, 'PASSED') or fetch.get('status') not in (None, 'PASSED'):
        raise ValueError('Upstream validation/fetch must have PASSED')
    expected_pm = f'raw/pm25/year={year}/run_id={run_id}/'
    expected_weather = f'raw/weather/year={year}/run_id={run_id}/'
    if not pm_key or not pm_key.startswith(expected_pm):
        raise ValueError(f'validation.raw_key must start with {expected_pm}')
    if not weather_key or not weather_key.startswith(expected_weather):
        raise ValueError(f'fetch.weather_key must start with {expected_weather}')

    pm = read_rows(bucket, pm_key, 'PM25', ['station_id', 'date', 'pm25'], year)
    weather = read_rows(bucket, weather_key, 'Weather', ['station_id', 'date'] + WEATHER_FIELDS, year)
    coordinates_key = os.environ.get('COORDINATES_KEY', 'metadata/station_coordinates.csv')
    coordinates = read_coordinates(bucket, coordinates_key)
    rows = []
    matched = 0
    unknown = 0
    missing_days = set()
    missing_coordinate_stations = set()
    missing_coordinate_rows = 0
    for (station, day), pm_values in sorted(pm.items()):
        weather_values = weather.get((station, day))
        if weather_values is None:
            missing_days.add(day)
        else:
            matched += 1
        values = weather_values if weather_values is not None else {name: None for name in WEATHER_FIELDS}
        station_coords = coordinates.get(station)
        if station_coords is None or station_coords['lat'] is None:
            missing_coordinate_rows += 1
            missing_coordinate_stations.add(station)
            station_coords = {'lat': None, 'lon': None}
        desc = description(values['weather_code'])
        unknown += desc == 'Unknown'
        rows.append({'station_id': station, 'date': day, 'pm25': pm_values['pm25'],
                     **values, 'weather_description': desc, **station_coords})
    metrics = {
        'pm25_rows': len(pm), 'weather_rows': len(weather), 'joined_rows': len(rows),
        'weather_matched_rows': matched, 'weather_missing_rows': len(pm) - matched,
        'weather_missing_dates': len(missing_days),
        'weather_unknown_code_rows': unknown, 'duplicate_keys': 0,
        'coordinates_metadata_rows': len(coordinates),
        'missing_coordinates_rows': missing_coordinate_rows,
        'missing_coordinates_stations': sorted(missing_coordinate_stations),
    }
    if len(rows) != len(pm):
        raise AssertionError('LEFT JOIN changed row count')

    database = os.environ.get('GLUE_DATABASE', 'pm25_weather')
    token = run_id.replace('-', '').lower()
    source_table = f'_pm25_transform_src_{token}'
    parquet_table = f'_pm25_transform_parquet_{token}'
    base = f'staging/pm25_weather/year={year}/run_id={run_id}/'
    input_prefix = base + 'input/'
    parquet_prefix = base + 'parquet/'
    input_key = input_prefix + 'data.csv'
    manifest_key = base + 'manifest.json'

    # Re-run for the same run_id replaces only this run's staging files.
    # It never deletes curated data.
    delete_prefix(bucket, parquet_prefix)
    put_csv(bucket, input_key, rows)
    create_source_table(database, source_table, bucket, input_prefix)
    qid = None
    try:
        select = [
            'CAST(station_id AS varchar) AS station_id',
            'CAST("date" AS date) AS "date"',
            'CAST(pm25 AS double) AS pm25',
            'TRY_CAST(NULLIF(temperature_2m_mean, \'\') AS double) AS temperature_2m_mean',
            'TRY_CAST(NULLIF(weather_code, \'\') AS integer) AS weather_code',
            'TRY_CAST(NULLIF(wind_speed_10m_max, \'\') AS double) AS wind_speed_10m_max',
            'TRY_CAST(NULLIF(wind_direction_10m_dominant, \'\') AS double) AS wind_direction_10m_dominant',
            'TRY_CAST(NULLIF(precipitation_sum, \'\') AS double) AS precipitation_sum',
            'TRY_CAST(NULLIF(precipitation_hours, \'\') AS double) AS precipitation_hours',
            'TRY_CAST(NULLIF(relative_humidity_2m_mean, \'\') AS double) AS relative_humidity_2m_mean',
            'NULLIF(weather_description, \'\') AS weather_description',
            'TRY_CAST(NULLIF(lat, \'\') AS double) AS lat',
            'TRY_CAST(NULLIF(lon, \'\') AS double) AS lon',
            f"CAST('{year}' AS varchar) AS year",
            'substr("date", 6, 2) AS month',
        ]
        sql = (f'CREATE TABLE "{database}"."{parquet_table}" WITH ('
               f"external_location = 's3://{bucket}/{parquet_prefix}', "
               "format = 'PARQUET', parquet_compression = 'SNAPPY', "
               "partitioned_by = ARRAY['year','month']) AS\nSELECT\n  "
               + ',\n  '.join(select) + f'\nFROM "{database}"."{source_table}"')
        qid = execute_athena(sql, database, bucket, context)
        # Athena CTAS normally writes Parquet objects without a .parquet suffix.
        # S3 has no rename operation: copy each object to a .parquet key, then
        # delete the original only after the copy has succeeded.
        parquet_keys = []
        for key in list_keys(bucket, parquet_prefix):
            if (key.endswith('/') or key.endswith('_$folder$')
                    or key.endswith('.metadata') or key.endswith('.crc')):
                continue
            if not key.endswith('.parquet'):
                # Verify the Parquet magic header before renaming any object.
                head = s3.get_object(Bucket=bucket, Key=key, Range='bytes=0-3')
                magic = head['Body'].read()
                if magic != b'PAR1':
                    raise RuntimeError(f'Unexpected non-Parquet object in CTAS output: {key}')
                new_key = key + '.parquet'
                s3.copy_object(Bucket=bucket, Key=new_key,
                               CopySource={'Bucket': bucket, 'Key': key})
                # Ensure the copied object exists before deleting the original.
                s3.head_object(Bucket=bucket, Key=new_key)
                s3.delete_object(Bucket=bucket, Key=key)
                key = new_key
            parquet_keys.append(key)
        if not parquet_keys:
            raise RuntimeError(
                f'Athena CTAS succeeded but no Parquet objects found under '
                f's3://{bucket}/{parquet_prefix}; QueryExecutionId={qid}'
            )
        parquet_keys.sort()
        manifest = {
            'status': 'STAGED', 'year': year, 'run_id': run_id,
            'staging_prefix': parquet_prefix, 'parquet_keys': parquet_keys,
            'schema_columns': OUTPUT_FIELDS, 'partition_columns': ['year', 'month'],
            'coordinates_key': coordinates_key,
            'metrics': metrics, 'athena_query_id': qid,
            'created_at': datetime.now(timezone.utc).isoformat(),
        }
        s3.put_object(Bucket=bucket, Key=manifest_key,
                      Body=json.dumps(manifest, ensure_ascii=False).encode('utf-8'),
                      ContentType='application/json')
        event['transform'] = {
            'status': 'PASSED', 'staging_prefix': parquet_prefix,
            'manifest_key': manifest_key, 'parquet_keys': parquet_keys,
            'athena_query_id': qid, 'metrics': metrics,
        }
        return event
    finally:
        # Glue metadata only; DROP does not delete S3 objects.
        for table in (parquet_table, source_table):
            try:
                glue.delete_table(DatabaseName=database, Name=table)
            except glue.exceptions.EntityNotFoundException:
                pass
