"""Verify staged PM2.5/Weather Parquet BEFORE an Iceberg commit.

Safety: first publication only; refuses replacing an existing published year.
Curated file names are part-00001.parquet per month.
Re-runs of the same completed run are idempotent. Replacement of a published
snapshot requires a separate versioned publishing design (not S3 copy/delete).
Runtime: AWS Lambda Python 3.12+, handler app.handler, boto3 only.
"""
import copy
from collections import defaultdict
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
glue = boto3.client('glue')
athena = boto3.client('athena')

DB = 'pm25_weather'
TABLE = 'pm25_weather'
EXPECTED = {
    'station_id': 'string', 'date': 'date', 'pm25': 'double',
    'temperature_2m_mean': 'double', 'weather_code': 'int',
    'wind_speed_10m_max': 'double', 'wind_direction_10m_dominant': 'double',
    'precipitation_sum': 'double', 'precipitation_hours': 'double',
    'relative_humidity_2m_mean': 'double', 'weather_description': 'string',
    'lat': 'double', 'lon': 'double',
}


def keys(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    return [obj for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
            for obj in page.get('Contents', []) if not obj['Key'].endswith('/')]


def read_json(bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read())


def missing(exc):
    return exc.response.get('Error', {}).get('Code') in ('NoSuchKey', '404', 'NotFound')


def athena_query(sql, database, bucket, context):
    params = {
        'QueryString': sql,
        'QueryExecutionContext': {'Database': database},
        'ResultConfiguration': {'OutputLocation': os.environ.get('ATHENA_RESULTS_S3', f's3://{bucket}/athena-results/')},
    }
    if os.environ.get('ATHENA_WORKGROUP'):
        params['WorkGroup'] = os.environ['ATHENA_WORKGROUP']
    qid = athena.start_query_execution(**params)['QueryExecutionId']
    while True:
        state = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']
        if state['State'] == 'SUCCEEDED':
            return qid
        if state['State'] in ('FAILED', 'CANCELLED'):
            raise RuntimeError(f'Athena {qid} {state["State"]}: {state.get("StateChangeReason")}')
        if context and context.get_remaining_time_in_millis() < 15000:
            athena.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f'Athena {qid} timed out')
        time.sleep(2)



def handler(event, context):
    transform = event.get('transform') or {}
    bucket = event.get('bucket') or os.environ.get('BUCKET_NAME', 'pm25-weather')
    year = str(event.get('year') or event.get('batch_id') or (event.get('check') or {}).get('year') or '')
    run_id = str(event.get('run_id') or (event.get('check') or {}).get('run_id') or '')
    database = os.environ.get('GLUE_DATABASE', DB)
    table_name = os.environ.get('GLUE_TABLE', TABLE)
    if not re.fullmatch(r'20\d{2}', year):
        raise ValueError('Invalid year')
    try:
        uuid.UUID(run_id)
    except (ValueError, AttributeError) as exc:
        raise ValueError('Invalid run_id') from exc
    if transform.get('status') != 'PASSED':
        raise ValueError('transform.status must be PASSED')
    base = f'staging/pm25_weather/year={year}/run_id={run_id}/'
    parquet_prefix = base + 'parquet/'
    manifest_key = base + 'manifest.json'
    if transform.get('manifest_key') != manifest_key or transform.get('staging_prefix') != parquet_prefix:
        raise ValueError('Transform paths do not match year/run_id')
    manifest = read_json(bucket, manifest_key)
    if (manifest.get('status') != 'STAGED' or str(manifest.get('year')) != year
            or manifest.get('run_id') != run_id or manifest.get('staging_prefix') != parquet_prefix):
        raise ValueError('Manifest year/run_id/status mismatch')
    expected_keys = manifest.get('parquet_keys')
    if not isinstance(expected_keys, list) or not expected_keys or len(set(expected_keys)) != len(expected_keys):
        raise ValueError('Invalid or empty manifest parquet_keys')
    if transform.get('parquet_keys') != expected_keys:
        raise ValueError('Transform and manifest parquet_keys differ')
    metrics = manifest.get('metrics') or {}
    if transform.get('metrics') != metrics:
        raise ValueError('Transform and manifest metrics differ')
    if metrics.get('joined_rows', 0) <= 0 or metrics.get('joined_rows') != metrics.get('pm25_rows'):
        raise ValueError('Manifest row counts inconsistent')
    if metrics.get('duplicate_keys') != 0:
        raise ValueError('Duplicate keys reported')
    if metrics.get('weather_matched_rows', 0) + metrics.get('weather_missing_rows', 0) != metrics['joined_rows']:
        raise ValueError('Weather counts inconsistent')
    actual = {obj['Key']: obj for obj in keys(bucket, parquet_prefix)}
    if set(actual) != set(expected_keys):
        raise ValueError(f'Staging object mismatch: missing={set(expected_keys)-set(actual)}, extra={set(actual)-set(expected_keys)}')
    for key in expected_keys:
        if not key.startswith(parquet_prefix + f'year={year}/month=') or not re.search(r'/month=(0[1-9]|1[0-2])/', key):
            raise ValueError(f'Unexpected partition key {key}')
        if not key.endswith('.parquet') or actual[key]['Size'] < 8:
            raise ValueError(f'Invalid Parquet object {key}')
        head = s3.get_object(Bucket=bucket, Key=key, Range='bytes=0-3')['Body'].read()
        tail = s3.get_object(Bucket=bucket, Key=key, Range='bytes=-4')['Body'].read()
        if head != b'PAR1' or tail != b'PAR1':
            raise ValueError(f'Invalid Parquet magic {key}')

    # Verify is read-only. Publication happens in the following Glue state.
    if not event.get('source_key') or not event.get('source_etag'):
        raise ValueError('source_key and source_etag required')

    # Validate the STAGING schema against the original contract, independently
    # of the target Iceberg table's partition specification.
    source_table_name = os.environ.get('STAGING_SCHEMA_TABLE', 'pm25_weather')
    target = glue.get_table(DatabaseName=database, Name=source_table_name)['Table']
    found = {c['Name'].lower(): c['Type'].lower() for c in target['StorageDescriptor']['Columns']}
    for name, dtype in EXPECTED.items():
        if found.get(name) != dtype:
            raise ValueError(f'Staging schema {name}: expected {dtype}, found {found.get(name)}')
    partitions = {p['Name'].lower(): p['Type'].lower() for p in target.get('PartitionKeys', [])}
    if partitions != {'year': 'string', 'month': 'string'}:
        raise ValueError(f'Staging template partition keys must be year/month STRING, found {partitions}')

    temp = '_pm25_verify_' + run_id.replace('-', '').lower()
    descriptor = copy.deepcopy(target['StorageDescriptor'])
    descriptor['Location'] = f's3://{bucket}/{parquet_prefix}'
    descriptor.pop('NumberOfBuckets', None)
    descriptor.pop('SortColumns', None)
    # Use the target Parquet schema/SerDe and projection for run-scoped files.
    params = {
        'EXTERNAL': 'TRUE', 'projection.enabled': 'true',
        'projection.year.type': 'enum', 'projection.year.values': year,
        'projection.month.type': 'enum',
        'projection.month.values': ','.join(f'{i:02d}' for i in range(1, 13)),
        'storage.location.template': f's3://{bucket}/{parquet_prefix}year=${{year}}/month=${{month}}/',
    }
    glue.create_table(DatabaseName=database, TableInput={
        'Name': temp, 'TableType': 'EXTERNAL_TABLE', 'StorageDescriptor': descriptor,
        'PartitionKeys': target['PartitionKeys'], 'Parameters': params,
    })
    try:
        sql = f'''SELECT count(*) AS rows_total,
            count_if(station_id IS NULL OR trim(station_id) = '' OR "date" IS NULL OR pm25 IS NULL OR pm25 < 0) AS invalid_rows,
            count(*) - count(DISTINCT concat(station_id, '|', cast("date" AS varchar))) AS duplicates,
            count_if(lat IS NULL OR lon IS NULL) AS missing_coordinates,
            count_if(weather_description = 'Unknown') AS unknown_codes,
            count_if((lat IS NULL) <> (lon IS NULL)) AS incomplete_coordinates,
            count_if(year <> '{year}' OR cast(year("date") AS varchar) <> '{year}' OR month <> date_format(cast("date" AS timestamp), '%m')) AS wrong_partition
            FROM "{database}"."{temp}" WHERE year = '{year}' '''
        qid = athena_query(sql, database, bucket, context)
        result = athena.get_query_results(QueryExecutionId=qid, MaxResults=2)['ResultSet']['Rows']
        if len(result) < 2:
            raise RuntimeError('Athena returned no metrics row')
        vals = [int(c.get('VarCharValue', '0') or 0) for c in result[1]['Data']]
        if len(vals) != 7:
            raise RuntimeError(f'Unexpected Athena metrics {vals}')
        total, invalid, duplicates, missing_coords, unknown, incomplete, wrong = vals
        if total != metrics['joined_rows'] or missing_coords != metrics['missing_coordinates_rows'] or unknown != metrics['weather_unknown_code_rows'] or any((invalid, duplicates, incomplete, wrong)):
            raise ValueError(f'Verification failed: Athena={vals}, transform_metrics={metrics}')
    finally:
        glue.delete_table(DatabaseName=database, Name=temp)

    event['verify'] = {
        'status': 'PASSED', 'publish_status': 'NOT_PUBLISHED',
        'verified_rows': total, 'athena_query_id': qid,
        'manifest_key': manifest_key, 'staging_prefix': parquet_prefix,
        'parquet_keys': expected_keys,
    }
    return event


