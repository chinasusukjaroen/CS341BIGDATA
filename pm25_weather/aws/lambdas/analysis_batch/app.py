"""PM2.5 + Weather analytics publisher. Lambda Python 3.12, handler analysis_batch.handler.
Dependencies: boto3 (Lambda runtime). Athena queries Glue pm25_weather.pm25_weather.
Writes analytics JSON into S3; never modifies curated data.
"""
import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
athena = boto3.client('athena')
DB = os.environ.get('GLUE_DATABASE', 'pm25_weather')
TABLE = os.environ.get('GLUE_TABLE', 'pm25_weather_iceberg')
PREFIX = os.environ.get('ANALYTICS_PREFIX', 'analytics').strip('/')
RESULTS_PREFIX = os.environ.get('ATHENA_RESULTS_PREFIX', 'athena-results/')
MAX_SCATTER = int(os.environ.get('MAX_SCATTER_POINTS', '20000'))
WEATHER = [
    ('temperature_2m_mean', 'Temperature', '°C'),
    ('relative_humidity_2m_mean', 'Humidity', '%'),
    ('precipitation_sum', 'Precipitation', 'mm/day'),
    ('precipitation_hours', 'Rain Duration', 'hours/day'),
    ('wind_speed_10m_max', 'Wind Speed', 'km/h'),
]


def query(sql, bucket, context):
    output = os.environ.get('ATHENA_RESULTS_S3', f's3://{bucket}/{RESULTS_PREFIX}')
    params = {'QueryString': sql,
              'QueryExecutionContext': {'Database': DB},
              'ResultConfiguration': {'OutputLocation': output}}
    if os.environ.get('ATHENA_WORKGROUP'):
        params['WorkGroup'] = os.environ['ATHENA_WORKGROUP']
    qid = athena.start_query_execution(**params)['QueryExecutionId']
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']
        if status['State'] == 'SUCCEEDED':
            break
        if status['State'] in ('FAILED', 'CANCELLED'):
            raise RuntimeError(f'Athena {qid}: {status["State"]}: {status.get("StateChangeReason", "")}')
        if context and context.get_remaining_time_in_millis() < 30000:
            athena.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f'Not enough Lambda time for Athena query {qid}')
        time.sleep(2)
    rows = []
    columns = None
    first = True
    for page in athena.get_paginator('get_query_results').paginate(QueryExecutionId=qid):
        result = page['ResultSet']
        if columns is None:
            columns = [(c['Name'], c['Type']) for c in result['ResultSetMetadata']['ColumnInfo']]
        for row in result['Rows']:
            if first:
                first = False  # Athena SELECT results have a header row
                continue
            cells = row['Data']
            obj = {}
            for idx, (name, typ) in enumerate(columns):
                raw = cells[idx].get('VarCharValue') if idx < len(cells) else None
                if raw is None:
                    value = None
                elif typ in ('tinyint', 'smallint', 'integer', 'bigint'):
                    value = int(raw)
                elif typ in ('float', 'real', 'double', 'decimal') or typ.startswith('decimal('):
                    value = float(raw)
                elif typ == 'boolean':
                    value = raw.lower() == 'true'
                else:
                    value = raw
                obj[name] = value
            rows.append(obj)
    return rows


def marker_exists(bucket, year):
    key = f'curated/pm25_weather/year={year}/_SUCCESS.json'
    try:
        data = json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read())
    except ClientError as exc:
        if exc.response['Error']['Code'] in ('NoSuchKey', '404', 'NotFound'):
            return False
        raise
    return data.get('status') == 'SUCCESS' and str(data.get('year')) == year


def published_years(bucket):
    years = set()
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix='curated/pm25_weather/', Delimiter='/'):
        for item in page.get('CommonPrefixes', []):
            match = re.fullmatch(r'curated/pm25_weather/year=(20\d{2})/', item['Prefix'])
            if match and marker_exists(bucket, match.group(1)):
                years.add(match.group(1))
    return sorted(years)


def put(bucket, key, payload):
    s3.put_object(Bucket=bucket, Key=key,
                  Body=json.dumps(payload, ensure_ascii=False, allow_nan=False,
                                  separators=(',', ':')).encode('utf-8'),
                  ContentType='application/json; charset=utf-8',
                  CacheControl='no-cache')
    return key


def handler(event, context):
    bucket = event.get('bucket') or os.environ.get('BUCKET_NAME', 'pm25-weather')
    if not re.fullmatch(r'[a-z0-9][a-z0-9.-]{2,62}', bucket):
        raise ValueError('Invalid S3 bucket')
    if MAX_SCATTER < 1 or MAX_SCATTER > 100000:
        raise ValueError('MAX_SCATTER_POINTS must be 1..100000')
    years = published_years(bucket)
    if not years:
        raise ValueError('No successfully published years found under curated/pm25_weather/')
    requested_year = str(event.get('year') or event.get('batch_id') or '')
    if requested_year and requested_year not in years:
        raise ValueError(f'Year {requested_year} has no SUCCESS marker')
    # Include only successfully published years, even if the Glue table has other data.
    year_filter = 'year IN (' + ','.join("'" + y + "'" for y in years) + ')'
    table = f'"{DB}"."{TABLE}"'
    now = datetime.now(timezone.utc).isoformat()
    meta = {'schema_version': '1.0', 'generated_at': now,
            'source': f'{DB}.{TABLE}', 'available_years': [int(y) for y in years],
            'missing_value_policy': 'exclude_null'}
    outputs = {}

    def save(path, payload):
        key = f'{PREFIX}/{path}'
        put(bucket, key, {'metadata': meta, **payload})
        outputs[path] = key

    kpi = query(f'''SELECT CAST(MIN(date) AS varchar) AS start_date,
        CAST(MAX(date) AS varchar) AS end_date,
        AVG(pm25) AS avg_pm25, MAX(pm25) AS max_pm25,
        AVG(temperature_2m_mean) AS avg_temperature,
        COUNT(DISTINCT station_id) AS total_stations,
        AVG(relative_humidity_2m_mean) AS avg_humidity,
        COUNT(*) AS total_records, COUNT(pm25) AS valid_pm25,
        COUNT(temperature_2m_mean) AS valid_temperature,
        COUNT(relative_humidity_2m_mean) AS valid_humidity
        FROM {table} WHERE {year_filter}''', bucket, context)[0]
    if not kpi['total_records']:
        raise ValueError('Athena returned 0 rows; check Iceberg table and published markers')
    save('overall/kpi.json', {
        'period': {'start_date': kpi['start_date'], 'end_date': kpi['end_date']},
        'metrics': {k: kpi[k] for k in ('avg_pm25', 'max_pm25', 'avg_temperature',
                    'total_stations', 'avg_humidity', 'total_records')},
        'valid_records': {'pm25': kpi['valid_pm25'], 'temperature': kpi['valid_temperature'],
                          'humidity': kpi['valid_humidity']}})

    monthly = query(f'''SELECT year, month, AVG(pm25) AS avg_pm25,
        COUNT(pm25) AS sample_size FROM {table} WHERE {year_filter}
        GROUP BY year, month ORDER BY year, month''', bucket, context)
    for r in monthly:
        r['year'] = int(r['year']); r['month'] = int(r['month'])
        r['label'] = datetime(r['year'], r['month'], 1).strftime('%b %Y')
    save('overall/yearly_trend.json', {'granularity': 'monthly', 'unit': 'ug/m3', 'data': monthly})

    stations = query(f'''SELECT year, station_id, AVG(pm25) AS avg_pm25,
        MAX(pm25) AS max_pm25, COUNT(pm25) AS sample_size
        FROM {table} WHERE {year_filter} AND pm25 IS NOT NULL
        GROUP BY year, station_id ORDER BY year, avg_pm25 DESC''', bucket, context)
    for y in years:
        y_months = {r['month']: r for r in monthly if r['year'] == int(y)}
        month_data = [{'month': m, 'label': datetime(2000, m, 1).strftime('%b'),
                       'avg_pm25': y_months[m]['avg_pm25'] if m in y_months else None,
                       'sample_size': y_months[m]['sample_size'] if m in y_months else 0}
                      for m in range(1, 13)]
        save(f'yearly/year={y}/monthly_trend.json',
             {'year': int(y), 'unit': 'ug/m3', 'data': month_data})
        save(f'yearly/year={y}/station_summary.json',
             {'year': int(y), 'unit': 'ug/m3',
              'data': [{k: r[k] for k in ('station_id', 'avg_pm25', 'max_pm25', 'sample_size')}
                       for r in stations if r['year'] == y]})

    weather_columns = [('pm25', 'avg_pm25')] + [(col, 'avg_' + col) for col, _, _ in WEATHER]
    agg = ', '.join(f'AVG({col}) AS {alias}, COUNT({col}) AS n_{col}'
                    for col, alias in weather_columns)
    weather_rows = query(f'''SELECT year, month, {agg} FROM {table}
        WHERE {year_filter} GROUP BY year, month ORDER BY year, month''', bucket, context)
    for r in weather_rows:
        r['year'] = int(r['year']); r['month'] = int(r['month'])
        r['label'] = datetime(r['year'], r['month'], 1).strftime('%b %Y')
        r['sample_size'] = {col: r.pop('n_' + col) for col, _ in weather_columns}
    save('overall/weather_trend.json', {'granularity': 'monthly', 'data': weather_rows})

    corr_fields = ', '.join(f'CORR(pm25, {col}) AS corr_{col}, '
                            f'COUNT_IF(pm25 IS NOT NULL AND {col} IS NOT NULL) AS n_{col}'
                            for col, _, _ in WEATHER)
    corr = query(f'SELECT {corr_fields} FROM {table} WHERE {year_filter}', bucket, context)[0]
    save('overall/correlation.json', {
        'method': 'pearson', 'granularity': 'station_daily',
        'data': [{'variable': col, 'label': label, 'unit': unit,
                  'correlation': corr['corr_' + col], 'sample_size': corr['n_' + col]}
                 for col, label, unit in WEATHER]})

    # Stable deterministic subset for manageable browser payloads.
    scatter_cols = ', '.join(col for col, _, _ in WEATHER)
    scatter = query(f'''SELECT station_id, CAST(date AS varchar) AS date, pm25,
        {scatter_cols} FROM {table}
        WHERE {year_filter} AND pm25 IS NOT NULL
          AND ({' OR '.join(col + ' IS NOT NULL' for col, _, _ in WEATHER)})
        ORDER BY xxhash64(to_utf8(concat(station_id, '|', CAST(date AS varchar))))
        LIMIT {MAX_SCATTER}''', bucket, context)
    total_pairs = query(f'''SELECT COUNT(*) AS total FROM {table}
        WHERE {year_filter} AND pm25 IS NOT NULL
          AND ({' OR '.join(col + ' IS NOT NULL' for col, _, _ in WEATHER)})''', bucket, context)[0]['total']
    save('overall/scatter_data.json', {
        'granularity': 'station_daily', 'variables': [c for c, _, _ in WEATHER],
        'total_eligible_records': total_pairs, 'returned_records': len(scatter),
        'is_sampled': total_pairs > len(scatter), 'data': scatter})

    rain = query(f'''SELECT CASE WHEN precipitation_sum > 0 THEN 'rain'
        ELSE 'no_rain' END AS condition, AVG(pm25) AS avg_pm25,
        COUNT(*) AS sample_size FROM {table} WHERE {year_filter}
        AND pm25 IS NOT NULL AND precipitation_sum IS NOT NULL
        GROUP BY 1 ORDER BY 1''', bucket, context)
    save('overall/weather_condition.json', {
        'classification': 'precipitation_sum > 0', 'unit': 'ug/m3',
        'data': [{'condition': r['condition'], 'label': 'Rain' if r['condition'] == 'rain' else 'No Rain',
                  'avg_pm25': r['avg_pm25'], 'sample_size': r['sample_size']} for r in rain]})

    # Angles: 0=N, 45=NE, ..., 315=NW; use modulo to wrap 360 to N.
    directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
    direction_sql = f'''WITH categorized AS (
        SELECT pm25, CAST(MOD(CAST(FLOOR(MOD(wind_direction_10m_dominant + 22.5, 360.0) / 45.0)
        AS integer), 8) AS integer) AS direction_idx
        FROM {table} WHERE {year_filter} AND pm25 IS NOT NULL
        AND wind_direction_10m_dominant IS NOT NULL
        AND wind_direction_10m_dominant BETWEEN 0 AND 360)
        SELECT direction_idx, AVG(pm25) AS avg_pm25, COUNT(*) AS sample_size
        FROM categorized GROUP BY direction_idx ORDER BY direction_idx'''
    direction_rows = query(direction_sql, bucket, context)
    direction_map = {r['direction_idx']: r for r in direction_rows}
    save('overall/wind_direction.json', {
        'unit': 'ug/m3', 'direction_unit': 'degrees',
        'data': [{'direction': directions[i],
                  'avg_pm25': direction_map[i]['avg_pm25'] if i in direction_map else None,
                  'sample_size': direction_map[i]['sample_size'] if i in direction_map else 0}
                 for i in range(8)]})

    # Publish manifest LAST, so consumers can check the expected output set.
    manifest_key = put(bucket, f'{PREFIX}/_SUCCESS.json', {
        'status': 'SUCCESS', 'schema_version': '1.0', 'generated_at': now,
        'available_years': [int(y) for y in years], 'source_table': f'{DB}.{TABLE}',
        'total_records': kpi['total_records'], 'json_keys': list(outputs.values())})
    return {'status': 'SUCCESS', 'bucket': bucket, 'available_years': [int(y) for y in years],
            'total_records': kpi['total_records'], 'json_count': len(outputs),
            'manifest_key': manifest_key, 'json_keys': list(outputs.values())}


