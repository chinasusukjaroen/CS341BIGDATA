"""AWS Glue 5.0 Spark job: Iceberg full-year replacement.

Required arguments: --BUCKET --YEAR --RUN_ID --DATABASE --TABLE --MANIFEST_KEY
Requires Iceberg Spark catalog configured in Glue job parameters.

WARNING: Test overwrite-by-filter behavior against a disposable year/table before
production use. Do not run concurrent publishers for the same year.
"""
import json
import re
import sys
import uuid

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F

args = getResolvedOptions(sys.argv, [
    'BUCKET', 'YEAR', 'RUN_ID', 'DATABASE', 'TABLE', 'MANIFEST_KEY'
])
bucket, year, run_id = args['BUCKET'], args['YEAR'], args['RUN_ID']
if not re.fullmatch(r'20\d{2}', year):
    raise ValueError('Invalid YEAR')
uuid.UUID(run_id)
for name in ('DATABASE', 'TABLE'):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', args[name]):
        raise ValueError(f'Invalid {name}')

expected_manifest = f'staging/pm25_weather/year={year}/run_id={run_id}/manifest.json'
if args['MANIFEST_KEY'] != expected_manifest:
    raise ValueError('Manifest key mismatch')

s3 = boto3.client('s3')
manifest = json.loads(s3.get_object(Bucket=bucket, Key=expected_manifest)['Body'].read())
prefix = f'staging/pm25_weather/year={year}/run_id={run_id}/parquet/'
if (manifest.get('status') != 'STAGED'
        or str(manifest.get('year')) != year
        or manifest.get('run_id') != run_id
        or manifest.get('staging_prefix') != prefix):
    raise ValueError('Invalid staged manifest')

expected_keys = manifest.get('parquet_keys') or []
if (not expected_keys
        or len(set(expected_keys)) != len(expected_keys)
        or any(not k.startswith(prefix) or not k.endswith('.parquet') for k in expected_keys)):
    raise ValueError('Invalid staged parquet keys')
for key in expected_keys:
    s3.head_object(Bucket=bucket, Key=key)

spark = SparkSession.builder.getOrCreate()
paths = [f's3://{bucket}/{key}' for key in expected_keys]
df = spark.read.option('basePath', f's3://{bucket}/{prefix}').parquet(*paths)
columns = [
    'station_id', 'date', 'pm25', 'temperature_2m_mean', 'weather_code',
    'wind_speed_10m_max', 'wind_direction_10m_dominant', 'precipitation_sum',
    'precipitation_hours', 'relative_humidity_2m_mean',
    'weather_description', 'lat', 'lon', 'year', 'month'
]
if set(df.columns) != set(columns):
    raise ValueError(f'Schema columns mismatch: {df.columns}')
df = df.select(*columns).withColumn('date', F.col('date').cast('date'))

invalid = (
    F.col('year').isNull()
    | F.col('month').isNull()
    | (F.col('year') != year)
    | (F.year('date') != int(year))
    | F.col('date').isNull()
    | (F.col('month') != F.date_format('date', 'MM'))
    | F.col('station_id').isNull()
    | F.col('pm25').isNull()
    | (F.col('pm25') < 0)
)
if df.filter(invalid).limit(1).count():
    raise ValueError('Invalid year/date/month/station/pm25 values')

count = df.count()
if count <= 0 or count != int(manifest.get('metrics', {}).get('joined_rows', -1)):
    raise ValueError('Row count mismatch')
if df.groupBy('station_id', 'date').count().filter('count > 1').limit(1).count():
    raise ValueError('Duplicate station_id/date')

target = f"glue_catalog.{args['DATABASE']}.{args['TABLE']}"

# Set once here for convenience; for production, prefer setting this during
# table provisioning instead of mutating table properties every publish.
spark.sql(f"""
    ALTER TABLE {target}
    SET TBLPROPERTIES ('write.object-storage.enabled' = 'false')
""")

metadata = spark.sql(f'SHOW TBLPROPERTIES {target}').collect()
props = {str(row[0]): str(row[1]) for row in metadata}
if props.get('write.object-storage.enabled', '').lower() != 'false':
    raise ValueError('Iceberg object-storage hash prefix is not disabled')

# Check partition spec from Iceberg metadata, not merely from Spark columns.
# DESCRIBE TABLE EXTENDED can vary by engine; use Iceberg Java table API only
# if a stronger partition-spec assertion is needed.

# Full-year replacement: new upload may omit some months.
# Must be tested with this Glue/Iceberg version before production use.
df.writeTo(target).overwrite(F.col('year') == F.lit(year))

receipt_key = f'iceberg-publish-receipts/year={year}/run_id={run_id}.json'
receipt = {
    'status': 'COMMITTED', 'year': year, 'run_id': run_id,
    'rows': count, 'database': args['DATABASE'], 'table': args['TABLE'],
    'manifest_key': expected_manifest
}
s3.put_object(
    Bucket=bucket, Key=receipt_key,
    Body=json.dumps(receipt).encode('utf-8'),
    ContentType='application/json'
)
print(json.dumps({
    'status': 'COMMITTED', 'year': year, 'run_id': run_id,
    'rows': count, 'target': target
}))

