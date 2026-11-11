"""Finalize only after Glue .sync succeeded; verifies receipt and Athena current year count.
IMPORTANT: serialize publications per year; no automatic replay after uncertain commit.
"""
import json, os, re, time, uuid
from datetime import datetime, timezone
import boto3
s3=boto3.client('s3'); athena=boto3.client('athena')

def handler(event, context):
    bucket=event.get('bucket') or os.environ.get('BUCKET_NAME','pm25-weather')
    year=str(event.get('batch_id') or event.get('year') or '')
    run_id=str(event.get('run_id') or '')
    if not re.fullmatch(r'20\d{2}',year): raise ValueError('Invalid year')
    uuid.UUID(run_id)
    verify=event.get('verify') or {}
    if verify.get('status')!='PASSED': raise ValueError('Verify not PASSED')
    result=event.get('glue_publish_result') or {}
    if result.get('JobRunState')!='SUCCEEDED': raise ValueError('Glue job not SUCCEEDED')
    if not event.get('source_key') or not event.get('source_etag'): raise ValueError('Missing source identity')
    receipt_key=f'iceberg-publish-receipts/year={year}/run_id={run_id}.json'
    receipt=json.loads(s3.get_object(Bucket=bucket,Key=receipt_key)['Body'].read())
    if (receipt.get('status')!='COMMITTED' or receipt.get('run_id')!=run_id
        or str(receipt.get('year'))!=year or receipt.get('rows')!=verify.get('verified_rows')
        or receipt.get('manifest_key')!=verify.get('manifest_key')):
        raise ValueError('Glue receipt mismatch')
    database=os.environ.get('GLUE_DATABASE','pm25_weather')
    table=os.environ.get('GLUE_TABLE','pm25_weather_iceberg')
    query=f'SELECT count(*) FROM "{database}"."{table}" WHERE year = \'{year}\''
    args={'QueryString':query,'QueryExecutionContext':{'Database':database},
          'ResultConfiguration':{'OutputLocation':os.environ.get('ATHENA_RESULTS_S3',f's3://{bucket}/athena-results/')}}
    if os.environ.get('ATHENA_WORKGROUP'): args['WorkGroup']=os.environ['ATHENA_WORKGROUP']
    qid=athena.start_query_execution(**args)['QueryExecutionId']
    while True:
        status=athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']
        if status['State']=='SUCCEEDED': break
        if status['State'] in ('FAILED','CANCELLED'): raise RuntimeError(f'Athena post-publish failed: {status}')
        if context and context.get_remaining_time_in_millis()<15000: raise TimeoutError('Athena post-publish timed out')
        time.sleep(2)
    rows=athena.get_query_results(QueryExecutionId=qid,MaxResults=2)['ResultSet']['Rows']
    if len(rows)<2: raise ValueError('Athena returned no count')
    actual=int(rows[1]['Data'][0].get('VarCharValue','0'))
    if actual!=verify['verified_rows']:
        raise ValueError(f'Iceberg published count mismatch: {actual} != {verify["verified_rows"]}')
    marker_key=f'curated/pm25_weather/year={year}/_SUCCESS.json'
    marker={'status':'SUCCESS','year':year,'run_id':run_id,
            'source_key':event['source_key'],'source_etag':event['source_etag'],
            'table':f'{database}.{table}','verified_rows':actual,
            'receipt_key':receipt_key,'manifest_key':verify['manifest_key'],
            'athena_query_id':qid,'published_at':datetime.now(timezone.utc).isoformat()}
    s3.put_object(Bucket=bucket,Key=marker_key,Body=json.dumps(marker).encode(),ContentType='application/json')
    event['verify']={**verify,'publish_status':'PUBLISHED','marker_key':marker_key,
                     'curated_athena_query_id':qid}
    return event

