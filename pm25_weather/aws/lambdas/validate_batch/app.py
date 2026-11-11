"""AWS Lambda handler: validate PM2.5 source, exclude stations with >20% missing,
return validation.status PASSED or NOT_PASSED. Uses openpyxl for XLSX.
"""
import csv, io, json, math, os, re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
import boto3

s3 = boto3.client('s3')
MISSING = {'', 'N/A', 'NA', 'NULL', 'NONE', '-'}

def norm(v):
    return str(v or '').strip().upper()

def parse_day(v):
    if isinstance(v, datetime): return v.date()
    if isinstance(v, date): return v
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return (datetime(1899, 12, 30) + timedelta(days=float(v))).date()
    if isinstance(v, str):
        for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%Y/%m/%d', '%d/%m/%Y', '%d-%m-%Y'):
            try: return datetime.strptime(v.strip(), fmt).date()
            except ValueError: pass
    raise ValueError('Invalid date')

def parse_pm(v):
    if v is None or isinstance(v, bool) or (isinstance(v, str) and norm(v) in MISSING):
        return None, 'missing'
    try:
        n = float(v)
        if math.isfinite(n) and 0 <= n <= 2000: return n, None
    except (ValueError, TypeError, OverflowError): pass
    return None, 'invalid'

def load_master(bucket):
    key = os.environ.get('MASTER_KEY', 'metadata/bmr_stations.csv')
    raw = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(raw))
    if not reader.fieldnames or 'station_id' not in reader.fieldnames:
        raise ValueError('Master CSV missing station_id')
    ids = {norm(r['station_id']) for r in reader if norm(r.get('station_id'))}
    if not ids: raise ValueError('Master station list empty')
    return ids, key


def records(blob, key, allowed):
    if key.lower().endswith('.xlsx'):
        from openpyxl import load_workbook

        try:
            wb = load_workbook(
                io.BytesIO(blob),
                read_only=True,
                data_only=True
            )
        except Exception as exc:
            raise ValueError('UNREADABLE_SOURCE_FILE') from exc

        try:
            # รองรับ DATA, Data, data
            sheet_name = next(
                (
                    name for name in wb.sheetnames
                    if name.strip().upper() == 'DATA'
                ),
                None
            )

            if sheet_name is None:
                raise ValueError('MISSING_DATA_SHEET')

            # ใช้ชื่อ Sheet ที่ค้นพบจริง
            iterator = wb[sheet_name].iter_rows(values_only=True)

            header = next(iterator, None)
            if not header or norm(header[0]) != 'DATE':
                raise ValueError('INVALID_SCHEMA_DATE_COLUMN')

            cols = [
                (i, norm(h))
                for i, h in enumerate(header[1:], 1)
                if norm(h) in allowed
            ]

            if len({sid for _, sid in cols}) != len(cols):
                raise ValueError('DUPLICATE_STATION_COLUMNS')

            if not cols:
                raise ValueError('NO_BMR_STATIONS')

            for row in iterator:
                if not row or row[0] is None:
                    continue

                if isinstance(row[0], str) and not re.match(r'^\d', row[0].strip()):
                    continue

                for i, sid in cols:
                    yield row[0], sid, row[i] if i < len(row) else None

        finally:
            wb.close()

    elif key.lower().endswith('.csv'):
        try:
            reader = csv.DictReader(
                io.StringIO(blob.decode('utf-8-sig'))
            )
        except UnicodeError as exc:
            raise ValueError('UNREADABLE_SOURCE_FILE') from exc

        names = {
            str(c).strip().lower(): c
            for c in (reader.fieldnames or [])
        }

        if not {'date', 'station_id', 'pm25'} <= set(names):
            raise ValueError('MISSING_REQUIRED_COLUMNS')

        for row in reader:
            sid = norm(row.get(names['station_id']))
            if sid in allowed:
                yield (
                    row.get(names['date']),
                    sid,
                    row.get(names['pm25'])
                )

    else:
        raise ValueError('UNSUPPORTED_SOURCE_FORMAT')

def put_json(bucket, key, value):
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(value, ensure_ascii=False, indent=2).encode(), ContentType='application/json')

def handler(event, context):
    bucket = event.get('bucket') or os.environ.get('DATA_BUCKET')
    year = str(event.get('batch_id') or '')
    source_key = event.get('source_key', '')
    if not bucket or not re.fullmatch(r'20\d{2}', year): raise ValueError('bucket and batch_id required')
    if not re.fullmatch(rf'incoming/pm25/year={year}/[^/]+\.(xlsx|csv)', source_key, re.I):
        raise ValueError('source_key/year mismatch')
    run_id = str(event.get('run_id') or 'manual')
    safe_run = re.sub(r'[^A-Za-z0-9_-]', '_', run_id)[:100]
    report_key = f'quality/pm25/year={year}/{safe_run}/quality_report.json'
    raw_key = f"raw/pm25/year={year}/run_id={run_id}/pm25_{year}.csv"
    threshold = float(os.environ.get('MAX_MISSING_PERCENT', '20'))
    min_stations = int(os.environ.get('MIN_VALID_STATIONS', '2'))
    if not (0 <= threshold < 100) or min_stations < 1: raise ValueError('Invalid quality configuration')
    allowed, master_key = load_master(bucket)  # Infrastructure failures are retryable.
    blob = s3.get_object(Bucket=bucket, Key=source_key)['Body'].read()
    values = defaultdict(dict)
    observed_dates = set()
    counts = Counter()
    problems = []
    try:
        for day_raw, sid, pm_raw in records(blob, source_key, allowed):
            try:
                d = parse_day(day_raw)
                if d.year != int(year): raise ValueError('Wrong year')
            except (ValueError, TypeError, OverflowError):
                counts['invalid_date_cells'] += 1
                continue
            observed_dates.add(d)
            pm, issue = parse_pm(pm_raw)
            if d in values[sid]:
                counts['duplicate_station_dates'] += 1
                # Keep first observation; report duplicate rather than double counting.
                continue
            values[sid][d] = pm
            if issue: counts[f'{issue}_pm25_cells'] += 1
    except (ValueError, KeyError, TypeError, ImportError) as exc:
        problems.append(str(exc))
    # Shared date window is the min/max valid date observed anywhere in source,
    # NOT the first/last valid PM2.5 date of each station.
    start, end = (min(observed_dates), max(observed_dates)) if observed_dates else (None, None)
    expected_days = (end - start).days + 1 if start else 0
    stations = []
    retained = []
    for sid in sorted(values):
        entries = values[sid]
        valid_dates = {d for d, v in entries.items() if v is not None}
        valid_count = len(valid_dates)
        missing_count = expected_days - valid_count
        rate = round(100 * missing_count / expected_days, 4) if expected_days else 100.0
        monthly = Counter()
        if start:
            d = start
            while d <= end:
                if d not in valid_dates: monthly[d.strftime('%Y-%m')] += 1
                d += timedelta(days=1)
        passed = rate <= threshold and valid_count > 0
        stations.append({'station_id': sid, 'status': 'PASS' if passed else 'EXCLUDE',
                         'expected_days': expected_days, 'valid_days': valid_count,
                         'missing_days': missing_count, 'missing_rate_percent': rate,
                         'monthly_missing_days': dict(sorted(monthly.items())),
                         'reason': None if passed else 'PM25_MISSING_RATE_EXCEEDED_20_PERCENT'})
        if passed:
            retained.extend({'station_id': sid, 'date': d.isoformat(), 'pm25': v}
                            for d, v in entries.items() if v is not None)
    passed_ids = [s['station_id'] for s in stations if s['status'] == 'PASS']
    excluded = [s for s in stations if s['status'] == 'EXCLUDE']
    if not stations and not problems: problems.append('NO_BMR_STATIONS_WITH_VALID_DATES')
    if len(passed_ids) < min_stations:
        problems.append('INSUFFICIENT_VALID_STATIONS')
    status = 'NOT_PASSED' if problems else 'PASSED'
    retained.sort(key=lambda r: (r['station_id'], r['date']))
    report = {'status': status, 'batch_id': year, 'run_id': run_id,
              'source_key': source_key, 'master_key': master_key,
              'evaluation_window': {'start_date': start.isoformat() if start else None,
                                    'end_date': end.isoformat() if end else None,
                                    'expected_days': expected_days,
                                    'assumption': 'All source stations expected daily observations throughout shared file date range; station activation dates unavailable'},
              'station_quality': {'max_missing_rate_percent': threshold,
                                  'min_valid_stations': min_stations,
                                  'total_stations_found': len(stations),
                                  'passed': len(passed_ids), 'excluded': len(excluded),
                                  'passed_station_ids': passed_ids,
                                  'excluded_station_ids': [s['station_id'] for s in excluded],
                                  'stations': stations},
              'validated_rows': len(retained), 'source_issues': dict(counts),
              'reason_codes': problems, 'generated_at': datetime.now(timezone.utc).isoformat()}
    put_json(bucket, report_key, report)
    if status == 'PASSED':
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=['station_id', 'date', 'pm25'])
        writer.writeheader(); writer.writerows(retained)
        s3.put_object(Bucket=bucket, Key=raw_key, Body=out.getvalue().encode(), ContentType='text/csv')
    result = dict(event)
    result['validation'] = {'status': status, 'report_key': report_key,
                            'raw_key': raw_key if status == 'PASSED' else None,
                            'station_count': len(passed_ids),
                            'stations_found': passed_ids,
                            'excluded_station_ids': [s['station_id'] for s in excluded],
                            'excluded_station_details': excluded,
                            'valid_rows': len(retained), 'errors': problems,
                            'first_date': start.isoformat() if start else None,
                            'last_date': end.isoformat() if end else None}
    return result


