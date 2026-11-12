"""Validate 2021–2024 Seoul AWS hourly Bronze without modifying source data.

Usage: python validate_seoul_weather.py
"""
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

BASE = Path('data/bronze/weather/seoul_hourly')
RAW = BASE / 'raw'
SEOUL = BASE / 'seoul'
MANIFEST = BASE / 'manifest.jsonl'
OUT = Path('data/silver/quality_checks/seoul_weather')
START = datetime(2021, 1, 1)
END = datetime(2024, 12, 31, 23)
STATION_IDS = {'108','110', *(str(i) for i in range(400,420)), '421','423','424','425','509','510','889'}
FEATURES = ('TA','PS','PA','HM','RN_HR1')
MISSING_MARKERS = {'', '-99', '-99.0', '-999', '-999.0', 'null', 'none', 'nan'}
SAMPLE_LIMIT = 50

def missing(value):
    if value is None: return True
    if isinstance(value, str) and value.strip().lower() in MISSING_MARKERS: return True
    if isinstance(value, (int, float)) and value in (-99, -999): return True
    return False

def write_csv(path, columns, rows):
    with path.open('w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)

def main():
    if not RAW.is_dir() or not SEOUL.is_dir():
        print('[오류] 서울 시간별 원본 폴더가 없습니다.')
        print('필요 경로:', RAW, '및', SEOUL)
        return
    OUT.mkdir(parents=True, exist_ok=True)
    manifest = {}
    manifest_duplicates = 0
    manifest_bad_lines = []
    if MANIFEST.exists():
        with MANIFEST.open(encoding='utf-8') as f:
            for no, line in enumerate(f, 1):
                try:
                    entry = json.loads(line)
                    tm = str(entry.get('requested_time_kst', ''))
                    if not re.fullmatch(r'\d{12}', tm):
                        manifest_bad_lines.append(no); continue
                    if tm in manifest: manifest_duplicates += 1
                    manifest[tm] = entry
                except (json.JSONDecodeError, TypeError):
                    manifest_bad_lines.append(no)

    stats = Counter()
    feature_counts = defaultdict(Counter)
    station_counts = Counter()
    yearly_stations = defaultdict(set)
    issue_counts = Counter()
    examples = defaultdict(list)
    missing_hours = []
    time = START
    total_hours = int((END - START).total_seconds() // 3600) + 1

    def issue(kind, tm, detail=''):
        issue_counts[kind] += 1
        if len(examples[kind]) < SAMPLE_LIMIT:
            examples[kind].append({'time_kst': tm, 'detail': str(detail)[:300]})

    while time <= END:
        tm = time.strftime('%Y%m%d%H%M')
        raw_path = RAW / f'aws_all_{tm}.json'
        seoul_path = SEOUL / f'seoul_{tm}.json'
        stats['expected_hours'] += 1
        if not raw_path.is_file():
            issue('raw_file_missing', tm)
        if not seoul_path.is_file():
            issue('seoul_file_missing', tm)
            missing_hours.append({'time_kst': tm, 'raw_exists': int(raw_path.is_file()), 'seoul_exists': 0})
        if not raw_path.is_file() or not seoul_path.is_file():
            time += timedelta(hours=1)
            continue
        stats['paired_file_hours'] += 1
        entry = manifest.get(tm)
        if entry is None:
            issue('manifest_entry_missing', tm)
        try:
            raw_bytes = raw_path.read_bytes()
            raw_rows = json.loads(raw_bytes)
            if not isinstance(raw_rows, list): raise ValueError('raw root is not list')
            if entry:
                if entry.get('sha256') != hashlib.sha256(raw_bytes).hexdigest():
                    issue('raw_sha256_mismatch', tm)
                if entry.get('raw_record_count') != len(raw_rows):
                    issue('raw_count_mismatch', tm)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            issue('raw_read_or_json_error', tm, exc)
            raw_rows = None
        try:
            seoul_rows = json.loads(seoul_path.read_text(encoding='utf-8'))
            if not isinstance(seoul_rows, list): raise ValueError('seoul root is not list')
            stats['seoul_records'] += len(seoul_rows)
            if entry and entry.get('seoul_record_count') != len(seoul_rows):
                issue('seoul_count_mismatch', tm)
            if not seoul_rows:
                issue('empty_seoul_hour', tm)
            seen = set()
            for row in seoul_rows:
                if not isinstance(row, dict):
                    issue('invalid_record_type', tm); continue
                sid = str(row.get('AWS_ID', ''))
                record_tm = str(row.get('TM', ''))
                if record_tm != tm:
                    issue('record_time_mismatch', tm, f'{sid}: {record_tm}')
                if sid not in STATION_IDS:
                    issue('unexpected_station', tm, sid)
                if sid in seen:
                    issue('duplicate_station_same_hour', tm, sid)
                seen.add(sid)
                station_counts[sid] += 1
                yearly_stations[time.year].add(sid)
                for field in FEATURES:
                    value = row.get(field)
                    if missing(value):
                        feature_counts[(sid,field)]['missing'] += 1
                    else:
                        try:
                            number = float(value)
                            if field == 'RN_HR1' and number < 0:
                                issue('negative_rain_not_missing', tm, f'{sid}: {value}')
                            feature_counts[(sid,field)]['valid_numeric'] += 1
                        except (TypeError, ValueError):
                            feature_counts[(sid,field)]['non_numeric'] += 1
                            issue('non_numeric_feature', tm, f'{sid} {field}={value}')
            if raw_rows is not None:
                raw_subset = [r for r in raw_rows if isinstance(r,dict) and str(r.get('AWS_ID')) in STATION_IDS and str(r.get('TM')) == tm]
                if len(raw_subset) != len(seoul_rows):
                    issue('raw_filtered_count_mismatch', tm, f'raw={len(raw_subset)}, seoul={len(seoul_rows)}')
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            issue('seoul_read_or_json_error', tm, exc)
        if stats['expected_hours'] % 1000 == 0:
            print(f"진행: {stats['expected_hours']:,}/{total_hours:,} 시간", flush=True)
        time += timedelta(hours=1)

    # Detect unexpected files outside the agreed 2021–2024 hourly window.
    for folder, prefix in [(RAW,'aws_all_'),(SEOUL,'seoul_')]:
        for p in folder.glob('*.json'):
            match = re.fullmatch(re.escape(prefix) + r'(\d{12})\.json', p.name)
            if not match:
                issue('unexpected_filename', '', str(p))
            else:
                t = match.group(1)
                if t < START.strftime('%Y%m%d%H%M') or t > END.strftime('%Y%m%d%H%M') or t[-2:] != '00':
                    issue('unexpected_hour_file', t, str(p))

    station_rows = []
    for sid in sorted(STATION_IDS | set(station_counts), key=lambda x: (not x.isdigit(), int(x) if x.isdigit() else x)):
        for field in FEATURES:
            c = feature_counts[(sid,field)]
            n = station_counts[sid]
            station_rows.append({
                'station_id':sid, 'variable':field, 'total_rows':n,
                'valid_numeric':c['valid_numeric'], 'missing':c['missing'],
                'non_numeric':c['non_numeric'],
                'missing_pct':round(c['missing']/n*100, 2) if n else None,
            })
    write_csv(OUT/'station_variable_quality.csv',
              ['station_id','variable','total_rows','valid_numeric','missing','non_numeric','missing_pct'],station_rows)
    write_csv(OUT/'missing_hours.csv', ['time_kst','raw_exists','seoul_exists'],missing_hours)
    with (OUT/'issue_samples.json').open('w', encoding='utf-8') as f:
        json.dump(dict(examples),f,ensure_ascii=False,indent=2)
    report = {
        'period_kst': {'start': START.isoformat(), 'end': END.isoformat()},
        'expected_hours': total_hours,
        'paired_file_hours':stats['paired_file_hours'],
        'seoul_records_read':stats['seoul_records'],
        'unique_observed_station_ids': len(station_counts),
        'manifest_present': MANIFEST.is_file(),
        'manifest_unique_entries': len(manifest),
        'manifest_duplicate_time_entries': manifest_duplicates,
        'manifest_unparseable_line_numbers_first_50':manifest_bad_lines[:50],
        'year_station_counts':{str(y):len(v) for y,v in sorted(yearly_stations.items())},
        'issue_counts':dict(sorted(issue_counts.items())),
        'note':'29 stations x 35064 hours is a reference upper bound, not a guaranteed completeness count. Source operations/absence must be checked.',
        'output_directory': str(OUT),
    }
    with (OUT/'quality_report.json').open('w',encoding='utf-8') as f:
        json.dump(report,f,ensure_ascii=False,indent=2)
    print('\n=== 서울 기상 Bronze 검증 결과 ===')
    print('예상 시간:', total_hours)
    print('원본+서울 필터 파일이 모두 있는 시간:',stats['paired_file_hours'])
    print('서울 관측 기록 수:',stats['seoul_records'])
    print('실제 출현 관측소 수:',len(station_counts))
    print('manifest:', '있음' if MANIFEST.is_file() else '없음')
    print('이상 항목:',dict(sorted(issue_counts.items())))
    print('결과 파일:',OUT/'quality_report.json')

if __name__ == '__main__':
    main()
