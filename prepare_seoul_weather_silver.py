"""Seoul AWS hourly Bronze -> station-level Silver CSV (no spatial interpolation).

Usage (from project root):
    python prepare_seoul_weather_silver.py

Optional smoke test:
    python prepare_seoul_weather_silver.py --start 202101010000 --end 202101012300

Only reads data/bronze; outputs data/silver/weather/station_hourly and QC report.
No API key, credentials or full raw nationwide payloads are exported.
"""
import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

SOURCE = Path('data/bronze/weather/seoul_hourly/seoul')
OUT = Path('data/silver/weather/station_hourly')
DEFAULT_START = '202101010000'
DEFAULT_END = '202412312300'
STATIONS = {'108', '110', *(str(i) for i in range(400, 420)), '421', '423', '424', '425', '509', '510', '889'}
FIELDS = ('TA', 'HM', 'PA', 'PS', 'RN_HR1', 'RN_DAY', 'WS', 'WD')
MISSING_NUMBERS = {-99.0, -999.0}
MISSING_TEXT = {'', 'nan', 'none', 'null'}

CSV_FIELDS = [
    'source_dataset', 'source_record_id', 'bronze_file', 'source_row_index',
    'station_id', 'observed_at_kst', 'observation_timezone',
    'temperature_c', 'relative_humidity_pct',
    'station_pressure_hpa', 'sea_level_pressure_hpa',
    'rainfall_1h_mm', 'rainfall_day_cumulative_mm',
    'wind_speed_ms', 'wind_direction_raw',
    *[f'raw_{name}' for name in FIELDS],
    *[f'{name}_qc' for name in FIELDS],
    'qc_status', 'processing_version',
]
MAPPING = {
    'TA': 'temperature_c', 'HM': 'relative_humidity_pct',
    'PA': 'station_pressure_hpa', 'PS': 'sea_level_pressure_hpa',
    'RN_HR1': 'rainfall_1h_mm', 'RN_DAY': 'rainfall_day_cumulative_mm',
    'WS': 'wind_speed_ms', 'WD': 'wind_direction_raw',
}


def parse_time(s):
    if not re.fullmatch(r'\d{12}', s):
        raise ValueError('시간은 YYYYMMDDHHMM 형식이어야 합니다: ' + s)
    d = datetime.strptime(s, '%Y%m%d%H%M')
    if d.minute != 0:
        raise ValueError('정시 자료만 처리합니다: ' + s)
    return d


def normalize(value, field):
    """Returns (number or None, qc). Never converts missing into 0."""
    if value is None or (isinstance(value, str) and value.strip().lower() in MISSING_TEXT):
        return None, 'missing'
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None, 'non_numeric'
    if not math.isfinite(v):
        return None, 'non_finite'
    if v in MISSING_NUMBERS:
        return None, 'source_missing_code'
    if field in ('RN_HR1', 'RN_DAY', 'WS') and v < 0:
        return None, 'invalid_negative'
    if field == 'HM' and not (0 <= v <= 100):
        return None, 'invalid_range'
    # Other numeric values are preserved, pending source-specific QC bounds.
    return v, 'ok'


def write_json(path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding='utf-8')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', default=DEFAULT_START)
    parser.add_argument('--end', default=DEFAULT_END)
    args = parser.parse_args()
    start, end = parse_time(args.start), parse_time(args.end)
    if end < start:
        parser.error('--end는 --start 이상이어야 합니다.')
    if not SOURCE.is_dir():
        raise SystemExit(f'[오류] 서울 원본 폴더가 없습니다: {SOURCE}')

    OUT.mkdir(parents=True, exist_ok=True)
    writers = {}
    handles = {}
    temp_paths = {}
    stats = Counter()
    qc_counts = defaultdict(Counter)
    year_counts = Counter()
    issue_samples = []
    missing_hours = []
    processed_years = set()

    def issue(code, tm, info):
        stats[code] += 1
        if len(issue_samples) < 100:
            issue_samples.append({'issue': code, 'time_kst': tm, 'detail': str(info)[:200]})

    current = start
    try:
        while current <= end:
            tm = current.strftime('%Y%m%d%H%M')
            path = SOURCE / f'seoul_{tm}.json'
            stats['expected_hours'] += 1
            if not path.is_file():
                missing_hours.append(tm)
                issue('missing_file', tm, path)
                current += timedelta(hours=1)
                continue
            try:
                records = json.loads(path.read_text(encoding='utf-8'))
                if not isinstance(records, list):
                    raise ValueError('JSON 최상위 구조가 list가 아닙니다')
            except (OSError, UnicodeError, ValueError) as exc:
                issue('invalid_json_file', tm, exc)
                current += timedelta(hours=1)
                continue
            stats['readable_hours'] += 1
            seen = set()
            for idx, record in enumerate(records):
                stats['source_records'] += 1
                if not isinstance(record, dict):
                    issue('invalid_record', tm, idx)
                    continue
                sid = str(record.get('AWS_ID', '')).strip()
                rtm = str(record.get('TM', '')).strip()
                if sid not in STATIONS or rtm != tm:
                    issue('station_or_time_mismatch', tm, f'index={idx}, sid={sid}, tm={rtm}')
                    continue
                if sid in seen:
                    issue('duplicate_station_time', tm, sid)
                    continue
                seen.add(sid)
                year = current.year
                if year not in writers:
                    outpath = OUT / f'seoul_station_hourly_{year}.csv'
                    tmppath = OUT / f'.seoul_station_hourly_{year}.csv.tmp'
                    handle = tmppath.open('w', newline='', encoding='utf-8-sig')
                    handles[year] = handle
                    temp_paths[year] = (tmppath, outpath)
                    writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
                    writer.writeheader()
                    writers[year] = writer
                row = {
                    'source_dataset': 'kma_aws_hourly',
                    'source_record_id': f'{tm}_{sid}',
                    'bronze_file': path.as_posix(),
                    'source_row_index': idx,
                    'station_id': sid,
                    'observed_at_kst': current.strftime('%Y-%m-%dT%H:%M:00+09:00'),
                    'observation_timezone': 'Asia/Seoul',
                    'processing_version': 'silver_station_hourly_v1',
                }
                flags = []
                for field in FIELDS:
                    num, qc = normalize(record.get(field), field)
                    row[MAPPING[field]] = num if num is not None else ''
                    # Retain raw value separately, including negatives/missing codes
                    raw = record.get(field)
                    row[f'raw_{field}'] = '' if raw is None else str(raw)
                    row[f'{field}_qc'] = qc
                    qc_counts[field][qc] += 1
                    if qc != 'ok':
                        flags.append(qc)
                row['qc_status'] = 'has_invalid_value' if any(x not in ('missing', 'source_missing_code') for x in flags) else ('has_missing_value' if flags else 'ok')
                writers[year].writerow(row)
                year_counts[year] += 1
                stats['silver_records'] += 1
            if stats['expected_hours'] % 1000 == 0:
                print(f"진행: {stats['expected_hours']:,}시간 / {stats['silver_records']:,}행", flush=True)
            current += timedelta(hours=1)
    finally:
        for handle in handles.values():
            handle.close()

    qc_path = OUT / 'variable_qc_counts.csv'
    with qc_path.open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['source_variable', 'qc_status', 'record_count'])
        for field in FIELDS:
            for qc, count in sorted(qc_counts[field].items()):
                writer.writerow([field, qc, count])

    report = {
        'processing_version': 'silver_station_hourly_v1',
        'input_root': SOURCE.as_posix(),
        'period_kst': {'start': args.start, 'end': args.end},
        'expected_hours': stats['expected_hours'],
        'readable_hours': stats['readable_hours'],
        'source_records': stats['source_records'],
        'silver_records': stats['silver_records'],
        'yearly_records': dict(sorted(year_counts.items())),
        'issues': {k:v for k,v in sorted(stats.items()) if k not in ('expected_hours','readable_hours','source_records','silver_records')},
        'qc_counts': {name:dict(qc_counts[name]) for name in FIELDS},
        'missing_hours_sample': missing_hours[:50],
        'issue_samples': issue_samples,
        'notes': [
            '관측소 원천 자료 정리 단계이며 행정동 배정·공간 보간은 하지 않았음.',
            'RN_HR1은 원천 시간강수량으로 사용하되 정확한 종료시각·공개시각은 별도 명세 검증 필요.',
            'RN_DAY는 당일 누적강수량이며 RN_HR1에 더하지 않음.',
            '-99/-999 및 원인 미확인 음수 강수는 결측 처리하고 원본값과 QC를 유지.',
            '관측소 행이 없는 시각은 임의로 0으로 생성하지 않았음.',
            'AWS 관측소 운영이력 및 기압 관측 가능 관측소는 추가 확인 필요.',
        ],
    }
    write_json(OUT / 'silver_quality_report.json', report)
    critical = ['missing_file','invalid_json_file','invalid_record','station_or_time_mismatch','duplicate_station_time']
    if any(stats[x] for x in critical):
        print('[확인 필요] 원본 구조 오류/누락이 있어 연도별 최종 CSV로 확정하지 않았습니다.')
        print('상세:', OUT / 'silver_quality_report.json')
        print('임시 CSV:', [str(p[0]) for p in temp_paths.values()])
        return 1
    for temp, dest in temp_paths.values():
        temp.replace(dest)
    print('=== Silver 관측소 시간자료 생성 완료 ===')
    print('대상 시간:', stats['expected_hours'])
    print('출력 행:', stats['silver_records'])
    print('연도별 행:', dict(sorted(year_counts.items())))
    print('결과 폴더:', OUT)
    print('검증 보고서:', OUT / 'silver_quality_report.json')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
