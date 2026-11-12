"""Inspect negative RN_HR1 values in Seoul hourly Bronze; never edits originals."""
import csv
import json
from collections import Counter
from pathlib import Path

SOURCE = Path('data/bronze/weather/seoul_hourly/seoul')
OUTPUT = Path('data/silver/quality_checks/seoul_weather/negative_rain_details.csv')
FIELDS = ['TM','AWS_ID','RN_HR1','RN_DAY','TA','HM','PA','PS','bronze_file']


def main():
    if not SOURCE.is_dir():
        raise SystemExit(f'원본 폴더를 찾지 못했습니다: {SOURCE}')
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    counts = Counter()
    station_counts = Counter()
    total = 0
    with OUTPUT.open('w', newline='', encoding='utf-8-sig') as output:
        writer = csv.DictWriter(output, fieldnames=FIELDS)
        writer.writeheader()
        for path in sorted(SOURCE.glob('seoul_*.json')):
            with path.open(encoding='utf-8') as f:
                rows = json.load(f)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                value = row.get('RN_HR1')
                if value is None or str(value).strip() == '':
                    continue
                try:
                    v = float(value)
                except (ValueError, TypeError):
                    continue
                if v < 0 and v not in (-99, -999):
                    writer.writerow({**{k:row.get(k) for k in FIELDS if k != 'bronze_file'}, 'bronze_file':str(path)})
                    total += 1
                    counts[str(value)] += 1
                    station_counts[str(row.get('AWS_ID'))] += 1
    print('=== 음수 강수량 확인 ===')
    print('대상 건수:', total)
    print('음수 값별 건수:', dict(counts.most_common()))
    print('관측소별 건수:', dict(station_counts.most_common()))
    print('상세 CSV:', OUTPUT)

if __name__ == '__main__':
    main()
