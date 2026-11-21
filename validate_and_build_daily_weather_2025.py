"""Validate all 2025 hourly weather Gold files and derive daily weather features.

Never changes hourly Gold or existing daily Gold / damage labels.

Run:
  python -u validate_and_build_daily_weather_2025.py
  python -u validate_and_build_daily_weather_2025.py --qa-only

Requires: duckdb (pip install duckdb)
"""
from __future__ import annotations

import argparse
import calendar
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit('duckdb가 없습니다: python -m pip install duckdb')

IN_DIR = Path('data/gold/weather/hourly/2025')
OUT_DIR = Path('data/gold/weather/daily_from_hourly/2025')


def quote(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def run():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input-dir', type=Path, default=IN_DIR)
    p.add_argument('--output-dir', type=Path, default=OUT_DIR)
    p.add_argument('--qa-only', action='store_true')
    p.add_argument('--memory-limit', default='4GB')
    p.add_argument('--threads', type=int, default=4)
    args = p.parse_args()

    files = [args.input_dir / f'hourly_2025_{m:02d}.parquet' for m in range(1, 13)]
    missing_files = [str(f) for f in files if not f.is_file()]
    if missing_files:
        sys.exit('필요한 파일이 없습니다:\n' + '\n'.join(missing_files))

    # Fail safely: no source files overwritten; do not overwrite derived files either.
    out = args.output_dir
    targets = [out / f'dong_daily_weather_2025_{m:02d}.parquet' for m in range(1,13)]
    report_path = out / 'qa' / 'hourly_full_year_2025.qa.json'
    if not args.qa_only:
        exists = [str(f) for f in targets if f.exists()]
        if exists:
            sys.exit('일별 결과 파일이 이미 있습니다. 덮어쓰지 않습니다:\n' + '\n'.join(exists))
    out.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{args.memory_limit}'")
    con.execute(f"SET threads={args.threads}")
    report = {'source':str(args.input_dir), 'year':2025, 'monthly':[], 'checks': {},
              'note':'Daily features are retrospective same-day summaries; define a prediction cutoff to avoid leakage.'}
    total_rows = 0
    total_rain_valid = 0
    warnings = []
    expected_dongs = None
    try:
        for month, file in enumerate(files, 1):
            print(f'[{month:02d}/12] 검증: {file}', flush=True)
            con.execute(f'CREATE OR REPLACE TEMP VIEW src AS SELECT * FROM read_parquet({quote(file)})')
            cols = {r[0] for r in con.execute('DESCRIBE src').fetchall()}
            required = {'admin_dong_code','observation_time_kst','rainfall_1h_mm',
                        'rain_1h','rain_3h','rain_6h','rain_12h','rain_24h',
                        'temperature_c','relative_humidity_pct','sea_level_pressure_hpa',
                        'rainfall_1h_mm_distance_m','rainfall_1h_mm_station_id'}
            absent = sorted(required-cols)
            if absent:
                raise RuntimeError(f'{file}: 필요한 컬럼 누락: {absent}')
            first = datetime(2025,month,1)
            end = datetime(2026,1,1) if month==12 else datetime(2025,month+1,1)
            n_days = (end-first).days
            if expected_dongs is None:
                expected_dongs = con.execute('SELECT COUNT(DISTINCT admin_dong_code) FROM src').fetchone()[0]
                if expected_dongs <= 0: raise RuntimeError('행정동이 없습니다')
                print(f'    기준 행정동 수: {expected_dongs:,}',flush=True)
            expected = expected_dongs * n_days * 24
            # One scan per month; NULLs remain NULL.
            row = con.execute('''
                SELECT COUNT(*) AS rows,
                       COUNT(DISTINCT (admin_dong_code,observation_time_kst)) AS unique_keys,
                       COUNT(DISTINCT admin_dong_code) AS dong_count,
                       MIN(observation_time_kst), MAX(observation_time_kst),
                       COUNT(DISTINCT observation_time_kst) AS distinct_hours,
                       COUNT(rain_1h), COUNT(rain_3h), COUNT(rain_6h), COUNT(rain_12h), COUNT(rain_24h),
                       COUNT(temperature_c), COUNT(relative_humidity_pct), COUNT(sea_level_pressure_hpa),
                       COUNT(rainfall_1h_mm_distance_m),
                       ROUND(quantile_cont(rainfall_1h_mm_distance_m,0.5),2),
                       ROUND(quantile_cont(rainfall_1h_mm_distance_m,0.95),2),
                       MAX(rainfall_1h_mm_distance_m),
                       COUNT(*) FILTER (WHERE rainfall_1h_mm IS DISTINCT FROM rain_1h),
                       COUNT(*) FILTER (WHERE rain_1h < 0 OR rain_3h < 0 OR rain_6h < 0 OR rain_12h < 0 OR rain_24h < 0),
                       COUNT(*) FILTER (WHERE rain_3h + 1e-8 < rain_1h OR rain_6h + 1e-8 < rain_3h
                                                   OR rain_12h + 1e-8 < rain_6h OR rain_24h + 1e-8 < rain_12h),
                       COUNT(*) FILTER (WHERE relative_humidity_pct < 0 OR relative_humidity_pct > 100),
                       COUNT(*) FILTER (WHERE rainfall_1h_mm_distance_m < 0),
                       COUNT(*) FILTER (WHERE rainfall_1h_mm IS NOT NULL AND rainfall_1h_mm_station_id IS NULL)
                FROM src
            ''').fetchone()
            (n, keys, dongs, mint, maxt, hours, n1,n3,n6,n12,n24,nta,nhm,nps,ndist,
             dist50,dist95,distmax,mismatch,negative,monotonic,invalid_hm,negative_dist,missing_station)=row
            errors=[]
            if n!=expected: errors.append(f'rows {n} != {expected}')
            if keys!=n: errors.append(f'duplicate keys {n-keys}')
            if dongs!=expected_dongs: errors.append(f'dongs {dongs} != {expected_dongs}')
            if hours!=n_days*24: errors.append(f'hours {hours} != {n_days*24}')
            if mint!=first or maxt!=end-timedelta(hours=1): errors.append(f'time range {mint}..{maxt}')
            if mismatch: errors.append(f'1h rainfall mismatch {mismatch}')
            if negative: errors.append(f'negative rain {negative}')
            if monotonic: errors.append(f'rolling-rain nonmonotonic {monotonic}')
            if invalid_hm: errors.append(f'humidity out of range {invalid_hm}')
            if negative_dist: errors.append(f'negative distance {negative_dist}')
            if missing_station: errors.append(f'missing rain station id {missing_station}')
            stats={'month':f'2025-{month:02d}', 'rows':n, 'expected_rows':expected,
                   'unique_keys':keys,'dongs':dongs,'distinct_hours':hours,
                   'nonmissing':{'rain_1h':n1,'rain_3h':n3,'rain_6h':n6,'rain_12h':n12,'rain_24h':n24,
                                 'temperature_c':nta,'relative_humidity_pct':nhm,'sea_level_pressure_hpa':nps},
                   'missing_pct':{k:round(100*(n-v)/n,3) for k,v in
                       [('rain_1h',n1),('rain_3h',n3),('rain_6h',n6),('rain_12h',n12),('rain_24h',n24)]},
                   'rain_station_distance_m':{'known':ndist,'median':dist50,'p95':dist95,'max':distmax},
                   'rain_1h_mismatch':mismatch, 'rolling_rain_monotonic_violations':monotonic,
                   'errors':errors}
            report['monthly'].append(stats)
            total_rows+=n
            total_rain_valid+=n24
            if dist95 is not None and dist95 > 30000:
                warnings.append(f'{month:02d}월 강수 관측소 거리 p95={dist95}m, 검토 권장 (기준값은 예시)')
            print(f'    행 {n:,} / 고유키 {keys:,} / rain_24h 결측률 {stats["missing_pct"]["rain_24h"]}%'
                  f' / 강수 관측소 거리 p95 {dist95}m / 오류 {len(errors)}', flush=True)
            if errors:
                raise RuntimeError(f'{month:02d}월 QA 실패: '+', '.join(errors))
            if args.qa_only:
                continue

            # Never copy labels into weather features. Keep precise time-derived summaries.
            # 24-hour daily sum is NULL unless all 24 hourly rain observations are present.
            dest=targets[month-1]
            staging=dest.with_suffix('.parquet.tmp')
            try:
                con.execute(f'''
                    COPY (
                        SELECT 2025 AS year,
                               admin_dong_code,
                               CAST(observation_time_kst AS DATE) AS observation_date,
                               MIN(admin_dong_name) AS admin_dong_name,
                               MIN(boundary_snapshot) AS boundary_snapshot,
                               COUNT(*) AS hours_total,
                               COUNT(temperature_c) AS temperature_valid_hours,
                               AVG(temperature_c) AS temperature_mean_c,
                               MAX(temperature_c) AS temperature_max_c,
                               MIN(temperature_c) AS temperature_min_c,
                               COUNT(relative_humidity_pct) AS humidity_valid_hours,
                               AVG(relative_humidity_pct) AS humidity_mean_pct,
                               COUNT(sea_level_pressure_hpa) AS pressure_valid_hours,
                               AVG(sea_level_pressure_hpa) AS pressure_mean_hpa,
                               COUNT(rain_1h) AS rain_valid_hours,
                               CASE WHEN COUNT(rain_1h)=24 THEN SUM(rain_1h) END AS rain_daily_total_mm,
                               MAX(rain_1h) AS rain_1h_max_mm,
                               MAX(rain_3h) AS rain_3h_max_mm,
                               MAX(rain_6h) AS rain_6h_max_mm,
                               MAX(rain_12h) AS rain_12h_max_mm,
                               MAX(rain_24h) AS rain_24h_max_mm,
                               COUNT(rain_3h) AS rain_3h_valid_hours_of_day,
                               COUNT(rain_6h) AS rain_6h_valid_hours_of_day,
                               COUNT(rain_12h) AS rain_12h_valid_hours_of_day,
                               COUNT(rain_24h) AS rain_24h_valid_hours_of_day,
                               COUNT(DISTINCT rainfall_1h_mm_station_id) AS rain_station_count_in_day,
                               MAX(rainfall_1h_mm_distance_m) AS rain_station_max_distance_m
                        FROM src
                        GROUP BY admin_dong_code, CAST(observation_time_kst AS DATE)
                        ORDER BY admin_dong_code, observation_date
                    ) TO {quote(staging)} (FORMAT PARQUET, COMPRESSION ZSTD)
                ''')
                daily_rows, daily_keys, bad_hour_count=con.execute(f'''
                    SELECT COUNT(*), COUNT(DISTINCT (admin_dong_code,observation_date)),
                           COUNT(*) FILTER (WHERE hours_total != 24)
                    FROM read_parquet({quote(staging)})
                ''').fetchone()
                if daily_rows!=expected_dongs*n_days or daily_keys!=daily_rows or bad_hour_count:
                    raise RuntimeError(f'일별 결과 검증 실패: rows={daily_rows} keys={daily_keys} != {expected_dongs*n_days}, bad_hours={bad_hour_count}')
                staging.replace(dest)
                print(f'    일별 Feature 저장: {dest} ({daily_rows:,}행)',flush=True)
            finally:
                if staging.exists(): staging.unlink()

        report['checks']['expected_total_rows']=expected_dongs*365*24
        report['checks']['total_rows']=total_rows
        report['checks']['total_rain_24h_nonmissing']=total_rain_valid
        report['checks']['rain_24h_missing_pct']=round(100*(total_rows-total_rain_valid)/total_rows,3)
        report['checks']['all_months_passed']=len(report['monthly'])==12 and total_rows==expected_dongs*365*24
        report['warnings']=warnings
        report_path.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
        if not report['checks']['all_months_passed']:
            raise RuntimeError('연간 행 수 불일치')
        print('\n완료: Hourly Gold 전체 검증 통과',flush=True)
        print('QA 결과:',report_path,flush=True)
        if not args.qa_only:
            print('일별 Feature 12개 파일:',out,flush=True)
            print('주의: 이 Feature는 당일 종료 후 계산되는 사후 요약값입니다. 사전 예측 모델에는 그대로 사용하지 마세요.',flush=True)
    finally:
        con.close()

if __name__=='__main__':
    run()
