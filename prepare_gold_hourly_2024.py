"""Create 2024 hourly dong weather Gold, monthly, without touching Daily Gold.

Usage:
  python -u prepare_gold_hourly_2024.py --month 2024-07
  python -u prepare_gold_hourly_2024.py --all

Requires: pip install duckdb
Input column names follow prepare_gold_hourly_sample.py exactly.
"""
from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

DEFAULT_WEATHER = Path('data/silver/weather/station_hourly/nationwide_station_hourly_2024.csv')
DEFAULT_MAPPING = Path('data/silver/mappings/nationwide_aws_dong/dong_station_distance_2024.csv')
DEFAULT_OUT = Path('data/gold/weather/hourly/2024')


def sql_path(p: Path) -> str:
    return "'" + str(p).replace("'", "''") + "'"


def run_month(con, month: int, mapping_count: int, dongs: int, weather: Path, outdir: Path,
              max_station_distance_m: float | None = None):
    first = datetime(2024, month, 1)
    end = datetime(2025, 1, 1) if month == 12 else datetime(2024, month + 1, 1)
    history = first - timedelta(hours=23)
    days = (end-first).days
    output = outdir / f'hourly_2024_{month:02d}.parquet'
    qa_path = outdir / 'qa' / f'hourly_2024_{month:02d}.qa.json'
    if output.exists():
        raise FileExistsError(f'{output} already exists (safe by default; remove manually to regenerate)')
    print(f'\n=== {first:%Y-%m} / {days} days ===', flush=True)

    # Read relevant raw observations once per month, including prior 23 hours.
    con.execute('DROP TABLE IF EXISTS hourly')
    con.execute(f"""
        CREATE TEMP TABLE hourly AS
        SELECT TRIM(station_id) AS station_id,
               TRY_CAST(observation_time_kst AS TIMESTAMP) AS t,
               CASE WHEN ta_qc='ok' THEN TRY_CAST(ta_c AS DOUBLE) END AS ta,
               CASE WHEN hm_qc='ok' THEN TRY_CAST(hm_pct AS DOUBLE) END AS hm,
               CASE WHEN ps_qc='ok' THEN TRY_CAST(ps_hpa AS DOUBLE) END AS ps,
               CASE WHEN rn_hr1_qc='ok' AND TRY_CAST(rn_hr1_mm AS DOUBLE)>=0
                    THEN TRY_CAST(rn_hr1_mm AS DOUBLE) END AS rn
        FROM read_csv({sql_path(weather)}, header=true, all_varchar=true)
        WHERE observation_time_kst >= '{history:%Y-%m-%d %H:%M:%S}'
          AND observation_time_kst < '{end:%Y-%m-%d %H:%M:%S}'
    """)
    observed, unique_obs = con.execute('''
        SELECT COUNT(*), COUNT(DISTINCT (station_id,t)) FROM hourly
    ''').fetchone()
    if observed != unique_obs:
        raise RuntimeError(f'Duplicate station-time keys in raw data: {observed-unique_obs:,}')
    if observed == 0:
        raise RuntimeError(f'No raw observations for {first:%Y-%m}; check full-year source coverage')
    covered_days = con.execute(f"""
        SELECT COUNT(DISTINCT CAST(t AS DATE)) FROM hourly
        WHERE t >= TIMESTAMP '{first:%Y-%m-%d %H:%M:%S}'
          AND t < TIMESTAMP '{end:%Y-%m-%d %H:%M:%S}'
    """).fetchone()[0]
    if covered_days != days:
        raise RuntimeError(f'Raw weather days missing: {covered_days}/{days} for {first:%Y-%m}. Stop to prevent empty Gold days.')
    print(f'Raw observation rows (including 23h history): {observed:,}; days covered: {covered_days}/{days}', flush=True)

    # Build a continuous hourly grid for each mapped station. Missing observations
    # stay NULL and are never treated as zero rainfall.
    con.execute('DROP TABLE IF EXISTS rain_windows')
    con.execute(f"""
        CREATE TEMP TABLE rain_windows AS
        WITH grid AS (
            SELECT s.station_id, series.t,
                   h.rn
            FROM (SELECT DISTINCT station_id FROM mapping) s
            CROSS JOIN generate_series(
                TIMESTAMP '{history:%Y-%m-%d %H:%M:%S}',
                TIMESTAMP '{(end-timedelta(hours=1)):%Y-%m-%d %H:%M:%S}',
                INTERVAL 1 HOUR
            ) AS series(t)
            LEFT JOIN hourly h ON s.station_id=h.station_id AND series.t=h.t
        ), roll AS (
            SELECT station_id, t, rn,
                   COUNT(rn) OVER (PARTITION BY station_id ORDER BY t ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS n3,
                   SUM(rn) OVER (PARTITION BY station_id ORDER BY t ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s3,
                   COUNT(rn) OVER (PARTITION BY station_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS n6,
                   SUM(rn) OVER (PARTITION BY station_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS s6,
                   COUNT(rn) OVER (PARTITION BY station_id ORDER BY t ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS n12,
                   SUM(rn) OVER (PARTITION BY station_id ORDER BY t ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS s12,
                   COUNT(rn) OVER (PARTITION BY station_id ORDER BY t ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS n24,
                   SUM(rn) OVER (PARTITION BY station_id ORDER BY t ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS s24
            FROM grid
        )
        SELECT station_id, t, rn AS rain_1h,
               CASE WHEN n3=3 THEN s3 END AS rain_3h,
               CASE WHEN n6=6 THEN s6 END AS rain_6h,
               CASE WHEN n12=12 THEN s12 END AS rain_12h,
               CASE WHEN n24=24 THEN s24 END AS rain_24h,
               CASE WHEN rn IS NOT NULL THEN 1 ELSE 0 END AS rain_1h_valid_hours,
               n3 AS rain_3h_valid_hours,
               n6 AS rain_6h_valid_hours,
               n12 AS rain_12h_valid_hours,
               n24 AS rain_24h_valid_hours
        FROM roll
        WHERE t >= TIMESTAMP '{first:%Y-%m-%d %H:%M:%S}'
    """)

    # Retain original feature-specific nearest-valid-station logic unchanged.
    # Rain history will be joined only AFTER selecting rainfall_1h_mm_station_id.
    features = {
        'temperature_c': 'ta',
        'relative_humidity_pct': 'hm',
        'sea_level_pressure_hpa': 'ps',
        'rainfall_1h_mm': 'rn',
    }
    selectors = []
    for output_name, source_name in features.items():
        cond = f'WHERE h.{source_name} IS NOT NULL'
        selectors.extend([
            f'arg_min(h.{source_name}, m.distance_rank) FILTER ({cond}) AS {output_name}',
            f'arg_min(m.station_id, m.distance_rank) FILTER ({cond}) AS {output_name}_station_id',
            f'arg_min(m.distance_rank, m.distance_rank) FILTER ({cond}) AS {output_name}_station_rank',
            f'arg_min(m.distance_m, m.distance_rank) FILTER ({cond}) AS {output_name}_distance_m',
        ])
    select_sql = ',\n'.join(selectors)

    outdir.mkdir(parents=True, exist_ok=True)
    qa_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f'hourly_{month:02d}_', dir=outdir) as td:
        tdir = Path(td)
        for day in range(1, days + 1):
            day_start = first + timedelta(days=day-1)
            day_end = day_start + timedelta(days=1)
            day_path = tdir / f'day_{day:02d}.parquet'
            print(f'  {day_start:%Y-%m-%d} building ...', flush=True)
            con.execute(f"""
                COPY (
                    WITH selected AS (
                        SELECT 2024 AS year,
                               m.admin_dong_code, m.admin_dong_name, m.boundary_snapshot,
                               t.observation_time_kst,
                               {select_sql}
                        FROM mapping m
                        CROSS JOIN generate_series(
                            TIMESTAMP '{day_start:%Y-%m-%d %H:%M:%S}',
                            TIMESTAMP '{(day_end-timedelta(hours=1)):%Y-%m-%d %H:%M:%S}',
                            INTERVAL 1 HOUR
                        ) AS t(observation_time_kst)
                        LEFT JOIN hourly h ON m.station_id=h.station_id
                                         AND t.observation_time_kst=h.t
                        GROUP BY 1,2,3,4,5
                    )
                    SELECT g.*,
                           rw.rain_1h, rw.rain_3h, rw.rain_6h,
                           rw.rain_12h, rw.rain_24h,
                           rw.rain_1h_valid_hours, rw.rain_3h_valid_hours,
                           rw.rain_6h_valid_hours, rw.rain_12h_valid_hours,
                           rw.rain_24h_valid_hours
                    FROM selected g
                    LEFT JOIN rain_windows rw
                      ON g.rainfall_1h_mm_station_id=rw.station_id
                     AND g.observation_time_kst=rw.t
                    ORDER BY admin_dong_code, observation_time_kst
                ) TO {sql_path(day_path)} (FORMAT PARQUET, COMPRESSION ZSTD)
            """)

        # Write into a temporary destination first: never leave a partial final month.
        staged = tdir / 'complete.parquet'
        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet({sql_path(str(tdir / 'day_*.parquet'))})
                ORDER BY admin_dong_code, observation_time_kst
            ) TO {sql_path(staged)} (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        source = sql_path(staged)
        row = con.execute(f"""
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT (admin_dong_code, observation_time_kst)) AS unique_keys,
                   COUNT(DISTINCT admin_dong_code) AS dongs,
                   SUM(CASE WHEN rainfall_1h_mm IS DISTINCT FROM rain_1h THEN 1 ELSE 0 END) AS rain_mismatches,
                   MIN(observation_time_kst), MAX(observation_time_kst),
                   COUNT(temperature_c), COUNT(relative_humidity_pct),
                   COUNT(sea_level_pressure_hpa), COUNT(rainfall_1h_mm),
                   COUNT(rain_3h), COUNT(rain_6h), COUNT(rain_12h), COUNT(rain_24h),
                   MAX(rainfall_1h_mm_distance_m),
                   quantile_cont(rainfall_1h_mm_distance_m, 0.95)
            FROM read_parquet({source})
        """).fetchone()
        expected = dongs * days * 24
        if row[0] != expected or row[1] != expected or row[2] != dongs:
            raise RuntimeError(f'Row/key validation failed for {first:%Y-%m}: {row[:3]}, expected {expected}')
        if row[3] != 0:
            raise RuntimeError(f'1h rainfall mismatch for {first:%Y-%m}: {row[3]:,}')
        if max_station_distance_m is not None and row[14] is not None and row[14] > max_station_distance_m:
            raise RuntimeError(f'Max selected rain station distance {row[14]}m exceeds limit')
        qa = {
            'month': f'{first:%Y-%m}', 'rows': row[0], 'expected_rows': expected,
            'unique_dong_time_keys': row[1], 'dongs': row[2],
            'mapping_rows': mapping_count, 'raw_station_observations': observed,
            'rain_1h_mismatches': row[3],
            'min_time': str(row[4]), 'max_time': str(row[5]),
            'nonmissing': dict(zip([
                'temperature_c','relative_humidity_pct','sea_level_pressure_hpa',
                'rainfall_1h_mm','rain_3h','rain_6h','rain_12h','rain_24h'
            ], row[6:14])),
            'rain_station_max_distance_m': row[14],
            'rain_station_p95_distance_m': row[15],
            'rain_window_rule': 'Strictly complete hourly observations; missing is NULL, never zero',
            'rain_station_rule': 'Same station selected for rainfall_1h_mm; no station switching for accumulations',
            'rain_time_assumption': 'observation_time_kst marks end of preceding 1h accumulation; confirm source definition',
            'data_limitation': 'At the beginning of 2024, 2023 history is unavailable from the 2024 input CSV',
        }
        if row[4] != first or row[5] != end-timedelta(hours=1):
            raise RuntimeError(f'Hour range mismatch in {first:%Y-%m}: {row[4]}..{row[5]}')
        staged.replace(output)
        qa_path.write_text(json.dumps(qa, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'  Saved: {output} ({expected:,} rows)', flush=True)
    print(f'  QA: {qa_path}', flush=True)
    print(f'  Nonmissing rain_24h: {row[13]:,}/{expected:,}; 1h mismatch: {row[3]}', flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--weather', type=Path, default=DEFAULT_WEATHER)
    ap.add_argument('--mapping', type=Path, default=DEFAULT_MAPPING)
    ap.add_argument('--outdir', type=Path, default=DEFAULT_OUT)
    choice = ap.add_mutually_exclusive_group(required=True)
    choice.add_argument('--month', help='YYYY-MM (2024 only)')
    choice.add_argument('--all', action='store_true')
    ap.add_argument('--max-station-distance-m', type=float, default=None,
                    help='Optional hard fail for excessive rain station distance')
    ap.add_argument('--memory-limit', default='4GB')
    ap.add_argument('--threads', type=int, default=4)
    args = ap.parse_args()
    for p in (args.weather, args.mapping):
        if not p.is_file():
            ap.error(f'Input not found: {p}')
    if args.month:
        try:
            year, month = map(int, args.month.split('-'))
        except ValueError:
            ap.error('Month format must be YYYY-MM')
        if year != 2024 or month not in range(1,13):
            ap.error('Month must be 2024-01 ... 2024-12')
        months = [month]
    else:
        months = list(range(1,13))
    args.outdir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{args.memory_limit}'")
    con.execute(f'SET threads={args.threads}')
    try:
        con.execute(f"""
            CREATE TEMP TABLE mapping AS
            SELECT TRIM(admin_dong_code) AS admin_dong_code,
                   admin_dong_name, TRIM(station_id) AS station_id,
                   CAST(distance_rank AS INTEGER) AS distance_rank,
                   CAST(distance_m AS DOUBLE) AS distance_m,
                   boundary_snapshot
            FROM read_csv({sql_path(args.mapping)}, header=true, all_varchar=true)
            WHERE year='2024' AND TRY_CAST(distance_rank AS INTEGER) BETWEEN 1 AND 30
        """)
        count, dongs, unique_ranks = con.execute('''
            SELECT COUNT(*), COUNT(DISTINCT admin_dong_code),
                   COUNT(DISTINCT (admin_dong_code,distance_rank)) FROM mapping
        ''').fetchone()
        print(f'Mapping: {count:,} rows, {dongs:,} dongs', flush=True)
        if count != unique_ranks or dongs != 3559:
            raise RuntimeError('Mapping validation failed: duplicate ranks or unexpected dong count')
        for month in months:
            run_month(con, month, count, dongs, args.weather, args.outdir, args.max_station_distance_m)
    finally:
        con.close()

if __name__ == '__main__':
    main()
