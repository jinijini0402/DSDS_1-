"""Join observed provisional property damage labels to 2024-25 daily weather features.

Creates diagnostic observed-label-only dataset; NOT a supervised training dataset.
Does NOT impute absent damage sources to zero and does NOT assume monetary units.
Usage: python -u join_property_daily_weather_2024_2025.py
"""
from pathlib import Path
import json
import duckdb

BASE = Path('data')

# 복구된 사유·공공시설 통합 Silver
LABEL = BASE / 'silver/labels/rain_damage/rain_property_sources_daily_recovered_2025_PROVISIONAL.parquet'

# 기존 시간별 기상자료 기반 일별 Gold
WEATHER = BASE / 'gold/weather/daily_from_hourly'

# 기존 Gold를 보존하고 새 파일 생성
OUT = BASE / 'gold/model_inputs/property_damage_observed_daily_2024_2025_recovered_PROVISIONAL.parquet'

QA = OUT.with_suffix('.qa.json')

def sql_path(p):
    return "'" + str(p).replace("'", "''") + "'"

def main():
    files = [WEATHER/str(y)/f'dong_daily_weather_{y}_{m:02d}.parquet'
             for y in (2024, 2025) for m in range(1,13)]
    missing = [str(p) for p in [LABEL, *files] if not p.is_file()]
    if missing:
        raise FileNotFoundError('입력 파일 없음:\n'+'\n'.join(missing))
    if OUT.exists() or QA.exists():
        raise FileExistsError(f'결과가 이미 있습니다. 기존 파일 보존: {OUT} / {QA}')
    OUT.parent.mkdir(parents=True, exist_ok=True)
    con=duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute('SET threads=4')
    try:
        wglob = str(WEATHER/'*'/'dong_daily_weather_*.parquet')
        con.execute(f"CREATE VIEW weather AS SELECT * FROM read_parquet({sql_path(wglob)}) WHERE year IN (2024,2025)")
        con.execute(f"CREATE VIEW labels AS SELECT * FROM read_parquet({sql_path(LABEL)}) WHERE year IN (2024,2025)")
        wcols = {r[0] for r in con.execute('DESCRIBE weather').fetchall()}
        lcols = {r[0] for r in con.execute('DESCRIBE labels').fetchall()}
        needw = {'year','admin_dong_code','observation_date'}
        needl = {'year','admin_dong_code','day','private_positive_observed',
                 'private_known_positive_amount_raw_unit','public_y_observed',
                 'public_known_positive_amount_raw_unit','public_amount_complete_y',
                 'observation_sources'}
        if needw-wcols or needl-lcols:
            raise RuntimeError(f'필수 컬럼 없음: weather={sorted(needw-wcols)}, labels={sorted(needl-lcols)}')
        n_weather, dup_weather = con.execute('''
            SELECT COUNT(*), COUNT(*)-COUNT(DISTINCT (year,admin_dong_code,observation_date)) FROM weather
        ''').fetchone()
        n_label, dup_label = con.execute('''
            SELECT COUNT(*), COUNT(*)-COUNT(DISTINCT (year,admin_dong_code,day)) FROM labels
        ''').fetchone()
        if dup_weather or dup_label:
            raise RuntimeError(f'키 중복! weather={dup_weather}, labels={dup_label}')
        con.execute('''
            CREATE VIEW joined AS
            SELECT l.year AS label_year, l.admin_dong_code AS label_dong_code,
                   l.day AS label_day, l.observation_sources,
                   l.private_positive_observed, l.public_y_observed,
                   l.public_amount_complete_y,
                   l.private_known_positive_amount_raw_unit AS private_damage_raw_unit,
                   l.public_known_positive_amount_raw_unit AS public_damage_raw_unit,
                   CASE
                     WHEN l.private_known_positive_amount_raw_unit IS NOT NULL
                       AND l.public_known_positive_amount_raw_unit IS NOT NULL
                       AND l.public_amount_complete_y = 'Y'
                     THEN l.private_known_positive_amount_raw_unit + l.public_known_positive_amount_raw_unit
                   END AS observed_component_sum_raw_unit,
                   CASE WHEN w.year IS NOT NULL THEN 1 ELSE 0 END AS weather_joined,
                   w.* EXCLUDE (year,admin_dong_code,observation_date)
            FROM labels l
            LEFT JOIN weather w ON l.year=w.year
              AND l.admin_dong_code=w.admin_dong_code
              AND l.day=w.observation_date
        ''')
        summary = con.execute('''
            SELECT COUNT(*) rows, COUNT(*) FILTER (WHERE weather_joined=1) weather_matched,
                COUNT(*) FILTER (WHERE weather_joined=0) weather_unmatched,
                COUNT(private_damage_raw_unit) private_known,
                COUNT(public_damage_raw_unit) public_known,
                COUNT(observed_component_sum_raw_unit) total_strict_known,
                COUNT(*) FILTER (WHERE public_y_observed IS NULL) public_not_observed,
                COUNT(*) FILTER (WHERE private_positive_observed IS NULL) private_not_observed
            FROM joined
        ''').fetchone()
        report = {'purpose':'observed-only provisional label-weather diagnostic join, not training-ready',
                  'source_label':str(LABEL), 'weather_files':len(files),
                  'weather_rows':n_weather,'label_rows':n_label,
                  'key_duplicates':{'weather':dup_weather,'labels':dup_label},
                  'joined_rows':summary[0], 'weather_matched':summary[1],
                  'weather_unmatched':summary[2], 'private_amount_present':summary[3],
                  'public_amount_present':summary[4], 'observed_component_sum_available':summary[5],
                  'public_not_observed':summary[6], 'private_not_observed':summary[7],
                  'notes':[
                    'raw_unit is NOT necessarily KRW: verify source metadata before currency conversion',
                    'missing source is unknown, never automatically 0',
                    'observed_component_sum_raw_unit only where both amounts known and public complete=Y',
                    'labels contain observed/positive-centric cases; do not call unmatched dates zero or use directly for unbiased regression',
                    'daily weather features are retrospective (not pre-event forecast features)',
                    'join exact administrative codes; unmatched records require mapping audit, no fuzzy join'
                  ]}
        print(json.dumps(report,ensure_ascii=False,indent=2), flush=True)
        # Atomic write and no overwrite
        tmp=OUT.with_suffix('.tmp.parquet')
        if tmp.exists(): raise FileExistsError(tmp)
        try:
            con.execute(f'COPY (SELECT * FROM joined ORDER BY label_year,label_dong_code,label_day) TO {sql_path(tmp)} (FORMAT PARQUET,COMPRESSION ZSTD)')
            tmp.replace(OUT)
            QA.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
        finally:
            if tmp.exists():tmp.unlink()
        print('저장:',OUT, '\nQA:',QA,flush=True)
    finally:
        con.close()

if __name__=='__main__': main()
