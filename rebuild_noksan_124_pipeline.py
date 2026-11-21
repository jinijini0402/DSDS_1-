"""Noksan 2024 safe 124-row recovery and downstream rebuild.

Run in project root: python -u rebuild_noksan_124_pipeline.py
Requires existing 2025-recovered inputs, original stage scripts, and Noksan by-index audit.
Never overwrites existing files. Partial outputs may remain if a later check fails.
No evidence of geolocation beyond 2025 official legal-code link is inferred.
"""
from __future__ import annotations

from pathlib import Path
from collections import Counter
from decimal import Decimal
from datetime import datetime
import csv
import importlib.util
import json
import sys
import types

import duckdb

ROOT = Path('data')
SIL = ROOT / 'silver/labels/rain_damage'
QC = ROOT / 'silver/quality_checks/labels'
GOLD = ROOT / 'gold/model_inputs'
AUDIT = QC / 'noksan_2024_split_diagnostic/noksan_2024_bronze_by_index_audit.csv'
REVIEW_IN = SIL / 'private_rain_sgis_join_review_recovered_2025_PROVISIONAL.parquet'
DAILY_IN = SIL / 'private_rain_daily_positive_recovered_2025_PROVISIONAL.parquet'
REVIEW_OUT = SIL / 'private_rain_sgis_join_review_recovered_2025_noksan124_PROVISIONAL.parquet'
DAILY_OUT = SIL / 'private_rain_daily_positive_recovered_2025_noksan124_PROVISIONAL.parquet'
MAP_REPORT = QC / 'private_rain_recovery_noksan124_report.json'
COMBINED_OUT = SIL / 'rain_property_sources_daily_recovered_2025_noksan124_PROVISIONAL.parquet'
COMBINED_REPORT = QC / 'rain_property_sources_daily_recovered_2025_noksan124_audit.json'
PROP_OUT = GOLD / 'property_damage_observed_daily_2024_2025_recovered_noksan124_PROVISIONAL.parquet'
HUMAN_OUT = GOLD / 'property_human_damage_observed_daily_2024_2025_recovered_noksan124_PROVISIONAL.parquet'
HUMAN_PREVIOUS = GOLD / 'property_human_damage_observed_daily_2024_2025_recovered_PROVISIONAL.parquet'

KEYS = [('disaster_year', 'MSTN_YR'), ('event_region_code','RGN_CD'),
        ('disaster_serial','MSTN_SN'), ('victim_serial','VCTM_SN'),
        ('damage_serial','DAM_SN'), ('history_serial','DAM_HSTRY_SN'),
        ('normalized_admin10','DAM_RGN_DONG_CD')]

def norm(v):
    if v is None: return ''
    s = str(v).strip()
    try:
        d = Decimal(s)
        if d == d.to_integral_value(): return str(int(d))
    except Exception: pass
    return s

def quoted(p): return "'" + str(p).replace("'", "''") + "'"

def require(paths):
    for p in paths:
        if not Path(p).is_file(): raise FileNotFoundError(f'입력 파일 없음: {p}')

def ensure_fresh(paths):
    for p in paths:
        if Path(p).exists(): raise FileExistsError(f'기존 결과 파일 보존: {p}')

def write_report(path, obj):
    ensure_fresh([path]); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=str), encoding='utf-8')

def stage1():
    print('\n[1/4] 녹산동 124건 원본 재확인 및 사유시설 Silver 생성', flush=True)
    require([AUDIT, REVIEW_IN, DAILY_IN])
    ensure_fresh([REVIEW_OUT, DAILY_OUT, MAP_REPORT])
    with AUDIT.open(encoding='utf-8-sig', newline='') as f:
        rows = list(csv.DictReader(f))
    if len(rows) != 125 or len({(norm(x['source_page']),norm(x['source_index'])) for x in rows}) != 125:
        raise RuntimeError('감사 CSV에 중복이 있거나 대상 125건을 재현하지 못함')
    selected = [x for x in rows if x['status']=='unique_legal_to_gold_candidate'
                and norm(x['target_sgis']) == '21120561'
                and norm(x['identifier_match']).lower() in ('true','1')]
    excluded = [x for x in rows if x not in selected]
    if len(selected) != 124 or len(excluded) != 1 or norm(excluded[0]['legal_code']) != '2644010800' \
            or norm(excluded[0]['target_sgis']) != '21120550':
        raise RuntimeError('124/1 후보 분류가 예상과 달라 중단')
    raw_dir = ROOT/'bronze/labels/safety_damage/private_facilities/raw'
    cache = {}
    for x in rows:  # INCLUDING held-out conflict, to verify 125 source identities
        page, pos = int(norm(x['source_page'])), int(norm(x['source_index']))
        if page not in cache:
            src = raw_dir / f'page_{page:05d}.json'
            require([src])
            obj = json.loads(src.read_text(encoding='utf-8-sig'))
            if norm(obj.get('pageNo')) != str(page) or not isinstance(obj.get('body'), list):
                raise RuntimeError(f'Bronze 페이지 구조 오류: {src}')
            cache[page] = obj['body']
        if not 0 <= pos < len(cache[page]): raise RuntimeError(f'Bronze 인덱스 오류: {page}/{pos}')
        b = cache[page][pos]
        if norm(b.get('DAM_RGN_STDG_CD')) != norm(x['legal_code']):
            raise RuntimeError(f'법정동 코드 변경 감지: {page}/{pos}')
        if norm(b.get('DAM_RGN_DONG_CD')) != '2644056000':
            raise RuntimeError(f'원본 행정동 코드 변경 감지: {page}/{pos}')
        if norm(b.get('MSTN_YR')) != '2024': raise RuntimeError('2024년 원본이 아님')
    # Re-derive official legal mappings rather than relying exclusively on the audit CSV.
    link = ROOT / 'silver/mappings/ndms_sgis_crosswalk/official_full_linkage_2025.csv'
    gold_weather = ROOT / 'gold/weather/daily_from_hourly/2024/dong_daily_weather_2024_01.parquet'
    require([link, gold_weather])
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute('SET threads=4')
    try:
        mapping = con.execute('''SELECT CAST(legal_dong10 AS VARCHAR),
              COUNT(DISTINCT sgis_dong8), MIN(CAST(sgis_dong8 AS VARCHAR))
              FROM read_csv(?, all_varchar=true) WHERE legal_dong10 IS NOT NULL
              GROUP BY 1''', [str(link)]).fetchall()
        legal_map = {code:(n, target) for code,n,target in mapping}
        gold_codes = set(r[0] for r in con.execute('''SELECT DISTINCT CAST(admin_dong_code AS VARCHAR)
                  FROM read_parquet(?)''', [str(gold_weather)]).fetchall())
        for x in selected:
            code = norm(x['legal_code'])
            if legal_map.get(code) != (1,'21120561') or '21120561' not in gold_codes:
                raise RuntimeError(f'법정동 공식 연계표/Gold 불일치: {code}')
        con.execute(f'CREATE VIEW previous AS SELECT * FROM read_parquet({quoted(REVIEW_IN)})')
        old_n = con.execute('SELECT COUNT(*) FROM previous').fetchone()[0]
        if old_n != 698421: raise RuntimeError(f'원본 검토 기록 수가 다름: {old_n}')
        # Recheck all seven damage identity components against Bronze, not just page/index.
        silver_125 = con.execute("""SELECT * FROM previous WHERE actual_year=2024
            AND normalized_admin10='2644056000' AND check_status='not_in_gold_boundary' """).df()
        if len(silver_125) != 125:
            raise RuntimeError(f'녹산동 원본 125건을 재현하지 못함: {len(silver_125)}')
        found = set()
        for _, r in silver_125.iterrows():
            page, pos = int(norm(r['source_page'])), int(norm(r['source_index']))
            key = (str(page),str(pos))
            if key not in {(norm(x['source_page']),norm(x['source_index'])) for x in rows}:
                raise RuntimeError(f'감사 대상 밖의 원본 위치: {key}')
            b = cache[page][pos]
            mismatch_fields = [a for a,z in KEYS if norm(r[a]) != norm(b.get(z))]
            if mismatch_fields:
                raise RuntimeError(f'Bronze/Silver 식별자 불일치: {key}: {mismatch_fields}')
            found.add(key)
        if len(found) != 125: raise RuntimeError('Bronze/Silver 125건 매칭 실패')

        # Do not materialize private address or identifier fields in audit output.
        con.execute('CREATE TEMP TABLE fix (source_page VARCHAR, source_index VARCHAR, sgis VARCHAR)')
        con.executemany('INSERT INTO fix VALUES (?,?,?)',
                        [(norm(x['source_page']), norm(x['source_index']), '21120561') for x in selected])
        con.execute('''CREATE TEMP VIEW matches AS
            SELECT p.*, f.sgis AS target
            FROM previous p LEFT JOIN fix f
              ON TRIM(p.source_page) = f.source_page
             AND TRIM(p.source_index) = f.source_index
             AND p.actual_year=2024 AND p.normalized_admin10='2644056000'
             AND p.check_status='not_in_gold_boundary' ''')
        stats = con.execute('''SELECT COUNT(*), COUNT(*) FILTER (WHERE target IS NOT NULL),
                    COUNT(*) FILTER (WHERE target IS NOT NULL AND amount_number>0),
                    COUNT(*) FILTER (WHERE target IS NOT NULL AND amount_number=0),
                    COUNT(*) FILTER (WHERE target IS NOT NULL AND (
                      history_time_flag IS NULL OR history_time_flag<>0 OR
                      valid_marker NOT IN ('U','V','I') OR amount_number IS NULL OR amount_number<0))
                    FROM matches''').fetchone()
        if stats != (698421,124,119,5,0):
            raise RuntimeError(f'복구 후보 상태가 예상과 다름: {stats}; 변경 없음')
        # Detect unexpected 1:N source linking.
        n_orig=con.execute('''SELECT COUNT(*) FROM previous p JOIN fix f
                      ON p.source_page=f.source_page AND p.source_index=f.source_index
                      WHERE p.actual_year=2024 AND p.normalized_admin10='2644056000' ''').fetchone()[0]
        if n_orig != 124: raise RuntimeError(f'125건 중 매핑 대상 식별자 수가 다름: {n_orig}')
        con.execute('''CREATE VIEW revised AS SELECT * EXCLUDE (target) REPLACE (
                COALESCE(target, sgis_dong8) AS sgis_dong8,
                CASE WHEN target IS NOT NULL THEN 'review_ready' ELSE check_status END AS check_status
            ) FROM matches''')
        # Verify the unrecovered code-conflict row remains unmapped.
        n_hold = con.execute('''SELECT COUNT(*) FROM revised
            WHERE source_page=? AND source_index=? AND actual_year=2024
              AND normalized_admin10='2644056000' AND check_status='not_in_gold_boundary' ''',
              [norm(excluded[0]['source_page']),norm(excluded[0]['source_index'])]).fetchone()[0]
        if n_hold != 1: raise RuntimeError('가락동 코드충돌 1건이 변경되었음')
        # Ensure no unexpected changed rows apart from the 124 candidates.
        affected = con.execute('''SELECT COUNT(*) FROM revised n JOIN previous o
            ON n.source_page=o.source_page AND n.source_index=o.source_index
            WHERE (n.sgis_dong8 IS DISTINCT FROM o.sgis_dong8
              OR n.check_status IS DISTINCT FROM o.check_status) ''').fetchone()[0]
        # If source_page+source_index isn't unique globally due to raw history, this join
        # could amplify. It is diagnostic, not used for data generation.
        if affected != 124: raise RuntimeError(f'예상 밖 상태 변경 확인: {affected}')
        REVIEW_OUT.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f'COPY (SELECT * FROM revised) TO {quoted(REVIEW_OUT)} (FORMAT PARQUET, COMPRESSION ZSTD)')
        con.execute(f'''COPY (
            SELECT actual_year AS year, sgis_dong8 AS admin_dong_code, actual_day AS day,
              COUNT(*) AS provisional_positive_record_count,
              SUM(amount_number) AS provisional_amount_raw_unit,
              1 AS provisional_damage_presence
            FROM revised WHERE check_status='review_ready' AND amount_number>0
            GROUP BY 1,2,3
            ) TO {quoted(DAILY_OUT)} (FORMAT PARQUET, COMPRESSION ZSTD)''')
        old_same=con.execute(f'''SELECT COUNT(*) FROM read_parquet({quoted(DAILY_IN)}) o
            FULL OUTER JOIN read_parquet({quoted(DAILY_OUT)}) n USING(year,admin_dong_code,day)
            WHERE o.year IS NOT NULL AND n.year IS NULL''').fetchone()[0]
        if old_same: raise RuntimeError(f'기존 양수 피해 동·일이 사라짐: {old_same}')
        old_amt,old_count=con.execute(f'SELECT SUM(provisional_amount_raw_unit), SUM(provisional_positive_record_count) FROM read_parquet({quoted(DAILY_IN)})').fetchone()
        new_amt,new_count=con.execute(f'SELECT SUM(provisional_amount_raw_unit), SUM(provisional_positive_record_count) FROM read_parquet({quoted(DAILY_OUT)})').fetchone()
        expected_added=con.execute('SELECT SUM(amount_number) FROM matches WHERE target IS NOT NULL AND amount_number>0').fetchone()[0]
        if new_count-old_count != 119 or new_amt-old_amt != expected_added:
            raise RuntimeError(f'양수 기록/금액 증가분 불일치: {new_count-old_count}, {new_amt-old_amt} vs 119, {expected_added}')
        daily_n=con.execute(f'SELECT COUNT(*) FROM read_parquet({quoted(DAILY_OUT)})').fetchone()[0]
        report={'status':'PROVISIONAL_NOT_TRAINING_READY','created_at':datetime.now().isoformat(),
                'source_review':str(REVIEW_IN),'audit_csv':str(AUDIT),
                'bronze_identifier_verified':125,'mapping_recovered':124,
                'positive_added':119,'zero_excluded_from_positive_daily':5,
                'conflicting_bongnim_held':1,'source_rows_unchanged':old_n,
                'old_daily_count':con.execute(f'SELECT COUNT(*) FROM read_parquet({quoted(DAILY_IN)})').fetchone()[0],
                'new_daily_count':daily_n,'positive_amount_added_raw_unit':str(expected_added),
                'outputs':[str(REVIEW_OUT),str(DAILY_OUT)],
                'note':'Uses 2025 legal-code crosswalk to target SGIS 2025-06-30 border. One internally inconsistent source location remains unresolved.'}
        write_report(MAP_REPORT,report)
        print(json.dumps(report,ensure_ascii=False,indent=2),flush=True)
    finally: con.close()

def import_module(path, name):
    require([path])
    spec=importlib.util.spec_from_file_location(name,path)
    obj=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(obj)
    return obj

def stage2():
    print('\n[2/4] 공공시설 Silver와 병렬 결합',flush=True)
    script=Path('combine_rain_property_provisional_sources_recovered_2025.py')
    mod=import_module(script,'combine_noksan')
    mod.PRIVATE=DAILY_OUT; mod.OUTPUT=COMBINED_OUT; mod.REPORT=COMBINED_REPORT
    ensure_fresh([COMBINED_OUT,COMBINED_REPORT])
    mod.main()

def stage3():
    print('\n[3/4] 기존 일별 기상 Gold 결합',flush=True)
    script=Path('join_property_daily_weather_2024_2025.py')
    mod=import_module(script,'join_weather_noksan')
    mod.LABEL=COMBINED_OUT; mod.OUT=PROP_OUT; mod.QA=PROP_OUT.with_suffix('.qa.json')
    ensure_fresh([mod.OUT,mod.QA]); mod.main()

def stage4():
    print('\n[4/4] 기존 인적 피해 상태를 보존하며 재결합',flush=True)
    # Adapt validated human overlay script to the current previously-recovered Gold.
    # Modify ONLY invariant checks: new 2024 Noksan days may change pre-existing property rows.
    template = Path('join_rain_casualty_to_recovered_gold.py')
    require([template,HUMAN_PREVIOUS])
    src=template.read_text(encoding='utf-8')
    subs = [
      ("np != 22519 or no != 22468", "np < 22519 or no != 22520"),
      ("예상 22519/22468", "예상 새 물적 >=22519/기존 통합 22520"),
      ("if mismatch:\n            raise RuntimeError(f'기존 행의 물적 피해·기상 값이 {mismatch}개 달라 작업 중단')",
       """if mismatch:
            allowed_sql = f'''SELECT COUNT(*) FROM prop p JOIN previous o
                USING(label_year,label_dong_code,label_day)
                WHERE p.label_year=2024 AND p.label_dong_code='21120561'
                  AND ({comparisons})'''
            allowed = con.execute(allowed_sql).fetchone()[0]
            if mismatch != allowed:
                raise RuntimeError(f'녹산동 외 기존 물적 피해·기상 값 변경: {mismatch} vs {allowed}')"""),
      ("if same != 22467:", "if same != 22519:"),
      ("예상 22467", "예상 22519"),
      ("if novel != 52:\n            raise RuntimeError(f'추가 물적 피해 동·일 {novel}, 예상 52')", "if novel < 0:\n            raise RuntimeError('추가 동·일 수가 음수')"),
      ("if nfinal != 22520:", "if nfinal != np + 1:"),
      ("'old_property_weather_unchanged':True", "'old_property_weather_unchanged_except_noksan_recovery':True"),
    ]
    for before,after in subs:
        n=src.count(before)
        if n!=1: raise RuntimeError(f'인적 피해 결합 스크립트 예상 구문 불일치({n}): {before[:80]}')
        src=src.replace(before,after,1)
    # This is a direct, in-memory adaptation. The original script remains untouched.
    module=types.ModuleType('human_noksan')
    module.__dict__['__name__']='human_noksan'
    exec(compile(src, str(template), 'exec'),module.__dict__)
    module.NEW_PROPERTY=PROP_OUT
    module.OLD_COMBINED=HUMAN_PREVIOUS
    module.OUT=HUMAN_OUT
    module.QA=HUMAN_OUT.with_suffix('.qa.json')
    ensure_fresh([module.OUT,module.QA])
    module.run()

if __name__=='__main__':
    print('Noksan 2024 recovery: 124 approved, 1 held; originals are never overwritten.',flush=True)
    stage1();stage2();stage3();stage4()
    print('\n전체 처리 완료:',HUMAN_OUT,flush=True)
