"""Safely rebuild property+human provisional Gold after 2025 property mapping recovery.

Uses the previous combined Gold as the *human-label processing reference*, not as
an authority for the updated property and weather fields. Recomputes unverified
spatial/temporal overlap for property-only dong-days newly introduced by recovery.

Does not mutate original Gold/Silver, does not treat missing casualty records as 0,
and does not produce a supervised-learning dataset.

Run from heavy-rain-prediction root:
    python -u join_rain_casualty_to_recovered_gold.py
"""
from pathlib import Path
from datetime import datetime
import json
import duckdb

BASE = Path('data')
NEW_PROPERTY = BASE / 'gold/model_inputs/property_damage_observed_daily_2024_2025_recovered_PROVISIONAL.parquet'
OLD_COMBINED = BASE / 'gold/model_inputs/property_human_damage_observed_daily_2024_2025_PROVISIONAL.parquet'
HUMAN_DAILY = BASE / 'silver/labels/rain_casualty/rain_casualty_dong_daily.parquet'
HUMAN_RECORDS = BASE / 'silver/labels/rain_casualty/rain_casualty_records.parquet'
OUT = BASE / 'gold/model_inputs/property_human_damage_observed_daily_2024_2025_recovered_PROVISIONAL.parquet'
QA = OUT.with_suffix('.qa.json')


def q(s):
    return "'" + str(s).replace("'", "''") + "'"


def run():
    for path in (NEW_PROPERTY, OLD_COMBINED, HUMAN_DAILY, HUMAN_RECORDS):
        if not path.is_file():
            raise FileNotFoundError(f'필요한 입력 파일 없음: {path}')
    if OUT.exists() or QA.exists():
        raise FileExistsError(f'기존 결과 보호: {OUT} / {QA}')
    OUT.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(':memory:')
    con.execute("SET memory_limit='4GB'")
    con.execute('SET threads=4')
    try:
        con.execute(f'CREATE VIEW prop AS SELECT * FROM read_parquet({q(NEW_PROPERTY)})')
        con.execute(f'CREATE VIEW previous AS SELECT * FROM read_parquet({q(OLD_COMBINED)})')
        con.execute(f'CREATE VIEW hd AS SELECT * FROM read_parquet({q(HUMAN_DAILY)})')
        con.execute(f'CREATE VIEW hr AS SELECT * FROM read_parquet({q(HUMAN_RECORDS)})')

        pcols = [r[0] for r in con.execute('DESCRIBE prop').fetchall()]
        hcols = [r[0] for r in con.execute('DESCRIBE previous').fetchall()]
        keys = ['label_year', 'label_dong_code', 'label_day']
        if not set(keys).issubset(pcols) or not set(keys).issubset(hcols):
            raise RuntimeError('물적 Gold 또는 기존 통합 Gold에 label_* 키가 없습니다.')
        human_cols = [c for c in hcols if c.startswith('human_')]
        required = {'human_label_status', 'human_unverified_scope',
                    'human_unverified_label_ids', 'human_unverified_persons_in_scope',
                    'human_unlocated_label_ids', 'human_processing_version',
                    'damage_sources', 'property_record_present'}
        if not required.issubset(hcols) or len(human_cols) < 18:
            raise RuntimeError('기존 통합 Gold의 인적 피해 컬럼 구성이 예상과 다릅니다.')
        conflict = set(pcols).intersection(human_cols + ['damage_sources', 'property_record_present'])
        if conflict:
            raise RuntimeError(f'물적 Gold에서 예상 밖 인적 컬럼 발견: {sorted(conflict)}')
        if not {'label_id','label_status','overlay_scope','overlay_area_codes',
                'label_period_start','label_period_end','person_count'}.issubset(
                    r[0] for r in con.execute('DESCRIBE hr').fetchall()):
            raise RuntimeError('인적 원본 Silver의 범위 판정 컬럼 누락')

        def unique(view):
            n, d = con.execute(f'''SELECT COUNT(*), COUNT(*)-COUNT(DISTINCT (
                label_year, label_dong_code, label_day)) FROM {view}''').fetchone()
            if d: raise RuntimeError(f'{view} 중복 키: {d}')
            return n
        np = unique('prop')
        no = unique('previous')
        if np != 22519 or no != 22468:
            raise RuntimeError(f'입력 행수 변경: 새 물적 {np}, 이전 통합 {no}. 예상 22519/22468')
        if con.execute('''SELECT COUNT(*) FROM previous WHERE damage_sources='HUMAN' ''').fetchone()[0] != 1:
            raise RuntimeError('기존 통합 Gold의 인적 피해 전용 행이 1개가 아닙니다.')

        # Verify the 22,467 pre-existing property rows still have exactly the same
        # property+weather fields. No legacy human fields are read as new property data.
        common = [c for c in pcols if c in hcols]
        sql_ident = lambda c: '"' + c.replace('"', '""') + '"'
        comparisons = ' OR '.join(
            f'p.{sql_ident(c)} IS DISTINCT FROM o.{sql_ident(c)}'
            for c in common if c not in keys
        )
        mismatch = con.execute(f'''
            SELECT COUNT(*) FROM prop p JOIN previous o USING(label_year,label_dong_code,label_day)
            WHERE {comparisons}''').fetchone()[0]
        if mismatch:
            raise RuntimeError(f'기존 행의 물적 피해·기상 값이 {mismatch}개 달라 작업 중단')
        same = con.execute('''SELECT COUNT(*) FROM prop p JOIN previous o
            USING(label_year,label_dong_code,label_day) WHERE o.damage_sources<>'HUMAN' ''').fetchone()[0]
        if same != 22467:
            raise RuntimeError(f'기존 물적 Gold 일치행 {same}, 예상 22467')
        novel = np - same
        if novel != 52:
            raise RuntimeError(f'추가 물적 피해 동·일 {novel}, 예상 52')

        # Confirm all 8 established human labels are already represented in the old Gold.
        unseen_human = con.execute('''
            SELECT COUNT(*) FROM hd d LEFT JOIN previous o
              ON d.label_year=o.label_year AND CAST(d.admin_dong_code AS VARCHAR)=o.label_dong_code
             AND CAST(d.day AS DATE)=o.label_day
            WHERE o.label_year IS NULL''').fetchone()[0]
        if unseen_human:
            raise RuntimeError(f'기존 통합 Gold에 없는 인적 피해 확정 키 {unseen_human}개')

        # Derive scope overlays ONLY for new dong-days. An unverified regional/period
        # record marks a day as potentially affected; it never distributes its persons.
        con.execute('''CREATE TEMP VIEW novel AS
            SELECT p.* FROM prop p LEFT JOIN previous o
              USING(label_year,label_dong_code,label_day)
            WHERE o.label_year IS NULL''')
        con.execute('''CREATE TEMP VIEW spatial_hits AS
            SELECT n.label_year, n.label_dong_code, n.label_day,
                   r.label_id, r.overlay_scope,
                   TRY_CAST(r.person_count AS INTEGER) AS persons
            FROM novel n JOIN hr r
              ON r.label_status='unverified'
             AND r.overlay_scope IN ('sido','sigungu')
             AND n.label_day BETWEEN TRY_CAST(r.label_period_start AS DATE)
                                 AND TRY_CAST(r.label_period_end AS DATE)
             AND (
                   (r.overlay_scope='sido' AND
                    LEFT(n.label_dong_code,2)=TRIM(CAST(r.overlay_area_codes AS VARCHAR)))
                OR (r.overlay_scope='sigungu' AND
                    LEFT(n.label_dong_code,5)=TRIM(CAST(r.overlay_area_codes AS VARCHAR)))
                 )
        ''')
        con.execute('''CREATE TEMP VIEW spatial AS
            SELECT label_year,label_dong_code,label_day,
                   CASE WHEN MAX(CASE WHEN overlay_scope='sigungu' THEN 1 ELSE 0 END)=1
                        THEN 'sigungu' ELSE 'sido' END AS scope,
                   STRING_AGG(DISTINCT label_id, '|' ORDER BY label_id) AS ids,
                   SUM(persons) AS persons_in_scope
            FROM spatial_hits GROUP BY 1,2,3''')
        con.execute('''CREATE TEMP VIEW unlocated AS
            SELECT n.label_year, n.label_dong_code, n.label_day,
                   STRING_AGG(DISTINCT r.label_id, '|' ORDER BY r.label_id) AS ids
            FROM novel n JOIN hr r
              ON r.label_status='unverified' AND r.overlay_scope='unlocated'
             AND n.label_day BETWEEN TRY_CAST(r.label_period_start AS DATE)
                                 AND TRY_CAST(r.label_period_end AS DATE)
            GROUP BY 1,2,3''')
        con.execute('''CREATE TEMP VIEW annotation AS
            SELECT n.label_year, n.label_dong_code, n.label_day,
                   s.scope, s.ids, s.persons_in_scope, u.ids AS unlocated_ids
            FROM novel n
            LEFT JOIN spatial s USING(label_year,label_dong_code,label_day)
            LEFT JOIN unlocated u USING(label_year,label_dong_code,label_day)''')

        special = {
            'human_unverified_scope': 'COALESCE(o.human_unverified_scope, a.scope)',
            'human_unverified_label_ids': 'COALESCE(o.human_unverified_label_ids, a.ids)',
            'human_unverified_persons_in_scope': 'COALESCE(o.human_unverified_persons_in_scope, a.persons_in_scope)',
            'human_unlocated_label_ids': 'COALESCE(o.human_unlocated_label_ids, a.unlocated_ids)',
            'human_label_status': "COALESCE(o.human_label_status, CASE WHEN a.scope IS NOT NULL THEN 'unverified' ELSE 'no_record' END)",
            'human_processing_version': "COALESCE(o.human_processing_version, 'gold_property_human_observed_daily_v1')",
        }
        computed = [f'{special.get(c, "o."+sql_ident(c))} AS {sql_ident(c)}'
                    for c in human_cols]
        # 'damage_sources' is independent of a missing-source label; carry it over for old rows.
        computed += ["CAST(1 AS TINYINT) AS property_record_present",
                     "COALESCE(o.damage_sources, 'PROPERTY') AS damage_sources"]
        select_cols = ',\n  '.join(['p.*'] + computed)
        con.execute(f'''CREATE TEMP VIEW combined AS
            SELECT {select_cols}
            FROM prop p
            LEFT JOIN previous o USING(label_year,label_dong_code,label_day)
            LEFT JOIN annotation a USING(label_year,label_dong_code,label_day)
            UNION ALL BY NAME
            SELECT * FROM previous WHERE damage_sources='HUMAN'
        ''')

        # Every previous property+human row must remain semantically unchanged except
        # updated property rows which have already been checked above.
        old_human_diff = ' OR '.join(
            f'c.{sql_ident(c)} IS DISTINCT FROM o.{sql_ident(c)}'
            for c in human_cols + ['damage_sources', 'property_record_present'])
        old_diffs = con.execute(f'''SELECT COUNT(*) FROM combined c
            JOIN previous o USING(label_year,label_dong_code,label_day)
            WHERE {old_human_diff}''').fetchone()[0]
        if old_diffs:
            raise RuntimeError(f'기존 동·일의 인적 피해 정보가 {old_diffs}개 변경됨')
        nfinal = unique('combined')
        if nfinal != 22520:
            raise RuntimeError(f'최종 행수 {nfinal}, 예상 22520')
        stats = con.execute('''SELECT human_label_status, COUNT(*) FROM combined
            GROUP BY 1 ORDER BY 1''').fetchall()
        stats = {str(k):v for k,v in stats}
        if stats.get('observed') != 8:
            raise RuntimeError(f'인적 피해 확정 8건이 보존되지 않음: {stats}')
        false_counts = con.execute('''SELECT COUNT(*) FROM combined
            WHERE human_label_status <> 'observed'
              AND human_casualty_count_total IS NOT NULL''').fetchone()[0]
        if false_counts: raise RuntimeError('미확정/미관측 기록에 인원 수가 채워졌음')
        src_stats = {str(k):v for k,v in con.execute('''SELECT damage_sources,COUNT(*)
            FROM combined GROUP BY 1''').fetchall()}
        if src_stats.get('HUMAN') != 1 or src_stats.get('PROPERTY+HUMAN') != 7:
            raise RuntimeError(f'피해 출처 분포 이상: {src_stats}')

        report = {
            'status':'PROVISIONAL_OBSERVED_CASES_NOT_TRAINING_READY',
            'created_at':datetime.now().isoformat(timespec='seconds'),
            'inputs':{'new_property_gold':str(NEW_PROPERTY),'previous_human_gold':str(OLD_COMBINED),
                      'human_dong_daily':str(HUMAN_DAILY),'human_records':str(HUMAN_RECORDS)},
            'counts':{'previous_gold':no,'new_property_gold':np,'reused_existing_property_days':same,
                      'new_property_days':novel,'human_only_appended':1,'output':nfinal},
            'human_label_status':stats, 'damage_sources':src_stats,
            'checks':{'old_property_weather_unchanged':True,'old_human_fields_unchanged':True,
                      'key_unique':True,'human_observed_8_preserved':True,
                      'no_casualty_counts_outside_observed':True},
            'notes':['New property-only dong-days are checked against spatial/period unverified scope.',
                     'Unverified persons are regional scope metadata, NOT allocated casualty labels.',
                     'No record means no recorded casualty, NOT confirmed zero.',
                     'Old human-only Unseo-dong weather row is preserved.',
                     'Damage amount units and official recognition are provisional.']
        }
        tmp=OUT.with_name(OUT.stem+'.tmp.parquet')
        if tmp.exists(): raise FileExistsError(tmp)
        try:
            con.execute(f'''COPY (SELECT * FROM combined ORDER BY label_year,label_dong_code,label_day)
                TO {q(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD)''')
            tmp.replace(OUT)
            QA.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        finally:
            if tmp.exists(): tmp.unlink()
        print(json.dumps(report,ensure_ascii=False,indent=2),flush=True)
        print('저장:',OUT,'\nQA:',QA,flush=True)
    finally:
        con.close()


if __name__ == '__main__':
    run()
