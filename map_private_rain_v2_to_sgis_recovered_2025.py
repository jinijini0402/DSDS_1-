"""기존 사유시설 매핑 로직 유지 + 검증된 2025년 252건만 보완.

원본 및 기존 *_fixed 파일은 건드리지 않음. 산출물은 *_recovered_2025_PROVISIONAL.
실행: python -u map_private_rain_v2_to_sgis_recovered_2025.py
"""
from pathlib import Path
from datetime import datetime
import json
import duckdb

ROOT = Path('data')
SRC = ROOT / 'silver/labels/rain_damage/private_rain_latest_history_review_v2.parquet'
MAP = ROOT / 'silver/mappings/ndms_sgis_crosswalk'
GOLD = ROOT / 'gold/weather/daily_from_hourly'
CANDIDATES = ROOT / 'silver/quality_checks/labels/mapping_recovery_2025_diagnostic/private_2025_recovery_252_candidates.parquet'
OUT = ROOT / 'silver/labels/rain_damage'
QC = ROOT / 'silver/quality_checks/labels'
REVIEW = OUT / 'private_rain_sgis_join_review_recovered_2025_PROVISIONAL.parquet'
DAILY = OUT / 'private_rain_daily_positive_recovered_2025_PROVISIONAL.parquet'
REPORT = QC / 'private_rain_sgis_join_recovered_2025_report.json'


def q(path):
    return "'" + str(path).replace("'", "''") + "'"


def main():
    files = [SRC, CANDIDATES]
    for y in (2024, 2025):
        files.extend([MAP / f'official_admin10_to_sgis8_{y}.csv',
                      GOLD / str(y) / f'dong_daily_weather_{y}_01.parquet'])
    for p in files:
        if not p.is_file():
            raise FileNotFoundError(f'입력 파일 없음: {p}')
    for p in (REVIEW, DAILY, REPORT):
        if p.exists():
            raise FileExistsError(f'기존 결과 보호: {p} (직접 검토 후 이름 변경 또는 별도 보관)')

    OUT.mkdir(parents=True, exist_ok=True)
    QC.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(':memory:')
    con.execute("SET memory_limit='4GB'")
    con.execute('SET threads=4')
    try:
        con.execute(f"""CREATE TEMP VIEW raw_codes AS
            SELECT *, TRIM(CAST(admin_code_raw AS VARCHAR)) AS admin10_text
            FROM read_parquet({q(SRC)})""")
        total, exact, empty, dotzero, scientific = con.execute("""
            SELECT COUNT(*),
              COUNT(*) FILTER (WHERE REGEXP_MATCHES(admin10_text, '^[0-9]{10}$')),
              COUNT(*) FILTER (WHERE admin10_text IS NULL OR admin10_text=''),
              COUNT(*) FILTER (WHERE admin10_text LIKE '%.0'),
              COUNT(*) FILTER (WHERE REGEXP_MATCHES(admin10_text,'[eE][+-]?[0-9]+$'))
            FROM raw_codes""").fetchone()
        print(f'1단계: 원본 {total:,}행 / 원래 10자리 {exact:,}행 / 빈 코드 {empty:,}행',flush=True)
        con.execute("""CREATE TEMP VIEW normalized AS
          SELECT *, CASE
            WHEN REGEXP_MATCHES(admin10_text,'^[0-9]{10}$') THEN admin10_text
            WHEN REGEXP_MATCHES(admin10_text,
              '^([0-9]+\\.0+|[0-9]+(\\.[0-9]+)?[eE][+-]?[0-9]+)$')
              AND TRY_CAST(admin10_text AS DOUBLE) BETWEEN 1000000000 AND 9999999999
              AND TRY_CAST(admin10_text AS DOUBLE) = FLOOR(TRY_CAST(admin10_text AS DOUBLE))
            THEN CAST(CAST(TRY_CAST(admin10_text AS DOUBLE) AS DECIMAL(20,0)) AS VARCHAR)
            ELSE NULL END AS normalized_admin10
          FROM raw_codes""")
        good, recovered = con.execute("""
          SELECT COUNT(*) FILTER (WHERE REGEXP_MATCHES(COALESCE(normalized_admin10,''),'^[0-9]{10}$')),
                 COUNT(*) FILTER (WHERE normalized_admin10 IS NOT NULL AND normalized_admin10<>admin10_text)
          FROM normalized""").fetchone()
        if total == 0 or good / total < .99:
            raise RuntimeError(f'10자리 코드 복원율이 99% 미만: {good}/{total}')

        sources = []
        golds = []
        for y in (2024, 2025):
            sources.append(f"""SELECT {y} AS y, TRIM(admin_dong10) AS admin10,
              TRIM(sgis_dong8) AS sgis8, TRIM(unique_admin_to_sgis) AS official_unique
              FROM read_csv({q(MAP / f'official_admin10_to_sgis8_{y}.csv')}, header=true, all_varchar=true)""")
            golds.append(f"""SELECT {y} AS y, CAST(admin_dong_code AS VARCHAR) AS sgis8
              FROM read_parquet({q(GOLD / str(y) / f'dong_daily_weather_{y}_01.parquet')})
              GROUP BY 1,2""")
        con.execute(f"CREATE TEMP VIEW official_raw AS {' UNION ALL '.join(sources)}")
        con.execute("""CREATE TEMP VIEW official_map AS
            SELECT y, admin10, MAX(sgis8) AS sgis8 FROM official_raw
            WHERE REGEXP_MATCHES(admin10, '^[0-9]{10}$')
              AND REGEXP_MATCHES(sgis8, '^[0-9]{8}$') AND official_unique='1'
            GROUP BY y, admin10 HAVING COUNT(DISTINCT sgis8)=1""")
        con.execute(f"CREATE TEMP VIEW gold_codes AS {' UNION ALL '.join(golds)}")
        con.execute("""CREATE TEMP VIEW original_joined AS
          WITH base AS (
            SELECT *, TRY_CAST(damage_day AS DATE) AS actual_day,
              TRY_CAST(REPLACE(TRIM(CAST(confirmed_damage_amount_raw AS VARCHAR)),',','') AS DECIMAL(24,2)) AS amount_number,
              TRY_CAST(history_vs_modified_disagrees AS INTEGER) AS history_time_flag
            FROM normalized)
          SELECT b.*, EXTRACT(YEAR FROM b.actual_day)::INTEGER AS actual_year,
            m.sgis8 AS sgis_dong8,
            CASE
              WHEN b.actual_day IS NULL OR b.actual_day<DATE '2024-01-01'
                OR b.actual_day>DATE '2025-12-31' THEN 'invalid_date'
              WHEN b.valid_marker NOT IN ('U','V','I') THEN 'invalid_status'
              WHEN b.history_time_flag IS NULL OR b.history_time_flag<>0 THEN 'history_order_review'
              WHEN b.normalized_admin10 IS NULL OR NOT REGEXP_MATCHES(b.normalized_admin10,'^[0-9]{10}$') THEN 'invalid_admin_code'
              WHEN m.sgis8 IS NULL THEN 'not_in_year_official_map'
              WHEN g.sgis8 IS NULL THEN 'not_in_gold_boundary'
              WHEN b.amount_number IS NULL THEN 'missing_or_invalid_amount'
              WHEN b.amount_number<0 THEN 'negative_amount_review'
              ELSE 'review_ready' END AS check_status
          FROM base b
          LEFT JOIN official_map m ON m.y=EXTRACT(YEAR FROM b.actual_day)::INTEGER
            AND m.admin10=b.normalized_admin10
          LEFT JOIN gold_codes g ON g.y=EXTRACT(YEAR FROM b.actual_day)::INTEGER AND g.sgis8=m.sgis8""")

        # 이전 진단에서 정확히 분리된 후보 파일만 사용한다. 원본 행의 page/index/코드를 함께 대조.
        con.execute(f"""CREATE TEMP VIEW candidates AS
          SELECT CAST(source_page AS VARCHAR) AS source_page,
                 CAST(source_index AS VARCHAR) AS source_index,
                 CAST(normalized_admin10 AS VARCHAR) AS normalized_admin10,
                 CAST(recovered_sgis AS VARCHAR) AS recovered_sgis
          FROM read_parquet({q(CANDIDATES)})
          WHERE actual_year=2025 AND check_status='not_in_year_official_map'""")
        cand_n, cand_unique = con.execute("""
          SELECT COUNT(*), COUNT(DISTINCT (source_page, source_index, normalized_admin10))
          FROM candidates""").fetchone()
        if cand_n != 252 or cand_unique != 252:
            raise RuntimeError(f'후보 파일 중복/건수 오류: rows={cand_n} unique={cand_unique}')
        # 원본 내 같은 소스 좌표가 겹치면 JOIN으로 행이 복제될 수 있으므로 방지.
        dup_source = con.execute("""SELECT COUNT(*) FROM (
          SELECT source_page, source_index, normalized_admin10 FROM original_joined
          WHERE actual_year=2025
          GROUP BY ALL HAVING COUNT(*)>1)""").fetchone()[0]
        if dup_source:
            raise RuntimeError(f'원본 소스 위치 중복 {dup_source}건: 자동 JOIN 중단')
        con.execute("""CREATE TEMP VIEW recovery_matches AS
          SELECT o.source_page, o.source_index, o.normalized_admin10,
                 c.recovered_sgis
          FROM original_joined o
          JOIN candidates c ON CAST(o.source_page AS VARCHAR)=c.source_page
            AND CAST(o.source_index AS VARCHAR)=c.source_index
            AND o.normalized_admin10=c.normalized_admin10
          JOIN gold_codes g ON g.y=2025 AND g.sgis8=c.recovered_sgis
          WHERE o.actual_year=2025 AND o.check_status='not_in_year_official_map'""")
        matched, match_unique = con.execute("""SELECT COUNT(*),
          COUNT(DISTINCT (source_page,source_index,normalized_admin10)) FROM recovery_matches""").fetchone()
        if matched != 252 or match_unique != 252:
            raise RuntimeError(f'복구 대상 불일치: {matched}건 / 고유 {match_unique}건. 출력 중단')
        print('2단계: 2025년 보완 대상 252건 확인 (나머지 기존 상태 유지)',flush=True)
        con.execute("""CREATE TEMP VIEW recovered_joined AS
          SELECT o.* EXCLUDE (sgis_dong8,check_status),
             COALESCE(r.recovered_sgis,o.sgis_dong8) AS sgis_dong8,
             CASE WHEN r.recovered_sgis IS NOT NULL THEN
                CASE WHEN o.amount_number IS NULL THEN 'missing_or_invalid_amount'
                     WHEN o.amount_number<0 THEN 'negative_amount_review'
                     ELSE 'review_ready' END
                  ELSE o.check_status END AS check_status,
             CASE WHEN r.recovered_sgis IS NOT NULL THEN 'official_2024_fallback_2025'
                  ELSE 'original_mapping' END AS mapping_method
          FROM original_joined o LEFT JOIN recovery_matches r
            ON CAST(o.source_page AS VARCHAR)=CAST(r.source_page AS VARCHAR)
           AND CAST(o.source_index AS VARCHAR)=CAST(r.source_index AS VARCHAR)
           AND o.normalized_admin10=r.normalized_admin10""")
        count_after, recovered_count = con.execute("""SELECT COUNT(*),
          COUNT(*) FILTER (WHERE mapping_method='official_2024_fallback_2025')
          FROM recovered_joined""").fetchone()
        if count_after != total or recovered_count != 252:
            raise RuntimeError(f'행 수 검증 실패 total={count_after} recovered={recovered_count}')
        # 결과물은 수정본만 생성하고 기존 _fixed 산출물은 보존.
        con.execute(f"COPY (SELECT * FROM recovered_joined) TO {q(REVIEW)} (FORMAT PARQUET,COMPRESSION ZSTD)")
        con.execute(f"""COPY (SELECT actual_year AS year, sgis_dong8 AS admin_dong_code,
          actual_day AS day, COUNT(*) AS provisional_positive_record_count,
          SUM(amount_number) AS provisional_amount_raw_unit,
          1 AS provisional_damage_presence
          FROM recovered_joined WHERE check_status='review_ready' AND amount_number>0
          GROUP BY 1,2,3) TO {q(DAILY)} (FORMAT PARQUET,COMPRESSION ZSTD)""")
        statuses = {}
        for y,s,n in con.execute("""SELECT actual_year,check_status,COUNT(*)
            FROM recovered_joined GROUP BY 1,2 ORDER BY 1,2""").fetchall():
            statuses.setdefault(str(y),{})[s]=n
        daily_rows=con.execute(f'SELECT COUNT(*) FROM read_parquet({q(DAILY)})').fetchone()[0]
        report = {
          'status':'PROVISIONAL_REVIEW_NOT_FINAL_LABEL',
          'created_at':datetime.now().isoformat(timespec='seconds'),
          'source':str(SRC),'candidate_source':str(CANDIDATES),
          'original_input_rows':total,'original_exact_10_digits':exact,
          'recovered_10_digits':good,'representation_recovered_rows':recovered,
          'mapping_recovered_rows':recovered_count,'output_rows':count_after,
          'status_counts_by_year':statuses,'positive_dong_day_rows':daily_rows,
          'official_snapshots':{'2024':'2024-10-01','2025':'2025-12-31'},
          'gold_boundary_snapshot':'2025-06-30',
          'review_file':str(REVIEW),'daily_positive_file':str(DAILY),
          'warnings':['2025 only: exactly 252 not_in_year_official_map candidates are patched.',
                      'History-order review 5, unresolved 14, 2024 Noksan cases are not patched.',
                      'PK/latest-history and amount units still provisional. No zero labels.',
                      'Do not overwrite existing downstream Gold before separate QA.']}
        REPORT.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
        print(f'완료: 원본 {total:,}행 유지 / 매핑 252건 보완 / 일별 {daily_rows:,}행',flush=True)
        print('검토 테이블:',REVIEW,'\n일별 집계:',DAILY,'\nQA:',REPORT,flush=True)
    finally:
        con.close()


if __name__=='__main__':
    main()
