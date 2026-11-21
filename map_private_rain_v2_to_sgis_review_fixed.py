"""사유시설 호우 최신이력(v2)의 10자리 코드 표현 진단 + SGIS 연결 재검증.

실행: python -u map_private_rain_v2_to_sgis_review_fixed.py
입력 원본 및 기존 출력 변경 없음. 신규 *_fixed 파일에만 출력.
중요: 피해 고유키/최신 이력/피해액 단위는 검토 중인 PROVISIONAL 자료.
"""
from pathlib import Path
from datetime import datetime
import json
import duckdb

ROOT = Path('data')
SRC = ROOT / 'silver/labels/rain_damage/private_rain_latest_history_review_v2.parquet'
MAP = ROOT / 'silver/mappings/ndms_sgis_crosswalk'
GOLD = ROOT / 'gold/weather'
OUT = ROOT / 'silver/labels/rain_damage'
QC = ROOT / 'silver/quality_checks/labels'
REVIEW = OUT / 'private_rain_sgis_join_review_fixed.parquet'
DAILY = OUT / 'private_rain_daily_positive_PROVISIONAL_fixed.parquet'
REPORT = QC / 'private_rain_sgis_join_review_report_fixed.json'


def q(path):
    return "'" + str(path).replace("'", "''") + "'"


def main():
    files = [SRC]
    for y in (2024, 2025):
        files += [MAP / f'official_admin10_to_sgis8_{y}.csv',
                  GOLD / f'dong_daily_features_{y}.parquet']
    for path in files:
        if not path.is_file():
            raise FileNotFoundError(f'필요한 입력 파일 없음: {path}')

    OUT.mkdir(parents=True, exist_ok=True)
    QC.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(':memory:')
    con.execute("SET memory_limit='4GB'")
    con.execute('SET threads=4')
    try:
        print('1단계: v2 Parquet의 지역코드 자료형/원래 표현 확인', flush=True)
        con.execute(f'''CREATE TEMP VIEW raw_codes AS
            SELECT *, TRIM(CAST(admin_code_raw AS VARCHAR)) AS admin10_text
            FROM read_parquet({q(SRC)})''')
        src_type = con.execute('SELECT typeof(admin_code_raw) FROM raw_codes LIMIT 1').fetchone()
        src_type = src_type[0] if src_type else '(empty)'
        diagnostic = con.execute('''
            SELECT COUNT(*) AS n,
                COUNT(*) FILTER (WHERE REGEXP_MATCHES(admin10_text, '^[0-9]{10}$')) AS original_exact10,
                COUNT(*) FILTER (WHERE admin10_text IS NULL OR admin10_text='') AS empty,
                COUNT(*) FILTER (WHERE admin10_text LIKE '%.0') AS decimal_dot_zero,
                COUNT(*) FILTER (WHERE REGEXP_MATCHES(admin10_text,'[eE][+-]?[0-9]+$')) AS scientific
            FROM raw_codes''').fetchone()
        print(f'  저장 자료형: {src_type}',flush=True)
        print(f'  총 {diagnostic[0]:,}행 / 정확한 10자리 {diagnostic[1]:,}행 / 빈 코드 {diagnostic[2]:,}행',flush=True)
        print(f'  끝이 .0 형태 {diagnostic[3]:,}행 / 지수표기 {diagnostic[4]:,}행',flush=True)
        print('  서로 다른 원본 표현 10개:', flush=True)
        for row in con.execute('SELECT DISTINCT admin10_text FROM raw_codes LIMIT 10').fetchall():
            print(f'    {row[0]!r}', flush=True)

        # v2 원본 생성 단계에서 모든 저장 행의 피해지역코드를 정규식으로 10자리 검증했음.
        # 단, CSV -> Parquet 시 DuckDB 자동형 변환에 의해 4683034000.0 등으로
        # 바뀌었다면 정수 범위/정수성/원래 표기 패턴을 함께 검사해 복원 가능.
        # 10자리 정수는 2^53 미만이므로 DOUBLE 형이어도 정수 정밀도를 보존함.
        con.execute('''CREATE TEMP VIEW normalized AS
            SELECT *,
                CASE
                    WHEN REGEXP_MATCHES(admin10_text,'^[0-9]{10}$')
                        THEN admin10_text
                    WHEN REGEXP_MATCHES(admin10_text,
                        '^([0-9]+\\.0+|[0-9]+(\\.[0-9]+)?[eE][+-]?[0-9]+)$')
                         AND TRY_CAST(admin10_text AS DOUBLE) BETWEEN 1000000000 AND 9999999999
                         AND TRY_CAST(admin10_text AS DOUBLE) = FLOOR(TRY_CAST(admin10_text AS DOUBLE))
                        THEN CAST(CAST(TRY_CAST(admin10_text AS DOUBLE) AS DECIMAL(20,0)) AS VARCHAR)
                    ELSE NULL
                END AS normalized_admin10
            FROM raw_codes''')
        rec = con.execute('''
            SELECT COUNT(*),
                COUNT(*) FILTER (WHERE REGEXP_MATCHES(COALESCE(normalized_admin10,''),'^[0-9]{10}$')),
                COUNT(*) FILTER (WHERE normalized_admin10 IS NOT NULL AND normalized_admin10 <> admin10_text)
            FROM normalized''').fetchone()
        recovered_pct = 100.0 * rec[1] / rec[0] if rec[0] else 0
        print(f'  복원 후 정상 10자리: {rec[1]:,} / {rec[0]:,} ({recovered_pct:.4f}%)',flush=True)
        print(f'  표기 복원 행: {rec[2]:,}',flush=True)
        if recovered_pct < 99.0:
            raise RuntimeError('코드 복원율이 99% 미만입니다. 원본 표기 이상을 먼저 확인해야 하므로 출력하지 않았습니다.')

        print('2단계: 공식 10자리↔8자리 연계표 및 Gold 경계 결합',flush=True)
        sources = []
        golds = []
        for y in (2024, 2025):
            sources.append(f'''
                SELECT {y} AS y, TRIM(admin_dong10) AS admin10,
                       TRIM(sgis_dong8) AS sgis8,
                       TRIM(unique_admin_to_sgis) AS official_unique
                FROM read_csv({q(MAP / f'official_admin10_to_sgis8_{y}.csv')},
                              header=true, all_varchar=true)''')
            golds.append(f'''
                SELECT {y} AS y, CAST(admin_dong_code AS VARCHAR) AS sgis8
                FROM read_parquet({q(GOLD / f'dong_daily_features_{y}.parquet')})
                GROUP BY 1,2''')
        con.execute(f"CREATE TEMP VIEW official_raw AS {' UNION ALL '.join(sources)}")
        con.execute('''CREATE TEMP VIEW official_map AS
            SELECT y, admin10, MAX(sgis8) AS sgis8
            FROM official_raw
            WHERE REGEXP_MATCHES(admin10, '^[0-9]{10}$')
              AND REGEXP_MATCHES(sgis8, '^[0-9]{8}$')
              AND official_unique='1'
            GROUP BY y, admin10
            HAVING COUNT(DISTINCT sgis8)=1''')
        con.execute(f"CREATE TEMP VIEW gold_codes AS {' UNION ALL '.join(golds)}")
        con.execute('''CREATE TEMP VIEW joined AS
            WITH base AS (
                SELECT *,
                    TRY_CAST(damage_day AS DATE) AS actual_day,
                    TRY_CAST(REPLACE(TRIM(CAST(confirmed_damage_amount_raw AS VARCHAR)),',','') AS DECIMAL(24,2)) AS amount_number,
                    TRY_CAST(history_vs_modified_disagrees AS INTEGER) AS history_time_flag
                FROM normalized
            )
            SELECT b.*,
                EXTRACT(YEAR FROM b.actual_day)::INTEGER AS actual_year,
                m.sgis8 AS sgis_dong8,
                CASE
                    WHEN b.actual_day IS NULL OR b.actual_day < DATE '2024-01-01'
                        OR b.actual_day > DATE '2025-12-31' THEN 'invalid_date'
                    WHEN b.valid_marker NOT IN ('U','V','I') THEN 'invalid_status'
                    WHEN b.history_time_flag IS NULL OR b.history_time_flag<>0 THEN 'history_order_review'
                    WHEN b.normalized_admin10 IS NULL OR NOT REGEXP_MATCHES(b.normalized_admin10,'^[0-9]{10}$') THEN 'invalid_admin_code'
                    WHEN m.sgis8 IS NULL THEN 'not_in_year_official_map'
                    WHEN g.sgis8 IS NULL THEN 'not_in_gold_boundary'
                    WHEN b.amount_number IS NULL THEN 'missing_or_invalid_amount'
                    WHEN b.amount_number<0 THEN 'negative_amount_review'
                    ELSE 'review_ready'
                END AS check_status
            FROM base b
            LEFT JOIN official_map m
                ON m.y=EXTRACT(YEAR FROM b.actual_day)::INTEGER AND m.admin10=b.normalized_admin10
            LEFT JOIN gold_codes g
                ON g.y=EXTRACT(YEAR FROM b.actual_day)::INTEGER AND g.sgis8=m.sgis8''')
        stat_rows = con.execute('''SELECT actual_year,check_status,COUNT(*)
            FROM joined GROUP BY 1,2 ORDER BY 1,3 DESC''').fetchall()
        statuses = {}
        for year,status,num in stat_rows:
            statuses.setdefault(str(year), {})[status] = num
            print(f'  {year} / {status}: {num:,}행',flush=True)
        ready = sum(n for _,s,n in stat_rows if s=='review_ready')
        if ready == 0:
            raise RuntimeError('review_ready가 0행입니다. 공식 연계표 또는 입력값 진단이 필요해 출력을 중단합니다.')

        print('3단계: 검토용 신규 결과 파일 생성',flush=True)
        con.execute(f"COPY (SELECT * FROM joined) TO {q(REVIEW)} (FORMAT PARQUET,COMPRESSION ZSTD)")
        con.execute(f'''COPY (
            SELECT actual_year AS year,sgis_dong8 AS admin_dong_code,actual_day AS day,
                COUNT(*) AS provisional_positive_record_count,
                SUM(amount_number) AS provisional_amount_raw_unit,
                1 AS provisional_damage_presence
            FROM joined WHERE check_status='review_ready' AND amount_number>0
            GROUP BY 1,2,3
        ) TO {q(DAILY)} (FORMAT PARQUET,COMPRESSION ZSTD)''')
        daily_count = con.execute(f"SELECT COUNT(*) FROM read_parquet({q(DAILY)})").fetchone()[0]
        info = {
            'status':'PROVISIONAL_REVIEW_NOT_FINAL_LABEL',
            'created_at':datetime.now().isoformat(timespec='seconds'),
            'source':str(SRC),
            'storage_code_type':src_type,
            'original_exact_10_digits':diagnostic[1],
            'recovered_10_digits':rec[1],
            'representation_recovered_rows':rec[2],
            'total_input_rows':rec[0],
            'status_counts_by_year':statuses,
            'ready_rows':ready,
            'positive_dong_day_rows':daily_count,
            'official_snapshots':{'2024':'2024-10-01','2025':'2025-12-31'},
            'gold_boundary_snapshot':'2025-06-30',
            'review_file':str(REVIEW),
            'daily_positive_file':str(DAILY),
            'warnings':[
                'This is a review table, not a final Label.',
                'Numeric-looking region codes may have changed display when CSV was converted to Parquet. Original text and normalized values are both preserved.',
                'Candidate identity and latest-history selection are not officially confirmed.',
                'History-order disputed records are excluded from ready status.',
                'Amount unit is unverified; do not label it KRW.',
                'No zero labels are generated.',
                'The 2024/2025 official maps have different snapshot dates and the Gold border is 2025-06-30.',
                'Previous 14-row raw/Silver snapshot discrepancy is separate.',
            ]
        }
        REPORT.write_text(json.dumps(info,ensure_ascii=False,indent=2),encoding='utf-8')
        print('\n완료:',flush=True)
        print('  지역코드 검토:', REVIEW)
        print('  양수 피해 예비집계:', DAILY)
        print('  진단 보고서:', REPORT)
        print(f'  검토 가능 {ready:,}행 / 양수 일별 {daily_count:,}행',flush=True)
    finally:
        con.close()

if __name__=='__main__':
    main()
