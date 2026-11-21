"""2024~2025 사유시설/공공시설 일별 피해 예비집계 품질검사 및 병렬 결합.



실행:

  python -u combine_rain_property_provisional_sources_recovered_2025.py



이 코드는 피해액을 서로 더하거나, 관측이 없는 날짜에 0을 채우지 않습니다.

정식 Label이 아닌 출처별 관측 테이블을 만듭니다.

필요 패키지: duckdb (이미 기존 프로젝트에서 사용).

"""

import json

from datetime import datetime

from pathlib import Path



import duckdb



ROOT = Path('data/silver/labels/rain_damage')

QUALITY = Path('data/silver/quality_checks/labels')

PRIVATE = ROOT / 'private_rain_daily_positive_recovered_2025_PROVISIONAL.parquet'

PUBLIC = ROOT / 'public_rain_daily_observed_PROVISIONAL.parquet'

PUBLIC_REVIEW = ROOT / 'public_rain_sgis_review.parquet'

OUTPUT = ROOT / 'rain_property_sources_daily_recovered_2025_PROVISIONAL.parquet'

REPORT = QUALITY / 'rain_property_sources_daily_recovered_2025_provisional_audit.json'





def sql_path(p):

    return "'" + p.as_posix().replace("'", "''") + "'"





def as_dicts(con, query):

    cursor = con.execute(query)

    cols = [i[0] for i in cursor.description]

    return [dict(zip(cols, record)) for record in cursor.fetchall()]





def counts_by_year(con, view):

    return as_dicts(con, f'SELECT year, COUNT(*) AS dong_days FROM {view} GROUP BY year ORDER BY year')





def check_keys(con, view):

    result = con.execute(f'''

        SELECT COUNT(*), COUNT(DISTINCT (year, admin_dong_code, day)),

               COUNT(*) FILTER (WHERE year NOT IN (2024, 2025)

                    OR NOT REGEXP_MATCHES(admin_dong_code, '^[0-9]{{8}}$')

                    OR day IS NULL)

        FROM {view}

    ''').fetchone()

    n, unique, invalid = result

    if n != unique or invalid:

        raise RuntimeError(f'{view} 키 오류: 행={n}, 고유키={unique}, 날짜/지역 오류={invalid}')

    return n





def main():

    for p in (PRIVATE, PUBLIC, PUBLIC_REVIEW):

        if not p.is_file():

            raise FileNotFoundError(f'필요한 파일이 없습니다: {p}')

    QUALITY.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(':memory:')

    con.execute("SET memory_limit='4GB'")

    con.execute('SET threads=4')

    try:

        print('1단계: 사유시설·공공시설 일별 Parquet 읽기 및 키 검증', flush=True)

        con.execute(f'''

            CREATE TEMP VIEW p AS

            SELECT TRY_CAST(year AS INTEGER) AS year,

                   TRIM(CAST(admin_dong_code AS VARCHAR)) AS admin_dong_code,

                   TRY_CAST(day AS DATE) AS day,

                   TRY_CAST(provisional_positive_record_count AS BIGINT) AS private_positive_record_count,

                   TRY_CAST(provisional_amount_raw_unit AS DECIMAL(38,2)) AS private_known_positive_amount_raw_unit

            FROM read_parquet({sql_path(PRIVATE)})

        ''')

        con.execute(f'''

            CREATE TEMP VIEW u AS

            SELECT TRY_CAST(year AS INTEGER) AS year,

                   TRIM(CAST(admin_dong_code AS VARCHAR)) AS admin_dong_code,

                   TRY_CAST(day AS DATE) AS day,

                   TRY_CAST(public_record_rows_y AS BIGINT) AS public_record_rows_y,

                   TRY_CAST(positive_amount_rows AS BIGINT) AS public_positive_amount_rows,

                   TRY_CAST(zero_amount_rows AS BIGINT) AS public_zero_amount_rows,

                   TRY_CAST(missing_or_negative_amount_rows AS BIGINT) AS public_missing_or_negative_amount_rows,

                   TRY_CAST(known_amount_sum_raw_unit AS DECIMAL(38,2)) AS public_known_positive_amount_raw_unit,

                   TRIM(CAST(amount_complete_among_selected_y AS VARCHAR)) AS public_amount_complete_y,

                   TRY_CAST(confirmation_marker_0 AS BIGINT) AS confirmation_marker_0,

                   TRY_CAST(confirmation_marker_1 AS BIGINT) AS confirmation_marker_1,

                   TRY_CAST(confirmation_marker_2 AS BIGINT) AS confirmation_marker_2,

                   TRY_CAST(confirmation_marker_other AS BIGINT) AS confirmation_marker_other

            FROM read_parquet({sql_path(PUBLIC)})

        ''')

        n_private = check_keys(con, 'p')

        n_public = check_keys(con, 'u')

        private_errors = con.execute('''

            SELECT COUNT(*) FROM p

            WHERE private_positive_record_count IS NULL OR private_positive_record_count <= 0

               OR private_known_positive_amount_raw_unit IS NULL

               OR private_known_positive_amount_raw_unit <= 0

        ''').fetchone()[0]

        if private_errors:

            raise RuntimeError(f'사유시설 일별 예비집계에서 비정상 양수 기록 {private_errors}개')

        public_errors = con.execute('''

            SELECT COUNT(*) FROM u

            WHERE public_record_rows_y IS NULL OR public_record_rows_y <= 0

               OR public_positive_amount_rows IS NULL OR public_zero_amount_rows IS NULL

               OR public_missing_or_negative_amount_rows IS NULL

               OR public_known_positive_amount_raw_unit IS NULL

               OR public_positive_amount_rows < 0 OR public_zero_amount_rows < 0

               OR public_missing_or_negative_amount_rows < 0

               OR public_known_positive_amount_raw_unit < 0

               OR public_record_rows_y <> public_positive_amount_rows

                   + public_zero_amount_rows + public_missing_or_negative_amount_rows

               OR public_amount_complete_y NOT IN ('Y','N')

               OR (public_missing_or_negative_amount_rows = 0 AND public_amount_complete_y <> 'Y')

               OR (public_missing_or_negative_amount_rows > 0 AND public_amount_complete_y <> 'N')

               OR confirmation_marker_0 IS NULL OR confirmation_marker_1 IS NULL

               OR confirmation_marker_2 IS NULL OR confirmation_marker_other IS NULL

               OR public_record_rows_y <> confirmation_marker_0 + confirmation_marker_1

                   + confirmation_marker_2 + confirmation_marker_other

        ''').fetchone()[0]

        if public_errors:

            raise RuntimeError(f'공공시설 일별 집계 내부 불일치 {public_errors}개. 기존 파일을 먼저 검증해야 합니다.')

        print(f'  사유시설 양수 일별 조합: {n_private:,}개', flush=True)

        print(f'  공공시설 사용 Y 일별 조합: {n_public:,}개', flush=True)



        print('2단계: 공공시설 행 단위 검토자료와 일별 집계 재검산', flush=True)

        # 원본 공공시설 스크립트는 사용 Y+지역 매핑 성공을 기준으로 관측 일별 파일을 만듦.

        # USE_YN=N, 미매핑, 미확정 의미는 임의 합산하지 않음.

        con.execute(f'''

            CREATE TEMP VIEW public_expected AS

            WITH r AS (

                SELECT CAST(LEFT(damage_day, 4) AS INTEGER) AS year,

                       TRIM(CAST(sgis_dong8 AS VARCHAR)) AS admin_dong_code,

                       TRY_CAST(damage_day AS DATE) AS day,

                       TRIM(CAST(confirmation_marker AS VARCHAR)) AS cf,

                       TRY_CAST(amount_numeric_raw_unit AS DECIMAL(38,2)) AS a

                FROM read_parquet({sql_path(PUBLIC_REVIEW)})

                WHERE check_status IN (

                    'mapped_y_zero','mapped_y_positive_unverified_confirmation',

                    'missing_amount','negative_amount_review'

                )

            )

            SELECT year, admin_dong_code, day,

                   COUNT(*) AS n,

                   COUNT(*) FILTER (WHERE a>0) AS pos,

                   COUNT(*) FILTER (WHERE a=0) AS zeros,

                   COUNT(*) FILTER (WHERE a IS NULL OR a<0) AS missing,

                   SUM(CASE WHEN a>0 THEN a ELSE CAST(0 AS DECIMAL(38,2)) END) AS amount,

                   COUNT(*) FILTER (WHERE cf='0') AS cf0,

                   COUNT(*) FILTER (WHERE cf='1') AS cf1,

                   COUNT(*) FILTER (WHERE cf='2') AS cf2,

                   COUNT(*) FILTER (WHERE cf NOT IN ('0','1','2')) AS cfother

            FROM r GROUP BY 1,2,3

        ''')

        mismatches = con.execute('''

            SELECT COUNT(*)

            FROM public_expected e FULL OUTER JOIN u

              ON e.year=u.year AND e.admin_dong_code=u.admin_dong_code AND e.day=u.day

            WHERE e.year IS NULL OR u.year IS NULL

               OR e.n IS DISTINCT FROM u.public_record_rows_y

               OR e.pos IS DISTINCT FROM u.public_positive_amount_rows

               OR e.zeros IS DISTINCT FROM u.public_zero_amount_rows

               OR e.missing IS DISTINCT FROM u.public_missing_or_negative_amount_rows

               OR e.amount IS DISTINCT FROM u.public_known_positive_amount_raw_unit

               OR e.cf0 IS DISTINCT FROM u.confirmation_marker_0

               OR e.cf1 IS DISTINCT FROM u.confirmation_marker_1

               OR e.cf2 IS DISTINCT FROM u.confirmation_marker_2

               OR e.cfother IS DISTINCT FROM u.confirmation_marker_other

        ''').fetchone()[0]

        if mismatches:

            sample = as_dicts(con, '''

                SELECT e.year AS source_year,e.admin_dong_code AS source_code,e.day AS source_day,

                       u.year AS daily_year,u.admin_dong_code AS daily_code,u.day AS daily_day,

                       e.n AS source_records,u.public_record_rows_y AS daily_records

                FROM public_expected e FULL OUTER JOIN u

                  ON e.year=u.year AND e.admin_dong_code=u.admin_dong_code AND e.day=u.day

                WHERE e.year IS NULL OR u.year IS NULL

                   OR e.n IS DISTINCT FROM u.public_record_rows_y

                LIMIT 5

            ''')

            raise RuntimeError(f'공공시설 검토원본과 일별 집계가 {mismatches}개 조합에서 불일치. 일부 샘플: {sample}')

        print('  공공시설 원본 재집계와 일별 집계 불일치: 0개', flush=True)



        print('3단계: 두 자료를 금액 합산 없이 행정동·날짜별로 병렬 결합', flush=True)

        con.execute('''

            CREATE TEMP VIEW combined AS

            SELECT

                COALESCE(p.year, u.year) AS year,

                COALESCE(p.admin_dong_code, u.admin_dong_code) AS admin_dong_code,

                COALESCE(p.day, u.day) AS day,

                CASE WHEN p.year IS NOT NULL THEN 1 ELSE NULL END AS private_positive_observed,

                p.private_positive_record_count,

                p.private_known_positive_amount_raw_unit,

                CASE WHEN u.year IS NOT NULL THEN 1 ELSE NULL END AS public_y_observed,

                CASE WHEN u.public_positive_amount_rows>0 THEN 1 ELSE NULL END AS public_positive_observed,

                u.public_record_rows_y,

                u.public_positive_amount_rows,

                u.public_zero_amount_rows,

                u.public_missing_or_negative_amount_rows,

                u.public_known_positive_amount_raw_unit,

                u.public_amount_complete_y,

                u.confirmation_marker_0,

                u.confirmation_marker_1,

                u.confirmation_marker_2,

                u.confirmation_marker_other,

                CASE WHEN p.year IS NOT NULL OR u.public_positive_amount_rows>0

                     THEN 1 ELSE NULL END AS any_positive_amount_observed,

                CASE WHEN p.year IS NOT NULL AND u.year IS NOT NULL THEN 'BOTH'

                     WHEN p.year IS NOT NULL THEN 'PRIVATE_POSITIVE_ONLY'

                     ELSE 'PUBLIC_Y_ONLY' END AS observation_sources

            FROM p FULL OUTER JOIN u

              ON p.year=u.year AND p.admin_dong_code=u.admin_dong_code AND p.day=u.day

        ''')

        n_combined = check_keys(con, 'combined')

        overlap = con.execute('''SELECT COUNT(*) FROM combined

            WHERE observation_sources='BOTH' ''').fetchone()[0]

        if n_combined != n_private + n_public - overlap:

            raise RuntimeError('병렬 결합 후 행 수가 예상과 다릅니다.')

        summary = as_dicts(con, '''

            SELECT year, COUNT(*) AS observed_dong_days,

                   COUNT(*) FILTER (WHERE observation_sources='BOTH') AS both_sources,

                   COUNT(*) FILTER (WHERE observation_sources='PRIVATE_POSITIVE_ONLY') AS private_only,

                   COUNT(*) FILTER (WHERE observation_sources='PUBLIC_Y_ONLY') AS public_only,

                   COUNT(*) FILTER (WHERE any_positive_amount_observed=1) AS any_positive_observed,

                   COUNT(*) FILTER (WHERE public_missing_or_negative_amount_rows>0) AS public_incomplete_amount_dong_days

            FROM combined GROUP BY 1 ORDER BY 1

        ''')

        con.execute(f'''

            COPY (SELECT * FROM combined ORDER BY year,admin_dong_code,day)

            TO {sql_path(OUTPUT)} (FORMAT PARQUET, COMPRESSION ZSTD)

        ''')

        report = {

            'status': 'PROVISIONAL_SOURCES_NOT_FINAL_LABEL',

            'created_at': datetime.now().isoformat(timespec='seconds'),

            'input_private': str(PRIVATE),

            'input_public': str(PUBLIC),

            'input_public_review': str(PUBLIC_REVIEW),

            'private_positive_dong_days': n_private,

            'public_y_observed_dong_days': n_public,

            'combined_observed_dong_days': n_combined,

            'overlapping_dong_days': overlap,

            'public_day_source_reaggregation_discrepancies': mismatches,

            'year_summary': summary,

            'output': str(OUTPUT),

            'limitations': [

                'Only observed source dong-days are included; missing days must not be interpreted as no damage.',

                'No zero-damage label is created. NULL means not observed or not established.',

                'Private and public damage amount values remain separate, in unverified source units; do not sum them.',

                'Public-facility CFMTN_SPTR meanings and full historical page completeness are not confirmed.',

                'Public USE_YN=N records are preserved only in the source review, not in the daily observed table.',

                'Private identity/latest-history rules and source page 14-row discrepancy remain provisional.',

                'The official mapping snapshots and Gold boundary are not exact same-day boundaries.',

                'Before supervised learning, define the label date, completeness of negatives, and prediction horizon to avoid leakage.'

            ],

        }

        REPORT.write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')

        print(f'  두 자료가 모두 관측된 조합: {overlap:,}개',flush=True)

        print(f'  합쳐진 서로 다른 관측 조합: {n_combined:,}개',flush=True)

        for r in summary:

            print(f"  {r['year']}: {r['observed_dong_days']:,}조합 (사유만 {r['private_only']:,}, 공공만 {r['public_only']:,}, 공통 {r['both_sources']:,})", flush=True)

        print('결합 파일:',OUTPUT,flush=True)

        print('검증 보고서:',REPORT,flush=True)

        print('주의: 금액 합산·0 레이블 생성·학습용 목표변수 확정은 하지 않았습니다.',flush=True)

    finally:

        con.close()



if __name__ == '__main__':

    main()