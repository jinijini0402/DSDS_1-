"""2024–2025 NDMS 피해 후보에서 명시적 '호우'(1001) 기록만 선별.

주의:
- 업로드된 '자연재난-사유시설피해정보코드.xlsx'에서 MSTN_TYPE_CD=1001은 호우.
- 공공시설 NDMS_MSTN_TYPE_CD에서 동일 코드 체계 적용은 별도 확인 필요.
- 이 파일은 최종 피해 Label이 아니라 코드 확인 후 공간/중복 검증용 Silver 추출물.
"""
import json
from pathlib import Path
import duckdb

SOURCE_DIR = Path('data/silver/labels/candidates')
OUTPUT_DIR = Path('data/silver/labels/rain_damage')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def literal(path):
    return "'" + path.as_posix().replace("'", "''") + "'"


def run_one(con, dataset):
    path = SOURCE_DIR / f'{dataset}_2024_2025_candidates.csv'
    if not path.is_file():
        raise FileNotFoundError(f'파일을 찾지 못했습니다: {path}')

    print(f'\n[{dataset}] 호우(1001) 후보 추출 중...', flush=True)
    con.execute('DROP VIEW IF EXISTS candidates')
    con.execute('DROP VIEW IF EXISTS rain_candidates')
    con.execute(f"""
        CREATE TEMP VIEW candidates AS
        SELECT
            *,
            TRIM(COALESCE(disaster_type_code, '')) AS disaster_code_clean,
            TRY_STRPTIME(
                LEFT(REGEXP_REPLACE(COALESCE(damage_date,''), '[^0-9]', '', 'g'), 8),
                '%Y%m%d'
            )::DATE AS damage_day,
            TRIM(COALESCE(admin_code_raw, '')) AS admin_code_clean,
            TRY_CAST(
                REPLACE(TRIM(COALESCE(confirmed_damage_amount_raw,'')), ',', '')
                AS DECIMAL(24,2)
            ) AS amount_numeric
        FROM read_csv({literal(path)}, header=true, all_varchar=true)
    """)
    con.execute("""
        CREATE TEMP VIEW rain_candidates AS
        SELECT
            *,
            (damage_day IS NOT NULL AND
             YEAR(damage_day)::VARCHAR <> TRIM(COALESCE(disaster_year,'')))
                AS disaster_year_mismatch,
            (LENGTH(admin_code_clean)=10 AND
             REGEXP_MATCHES(admin_code_clean, '^[0-9]{10}$'))
                AS admin_code_format_valid
        FROM candidates
        WHERE disaster_code_clean = '1001'
    """)

    summary = con.execute("""
        SELECT
            COUNT(*) AS code1001_records,
            SUM(CASE WHEN damage_day BETWEEN DATE '2024-01-01'
                     AND DATE '2025-12-31' THEN 1 ELSE 0 END) AS selected,
            SUM(CASE WHEN damage_day IS NULL THEN 1 ELSE 0 END) AS invalid_date,
            SUM(CASE WHEN damage_day IS NOT NULL AND NOT
                     (damage_day BETWEEN DATE '2024-01-01'
                      AND DATE '2025-12-31') THEN 1 ELSE 0 END) AS outside_period,
            SUM(CASE WHEN disaster_year_mismatch THEN 1 ELSE 0 END) AS year_mismatch,
            SUM(CASE WHEN NOT admin_code_format_valid THEN 1 ELSE 0 END)
                AS invalid_admin_code,
            SUM(CASE WHEN amount_numeric IS NULL THEN 1 ELSE 0 END)
                AS amount_not_numeric
        FROM rain_candidates
    """).fetchone()

    out = OUTPUT_DIR / f'{dataset}_rain_2024_2025_review.parquet'
    con.execute(f"""
        COPY (
            SELECT *
            FROM rain_candidates
            WHERE damage_day BETWEEN DATE '2024-01-01'
                                 AND DATE '2025-12-31'
        ) TO {literal(out)} (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    result = dict(zip([
        'code1001_records', 'selected_2024_2025', 'invalid_date',
        'outside_period', 'disaster_year_mismatch', 'invalid_admin_code',
        'amount_not_numeric'
    ], summary))
    result['output_file'] = str(out)
    result['status'] = 'provisional_code_match' if dataset == 'public' else 'code_confirmed_from_private_facility_workbook'
    for k, v in result.items():
        print(f'  {k}: {v}', flush=True)
    return result


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute('SET threads=4')
    try:
        report = {
            'selected_code': '1001',
            'selected_code_name': '호우',
            'code_source': '사용자 제공 자연재난-사유시설피해정보코드.xlsx, MSTN_TYPE_CD',
            'date_rule': '실제 damage_date가 2024-01-01부터 2025-12-31 사이',
            'notes': [
                '공공시설 NDMS_MSTN_TYPE_CD와 사유시설 MSTN_TYPE_CD가 동일 표인지 추가 검증 필요',
                '지역코드 형식 오류·재난연도 불일치 기록은 검토용 플래그를 유지',
                '피해액 단위와 행 중복 미검증: 최종 피해액 합계/학습 Label로 사용 금지',
                '1008(태풍·호우), 1016(홍수), 1041(풍수해)는 엄격한 호우(1001) 선별에서 제외',
            ],
            'datasets': {}
        }
        for dataset in ('private', 'public'):
            report['datasets'][dataset] = run_one(con, dataset)
        output_report = OUTPUT_DIR / 'rain_filter_report.json'
        output_report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        print('\n보고서:', output_report)
    finally:
        con.close()


if __name__ == '__main__':
    main()
