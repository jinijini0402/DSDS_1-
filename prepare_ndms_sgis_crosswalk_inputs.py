"""Prepare compact, non-identifying inputs for official NDMS 10-digit -> SGIS 8-digit code linkage.

Run from the heavy-rain-prediction project root:
    python -u prepare_ndms_sgis_crosswalk_inputs.py

Requires: duckdb (already used by earlier project scripts).

IMPORTANT:
- No 10-to-8 code conversion is guessed or applied.
- No monetary amounts are summed; no duplicates are deleted.
- The output is an audit/reference set, NOT the finished crosswalk.
"""
import csv
import json
from pathlib import Path

import duckdb

RAIN = Path('data/silver/labels/rain_damage')
MAPPING = Path('data/silver/mappings/nationwide_aws_dong')
OUT = Path('data/silver/mappings/ndms_sgis_crosswalk')
OUT.mkdir(parents=True, exist_ok=True)


def quote(path):
    return "'" + path.as_posix().replace("'", "''") + "'"


def main():
    files = {
        'private': RAIN / 'private_rain_2024_2025_review.parquet',
        'public': RAIN / 'public_rain_2024_2025_review.parquet',
    }
    mapping_files = {
        2024: MAPPING / 'dong_station_distance_2024.csv',
        2025: MAPPING / 'dong_station_distance_2025.csv',
    }
    for p in list(files.values()) + list(mapping_files.values()):
        if not p.is_file():
            raise FileNotFoundError(f'파일이 없습니다: {p}')

    con = duckdb.connect()
    con.execute("SET memory_limit = '4GB'")
    con.execute('SET threads = 4')

    # Only year/code/dataset are extracted. No personal address or damage narrative is output.
    parts = []
    for kind, path in files.items():
        parts.append(f"""
            SELECT '{kind}' AS dataset,
                   CAST(YEAR(CAST(damage_day AS DATE)) AS VARCHAR) AS year,
                   TRIM(COALESCE(CAST(admin_code_clean AS VARCHAR), '')) AS ndms_dong10,
                   NULLIF(TRIM(COALESCE(CAST(facility_serial_number AS VARCHAR), '')), '')
                       AS candidate_facility_id,
                   NULLIF(TRIM(COALESCE(CAST(disaster_serial_number AS VARCHAR), '')), '')
                       AS candidate_disaster_id
            FROM read_parquet({quote(path)})
            WHERE CAST(damage_day AS DATE) BETWEEN DATE '2024-01-01' AND DATE '2025-12-31'
        """)
    con.execute('CREATE TEMP VIEW rain_rows AS ' + '\n UNION ALL \n'.join(parts))

    print('1단계: 피해자료에 등장하는 10자리 지역코드 집계', flush=True)
    out_codes = OUT / 'ndms_dong10_frequency_2024_2025.csv'
    con.execute(f"""
        COPY (
            SELECT year, ndms_dong10,
                   SUM(CASE WHEN dataset = 'private' THEN 1 ELSE 0 END) AS private_records,
                   SUM(CASE WHEN dataset = 'public' THEN 1 ELSE 0 END) AS public_records,
                   COUNT(*) AS total_records
            FROM rain_rows
            WHERE REGEXP_MATCHES(ndms_dong10, '^[0-9]{{10}}$')
            GROUP BY year, ndms_dong10
            ORDER BY year, total_records DESC, ndms_dong10
        ) TO {quote(out_codes)} (HEADER, DELIMITER ',')
    """)

    out_invalid = OUT / 'invalid_ndms_region_codes.csv'
    con.execute(f"""
        COPY (
            SELECT year, dataset, ndms_dong10 AS invalid_raw_code,
                   COUNT(*) AS record_count
            FROM rain_rows
            WHERE NOT REGEXP_MATCHES(ndms_dong10, '^[0-9]{{10}}$')
            GROUP BY 1,2,3
            ORDER BY year, dataset, record_count DESC
        ) TO {quote(out_invalid)} (HEADER, DELIMITER ',')
    """)

    print('2단계: Gold 경계에 사용한 SGIS 8자리 코드 목록 생성', flush=True)
    sgis = []
    for year, path in mapping_files.items():
        sgis.append(f"""
            SELECT '{year}' AS year,
                   TRIM(admin_dong_code) AS sgis_dong8,
                   TRIM(admin_dong_name) AS sgis_dong_name
            FROM read_csv({quote(path)}, header=true, all_varchar=true)
        """)
    con.execute('CREATE TEMP VIEW sgis_rows AS ' + '\n UNION ALL \n'.join(sgis))
    out_sgis = OUT / 'sgis_dong8_master_2024_2025.csv'
    con.execute(f"""
        COPY (
            SELECT DISTINCT year, sgis_dong8, sgis_dong_name
            FROM sgis_rows
            ORDER BY year, sgis_dong8, sgis_dong_name
        ) TO {quote(out_sgis)} (HEADER, DELIMITER ',')
    """)

    print('3단계: 데이터 범위 및 식별자 보존 여부 진단', flush=True)
    source_stats = con.execute("""
        SELECT dataset, year,
               COUNT(*) AS rows_total,
               COUNT(DISTINCT ndms_dong10) AS unique_source_codes,
               SUM(CASE WHEN REGEXP_MATCHES(ndms_dong10, '^[0-9]{10}$') THEN 0 ELSE 1 END) AS malformed_codes,
               SUM(CASE WHEN candidate_facility_id IS NULL THEN 1 ELSE 0 END) AS facility_id_missing,
               SUM(CASE WHEN candidate_disaster_id IS NULL THEN 1 ELSE 0 END) AS disaster_id_missing
        FROM rain_rows
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).fetchall()
    master_stats = con.execute("""
        SELECT year, COUNT(DISTINCT sgis_dong8) AS sgis_dong_count
        FROM sgis_rows
        GROUP BY year ORDER BY year
    """).fetchall()

    report = {
        'stage': 'Prepare lookup inputs; no crosswalk applied',
        'damage_source_code': 'DAM_RGN_DONG_CD (private) / DAM_RGN_DONG_CD_1 (public)',
        'source_code_digits': 10,
        'destination_code': 'SGIS ADM_CD from yearly dong_station_distance CSV',
        'destination_code_digits': 8,
        'source_row_counts': [
            {'dataset': kind, 'year': year,
             'record_count': n, 'unique_source_codes': distinct,
             'malformed_source_codes': malformed,
             'candidate_facility_id_missing': fmissing,
             'candidate_disaster_id_missing': dmissing}
            for kind, year, n, distinct, malformed, fmissing, dmissing in source_stats
        ],
        'sgis_code_counts': {year: n for year, n in master_stats},
        'outputs': [str(out_codes), str(out_sgis), str(out_invalid)],
        'important_notes': [
            '10자리 행정기관코드와 SGIS 8자리 분류코드는 같은 번호체계가 아닙니다.',
            '법정동 코드와 행정동 코드 역시 구별해야 합니다.',
            '2024년과 2025년의 공식 기준시점별 연계DB를 확보해야 실제 변환표를 만들 수 있습니다.',
            '일부 원본 식별자(DAM_SN, VCTM_SN, DAM_HSTRY_SN, PBLC_FCLT_SN 등)가 앞선 후보 CSV 생성 중 누락되었습니다.',
            '반복된 날짜/지역/금액을 근거로 중복행을 삭제하거나 피해액을 합산해서는 안 됩니다.',
            '2025-06-30 SGIS 경계로 2024년까지 매핑한 현재 Gold 기상자료는 별도 과거경계 검증이 필요합니다.',
        ]
    }
    out_report = OUT / 'crosswalk_inputs_report.json'
    out_report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    con.close()

    for item in report['source_row_counts']:
        print(f"{item['dataset']} {item['year']}: {item['record_count']:,}행 / "
              f"고유 10자리 코드 {item['unique_source_codes']:,}개 / "
              f"형식 오류 {item['malformed_source_codes']:,}행 / "
              f"시설 식별자 누락 {item['candidate_facility_id_missing']:,}행", flush=True)
    print('SGIS 행정동 코드:', report['sgis_code_counts'], flush=True)
    print('완료:', OUT, flush=True)
    print('주의: 이 파일들은 실제 10자리↔8자리 코드 변환표가 아닙니다.', flush=True)


if __name__ == '__main__':
    main()
