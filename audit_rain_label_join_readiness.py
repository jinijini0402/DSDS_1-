"""호우(1001) Silver Parquet의 지역코드 연결 가능성과 중복 위험을 검사.

- 입력을 변경하지 않으며 Label을 생성/합산하지 않습니다.
- 원본 행정기관 10자리 코드와 SGIS 8자리 코드는 서로 다를 수 있습니다.
- 앞 8자리 비교는 진단 전용이며 매핑으로 사용하지 않습니다.

실행: python -u audit_rain_label_join_readiness.py
"""
import csv
import json
from pathlib import Path

import duckdb

RAIN = Path("data/silver/labels/rain_damage")
MAPPING = Path("data/silver/mappings/nationwide_aws_dong")
OUTPUT = Path("data/silver/quality_checks/labels")
OUTPUT.mkdir(parents=True, exist_ok=True)


def sqlstr(path):
    return "'" + path.as_posix().replace("'", "''") + "'"


def to_int(value):
    return int(value or 0)


def main():
    source_files = {
        "private": RAIN / "private_rain_2024_2025_review.parquet",
        "public": RAIN / "public_rain_2024_2025_review.parquet",
    }
    map_files = {
        2024: MAPPING / "dong_station_distance_2024.csv",
        2025: MAPPING / "dong_station_distance_2025.csv",
    }
    for path in list(source_files.values()) + list(map_files.values()):
        if not path.is_file():
            raise FileNotFoundError(f"입력 파일 없음: {path}")

    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=4")

    con.execute(f"""
        CREATE VIEW sgis_codes AS
        SELECT '2024' AS target_year, TRIM(admin_dong_code) AS sgis_code
          FROM read_csv({sqlstr(map_files[2024])}, all_varchar=true)
        UNION
        SELECT '2025' AS target_year, TRIM(admin_dong_code) AS sgis_code
          FROM read_csv({sqlstr(map_files[2025])}, all_varchar=true)
    """)

    report = {
        "purpose": "호우 1001 후보의 피해일/피해액/코드 연결 가능성 진단",
        "year_key": "damage_day (실제 피해일)",
        "warning": (
            "행정안전부 10자리 행정기관코드와 SGIS 8자리 행정구역분류코드의 "
            "앞 8자리는 직접 결합용으로 사용하면 안 됨"
        ),
        "public_code_status": "공공시설 1001 동일 코드체계 별도 확인 필요",
        "datasets": {},
    }

    for dataset, path in source_files.items():
        print(f"\n===== {dataset} 코드/피해 검증 =====", flush=True)
        con.execute("DROP VIEW IF EXISTS mapped")
        con.execute("DROP VIEW IF EXISTS rain")
        con.execute(f"""
            CREATE VIEW rain AS
            SELECT
                CAST(damage_day AS DATE) AS damage_day,
                TRIM(COALESCE(admin_code_clean, '')) AS admin_code,
                TRY_CAST(amount_numeric AS DOUBLE) AS damage_amount,
                TRIM(COALESCE(disaster_year, '')) AS disaster_year,
                COALESCE(disaster_year_mismatch, FALSE) AS year_mismatch,
                TRIM(COALESCE(disaster_serial_number, '')) AS disaster_serial,
                TRIM(COALESCE(facility_serial_number, '')) AS facility_serial,
                source_page,
                source_index
            FROM read_parquet({sqlstr(path)})
        """)
        con.execute("""
            CREATE VIEW mapped AS
            SELECT r.*,
                   CAST(YEAR(r.damage_day) AS VARCHAR) AS target_year,
                   LEFT(r.admin_code, 8) AS prefix8,
                   m.sgis_code IS NOT NULL AS prefix8_matches_sgis
            FROM rain r
            LEFT JOIN sgis_codes m
              ON m.target_year = CAST(YEAR(r.damage_day) AS VARCHAR)
             AND m.sgis_code = LEFT(r.admin_code, 8)
        """)

        results = con.execute("""
            SELECT
                target_year,
                COUNT(*) AS rows_total,
                SUM(CASE WHEN REGEXP_MATCHES(admin_code, '^[0-9]{10}$')
                     THEN 1 ELSE 0 END) AS valid_10_digit_code,
                SUM(CASE WHEN RIGHT(admin_code, 2) = '00'
                     THEN 1 ELSE 0 END) AS ends_in_00,
                SUM(CASE WHEN prefix8_matches_sgis
                     THEN 1 ELSE 0 END) AS matching_prefix8_rows,
                COUNT(DISTINCT admin_code) AS unique_10_digit_codes,
                SUM(CASE WHEN damage_amount > 0 THEN 1 ELSE 0 END) AS positive_amount_rows,
                SUM(CASE WHEN damage_amount = 0 THEN 1 ELSE 0 END) AS zero_amount_rows,
                SUM(CASE WHEN damage_amount IS NULL THEN 1 ELSE 0 END) AS null_amount_rows,
                SUM(CASE WHEN damage_amount < 0 THEN 1 ELSE 0 END) AS negative_amount_rows,
                SUM(CASE WHEN year_mismatch THEN 1 ELSE 0 END) AS disaster_year_mismatch_rows,
                SUM(CASE WHEN facility_serial = '' THEN 1 ELSE 0 END) AS missing_facility_serial_rows
            FROM mapped
            GROUP BY target_year
            ORDER BY target_year
        """).fetchall()
        fields = ["year", "rows_total", "valid_10_digit_code", "ends_in_00",
                  "matching_prefix8_rows", "unique_10_digit_codes",
                  "positive_amount_rows", "zero_amount_rows", "null_amount_rows",
                  "negative_amount_rows", "disaster_year_mismatch_rows",
                  "missing_facility_serial_rows"]
        yearly = []
        for data in results:
            row = dict(zip(fields, [data[0]] + [to_int(x) for x in data[1:]]))
            row["prefix8_match_rate_pct"] = round(
                100 * row["matching_prefix8_rows"] / max(row["rows_total"], 1), 2
            )
            yearly.append(row)
            print(f"{row['year']}년: {row['rows_total']:,}건 / "
                  f"10자리 형식 {row['valid_10_digit_code']:,}건 / "
                  f"앞 8자리 SGIS 일치 {row['prefix8_match_rate_pct']}% / "
                  f"피해액 결측 {row['null_amount_rows']:,}건 / "
                  f"재난연도 불일치 {row['disaster_year_mismatch_rows']:,}건")

        # '중복' 확정은 불가: 동일 사건·지역·날짜·금액 조합이 얼마나 반복되는지만 확인.
        repeated = con.execute("""
            SELECT COUNT(*) AS repeated_groups,
                   COALESCE(SUM(n-1), 0) AS extra_rows_in_repeated_groups
            FROM (
                SELECT COUNT(*) AS n
                FROM rain
                GROUP BY damage_day, admin_code, damage_amount,
                         disaster_serial, facility_serial
                HAVING COUNT(*) > 1
            ) x
        """).fetchone()
        print(f"반복 조합: {to_int(repeated[0]):,}개 그룹, "
              f"추가 행 {to_int(repeated[1]):,}개 (실제 중복 확정 아님)")

        # 코드별 요약만 저장. 원본 레코드나 개인정보는 기록하지 않음.
        examples_path = OUTPUT / f"{dataset}_rain_region_code_check.csv"
        con.execute(f"""
            COPY (
                SELECT target_year, admin_code AS source_10digit_code,
                       prefix8 AS diagnostic_prefix8,
                       BOOL_OR(prefix8_matches_sgis) AS prefix8_in_sgis,
                       COUNT(*) AS damage_record_count
                FROM mapped
                GROUP BY 1, 2, 3
                ORDER BY damage_record_count DESC
            ) TO {sqlstr(examples_path)} (HEADER, DELIMITER ',')
        """)

        report["datasets"][dataset] = {
            "yearly": yearly,
            "repeated_combination_groups_not_proven_duplicates": to_int(repeated[0]),
            "extra_rows_in_repeated_combinations": to_int(repeated[1]),
            "source_10digit_code_profile": examples_path.as_posix(),
        }

    con.close()
    report_path = OUTPUT / "rain_label_join_readiness_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\n검증 완료:", report_path)
    print("주의: 앞 8자리와 SGIS 코드 일치율은 진단용이며, 지역코드 변환표가 아닙니다.")


if __name__ == "__main__":
    main()
