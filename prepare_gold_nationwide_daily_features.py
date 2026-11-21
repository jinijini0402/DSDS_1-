
import json
from pathlib import Path

import duckdb

# ======================================
# 1. 입력 및 출력 경로
# ======================================

SILVER = Path("data/silver/weather/station_hourly")
MAPPING = Path("data/silver/mappings/nationwide_aws_dong")
GOLD = Path("data/gold/weather")

GOLD.mkdir(parents=True, exist_ok=True)

HOURLY_FILES = [
    SILVER / "nationwide_station_hourly_2024.csv",
    SILVER / "nationwide_station_hourly_2025.csv",
]

MIN_WEATHER_HOURS = 18
REQUIRED_RAIN_HOURS = 24
MAX_DISTANCE_M = 30000


def sql_path(path):
    return "'" + path.as_posix().replace("'", "''") + "'"


def main():
    for file in HOURLY_FILES:
        if not file.exists():
            raise FileNotFoundError(file)

    source = "[" + ", ".join(
        sql_path(p) for p in HOURLY_FILES
    ) + "]"

    con = duckdb.connect(database=":memory:")
    con.execute("SET memory_limit = '4GB'")

    # ==================================
    # 2. 관측소별 일평균 기상자료
    # ==================================

    print("1단계: 관측소별 일평균 계산", flush=True)

    con.execute(f"""
        CREATE TEMP TABLE station_weather AS
        SELECT
            TRIM(station_id) AS station_id,
            CAST(
                TRY_CAST(
                    observation_time_kst AS TIMESTAMP
                ) AS DATE
            ) AS day,

            AVG(TRY_CAST(ta_c AS DOUBLE)) AS ta_avg,
            AVG(TRY_CAST(hm_pct AS DOUBLE)) AS hm_avg,
            AVG(TRY_CAST(ps_hpa AS DOUBLE)) AS ps_avg,

            COUNT(TRY_CAST(ta_c AS DOUBLE)) AS ta_hours,
            COUNT(TRY_CAST(hm_pct AS DOUBLE)) AS hm_hours,
            COUNT(TRY_CAST(ps_hpa AS DOUBLE)) AS ps_hours

        FROM read_csv(
            {source},
            header = true,
            all_varchar = true,
            union_by_name = true
        )
        WHERE
            TRY_CAST(
                observation_time_kst AS TIMESTAMP
            ) IS NOT NULL
        GROUP BY 1, 2
    """)

    # ==================================
    # 3. 관측소별 일강수량
    # ==================================

    print("2단계: 관측소별 일강수량 계산", flush=True)

    # RN_HR1은 관측 종료시각 기준 1시간 강수량
    # 00:00의 강수량은 전날에 포함
    con.execute(f"""
        CREATE TEMP TABLE station_rain AS
        SELECT
            TRIM(station_id) AS station_id,

            CAST(
                TRY_CAST(
                    observation_time_kst AS TIMESTAMP
                ) - INTERVAL '1 SECOND'
                AS DATE
            ) AS day,

            SUM(
                TRY_CAST(rn_hr1_mm AS DOUBLE)
            ) AS rain_sum,

            COUNT(
                TRY_CAST(rn_hr1_mm AS DOUBLE)
            ) AS rain_hours,

            COUNT(*) AS rain_rows

        FROM read_csv(
            {source},
            header = true,
            all_varchar = true,
            union_by_name = true
        )
        WHERE
            TRY_CAST(
                observation_time_kst AS TIMESTAMP
            ) IS NOT NULL
        GROUP BY 1, 2
    """)

    # ==================================
    # 4. 유효 관측 기준 적용
    # ==================================

    print("3단계: 일별 품질 기준 적용", flush=True)

    con.execute(f"""
        CREATE TEMP TABLE station_daily AS
        SELECT
            COALESCE(w.station_id, r.station_id)
                AS station_id,

            COALESCE(w.day, r.day) AS day,

            CASE WHEN w.ta_hours >= {MIN_WEATHER_HOURS}
                THEN w.ta_avg END AS ta_mean_c,

            CASE WHEN w.hm_hours >= {MIN_WEATHER_HOURS}
                THEN w.hm_avg END AS hm_mean_pct,

            CASE WHEN w.ps_hours >= {MIN_WEATHER_HOURS}
                THEN w.ps_avg END AS ps_mean_hpa,

            CASE WHEN
                r.rain_hours = {REQUIRED_RAIN_HOURS}
                AND r.rain_rows = 24
                THEN r.rain_sum
            END AS rain_mm_1d,

            w.ta_hours,
            w.hm_hours,
            w.ps_hours,
            r.rain_hours

        FROM station_weather w
        FULL OUTER JOIN station_rain r
            ON w.station_id = r.station_id
            AND w.day = r.day

        WHERE COALESCE(w.day, r.day)
            BETWEEN DATE '2024-01-01'
            AND DATE '2025-12-31'
    """)

    con.execute("DROP TABLE station_weather")
    con.execute("DROP TABLE station_rain")

    # ==================================
    # 5. 행정동별 일별 원자료 저장 테이블
    # ==================================

    con.execute("""
        CREATE TEMP TABLE gold_raw (
            admin_dong_code VARCHAR,
            admin_dong_name VARCHAR,
            day DATE,

            weather_station_id VARCHAR,
            weather_distance_m DOUBLE,

            rain_station_id VARCHAR,
            rain_distance_m DOUBLE,

            ta_mean_c DOUBLE,
            hm_mean_pct DOUBLE,
            ps_mean_hpa DOUBLE,
            rain_mm_1d DOUBLE,

            ta_valid_hours INTEGER,
            hm_valid_hours INTEGER,
            ps_valid_hours INTEGER,
            rain_valid_hours INTEGER
        )
    """)

    # ==================================
    # 6. 연도별 공간 정합
    # ==================================

    for year in (2024, 2025):
        print(
            f"4단계: {year}년 행정동 공간 정합",
            flush=True,
        )

        mapping_path = (
            MAPPING / f"dong_station_distance_{year}.csv"
        )

        if not mapping_path.exists():
            raise FileNotFoundError(mapping_path)

        map_source = sql_path(mapping_path)

        con.execute(f"""
            INSERT INTO gold_raw

            WITH mapping AS (
                SELECT
                    admin_dong_code,
                    admin_dong_name,
                    TRIM(station_id) AS station_id,
                    CAST(distance_rank AS INTEGER)
                        AS distance_rank,
                    CAST(distance_m AS DOUBLE)
                        AS distance_m
                FROM read_csv(
                    {map_source},
                    header = true,
                    all_varchar = true
                )
            ),

            dates AS (
                SELECT CAST(d AS DATE) AS day
                FROM generate_series(
                    DATE '{year}-01-01',
                    DATE '{year}-12-31',
                    INTERVAL '1 DAY'
                ) AS t(d)
            ),

            base AS (
                SELECT
                    m.admin_dong_code,
                    m.admin_dong_name,
                    d.day
                FROM (
                    SELECT DISTINCT
                        admin_dong_code,
                        admin_dong_name
                    FROM mapping
                ) m
                CROSS JOIN dates d
            ),

            weather_best AS (
                SELECT
                    m.admin_dong_code,
                    s.day,
                    m.station_id,
                    m.distance_m,
                    s.ta_mean_c,
                    s.hm_mean_pct,
                    s.ps_mean_hpa,
                    s.ta_hours,
                    s.hm_hours,
                    s.ps_hours

                FROM mapping m
                JOIN station_daily s
                    ON m.station_id = s.station_id

                WHERE
                    s.day BETWEEN
                        DATE '{year}-01-01'
                        AND DATE '{year}-12-31'

                    AND m.distance_m <= {MAX_DISTANCE_M}

                    AND s.ta_mean_c IS NOT NULL
                    AND s.hm_mean_pct IS NOT NULL
                    AND s.ps_mean_hpa IS NOT NULL

                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY
                        m.admin_dong_code, s.day
                    ORDER BY m.distance_rank
                ) = 1
            ),

            rain_best AS (
                SELECT
                    m.admin_dong_code,
                    s.day,
                    m.station_id,
                    m.distance_m,
                    s.rain_mm_1d,
                    s.rain_hours

                FROM mapping m
                JOIN station_daily s
                    ON m.station_id = s.station_id

                WHERE
                    s.day BETWEEN
                        DATE '{year}-01-01'
                        AND DATE '{year}-12-31'

                    AND m.distance_m <= {MAX_DISTANCE_M}

                    AND s.rain_mm_1d IS NOT NULL

                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY
                        m.admin_dong_code, s.day
                    ORDER BY m.distance_rank
                ) = 1
            )

            SELECT
                b.admin_dong_code,
                b.admin_dong_name,
                b.day,

                w.station_id,
                w.distance_m,

                r.station_id,
                r.distance_m,

                w.ta_mean_c,
                w.hm_mean_pct,
                w.ps_mean_hpa,
                r.rain_mm_1d,

                w.ta_hours,
                w.hm_hours,
                w.ps_hours,
                r.rain_hours

            FROM base b

            LEFT JOIN weather_best w
                ON b.admin_dong_code = w.admin_dong_code
                AND b.day = w.day

            LEFT JOIN rain_best r
                ON b.admin_dong_code = r.admin_dong_code
                AND b.day = r.day
        """)

    # ==================================
    # 7. 최근 3일·7일 누적강수량
    # ==================================

    print("5단계: 누적강수량 계산", flush=True)

    con.execute("""
        CREATE TEMP TABLE gold_features AS
        SELECT
            *,

            CASE
                WHEN COUNT(rain_mm_1d) OVER (
                    PARTITION BY admin_dong_code
                    ORDER BY day
                    ROWS BETWEEN
                        2 PRECEDING AND CURRENT ROW
                ) = 3

                THEN SUM(rain_mm_1d) OVER (
                    PARTITION BY admin_dong_code
                    ORDER BY day
                    ROWS BETWEEN
                        2 PRECEDING AND CURRENT ROW
                )
            END AS rain_mm_3d,

            CASE
                WHEN COUNT(rain_mm_1d) OVER (
                    PARTITION BY admin_dong_code
                    ORDER BY day
                    ROWS BETWEEN
                        6 PRECEDING AND CURRENT ROW
                ) = 7

                THEN SUM(rain_mm_1d) OVER (
                    PARTITION BY admin_dong_code
                    ORDER BY day
                    ROWS BETWEEN
                        6 PRECEDING AND CURRENT ROW
                )
            END AS rain_mm_7d

        FROM gold_raw
    """)

    # ==================================
    # 8. 연도별 Parquet 저장
    # ==================================

    print("6단계: Gold 파일 저장", flush=True)

    report = {
        "period": "2024-01-01 ~ 2025-12-31",
        "spatial_unit": "administrative_dong",
        "temporal_unit": "daily",
        "boundary_snapshot": "2025-06-30",
        "max_station_distance_m": MAX_DISTANCE_M,
        "min_weather_valid_hours": MIN_WEATHER_HOURS,
        "required_rain_valid_hours": REQUIRED_RAIN_HOURS,
        "rainfall_time_convention":
            "1-hour rainfall assigned to interval ending time minus 1 second",
        "years": {},
        "limitations": [
            "2025-06-30 boundary used for both years",
            "some historical station coordinates from 2022",
            "end-of-2025 rainfall may lack 2026-01-01 00:00 observation",
            "missing weather values are not replaced with zero",
            "same-day features must be lagged for next-day forecasting",
        ],
    }

    for year in (2024, 2025):
        output = GOLD / f"dong_daily_features_{year}.parquet"

        con.execute(f"""
            COPY (
                SELECT *
                FROM gold_features
                WHERE YEAR(day) = {year}
                ORDER BY admin_dong_code, day
            )
            TO {sql_path(output)}
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        stats = con.execute(f"""
            SELECT
                COUNT(*),
                COUNT(DISTINCT admin_dong_code),
                COUNT(rain_mm_1d),
                COUNT(ta_mean_c),
                COUNT(rain_mm_3d),
                COUNT(rain_mm_7d),
                COUNT(weather_station_id),
                COUNT(rain_station_id)
            FROM gold_features
            WHERE YEAR(day) = {year}
        """).fetchone()

        report["years"][str(year)] = {
            "total_rows": stats[0],
            "admin_dong_count": stats[1],
            "rain_1d_valid_rows": stats[2],
            "temperature_valid_rows": stats[3],
            "rain_3d_valid_rows": stats[4],
            "rain_7d_valid_rows": stats[5],
            "weather_station_assigned_rows": stats[6],
            "rain_station_assigned_rows": stats[7],
        }

        print(f"\n[{year}년 Gold 결과]")
        print("전체 행:", stats[0])
        print("행정동:", stats[1])
        print("일강수량 유효:", stats[2])
        print("3일 강수량 유효:", stats[4])
        print("7일 강수량 유효:", stats[5])
        print("저장:", output)

    report_path = GOLD / "gold_weather_quality_report.json"

    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    con.close()

    print("\n===== Gold 기상 Feature 생성 완료 =====")
    print("품질 보고서:", report_path)


if __name__ == "__main__":
    main()
