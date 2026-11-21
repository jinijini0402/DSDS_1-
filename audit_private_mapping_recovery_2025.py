"""Read-only audit of 2025 private-facility damage records missing a valid Gold dong.

Does not modify Silver or Gold inputs. Requires duckdb.
Run from the heavy-rain-prediction project root.
"""
from pathlib import Path
import json
import duckdb

ROOT = Path('data')
PRIVATE = ROOT / 'silver/labels/rain_damage/private_rain_sgis_join_review_fixed.parquet'
MAP24 = ROOT / 'silver/mappings/ndms_sgis_crosswalk/official_admin10_to_sgis8_2024.csv'
MAP25 = ROOT / 'silver/mappings/ndms_sgis_crosswalk/official_admin10_to_sgis8_2025.csv'
GOLD25 = ROOT / 'gold/weather/daily_from_hourly/2025'
OUT = ROOT / 'silver/quality_checks/labels/mapping_recovery_2025_diagnostic'

def quote(path):
    return "'" + str(path).replace("'", "''") + "'"


def main():
    for p in (PRIVATE, MAP24, MAP25):
        if not p.is_file():
            raise FileNotFoundError(p)
    months = sorted(GOLD25.glob('dong_daily_weather_2025_??.parquet'))
    if len(months) != 12:
        raise RuntimeError(f'Expected 12 monthly Gold files, found {len(months)}')
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET threads=4")
    con.execute("SET memory_limit='4GB'")
    try:
        con.execute(f"""
            CREATE TEMP VIEW gold_dongs AS
            SELECT DISTINCT TRIM(CAST(admin_dong_code AS VARCHAR)) AS sgis
            FROM read_parquet({quote(str(GOLD25 / 'dong_daily_weather_2025_*.parquet'))})
        """)
        for year, path in ((24, MAP24), (25, MAP25)):
            con.execute(f"""
              CREATE TEMP VIEW m{year} AS
              SELECT TRIM(admin_dong10) AS code,
                     MIN(TRIM(sgis_dong8)) AS sgis,
                     COUNT(DISTINCT TRIM(sgis_dong8)) AS targets,
                     MAX(TRY_CAST(unique_admin_to_sgis AS INTEGER)) AS flagged_unique,
                     MAX(sigungu) AS sigungu,
                     MAX(admin_dong_name) AS admin_name
              FROM read_csv({quote(path)}, header=true, all_varchar=true)
              WHERE admin_dong10 IS NOT NULL
              GROUP BY 1
            """)
        con.execute(f"""
            CREATE TEMP VIEW src AS
            SELECT *, TRIM(CAST(normalized_admin10 AS VARCHAR)) AS code,
                   NULLIF(TRIM(CAST(sgis_dong8 AS VARCHAR)), '') AS old_sgis
            FROM read_parquet({quote(PRIVATE)})
            WHERE TRY_CAST(actual_year AS INTEGER)=2025
        """)
        con.execute("""
           CREATE TEMP VIEW reviewed AS
           SELECT s.*, m25.sgis AS m25_sgis, m25.targets AS m25_targets,
                  m24.sgis AS m24_sgis, m24.targets AS m24_targets,
                  m24.sigungu AS m24_sigungu, m24.admin_name AS m24_admin_name,
                  (g_old.sgis IS NOT NULL) AS old_in_gold,
                  (g24.sgis IS NOT NULL) AS m24_in_gold,
                  (g25.sgis IS NOT NULL) AS m25_in_gold,
                  CASE
                    WHEN g_old.sgis IS NOT NULL THEN 'already_in_gold'
                    WHEN s.code IS NULL OR s.code='' THEN 'missing_admin10'
                    WHEN m25.code IS NOT NULL AND m25.targets=1 AND g25.sgis IS NOT NULL THEN 'check_2025_mapping_first'
                    WHEN m24.code IS NOT NULL AND m24.targets=1 AND g24.sgis IS NOT NULL THEN 'fallback_2024_candidate'
                    WHEN m24.code IS NOT NULL AND (m24.targets<>1 OR g24.sgis IS NULL) THEN 'fallback_ambiguous_or_not_in_gold'
                    ELSE 'unresolved_no_valid_fallback'
                  END AS audit_class
           FROM src s
           LEFT JOIN m25 ON s.code=m25.code
           LEFT JOIN m24 ON s.code=m24.code
           LEFT JOIN gold_dongs g_old ON s.old_sgis=g_old.sgis
           LEFT JOIN gold_dongs g24 ON m24.sgis=g24.sgis
           LEFT JOIN gold_dongs g25 ON m25.sgis=g25.sgis
        """)
        print('\n=== 2025 record coverage (not just excluded records) ===')
        for label, n in con.execute("SELECT audit_class, COUNT(*) FROM reviewed GROUP BY 1 ORDER BY 1").fetchall():
            print(f'{label}: {n:,}')
        print('\n=== Invalid/missing Gold SGIS only, by existing check_status and proposed class ===')
        records = con.execute("""
           SELECT COALESCE(CAST(check_status AS VARCHAR), '<NULL>'), audit_class, COUNT(*)
           FROM reviewed WHERE NOT old_in_gold
           GROUP BY 1,2 ORDER BY 1,2
        """).fetchall()
        for status, label, count in records:
            print(f'{status} | {label}: {count:,}')
        print('\n=== 2024 fallback candidates by administrative code ===')
        rows = con.execute("""
           SELECT code, m24_sgis, m24_sigungu, m24_admin_name,
                  COUNT(*) AS source_records,
                  COUNT(DISTINCT actual_day) AS distinct_days,
                  MIN(actual_day) AS first_day,
                  MAX(actual_day) AS last_day
           FROM reviewed WHERE audit_class='fallback_2024_candidate'
           GROUP BY 1,2,3,4 ORDER BY source_records DESC, code
        """).fetchall()
        for row in rows:
            print(' | '.join(map(str,row)))
        print('\n=== 2024 fallback candidate amounts (source units, NOT final deduplicated totals) ===')
        print(con.execute("""
          SELECT COUNT(*) AS records,
                 COUNT(amount_number) AS amount_nonnull,
                 SUM(amount_number) AS source_sum_raw_unit,
                 COUNT(DISTINCT (event_region_code,disaster_serial,victim_serial,damage_serial,actual_day)) AS distinct_candidate_keys
          FROM reviewed WHERE audit_class='fallback_2024_candidate'
        """).fetchall()[0])
        # Restrict detailed output to invalid/missing-Gold cases. Never overwrite original.
        sql = """
          SELECT disaster_year, event_region_code, disaster_serial, victim_serial,
                 damage_serial, history_serial, actual_day, code AS admin10,
                 old_sgis, check_status, audit_class,
                 m25_sgis, m25_targets, m25_in_gold,
                 m24_sgis AS suggested_sgis_dong8, m24_targets, m24_in_gold,
                 m24_sigungu, m24_admin_name, amount_number AS amount_raw_unit
          FROM reviewed WHERE NOT old_in_gold
          ORDER BY audit_class, code, actual_day
        """
        destination = OUT/'review_2025_invalid_gold_mapping.csv'
        con.execute(f'COPY ({sql}) TO {quote(destination)} (HEADER TRUE)')
        report = {
            'source': str(PRIVATE),
            'year': 2025,
            'total_2025_rows': con.execute('SELECT COUNT(*) FROM reviewed').fetchone()[0],
            'old_sgis_outside_gold_rows': con.execute('SELECT COUNT(*) FROM reviewed WHERE NOT old_in_gold').fetchone()[0],
            'classes': dict(con.execute('SELECT audit_class, COUNT(*) FROM reviewed GROUP BY 1').fetchall()),
            'invalid_by_check_status_and_class': [
                {'check_status':s, 'class':c, 'rows':n} for s,c,n in records
            ],
            'candidate_raw_record_count': con.execute("SELECT COUNT(*) FROM reviewed WHERE audit_class='fallback_2024_candidate'").fetchone()[0],
            'candidate_unique_code_count': len(rows),
            'notes': [
                'This is a diagnostic only; do not directly append records or sum raw amounts into daily labels.',
                'A record with a Gold-valid existing mapping is never re-mapped.',
                'Verify original private damage deduplication/history, valid flags and source status before rebuilding Silver.',
                '2024 fallback requires valid code at the 2025 event date, not merely presence in the 2024 lookup.',
                'This script deliberately does not resolve 2024 Noksan-dong split cases.',
            ],
        }
        (OUT/'summary.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        print('\nSaved:', destination)
        print('Saved:', OUT/'summary.json')
        print('\nExpected: compare 2024 fallback candidate count against 252; investigate any difference before patching.')
    finally:
        con.close()

if __name__ == '__main__':
    main()
