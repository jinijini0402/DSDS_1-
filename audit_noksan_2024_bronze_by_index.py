"""Read-only audit: link 2024 Noksan 125 Silver records to Bronze by page/index.

Run from heavy-rain-prediction project root:
  python -u audit_noksan_2024_bronze_by_index.py

Never changes Silver/Gold labels. Only outputs diagnosis CSV/JSON.
"""
from pathlib import Path
from collections import Counter
from decimal import Decimal, InvalidOperation
import csv
import json
import re

import duckdb

ROOT = Path('data')
SILVER = ROOT / 'silver/labels/rain_damage/private_rain_sgis_join_review_fixed.parquet'
RAW = ROOT / 'bronze/labels/safety_damage/private_facilities/raw'
MAP = ROOT / 'silver/mappings/ndms_sgis_crosswalk/official_full_linkage_2025.csv'
WEATHER = ROOT / 'gold/weather/daily_from_hourly/2024'
OUT = ROOT / 'silver/quality_checks/labels/noksan_2024_split_diagnostic'
CSV = OUT / 'noksan_2024_bronze_by_index_audit.csv'
JSON = OUT / 'noksan_2024_bronze_by_index_summary.json'

KEYS = [
    ('disaster_year', 'MSTN_YR'),
    ('event_region_code', 'RGN_CD'),
    ('disaster_serial', 'MSTN_SN'),
    ('victim_serial', 'VCTM_SN'),
    ('damage_serial', 'DAM_SN'),
    ('history_serial', 'DAM_HSTRY_SN'),
    ('normalized_admin10', 'DAM_RGN_DONG_CD'),
]


def normalized(value):
    if value is None:
        return ''
    s = str(value).strip()
    if not s:
        return ''
    try:
        d = Decimal(s)
        if d == d.to_integral_value():
            return str(int(d))
    except (InvalidOperation, ValueError):
        pass
    return s


def legal_code(value):
    val = normalized(value)
    return val if re.fullmatch(r'\d{10}', val) else None


def main():
    for path in [SILVER, MAP, WEATHER]:
        if not path.exists():
            raise FileNotFoundError(path)
    con = duckdb.connect()
    try:
        rows = con.execute('''
            SELECT * FROM read_parquet(?)
            WHERE actual_year=2024 AND normalized_admin10='2644056000'
              AND check_status='not_in_gold_boundary'
        ''', [str(SILVER)]).df().to_dict('records')
        if len(rows) != 125:
            raise RuntimeError(f'예상 125건과 다름: {len(rows)}; 분석 중단')

        mapping = con.execute('''
          SELECT CAST(legal_dong10 AS VARCHAR) AS legal,
                 CAST(sgis_dong8 AS VARCHAR) AS sgis,
                 legal_dong_name, admin_dong_name
          FROM read_csv(?, header=true, all_varchar=true)
          WHERE legal_dong10 IS NOT NULL AND sgis_dong8 IS NOT NULL
        ''', [str(MAP)]).fetchall()
        legal_targets = {}
        for legal, sgis, lname, aname in mapping:
            legal_targets.setdefault(legal, set()).add((sgis, aname))

        gold_codes = set(row[0] for row in con.execute('''
          SELECT DISTINCT CAST(admin_dong_code AS VARCHAR)
          FROM read_parquet(?)
        ''', [str(WEATHER / 'dong_daily_weather_2024_01.parquet')]).fetchall())
    finally:
        con.close()

    page_cache = {}
    audit = []
    for r in rows:
        page = normalized(r['source_page'])
        idx_s = normalized(r['source_index'])
        result = {
            'source_page': page, 'source_index': idx_s,
            'damage_day': str(r.get('actual_day') or ''),
            'amount_raw_unit': str(r.get('amount_number') or ''),
            'identifier_match': False, 'legal_code': '', 'legal_name': '',
            'target_sgis': '', 'target_admin_name': '', 'status': '',
            'mismatch_fields': ''
        }
        if not page.isdigit() or not idx_s.isdigit():
            result['status'] = 'invalid_page_or_index'
            audit.append(result)
            continue
        page_num = int(page)
        ix = int(idx_s)
        path = RAW / f'page_{page_num:05d}.json'
        if not path.is_file():
            result['status'] = 'missing_raw_page'
            audit.append(result)
            continue
        if page_num not in page_cache:
            obj = json.loads(path.read_text(encoding='utf-8-sig'))
            body = obj.get('body')
            if not isinstance(body, list) or normalized(obj.get('pageNo')) != str(page_num):
                raise RuntimeError(f'원본 페이지 메타데이터 불일치: {path}')
            page_cache[page_num] = body
        body = page_cache[page_num]
        if not 0 <= ix < len(body):
            result['status'] = 'index_out_of_range'
            audit.append(result)
            continue
        b = body[ix]
        mismatches = [f'{s}:{normalized(r.get(s))}!={normalized(b.get(t))}'
                      for s, t in KEYS if normalized(r.get(s)) != normalized(b.get(t))]
        if mismatches:
            result['status'] = 'identifier_mismatch'
            result['mismatch_fields'] = ';'.join(mismatches)
            audit.append(result)
            continue
        result['identifier_match'] = True
        code = legal_code(b.get('DAM_RGN_STDG_CD'))
        result['legal_code'] = code or ''
        if not code:
            result['status'] = 'missing_or_invalid_legal_code'
            audit.append(result)
            continue
        candidates = legal_targets.get(code, set())
        valid = {(sgis, name) for sgis, name in candidates if sgis in gold_codes}
        if len(valid) == 1:
            sgis, name = next(iter(valid))
            result['target_sgis'] = sgis
            result['target_admin_name'] = name
            result['status'] = 'unique_legal_to_gold_candidate'
        elif not candidates:
            result['status'] = 'legal_not_in_map'
        elif not valid:
            result['status'] = 'legal_target_not_in_gold'
        else:
            result['status'] = 'ambiguous_legal_to_gold'
        audit.append(result)

    OUT.mkdir(parents=True, exist_ok=True)
    with CSV.open('w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=list(audit[0]))
        writer.writeheader()
        writer.writerows(sorted(audit, key=lambda r: (r['source_page'], r['source_index'])))
    summary = {
        'target_rows': len(audit),
        'identifier_matched': sum(bool(x['identifier_match']) for x in audit),
        'status_counts': dict(Counter(x['status'] for x in audit)),
        'candidate_target_counts': dict(Counter(x['target_admin_name'] for x in audit if x['target_sgis'])),
        'legal_code_counts': dict(Counter(x['legal_code'] for x in audit if x['legal_code'])),
        'audit_csv': str(CSV),
        'caution': 'Diagnostic candidates only: verify legal code field meaning & target boundary before applying. No Silver or Gold altered.',
    }
    JSON.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding='utf-8')
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print('저장:', CSV, '\nQA:', JSON)


if __name__ == '__main__':
    main()
