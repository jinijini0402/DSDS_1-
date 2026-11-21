"""NDMS 10자리 지역코드와 공식 한국행정구역분류 8자리 연계 품질 검사.

입력 (프로젝트 최상위에서 실행):
 data/silver/mappings/ndms_sgis_crosswalk/ndms_dong10_frequency_2024_2025.csv
 data/silver/mappings/ndms_sgis_crosswalk/official_full_linkage_2024.csv
 data/silver/mappings/ndms_sgis_crosswalk/official_full_linkage_2025.csv
 data/gold/weather/dong_daily_features_2024.parquet
 data/gold/weather/dong_daily_features_2025.parquet

출력:
 data/silver/mappings/ndms_sgis_crosswalk/ndms_sgis_match_audit.csv
 data/silver/mappings/ndms_sgis_crosswalk/ndms_sgis_match_report.json

중요: 이 스크립트는 NDMS 원본 필드가 행정기관코드인지 법정동코드인지
확인하지 못한다. 따라서 후보 연결과 품질 확인만 수행하며 Gold Label을 생성하지 않는다.
"""
from collections import defaultdict, Counter
from pathlib import Path
import csv
import json

import duckdb

ROOT = Path('data/silver/mappings/ndms_sgis_crosswalk')
FREQ = ROOT / 'ndms_dong10_frequency_2024_2025.csv'
GOLD = Path('data/gold/weather')
OUT_CSV = ROOT / 'ndms_sgis_match_audit.csv'
OUT_JSON = ROOT / 'ndms_sgis_match_report.json'


def read_csv(path):
    if not path.exists():
        raise FileNotFoundError(f'필요한 파일이 없습니다: {path}')
    with path.open('r', encoding='utf-8-sig', newline='') as f:
        yield from csv.DictReader(f)


def read_gold_codes(year):
    path = GOLD / f'dong_daily_features_{year}.parquet'
    if not path.exists():
        raise FileNotFoundError(path)
    con = duckdb.connect()
    try:
        codes = con.execute(
            'SELECT DISTINCT CAST(admin_dong_code AS VARCHAR) '
            'FROM read_parquet(?) WHERE admin_dong_code IS NOT NULL',
            [str(path)]
        ).fetchall()
    finally:
        con.close()
    return {str(x[0]).strip() for x in codes}


def load_official(year):
    src = ROOT / f'official_full_linkage_{year}.csv'
    admin = defaultdict(set)
    legal = defaultdict(set)
    for r in read_csv(src):
        a = r['admin_dong10'].strip()
        l = r['legal_dong10'].strip()
        s = r['sgis_dong8'].strip()
        if len(s) != 8 or not s.isdigit():
            continue
        if len(a) == 10 and a.isdigit():
            admin[a].add(s)
        if len(l) == 10 and l.isdigit():
            legal[l].add(s)
    return admin, legal


def classify(code, admin, legal):
    admin_matches = admin.get(code, set())
    legal_matches = legal.get(code, set())
    if len(admin_matches) == 1:
        candidate = next(iter(admin_matches))
        if legal_matches and legal_matches != admin_matches:
            return 'admin_legal_conflict', '', candidate, legal_matches
        return 'admin_code_match', candidate, candidate, legal_matches
    if len(admin_matches) > 1:
        return 'admin_ambiguous', '', '', legal_matches
    if len(legal_matches) == 1:
        return 'legal_code_only_review', '', '', legal_matches
    if len(legal_matches) > 1:
        return 'legal_code_ambiguous', '', '', legal_matches
    return 'not_in_official_linkage', '', '', set()


def main():
    if not FREQ.exists():
        raise FileNotFoundError(FREQ)

    yearly = {year: {
        'official': load_official(year),
        'gold_codes': read_gold_codes(year)
    } for year in (2024, 2025)}

    output = []
    results = {}
    stats = {year: defaultdict(lambda: {'region_codes':0, 'damage_records':0}) for year in (2024, 2025)}
    gold_miss_samples = {2024: [], 2025: []}

    for r in read_csv(FREQ):
        year = int(r['year'])
        if year not in yearly:
            continue
        ndms_code = r['ndms_dong10'].strip()
        amount = int(r['total_records'])
        admin, legal = yearly[year]['official']
        gold_codes = yearly[year]['gold_codes']
        status, matched, admin_candidate, legal_candidates = classify(ndms_code,admin,legal)
        in_gold = (matched in gold_codes) if matched else False
        if status == 'admin_code_match' and not in_gold:
            status = 'admin_matches_but_not_in_gold_boundary'
            gold_miss_samples[year].append({'ndms_dong10': ndms_code, 'sgis_candidate': matched,'damage_records':amount})
        # No automatic mapping when not present in the precise Gold code set
        joined_sgis = matched if status == 'admin_code_match' and in_gold else ''
        output.append({
            'year':year,
            'ndms_dong10':ndms_code,
            'private_records':r['private_records'],
            'public_records':r['public_records'],
            'total_records':amount,
            'match_status':status,
            'official_admin_candidate_sgis8':admin_candidate,
            'official_legal_candidates_sgis8':';'.join(sorted(legal_candidates)),
            'sgis_dong8_candidate_for_join':joined_sgis,
            'is_in_gold_boundary':'Y' if in_gold else 'N',
            'approved_for_gold_label':'N',
        })
        stats[year][status]['region_codes'] += 1
        stats[year][status]['damage_records'] += amount

    if not output:
        raise RuntimeError('입력 frequency CSV에 2024~2025년 데이터가 없습니다.')
    ROOT.mkdir(parents=True,exist_ok=True)
    with OUT_CSV.open('w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(output[0]))
        w.writeheader();w.writerows(output)

    for year in (2024,2025):
        sub = [r for r in output if r['year']==year]
        all_records = sum(int(r['total_records']) for r in sub)
        candidates = sum(int(r['total_records']) for r in sub if r['sgis_dong8_candidate_for_join'])
        results[str(year)] = {
            'gold_unique_sgis_dongs':len(yearly[year]['gold_codes']),
            'ndms_unique_codes':len(sub),
            'ndms_records':all_records,
            'admin_code_candidate_join_records':candidates,
            'admin_code_candidate_join_rate_pct':round(candidates / all_records * 100, 4) if all_records else None,
            'match_status_counts':dict(stats[year]),
            'admin_matches_outside_gold_samples':gold_miss_samples[year][:30],
        }
        print(f'\n[{year}] Gold 8자리 코드: {len(yearly[year]["gold_codes"]):,}개')
        print(f'NDMS 피해지역코드: {len(sub):,}개 / 피해후보: {all_records:,}건')
        print(f'행정기관코드 기준 결합 후보: {candidates:,}건 ({results[str(year)]["admin_code_candidate_join_rate_pct"]}%)')
        for k,v in sorted(stats[year].items()):
            print(f'  {k}: 지역코드 {v["region_codes"]:,}개, 피해후보 {v["damage_records"]:,}건')

    OUT_JSON.write_text(json.dumps({
        'note':'공식 연계표의 행정기관 10자리와 법정동 10자리를 따로 확인함. NDMS 필드의 코드 종류 및 시점 검증 전에는 최종 Gold Label로 취급하지 않음.',
        'official_snapshots': {'2024':'2024-10-01','2025':'2025-12-31'},
        'gold_boundary_snapshot':'2025-06-30',
        'years':results,
    }, ensure_ascii=False,indent=2), encoding='utf-8')
    print('\n검증 CSV:',OUT_CSV)
    print('검증 보고서:',OUT_JSON)

if __name__ == '__main__':
    main()
