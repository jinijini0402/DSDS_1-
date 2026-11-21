"""공공시설 2024~2025 호우 후보의 SGIS 연결 및 일별 예비집계.

실행 (heavy-rain-prediction 프로젝트 최상위):
    python -u map_public_rain_to_sgis_review.py --max-pages 20
    python -u map_public_rain_to_sgis_review.py

데이터 원본은 읽기만 함. 추가 API 호출 없음.
- NDMS_MSTN_TYPE_CD=1001은 '호우' 후보로 취급하며 코드 표 최종 확인 전.
- USE_YN=Y/N(사용여부), CFMTN_SPTR=0/1/2(확정구분자)를 보존.
- CFMTN_SPTR 값별 의미는 확정하지 않으며, USE_YN=N은 집계 대상에서 보류.
- 금액 누락은 0으로 대체하지 않음; 원천 전체 페이지 완결성 보장 없음.
- 예비집계는 정식 Label이 아님. Parquet 변환에는 duckdb 필요.
"""
import argparse
import csv
import gzip
import json
import re
import zipfile
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

ROOT = Path('data')
RAW = ROOT / 'bronze/labels/safety_damage/public_facilities/raw'
ZIP = ROOT / 'bronze/labels/safety_damage/public_facilities.zip'
MAP = ROOT / 'silver/mappings/ndms_sgis_crosswalk'
GOLD = ROOT / 'gold/weather'
OUT = ROOT / 'silver/labels/rain_damage'
QC = ROOT / 'silver/quality_checks/labels'


def txt(v):
    return '' if v is None else str(v).strip()


def day_from(v):
    s = re.sub('[^0-9]', '', txt(v))[:8]
    if len(s) != 8: return None
    try:
        return datetime.strptime(s, '%Y%m%d').strftime('%Y-%m-%d')
    except ValueError:
        return None


def pages(raw_dir, zip_path, max_pages):
    if raw_dir.is_dir() and any(raw_dir.glob('page_*.json')):
        paths = []
        for p in raw_dir.glob('page_*.json'):
            m = re.fullmatch(r'page_(\d+)\.json', p.name)
            if m: paths.append((int(m.group(1)), p))
        paths.sort()
        print('원본 폴더 사용:', raw_dir, flush=True)
        for i, (page, p) in enumerate(paths):
            if max_pages and i >= max_pages: break
            yield page, json.loads(p.read_text(encoding='utf-8-sig'))
    elif zip_path.is_file():
        print('원본 ZIP 사용:', zip_path, flush=True)
        with zipfile.ZipFile(zip_path) as z:
            paths = []
            for n in z.namelist():
                m = re.search(r'(?:^|/)raw/page_(\d+)\.json$', n)
                if m: paths.append((int(m.group(1)), n))
            paths.sort()
            for i, (page, n) in enumerate(paths):
                if max_pages and i >= max_pages: break
                yield page, json.loads(z.read(n))
    else:
        raise FileNotFoundError(f'공공시설 원본 없음: {raw_dir} 또는 {zip_path}')


def mappings():
    # 공식 연계표가 기존 Gold 경계의 8자리 행정동 목록과 연결되는지 확인한다.
    # SGIS 목록은 Gold의 원천 행정동 매핑에서 이미 추출한 2024/2025 master.
    master = MAP / 'sgis_dong8_master_2024_2025.csv'
    if not master.is_file(): raise FileNotFoundError(master)
    gold_codes = defaultdict(set)
    with master.open(encoding='utf-8-sig', newline='') as f:
        for r in csv.DictReader(f):
            if re.fullmatch(r'\d{8}', txt(r['sgis_dong8'])):
                gold_codes[txt(r['year'])].add(txt(r['sgis_dong8']))
    official = {}
    for year in (2024, 2025):
        p = MAP / f'official_admin10_to_sgis8_{year}.csv'
        if not p.is_file(): raise FileNotFoundError(p)
        candidates = defaultdict(set)
        with p.open(encoding='utf-8-sig', newline='') as f:
            for r in csv.DictReader(f):
                adm = txt(r['admin_dong10'])
                sgis = txt(r['sgis_dong8'])
                if re.fullmatch(r'\d{10}',adm) and re.fullmatch(r'\d{8}',sgis) and txt(r['unique_admin_to_sgis']) == '1':
                    candidates[adm].add(sgis)
        conflicts = {k:v for k,v in candidates.items() if len(v)!=1}
        if conflicts: raise RuntimeError(f'{year} 공식행정기관코드에 중복 SGIS 매핑: {len(conflicts)}개')
        official[str(year)] = {k:next(iter(v)) for k,v in candidates.items()}
        if len(gold_codes[str(year)]) < 3500:
            raise RuntimeError(f'{year} SGIS 목록이 비정상적으로 적음: {len(gold_codes[str(year)])}')
        print(f'{year} 공식 행정기관코드 {len(official[str(year)]):,}개 / Gold 기반 SGIS 코드 {len(gold_codes[str(year)]):,}개',flush=True)
    # duckdb 설치돼 있다면 실제 Gold parquet 코드와 일치하는지 별도로 검사.
    try:
        import duckdb
    except ImportError:
        print('주의: duckdb가 없어 Gold Parquet과 직접 비교하지 않습니다. Parquet 변환도 생략합니다.',flush=True)
    else:
        db = duckdb.connect()
        try:
            for year in (2024,2025):
                p = GOLD / f'dong_daily_features_{year}.parquet'
                if not p.is_file():raise FileNotFoundError(p)
                vals = {txt(x[0]) for x in db.execute(
                    'SELECT DISTINCT CAST(admin_dong_code AS VARCHAR) FROM read_parquet(?)', [str(p)]).fetchall()}
                if vals != gold_codes[str(year)]:
                    print(f'주의: {year} master와 실제 Gold 코드 다름. master 고유 {len(gold_codes[str(year)])}, 실제 {len(vals)}',flush=True)
                    gold_codes[str(year)] = vals  # 실제 Gold에 있는 것만 허용
        finally:
            db.close()
    return official, gold_codes


def decimal_amount(s):
    v = txt(s).replace(',', '')
    if not v:return None
    try:
        d = Decimal(v)
        if not d.is_finite():return None
        return d
    except InvalidOperation:return None


def make_parquet(gz_file, parquet_file):
    try:
        import duckdb
    except ImportError:
        return False
    g = "'" + str(gz_file).replace("'", "''") + "'"
    p = "'" + str(parquet_file).replace("'", "''") + "'"
    db = duckdb.connect()
    try:
        # 명시적으로 문자열형으로 읽으므로 행정기관 코드의 앞자리/자릿수가 유지된다.
        db.execute(f"COPY (SELECT * FROM read_csv({g}, header=true, all_varchar=true, compression='gzip')) TO {p} (FORMAT PARQUET, COMPRESSION ZSTD)")
    finally:
        db.close()
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--max-pages',type=int,default=0)
    ap.add_argument('--raw-dir',type=Path,default=RAW)
    ap.add_argument('--zip',type=Path,default=ZIP)
    args = ap.parse_args()
    if args.max_pages < 0: ap.error('--max-pages는 0 이상이어야 합니다')
    tag = f'_TEST_{args.max_pages}' if args.max_pages else ''
    OUT.mkdir(parents=True,exist_ok=True)
    QC.mkdir(parents=True,exist_ok=True)
    review_gz = OUT / f'public_rain_sgis_review{tag}.csv.gz'
    daily_gz = OUT / f'public_rain_daily_observed_PROVISIONAL{tag}.csv.gz'
    review_pq = OUT / f'public_rain_sgis_review{tag}.parquet'
    daily_pq = OUT / f'public_rain_daily_observed_PROVISIONAL{tag}.parquet'
    report = QC / f'public_rain_sgis_review_report{tag}.json'
    exception_csv = QC / f'public_rain_sgis_review_exceptions{tag}.csv'

    print('1단계: 연도별 공식코드와 Gold 기반 행정동 목록 확인',flush=True)
    official,gold_codes = mappings()
    print('2단계: 공공시설 수집분에서 2024~2025 호우 피해 추출 및 연결',flush=True)
    cols = ['source_page','source_index','disaster_year','damage_day','event_region_code',
            'disaster_serial','facility_serial','damage_region_admin10','sgis_dong8',
            'use_yn','confirmation_marker','last_modified_at',
            'confirmed_damage_amount_raw','amount_numeric_raw_unit','check_status']
    statuses = Counter()
    amounts = Counter()
    use_counts = Counter()
    confirm_counts = Counter()
    total_selected = 0
    total_pages = 0
    all_pages = set()
    reported_totals = Counter()
    keys_seen = set()
    dup_keys = set()
    daily = defaultdict(lambda: {'rows':0, 'used_y_rows':0, 'used_n_rows':0,
                                'positive_rows':0,'zero_rows':0,'missing_amount_rows':0,
                                'sum':Decimal(0),'cf0':0,'cf1':0,'cf2':0,'cfother':0})
    exceptions = Counter()
    bad_samples = []
    with gzip.open(review_gz,'wt',encoding='utf-8-sig',newline='') as f:
        w = csv.DictWriter(f,fieldnames=cols)
        w.writeheader()
        for page,payload in pages(args.raw_dir,args.zip,args.max_pages):
            if page in all_pages:raise ValueError(f'원본 중복 페이지: {page}')
            all_pages.add(page);total_pages+=1
            rc = txt(payload.get('header',{}).get('resultCode'))
            if rc not in ('','00','0','0000'):raise ValueError(f'{page}페이지 API 에러 {rc}')
            n = txt(payload.get('totalCount'))
            if n.isdigit():reported_totals[n]+=1
            rows = payload.get('body')
            if not isinstance(rows,list):raise ValueError(f'원본 body 구조 오류 페이지 {page}')
            for ix,r in enumerate(rows):
                if txt(r.get('MSTN_YR')) not in ('2024','2025') or txt(r.get('NDMS_MSTN_TYPE_CD'))!='1001':continue
                d = day_from(r.get('DMG_YMD'))
                if not d or d[:4] not in ('2024','2025'):continue
                total_selected+=1
                yr=d[:4]
                rawcode=txt(r.get('DAM_RGN_DONG_CD_1'))
                sgis=official[yr].get(rawcode,'')
                k=(txt(r.get('MSTN_YR')),txt(r.get('RGN_CD_1')),txt(r.get('MSTN_SN')),txt(r.get('PBLC_FCLT_SN')))
                missing_key = any(not x for x in k)
                duplicate_key = k in keys_seen
                if duplicate_key:dup_keys.add(k)
                keys_seen.add(k)
                use_yn=txt(r.get('USE_YN'))
                cflag=txt(r.get('CFMTN_SPTR'))
                a=decimal_amount(r.get('CFMTN_DMG_AMT'))
                use_counts[(yr,use_yn or '(empty)')]+=1
                confirm_counts[(yr,cflag or '(empty)')]+=1
                if missing_key:status='missing_candidate_key'
                elif duplicate_key:status='duplicate_candidate_key_review'
                elif not re.fullmatch(r'\d{10}',rawcode):status='invalid_admin_code'
                elif not sgis:status='not_in_year_official_map'
                elif sgis not in gold_codes[yr]:status='not_in_gold_boundary'
                elif use_yn=='N':status='use_no_review'
                elif use_yn!='Y':status='use_unknown_review'
                elif a is None:status='missing_amount'
                elif a<0:status='negative_amount_review'
                elif a==0:status='mapped_y_zero'
                else:status='mapped_y_positive_unverified_confirmation'
                statuses[(yr,status)]+=1
                if a is None: amounts[(yr,'missing')]+=1
                elif a>0: amounts[(yr,'positive')]+=1
                elif a==0: amounts[(yr,'zero')]+=1
                else: amounts[(yr,'negative')]+=1
                if status not in ('mapped_y_zero','mapped_y_positive_unverified_confirmation'):
                    exceptions[(yr,rawcode,status)]+=1
                if len(bad_samples)<10 and status in ('missing_candidate_key','duplicate_candidate_key_review'):
                    bad_samples.append({'page':page,'index':ix,'key':k,'status':status})
                w.writerow({
                    'source_page':page,'source_index':ix,'disaster_year':txt(r.get('MSTN_YR')),
                    'damage_day':d,'event_region_code':txt(r.get('RGN_CD_1')),
                    'disaster_serial':txt(r.get('MSTN_SN')),
                    'facility_serial':txt(r.get('PBLC_FCLT_SN')),
                    'damage_region_admin10':rawcode,'sgis_dong8':sgis,
                    'use_yn':use_yn,'confirmation_marker':cflag,
                    'last_modified_at':txt(r.get('LAST_MDFCN_DT')),
                    'confirmed_damage_amount_raw':txt(r.get('CFMTN_DMG_AMT')),
                    'amount_numeric_raw_unit':str(a) if a is not None else '',
                    'check_status':status,
                })
                # 집계는 사용여부 Y이고 확인된 지역에만. CFMTN_SPTR 의미 검증 전이므로 예비집계.
                if (sgis and sgis in gold_codes[yr] and use_yn=='Y' and
                    not missing_key and not duplicate_key):
                    q=daily[(yr,sgis,d)]
                    q['rows']+=1;q['used_y_rows']+=1
                    if cflag=='0':q['cf0']+=1
                    elif cflag=='1':q['cf1']+=1
                    elif cflag=='2':q['cf2']+=1
                    else:q['cfother']+=1
                    if a is None or a<0:q['missing_amount_rows']+=1
                    elif a==0:q['zero_rows']+=1
                    else:q['positive_rows']+=1;q['sum']+=a
            if total_pages%200==0:
                print(f'  {total_pages:,}페이지 / 호우 후보 {total_selected:,}행',flush=True)

    if not all_pages:raise RuntimeError('원본 페이지가 없습니다')
    if dup_keys:
        # 같은 후보 키가 발견되면 기존처럼 먼저 등장한 행이 일별 합계에 이미 포함되었을 수 있으므로
        # 자동 중복제거 대신 출력을 중단한다.
        review_gz.unlink(missing_ok=True)
        raise RuntimeError(f'후보 식별자 충돌 {len(dup_keys)}개 발견: 자동 집계 중단. 공식 고유키 재검토 필요')
    daily_cols=['year','admin_dong_code','day','public_record_rows_y',
                'positive_amount_rows','zero_amount_rows','missing_or_negative_amount_rows',
                'known_amount_sum_raw_unit','amount_complete_among_selected_y',
                'confirmation_marker_0','confirmation_marker_1','confirmation_marker_2','confirmation_marker_other',
                'provisional_positive_presence']
    with gzip.open(daily_gz,'wt',encoding='utf-8-sig',newline='') as f:
        w=csv.DictWriter(f,fieldnames=daily_cols)
        w.writeheader()
        for (yr,sgis,d),a in sorted(daily.items()):
            w.writerow({'year':yr,'admin_dong_code':sgis,'day':d,
                        'public_record_rows_y':a['rows'],'positive_amount_rows':a['positive_rows'],
                        'zero_amount_rows':a['zero_rows'],
                        'missing_or_negative_amount_rows':a['missing_amount_rows'],
                        'known_amount_sum_raw_unit':str(a['sum']),
                        'amount_complete_among_selected_y':'Y' if a['missing_amount_rows']==0 else 'N',
                        'confirmation_marker_0':a['cf0'],'confirmation_marker_1':a['cf1'],
                        'confirmation_marker_2':a['cf2'],'confirmation_marker_other':a['cfother'],
                        'provisional_positive_presence':int(a['positive_rows']>0)})
    with exception_csv.open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.writer(f);w.writerow(['year','ndms_admin10','reason','record_count'])
        for (y,code,s),n in sorted(exceptions.items(),key=lambda x:(x[0][0],-x[1],x[0][1])):
            w.writerow([y,code,s,n])

    print('3단계: CSV 압축 결과 검증 및 Parquet 생성',flush=True)
    report_data={
        'stage':'PUBLIC_RAIN_PROVISIONAL_NOT_FINAL',
        'page_count':len(all_pages),
        'source_contiguous_pages_1_to_last': all_pages==set(range(1,max(all_pages)+1)),
        'first_page':min(all_pages),'last_page':max(all_pages),
        'reported_total_count_distribution':dict(reported_totals),
        'complete_api_snapshot': False,
        'api_sample_note':'Live pages 1500/2500/3500/4200/4295 checked by user and contained no 2024-25, but completeness not proven.',
        'selected_2024_2025_rain_rows':total_selected,
        'year_statuses':{year:{s:n for (y,s),n in statuses.items() if y==year} for year in ('2024','2025')},
        'year_amounts_all_use_flags':{year:{s:n for (y,s),n in amounts.items() if y==year} for year in ('2024','2025')},
        'year_use_flags':{year:{s:n for (y,s),n in use_counts.items() if y==year} for year in ('2024','2025')},
        'year_confirmation_markers':{year:{s:n for (y,s),n in confirm_counts.items() if y==year} for year in ('2024','2025')},
        'candidate_key_duplicate_groups':len(dup_keys),
        'candidate_key_duplicate_samples':bad_samples,
        'daily_selected_Y_dong_days':len(daily),
        'daily_selected_Y_positive_dong_days':sum(x['positive_rows']>0 for x in daily.values()),
        'daily_selected_Y_missing_amount_dong_days':sum(x['missing_amount_rows']>0 for x in daily.values()),
        'official_snapshots':{'2024':'2024-10-01','2025':'2025-12-31'},
        'gold_boundary_snapshot':'2025-06-30',
        'caveats':[
            'Only collected source pages, not full 4295 pages. Source API sort not guaranteed.',
            'NDMS_MSTN_TYPE_CD 1001 is provisionally treated as rain; confirm public source code table.',
            'USE_YN means use flag, but exact Y/N business semantics and CFMTN_SPTR 0/1/2 must be confirmed.',
            'USE_YN N rows are preserved in row-level review and withheld from daily totals, not deleted.',
            'CFMTN_SPTR is not filtered because code meanings are unverified.',
            'Positive and zero amounts are numerically parsed, but the monetary UNIT remains unverified.',
            'Missing and negative amounts are never set to zero; daily sum reflects observed positive values only.',
            'The row identity is a candidate key, not confirmed official primary key.',
            'Zero-damage days are not generated; do not treat absence as zero.',
            'Actual damage date vs disaster date can differ; time alignment requires review.'
        ]
    }
    report.write_text(json.dumps(report_data,ensure_ascii=False,indent=2),encoding='utf-8')
    pq1=make_parquet(review_gz,review_pq)
    pq2=make_parquet(daily_gz,daily_pq)
    if pq1 and pq2:
        print('검토 데이터:',review_pq,flush=True)
        print('일별 예비집계:',daily_pq,flush=True)
    else:
        print('검토 데이터(csv.gz):',review_gz,flush=True)
        print('일별 예비집계(csv.gz):',daily_gz,flush=True)
    print('보고서:',report,flush=True)
    print('예외 코드 요약:',exception_csv,flush=True)
    for yr in ('2024','2025'):
        print(f'[{yr}]', {s:n for (y,s),n in statuses.items() if y==yr},flush=True)
    print('전체 대상 호우 후보:',f'{total_selected:,}',flush=True)
    print('일별 사용 Y 관측행:',f'{len(daily):,}',flush=True)
    print('주의: 본 예비집계는 정식 피해 Label이 아닙니다.',flush=True)

if __name__=='__main__':
    main()
