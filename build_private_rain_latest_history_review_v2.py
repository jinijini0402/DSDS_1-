"""사유시설 호우 이력: 수정시각 동률을 보정한 최신 이력 후보 검증 (v2).

실행 (기존 프로젝트 루트):
  python -u build_private_rain_latest_history_review_v2.py
  python -u build_private_rain_latest_history_review_v2.py --max-pages 20  # 테스트

원본은 변경하지 않습니다. 피해금액 최종 합계나 최종 Label이 아닙니다.
전제: 사용자가 제공한 VLD_DTIN_SPTR 설명을 사용하며,
      최신 판정에 DAM_HSTRY_SN(숫자 최대값)을 임시 사용합니다.
      집계 전 공식 고유키/이력 정렬 규칙 확인이 필요합니다.
"""

import argparse
import csv
import gzip
import json
import re
import sqlite3
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path

RAW = Path('data/bronze/labels/safety_damage/private_facilities/raw')
ZIP = Path('data/bronze/labels/safety_damage/private_facilities.zip')
OUT = Path('data/silver/labels/rain_damage')
QC = Path('data/silver/quality_checks/labels')

# 이번 스크립트는 2024, 2025년 재난 이력 전체를 먼저 검사합니다.
# 호우(1001)와 피해일자(24~25년)는 그룹별 최신 이력을 선택한 다음 적용합니다.


def value(x):
    return '' if x is None else str(x).strip()


def parse_day(x):
    digits = re.sub(r'\D', '', value(x))[:8]
    if len(digits) != 8:
        return None
    try:
        return datetime.strptime(digits, '%Y%m%d').strftime('%Y-%m-%d')
    except ValueError:
        return None


def raw_pages(raw_dir, zip_path, max_pages):
    if raw_dir.is_dir():
        pages = []
        for p in raw_dir.glob('page_*.json'):
            m = re.fullmatch(r'page_(\d+)\.json', p.name)
            if m:
                pages.append((int(m.group(1)), p))
        pages.sort()
        if not pages:
            raise RuntimeError('raw 폴더에 page_*.json이 없습니다.')
        print('원본:', raw_dir, flush=True)
        for idx, (page_no, path) in enumerate(pages):
            if max_pages and idx >= max_pages:
                break
            with path.open(encoding='utf-8-sig') as f:
                yield page_no, json.load(f)
    elif zip_path.is_file():
        print('원본:', zip_path, flush=True)
        with zipfile.ZipFile(zip_path) as z:
            pages = []
            for name in z.namelist():
                m = re.search(r'(?:^|/)raw/page_(\d+)\.json$', name)
                if m:
                    pages.append((int(m.group(1)), name))
            pages.sort()
            if not pages:
                raise RuntimeError('ZIP에서 raw/page_*.json을 찾지 못했습니다.')
            for idx, (page_no, name) in enumerate(pages):
                if max_pages and idx >= max_pages:
                    break
                yield page_no, json.loads(z.read(name))
    else:
        raise FileNotFoundError(f'원본 없음: {raw_dir} / {zip_path}')


# 매 행을 SQLite에서 동일 피해 후보의 최대 이력번호와 비교해 저장한다.
# 기록 원본이 피해일/재난유형별로 나뉘어 있어도 과거 이력이 부활하지 않도록
# WHERE 조건으로 먼저 호우/피해일을 필터링하지 않는다.
UPSERT = '''
INSERT INTO latest (
  yr, region, event_no, victim_no, damage_no,
  history_no, status, disaster_code, day_raw, damage_region,
  amount_raw, modified_at, source_page, source_index,
  history_rows, latest_time, history_at_latest_time, ambiguous_latest_revision
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,0)
ON CONFLICT(yr,region,event_no,victim_no,damage_no) DO UPDATE SET
 history_rows=latest.history_rows+1,
 ambiguous_latest_revision = CASE
    WHEN excluded.history_no > latest.history_no THEN 0
    WHEN excluded.history_no = latest.history_no AND
      (excluded.status<>latest.status OR excluded.disaster_code<>latest.disaster_code
       OR excluded.day_raw<>latest.day_raw OR excluded.damage_region<>latest.damage_region
       OR excluded.amount_raw<>latest.amount_raw)
      THEN latest.ambiguous_latest_revision + 1
    ELSE latest.ambiguous_latest_revision END,
 history_at_latest_time=CASE
    WHEN excluded.modified_at>latest.latest_time THEN excluded.history_no
    WHEN excluded.modified_at=latest.latest_time
      THEN MAX(excluded.history_no, latest.history_at_latest_time)
    ELSE latest.history_at_latest_time END,
 latest_time=CASE WHEN excluded.modified_at>latest.latest_time
                  THEN excluded.modified_at ELSE latest.latest_time END,
 history_no=CASE WHEN excluded.history_no>latest.history_no THEN excluded.history_no ELSE latest.history_no END,
 status=CASE WHEN excluded.history_no>latest.history_no THEN excluded.status ELSE latest.status END,
 disaster_code=CASE WHEN excluded.history_no>latest.history_no THEN excluded.disaster_code ELSE latest.disaster_code END,
 day_raw=CASE WHEN excluded.history_no>latest.history_no THEN excluded.day_raw ELSE latest.day_raw END,
 damage_region=CASE WHEN excluded.history_no>latest.history_no THEN excluded.damage_region ELSE latest.damage_region END,
 amount_raw=CASE WHEN excluded.history_no>latest.history_no THEN excluded.amount_raw ELSE latest.amount_raw END,
 modified_at=CASE WHEN excluded.history_no>latest.history_no THEN excluded.modified_at ELSE latest.modified_at END,
 source_page=CASE WHEN excluded.history_no>latest.history_no THEN excluded.source_page ELSE latest.source_page END,
 source_index=CASE WHEN excluded.history_no>latest.history_no THEN excluded.source_index ELSE latest.source_index END
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--max-pages', type=int, default=0,
                        help='0=전체 수집본, 양수=테스트할 페이지 개수')
    parser.add_argument('--raw-dir', type=Path, default=RAW)
    parser.add_argument('--zip', type=Path, default=ZIP)
    args = parser.parse_args()

    if args.max_pages < 0:
        raise SystemExit('--max-pages에는 0 이상의 숫자를 사용하세요.')

    suffix = f'_TEST_{args.max_pages}' if args.max_pages else ''
    OUT.mkdir(parents=True, exist_ok=True)
    QC.mkdir(parents=True, exist_ok=True)
    staging = QC / f'_private_history_stage_v2{suffix}.sqlite3'
    out_gz = OUT / f'private_rain_latest_history_review_v2{suffix}.csv.gz'
    out_parquet = OUT / f'private_rain_latest_history_review_v2{suffix}.parquet'
    report_file = QC / f'private_rain_latest_history_report_v2{suffix}.json'

    if staging.exists():
        raise SystemExit(f'임시 DB가 이미 있습니다: {staging}\n'
                         '다른 실행이 진행 중인지 먼저 확인하세요.')

    conn = sqlite3.connect(staging)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('''
      CREATE TABLE latest (
        yr TEXT NOT NULL, region TEXT NOT NULL,
        event_no TEXT NOT NULL, victim_no TEXT NOT NULL, damage_no TEXT NOT NULL,
        history_no INTEGER NOT NULL,
        status TEXT NOT NULL, disaster_code TEXT NOT NULL,
        day_raw TEXT, damage_region TEXT, amount_raw TEXT,
        modified_at TEXT NOT NULL, source_page INTEGER, source_index INTEGER,
        history_rows INTEGER NOT NULL,
        latest_time TEXT NOT NULL, history_at_latest_time INTEGER NOT NULL,
        ambiguous_latest_revision INTEGER NOT NULL,
        PRIMARY KEY (yr,region,event_no,victim_no,damage_no)
      )
    ''')

    counter = Counter()
    seen_pages = set()
    buffered = []
    normal_end = False

    try:
        for page, payload in raw_pages(args.raw_dir, args.zip, args.max_pages):
            if page in seen_pages:
                raise ValueError(f'중복된 원본 페이지: {page}')
            seen_pages.add(page)
            code = value(payload.get('header', {}).get('resultCode'))
            if code not in ('', '00', '0', '0000'):
                raise ValueError(f'원본 페이지 {page}: API 오류코드 {code}')
            rows = payload.get('body')
            if not isinstance(rows, list):
                raise ValueError(f'원본 페이지 {page}: body가 리스트가 아닙니다.')
            counter['all_source_rows'] += len(rows)
            for index, r in enumerate(rows):
                yr = value(r.get('MSTN_YR'))
                if yr not in ('2024', '2025'):
                    continue
                counter['source_rows_year_2024_2025'] += 1
                if value(r.get('MSTN_TYPE_CD')) == '1001':
                    day = parse_day(r.get('DMG_YMD'))
                    if day and '2024-01-01' <= day <= '2025-12-31':
                        counter['raw_rain_in_period_rows'] += 1

                key = tuple(value(r.get(f)) for f in (
                    'MSTN_YR','RGN_CD','MSTN_SN','VCTM_SN','DAM_SN'
                ))
                history_str = value(r.get('DAM_HSTRY_SN'))
                if any(not x for x in key) or not re.fullmatch(r'\d+', history_str):
                    counter['excluded_missing_key_or_history'] += 1
                    continue
                buffered.append((*key,
                    int(history_str),
                    value(r.get('VLD_DTIN_SPTR')),
                    value(r.get('MSTN_TYPE_CD')),
                    value(r.get('DMG_YMD')),
                    value(r.get('DAM_RGN_DONG_CD')),
                    value(r.get('CFMTN_DMG_AMT')),
                    value(r.get('LAST_MDFCN_DT')),
                    page, index,
                    value(r.get('LAST_MDFCN_DT')),int(history_str)
                ))
                if len(buffered) >= 15000:
                    conn.executemany(UPSERT, buffered)
                    conn.commit()
                    buffered.clear()

            if len(seen_pages) % 300 == 0:
                print(f"  {len(seen_pages):,}페이지 / 연도 대상 {counter['source_rows_year_2024_2025']:,}행",flush=True)

        if buffered:
            conn.executemany(UPSERT, buffered)
            conn.commit()
            buffered.clear()

        marker_dist = dict(conn.execute('SELECT status,COUNT(*) FROM latest GROUP BY status'))
        discrepancy = conn.execute('''
            SELECT COUNT(*) FROM latest
            WHERE history_rows > 1 AND modified_at < latest_time
        ''').fetchone()[0]
        tied_timestamp_groups = conn.execute("SELECT COUNT(*) FROM latest WHERE history_rows>1 AND modified_at=latest_time").fetchone()[0]
        ambiguous = conn.execute('''
            SELECT COUNT(*) FROM latest WHERE ambiguous_latest_revision > 0
        ''').fetchone()[0]

        outputs = Counter()
        fields = [
            'disaster_year','event_region_code','disaster_serial',
            'victim_serial','damage_serial',
            'damage_day','admin_code_raw','confirmed_damage_amount_raw',
            'valid_marker','disaster_type_code','history_serial',
            'history_row_count','history_vs_modified_disagrees',
            'selected_history_modified_at','latest_group_modified_at',
            'source_page','source_index'
        ]
        with gzip.open(out_gz,'wt',encoding='utf-8-sig',newline='') as out:
            writer = csv.DictWriter(out,fieldnames=fields)
            writer.writeheader()
            for row in conn.execute('''
                SELECT yr, region, event_no, victim_no, damage_no,
                       status, disaster_code, day_raw, damage_region, amount_raw,
                       history_no, history_rows, modified_at, latest_time,
                       ambiguous_latest_revision, source_page, source_index
                FROM latest WHERE yr IN ('2024','2025')
            '''):
                (yr,event_region,event_no,victim_no,damage_no,
                 status,code,day_raw,region,amount,hist,n,modified_at,max_modified_at,
                 ambiguity,page,idx) = row
                outputs['all_latest_groups'] += 1
                if status == 'D':
                    outputs['latest_deleted_groups'] += 1
                    continue
                if status not in ('U','V','I'):
                    outputs['latest_unknown_status_groups'] += 1
                    continue
                outputs['latest_active_groups'] += 1
                if code != '1001':
                    continue
                outputs['active_rain_groups'] += 1
                day = parse_day(day_raw)
                if day is None or not ('2024-01-01' <= day <= '2025-12-31'):
                    outputs['active_rain_invalid_or_outside_day'] += 1
                    continue
                if ambiguity:
                    outputs['active_rain_ambiguous_latest_revision'] += 1
                    continue
                if not re.fullmatch(r'\d{10}', region):
                    outputs['active_rain_invalid_region_code'] += 1
                    continue
                if not amount or not re.fullmatch(r'-?\d+(?:\.\d+)?',amount.replace(',','')):
                    outputs['active_rain_missing_or_non_numeric_amount'] += 1
                writer.writerow({
                    'disaster_year':yr,'event_region_code':event_region,
                    'disaster_serial':event_no,'victim_serial':victim_no,
                    'damage_serial':damage_no,
                    'damage_day':day,'admin_code_raw':region,
                    'confirmed_damage_amount_raw':amount,'valid_marker':status,
                    'disaster_type_code':code,'history_serial':hist,
                    'history_row_count':n,
                    'history_vs_modified_disagrees':int(modified_at < max_modified_at),
                    'selected_history_modified_at':modified_at,
                    'latest_group_modified_at':max_modified_at,
                    'source_page':page,'source_index':idx,
                })
                outputs['exported_review_rows'] += 1

        # DuckDB가 설치돼 있으면 작은 Parquet으로 변환하고 임시 CSV.gz를 제거한다.
        actual_output = out_gz
        try:
            import duckdb
            db = duckdb.connect(database=':memory:')
            quoted_gz = out_gz.as_posix().replace("'", "''")
            quoted_parquet = out_parquet.as_posix().replace("'", "''")
            db.execute(f"COPY (SELECT * FROM read_csv('{quoted_gz}', header=true, all_varchar=true, compression='gzip')) TO '{quoted_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            db.close()
            actual_output = out_parquet
            out_gz.unlink()
        except ImportError:
            print('DuckDB 미설치: 압축 CSV.gz로 결과를 남깁니다.', flush=True)

        report = {
            'status': 'PROVISIONAL_REVIEW_V2_NOT_FINAL_LABEL',
            'status_meanings_basis': 'user-provided U/V/I valid, D invalid; last-history decision',
            'history_selection_rule': 'max numeric DAM_HSTRY_SN per candidate key',
            'candidate_key': ['MSTN_YR','RGN_CD','MSTN_SN','VCTM_SN','DAM_SN'],
            'candidate_key_officially_verified':False,
            'history_sort_officially_verified':False,
            'source':str(args.raw_dir if args.raw_dir.is_dir() else args.zip),
            'processed_page_count':len(seen_pages),
            'first_page':min(seen_pages) if seen_pages else None,
            'last_page':max(seen_pages) if seen_pages else None,
            'missing_page_count':len(set(range(1,max(seen_pages)+1))-seen_pages) if seen_pages else None,
            'partial_test':bool(args.max_pages),
            'counter':dict(counter),
            'latest_marker_distribution':marker_dist,
            'groups_latest_history_strictly_older_than_latest_modification':discrepancy,
            'groups_latest_revision_has_max_modified_time':tied_timestamp_groups,
            'groups_with_ambiguous_max_history':ambiguous,
            'output_stats':dict(outputs),
            'output_file':str(actual_output),
            'notes':[
                'Original API pages are never modified.',
                'Do not sum all U/V rows. Retain one candidate latest history per group, then apply D exclusion.',
                'Latest by history number remains provisional; timestamp ties were previously counted as disagreements. The reported mismatch now uses strictly older modification times.',
                'Records with invalid region code or ambiguous highest revision are excluded from this review export and counted.',
                'No final amount sum, Gold join, or zero-label creation is performed.',
                'Previous Silver and raw sources differed by 14 records; the present export uses only the stated source snapshot.',
            ]
        }
        report_file.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
        normal_end = True
        print('\n===== 최신 이력 검토용 추출 완료 =====')
        print('최신 이력 후보 그룹:',outputs['all_latest_groups'])
        print('최신 상태별:',marker_dist)
        print('최신 D 제외 그룹:',outputs['latest_deleted_groups'])
        print('호우 후보 유효 이력:',outputs['active_rain_groups'])
        print('검토용 저장 행:',outputs['exported_review_rows'])
        print('최대 이력번호의 수정시각이 엄격하게 이전인 그룹:',discrepancy)
        print('최대 이력번호의 수정시각이 최댓값과 같았던 그룹:',tied_timestamp_groups)
        print('최대 이력번호 중복·내용 충돌 그룹:',ambiguous)
        print('검토 데이터:',actual_output)
        print('보고서:',report_file)
        print('주의: 최종 피해액 합산이나 Gold Label은 아닙니다.')
    finally:
        conn.close()
        if normal_end:
            staging.unlink(missing_ok=True)
            Path(str(staging)+'-wal').unlink(missing_ok=True)
            Path(str(staging)+'-shm').unlink(missing_ok=True)

if __name__ == '__main__':
    main()
