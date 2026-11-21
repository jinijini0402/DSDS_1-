#!/usr/bin/env python3
"""data/bronze 전체 파일 목록/무결성 manifest 재생성 (API 호출이나 원본 변경 없음).

예시: python -u rebuild_bronze_manifest.py
      python -u rebuild_bronze_manifest.py --count-json-rows

출력: data/bronze/manifests/bronze_manifest.csv
      data/bronze/manifests/bronze_manifest.jsonl
      data/bronze/manifests/bronze_manifest_summary.json

내용: 각 파일의 상대경로, bytes, UTC mtime, SHA-256, 수집 페이지 번호(있는 경우).
검증 가능한 body=list인 JSON에 대해서만 --count-json-rows 시 record_count를 기입.
주의: ZIP은 압축파일 자체를 해시하고, ZIP 내 개별 페이지 개수는 별도 보증하지 않음.
      source totalCount/전체 수집 완결성은 이 manifest만으로 증명하지 못함.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

FIELDS = [
    'relative_path', 'source_group', 'extension', 'size_bytes',
    'modified_utc', 'modified_ns', 'sha256', 'page_number',
    'json_body_records', 'json_result_code'
]
DENY_NAMES = {'.ds_store', '.env', 'credentials.json', 'token.json', 'secrets.json'}
DENY_WORDS = ('api_key', 'auth_key', 'apikey', 'access_token', 'private_key', 'credential', 'secret')
PAGE = re.compile(r'^page_(\d+)\.json$', re.IGNORECASE)


def source_group(p: Path) -> str:
    segs = p.parts
    if not segs:
        return 'unknown'
    if segs[0] == 'labels' and len(segs) >= 3 and segs[1] == 'safety_damage':
        return '/'.join(segs[:3])
    if segs[0] == 'weather' and len(segs) >= 2:
        return '/'.join(segs[:2])
    if segs[0] == 'boundaries' and len(segs) >= 2:
        return '/'.join(segs[:2])
    return '/'.join(segs[:2]) if len(segs) > 1 else segs[0]


def should_skip(p: Path) -> bool:
    if any(part.startswith('.') or part == '__MACOSX' for part in p.parts):
        return True
    low = p.name.lower()
    if low in DENY_NAMES or any(term in low for term in DENY_WORDS):
        return True
    return (low.endswith('.tmp') or low.endswith('.part') or low.endswith('~'))


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open('rb') as f:
        for block in iter(lambda: f.read(4 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def json_metadata(p: Path) -> tuple[str, str]:
    try:
        with p.open('r', encoding='utf-8-sig') as f:
            payload = json.load(f)
    except (OSError, ValueError, UnicodeDecodeError):
        return '', ''
    if not isinstance(payload, dict):
        return '', ''
    body = payload.get('body')
    count = str(len(body)) if isinstance(body, list) else ''
    header = payload.get('header')
    result = str(header.get('resultCode', '')) if isinstance(header, dict) else ''
    return count, result


def temp_file_in(folder: Path) -> Path:
    fd, path = tempfile.mkstemp(prefix='.bronze_manifest_', suffix='.tmp', dir=folder)
    os.close(fd)
    return Path(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--project-root', type=Path, default=Path.cwd())
    parser.add_argument('--count-json-rows', action='store_true',
                        help='JSON body 배열을 읽어서 record_count까지 기록 (더 오래 걸림)')
    args = parser.parse_args()
    root = args.project_root.expanduser().resolve()
    bronze = root / 'data' / 'bronze'
    if not bronze.is_dir():
        raise SystemExit('프로젝트 최상위에서 실행하세요: data/bronze 폴더를 찾지 못했습니다.')
    manifests = bronze / 'manifests'
    manifests.mkdir(exist_ok=True)
    output_csv = manifests / 'bronze_manifest.csv'
    output_jsonl = manifests / 'bronze_manifest.jsonl'
    output_summary = manifests / 'bronze_manifest_summary.json'

    files = []
    skipped_links = []
    for p in bronze.rglob('*'):
        if p.is_symlink():
            skipped_links.append(p.relative_to(root).as_posix())
            continue
        if not p.is_file() or p.is_relative_to(manifests):
            continue
        relative = p.relative_to(bronze)
        if should_skip(relative):
            continue
        files.append(p)
    files.sort(key=lambda p: p.relative_to(bronze).as_posix())
    if not files:
        raise SystemExit('data/bronze에 원본 파일이 없습니다. 기존 manifest를 덮어쓰지 않습니다.')

    csv_tmp, jsonl_tmp, summary_tmp = (temp_file_in(manifests) for _ in range(3))
    groups = defaultdict(lambda: {'file_count': 0, 'total_bytes': 0,
                                  'page_count': 0, 'min_page_seen': None,
                                  'max_page_seen': None, 'json_body_records_counted': 0})
    total_bytes = 0
    counts_with_json = 0
    try:
        print(f'Bronze 원본 목록 확인: {len(files):,}개 파일')
        print('원본은 변경하지 않고 manifest만 새로 생성합니다.', flush=True)
        with csv_tmp.open('w', encoding='utf-8-sig', newline='') as csvout, \
             jsonl_tmp.open('w', encoding='utf-8') as jsonout:
            writer = csv.DictWriter(csvout, fieldnames=FIELDS)
            writer.writeheader()
            for i, p in enumerate(files, 1):
                stat = p.stat()
                rel_bronze = p.relative_to(bronze)
                grp = source_group(rel_bronze)
                page_match = PAGE.match(p.name)
                page_no = int(page_match.group(1)) if page_match else None
                body_count, result_code = ('', '')
                if args.count_json_rows and p.suffix.lower() == '.json':
                    body_count, result_code = json_metadata(p)
                record = {
                    'relative_path': p.relative_to(root).as_posix(),
                    'source_group': grp,
                    'extension': p.suffix.lower(),
                    'size_bytes': stat.st_size,
                    'modified_utc': datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                    'modified_ns': stat.st_mtime_ns,
                    'sha256': sha256_file(p),
                    'page_number': page_no if page_no is not None else '',
                    'json_body_records': body_count,
                    'json_result_code': result_code,
                }
                writer.writerow(record)
                jsonout.write(json.dumps(record, ensure_ascii=False) + '\n')
                total_bytes += stat.st_size
                x = groups[grp]
                x['file_count'] += 1
                x['total_bytes'] += stat.st_size
                if body_count != '':
                    x['json_body_records_counted'] += int(body_count)
                    counts_with_json += 1
                if page_no is not None:
                    x['page_count'] += 1
                    x['min_page_seen'] = page_no if x['min_page_seen'] is None else min(page_no, x['min_page_seen'])
                    x['max_page_seen'] = page_no if x['max_page_seen'] is None else max(page_no, x['max_page_seen'])
                if i % 500 == 0 or i == len(files):
                    print(f'  처리 {i:,}/{len(files):,}개 | SHA-256 계산 완료', flush=True)
        summary = {
            'schema_version': '1.0',
            'generated_at_utc': datetime.now(timezone.utc).isoformat(),
            'project_relative_root': 'data/bronze',
            'scanned_file_count': len(files),
            'scanned_total_bytes': total_bytes,
            'scanned_total_gib': round(total_bytes / (1024 ** 3), 3),
            'json_body_row_count_enabled': args.count_json_rows,
            'json_files_with_body_counts': counts_with_json,
            'source_groups': dict(sorted(groups.items())),
            'skipped_symlinks': skipped_links,
            'excluded_from_scan': ['data/bronze/manifests/**/*', 'hidden files', 'secret-named files'],
            'warnings': [
                'SHA-256 proves file content fingerprint only, not data correctness or API page completeness.',
                'ZIP files are hashed as ZIP files; ZIP contents are not counted separately.',
                'The public-facility API result count was 429495; collected raw pages were 1096, and completeness is NOT established by this manifest.',
                'Files in data/_archive are excluded because they are outside Bronze.',
                'No API keys or private credential values are recorded.'
            ]
        }
        with summary_tmp.open('w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        # Successful full scan before committing updates
        os.replace(csv_tmp, output_csv)
        os.replace(jsonl_tmp, output_jsonl)
        os.replace(summary_tmp, output_summary)
    finally:
        for temp in (csv_tmp, jsonl_tmp, summary_tmp):
            temp.unlink(missing_ok=True)
    print('\nBronze manifest 재생성 완료')
    print(f'  파일: {len(files):,}개 / 총 {total_bytes/(1024**3):.2f} GiB')
    print(' ', output_csv.relative_to(root))
    print(' ', output_jsonl.relative_to(root))
    print(' ', output_summary.relative_to(root))


if __name__ == '__main__':
    main()
