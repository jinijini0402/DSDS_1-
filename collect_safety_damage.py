"""재난안전데이터공유플랫폼 공공시설/사유시설 피해자료 수집.

.env:
    SAFETY_PUBLIC_KEY=공공시설용_승인키
    SAFETY_PRIVATE_KEY=사유시설용_승인키
    SAFETY_PRIVATE_URL=https://www.safetydata.go.kr/V2/api/DSSP-IF-10709

예:
    python collect_safety_damage.py --dataset public --max-pages 1
    python collect_safety_damage.py --dataset private --max-pages 1
"""
import argparse
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

PUBLIC_URL = "https://www.safetydata.go.kr/V2/api/DSSP-IF-10297"
BASE_DIR = Path("data/bronze/labels/safety_damage")
DATASETS = {
    "public": ("public_facilities", "SAFETY_PUBLIC_KEY"),
    "private": ("private_facilities", "SAFETY_PRIVATE_KEY"),
}


def lookup(obj, keys, depth=0):
    if depth > 7 or not isinstance(obj, dict):
        return None
    for key in keys:
        if key in obj and obj[key] is not None:
            return obj[key]
    for child in obj.values():
        if isinstance(child, dict):
            value = lookup(child, keys, depth + 1)
            if value is not None:
                return value
    return None


def extract_rows(obj, depth=0):
    if depth > 7:
        raise ValueError("데이터 배열 탐색 깊이를 초과했습니다.")
    if isinstance(obj, list):
        if all(isinstance(row, dict) for row in obj):
            return obj
        raise ValueError("자료 배열에 객체가 아닌 값이 있습니다.")
    if not isinstance(obj, dict):
        raise ValueError("예상하지 못한 JSON 구조입니다.")

    for key in ("data", "items", "item", "rows", "records", "list", "result", "body", "response"):
        if key in obj and isinstance(obj[key], (dict, list)):
            try:
                return extract_rows(obj[key], depth + 1)
            except ValueError:
                pass
    raise ValueError(f"자료 배열을 찾지 못했습니다. 최상위 키: {list(obj)[:12]}")


def inspect_response(payload):
    if not isinstance(payload, (dict, list)):
        raise ValueError("JSON 응답 구조가 잘못되었습니다.")
    if isinstance(payload, dict):
        code = lookup(payload, ("resultCode",))
        if code is not None and str(code).strip() not in ("00", "0", "0000", "NORMAL_SERVICE", "SUCCESS"):
            message = lookup(payload, ("resultMsg", "resultMessage"))
            raise ValueError(f"API 반환 오류: 코드 {code}, 메시지 {message}")
    rows = extract_rows(payload)
    total = lookup(payload, ("totalCount",))
    try:
        total = int(total) if total is not None else None
    except (ValueError, TypeError):
        total = None
    return rows, total


def request_page(session, url, key, page, rows_per_page):
    params = {
        "serviceKey": key,
        "returnType": "json",
        "pageNo": page,
        "numOfRows": rows_per_page,
    }
    for attempt in range(1, 4):
        try:
            response = session.get(url, params=params, timeout=60)
            response.raise_for_status()
            try:
                payload = response.json()
            except ValueError as exc:
                match = re.search(r"<returnReasonCode>\s*([^<]+)", response.text)
                reason = match.group(1) if match else "미확인"
                raise ValueError(f"JSON이 아닌 응답입니다. 게이트웨이 코드: {reason}") from exc
            return payload, response.content
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status in (400, 401, 403, 404) or attempt == 3:
                raise RuntimeError(f"HTTP 통신 실패 ({type(exc).__name__}, 상태 {status})") from exc


def atomic_write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".tmp")
    temp.write_bytes(content)
    temp.replace(path)


def main():
    parser = argparse.ArgumentParser(description="공공/사유시설 피해 API 원본 페이지 수집")
    parser.add_argument("--dataset", choices=tuple(DATASETS), required=True)
    parser.add_argument("--max-pages", type=int, default=1, help="이번 실행에서 처리할 최대 페이지 (0=전체)")
    parser.add_argument("--rows", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.2)
    args = parser.parse_args()

    if args.max_pages < 0 or args.rows < 1 or args.delay < 0:
        parser.error("max-pages >= 0, rows >= 1, delay >= 0 이어야 합니다.")

    folder_name, env_name = DATASETS[args.dataset]
    key = os.getenv(env_name, "").strip()
    if not key:
        raise SystemExit(f".env에 {env_name}=승인받은_서비스키 를 등록해 주세요.")

    url = PUBLIC_URL if args.dataset == "public" else os.getenv("SAFETY_PRIVATE_URL", "").strip()
    if args.dataset == "private" and not re.fullmatch(
        r"https://(?:www\.)?safetydata\.go\.kr/V2/api/DSSP-IF-\d+", url
    ):
        raise SystemExit("SAFETY_PRIVATE_URL에 https://www.safetydata.go.kr/V2/api/DSSP-IF-숫자 형태로 입력하세요.")

    out = BASE_DIR / folder_name
    raw = out / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    manifest = out / "manifest.jsonl"

    with requests.Session() as session:
        page = 1
        processed = 0
        while args.max_pages == 0 or processed < args.max_pages:
            dest = raw / f"page_{page:05d}.json"
            if dest.is_file():
                content = dest.read_bytes()
                try:
                    payload = json.loads(content.decode("utf-8-sig"))
                    rows, total = inspect_response(payload)
                except (ValueError, UnicodeError) as exc:
                    raise SystemExit(f"기존 파일 읽기 실패: {dest} - {exc}") from exc
                fetched = False
            else:
                try:
                    payload, content = request_page(session, url, key, page, args.rows)
                    rows, total = inspect_response(payload)
                except (ValueError, RuntimeError, requests.RequestException) as exc:
                    raise SystemExit(f"{args.dataset} / 페이지 {page}에서 중단: {exc}") from exc
                if rows:
                    atomic_write(dest, content)
                    entry = {
                        "dataset": args.dataset,
                        "endpoint": url,
                        "page_no": page,
                        "requested_page_size": args.rows,
                        "record_count": len(rows),
                        "total_count_reported": total,
                        "file": dest.as_posix(),
                        "sha256": hashlib.sha256(content).hexdigest(),
                        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
                    }
                    with manifest.open("a", encoding="utf-8") as handle:
                        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
                fetched = True

            processed += 1
            print(f'[{"수집" if fetched else "기존"}] {args.dataset}: '
                  f'{page}페이지, {len(rows)}건, 총 건수 {total if total is not None else "미제공"}')

            if not rows:
                break
            if total is not None and page * args.rows >= total:
                print("전체 페이지 완료 (totalCount 기준)")
                break
            if total is None and len(rows) < args.rows:
                print("마지막 페이지 추정 (반환 건수 기준)")
                break
            page += 1

    print("원본 저장 위치:", out)
    print("주의: 2024~2025년·호우 필터링 전 자료입니다.")


if __name__ == "__main__":
    main()
