
import os
import json
import time
import argparse
import hashlib
from pathlib import Path
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("KMA_API_KEY")
if not API_KEY:
    raise RuntimeError(".env의 KMA_API_KEY를 확인하세요.")

URL = "https://apihub.kma.go.kr/api/typ01/url/awsh.php"

SEOUL_IDS = {
    "108", "110",
    *[str(i) for i in range(400, 420)],
    "421", "423", "424", "425",
    "509", "510", "889"
}

START = datetime(2021, 1, 1, 0, 0)
END = datetime(2024, 12, 31, 23, 0)

BASE = Path("data/bronze/weather/seoul_hourly")
RAW = BASE / "raw"
FILTERED = BASE / "seoul"
MANIFEST = BASE / "manifest.jsonl"

RAW.mkdir(parents=True, exist_ok=True)
FILTERED.mkdir(parents=True, exist_ok=True)

parser = argparse.ArgumentParser()
parser.add_argument("--max-calls", type=int, default=12)
args = parser.parse_args()

if args.max_calls < 1:
    raise ValueError("--max-calls는 1 이상이어야 합니다.")

current = START
calls = 0
saved = 0

with requests.Session() as session:
    while current <= END and calls < args.max_calls:
        tm = current.strftime("%Y%m%d%H%M")

        raw_file = RAW / f"aws_all_{tm}.json"
        seoul_file = FILTERED / f"seoul_{tm}.json"

        # 두 파일이 있으면 이미 수집한 시각
        if raw_file.exists() and seoul_file.exists():
            current += timedelta(hours=1)
            continue

        params = {
            "tm": tm,
            "stn": "0",
            "disp": "1",
            "authKey": API_KEY
        }

        calls += 1

        try:
            response = session.get(
                URL, params=params, timeout=60
            )
            response.raise_for_status()

            rows = response.json()
            if not isinstance(rows, list) or not rows:
                raise ValueError("관측자료 배열이 비어 있음")

            seoul_rows = [
                row for row in rows
                if str(row.get("AWS_ID")) in SEOUL_IDS
                and str(row.get("TM")) == tm
            ]

            if not seoul_rows:
                raise ValueError("서울 관측자료가 없음")

            # 기존 파일을 직접 덮어쓰지 않고 임시 저장
            raw_tmp = raw_file.with_suffix(".tmp")
            seoul_tmp = seoul_file.with_suffix(".tmp")

            raw_tmp.write_bytes(response.content)
            seoul_tmp.write_text(
                json.dumps(
                    seoul_rows, ensure_ascii=False, indent=2
                ),
                encoding="utf-8"
            )

            raw_tmp.replace(raw_file)
            seoul_tmp.replace(seoul_file)

            entry = {
                "source_dataset": "kma_aws_hourly",
                "provider": "기상청",
                "endpoint": URL,
                "requested_time_kst": tm,
                "collected_at": datetime.now(
                    timezone.utc
                ).isoformat(),
                "bronze_file": raw_file.as_posix(),
                "seoul_file": seoul_file.as_posix(),
                "sha256": hashlib.sha256(
                    response.content
                ).hexdigest(),
                "raw_record_count": len(rows),
                "seoul_record_count": len(seoul_rows),
                "collection_status": "parsed_unvalidated"
            }

            with MANIFEST.open("a", encoding="utf-8") as f:
                f.write(
                    json.dumps(entry, ensure_ascii=False) + "\n"
                )

            saved += 1
            print(
                f"[성공] {tm} | "
                f"전국 {len(rows)}건 | "
                f"서울 {len(seoul_rows)}건"
            )

        except (requests.RequestException, ValueError) as e:
            print(f"[중단] {tm} | {type(e).__name__}")
            print("인증키·요청 상태 또는 응답 자료 확인 필요")
            break

        current += timedelta(hours=1)
        time.sleep(0.0)

print(f"\n이번 실행: API {calls}회, 저장 {saved}시간")
print("저장 폴더:", BASE)
