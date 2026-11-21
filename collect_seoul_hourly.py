
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

# 전국 AWS, 2024~2025년
START = datetime(2024, 1, 1, 0, 0)
END = datetime(2025, 12, 31, 23, 0)

# 기존 서울 데이터와 분리하여 저장
BASE = Path("data/bronze/weather/all_hourly")
RAW = BASE / "raw"
MANIFEST = BASE / "manifest.jsonl"

RAW.mkdir(parents=True, exist_ok=True)

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

        # 이미 저장한 시각은 다시 요청하지 않음
        if raw_file.exists():
            current += timedelta(hours=1)
            continue

        params = {
            "tm": tm,
            "stn": "0",       # 전국 전체 관측소
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
                raise ValueError("전국 관측자료 배열이 비어 있음")

            # 임시 파일 저장 후 이름 변경
            raw_tmp = raw_file.with_suffix(".tmp")
            raw_tmp.write_bytes(response.content)
            raw_tmp.replace(raw_file)

            entry = {
                "source_dataset": "kma_aws_hourly",
                "provider": "기상청",
                "endpoint": URL,
                "requested_time_kst": tm,
                "collected_at": datetime.now(
                    timezone.utc
                ).isoformat(),
                "bronze_file": raw_file.as_posix(),
                "sha256": hashlib.sha256(
                    response.content
                ).hexdigest(),
                "raw_record_count": len(rows),
                "collection_status": "parsed_unvalidated"
            }

            with MANIFEST.open("a", encoding="utf-8") as f:
                f.write(
                    json.dumps(entry, ensure_ascii=False) + "\n"
                )

            saved += 1

            print(
                f"[성공] {tm} | 전국 {len(rows)}건"
            )

        except (requests.RequestException, ValueError) as e:
            print(f"[중단] {tm} | {type(e).__name__}: {e}")
            print("인증키·요청 상태 또는 응답 자료 확인 필요")
            break

        current += timedelta(hours=1)
        time.sleep(0.0)

print(f"\n이번 실행: API {calls}회, 저장 {saved}시간")
print("저장 폴더:", BASE)
