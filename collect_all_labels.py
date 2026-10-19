
import os
import json
import time
import hashlib
import argparse
import math
from pathlib import Path
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

# 1. API 키 및 설정 읽기
load_dotenv()

api_key = os.getenv("SAFETY_API_KEY")
if not api_key:
    raise ValueError(".env에 SAFETY_API_KEY가 없습니다.")

config = json.loads(
    Path("config/labels.json").read_text(encoding="utf-8")
)

url = config["endpoint"]
page_size = 100

# 2. 실행당 최대 요청 횟수
parser = argparse.ArgumentParser()
parser.add_argument("--max-calls", type=int, default=5)
args = parser.parse_args()

if not 1 <= args.max_calls <= 100:
    raise ValueError("--max-calls는 1~100 사이여야 합니다.")

# 3. 저장 경로
out = Path("data/bronze/labels/full_100")
out.mkdir(parents=True, exist_ok=True)

manifest = out / "manifest.jsonl"

# 저장 완료된 페이지를 건너뛰기 위한 설정
page = 1
calls = 0
total_pages = None

with requests.Session() as session:
    while calls < args.max_calls:

        file_path = out / f"page_{page:05d}.json"

        # 이미 저장된 파일 검증 후 건너뛰기
        if file_path.exists():
            saved = json.loads(
                file_path.read_text(encoding="utf-8")
            )

            if saved.get("header", {}).get("resultCode") != "00":
                raise RuntimeError("기존 파일 API 오류")

            if saved.get("pageNo") != page:
                raise RuntimeError("기존 파일 페이지 번호 오류")

            rows = saved.get("body")
            if not isinstance(rows, list) or not rows:
                raise RuntimeError("기존 파일 데이터 오류")

            count = saved.get("totalCount")
            if not isinstance(count, int) or count < 1:
                raise RuntimeError("전체 건수 확인 실패")

            pages = math.ceil(count / page_size)
            if total_pages is None:
                total_pages = pages
            elif total_pages != pages:
                raise RuntimeError("전체 건수가 변경되었습니다.")

            print(f"{page}페이지: 기존 파일 확인, 건너뜀")

            if page >= total_pages:
                print("모든 페이지 저장 완료!")
                break

            page += 1
            continue

        # API 요청 조건
        params = dict(config["params"])
        params["pageNo"] = page
        params["numOfRows"] = page_size
        params[config["key_param"]] = api_key

        print(f"{page}페이지 요청 중...")

        # API 호출 (호출 횟수 보수적으로 계산)
        calls += 1
        response = session.get(
            url,
            params=params,
            timeout=60
        )
        response.raise_for_status()

        data = response.json()

        if data.get("header", {}).get("resultCode") != "00":
            raise RuntimeError(
                "API 오류: " +
                str(data.get("header", {}).get("resultMsg"))
            )

        if data.get("pageNo") != page:
            raise RuntimeError("응답 페이지 번호 불일치")

        rows = data.get("body")
        count = data.get("totalCount")

        if not isinstance(rows, list) or not rows:
            raise RuntimeError("침수 데이터가 없습니다.")

        if not isinstance(count, int) or count < 1:
            raise RuntimeError("totalCount가 올바르지 않습니다.")

        pages = math.ceil(count / page_size)

        if total_pages is None:
            total_pages = pages
        elif total_pages != pages:
            raise RuntimeError("수집 중 전체 건수가 변경되었습니다.")

        expected = min(
            page_size,
            count - (page - 1) * page_size
        )

        if len(rows) != expected:
            raise RuntimeError(
                f"데이터 건수 불일치: {len(rows)} / {expected}"
            )

        # 인증키가 원본에 포함되지 않았는지 확인
        if api_key.encode() in response.content:
            raise RuntimeError(
                "응답에 인증키가 포함되어 저장을 중단합니다."
            )

        # 4. Bronze 원본 저장
        file_path.write_bytes(response.content)

        # 5. Manifest 저장
        record = {
            "source_dataset": "MOIS flood trace",
            "page_no": page,
            "page_size": page_size,
            "record_count": len(rows),
            "total_count": count,
            "collected_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "bronze_file": str(file_path),
            "sha256": hashlib.sha256(
                response.content
            ).hexdigest(),
            "status": "validated_download"
        }

        with manifest.open("a", encoding="utf-8") as f:
            f.write(
                json.dumps(record, ensure_ascii=False) + "\n"
            )

        print(
            f"저장 완료: {page}페이지, "
            f"{len(rows)}건, "
            f"이번 실행 API 호출 {calls}회"
        )

        if page >= total_pages:
            print("모든 페이지 저장 완료!")
            break

        page += 1
        time.sleep(1)

print(f"이번 실행에서 사용한 API 요청: {calls}회")
print("저장 경로:", out)
