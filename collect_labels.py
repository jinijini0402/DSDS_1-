
import os
import json
import time
import hashlib
import argparse
from pathlib import Path
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

# 1. API 인증키 불러오기
load_dotenv()

api_key = os.getenv("SAFETY_API_KEY")

if not api_key:
    raise ValueError(".env에서 SAFETY_API_KEY를 찾을 수 없습니다.")

# 2. 기존 API 설정 파일 불러오기
config = json.loads(
    Path("config/labels.json").read_text(encoding="utf-8")
)

url = config["endpoint"]
params = dict(config["params"])
params[config["key_param"]] = api_key

# 3. 페이지 수 설정
parser = argparse.ArgumentParser()
parser.add_argument("--start", type=int, default=2)
parser.add_argument("--end", type=int, default=3)
args = parser.parse_args()

if args.start < 1 or args.end < args.start:
    raise ValueError("페이지 범위를 확인하세요.")

# 4. 저장 폴더 생성
output_dir = Path("data/bronze/labels/pages")
output_dir.mkdir(parents=True, exist_ok=True)

manifest_path = output_dir / "manifest.jsonl"

# 5. 페이지별 API 호출
for page in range(args.start, args.end + 1):

    params["pageNo"] = page

    print(f"{page}페이지 요청 중...")

    response = requests.get(
        url,
        params=params,
        timeout=30
    )
    response.raise_for_status()

    # JSON 응답 검증
    data = response.json()

    header = data.get("header", {})

    if header.get("resultCode") != "00":
        raise RuntimeError(
            f"API 오류: {header.get('resultMsg')}"
        )

    rows = data.get("body")

    if not isinstance(rows, list):
        raise RuntimeError("body 데이터 형식 오류")

    # 6. 원본 JSON 저장
    file_path = output_dir / f"page_{page:05d}.json"

    if file_path.exists():
        raise FileExistsError(
            f"이미 파일이 존재합니다: {file_path}"
        )

    file_path.write_bytes(response.content)

    # 7. 수집 기록 저장
    record = {
        "pageNo": page,
        "record_count": len(rows),
        "totalCount": data.get("totalCount"),
        "collected_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "bronze_file": str(file_path),
        "sha256": hashlib.sha256(
            response.content
        ).hexdigest(),
        "status": "downloaded"
    }

    with manifest_path.open(
        "a", encoding="utf-8"
    ) as f:
        f.write(
            json.dumps(record, ensure_ascii=False) + "\n"
        )

    print(f"저장 완료: {len(rows)}건")

    # 호출 간격
    time.sleep(1)

print("지정한 페이지 수집 완료!")
