
import os
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

# 1. 서비스키 불러오기
load_dotenv()

api_key = os.getenv("SAFETY_API_KEY")

if not api_key:
    raise ValueError("API 키가 없습니다.")

# 2. 기존 설정 불러오기
config = json.loads(
    Path("config/labels.json").read_text(encoding="utf-8")
)

# 3. 100건 요청 설정
params = dict(config["params"])
params["pageNo"] = 1
params["numOfRows"] = 100
params[config["key_param"]] = api_key

# 4. API 호출
response = requests.get(
    config["endpoint"],
    params=params,
    timeout=60
)

response.raise_for_status()
data = response.json()

# 5. 결과 확인
header = data.get("header", {})
body = data.get("body")

print("API 상태:", header.get("resultCode"))
print("메시지:", header.get("resultMsg"))
print("요청 건수:", 100)
print("응답 numOfRows:", data.get("numOfRows"))
print("실제 반환 건수:",
      len(body) if isinstance(body, list) else 0)
print("전체 데이터 건수:", data.get("totalCount"))

if (
    header.get("resultCode") == "00"
    and isinstance(body, list)
    and len(body) == 100
):
    print("성공! 한 페이지에 100건을 가져올 수 있어.")

    # 테스트 결과 원본 보존
    output = Path("data/bronze/labels/test_100.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(response.content)

    print("원본 저장:", output)
else:
    print("100건 요청이 정상적으로 처리되지 않았어.")
