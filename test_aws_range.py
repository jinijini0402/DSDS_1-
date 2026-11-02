
import os
import urllib.parse
import urllib.request
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("KMA_API_KEY")

if not api_key:
    raise ValueError(".env에 KMA_API_KEY가 없습니다.")

# AWS 매분자료 기간 조회 API
url = "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min"

params = {
    "tm1": "202209061741",
    "tm2": "202209062359",
    "stn": "288",
    "disp": "1",
    "help": "1",
    "authKey": api_key,
}

request_url = url + "?" + urllib.parse.urlencode(params)

# API 호출: 정확히 1번
with urllib.request.urlopen(request_url, timeout=60) as response:
    raw = response.read()

try:
    result = raw.decode("cp949")
except UnicodeDecodeError:
    result = raw.decode("utf-8", errors="replace")

# 응답 파일 저장
folder = Path("data/bronze/weather/tests")
folder.mkdir(parents=True, exist_ok=True)

output = folder / "aws_range_288_20220906_part2.txt"
output.write_text(result, encoding="utf-8")

# 응답 일부만 출력
lines = result.splitlines()

print("=== API 조회 결과 ===")
print("전체 줄 수:", len(lines))
print("응답 크기:", len(raw), "bytes")

print("\n=== 앞부분 15줄 ===")
for line in lines[:15]:
    print(line)

print("\n저장 위치:", output)
