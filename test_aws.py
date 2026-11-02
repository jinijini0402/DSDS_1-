import os
import urllib.parse
import urllib.request

from dotenv import load_dotenv

# 1. API 인증키 불러오기
load_dotenv()
api_key = os.getenv("KMA_API_KEY")

if not api_key:
    raise ValueError(".env에 KMA_API_KEY가 없습니다.")

# 2. AWS 관측소 목록 조회
url = "https://apihub.kma.go.kr/api/typ01/url/stn_inf.php"

params = {
    "inf": "AWS",
    "stn": "",
    "tm": "202209061200",
    "help": "1",
    "authKey": api_key,
}

request_url = url + "?" + urllib.parse.urlencode(params)

with urllib.request.urlopen(request_url, timeout=30) as response:
    text = response.read().decode("cp949")

# 3. 응답 저장
os.makedirs("data/bronze/weather", exist_ok=True)

with open(
    "data/bronze/weather/aws_stations_2022.txt",
    "w",
    encoding="utf-8"
) as f:
    f.write(text)

# 4. 밀양시 및 주변 지역 검색
keywords = ["밀양", "삼랑진", "창녕", "청도", "김해", "양산"]

print("=== AWS 관측소 검색 결과 ===")

for line in text.splitlines():
    if any(keyword in line for keyword in keywords):
        print(line)

print("\n전체 관측소 목록 저장 완료")