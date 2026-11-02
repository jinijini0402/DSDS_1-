import os
import urllib.parse
import urllib.request
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("KMA_API_KEY")

if not api_key:
    raise ValueError(".env 파일에 KMA_API_KEY가 없습니다.")

url = "https://apihub.kma.go.kr/api/typ01/url/awsh.php"

dates = [
    "201910021200",
    "202009031200",
    "202108241200",
    "202209061200",
]

stations = ["288", "922", "927"]

folder = Path("data/bronze/weather/tests")
folder.mkdir(parents=True, exist_ok=True)

for station in stations:
    for tm in dates:
        params = {
            "var": "RN",
            "tm": tm,
            "stn": station,
            "disp": "1",
            "authKey": api_key,
        }

        request_url = url + "?" + urllib.parse.urlencode(params)

        print(f"\n=== 관측소 {station} / {tm} ===")

        try:
            with urllib.request.urlopen(
                request_url, timeout=30
            ) as response:
                raw = response.read()

            try:
                result = raw.decode("cp949")
            except UnicodeDecodeError:
                result = raw.decode("utf-8")

            output = folder / f"rain_{station}_{tm}.txt"
            output.write_text(result, encoding="utf-8")

            print(result[:1500])
            print(f"저장 완료: {output}")

        except Exception as e:
            print(f"요청 실패: {type(e).__name__}: {e}")