import os
import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("KMA_API_KEY")
if not api_key:
    raise RuntimeError("KMA_API_KEY가 없습니다.")

url = "https://apihub.kma.go.kr/api/typ01/url/awsh.php"

params = {
    "tm": "202209061200",
    "stn": "0",
    "disp": "1",
    "authKey": api_key
}

try:
    response = requests.get(url, params=params, timeout=60)
    print("HTTP 상태:", response.status_code)

    if response.status_code == 200:
        try:
            data = response.json()
        except ValueError:
            print("JSON이 아닌 응답입니다.")
            print(response.text[:300])
            raise SystemExit(1)

        if not isinstance(data, list) or not data:
            print("관측 데이터가 없습니다.")
            raise SystemExit(1)

        print("전체 관측소 레코드 수:", len(data))
        print("응답 컬럼:", list(data[0].keys()))

        # 서울 관측소 예시
        seoul_ids = {"108", "400", "401", "402", "403"}

        print("\n=== 서울 관측소 샘플 ===")
        for row in data:
            if str(row.get("AWS_ID")) in seoul_ids:
                print({
                    "AWS_ID": row.get("AWS_ID"),
                    "TA": row.get("TA"),
                    "PS": row.get("PS"),
                    "HM": row.get("HM"),
                    "RN_HR1": row.get("RN_HR1")
                })
    else:
        print("요청 실패:", response.status_code)
        # 인증키가 포함될 수 있는 URL은 출력하지 않음

except requests.RequestException as e:
    print("요청 오류 유형:", type(e).__name__)
