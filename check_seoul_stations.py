
import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("KMA_API_KEY")
if not API_KEY:
    raise RuntimeError("KMA_API_KEY가 없습니다.")

URL = "https://apihub.kma.go.kr/api/typ01/url/awsh.php"

# 기존 관측소 목록에서 확인한 서울 소재 29곳
SEOUL_IDS = {
    "108", "110",
    *[str(i) for i in range(400, 420)],
    "421", "423", "424", "425",
    "509", "510", "889"
}

# 각 연도에서 한 시각씩 테스트
TEST_TIMES = [
    "202101011200",
    "202201011200",
    "202301011200",
    "202401011200",
]

VARIABLES = ["TA", "PS", "HM", "RN_HR1"]

for tm in TEST_TIMES:
    print(f"\n=== {tm} ===")

    try:
        response = requests.get(
            URL,
            params={
                "tm": tm,
                "stn": "0",
                "disp": "1",
                "authKey": API_KEY
            },
            timeout=60
        )

        response.raise_for_status()
        rows = response.json()

        if not isinstance(rows, list):
            print("예상과 다른 응답 구조")
            continue

        seoul = [
            row for row in rows
            if str(row.get("AWS_ID")) in SEOUL_IDS
        ]

        print("서울 관측소 레코드:", len(seoul))

        for variable in VARIABLES:
            valid = sum(
                1 for row in seoul
                if row.get(variable) not in
                (None, "", "-99", "-99.0")
            )
            print(f"{variable}: {valid}/{len(seoul)}건 값 존재")

    except (requests.RequestException, ValueError) as e:
        print("오류 유형:", type(e).__name__)
        # 인증키가 담길 수 있는 요청 URL은 출력하지 않음
