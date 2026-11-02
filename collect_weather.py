
import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("KMA_API_KEY")
if not API_KEY:
    raise ValueError(".env에 KMA_API_KEY가 없습니다.")

URL = "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min"
CONFIG = Path("config/weather_dates.json")
OUTPUT = Path("data/bronze/weather/daily")
OUTPUT.mkdir(parents=True, exist_ok=True)

parser = argparse.ArgumentParser()
parser.add_argument("--max-calls", type=int, default=2)
args = parser.parse_args()

if args.max_calls < 1:
    raise ValueError("--max-calls는 1 이상이어야 합니다.")

config = json.loads(CONFIG.read_text(encoding="utf-8"))
calls = 0

def read_records(path, station, day):
    if not path.exists():
        return {}

    data = json.loads(path.read_text(encoding="utf-8"))
    records = {}

    for row in data:
        if not isinstance(row, list) or len(row) < 3:
            continue

        tm = str(row[0]).strip()
        stn = str(row[1]).strip()

        if tm.startswith(day) and stn == station:
            records[tm] = row

    return records


def save_records(path, records):
    rows = [records[tm] for tm in sorted(records)]
    path.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def request_range(station, start, end):
    params = {
        "tm1": start,
        "tm2": end,
        "stn": station,
        "disp": "1",
        "help": "1",
        "authKey": API_KEY,
    }

    url = URL + "?" + urllib.parse.urlencode(params)

    with urllib.request.urlopen(url, timeout=90) as response:
        raw = response.read()

    try:
        text = raw.decode("cp949")
    except UnicodeDecodeError:
        text = raw.decode("utf-8")

    result = {}

    for line in text.splitlines():
        parts = line.strip().split(",")

        if len(parts) < 3:
            continue

        tm = parts[0].strip()
        stn = parts[1].strip()

        if len(tm) == 12 and tm.isdigit() and stn == station:
            result[tm] = parts

    return result


for station in config["stations"]:
    for date_str in config["dates"]:
        day = date_str.replace("-", "")
        output = OUTPUT / f"aws_{station}_{day}.json"

        records = read_records(output, station, day)

        # 기존에 병합한 밀양 2022-09-06 자료 재사용
        if station == "288" and day == "20220906":
            old = Path("data/bronze/weather/aws_288_20220906.json")
            records.update(read_records(old, station, day))

        start_dt = datetime.strptime(day, "%Y%m%d")
        expected = [
            (start_dt + timedelta(minutes=i)).strftime("%Y%m%d%H%M")
            for i in range(1440)
        ]

        if all(tm in records for tm in expected):
            if not output.exists():
                save_records(output, records)
            print(f"[완료] {station} / {day} / 1440건")
            continue

        # 요청은 최대 12시간(720분) 단위
        while calls < args.max_calls:
            missing = [tm for tm in expected if tm not in records]

            if not missing:
                break

            first = missing[0]
            first_idx = expected.index(first)
            last_idx = min(first_idx + 719, 1439)

            # 이미 수집한 구간은 재요청하지 않음
            while last_idx > first_idx and expected[last_idx] in records:
                last_idx -= 1

            last = expected[last_idx]

            print(f"[요청] {station} / {first} ~ {last}")

            try:
                new_records = request_range(station, first, last)
            except Exception as e:
                calls += 1
                print(f"  요청 실패: {type(e).__name__}: {e}")
                break

            calls += 1
            before = len(records)
            records.update(new_records)

            # 요청할 때마다 저장하므로 중단 후 이어서 가능
            save_records(output, records)

            added = len(records) - before
            print(f"  신규 {added}건 / 누적 {len(records)}건")

            if added == 0:
                print("  새 데이터가 없어 해당 날짜를 중단합니다.")
                break

        if calls >= args.max_calls:
            break

    if calls >= args.max_calls:
        break

print(f"\n이번 실행의 API 호출: {calls}회")
print("수집 파일 위치:", OUTPUT)
