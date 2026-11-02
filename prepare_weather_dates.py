
import json
from pathlib import Path
from datetime import datetime, timedelta

# 설정
LABEL_FOLDER = Path("data/bronze/labels/full_100")
OUTPUT = Path("config/weather_dates.json")

REGION_CODE = "48270"  # 밀양시
START_YEAR = 2019
END_YEAR = 2022
DAYS_BEFORE = 2

# 날짜별로 중복 제거
weather_dates = set()
events = set()
missing_date_count = 0

files = sorted(LABEL_FOLDER.glob("page_*.json"))

for file in files:
    data = json.loads(file.read_text(encoding="utf-8"))

    for row in data.get("body", []):
        region = str(row.get("STDG_SGG_CD", ""))
        year = str(row.get("FLDN_YR", ""))

        if region != REGION_CODE:
            continue

        if not year.isdigit():
            continue

        if not START_YEAR <= int(year) <= END_YEAR:
            continue

        start_str = row.get("FLDN_BGNG_YMD")
        end_str = row.get("FLDN_END_YMD")

        # 날짜가 없는 기록은 별도로 집계
        if not start_str or not end_str:
            missing_date_count += 1
            continue

        try:
            start = datetime.strptime(
                str(start_str), "%Y%m%d"
            ).date()
            end = datetime.strptime(
                str(end_str), "%Y%m%d"
            ).date()
        except ValueError:
            missing_date_count += 1
            continue

        if end < start:
            missing_date_count += 1
            continue

        event = (
            start.isoformat(),
            end.isoformat(),
            str(row.get("FLDN_DST_NM") or "")
        )
        events.add(event)

        # 침수 시작 2일 전부터 종료일까지
        current = start - timedelta(days=DAYS_BEFORE)

        while current <= end:
            weather_dates.add(current.isoformat())
            current += timedelta(days=1)

# 결과 정리
result = {
    "region_code": REGION_CODE,
    "region_name": "밀양시",
    "start_year": START_YEAR,
    "end_year": END_YEAR,
    "days_before": DAYS_BEFORE,
    "stations": ["288", "922", "927"],
    "dates": sorted(weather_dates),
    "events": [
        {
            "start": start,
            "end": end,
            "name": name
        }
        for start, end, name in sorted(events)
    ]
}

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
OUTPUT.write_text(
    json.dumps(result, ensure_ascii=False, indent=2),
    encoding="utf-8"
)

print("=== 밀양시 기상자료 수집 대상 ===")
print("분석 기간:", START_YEAR, "~", END_YEAR)
print("사건 날짜 조합:", len(events), "개")
print("수집 대상 날짜:", len(weather_dates), "일")
print("관측소:", ", ".join(result["stations"]))
print("날짜 결측 기록:", missing_date_count, "건")

print("\n=== 수집 대상 날짜 ===")
for day in sorted(weather_dates):
    print(day)

print("\n저장 완료:", OUTPUT)
