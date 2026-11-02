
import json
from pathlib import Path
from collections import Counter

folder = Path("data/bronze/labels/full_100")
files = sorted(folder.glob("page_*.json"))

years = Counter()
regions = Counter()
year_region = Counter()

for file in files:
    data = json.loads(file.read_text(encoding="utf-8"))

    for row in data.get("body", []):
        year = str(row.get("FLDN_YR", "UNKNOWN"))
        region = str(row.get("STDG_SGG_CD", "UNKNOWN"))

        years[year] += 1
        regions[region] += 1
        year_region[(year, region)] += 1

print("=== 연도별 침수 기록 ===")
for year, count in sorted(years.items()):
    print(year, count, "건")

print("\n=== 시군구 코드별 상위 15개 ===")
for code, count in regions.most_common(15):
    print(code, count, "건")

print("\n=== 2021~2025년 지역별 상위 15개 ===")
recent = [
    (year, code, count)
    for (year, code), count in year_region.items()
    if year in ["2021", "2022", "2023", "2024", "2025"]
]

for year, code, count in sorted(
    recent,
    key=lambda x: x[2],
    reverse=True
)[:15]:
    print(year, code, count, "건")

print("\n=== 서울특별시 연도별 침수 기록 ===")

seoul_years = Counter()

for (year, region), count in year_region.items():
    if region.startswith("11"):
        seoul_years[year] += count

for year, count in sorted(seoul_years.items()):
    print(year, count, "건")

print("\n=== 여러 연도에 침수 기록이 있는 지역 TOP 15 ===")

region_years = {}

for (year, region), count in year_region.items():
    if year.isdigit():
        if region not in region_years:
            region_years[region] = {}

        region_years[region][year] = count

results = []

for region, years in region_years.items():
    # 서로 다른 연도에 기록이 존재하는 횟수
    year_count = len(years)

    # 전체 침수 기록 수
    total_count = sum(years.values())

    if year_count >= 3:
        results.append(
            (region, year_count, total_count, years)
        )

results.sort(
    key=lambda x: (-x[1], -x[2])
)

for region, year_count, total, years in results[:15]:
    print(
        f"지역코드: {region} | "
        f"기록 연도: {year_count}개 | "
        f"총 기록: {total}건"
    )
    print("연도별:", dict(sorted(years.items())))
    print()

print("\n=== 밀양시 2019~2022년 침수 사건 ===")

events = set()

for file in files:
    data = json.loads(file.read_text(encoding="utf-8"))

    for row in data.get("body", []):
        year = str(row.get("FLDN_YR", ""))
        region = str(row.get("STDG_SGG_CD", ""))

        if region == "48270" and year in [
            "2019", "2020", "2021", "2022"
        ]:
            event = (
                year,
                row.get("FLDN_BGNG_YMD"),
                row.get("FLDN_END_YMD"),
                row.get("FLDN_DST_NM")
            )
            events.add(event)

print("서로 다른 사건 후보 수:", len(events))

for event in sorted(events, key=str):
    print(event)