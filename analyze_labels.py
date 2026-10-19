
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
