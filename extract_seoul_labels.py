
import json
from pathlib import Path
from collections import Counter

INPUT_DIR = Path("data/bronze/labels/full_100")
OUTPUT_DIR = Path("data/bronze/labels/seoul_2021_2024")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

selected = []
year_counts = Counter()

for file in sorted(INPUT_DIR.glob("page_*.json")):
    data = json.loads(file.read_text(encoding="utf-8"))

    for index, row in enumerate(data.get("body", [])):
        year = str(row.get("FLDN_YR", "")).strip()
        region = str(row.get("STDG_SGG_CD", "")).strip()

        # 서울특별시 시군구 코드: 11로 시작
        is_seoul = region.startswith("11")
        is_target_year = year in {"2021", "2022", "2023", "2024"}

        if is_seoul and is_target_year:
            selected.append({
                "source_file": file.as_posix(),
                "source_index": index,
                "record": row
            })
            year_counts[year] += 1

output_file = OUTPUT_DIR / "seoul_flood_2021_2024.json"

output_file.write_text(
    json.dumps(selected, ensure_ascii=False, indent=2),
    encoding="utf-8"
)

print("=== 서울시 2021~2024 침수 기록 ===")

for year in range(2021, 2025):
    print(f"{year}: {year_counts[str(year)]:,}건")

print(f"합계: {len(selected):,}건")
print(f"저장 위치: {output_file}")
