
import json
from pathlib import Path

folder = Path("data/bronze/weather/tests")

files = [
    folder / "aws_range_288_20220906.txt",
    folder / "aws_range_288_20220906_part2.txt",
]

records = {}

for file in files:
    for line in file.read_text(encoding="utf-8").splitlines():
        if not line.startswith("20220906"):
            continue

        values = line.strip().split(",")

        if len(values) < 3:
            continue

        tm = values[0]
        station = values[1]

        if station != "288":
            continue

        records[tm] = values

output = Path("data/bronze/weather/aws_288_20220906.json")
output.parent.mkdir(parents=True, exist_ok=True)

output.write_text(
    json.dumps(
        [records[tm] for tm in sorted(records)],
        ensure_ascii=False,
        indent=2
    ),
    encoding="utf-8"
)

print("병합 완료:", output)
print("고유 관측 시각:", len(records))

if records:
    print("첫 시각:", min(records))
    print("마지막 시각:", max(records))
else:
    print("병합할 데이터가 없습니다.")
