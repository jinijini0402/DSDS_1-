
import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

# ===== 1. 경로 설정 =====

LABEL_DIR = Path("data/bronze/labels/full_100")
WEATHER_DIR = Path("data/bronze/weather/daily")
OLD_WEATHER = Path("data/bronze/weather/aws_288_20220906.json")

OUTPUT_DIR = Path("data/bronze/manifests")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LABEL_ENDPOINT = (
    "https://www.safetydata.go.kr/V2/api/DSSP-IF-00117"
)
WEATHER_ENDPOINT = (
    "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min"
)

# ===== 2. 파일 무결성 확인값 =====

def sha256_file(path):
    h = hashlib.sha256()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)

    return h.hexdigest()


# ===== 3. manifest 공통 정보 =====

def make_entry(path, dataset, provider, endpoint, count, **extra):
    return {
        "source_dataset": dataset,
        "provider": provider,
        "endpoint": endpoint,
        "request_method": "GET",
        "request_params_redacted": extra.get("request_params"),
        "source_schema_version": None,
        "requested_period_start": extra.get("period_start"),
        "requested_period_end": extra.get("period_end"),
        # 기존 파일에는 실제 API 요청시각이 저장되지 않았으므로
        # 파일 수정시각으로 대체하지 않는다.
        "collected_at": None,
        "bronze_file": path.as_posix(),
        "sha256": sha256_file(path),
        "record_count": count,
        "collection_status": extra.get("status", "not_validated"),
        "error_code": extra.get("error_code"),
        "source_temporal_resolution": extra.get("temporal"),
        "source_spatial_resolution": extra.get("spatial"),
        "license_url": None,
        "artifact_type": extra.get("artifact_type"),
        "manifest_generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }


entries = []

# ===== 4. 침수흔적도 파일 =====

for path in sorted(LABEL_DIR.glob("page_*.json")):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        rows = data.get("body", [])

        if not isinstance(rows, list):
            raise ValueError("body가 리스트가 아님")

        count = len(rows)
        status = "parsed"

    except (ValueError, OSError, TypeError):
        count = 0
        status = "parse_error"

    match = re.search(r"page_(\d+)", path.stem)
    page_number = int(match.group(1)) if match else None

    entries.append(make_entry(
        path,
        "flood_trace",
        "행정안전부",
        LABEL_ENDPOINT,
        count,
        request_params={"page": page_number},
        temporal="year/event/date",
        spatial="flood_area/region_code",
        artifact_type="api_response_json",
        status=status,
    ))


# ===== 5. AWS 기상자료 파일 =====

weather_files = list(sorted(WEATHER_DIR.glob("aws_*.json")))

# 기존에 별도로 병합해 둔 파일 처리
if OLD_WEATHER.exists():
    duplicate = WEATHER_DIR / OLD_WEATHER.name

    if not duplicate.exists():
        weather_files.append(OLD_WEATHER)

for path in weather_files:
    match = re.search(r"aws_(\d+)_(\d{8})", path.stem)

    if not match:
        continue

    station, day = match.groups()

    try:
        rows = json.loads(path.read_text(encoding="utf-8"))

        if not isinstance(rows, list):
            raise ValueError("최상위 구조가 리스트가 아님")

        count = len(rows)
        status = "parsed"

    except (ValueError, OSError, TypeError):
        count = 0
        status = "parse_error"

    entries.append(make_entry(
        path,
        "kma_aws_minute",
        "기상청",
        WEATHER_ENDPOINT,
        count,
        request_params={
            "stn": station,
            "date": day,
            "disp": "1",
            "help": "1",
        },
        period_start=f"{day}0000",
        period_end=f"{day}2359",
        temporal="1 minute",
        spatial="AWS station",
        artifact_type="merged_source_records_json",
        status=status,
    ))


# ===== 6. JSONL 저장 =====

jsonl_path = OUTPUT_DIR / "bronze_manifest.jsonl"

with jsonl_path.open("w", encoding="utf-8") as f:
    for entry in entries:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


# ===== 7. CSV 저장 =====

csv_path = OUTPUT_DIR / "bronze_manifest.csv"

if entries:
    fieldnames = list(entries[0].keys())

    with csv_path.open(
        "w", newline="", encoding="utf-8-sig"
    ) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for entry in entries:
            row = dict(entry)
            row["request_params_redacted"] = json.dumps(
                row["request_params_redacted"],
                ensure_ascii=False
            )
            writer.writerow(row)


# ===== 8. 결과 요약 =====

labels = [e for e in entries if e["source_dataset"] == "flood_trace"]
weather = [e for e in entries if e["source_dataset"] == "kma_aws_minute"]

print("\n=== Bronze manifest 생성 결과 ===")
print("침수흔적도 파일:", len(labels))
print("침수흔적도 레코드:", sum(e["record_count"] for e in labels))
print("AWS 파일:", len(weather))
print("AWS 레코드:", sum(e["record_count"] for e in weather))
print("파싱 오류 파일:", sum(
    e["collection_status"] == "parse_error"
    for e in entries
))

print("\n저장 위치:")
print(jsonl_path)
print(csv_path)
