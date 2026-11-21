
import csv
import json
import math
import re
from collections import Counter
from contextlib import ExitStack
from datetime import datetime, timedelta
from pathlib import Path

# 1. 파일 경로
RAW = Path("data/bronze/weather/all_hourly/raw")

SILVER = Path("data/silver/weather/station_hourly")
QC = Path("data/silver/quality_checks/nationwide_weather")

SILVER.mkdir(parents=True, exist_ok=True)
QC.mkdir(parents=True, exist_ok=True)

YEARS = (2024, 2025)

# 2. 필요한 변수와 정상값 범위
VARIABLES = {
    "TA": ("ta_c", -60, 60),
    "HM": ("hm_pct", 0, 100),
    "PS": ("ps_hpa", 700, 1100),
    "RN_HR1": ("rn_hr1_mm", 0, 1000),
}

FIELDS = [
    "station_id",
    "observation_time_kst",
    "ta_c",
    "hm_pct",
    "ps_hpa",
    "rn_hr1_mm",
    "ta_qc",
    "hm_qc",
    "ps_qc",
    "rn_hr1_qc",
    "source_file",
    "source_record_index",
]

def parse_value(value, minimum, maximum):
    # 결측과 이상값을 구분
    if value is None or str(value).strip() == "":
        return "", "missing"

    try:
        number = float(value)
    except (TypeError, ValueError):
        return "", "invalid"

    if not math.isfinite(number):
        return "", "invalid"

    # 기상청 결측 코드 및 허용 범위 확인
    if number in (-99, -99.9, -999, -9999):
        return "", "invalid"

    if not minimum <= number <= maximum:
        return "", "invalid"

    return number, "ok"


def extract_rows(obj):
    if isinstance(obj, list):
        if all(isinstance(row, dict) for row in obj):
            return obj
        raise ValueError("JSON 배열 형식 오류")

    if isinstance(obj, dict):
        for key in (
            "data", "items", "item", "rows",
            "records", "list", "result", "body", "response"
        ):
            if key in obj:
                try:
                    return extract_rows(obj[key])
                except ValueError:
                    pass

    raise ValueError("관측자료 배열을 찾을 수 없음")


def expected_hours(year):
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31, 23)

    result = set()
    current = start

    while current <= end:
        result.add(current.strftime("%Y%m%d%H%M"))
        current += timedelta(hours=1)

    return result


def main():
    if not RAW.exists():
        raise SystemExit(f"원본 폴더가 없습니다: {RAW}")

    files = sorted(RAW.rglob("aws_all_*.json"))

    if not files:
        raise SystemExit(
            "aws_all_YYYYMMDDHHMM.json 파일을 찾지 못했습니다."
        )

    observed_hours = {year: set() for year in YEARS}
    station_ids = {year: set() for year in YEARS}
    row_counts = Counter()
    quality_counts = Counter()

    bad_files = []
    mismatched_times = 0
    duplicate_station_hours = 0
    processed_files = 0

    with ExitStack() as stack:
        writers = {}

        for year in YEARS:
            path = SILVER / f"nationwide_station_hourly_{year}.csv"

            handle = stack.enter_context(
                path.open("w", newline="", encoding="utf-8-sig")
            )

            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writers[year] = writer

        for path in files:
            match = re.search(
                r"aws_all_(\d{12})\.json$", path.name
            )

            if not match:
                continue

            tm = match.group(1)
            year = int(tm[:4])

            if year not in YEARS:
                continue

            try:
                dt = datetime.strptime(tm, "%Y%m%d%H%M")
                if dt.minute != 0:
                    raise ValueError("정시 관측 파일이 아님")

                with path.open(encoding="utf-8-sig") as f:
                    payload = json.load(f)

                rows = extract_rows(payload)

            except (ValueError, OSError, UnicodeError) as exc:
                bad_files.append({
                    "file": str(path),
                    "error": str(exc),
                })
                continue

            observed_hours[year].add(tm)
            processed_files += 1

            seen_stations = set()

            for index, source in enumerate(rows):

                station = source.get("AWS_ID")

                if station is None or str(station).strip() == "":
                    quality_counts["missing_station_id"] += 1
                    continue

                station = str(station).strip()

                row_tm = str(source.get("TM", "")).strip()

                if row_tm != tm:
                    mismatched_times += 1
                    continue

                if station in seen_stations:
                    duplicate_station_hours += 1
                    continue

                seen_stations.add(station)

                record = {
                    "station_id": station,
                    "observation_time_kst": dt.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "source_file": path.as_posix(),
                    "source_record_index": index,
                }

                for original, (column, minimum, maximum) in VARIABLES.items():

                    value, status = parse_value(
                        source.get(original),
                        minimum,
                        maximum,
                    )

                    record[column] = value
                    record[column.replace("_c", "")
                                 .replace("_pct", "")
                                 .replace("_hpa", "")
                                 .replace("_mm", "") + "_qc"] = status

                    quality_counts[
                        f"{year}_{original}_{status}"
                    ] += 1

                writers[year].writerow(record)

                station_ids[year].add(station)
                row_counts[year] += 1

            if processed_files % 500 == 0:
                print(
                    f"처리 완료: {processed_files:,}개 시간 파일"
                )

    report = {
        "source_path": str(RAW),
        "years": {},
        "mismatched_record_times": mismatched_times,
        "duplicate_station_hours": duplicate_station_hours,
        "bad_file_count": len(bad_files),
        "bad_file_samples": bad_files[:20],
        "variable_quality_counts": dict(quality_counts),
    }

    for year in YEARS:
        expected = expected_hours(year)
        missing = sorted(expected - observed_hours[year])

        report["years"][str(year)] = {
            "expected_hours": len(expected),
            "observed_hours": len(observed_hours[year]),
            "missing_hour_count": len(missing),
            "missing_hour_samples": missing[:30],
            "station_count": len(station_ids[year]),
            "silver_record_count": row_counts[year],
        }

    report_path = QC / "silver_quality_report.json"

    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("\n===== Silver 표준화 결과 =====")

    for year, result in report["years"].items():
        print(f"\n{year}년")
        print("예상 시간:", result["expected_hours"])
        print("관측 시간:", result["observed_hours"])
        print("누락 시간:", result["missing_hour_count"])
        print("관측소 수:", result["station_count"])
        print("Silver 행 수:", result["silver_record_count"])

    print("\n파싱 오류 파일:", len(bad_files))
    print("관측시각 불일치:", mismatched_times)
    print("중복 관측소·시각:", duplicate_station_hours)
    print("품질 보고서:", report_path)


if __name__ == "__main__":
    main()
