
import csv
import math
import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

import repair_nationwide_aws_coordinates as mapping

load_dotenv(dotenv_path=".env")

ROOT = Path("data/silver/mappings/nationwide_aws_dong")
AUDIT = ROOT / "unresolved_station_observation_audit.csv"
CACHE = ROOT / "recovered_missing_station_coordinates.csv"

API = "https://apihub.kma.go.kr/api/typ01/url/stn_inf.php"
KEY = os.getenv("KMA_API_KEY")

FIELDS = [
    "station_id", "longitude", "latitude",
    "year", "month", "snapshot", "station_type"
]


def load_cached():
    if not CACHE.exists():
        return pd.DataFrame(columns=FIELDS)

    return pd.read_csv(
        CACHE, dtype={"station_id": str}
    )


def save_coordinate(record):
    exists = CACHE.exists()

    with CACHE.open(
        "a", newline="", encoding="utf-8-sig"
    ) as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)

        if not exists:
            writer.writeheader()

        writer.writerow(record)


def query(session, station_id, timestamp):
    tm = timestamp.strftime("%Y%m%d%H%M")

    for kind in ("AWS", "SFC"):
        params = {
            "inf": kind,
            "stn": station_id,
            "tm": tm,
            "help": "0",
            "authKey": KEY,
        }

        for attempt in range(2):
            try:
                response = session.get(
                    API,
                    params=params,
                    timeout=(8, 30),
                )

                if response.status_code in (401, 403, 429):
                    raise RuntimeError(
                        f"API 접근 오류: HTTP {response.status_code}"
                    )

                if response.status_code >= 500:
                    time.sleep(2 * (attempt + 1))
                    continue

                response.raise_for_status()

                for line in response.text.splitlines():
                    line = line.strip()

                    if not line or line.startswith("#"):
                        continue

                    parts = line.split()

                    if len(parts) < 3:
                        continue

                    if parts[0] != station_id:
                        continue

                    try:
                        lon = float(parts[1])
                        lat = float(parts[2])
                    except ValueError:
                        continue

                    if 124 <= lon <= 132 and 32 <= lat <= 40:
                        return {
                            "station_id": station_id,
                            "longitude": lon,
                            "latitude": lat,
                            "year": timestamp.year,
                            "month": timestamp.month,
                            "snapshot": tm,
                            "station_type": kind,
                        }

                break

            except requests.RequestException as exc:
                print(
                    f"  요청 오류: {type(exc).__name__}",
                    flush=True,
                )
                time.sleep(2 * (attempt + 1))

    return None


def distance_km(a, b):
    lon1, lat1 = map(
        math.radians,
        (a["longitude"], a["latitude"])
    )
    lon2, lat2 = map(
        math.radians,
        (b["longitude"], b["latitude"])
    )

    x = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1)
        * math.cos(lat2)
        * math.sin((lon2 - lon1) / 2) ** 2
    )

    return 12742 * math.asin(min(1, math.sqrt(x)))


def main():
    if not KEY:
        raise SystemExit(".env의 KMA_API_KEY를 확인하세요.")

    if not AUDIT.exists():
        raise FileNotFoundError(AUDIT)

    audit = pd.read_csv(
        AUDIT,
        dtype={"station_id": str},
    )

    cached = load_cached()
    completed = {
        (str(r.station_id), int(r.year))
        for r in cached.itertuples(index=False)
    }

    unresolved = []
    changed = []

    with requests.Session() as session:
        for row in audit.itertuples(index=False):
            year = int(row.year)
            station_id = str(row.station_id)

            if (station_id, year) in completed:
                print(
                    f"[기존 좌표] {year} / {station_id}",
                    flush=True,
                )
                continue

            first = pd.to_datetime(row.first_time)
            last = pd.to_datetime(row.last_time)

            if pd.isna(first) or pd.isna(last):
                unresolved.append((year, station_id))
                continue

            print(
                f"\n[조회] {year}년 관측소 {station_id}",
                flush=True,
            )
            print("  첫 관측:", first)
            print("  마지막 관측:", last)

            snapshots = list(dict.fromkeys([first, last]))
            found = []

            for timestamp in snapshots:
                record = query(
                    session,
                    station_id,
                    timestamp,
                )

                if record:
                    found.append(record)
                    print(
                        f"  좌표 발견: "
                        f"{record['longitude']}, "
                        f"{record['latitude']}",
                        flush=True,
                    )
                else:
                    print(
                        f"  {timestamp}: 좌표 미확보",
                        flush=True,
                    )

                time.sleep(0.3)

            if not found:
                unresolved.append((year, station_id))
                continue

            if len(found) >= 2:
                moved_km = distance_km(
                    found[0], found[-1]
                )

                if moved_km > 0.5:
                    print(
                        f"  주의: 좌표 차이 {moved_km:.2f}km"
                    )
                    changed.append((year, station_id))
                    continue

            if len(found) == 1:
                print("  주의: 한 시점에서만 좌표 확인")

            save_coordinate(found[-1])
            completed.add((station_id, year))

    print("\n===== 조회 결과 =====")
    print("좌표 미확보:", unresolved)
    print("위치변경 검토 필요:", changed)

    metadata = load_cached()
    metadata["year"] = metadata["year"].astype(int)

    observed = mapping.read_observed_stations()
    old_coords, _ = mapping.read_old_data()
    boundary = mapping.read_boundaries()

    for year in (2024, 2025):
        result = mapping.make_yearly_mapping(
            year,
            observed[year],
            metadata,
            old_coords,
            boundary,
        )

        print(f"\n===== {year}년 최종 =====")
        print(
            "좌표 확보:",
            result["matched"],
            "/",
            result["observed"],
        )
        print(
            "좌표 누락:",
            result["unresolved_ids"],
        )

    print("\n완료: 연도별 공간 정합 파일 재생성")


if __name__ == "__main__":
    main()
