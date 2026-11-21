
import csv
import json
import math
import os
import re
from collections import defaultdict
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

# =====================================
# 1. 경로 설정
# =====================================

ROOT = Path("data")

SILVER = ROOT / "silver/weather/station_hourly"

BOUNDARY = (
    ROOT / "bronze/boundaries/"
    "BND_ADM_DONG_PG/BND_ADM_DONG_PG.shp"
)

OUT = (
    ROOT / "silver/mappings/nationwide_aws_dong"
)
OUT.mkdir(parents=True, exist_ok=True)

OLD_LOCATION = (
    OUT / "station_admin_location_2025_snapshot.csv"
)

OLD_REPORT = OUT / "mapping_quality_report.json"

API_URL = (
    "https://apihub.kma.go.kr/"
    "api/typ01/url/stn_inf.php"
)

API_KEY = os.getenv("KMA_API_KEY")

YEARS = [2024, 2025]
MONTHS = [1, 4, 7, 10, 12]
TOP_K = 30

# =====================================
# 2. 지점번호 정규화
# =====================================

def normalize_id(value):
    value = str(value).strip()

    if re.fullmatch(r"\d+(?:\.0+)?", value):
        return str(int(float(value)))

    return value


# =====================================
# 3. 실제 관측된 관측소 목록
# =====================================

def read_observed_stations():
    result = {}

    for year in YEARS:
        file = (
            SILVER /
            f"nationwide_station_hourly_{year}.csv"
        )

        if not file.exists():
            raise FileNotFoundError(file)

        ids = set()

        for chunk in pd.read_csv(
            file,
            usecols=["station_id"],
            dtype={"station_id": "string"},
            chunksize=200000,
        ):
            ids.update(
                normalize_id(x)
                for x in chunk["station_id"].dropna().unique()
            )

        result[year] = ids
        print(f"{year}년 관측소: {len(ids)}개", flush=True)

    return result


# =====================================
# 4. 기존 좌표 및 누락 목록 읽기
# =====================================

def read_old_data():
    old = pd.read_csv(
        OLD_LOCATION,
        dtype={"station_id": "string"},
    )

    old["station_id"] = old["station_id"].map(
        normalize_id
    )

    old = old.dropna(
        subset=["longitude", "latitude"]
    )

    old_coords = {}

    for row in old.itertuples(index=False):
        old_coords[row.station_id] = (
            float(row.longitude),
            float(row.latitude),
        )

    report = json.loads(
        OLD_REPORT.read_text(encoding="utf-8")
    )

    missing = {
        normalize_id(x)
        for x in report.get(
            "unmatched_station_ids", []
        )
    }

    return old_coords, missing


# =====================================
# 5. 공식 기상청 지점정보 조회
# =====================================

def fetch_station_metadata(kind, year, month):
    tm = f"{year}{month:02d}151200"

    params = {
        "inf": kind,
        "stn": "0",
        "tm": tm,
        "help": "0",
        "authKey": API_KEY,
    }

    try:
        response = requests.get(
            API_URL,
            params=params,
            timeout=45,
        )
    except requests.RequestException:
        raise SystemExit(
            "기상청 지점정보 API 연결에 실패했습니다."
        )

    # 인증키가 포함된 요청 URL은 출력하지 않음
    if response.status_code != 200:
        raise SystemExit(
            f"기상청 API HTTP 오류: "
            f"{response.status_code}\n"
            "KMA_API_KEY와 API 승인 상태를 확인하세요."
        )

    records = []

    for line in response.text.splitlines():
        line = line.strip()

        if not line or line.startswith("#"):
            continue

        parts = line.split()

        # 공식 문서: STN_ID LON LAT ...
        if len(parts) < 3:
            continue

        if not re.fullmatch(r"\d+", parts[0]):
            continue

        try:
            lon = float(parts[1])
            lat = float(parts[2])
        except ValueError:
            continue

        if not (
            124 <= lon <= 132
            and 32 <= lat <= 40
        ):
            continue

        records.append({
            "station_id": normalize_id(parts[0]),
            "longitude": lon,
            "latitude": lat,
            "year": year,
            "month": month,
            "snapshot": tm,
            "station_type": kind,
        })

    print(
        f"  {kind} {year}-{month:02d}: "
        f"{len(records)}개",
        flush=True,
    )

    return records


# =====================================
# 6. 거리 계산 보조 함수
# =====================================

def haversine_km(a, b):
    lon1, lat1 = map(math.radians, a)
    lon2, lat2 = map(math.radians, b)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    h = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1)
        * math.cos(lat2)
        * math.sin(dlon / 2) ** 2
    )

    return 6371 * 2 * math.asin(
        min(1, math.sqrt(h))
    )


# =====================================
# 7. 행정동 경계 읽기
# =====================================

def read_boundaries():
    boundary = gpd.read_file(BOUNDARY)

    boundary = boundary.rename(columns={
        "ADM_CD": "ADM_DR_CD",
        "ADM_NM": "ADM_DR_NM",
    })

    required = {
        "ADM_DR_CD",
        "ADM_DR_NM",
    }

    if not required.issubset(boundary.columns):
        raise ValueError(
            f"행정동 컬럼 오류: {list(boundary.columns)}"
        )

    if boundary.crs is None:
        raise ValueError("행정동 좌표계 누락")

    boundary = boundary[
        boundary.geometry.notna()
        & ~boundary.geometry.is_empty
    ].copy()

    boundary["ADM_DR_CD"] = (
        boundary["ADM_DR_CD"].astype(str)
    )

    if boundary["ADM_DR_CD"].duplicated().any():
        boundary = boundary.dissolve(
            by="ADM_DR_CD",
            aggfunc={"ADM_DR_NM": "first"},
            as_index=False,
        )

    return boundary.to_crs("EPSG:5179")


# =====================================
# 8. 연도별 좌표표와 거리표 생성
# =====================================

def make_yearly_mapping(
    year,
    observed_ids,
    metadata,
    old_coords,
    boundary,
):
    year_meta = metadata[
        metadata["year"] == year
    ].sort_values("snapshot")

    # 같은 연도에서 가장 최근에 확인된 좌표 선택
    latest = year_meta.drop_duplicates(
        subset=["station_id"],
        keep="last",
    )

    current = {
        row.station_id: row
        for row in latest.itertuples(index=False)
    }

    records = []
    unresolved = []

    for station_id in sorted(observed_ids):
        if station_id in current:
            row = current[station_id]

            records.append({
                "station_id": station_id,
                "longitude": row.longitude,
                "latitude": row.latitude,
                "coordinate_source": "KMA_API",
                "metadata_snapshot": row.snapshot,
            })

        elif station_id in old_coords:
            lon, lat = old_coords[station_id]

            records.append({
                "station_id": station_id,
                "longitude": lon,
                "latitude": lat,
                "coordinate_source": "fallback_2022",
                "metadata_snapshot": "2022",
            })

        else:
            unresolved.append(station_id)

    coordinates = pd.DataFrame(records)

    coord_file = (
        OUT / f"station_coordinates_{year}.csv"
    )

    coordinates.to_csv(
        coord_file,
        index=False,
        encoding="utf-8-sig",
    )

    # 관측소를 공간 객체로 변환
    stations = gpd.GeoDataFrame(
        coordinates,
        geometry=gpd.points_from_xy(
            coordinates["longitude"],
            coordinates["latitude"],
        ),
        crs="EPSG:4326",
    ).to_crs("EPSG:5179")

    locations = gpd.sjoin(
        stations,
        boundary[
            ["ADM_DR_CD", "ADM_DR_NM", "geometry"]
        ],
        how="left",
        predicate="intersects",
    )

    locations = locations.drop_duplicates(
        subset=["station_id"]
    )

    locations = locations.rename(columns={
        "ADM_DR_CD": "station_admin_code",
        "ADM_DR_NM": "station_admin_name",
    })

    location_cols = [
        "station_id",
        "longitude",
        "latitude",
        "station_admin_code",
        "station_admin_name",
        "coordinate_source",
    ]

    locations[location_cols].to_csv(
        OUT / f"station_admin_location_{year}.csv",
        index=False,
        encoding="utf-8-sig",
    )

    loc_dict = locations.set_index(
        "station_id"
    )[[
        "station_admin_code",
        "station_admin_name",
    ]].to_dict("index")

    # 행정동별 가까운 관측소 계산
    sx = stations.geometry.x.to_numpy()
    sy = stations.geometry.y.to_numpy()

    station_rows = stations.reset_index(drop=True)

    distance_file = (
        OUT / f"dong_station_distance_{year}.csv"
    )

    fields = [
        "year",
        "admin_dong_code",
        "admin_dong_name",
        "station_id",
        "distance_rank",
        "distance_m",
        "station_admin_code",
        "station_admin_name",
        "coordinate_source",
        "metadata_snapshot",
        "boundary_snapshot",
    ]

    with distance_file.open(
        "w", newline="", encoding="utf-8-sig"
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=fields
        )
        writer.writeheader()

        for _, dong in boundary.iterrows():
            point = (
                dong.geometry.representative_point()
            )

            distances = np.hypot(
                sx - point.x,
                sy - point.y,
            )

            indices = np.argsort(distances)[
                :min(TOP_K, len(stations))
            ]

            for rank, index in enumerate(indices, 1):
                station = station_rows.iloc[int(index)]
                sid = station["station_id"]
                loc = loc_dict.get(sid, {})

                writer.writerow({
                    "year": year,
                    "admin_dong_code":
                        dong["ADM_DR_CD"],
                    "admin_dong_name":
                        dong["ADM_DR_NM"],
                    "station_id": sid,
                    "distance_rank": rank,
                    "distance_m":
                        round(float(distances[index]), 2),
                    "station_admin_code":
                        loc.get("station_admin_code"),
                    "station_admin_name":
                        loc.get("station_admin_name"),
                    "coordinate_source":
                        station["coordinate_source"],
                    "metadata_snapshot":
                        station["metadata_snapshot"],
                    "boundary_snapshot": "2025-06-30",
                })

    print(f"\n[{year}년 보완 결과]")
    print("대상 관측소:", len(observed_ids))
    print("좌표 확보:", len(coordinates))
    print("좌표 누락:", len(unresolved))
    print(
        "2022년 좌표 대체 사용:",
        sum(
            coordinates["coordinate_source"]
            == "fallback_2022"
        ),
    )
    print("거리순위 파일:", distance_file)

    return {
        "observed": len(observed_ids),
        "matched": len(coordinates),
        "unresolved_ids": unresolved,
        "fallback_2022_count": int(
            sum(
                coordinates["coordinate_source"]
                == "fallback_2022"
            )
        ),
    }


# =====================================
# 9. 전체 실행
# =====================================

def main():
    if not API_KEY:
        raise SystemExit(
            ".env 파일에 KMA_API_KEY가 없습니다."
        )

    observed = read_observed_stations()
    old_coords, missing = read_old_data()

    print("\n기존 좌표 누락:", len(missing))
    print("누락 지점번호:", sorted(missing))

    print("\n기상청 공식 관측소 정보 조회")

    all_records = []

    for year in YEARS:
        for month in MONTHS:
            for kind in ("AWS", "SFC"):
                rows = fetch_station_metadata(
                    kind, year, month
                )
                all_records.extend(rows)

    if not all_records:
        raise SystemExit(
            "기상청 API에서 관측소 좌표를 "
            "하나도 가져오지 못했습니다."
        )

    metadata = pd.DataFrame(all_records)

    metadata = metadata.drop_duplicates(
        subset=[
            "station_id",
            "year",
            "month",
            "station_type",
        ]
    )

    metadata.to_csv(
        OUT / "kma_station_metadata_snapshots.csv",
        index=False,
        encoding="utf-8-sig",
    )

    recovered = (
        set(metadata["station_id"]) & missing
    )
    remaining = missing - recovered

    print("\n===== 기존 누락 24개 점검 =====")
    print("공식 좌표 확보:", len(recovered))
    print("미확보:", len(remaining))
    print("미확보 지점:", sorted(remaining))

    # 동일 연도 내 관측소 좌표 변경 검증
    moved = []

    for (year, station_id), group in metadata.groupby(
        ["year", "station_id"]
    ):
        coords = list(zip(
            group["longitude"],
            group["latitude"],
        ))

        first = coords[0]

        max_distance = max(
            haversine_km(first, item)
            for item in coords
        )

        if max_distance > 0.5:
            moved.append({
                "year": int(year),
                "station_id": station_id,
                "max_displacement_km":
                    round(max_distance, 3),
            })

    pd.DataFrame(
        moved,
        columns=[
            "year",
            "station_id",
            "max_displacement_km",
        ],
    ).to_csv(
        OUT / "station_coordinate_changes_review.csv",
        index=False,
        encoding="utf-8-sig",
    )

    print("연도 내 500m 초과 위치 변화:", len(moved))

    boundary = read_boundaries()
    results = {}

    for year in YEARS:
        results[year] = make_yearly_mapping(
            year,
            observed[year],
            metadata,
            old_coords,
            boundary,
        )

    summary = {
        "old_missing_station_count": len(missing),
        "recovered_from_kma": len(recovered),
        "still_missing_from_kma": sorted(remaining),
        "coordinate_changes_review_count": len(moved),
        "boundary_snapshot": "2025-06-30",
        "yearly_results": results,
        "warning": (
            "경계는 2025년 기준이며, "
            "일부 좌표는 2022년 대체자료임. "
            "연도 내 위치변경 관측소는 검토 필요."
        ),
    }

    report_file = (
        OUT / "coordinate_repair_report.json"
    )

    report_file.write_text(
        json.dumps(
            summary,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("\n===== 전체 보완 작업 완료 =====")
    print("품질 보고서:", report_file)


if __name__ == "__main__":
    main()
