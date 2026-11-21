
import csv
import json
import re
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

# ==========================================
# 1. 입력 및 출력 경로
# ==========================================

STATION_FILE = Path(
    "data/bronze/weather/aws_stations_2022.txt"
)

BOUNDARY_FILE = Path(
    "data/bronze/boundaries/BND_ADM_DONG_PG/"
    "BND_ADM_DONG_PG.shp"
)

SILVER_DIR = Path(
    "data/silver/weather/station_hourly"
)

OUT = Path(
    "data/silver/mappings/nationwide_aws_dong"
)
OUT.mkdir(parents=True, exist_ok=True)

YEARS = (2024, 2025)
TOP_K = 30

# ==========================================
# 2. 관측소 번호 표준화
# ==========================================

def normalize_id(value):
    s = str(value).strip()

    if re.fullmatch(r"\d+(?:\.0+)?", s):
        return str(int(float(s)))

    return s


# ==========================================
# 3. Silver에서 실제 관측소 번호 확인
# ==========================================

def read_observed_stations():
    stations_by_year = {}

    for year in YEARS:
        path = SILVER_DIR / (
            f"nationwide_station_hourly_{year}.csv"
        )

        if not path.exists():
            raise FileNotFoundError(
                f"Silver 파일이 없습니다: {path}"
            )

        ids = set()

        for chunk in pd.read_csv(
            path,
            usecols=["station_id"],
            dtype={"station_id": "string"},
            chunksize=200000,
        ):
            for value in chunk["station_id"].dropna().unique():
                ids.add(normalize_id(value))

        stations_by_year[year] = ids
        print(f"{year}년 관측소: {len(ids)}개")

    return stations_by_year


# ==========================================
# 4. AWS 좌표 파일 읽기
# ==========================================

def read_station_coordinates(path):
    if not path.exists():
        raise FileNotFoundError(
            f"관측소 좌표 파일이 없습니다: {path}"
        )

    found = {}
    coordinate_conflicts = []

    with path.open(
        "r", encoding="utf-8", errors="replace"
    ) as f:
        for line_number, line in enumerate(f, 1):
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            parts = line.replace(",", " ").split()

            if not parts or not parts[0].isdigit():
                continue

            station_id = normalize_id(parts[0])
            coords = None

            # 관측소 메타데이터의 경도·위도 쌍 검색
            for i in range(1, len(parts) - 1):
                try:
                    a = float(parts[i])
                    b = float(parts[i + 1])
                except ValueError:
                    continue

                if 124 <= a <= 132 and 32 <= b <= 40:
                    coords = (a, b)
                    break

                if 32 <= a <= 40 and 124 <= b <= 132:
                    coords = (b, a)
                    break

            if coords is None:
                continue

            lon, lat = coords

            if station_id in found:
                old = found[station_id]

                if (
                    old["longitude"] != lon
                    or old["latitude"] != lat
                ):
                    coordinate_conflicts.append(station_id)

                continue

            found[station_id] = {
                "station_id": station_id,
                "longitude": lon,
                "latitude": lat,
            }

    if not found:
        raise ValueError(
            "관측소 좌표를 찾지 못했습니다. "
            "aws_stations_2022.txt의 컬럼 구조를 확인하세요."
        )

    return pd.DataFrame(found.values()), coordinate_conflicts


# ==========================================
# 5. 전국 행정동 경계 읽기
# ==========================================


def read_boundaries():
    if not BOUNDARY_FILE.exists():
        raise FileNotFoundError(
            f"행정동 SHP 파일이 없습니다: {BOUNDARY_FILE}"
        )

    boundary = gpd.read_file(BOUNDARY_FILE)

    print("실제 행정동 경계 컬럼:", list(boundary.columns))

    # 실제 SHP 컬럼명을 코드 내부의 표준 이름으로 통일
    rename_map = {
        "ADM_CD": "ADM_DR_CD",
        "ADM_NM": "ADM_DR_NM",
    }

    boundary = boundary.rename(columns=rename_map)

    required = {"ADM_DR_CD", "ADM_DR_NM"}

    if not required.issubset(boundary.columns):
        raise ValueError(
            "필수 행정동 컬럼이 없습니다.\n"
            f"실제 컬럼: {list(boundary.columns)}"
        )

    if boundary.crs is None:
        raise ValueError(
            "행정동 경계의 좌표계를 알 수 없습니다. "
            ".prj 파일을 확인하세요."
        )

    # geometry 결측 제거
    boundary = boundary[
        boundary.geometry.notna()
        & ~boundary.geometry.is_empty
    ].copy()

    # 행정동 코드 문자열로 변환
    boundary["ADM_DR_CD"] = (
        boundary["ADM_DR_CD"].astype(str).str.strip()
    )

    # 동일한 행정동 코드가 여러 개면 합치기
    if boundary["ADM_DR_CD"].duplicated().any():

        agg = {"ADM_DR_NM": "first"}

        if "BASE_DATE" in boundary.columns:
            agg["BASE_DATE"] = "first"

        boundary = boundary.dissolve(
            by="ADM_DR_CD",
            aggfunc=agg,
            as_index=False,
        )

    # 거리 계산을 위해 미터 단위 좌표계로 변환
    boundary = boundary.to_crs(epsg=5179)

    print("행정동 경계 개수:", len(boundary))

    if "BASE_DATE" in boundary.columns:
        print(
            "경계 기준일:",
            boundary["BASE_DATE"].dropna().unique()[:10]
        )

    return boundary


# ==========================================
# 6. 관측소와 행정동 공간 정합
# ==========================================

def main():
    observed_by_year = read_observed_stations()
    observed_ids = set().union(*observed_by_year.values())

    station_info, conflicts = read_station_coordinates(
        STATION_FILE
    )

    station_info = station_info[
        station_info["station_id"].isin(observed_ids)
    ].copy()

    matched_ids = set(station_info["station_id"])
    unmatched_ids = sorted(observed_ids - matched_ids)

    if station_info.empty:
        raise ValueError(
            "Silver 관측소와 좌표 메타데이터가 "
            "하나도 일치하지 않습니다."
        )

    stations = gpd.GeoDataFrame(
        station_info,
        geometry=gpd.points_from_xy(
            station_info["longitude"],
            station_info["latitude"],
        ),
        crs="EPSG:4326",
    ).to_crs(epsg=5179)

    boundary = read_boundaries()

    print("행정동 수:", len(boundary))
    print("좌표 확보 관측소:", len(stations))
    print("좌표 누락 관측소:", len(unmatched_ids))

    # 관측소가 실제 위치한 행정동
    joined = gpd.sjoin(
        stations,
        boundary[
            ["ADM_DR_CD", "ADM_DR_NM", "geometry"]
        ],
        how="left",
        predicate="within",
    )

    location = joined[
        [
            "station_id",
            "longitude",
            "latitude",
            "ADM_DR_CD",
            "ADM_DR_NM",
        ]
    ].drop_duplicates(subset=["station_id"])

    location = location.rename(columns={
        "ADM_DR_CD": "station_admin_code",
        "ADM_DR_NM": "station_admin_name",
    })

    location_file = OUT / (
        "station_admin_location_2025_snapshot.csv"
    )

    location.to_csv(
        location_file,
        index=False,
        encoding="utf-8-sig",
    )

    station_admin = location.set_index(
        "station_id"
    )[["station_admin_code", "station_admin_name"]].to_dict(
        orient="index"
    )

    # ======================================
    # 7. 행정동별 거리 순위 계산
    # ======================================

    distance_file = OUT / (
        "dong_station_distance_2025_snapshot.csv"
    )

    sx = stations.geometry.x.to_numpy()
    sy = stations.geometry.y.to_numpy()
    station_rows = stations.reset_index(drop=True)

    fields = [
        "admin_dong_code",
        "admin_dong_name",
        "station_id",
        "station_longitude",
        "station_latitude",
        "distance_rank",
        "distance_m",
        "station_admin_code",
        "station_admin_name",
        "boundary_snapshot",
        "station_metadata_snapshot",
    ]

    with distance_file.open(
        "w", newline="", encoding="utf-8-sig"
    ) as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()

        for _, dong in boundary.iterrows():

            # 폴리곤 내부 대표점
            point = dong.geometry.representative_point()

            distances = np.hypot(
                sx - point.x,
                sy - point.y,
            )

            nearest = np.argsort(distances)[
                :min(TOP_K, len(distances))
            ]

            for rank, index in enumerate(nearest, 1):
                station = station_rows.iloc[int(index)]
                station_id = station["station_id"]

                admin = station_admin.get(
                    station_id, {}
                )

                writer.writerow({
                    "admin_dong_code": dong["ADM_DR_CD"],
                    "admin_dong_name": dong["ADM_DR_NM"],
                    "station_id": station_id,
                    "station_longitude":
                        station["longitude"],
                    "station_latitude":
                        station["latitude"],
                    "distance_rank": rank,
                    "distance_m":
                        round(float(distances[index]), 2),
                    "station_admin_code":
                        admin.get("station_admin_code"),
                    "station_admin_name":
                        admin.get("station_admin_name"),
                    "boundary_snapshot": "2025-06",
                    "station_metadata_snapshot": "2022",
                })

    # ======================================
    # 8. 품질 보고서
    # ======================================

    base_dates = (
        sorted(
            boundary["BASE_DATE"].dropna()
            .astype(str).unique().tolist()
        )
        if "BASE_DATE" in boundary.columns
        else []
    )

    report = {
        "boundary_crs_for_distance": "EPSG:5179",
        "boundary_base_dates": base_dates,
        "boundary_dong_count": len(boundary),
        "candidate_count_per_dong": min(
            TOP_K, len(stations)
        ),
        "observed_station_count": len(observed_ids),
        "matched_station_count": len(matched_ids),
        "unmatched_station_ids": unmatched_ids,
        "metadata_coordinate_conflicts":
            sorted(set(conflicts)),
        "observed_station_counts_by_year": {
            str(y): len(ids)
            for y, ids in observed_by_year.items()
        },
        "station_metadata_snapshot": "2022",
        "boundary_snapshot": "2025-06",
        "historical_validation_complete": False,
    }

    report_file = OUT / "mapping_quality_report.json"

    report_file.write_text(
        json.dumps(
            report, ensure_ascii=False, indent=2
        ),
        encoding="utf-8",
    )

    print("\n===== 공간 정합 결과 =====")
    print("행정동:", len(boundary))
    print("관측소 좌표 확보:", len(matched_ids))
    print("관측소 좌표 누락:", len(unmatched_ids))
    print("거리순위 파일:", distance_file)
    print("관측소 소재지:", location_file)
    print("품질 보고서:", report_file)


if __name__ == "__main__":
    main()
