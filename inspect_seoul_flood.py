from pathlib import Path

import geopandas as gpd
import pandas as pd

base = Path(__file__).resolve().parent
source_dir = base / "data/bronze/labels"
date_pattern = r"^(\d{8}|\d{4}-\d{2}-\d{2})(?:\.0+|\s*-\s*\d{4})?$"

all_dates = []

for file_year in (2022, 2023, 2024):
    year_dir = source_dir / str(file_year)
    shp_files = list(year_dir.rglob("*.shp"))
    if len(shp_files) != 1:
        raise SystemExit(f"{year_dir}에서 SHP 파일을 {len(shp_files)}개 찾았어.")

    records = gpd.read_file(shp_files[0])
    raw_dates = records["F_SAT_YMD"].astype("string").str.strip()
    date_part = raw_dates.str.extract(date_pattern, expand=False)
    parsed_dates = pd.to_datetime(
        date_part.str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    )
    all_dates.append(parsed_dates)

    if file_year == 2022:
        print("=== 2022년 파일의 침수 시작일 기준 연도별 기록 수 ===")
        print(parsed_dates.dt.year.dropna().astype(int).value_counts().sort_index().to_string())
        print(f"시작일을 읽은 기록: {parsed_dates.notna().sum():,}개")
        print(f"시작일 확인이 남은 기록: {parsed_dates.isna().sum():,}개")

        print("\n=== 아직 읽지 못한 원본 값 (상위 15개) ===")
        unchecked = raw_dates[parsed_dates.isna()].fillna("<빈 값>").replace("", "<빈 값>")
        print(unchecked.value_counts().head(15).to_string())

        unusual_year = parsed_dates.dt.year.eq(2208)
        columns = ["F_SAT_YMD", "F_END_YMD", "F_YR", "F_DISA_NM"]
        print(f"\n=== 시작일이 2208년으로 읽힌 {unusual_year.sum()}행 ===")
        print(records.loc[unusual_year, columns].to_string(index=False))

dates = pd.concat(all_dates, ignore_index=True)
counts = dates.dt.to_period("M").value_counts().sort_index()

months = pd.period_range("2022-01", "2024-12", freq="M")
monthly_counts = counts.reindex(months, fill_value=0)
outside_counts = counts[~counts.index.isin(months)]
empty_months = monthly_counts.index[monthly_counts.eq(0)].astype(str)

print("\n=== 2022~2024년 월별 기록 수 ===")
print(monthly_counts.to_string())
print("\n기록이 0건인 달:", ", ".join(empty_months) or "없음")

print("\n=== 대상 기간 밖에 기록된 시작일 ===")
print(outside_counts.to_string() if not outside_counts.empty else "없음")
print(f"시작일을 읽지 못한 기록: {dates.isna().sum():,}행")