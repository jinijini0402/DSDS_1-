import pandas as pd

base = "data/silver/labels/rain_casualty/"

files = [
    "rain_casualty_records.parquet",
    "rain_casualty_dong_daily.parquet"
]

for file in files:
    df = pd.read_parquet(base + file)

    print("\n" + "=" * 60)
    print("파일명:", file)
    print("데이터 크기:", df.shape)

    print("\n===== 컬럼 목록 =====")
    print(df.columns.tolist())

    print("\n===== 결측치 현황 =====")
    print(df.isna().sum())

    print("\n===== 전체 데이터 =====")
    print(df.to_string(index=False, max_rows=100))