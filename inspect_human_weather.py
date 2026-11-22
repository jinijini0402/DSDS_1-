
from pathlib import Path
import pandas as pd

base = Path("data/gold/weather/daily_from_hourly")

for year in [2024, 2025]:
    files = sorted(
        (base / str(year)).glob(
            f"dong_daily_weather_{year}_*.parquet"
        )
    )

    print(f"\n===== {year}년 기상 Gold =====")
    print("파일 수:", len(files))

    if not files:
        continue

    df = pd.read_parquet(files[0])

    print("확인 파일:", files[0].name)
    print("데이터 크기:", df.shape)

    print("\n===== 컬럼 목록 =====")
    print(df.columns.tolist())

    print("\n===== 데이터 타입 =====")
    print(df.dtypes)

    print("\n===== 데이터 샘플 =====")
    print(df.head(5).to_string(index=False))

    print("\n===== 결측치 =====")
    print(df.isna().sum())
