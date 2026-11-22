import pandas as pd

path = (
    "data/gold/model_inputs/"
    "property_human_damage_observed_daily_"
    "2024_2025_recovered_noksan124_PROVISIONAL.parquet"
)

df = pd.read_parquet(path)

print("===== 전체 데이터 크기 =====")
print(df.shape)

print("\n===== 인적 피해 Label 상태 =====")
print(df["human_label_status"].value_counts())

print("\n===== 연도별 Label 상태 =====")
print(pd.crosstab(
    df["label_year"],
    df["human_label_status"]
))

# 실제 인적 피해가 관측된 데이터
observed = df[
    df["human_label_status"] == "observed"
].copy()

columns = [
    "label_year",
    "label_day",
    "label_dong_code",
    "admin_dong_name",
    "human_deaths_count",
    "human_missing_persons_count",
    "human_injured_count",
    "human_casualty_count_total",
    "human_all_officially_confirmed",
    "rain_daily_total_mm",
    "rain_1h_max_mm",
    "rain_3h_max_mm",
    "rain_6h_max_mm",
    "rain_12h_max_mm",
    "rain_24h_max_mm"
]

print("\n===== 관측된 인적 피해 상세 =====")
print(observed[columns].to_string(index=False))

# 공식 확인된 인적 피해
official = observed[
    observed["human_all_officially_confirmed"] == True
].copy()

print("\n===== 공식 인적 피해 =====")
print(official[columns].to_string(index=False))

y = official["human_casualty_count_total"].dropna()

print("\n===== 사상자 수 기초통계 =====")
print(y.describe())
print("평균:", y.mean())
print("분산:", y.var())
print("표본 수:", len(y))
print("0명 표본:", (y == 0).sum())
print("양수 표본:", (y > 0).sum())