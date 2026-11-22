from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

BASE = Path(
    "data/gold/model_inputs/human_casualty_candidates"
)

OUTPUT = Path("results/human_casualty/eda")
OUTPUT.mkdir(parents=True, exist_ok=True)

FEATURES = [
    "rain_daily_total_mm",
    "rain_1h_max_mm",
    "rain_3h_max_mm",
    "rain_6h_max_mm",
    "rain_12h_max_mm",
    "rain_24h_max_mm"
]

COLS = [
    "year",
    "admin_dong_code",
    "admin_dong_name",
    "observation_date",
    "human_label_status",
    "human_target_candidate"
] + FEATURES

files = sorted(
    BASE.glob("human_casualty_candidates_*.parquet")
)

assert len(files) == 24

df = pd.concat(
    [pd.read_parquet(f, columns=COLS) for f in files],
    ignore_index=True
)

official = df[
    df["human_label_status"].eq("official_observed")
].copy()

print("전체 표본:", len(df))
print("공식 피해 관측:", len(official))

# 1. Label 상태 분석
status = df["human_label_status"].value_counts()

status.to_csv(
    OUTPUT / "label_status_summary.csv"
)

# 2. 전체 기상 데이터 기초통계
weather_stats = df[FEATURES].describe(
    percentiles=[0.90, 0.95, 0.99, 0.999]
).T

weather_stats.to_csv(
    OUTPUT / "weather_statistics.csv"
)

print("\n===== 전체 강수량 기초통계 =====")
print(weather_stats)

# 3. 인적 피해 사례별 강수량 백분위
for feature in FEATURES:

    background = df[feature].dropna()

    official[f"{feature}_percentile"] = (
        official[feature].apply(
            lambda x: (
                (background <= x).mean() * 100
                if pd.notna(x)
                else float("nan")
            )
        )
    )

official.to_csv(
    OUTPUT / "official_casualty_weather.csv",
    index=False,
    encoding="utf-8-sig"
)

print("\n===== 공식 피해 사례 강수량 백분위 =====")

percentile_cols = [
    c for c in official.columns
    if c.endswith("_percentile")
]

print(
    official[
        ["admin_dong_name"] + percentile_cols
    ].round(2).to_string(index=False)
)

# 4. 공식 피해 사례별 강수량 비교
ax = official.set_index(
    "admin_dong_code"
)[FEATURES].plot(
    kind="bar",
    figsize=(13, 6)
)

ax.set_title("Rainfall in Official Casualty Cases")
ax.set_xlabel("Administrative Dong Code")
ax.set_ylabel("Rainfall (mm)")

plt.xticks(rotation=0)
plt.tight_layout()

plt.savefig(
    OUTPUT / "casualty_rainfall_comparison.png",
    dpi=200
)
plt.close()

# 5. 전체 강수량 분포와 피해 사례 비교
for feature in FEATURES:

    plt.figure(figsize=(9, 5))

    values = df[feature].dropna()

    plt.hist(
        values,
        bins=80,
        log=True,
        alpha=0.6,
        label="All weather observations"
    )

    for i, value in enumerate(
        official[feature].dropna()
    ):
        plt.axvline(
            value,
            color="red",
            linestyle="--",
            alpha=0.7,
            label="Official casualty case"
            if i == 0 else None
        )

    plt.title(feature)
    plt.xlabel("Rainfall (mm)")
    plt.ylabel("Frequency (log scale)")
    plt.legend()
    plt.tight_layout()

    plt.savefig(
        OUTPUT / f"{feature}_distribution.png",
        dpi=200
    )
    plt.close()

print("\nEDA 완료")
print("저장 위치:", OUTPUT)