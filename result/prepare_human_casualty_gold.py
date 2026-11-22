
from pathlib import Path
import pandas as pd

WEATHER_DIR = Path("data/gold/weather/daily_from_hourly")
HUMAN_DIR = Path("data/silver/labels/rain_casualty")
OUTPUT_DIR = Path(
    "data/gold/model_inputs/human_casualty_candidates"
)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

KEYS = ["admin_dong_code", "observation_date"]

# 1. 인적 피해 Silver 불러오기
human = pd.read_parquet(
    HUMAN_DIR / "rain_casualty_dong_daily.parquet"
)

records = pd.read_parquet(
    HUMAN_DIR / "rain_casualty_records.parquet"
)

human = human.rename(columns={
    "day": "observation_date",
    "casualty_count_total": "human_recorded_count"
})

human["admin_dong_code"] = (
    human["admin_dong_code"].astype("string").str.strip()
)

human["observation_date"] = pd.to_datetime(
    human["observation_date"]
).dt.normalize()

human = human[
    KEYS + [
        "human_recorded_count",
        "all_officially_confirmed",
        "any_rain_relation_uncertain",
        "event_ids"
    ]
].copy()

assert not human.duplicated(KEYS).any()

# 2. 행정동·일자 미정합 피해의 사건 기간 확인
unverified = records[
    records["label_status"].eq("unverified")
    & records["include_in_total"].eq(True)
].copy()

periods = unverified[
    ["event_period_start", "event_period_end"]
].drop_duplicates()

summary = []
matched_keys = []

# 3. 월별 전국 기상 데이터와 결합
for year in [2024, 2025]:

    for month in range(1, 13):

        path = (
            WEATHER_DIR / str(year)
            / f"dong_daily_weather_{year}_{month:02d}.parquet"
        )

        if not path.exists():
            raise FileNotFoundError(path)

        weather = pd.read_parquet(path)

        weather["admin_dong_code"] = (
            weather["admin_dong_code"]
            .astype("string").str.strip()
        )

        weather["observation_date"] = pd.to_datetime(
            weather["observation_date"]
        ).dt.normalize()

        assert not weather[KEYS].isna().any().any()
        assert not weather.duplicated(KEYS).any()

        df = weather.merge(
            human,
            on=KEYS,
            how="left",
            validate="one_to_one"
        )

        assert len(df) == len(weather)

        # 4. 미정합 피해 가능 기간 표시
        # 지역별 세부 범위 검증 전까지 전국을 보수적으로 표시
        df["unverified_event_window"] = False

        for _, row in periods.iterrows():

            start = pd.Timestamp(row["event_period_start"])
            end = pd.Timestamp(row["event_period_end"])

            mask = df["observation_date"].between(
                start, end
            )

            df.loc[mask, "unverified_event_window"] = True

        # 5. 인적 피해 Label 상태 구분
        df["human_label_status"] = "no_record"

        df.loc[
            df["unverified_event_window"],
            "human_label_status"
        ] = "unverified_scope"

        observed = df["human_recorded_count"].notna()

        official = (
            observed
            & df["all_officially_confirmed"].eq(True)
            & df["any_rain_relation_uncertain"].eq(False)
        )

        df.loc[
            observed & ~official,
            "human_label_status"
        ] = "nonofficial"

        df.loc[
            official,
            "human_label_status"
        ] = "official_observed"

        # 공식 관측 인원만 후보 Y로 보존
        # 기록 없음은 0으로 채우지 않음
        df["human_target_candidate"] = (
            df["human_recorded_count"]
            .where(official)
            .astype("Int64")
        )

        matched_keys.append(df.loc[observed, KEYS])

        # 6. 월별 후보 데이터 저장
        output = OUTPUT_DIR / (
            f"human_casualty_candidates_"
            f"{year}_{month:02d}.parquet"
        )

        df.to_parquet(output, index=False)

        counts = df["human_label_status"].value_counts()

        summary.append({
            "year": year,
            "month": month,
            "total_rows": len(df),
            "official_observed": counts.get(
                "official_observed", 0
            ),
            "nonofficial": counts.get("nonofficial", 0),
            "unverified_scope": counts.get(
                "unverified_scope", 0
            ),
            "no_record": counts.get("no_record", 0),
            "target_nonnull": df[
                "human_target_candidate"
            ].notna().sum(),
            "rain_daily_missing": df[
                "rain_daily_total_mm"
            ].isna().sum(),
            "rain_24h_missing": df[
                "rain_24h_max_mm"
            ].isna().sum()
        })

        print(
            f"[완료] {year}-{month:02d} | "
            f"{len(df):,}행 | "
            f"공식 피해 {official.sum()}건"
        )

# 7. 관측된 피해 기록 누락 여부 확인
matched = pd.concat(matched_keys, ignore_index=True)

coverage = human[KEYS].merge(
    matched,
    on=KEYS,
    how="left",
    indicator=True,
    validate="one_to_one"
)

assert coverage["_merge"].eq("both").all(), \
    "기상 데이터에 결합되지 않은 인적 피해 기록 존재"

# 8. 전체 검증 결과 저장
summary_df = pd.DataFrame(summary)

summary_df.to_csv(
    OUTPUT_DIR / "human_casualty_summary.csv",
    index=False,
    encoding="utf-8-sig"
)

print("\n===== 월별 검증 결과 =====")
print(summary_df.to_string(index=False))

print("\n===== 전체 데이터 검증 =====")
print(summary_df.drop(columns=["year", "month"]).sum())

print("\n저장 위치:", OUTPUT_DIR)
print("상태: PROVISIONAL_NOT_TRAINING_READY")
