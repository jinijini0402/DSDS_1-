"""EDA for the provisional observed-only property damage Gold table (2024-2025).

Requirements:
    pip install pandas numpy pyarrow matplotlib
    (matplotlib >= 3.9 is needed for boxplot(tick_labels=...); scipy is not required)

Usage:
    python result/eda_property_damage.py --gold-root data/gold
    python result/eda_property_damage.py --gold-root D:/path/to/gold

Inputs (relative to --gold-root):
    model_inputs/property_damage_observed_daily_2024_2025_recovered_noksan124_PROVISIONAL.parquet
    weather/daily_from_hourly/{2024,2025}/dong_daily_weather_YYYY_MM.parquet  (background + lag analysis)

Outputs: results/property_damage/eda/*.csv, *.png, summary.md

Caveats carried from the QA report:
  - raw_unit is NOT necessarily KRW; amounts are analysed on log scale only.
  - Missing source means unknown, never 0.
  - Labels are positive-only; dong-days without a record are not "no damage".
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

LABEL_FILE = (
    "model_inputs/"
    "property_damage_observed_daily_2024_2025_recovered_noksan124_PROVISIONAL.parquet"
)
AMOUNT_COLS = [
    "private_damage_raw_unit",
    "public_damage_raw_unit",
    "observed_component_sum_raw_unit",
]
TARGETS = ["private_damage_raw_unit", "public_damage_raw_unit"]
RAIN = [
    "rain_daily_total_mm",
    "rain_1h_max_mm",
    "rain_3h_max_mm",
    "rain_6h_max_mm",
    "rain_12h_max_mm",
    "rain_24h_max_mm",
]
OTHER_WX = ["temperature_mean_c", "humidity_mean_pct", "pressure_mean_hpa"]
LAG_DAYS = [0, 1, 2, 3, 5, 7, 10, 14]
RAIN_BINS = [-0.1, 0, 10, 30, 60, 100, 150, 250, np.inf]
RAIN_BIN_LABELS = ["0", "0-10", "10-30", "30-60", "60-100", "100-150", "150-250", "250+"]

# Legacy 2-digit sido prefix used by the NDMS/SGIS admin codes in this table.
SIDO = {
    "11": "Seoul", "21": "Busan", "22": "Daegu", "23": "Incheon", "24": "Gwangju",
    "25": "Daejeon", "26": "Ulsan", "29": "Sejong", "31": "Gyeonggi", "32": "Gangwon",
    "33": "Chungbuk", "34": "Chungnam", "35": "Jeonbuk", "36": "Jeonnam",
    "37": "Gyeongbuk", "38": "Gyeongnam", "39": "Jeju",
}


def to_float(series: pd.Series) -> pd.Series:
    """Decimal/object amounts -> float, keeping None as NaN (unknown, not 0)."""
    return series.map(lambda v: float(v) if v is not None else np.nan).astype(float)


def load_labels(gold: Path) -> pd.DataFrame:
    df = pd.read_parquet(gold / LABEL_FILE)
    for col in AMOUNT_COLS:
        df[col] = to_float(df[col])
    df["label_day"] = pd.to_datetime(df["label_day"])
    df["month"] = df["label_day"].dt.to_period("M")
    df["sido"] = df["label_dong_code"].str[:2].map(SIDO).fillna("unknown")
    df["rain_zero"] = df["rain_daily_total_mm"].eq(0)
    return df


def load_weather(gold: Path) -> pd.DataFrame:
    files = sorted((gold / "weather" / "daily_from_hourly").glob("*/dong_daily_weather_*.parquet"))
    if not files:
        raise FileNotFoundError(f"no daily weather files under {gold / 'weather/daily_from_hourly'}")
    cols = ["admin_dong_code", "observation_date"] + RAIN
    wx = pd.concat([pd.read_parquet(f, columns=cols) for f in files], ignore_index=True)
    wx["observation_date"] = pd.to_datetime(wx["observation_date"])
    return wx


def lag_windows(df: pd.DataFrame, wx: pd.DataFrame) -> pd.DataFrame:
    """For each label row, max daily rain / max 1h rain over [label_day - k, label_day]."""
    wx = wx.sort_values(["admin_dong_code", "observation_date"])
    out = df[["label_dong_code", "label_day"]].copy()
    grouped = wx.groupby("admin_dong_code", sort=False)
    pieces = []
    for k in LAG_DAYS:
        roll = grouped.rolling(f"{k + 1}D", on="observation_date")[
            ["rain_daily_total_mm", "rain_1h_max_mm"]
        ].max().reset_index()
        roll = roll.rename(columns={
            "admin_dong_code": "label_dong_code",
            "observation_date": "label_day",
            "rain_daily_total_mm": f"rain_total_max_{k}d",
            "rain_1h_max_mm": f"rain_1h_max_{k}d",
        }).drop(columns=[c for c in roll.columns if c.startswith("level_")], errors="ignore")
        pieces.append(roll.set_index(["label_dong_code", "label_day"]))
    lagged = pd.concat(pieces, axis=1).reset_index()
    return out.merge(lagged, on=["label_dong_code", "label_day"], how="left")


def spearman(a: pd.Series, b: pd.Series) -> float:
    """Spearman rho without scipy: Pearson on ranks."""
    return a.rank().corr(b.rank())


def save_hist_log(values: pd.Series, title: str, path: Path) -> None:
    v = values.dropna()
    v = v[v > 0]
    plt.figure(figsize=(8, 4.5))
    plt.hist(np.log10(v), bins=60, color="#3b6ea8")
    plt.title(f"{title} (n={len(v):,}, >0 only)")
    plt.xlabel("log10(raw_unit)")
    plt.ylabel("rows")
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--gold-root", default="data/gold", type=Path)
    parser.add_argument("--out", default="results/property_damage/eda", type=Path)
    args = parser.parse_args()
    out: Path = args.out
    out.mkdir(parents=True, exist_ok=True)
    notes: list[str] = []

    df = load_labels(args.gold_root)
    notes.append(f"rows={len(df):,}, cols={df.shape[1]}, dongs={df.label_dong_code.nunique():,}, "
                 f"days={df.label_day.nunique()}, range={df.label_day.min():%Y-%m-%d}..{df.label_day.max():%Y-%m-%d}")
    dup = df.duplicated(["label_dong_code", "label_day"]).sum()
    notes.append(f"duplicate (dong, day) keys: {dup}")

    # 1. Schema and missingness
    schema = pd.DataFrame({
        "dtype": df.dtypes.astype(str),
        "null": df.isna().sum(),
        "null_pct": (df.isna().mean() * 100).round(2),
        "n_unique": df.nunique(),
    })
    schema.to_csv(out / "01_schema_missingness.csv", encoding="utf-8-sig")
    constant = schema.index[schema["n_unique"] <= 1].tolist()
    notes.append(f"constant columns (drop for modelling): {constant}")

    # 2. Source composition
    src = pd.crosstab(df["observation_sources"], df["public_amount_complete_y"].fillna("NA"), margins=True)
    src.to_csv(out / "02_source_composition.csv", encoding="utf-8-sig")
    notes.append("sources: " + df["observation_sources"].value_counts().to_dict().__repr__())

    # 3. Target distributions (raw unit, unknown currency)
    tstats = df[AMOUNT_COLS].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9, 0.99]).T
    tstats["zeros"] = (df[AMOUNT_COLS] == 0).sum()
    tstats["skew_log1p"] = np.log1p(df[AMOUNT_COLS]).skew()
    tstats.to_csv(out / "03_target_stats.csv", encoding="utf-8-sig")
    for col in AMOUNT_COLS:
        save_hist_log(df[col], col, out / f"03_hist_log_{col}.png")
    both = df.dropna(subset=TARGETS)
    both = both[(both[TARGETS] > 0).all(axis=1)]
    plt.figure(figsize=(6, 6))
    plt.scatter(np.log10(both[TARGETS[0]]), np.log10(both[TARGETS[1]]), s=4, alpha=0.4)
    plt.xlabel("log10 private")
    plt.ylabel("log10 public")
    plt.title(f"private vs public (n={len(both):,})")
    plt.tight_layout()
    plt.savefig(out / "03_private_vs_public.png", dpi=150)
    plt.close()
    rho = both[TARGETS].corr(method="spearman").iloc[0, 1]
    notes.append(f"private vs public Spearman (both>0, n={len(both)}): {rho:.3f}")
    top_share = (df["public_damage_raw_unit"].sort_values(ascending=False).head(10).sum()
                 / df["public_damage_raw_unit"].sum())
    notes.append(f"top-10 public rows hold {top_share:.1%} of total public amount")

    # 4. Temporal profile
    monthly = df.groupby("month").agg(
        rows=("label_day", "size"),
        rain_zero_share=("rain_zero", "mean"),
        rain_total_median=("rain_daily_total_mm", "median"),
        private_sum=("private_damage_raw_unit", "sum"),
        public_sum=("public_damage_raw_unit", "sum"),
    )
    monthly.to_csv(out / "04_monthly.csv", encoding="utf-8-sig")
    fig, ax1 = plt.subplots(figsize=(11, 4.5))
    x = monthly.index.astype(str)
    ax1.bar(x, monthly["rows"], color="#3b6ea8")
    ax1.set_ylabel("label rows")
    ax2 = ax1.twinx()
    ax2.plot(x, monthly["rain_zero_share"], color="#c0392b", marker="o")
    ax2.set_ylabel("share with 0 mm same-day rain", color="#c0392b")
    ax2.set_ylim(0, 1)
    ax1.tick_params(axis="x", rotation=60)
    ax1.set_title("Label rows per month and same-day zero-rain share")
    fig.tight_layout()
    fig.savefig(out / "04_monthly_rows_rain_zero.png", dpi=150)
    plt.close(fig)
    top_days = df.groupby("label_day").agg(
        rows=("label_dong_code", "size"),
        rain_zero_share=("rain_zero", "mean"),
        rain_total_median=("rain_daily_total_mm", "median"),
        private_sum=("private_damage_raw_unit", "sum"),
        public_sum=("public_damage_raw_unit", "sum"),
    ).sort_values("rows", ascending=False).head(30)
    top_days.to_csv(out / "04_top_label_days.csv", encoding="utf-8-sig")

    # 5. Spatial profile
    sido = df.groupby("sido").agg(
        rows=("label_dong_code", "size"),
        dongs=("label_dong_code", "nunique"),
        rain_zero_share=("rain_zero", "mean"),
        private_median=("private_damage_raw_unit", "median"),
        public_median=("public_damage_raw_unit", "median"),
        public_sum=("public_damage_raw_unit", "sum"),
    ).sort_values("rows", ascending=False)
    sido.to_csv(out / "05_sido.csv", encoding="utf-8-sig")
    per_dong = df.groupby(["label_dong_code", "admin_dong_name"]).size().sort_values(ascending=False)
    per_dong.head(50).rename("label_days").to_csv(out / "05_top_dongs.csv", encoding="utf-8-sig")
    notes.append(f"label days per dong: median={per_dong.median():.0f}, max={per_dong.max()}")

    # 6. Same-day weather vs damage
    zero_share = df["rain_zero"].mean()
    notes.append(f"same-day rain_daily_total_mm == 0: {zero_share:.1%} of rows")
    corr_rows = []
    for t in TARGETS:
        sub = df[[t] + RAIN + OTHER_WX].dropna(subset=[t])
        for f in RAIN + OTHER_WX:
            s = sub[[t, f]].dropna()
            corr_rows.append({"target": t, "feature": f, "n": len(s),
                              "spearman": spearman(s[t], s[f])})
    corr = pd.DataFrame(corr_rows)
    corr.to_csv(out / "06_sameday_spearman.csv", index=False, encoding="utf-8-sig")
    df["rain_bin"] = pd.cut(df["rain_daily_total_mm"], RAIN_BINS, labels=RAIN_BIN_LABELS)
    by_bin = df.groupby("rain_bin", observed=False).agg(
        rows=("label_dong_code", "size"),
        private_median=("private_damage_raw_unit", "median"),
        private_p90=("private_damage_raw_unit", lambda s: s.quantile(0.9)),
        public_median=("public_damage_raw_unit", "median"),
        public_p90=("public_damage_raw_unit", lambda s: s.quantile(0.9)),
    )
    by_bin.to_csv(out / "06_damage_by_rain_bin.csv", encoding="utf-8-sig")
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharey=False)
    for ax, t in zip(axes, TARGETS):
        groups = [np.log10(df.loc[(df.rain_bin == b) & (df[t] > 0), t]) for b in RAIN_BIN_LABELS]
        ax.boxplot(groups, tick_labels=RAIN_BIN_LABELS, showfliers=False)
        ax.set_title(f"{t} by same-day rain (mm)")
        ax.set_ylabel("log10(raw_unit)")
        ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(out / "06_damage_by_rain_bin.png", dpi=150)
    plt.close(fig)

    # 7. Background comparison + lag window analysis (needs daily weather Gold)
    try:
        wx = load_weather(args.gold_root)
    except FileNotFoundError as exc:
        notes.append(f"weather background skipped: {exc}")
        wx = None
    if wx is not None:
        bg = wx["rain_daily_total_mm"].dropna()
        bg_rows = []
        for f in RAIN:
            b, l = wx[f].dropna(), df[f].dropna()
            bg_rows.append({"feature": f,
                            "background_median": b.median(), "label_median": l.median(),
                            "background_p95": b.quantile(0.95), "label_p95": l.quantile(0.95),
                            "background_share_zero": (b == 0).mean(), "label_share_zero": (l == 0).mean()})
        pd.DataFrame(bg_rows).to_csv(out / "07_background_vs_label.csv", index=False, encoding="utf-8-sig")
        notes.append(f"background dong-days: {len(wx):,}; background zero-rain share: {(bg == 0).mean():.1%}")

        lag = lag_windows(df, wx)
        lag_rows = []
        for k in LAG_DAYS:
            col = f"rain_total_max_{k}d"
            row = {"window_days": k + 1,
                   "share_max_ge_10mm": (lag[col] >= 10).mean(),
                   "share_max_ge_30mm": (lag[col] >= 30).mean(),
                   "share_max_ge_80mm": (lag[col] >= 80).mean()}
            for t in TARGETS:
                s = pd.concat([df[t], lag[col]], axis=1).dropna()
                row[f"spearman_{t}"] = spearman(s.iloc[:, 0], s.iloc[:, 1])
            lag_rows.append(row)
        lag_tbl = pd.DataFrame(lag_rows)
        lag_tbl.to_csv(out / "07_lag_window_rain.csv", index=False, encoding="utf-8-sig")
        plt.figure(figsize=(8, 4.5))
        for thr in ["10", "30", "80"]:
            plt.plot(lag_tbl["window_days"], lag_tbl[f"share_max_ge_{thr}mm"], marker="o", label=f">= {thr} mm")
        plt.xlabel("window: label_day and previous N-1 days")
        plt.ylabel("share of label rows")
        plt.ylim(0, 1)
        plt.title("Max daily rain in lookback window")
        plt.legend()
        plt.tight_layout()
        plt.savefig(out / "07_lag_window_rain.png", dpi=150)
        plt.close()
        best = lag_tbl.iloc[-1]
        notes.append(
            f"share with >=30 mm on some day in window: 1d={lag_tbl.iloc[0].share_max_ge_30mm:.1%}, "
            f"{int(best.window_days)}d={best.share_max_ge_30mm:.1%}")

    # 8. Weather join QA
    wqa = df[["rain_station_count_in_day", "rain_station_max_distance_m"]].describe().T
    wqa.to_csv(out / "08_weather_join_qa.csv", encoding="utf-8-sig")
    notes.append(f"rain_station_max_distance_m > 10 km: {(df.rain_station_max_distance_m > 10_000).mean():.1%}")
    notes.append(f"rain_24h_max_mm nulls: {df.rain_24h_max_mm.isna().sum()}")

    summary = "# Property damage EDA summary\n\n" + "\n".join(f"- {n}" for n in notes) + "\n"
    (out / "summary.md").write_text(summary, encoding="utf-8")
    print(summary)
    print("outputs:", out.resolve())


if __name__ == "__main__":
    main()
