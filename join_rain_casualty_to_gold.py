"""물적 피해 Gold(동·일 관측자료)에 인적 피해 Silver를 붙인다.

- 기존 Gold 파일은 덮어쓰지 않고 새 파일로 저장한다.
- 결합 키: label_dong_code(SGIS 8자리) + label_day.
- 인적 피해만 있는 동·일은 행을 추가하고, 같은 날짜의 Gold 일별 기상(dong_daily_weather)을 조회해 붙인다.
  (기상 Feature 재계산 없음, 물적 피해 컬럼은 NULL = 미확인)
- 인적 피해 상태(human_label_status)
    observed   : 해당 동·일에 확정된 인적 피해 기록이 있음
    unverified : 동/날짜를 확정 못 한 기록(시군구·시도 단위, 날짜 범위, 행정동 모호)이 이 동·일을 포함할 수 있음
    no_record  : 걸치는 기록이 없음. 피해 0이라는 뜻이 아님
  위치를 전혀 모르는 기록(전국/미기재)은 상태를 바꾸지 않고 human_unlocated_label_ids에만 남긴다.

실행 예 (프로젝트 루트에서):
  python scripts/join_rain_casualty_to_gold.py
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd

PROCESSING_VERSION = "gold_property_human_observed_daily_v1"
SCOPE_RANK = {"admin_dong_candidates": 1, "sigungu": 2, "sido": 3}
PERSONAL_COLUMNS = ["성별/연령", "주민등록지", "gender_age", "resident_address"]


def load_weather(weather_dir: Path, keys: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for (y, m), g in keys.groupby([keys["label_day"].map(lambda d: d.year), keys["label_day"].map(lambda d: d.month)]):
        f = weather_dir / f"{y}" / f"dong_daily_weather_{y}_{m:02d}.parquet"
        if not f.exists():
            raise FileNotFoundError(f"기상 Gold 파일 없음: {f}")
        w = pd.read_parquet(f)
        w = w.rename(columns={"admin_dong_code": "label_dong_code", "observation_date": "label_day", "year": "label_year"})
        w["label_day"] = pd.to_datetime(w["label_day"]).dt.date
        frames.append(w.merge(g[["label_dong_code", "label_day"]], on=["label_dong_code", "label_day"], how="inner"))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def run(args) -> None:
    gold_in = Path(args.gold_in)
    prop = pd.read_parquet(gold_in)
    prop["label_day"] = pd.to_datetime(prop["label_day"]).dt.date
    orig_cols = list(prop.columns)
    orig_dtypes = prop.dtypes.astype(str).to_dict()
    if prop.duplicated(["label_dong_code", "label_day"]).any():
        raise ValueError("물적 피해 Gold 키 중복")

    hum = pd.read_parquet(args.human_dong_daily)
    hum["day"] = pd.to_datetime(hum["day"]).dt.date
    rec = pd.read_parquet(args.human_records)
    for c in ("label_period_start", "label_period_end"):
        rec[c] = pd.to_datetime(rec[c]).dt.date

    # ---------------------------------------------- 인적 피해만 있는 동·일 → 행 추가 (기상은 Gold 조회)
    hk = hum.rename(columns={"admin_dong_code": "label_dong_code", "day": "label_day"})
    prop_keys = set(zip(prop["label_dong_code"], prop["label_day"]))
    new_keys = hk[[k not in prop_keys for k in zip(hk["label_dong_code"], hk["label_day"])]][["label_dong_code", "label_day"]]
    appended = pd.DataFrame(columns=orig_cols)
    if len(new_keys):
        w = load_weather(Path(args.weather_dir), new_keys)
        appended = new_keys.merge(w, on=["label_dong_code", "label_day"], how="left")
        appended["label_year"] = appended["label_day"].map(lambda d: d.year).astype("int32")
        appended["weather_joined"] = appended["hours_total"].notna().astype("int32")
        for c in orig_cols:
            if c not in appended.columns:  # 물적 피해 컬럼: 미확인(0 아님)
                appended[c] = prop[c].iloc[:0].reindex(appended.index).to_numpy()
        appended = appended[orig_cols]
        for c in orig_cols:  # 원래 dtype 유지 (정수 컬럼에 결측이 생기면 nullable 정수로)
            try:
                appended[c] = appended[c].astype(prop[c].dtype)
            except (TypeError, ValueError):
                if pd.api.types.is_integer_dtype(prop[c].dtype):
                    appended[c] = appended[c].astype(str(prop[c].dtype).capitalize())
                    prop[c] = prop[c].astype(str(prop[c].dtype).capitalize())
    prop["property_record_present"] = 1
    appended["property_record_present"] = 0
    base = pd.concat([prop, appended], ignore_index=True) if len(appended) else prop.copy()
    base["property_record_present"] = base["property_record_present"].astype("int8")

    # ---------------------------------------------- observed 결합
    hcols = {
        "deaths_count": "human_deaths_count",
        "missing_persons_count": "human_missing_persons_count",
        "injured_count": "human_injured_count",
        "deaths_or_missing_count": "human_deaths_or_missing_count",
        "casualty_count_total": "human_casualty_count_total",
        "best_confidence_grade": "human_best_confidence_grade",
        "all_officially_confirmed": "human_all_officially_confirmed",
        "any_rain_relation_uncertain": "human_any_rain_relation_uncertain",
        "any_estimated_mapping": "human_any_estimated_mapping",
        "label_ids": "human_label_ids",
        "event_ids": "human_event_ids",
        "label_sources": "human_label_sources",
        "label_available_at": "human_label_available_at",
        "human_damage_observed": "human_damage_observed",
    }
    h = hk[["label_dong_code", "label_day", *hcols]].rename(columns=hcols)
    out = base.merge(h, on=["label_dong_code", "label_day"], how="left", validate="one_to_one")
    out["human_damage_observed"] = out["human_damage_observed"].astype("Int8")
    for c in ["human_deaths_count", "human_missing_persons_count", "human_injured_count",
              "human_deaths_or_missing_count", "human_casualty_count_total"]:
        out[c] = out[c].astype("Int32")
    for c in ["human_all_officially_confirmed", "human_any_rain_relation_uncertain", "human_any_estimated_mapping"]:
        out[c] = out[c].astype("boolean")

    # ---------------------------------------------- 미확정 기록 표시 (분할·복제 없이 '걸칠 수 있음'만 표시)
    unv = rec[(rec["label_status"] == "unverified")]
    located = unv[unv["overlay_scope"].isin(SCOPE_RANK)]
    unlocated = unv[unv["overlay_scope"] == "unlocated"]
    codes, days = out["label_dong_code"], out["label_day"]
    scope_rank = pd.Series(99, index=out.index)
    unv_ids = pd.Series([[] for _ in range(len(out))], index=out.index)
    unv_persons = pd.Series(0, index=out.index, dtype="int64")
    for r in located.itertuples(index=False):
        area = r.overlay_area_codes.split("|")
        in_time = (days >= r.label_period_start) & (days <= r.label_period_end)
        if r.overlay_scope == "admin_dong_candidates":
            in_area = codes.isin(area)
        elif r.overlay_scope == "sigungu":
            in_area = codes.str[:5].isin(area)
        else:
            in_area = codes.str[:2].isin(area)
        m = in_time & in_area
        scope_rank[m] = scope_rank[m].clip(upper=SCOPE_RANK[r.overlay_scope])
        for i in out.index[m]:
            unv_ids[i].append(r.label_id)
        unv_persons[m] += int(r.person_count)
    inv = {v: k for k, v in SCOPE_RANK.items()}
    out["human_unverified_scope"] = scope_rank.map(inv)
    out["human_unverified_label_ids"] = unv_ids.map(lambda l: "|".join(l) if l else None)
    out["human_unverified_persons_in_scope"] = unv_persons.where(unv_persons > 0).astype("Int32")
    ul = pd.Series([[] for _ in range(len(out))], index=out.index)
    for r in unlocated.itertuples(index=False):
        m = (days >= r.label_period_start) & (days <= r.label_period_end)
        for i in out.index[m]:
            ul[i].append(r.label_id)
    out["human_unlocated_label_ids"] = ul.map(lambda l: "|".join(l) if l else None)

    out["human_label_status"] = "no_record"
    out.loc[out["human_unverified_scope"].notna(), "human_label_status"] = "unverified"
    out.loc[out["human_damage_observed"] == 1, "human_label_status"] = "observed"
    out["damage_sources"] = (
        out["property_record_present"].map({1: "PROPERTY", 0: ""})
        + out["human_damage_observed"].fillna(0).astype(int).map({1: "+HUMAN", 0: ""})
    ).str.lstrip("+")
    out["human_processing_version"] = PROCESSING_VERSION
    out = out.sort_values(["label_year", "label_dong_code", "label_day"], kind="stable").reset_index(drop=True)

    # ---------------------------------------------- 검증
    checks = {}
    checks["key_unique"] = not out.duplicated(["label_dong_code", "label_day"]).any()
    checks["row_count"] = len(out) == len(prop) + len(appended)
    a = prop[orig_cols].sort_values(["label_dong_code", "label_day"]).reset_index(drop=True)
    b = out[out["property_record_present"] == 1][orig_cols].sort_values(["label_dong_code", "label_day"]).reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(a, b, check_dtype=False)
        checks["property_rows_unchanged"] = True
    except AssertionError:
        checks["property_rows_unchanged"] = False
    dtype_changes = {c: [orig_dtypes[c], str(out[c].dtype)] for c in orig_cols if orig_dtypes[c] != str(out[c].dtype)}
    checks["human_observed_dong_days_match_silver"] = int((out["human_damage_observed"] == 1).sum()) == len(hum)
    checks["human_persons_match_silver"] = int(out["human_casualty_count_total"].sum()) == int(hum["casualty_count_total"].sum())
    checks["no_counts_outside_observed"] = bool(out.loc[out["human_label_status"] != "observed", "human_casualty_count_total"].isna().all())
    checks["no_personal_columns"] = not any(c in out.columns for c in PERSONAL_COLUMNS)
    checks["appended_rows_weather_joined"] = bool((out.loc[out["property_record_present"] == 0, "weather_joined"] == 1).all())
    checks["appended_rows_property_null"] = bool(
        out.loc[out["property_record_present"] == 0, ["private_positive_observed", "public_y_observed"]].isna().all().all())
    checks["codes_8_digit"] = bool(out["label_dong_code"].astype(str).str.fullmatch(r"\d{8}").all())

    gold_out = Path(args.out)
    gold_out.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(gold_out, index=False)

    def vc(s):
        return {str(k): int(v) for k, v in s.value_counts(dropna=False).items()}

    qa = {
        "purpose": "observed-only provisional property + human damage dong-day table (not training-ready)",
        "processing_version": PROCESSING_VERSION,
        "created_at": dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec="seconds"),
        "inputs": {"property_gold": str(gold_in), "human_dong_daily": args.human_dong_daily,
                   "human_records": args.human_records, "weather_dir": args.weather_dir},
        "rows": {"property_input": len(prop), "human_only_appended": len(appended), "output": len(out)},
        "human_label_status": vc(out["human_label_status"]),
        "human_label_status_by_year": {str(y): vc(g["human_label_status"]) for y, g in out.groupby("label_year")},
        "human_unverified_scope": vc(out["human_unverified_scope"]),
        "damage_sources": vc(out["damage_sources"]),
        "human_observed_rows": out.loc[out["human_label_status"] == "observed",
                                       ["label_dong_code", "admin_dong_name", "label_day", "human_casualty_count_total",
                                        "human_best_confidence_grade", "human_label_ids"]].astype(str).to_dict("records"),
        "rows_with_unlocated_record_in_period": int(out["human_unlocated_label_ids"].notna().sum()),
        "dtype_changes_vs_input": dtype_changes,
        "checks": checks,
        "all_checks_passed": all(checks.values()),
        "notes": [
            "human_label_status=no_record는 피해 0이 아님. 0/1 학습 라벨로 쓸 때 처리 규칙을 따로 정해야 함.",
            "unverified 행은 시군구·시도 단위 또는 날짜 범위 기록이 걸칠 수 있는 동·일. 인원은 나누지 않았고 human_unverified_persons_in_scope는 참고값(합산 금지).",
            "human_unlocated_label_ids: 위치 미기재 기록(2024.7 호우 부상 1명)이 기간에 걸침. 상태에는 반영하지 않음.",
            "이 파일은 물적 피해 관측 동·일 중심이므로 피해 없음(음성) 표본은 포함하지 않음.",
            "같은 날짜 기상은 사후 기술 통계용이며 사전 예측 Feature가 아님 (기존 Gold qa와 동일).",
        ],
    }
    qa_path = gold_out.with_suffix(".qa.json")
    qa_path.write_text(json.dumps(qa, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: qa[k] for k in ("rows", "human_label_status", "human_unverified_scope", "checks")},
                     ensure_ascii=False, indent=2))
    if not qa["all_checks_passed"]:
        raise SystemExit("검증 실패: qa.json 확인")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold-in", default="data/gold/model_inputs/property_damage_observed_daily_2024_2025_PROVISIONAL.parquet")
    ap.add_argument("--human-dong-daily", default="data/silver/labels/rain_casualty/rain_casualty_dong_daily.parquet")
    ap.add_argument("--human-records", default="data/silver/labels/rain_casualty/rain_casualty_records.parquet")
    ap.add_argument("--weather-dir", default="data/gold/weather/daily_from_hourly")
    ap.add_argument("--out", default="data/gold/model_inputs/property_human_damage_observed_daily_2024_2025_PROVISIONAL.parquet")
    run(ap.parse_args())


if __name__ == "__main__":
    main()
