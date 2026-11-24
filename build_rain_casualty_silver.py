"""인적 피해(호우 사상자) Silver 생성 + 행정동 정합.

이슈 11.19 '다음 할 일' 규칙:
  - 키: admin_dong_code(SGIS 8자리, 문자열), day(YYYY-MM-DD), 근거 출처 / 신뢰등급 / 합산여부
  - 날짜가 범위이거나 행정동이 모호한 기록은 키를 NULL로 두고 분리 (임의 동·일 분할 금지)
  - 개인 신상/식별 정보(성별/연령, 주민등록지)는 Silver에 남기지 않음
  - 물적 피해 Gold / 기상 Feature는 재처리하지 않음

행정동 기준: Gold와 같은 SGIS BND_ADM_DONG_PG (BASE_DATE 2025-06-30, 3,559개 동).
법정동 → 행정동 해석: official_full_linkage_2024.csv(2024-10-01) ∪ official_full_linkage_2025.csv(2025-12-31)
  를 Gold 경계에 존재하는 코드로 제한해서 사용. 후보가 정확히 1개일 때만 확정.

실행 예 (프로젝트 루트에서):
  python scripts/build_rain_casualty_silver.py \
      --input data/bronze/labels/rain_casualty_2024_2025.xlsx
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
from pathlib import Path

import pandas as pd
from dbfread import DBF

PROCESSING_VERSION = "silver_rain_casualty_v1"
BOUNDARY_VERSION = "SGIS_BND_ADM_DONG_PG_20250630"
BOUNDARY_DATE = "2025-06-30"

EXPECTED_COLUMNS = [
    "연도", "재해명(기간)", "구분", "시도", "시군구", "읍면동", "발생일", "발생시각",
    "피해유형/경위", "성별/연령", "주민등록지", "인원", "집계시각(as-of)", "Status",
    "합산포함", "출처", "페이지/URL", "비고",
]
PERSONAL_COLUMNS = ["성별/연령", "주민등록지"]  # 공유·모델 입력 제외

SIDO_FULL = {
    "서울": "서울특별시", "부산": "부산광역시", "대구": "대구광역시", "인천": "인천광역시",
    "광주": "광주광역시", "대전": "대전광역시", "울산": "울산광역시", "세종": "세종특별자치시",
    "경기": "경기도", "강원": "강원특별자치도", "충북": "충청북도", "충남": "충청남도",
    "전북": "전북특별자치도", "전남": "전라남도", "경북": "경상북도", "경남": "경상남도",
    "제주": "제주특별자치도",
}
CASUALTY_TYPE = {"사망": "death", "실종": "missing", "부상": "injured", "사망+실종": "death_or_missing"}
COUNT_COLUMN = {
    "death": "deaths_count",
    "missing": "missing_persons_count",
    "injured": "injured_count",
    "death_or_missing": "deaths_or_missing_count",
}
UNKNOWN_TOKENS = {"", "(미기재)", "미기재", "nan", "None"}


def clean(v) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return None if s in UNKNOWN_TOKENS else s


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------- 날짜 해석
def parse_event_period(name: str | None, year: int) -> tuple[dt.date | None, dt.date | None]:
    """'7.8.~19. 호우', '2025.7.16.~20. 호우', '9.19.~21. 호우' → (시작, 종료)."""
    if not name:
        return None, None
    m = re.match(r"^\s*(?:(\d{4})\.)?(\d{1,2})\.(\d{1,2})\.?\s*~\s*(?:(\d{1,2})\.)?(\d{1,2})\.?", name)
    if not m:
        return None, None
    y = int(m.group(1) or year)
    m1, d1 = int(m.group(2)), int(m.group(3))
    m2 = int(m.group(4)) if m.group(4) else m1
    d2 = int(m.group(5))
    return dt.date(y, m1, d1), dt.date(y, m2, d2)


def parse_occurred(v) -> tuple[str | None, dt.date | None, dt.date | None, dt.date | None]:
    """발생일 → (원문, 단일일자 또는 None, 기간시작, 기간종료). 범위면 단일일자는 None."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None, None, None, None
    if isinstance(v, (dt.datetime, pd.Timestamp)):
        d = v.date()
        return d.isoformat(), d, d, d
    if isinstance(v, dt.date):
        return v.isoformat(), v, v, v
    s = str(v).strip()
    if s in UNKNOWN_TOKENS:
        return None, None, None, None
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        d = dt.date(*map(int, m.groups()))
        return s, d, d, d
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})\s*~\s*(?:(\d{2})-)?(\d{1,2})", s)
    if m:
        y, mo, d1 = int(m.group(1)), int(m.group(2)), int(m.group(3))
        mo2 = int(m.group(4)) if m.group(4) else mo
        return s, None, dt.date(y, mo, d1), dt.date(y, mo2, int(m.group(5)))
    return s, None, None, None


def parse_as_of(v) -> str | None:
    s = clean(v)
    if not s:
        return None
    m = re.match(r"(\d{4})[.-](\d{1,2})[.-](\d{1,2})(?:\s+(\d{1,2}):(\d{2}))?", s)
    if not m:
        return None
    y, mo, d = map(int, m.group(1, 2, 3))
    if m.group(4):
        return dt.datetime(y, mo, d, int(m.group(4)), int(m.group(5))).isoformat(timespec="minutes") + "+09:00"
    return dt.date(y, mo, d).isoformat()


def confidence_grade(status: str | None) -> str | None:
    if not status:
        return None
    s = status.upper()
    if s.startswith("OFFICIAL-INTERIM"):
        return "B_official_interim"
    if s.startswith("OFFICIAL"):
        return "A_official"
    if s.startswith("MEDIA"):
        return "C_media"
    return "U_unknown"


# ---------------------------------------------------------------- 행정동 참조
class AdminReference:
    def __init__(self, boundary_dbf: Path, linkage_paths: list[Path]):
        bnd = pd.DataFrame(iter(DBF(str(boundary_dbf), encoding="cp949")))
        bnd["ADM_CD"] = bnd["ADM_CD"].astype(str)
        if set(bnd["BASE_DATE"].astype(str)) != {BOUNDARY_DATE.replace("-", "")}:
            raise ValueError("경계 파일 BASE_DATE가 2025-06-30이 아님")
        self.bnd = bnd
        self.bnd_codes = set(bnd["ADM_CD"])
        links = []
        for p in linkage_paths:
            l = pd.read_csv(p, dtype=str)
            l["linkage_file"] = p.name
            links.append(l)
        self.link = pd.concat(links, ignore_index=True)
        self.link = self.link[self.link["sgis_dong8"].isin(self.bnd_codes)].copy()
        self.link["sgg5"] = self.link["sgis_dong8"].str[:5]
        # (시도, 시군구) → SGIS 시군구 5자리 코드
        self.sgg_codes = (
            self.link.groupby(["sido", "sigungu"])["sgg5"].apply(lambda s: sorted(set(s))).to_dict()
        )
        self.sido_codes = self.link.groupby("sido")["sgis_dong8"].apply(lambda s: sorted({c[:2] for c in s})).to_dict()
        self.bnd["sgg5"] = self.bnd["ADM_CD"].str[:5]
        self.name_by_code = dict(zip(self.bnd["ADM_CD"], self.bnd["ADM_NM"]))

    def sgg_prefixes(self, sido_full: str | None, sigungu: str | None) -> list[str]:
        if not sido_full or not sigungu:
            return []
        exact = self.sgg_codes.get((sido_full, sigungu))
        if exact:
            return exact
        # '수원시 장안구'처럼 표기가 다른 경우 공백 제거 후 비교
        key = sigungu.replace(" ", "")
        hits = [v for (sd, sg), v in self.sgg_codes.items() if sd == sido_full and sg.replace(" ", "") == key]
        return sorted({c for v in hits for c in v})

    def resolve_token(self, token: str, sgg5: list[str]) -> dict:
        admin = sorted(self.bnd.loc[(self.bnd["ADM_NM"] == token) & (self.bnd["sgg5"].isin(sgg5)), "ADM_CD"].unique())
        sub = self.link[self.link["sgg5"].isin(sgg5)]
        legal = sorted(sub.loc[sub["legal_dong_name"] == token, "sgis_dong8"].unique())
        split = sorted(sub.loc[(sub["legal_dong_name"] == token) & (sub["legal_split_flag"].notna()), "sgis_dong8"].unique())
        return {"token": token, "admin_name_match": admin, "legal_name_match": legal, "legal_split": split,
                "candidates": sorted(set(admin) | set(legal))}

    def resolve(self, emd_raw: str, sgg5: list[str]) -> dict:
        tokens = emd_raw.split()
        parts = [self.resolve_token(t, sgg5) for t in tokens]
        cand = None
        for p in parts:
            cand = set(p["candidates"]) if cand is None else cand & set(p["candidates"])
        cand = sorted(cand or [])
        methods = []
        for p in parts:
            if p["admin_name_match"]:
                methods.append(f"admin_name_exact:{p['token']}")
            if p["legal_name_match"]:
                methods.append(f"legal_dong_linkage:{p['token']}")
        return {"tokens": parts, "candidates": cand, "method": "+".join(methods) or None}


# ---------------------------------------------------------------- 본 처리
def build(args) -> None:
    src = Path(args.input)
    raw = pd.read_excel(src, sheet_name=args.sheet, dtype=object)
    missing = [c for c in EXPECTED_COLUMNS if c not in raw.columns]
    if missing:
        raise ValueError(f"원본 컬럼 누락: {missing}")
    raw = raw[EXPECTED_COLUMNS]
    raw = raw[raw["구분"].notna()].reset_index(drop=True)

    personal_nonnull = {c: int(raw[c].notna().sum()) for c in PERSONAL_COLUMNS}
    work = raw.drop(columns=PERSONAL_COLUMNS)  # 개인 신상 정보는 이 시점 이후 사용하지 않음

    ref = AdminReference(Path(args.boundary_dbf), [Path(p) for p in args.linkage])

    # 수동 행정동 확정 목록 (근거 출처 필수). 없으면 빈 목록.
    overrides: dict[str, dict] = {}
    ov_path = Path(args.overrides) if args.overrides else None
    if ov_path and ov_path.exists():
        ov = pd.read_csv(ov_path, dtype=str).fillna("")
        for o in ov.to_dict("records"):
            if o["source_record_id"] in overrides:
                raise ValueError(f"오버라이드 중복: {o['source_record_id']}")
            overrides[o["source_record_id"]] = o
    overrides_applied: list[dict] = []

    rows, mapping_audit = [], []
    for i, r in work.iterrows():
        excel_row = i + 2  # 헤더가 1행
        year = int(float(r["연도"]))
        ev_name = clean(r["재해명(기간)"])
        ev_start, ev_end = parse_event_period(ev_name, year)
        ctype = CASUALTY_TYPE.get(clean(r["구분"]) or "")
        if ctype is None:
            raise ValueError(f"알 수 없는 구분 값: {r['구분']} (행 {excel_row})")
        persons = int(float(r["인원"]))
        sido_raw, sgg_raw, emd_raw = clean(r["시도"]), clean(r["시군구"]), clean(r["읍면동"])
        sido_full = SIDO_FULL.get(sido_raw) if sido_raw else None
        if sido_raw == "전국":
            level = "national"
        elif emd_raw and sgg_raw:
            level = "admin_dong"
        elif sgg_raw:
            level = "sigungu"
        elif sido_full:
            level = "sido"
        else:
            level = "unknown"
        occ_raw, occ_day, occ_start, occ_end = parse_occurred(r["발생일"])
        status_raw = clean(r["Status"])
        include = (clean(r["합산포함"]) or "").upper() == "Y"
        note = clean(r["비고"])
        desc = clean(r["피해유형/경위"])
        sgg5 = ref.sgg_prefixes(sido_full, sgg_raw)
        sido2 = ref.sido_codes.get(sido_full, []) if sido_full else []

        res = None
        orig_candidates: list[str] = []
        override = overrides.get(f"Sheet1!R{excel_row}")
        if level == "admin_dong":
            if not sgg5:
                raise ValueError(f"시군구 코드를 찾지 못함: {sido_full} {sgg_raw} (행 {excel_row})")
            res = ref.resolve(emd_raw, sgg5)
            orig_candidates = list(res["candidates"])
            if override is not None:
                # 수동 확정: 근거(보도 위치 + 경계 확인)가 기록된 경우에만, 보고된 시군구 안의 Gold 코드로만 허용
                oc = override["admin_dong_code"]
                if override.get("eupmyeondong_raw") and override["eupmyeondong_raw"] != emd_raw:
                    raise ValueError(f"오버라이드 대상 불일치 (행 {excel_row}): {override['eupmyeondong_raw']} != {emd_raw}")
                if oc not in ref.bnd_codes or oc[:5] not in sgg5:
                    raise ValueError(f"오버라이드 코드가 Gold 경계/보고 시군구 밖임 (행 {excel_row}): {oc}")
                if not override.get("evidence_sources"):
                    raise ValueError(f"오버라이드 근거 출처 없음 (행 {excel_row})")
                res = {**res, "candidates": [oc], "method": f"override:{override['evidence_method']}"}
                overrides_applied.append({"source_record_id": f"Sheet1!R{excel_row}", **override,
                                          "candidates_before_override": orig_candidates})
            mapping_audit.append({"source_record_id": f"Sheet1!R{excel_row}", "sido": sido_full, "sigungu": sgg_raw,
                                  "eupmyeondong_raw": emd_raw, "candidates_before_override": orig_candidates,
                                  **{k: v for k, v in res.items()}})
        elif override is not None:
            raise ValueError(f"오버라이드는 읍면동이 있는 기록에만 적용 가능 (행 {excel_row})")

        # 판정
        reason = None
        if not include:
            status = "excluded_not_in_total"
            reason = "합산포함=N (이전 시점 스냅샷 또는 교차검증용 합계)"
        elif level != "admin_dong":
            status, reason = "unverified", f"location_{level}_only"
        elif len(res["candidates"]) == 0:
            status, reason = "unverified", "dong_unmatched"
        elif len(res["candidates"]) > 1:
            status, reason = "unverified", "dong_ambiguous"
        elif occ_day is None:
            status, reason = "unverified", "date_range" if occ_start else "date_missing"
        else:
            status = "observed"

        observed = status == "observed"
        code = res["candidates"][0] if observed else None
        p_start = occ_start or ev_start
        p_end = occ_end or ev_end

        # 미확정 기록이 걸칠 수 있는 범위 (Gold에서 'unverified' 표시용; 피해를 나누거나 복제하지 않음)
        if status == "unverified":
            if level == "admin_dong":
                scope, area = "admin_dong_candidates", res["candidates"] or sgg5
            elif level == "sigungu":
                scope, area = "sigungu", sgg5
            elif level == "sido":
                scope, area = "sido", sido2
            else:
                scope, area = "unlocated", []
        else:
            scope, area = None, []

        if observed:
            spatial_unit, spatial_id = "admin_dong", code
        elif level == "admin_dong":
            spatial_unit, spatial_id = "admin_dong_candidates", "|".join(res["candidates"]) or None
        elif level == "sigungu":
            spatial_unit, spatial_id = "sigungu", "|".join(sgg5) or None
        elif level == "sido":
            spatial_unit, spatial_id = "sido", "|".join(sido2) or None
        else:
            spatial_unit, spatial_id = level, None

        rec = {
            "label_id": f"HC-{excel_row:04d}",
            "source_dataset": "rain_casualty_2024_2025",
            "source_record_id": f"Sheet1!R{excel_row}",
            "bronze_file": args.bronze_file_label or str(src),
            "disaster_type": "HEAVY_RAIN",
            "event_id": f"HEAVY_RAIN_{ev_start.isoformat()}_{ev_end.isoformat()}" if ev_start else None,
            "event_name_raw": ev_name,
            "event_period_start": ev_start,
            "event_period_end": ev_end,
            "casualty_type": ctype,
            "person_count": persons,
            "deaths_count": None, "missing_persons_count": None, "injured_count": None, "deaths_or_missing_count": None,
            "sido_name": sido_full or sido_raw,
            "sigungu_name": sgg_raw,
            "eupmyeondong_raw": emd_raw,
            "occurred_date_raw": occ_raw,
            "occurred_time_raw": clean(r["발생시각"]) if not isinstance(r["발생시각"], dt.time) else r["발생시각"].strftime("%H:%M"),
            "incident_description": desc,
            "label_status": status,
            "unresolved_reason": reason,
            "include_in_total": include,
            "source_status_raw": status_raw,
            "confidence_grade": confidence_grade(status_raw),
            "officially_confirmed": bool(status_raw and status_raw.upper().startswith("OFFICIAL")
                                         and "not confirmed" not in (note or "")),
            "rain_relation_uncertain": bool(desc and "호우 관련성 불확실" in desc),
            "label_source": clean(r["출처"]),
            "label_source_ref": clean(r["페이지/URL"]),
            "source_note": note,
            "as_of_raw": clean(r["집계시각(as-of)"]),
            "label_available_at": parse_as_of(r["집계시각(as-of)"]),
            # 키 (observed일 때만 채움)
            "admin_dong_code": code,
            "admin_dong_name": ref.name_by_code.get(code) if code else None,
            "day": occ_day if observed else None,
            "admin_boundary_version": BOUNDARY_VERSION,
            "admin_code_reference_date": BOUNDARY_DATE,
            "spatial_mapping_method": res["method"] if res else None,
            "mapping_candidate_codes": "|".join(orig_candidates) if res else None,
            "mapping_source": "BND_ADM_DONG_PG(2025-06-30) + official_full_linkage_2024/2025"
                              + (" + manual override" if override is not None else "") if res else None,
            "mapping_evidence": override["evidence_sources"] if override is not None else None,
            "mapping_note": override.get("note") if override is not None else None,
            "is_estimated": override is not None,
            "label_spatial_unit": spatial_unit,
            "label_spatial_id": spatial_id,
            "label_temporal_resolution": "day" if occ_day else ("period" if occ_start else ("event_period" if ev_start else "unknown")),
            "label_period_start": p_start,
            "label_period_end": p_end,
            "label_unit_original": "persons",
            "label_definition": "호우 관련 인명피해 인원(사망/실종/부상 구분). 기록 없음은 0이 아님",
            "overlay_scope": scope,
            "overlay_area_codes": "|".join(area) if area else None,
            "processing_version": PROCESSING_VERSION,
        }
        rec[COUNT_COLUMN[ctype]] = persons
        rows.append(rec)

    rec_df = pd.DataFrame(rows)
    for c in ["deaths_count", "missing_persons_count", "injured_count", "deaths_or_missing_count", "person_count"]:
        rec_df[c] = rec_df[c].astype("Int32")
    for c in ["event_period_start", "event_period_end", "label_period_start", "label_period_end", "day"]:
        rec_df[c] = pd.to_datetime(rec_df[c]).dt.date

    # ------------------------------------------------ 동·일 집계 (observed만)
    obs = rec_df[rec_df["label_status"] == "observed"].copy()
    grade_rank = {"A_official": 1, "B_official_interim": 2, "C_media": 3, "U_unknown": 4}
    agg = (
        obs.groupby(["admin_dong_code", "day"], as_index=False)
        .agg(
            admin_dong_name=("admin_dong_name", "first"),
            deaths_count=("deaths_count", lambda s: s.sum(min_count=1)),
            missing_persons_count=("missing_persons_count", lambda s: s.sum(min_count=1)),
            injured_count=("injured_count", lambda s: s.sum(min_count=1)),
            deaths_or_missing_count=("deaths_or_missing_count", lambda s: s.sum(min_count=1)),
            casualty_count_total=("person_count", "sum"),
            record_count=("label_id", "size"),
            label_ids=("label_id", lambda s: "|".join(s)),
            event_ids=("event_id", lambda s: "|".join(sorted(set(s.dropna())))),
            label_sources=("label_source", lambda s: " | ".join(sorted(set(s.dropna())))),
            best_confidence_grade=("confidence_grade", lambda s: min(s, key=lambda g: grade_rank.get(g, 9))),
            all_officially_confirmed=("officially_confirmed", "all"),
            any_rain_relation_uncertain=("rain_relation_uncertain", "any"),
            any_estimated_mapping=("is_estimated", "any"),
            label_available_at=("label_available_at", "max"),
        )
    )
    agg.insert(0, "label_year", pd.to_datetime(agg["day"]).dt.year.astype("int32"))
    agg["human_damage_observed"] = 1
    agg["admin_boundary_version"] = BOUNDARY_VERSION
    agg["label_temporal_resolution"] = "day"
    agg["label_status"] = "observed"
    agg["processing_version"] = PROCESSING_VERSION

    # ------------------------------------------------ 저장
    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)
    qc = Path(args.qc_dir); qc.mkdir(parents=True, exist_ok=True)
    rec_df.to_parquet(out / "rain_casualty_records.parquet", index=False)
    rec_df.to_csv(out / "rain_casualty_records.csv", index=False, encoding="utf-8-sig")
    agg.to_parquet(out / "rain_casualty_dong_daily.parquet", index=False)
    agg.to_csv(out / "rain_casualty_dong_daily.csv", index=False, encoding="utf-8-sig")
    unresolved = rec_df[rec_df["label_status"] != "observed"]
    unresolved.to_csv(qc / "rain_casualty_unresolved.csv", index=False, encoding="utf-8-sig")

    # ------------------------------------------------ 검증 + 품질보고서
    checks = {}
    checks["no_personal_columns_in_outputs"] = not any(
        c in df.columns for df in (rec_df, agg) for c in PERSONAL_COLUMNS + ["gender_age", "resident_address"]
    )
    checks["record_count_preserved"] = len(rec_df) == len(raw)
    checks["observed_keys_present"] = bool(obs["admin_dong_code"].notna().all() and obs["day"].notna().all())
    checks["unresolved_keys_null"] = bool(unresolved["admin_dong_code"].isna().all() and unresolved["day"].isna().all())
    checks["observed_codes_in_gold_boundary"] = bool(obs["admin_dong_code"].isin(ref.bnd_codes).all())
    checks["observed_codes_are_8_digit_str"] = bool(obs["admin_dong_code"].str.fullmatch(r"\d{8}").all())
    checks["observed_day_within_event_period"] = bool(
        ((obs["day"] >= obs["event_period_start"]) & (obs["day"] <= obs["event_period_end"])).all()
    )
    checks["observed_code_in_reported_sigungu"] = bool(all(
        c[:5] in ref.sgg_prefixes(sd, sg) for c, sd, sg in zip(obs["admin_dong_code"], obs["sido_name"], obs["sigungu_name"])
    ))
    checks["dong_daily_key_unique"] = not agg.duplicated(["admin_dong_code", "day"]).any()
    inc = rec_df[rec_df["include_in_total"]]
    checks["persons_reconcile_observed_plus_unverified"] = int(inc["person_count"].sum()) == int(
        inc.loc[inc["label_status"] == "observed", "person_count"].sum()
        + inc.loc[inc["label_status"] == "unverified", "person_count"].sum()
    )
    checks["dong_daily_total_equals_observed_records"] = int(agg["casualty_count_total"].sum()) == int(obs["person_count"].sum())
    checks["all_overrides_applied"] = len(overrides_applied) == len(overrides)
    checks["overridden_records_marked_estimated"] = bool(
        rec_df.loc[rec_df["source_record_id"].isin(list(overrides)), "is_estimated"].all())

    def persons_by(df, cols):
        g = df.groupby(cols, dropna=False)["person_count"].sum().reset_index()
        return [{**{c: (None if pd.isna(v) else (v if not isinstance(v, (bool,)) else bool(v))) for c, v in zip(cols, row[:-1])},
                 "persons": int(row[-1])} for row in g.itertuples(index=False)]

    # 2025년 7월 호우: 합산포함=Y 지역분할 합계 vs 최종 합계(사망+실종 24, 합산포함=N) 교차검증
    ev = rec_df[rec_df["event_id"] == "HEAVY_RAIN_2025-07-16_2025-07-20"]
    y_dm = int(ev.loc[ev["include_in_total"] & ev["casualty_type"].isin(["death", "missing"]), "person_count"].sum())
    final_dm = ev.loc[(~ev["include_in_total"]) & (ev["casualty_type"] == "death_or_missing"), "person_count"]
    cross = {
        "event_id": "HEAVY_RAIN_2025-07-16_2025-07-20",
        "included_regional_death_plus_missing": y_dm,
        "final_national_death_or_missing": int(final_dm.iloc[0]) if len(final_dm) else None,
        "note": "지역분할 스냅샷(7.21 21시)과 최종 합계(8.17) 기준시각이 달라 차이가 날 수 있음. 인원을 조정하지 않음.",
    }

    report = {
        "processing_version": PROCESSING_VERSION,
        "created_at": dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec="seconds"),
        "input": {"path": str(src), "sheet": args.sheet, "sha256": sha256(src), "bronze_file_label": args.bronze_file_label,
                  "source_meta": json.loads(args.source_meta) if args.source_meta else None},
        "reference": {"boundary": f"{args.boundary_dbf} ({BOUNDARY_VERSION}, {len(ref.bnd_codes)} dongs)",
                      "linkage": [str(p) for p in args.linkage],
                      "linkage_rule": "2024·2025 연계표 행 중 Gold 경계(2025-06-30)에 있는 코드만 사용; 행정동명 일치 ∪ 법정동명 연계 후보가 정확히 1개일 때만 확정"},
        "rows": {"input": int(len(raw)), "silver_records": int(len(rec_df)), "dong_daily": int(len(agg))},
        "personal_columns_dropped": {"columns": PERSONAL_COLUMNS, "nonnull_values_in_input": personal_nonnull},
        "label_status_counts": rec_df["label_status"].value_counts().to_dict(),
        "unresolved_reason_counts": rec_df["unresolved_reason"].value_counts().to_dict(),
        "persons_by_status": persons_by(rec_df, ["label_status", "casualty_type"]),
        "persons_by_year_include": [
            {"year": int(y), "include_in_total": bool(i), "casualty_type": t, "persons": int(n)}
            for (y, i, t), n in rec_df.assign(y=pd.to_datetime(rec_df["event_period_start"]).dt.year)
            .groupby(["y", "include_in_total", "casualty_type"])["person_count"].sum().items()],
        "dong_mapping_audit": mapping_audit,
        "overrides": {"file": str(ov_path) if ov_path else None, "applied": overrides_applied},
        "cross_check": [cross],
        "checks": checks,
        "all_checks_passed": all(checks.values()),
        "limitations": [
            "원본은 드라이브 구글 시트(rain_casualty_2024_2025)를 xlsx로 내보낸 파일. 로컬 manifest의 원본 xlsx와 sha256이 다를 수 있음.",
            "행정동 확정 기준은 Gold와 같은 2025-06-30 경계. 2024년 사건도 이 경계 코드로 표시함 (해당 8건 코드는 2024 연계표에서도 동일).",
            "언론 보도는 보통 법정동 주소를 씀. 법정동이 여러 행정동에 걸치면(분할연계) 모호로 보고 키를 비움.",
            "단, 보도에 구체 위치가 있고 경계 확인으로 한 행정동이 확실한 경우 오버라이드 파일에 근거와 함께 기록해 확정함 (is_estimated=True).",
            "observed가 아닌 기록은 admin_dong_code/day가 NULL. 인원을 여러 동·일에 나누거나 복제하지 않음.",
            "'기록 없음'은 피해 0이 아님. 2025년 기록 상당수는 중간집계·언론 기반(confidence_grade 참고).",
            "incident_description/occurred_time_raw는 메타데이터이며 모델 입력으로 쓰지 않음.",
        ],
    }
    (qc / "rain_casualty_quality_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"rows": report["rows"], "label_status_counts": report["label_status_counts"],
                      "checks": checks}, ensure_ascii=False, indent=2))
    if not report["all_checks_passed"]:
        raise SystemExit("검증 실패: rain_casualty_quality_report.json 확인")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/bronze/labels/rain_casualty_2024_2025.xlsx")
    ap.add_argument("--sheet", default="Sheet1")
    ap.add_argument("--boundary-dbf", default="data/bronze/boundaries/BND_ADM_DONG_PG/BND_ADM_DONG_PG.dbf")
    ap.add_argument("--linkage", nargs="+", default=[
        "data/silver/mappings/ndms_sgis_crosswalk/official_full_linkage_2024.csv",
        "data/silver/mappings/ndms_sgis_crosswalk/official_full_linkage_2025.csv"])
    ap.add_argument("--out-dir", default="data/silver/labels/rain_casualty")
    ap.add_argument("--qc-dir", default="data/silver/quality_checks/labels")
    ap.add_argument("--bronze-file-label", default=None, help="계보에 기록할 Bronze 경로 (기본: --input)")
    ap.add_argument("--source-meta", default=None, help="원천 메타 JSON 문자열 (예: 드라이브 파일 ID)")
    ap.add_argument("--overrides", default="data/silver/mappings/rain_casualty_dong_overrides.csv",
                    help="근거가 있는 수동 행정동 확정 목록 (source_record_id, admin_dong_code, evidence_*)")
    build(ap.parse_args())


if __name__ == "__main__":
    main()
