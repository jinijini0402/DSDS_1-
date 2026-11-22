"""2024 heavy-rain death/missing count: assumption-based exploratory dataset.

Run from the project root. Original Bronze/Silver/Gold are read-only.
The zero label is inferred from the official annual count, not individually verified.
"""
import argparse
import calendar
import json
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path('data/gold/model_inputs/human_casualty_candidates')
SILVER = Path('data/silver/labels/rain_casualty/rain_casualty_records.parquet')
OUT = Path('data/gold/model_inputs/poisson_casualty_2024')
SOURCE = ('https://www.index.go.kr/unity/potal/eNara/sub/'
          'showStblGams3.do?freq=Y&idx_cd=F0240&period=N&stts_cd=F024001')
YEAR, OFFICIAL_TOTAL, N_DONG = 2024, 6, 3559
KEYS = ['admin_dong_code', 'observation_date']
TARGET = 'death_missing_count'
RAIN = ['rain_daily_total_mm', 'rain_1h_max_mm', 'rain_3h_max_mm',
        'rain_6h_max_mm', 'rain_12h_max_mm', 'rain_24h_max_mm']
# Fix a small feature set in advance; more variables are exploratory only.
MODEL_FEATURES = ['rain_1h_max_mm', 'rain_24h_max_mm']
QUALITY = ['hours_total', 'rain_valid_hours', 'rain_24h_valid_hours_of_day']
WEATHER_COLS = ['year', *KEYS, 'admin_dong_name', 'boundary_snapshot',
                *RAIN, 'temperature_mean_c', 'humidity_mean_pct',
                'pressure_mean_hpa', *QUALITY, 'human_label_status',
                'human_target_candidate']


def require(ok, message):
    if not ok:
        raise ValueError(message)


def date(series):
    return pd.to_datetime(series, errors='raise').dt.normalize()


def load_labels():
    r = pd.read_parquet(SILVER)
    starts = date(r['event_period_start'])
    r = r.loc[starts.dt.year.eq(YEAR) & r['disaster_type'].eq('HEAVY_RAIN')
              & r['officially_confirmed'].eq(True)
              & r['include_in_total'].eq(True)
              & r['casualty_type'].isin(['death', 'missing', 'death_or_missing'])].copy()
    r['person_count'] = pd.to_numeric(r['person_count'], errors='raise')
    n = r['person_count']
    require(n.notna().all() and (n > 0).all() and (n % 1 == 0).all(),
            'Non-positive, missing or non-integer official count')
    require(r['label_id'].is_unique, 'Duplicate official label ID')
    require(r['source_record_id'].is_unique, 'Duplicate source record ID')
    require(not r['rain_relation_uncertain'].eq(True).any(), 'Uncertain rain relationship')
    require(n.sum() == OFFICIAL_TOTAL, 'Official total does not reconcile to 6; stop')
    # These checks are specific to the reviewed 2024 source snapshot.
    require(set(r['label_id']) == {'HC-0002', 'HC-0003', 'HC-0004',
                                  'HC-0005', 'HC-0006', 'HC-0008'},
            'Official label inventory changed; re-review before inferring zeros')
    located = r.loc[r['label_status'].eq('observed')
                    & r['label_spatial_unit'].eq('admin_dong')
                    & r['label_temporal_resolution'].eq('day')].copy()
    located['observation_date'] = date(located['day'])
    located['admin_dong_code'] = located['admin_dong_code'].astype('string').str.strip()
    require(not located[KEYS].isna().any().any(), 'Missing located case keys')
    require(located['admin_dong_code'].str.fullmatch(r'\d{8}').all(), 'Invalid dong code')
    require(located['observation_date'].dt.year.eq(YEAR).all(), 'Case is outside 2024')
    require(located['person_count'].sum() == 5 and len(located) == 5,
            'Expected five located cases')
    unresolved = r.loc[~r['label_id'].isin(located['label_id'])]
    require(len(unresolved) == 1 and unresolved['person_count'].sum() == 1
            and unresolved['label_id'].iloc[0] == 'HC-0008',
            'Unresolved label inventory changed')
    # Exclude the whole country during the unresolved case window conservatively.
    u = unresolved.iloc[0]
    start, end = pd.Timestamp(u['label_period_start']), pd.Timestamp(u['label_period_end'])
    require((start, end) == (pd.Timestamp('2024-09-19'), pd.Timestamp('2024-09-21')),
            'Unresolved dates changed; re-review')
    labels = (located.groupby(KEYS, as_index=False)['person_count'].sum()
              .rename(columns={'person_count': TARGET}))
    labels[TARGET] = labels[TARGET].astype('Int64')
    require(len(labels) == 5, 'Unexpected number of distinct positive dong-days')
    return labels, start, end


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--assume-complete-official-register', action='store_true',
                        help='Explicitly accept inferred zero labels for documented official deaths/missing')
    args = parser.parse_args()
    if not args.assume_complete_official_register:
        parser.error('Review the annual-total reconciliation and use the explicit assumption flag.')

    labels, start, end = load_labels()
    source_total = 0
    excluded_unresolved = 0
    reference_dongs = None
    chunks, matched, all_positive = [], [], []
    monthly = []

    for month in range(1, 13):
        p = BASE / f'human_casualty_candidates_{YEAR}_{month:02d}.parquet'
        w = pd.read_parquet(p, columns=WEATHER_COLS)
        w['admin_dong_code'] = w['admin_dong_code'].astype('string').str.strip()
        w['observation_date'] = date(w['observation_date'])
        require(not w[KEYS].isna().any().any(), f'{p.name}: missing key')
        require(w['admin_dong_code'].str.fullmatch(r'\d{8}').all(), f'{p.name}: invalid code')
        require(not w.duplicated(KEYS).any(), f'{p.name}: duplicate key')
        require(w['year'].eq(YEAR).all() and w['observation_date'].dt.year.eq(YEAR).all()
                and w['observation_date'].dt.month.eq(month).all(), f'{p.name}: wrong period')
        require(w['boundary_snapshot'].astype(str).eq('2025-06-30').all(),
                f'{p.name}: boundary version changed')
        dongs = set(w['admin_dong_code'])
        days = calendar.monthrange(YEAR, month)[1]
        require(len(dongs) == N_DONG and len(w) == N_DONG * days
                and w['observation_date'].nunique() == days, f'{p.name}: incomplete grid')
        if reference_dongs is None:
            reference_dongs = dongs
        require(dongs == reference_dongs, f'{p.name}: dong set changed')
        source_total += len(w)
        w = w.merge(labels, on=KEYS, how='left', validate='one_to_one')
        positive = w[TARGET].notna()
        require(positive.eq(w['human_label_status'].eq('official_observed')).all(),
                f'{p.name}: candidate and source labels disagree')
        require(w.loc[positive, TARGET].eq(w.loc[positive, 'human_target_candidate']).all(),
                f'{p.name}: candidate count differs from official death/missing count')
        require(not w['human_label_status'].eq('nonofficial').any(),
                f'{p.name}: unexpected nonofficial record; review')
        matched.append(w.loc[positive, KEYS])
        unresolved = w['observation_date'].between(start, end)
        require(not (positive & unresolved).any(), 'Located case overlaps unresolved exclusion')
        excluded_unresolved += int(unresolved.sum())
        w['target_source'] = 'inferred_zero_annual_register'
        w.loc[positive, 'target_source'] = 'official_observed'
        w.loc[unresolved, 'target_source'] = 'excluded_unresolved_period'
        w[TARGET] = w[TARGET].fillna(0).astype('Int64')
        w.loc[unresolved, TARGET] = pd.NA
        # Quality screen only the two pre-specified model features. No imputation.
        finite_rain = np.isfinite(w[MODEL_FEATURES].to_numpy(dtype=float)).all(axis=1)
        nonnegative_rain = w[MODEL_FEATURES].ge(0).all(axis=1)
        w['weather_quality_ok'] = (w[QUALITY].eq(24).all(axis=1)
                                   & finite_rain & nonnegative_rain)
        w['model_eligible'] = w[TARGET].notna() & w['weather_quality_ok']
        all_positive.append(w.loc[positive].copy())
        model = w.loc[w['model_eligible']].copy()
        chunks.append(model)
        stats = {'month': month, 'source_rows': len(w),
                 'excluded_unresolved': int(unresolved.sum()),
                 'excluded_weather_quality': int((~unresolved & ~w['weather_quality_ok']).sum()),
                 'eligible_rows': len(model),
                 'eligible_positive': int(model[TARGET].gt(0).sum()),
                 'eligible_inferred_zero': int(model[TARGET].eq(0).sum())}
        monthly.append(stats)
        print(f"[완료] {YEAR}-{month:02d} | 후보 {len(model):,}행 | 양수 {stats['eligible_positive']}건")

    coverage = labels[KEYS].merge(pd.concat(matched, ignore_index=True), on=KEYS,
                                  how='outer', indicator=True, validate='one_to_one')
    require(coverage['_merge'].eq('both').all(), 'Missing or unexpected matched official cases')
    require(source_total == N_DONG * 366, 'Incomplete 2024 coverage')
    require(excluded_unresolved == N_DONG * 3, 'Unexpected exclusion size')
    ds = pd.concat(chunks, ignore_index=True)
    positives = pd.concat(all_positive, ignore_index=True)
    require(not ds.duplicated(KEYS).any(), 'Duplicate final keys')
    n_positive = int(ds[TARGET].gt(0).sum())
    n_zero = int(ds[TARGET].eq(0).sum())
    qa = {
        'status': 'EXPLORATORY_ASSUMED_ZERO_NOT_VALIDATED_FOR_GENERALIZATION',
        'target': TARGET, 'year': YEAR, 'unit': 'persons per administrative dong-day',
        'scope': 'officially recorded heavy-rain deaths and missing persons; excludes injuries',
        'annual_total_source': SOURCE, 'official_annual_total': OFFICIAL_TOTAL,
        'annual_reconciliation': {'located_persons': 5, 'unresolved_persons': 1},
        'zero_assumption': 'Annual register and reviewed non-overlapping records comprise all recorded cases. Zeros are inferred, not independently verified.',
        'source_rows': source_total, 'excluded_unresolved_rows': excluded_unresolved,
        'excluded_unresolved_dates': [str(start.date()), str(end.date())],
        'excluded_weather_quality_rows': sum(x['excluded_weather_quality'] for x in monthly),
        'eligible_rows': len(ds), 'eligible_positive_rows': n_positive,
        'eligible_inferred_zero_rows': n_zero,
        'eligible_sum_target': int(ds[TARGET].sum()),
        'positive_lost_to_weather_quality': 5 - n_positive,
        'model_features_prespecified': MODEL_FEATURES,
        'quality_rule': '24 observed hours, 24 valid rain hours, 24 valid rolling-24h values; finite nonnegative model features',
        'limitations': ['Five located positives in one storm; no credible independent positive-event validation in 2024.',
                        'Daily features are retrospective and may include rainfall after the incident.',
                        'No population or exposure adjustment; not an individual risk estimate.',
                        '2025 is not assigned zeros and is not a validated test set.']}
    require(source_total == len(ds) + excluded_unresolved + qa['excluded_weather_quality_rows'],
            'Row reconciliation failed')
    OUT.mkdir(parents=True, exist_ok=True)
    ds.to_parquet(OUT / 'poisson_training_2024_assumed_zero.parquet', index=False)
    pd.DataFrame(monthly).to_csv(OUT / 'monthly_summary.csv', index=False, encoding='utf-8-sig')
    positives.to_csv(OUT / 'official_positive_cases.csv', index=False, encoding='utf-8-sig')
    (OUT / 'poisson_training_2024.qa.json').write_text(json.dumps(qa, ensure_ascii=False, indent=2), encoding='utf-8')
    print('\n===== 포아송 학습 후보 QA =====')
    print(json.dumps(qa, ensure_ascii=False, indent=2))
    print('\n저장 위치:', OUT)
    if n_positive != 5:
        print('주의: 품질 검증에서 양수 표본이 제외됨. 모델 학습 전 원인 확인 필요.')


if __name__ == '__main__':
    main()
