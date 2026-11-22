"""Exploratory 2024 heavy-rain death/missing Poisson regression.
All scores are IN-SAMPLE, conditional on inferred-zero labels. No held-out test.
Run from heavy-rain-prediction root after prepare_poisson_training_data.py.
"""
import argparse
import json
import warnings
from pathlib import Path

import joblib
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sklearn
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import PoissonRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_poisson_deviance
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

BASE = Path('data/gold/model_inputs/poisson_casualty_2024')
OUTPUT = Path('results/human_casualty/poisson_2024')
FEATURES = ['rain_1h_max_mm', 'rain_24h_max_mm']
TARGET = 'death_missing_count'
KEYS = ['admin_dong_code', 'observation_date']
STATUS = 'EXPLORATORY_ASSUMED_ZERO_NOT_VALIDATED_FOR_GENERALIZATION'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def fit_model(x, y):
    model = Pipeline([
        ('scaler', StandardScaler()),
        ('poisson', PoissonRegressor(alpha=0.0, max_iter=1000, tol=1e-12))
    ])
    with warnings.catch_warnings():
        warnings.simplefilter('error', ConvergenceWarning)
        model.fit(x, y)
    pred = model.predict(x)
    require(np.isfinite(pred).all() and (pred > 0).all(),
            'Non-finite or non-positive predictions')
    # Unpenalized Poisson with an intercept should match the observed total.
    # Check the score relative to the very small number of nonzero outcomes.
    xs = model.named_steps['scaler'].transform(x)
    score = np.r_[(pred-y).sum(), xs.T @ (pred-y)]
    rel_score = float(np.max(np.abs(score)) / y.sum())
    require(rel_score < 0.01, f'Optimization score check failed: {rel_score:.6g}')
    return model, pred, rel_score


def scores(y, pred, name):
    return {'model': name, 'evaluation': 'IN_SAMPLE_ASSUMED_ZERO',
            'MAE': float(mean_absolute_error(y, pred)),
            'RMSE': float(np.sqrt(mean_squared_error(y, pred))),
            'mean_poisson_deviance': float(mean_poisson_deviance(y, pred)),
            'observed_total': float(y.sum()), 'predicted_total': float(pred.sum()),
            'positive_mean_prediction': float(pred[y > 0].mean()),
            'inferred_zero_mean_prediction': float(pred[y == 0].mean())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', type=Path, default=BASE)
    parser.add_argument('--output-dir', type=Path, default=OUTPUT)
    args = parser.parse_args()
    qa = json.loads((args.input_dir / 'poisson_training_2024.qa.json').read_text())
    require(qa['status'] == STATUS and qa['year'] == 2024 and qa['target'] == TARGET,
            'Unexpected dataset QA, target or year; review before fitting')
    require(qa['model_features_prespecified'] == FEATURES, 'Feature specification changed')
    cols = ['year', *KEYS, 'admin_dong_name', 'target_source', TARGET, *FEATURES]
    df = pd.read_parquet(args.input_dir / 'poisson_training_2024_assumed_zero.parquet', columns=cols)
    dates = pd.to_datetime(df['observation_date'], errors='raise')
    require(df['year'].eq(2024).all() and dates.dt.year.eq(2024).all(), 'Non-2024 records')
    require(not dates.between('2024-09-19', '2024-09-21').any(), 'Unresolved dates present')
    require(not df[KEYS].isna().any().any() and not df.duplicated(KEYS).any(), 'Invalid keys')
    y = df[TARGET].to_numpy(dtype=float)
    x = df[FEATURES].astype(float)
    require(np.isfinite(y).all() and np.isin(y, [0, 1]).all(), 'Unexpected target values')
    require(len(df) == qa['eligible_rows'] and int((y > 0).sum()) == 5
            and int((y == 0).sum()) == qa['eligible_inferred_zero_rows'], 'QA row mismatch')
    require(df.loc[y > 0, 'target_source'].eq('official_observed').all() and
            df.loc[y == 0, 'target_source'].eq('inferred_zero_annual_register').all(),
            'Invalid target provenance')
    require(np.isfinite(x.to_numpy()).all() and x.ge(0).all().all(), 'Invalid rainfall features')
    require(x.std().gt(0).all(), 'Constant feature')
    mean = float(y.mean())
    baseline = np.full(len(y), mean)
    model, pred, score_check = fit_model(x, y)
    metrics = pd.DataFrame([scores(y, baseline, 'Intercept-only mean'),
                            scores(y, pred, 'Poisson regression')])
    dev_base, dev_model = metrics['mean_poisson_deviance'].tolist()
    improvement = 1 - dev_model/dev_base
    scaler, reg = model.named_steps['scaler'], model.named_steps['poisson']
    beta = reg.coef_ / scaler.scale_
    raw_intercept = float(reg.intercept_ - beta @ scaler.mean_)
    coeff = pd.DataFrame({'feature': FEATURES, 'standardized_coefficient': reg.coef_,
                          'coefficient_per_mm': beta, 'expected_count_ratio_per_10mm': np.exp(10*beta)})
    cases = df.loc[y > 0, [*KEYS, 'admin_dong_name', *FEATURES, TARGET]].copy()
    cases['fitted_expected_count'] = pred[y > 0]
    cases['baseline_expected_count'] = mean
    # Empirical risk-group calibration (descriptive, not held-out validation).
    band = pd.cut(x[FEATURES[1]], [-np.inf, 10, 30, 60, 100, 150, 200, np.inf], right=False)
    groups = pd.DataFrame({'rain_24h_band_mm': band, 'actual': y, 'fitted': pred,
                           'baseline': baseline}).groupby('rain_24h_band_mm', observed=True)
    bands = groups.agg(dong_days=('actual','size'), recorded_death_missing=('actual','sum'),
                       fitted_expected_total=('fitted','sum'), baseline_expected_total=('baseline','sum')).reset_index()
    bands['rain_24h_band_mm'] = bands['rain_24h_band_mm'].astype(str)
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(out / 'model_metrics_in_sample.csv', index=False)
    coeff.to_csv(out / 'poisson_coefficients.csv', index=False)
    cases.to_csv(out / 'official_positive_case_fitted_values.csv', index=False, encoding='utf-8-sig')
    bands.to_csv(out / 'rainfall_band_summary.csv', index=False)
    fit_rows = df[[*KEYS, TARGET, 'target_source']].copy()
    fit_rows['fitted_expected_count'] = pred
    fit_rows.to_parquet(out / 'fitted_values_2024.parquet', index=False)
    joblib.dump({'pipeline': model, 'features': FEATURES, 'target': TARGET,
                 'status': STATUS, 'evaluation': 'IN_SAMPLE_ONLY', 'sklearn_version': sklearn.__version__},
                out / 'poisson_model.joblib')
    report = {'status': STATUS, 'evaluation': 'IN_SAMPLE_ONLY', 'rows': len(y), 'positive_rows': 5,
              'inferred_zero_rows': int((y == 0).sum()), 'observed_sum': float(y.sum()),
              'baseline_rate': mean, 'poisson_fitted_sum': float(pred.sum()),
              'deviance_reduction_fraction_in_sample': float(improvement),
              'raw_intercept': raw_intercept, 'standardized_intercept': float(reg.intercept_),
              'iterations': int(reg.n_iter_), 'relative_score_check': score_check,
              'feature_correlation': float(x.corr().iloc[0,1]),
              'scikit_learn_version': sklearn.__version__,
              'limits': qa['limitations'] + ['All zero labels are inferred; coefficient estimates are unstable with five cases.',
                  'In-sample deviance reduction does not establish out-of-sample performance or causal effects.']}
    (out / 'model_fit_qa.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    ax = bands.set_index('rain_24h_band_mm')[['recorded_death_missing','fitted_expected_total','baseline_expected_total']].plot.bar(figsize=(10,5))
    ax.set_title('2024 recorded vs fitted counts by 24h rainfall (in-sample)')
    ax.set_xlabel('Maximum 24h rainfall (mm)'); ax.set_ylabel('Total deaths and missing persons')
    plt.xticks(rotation=30, ha='right'); plt.tight_layout()
    plt.savefig(out / 'rainfall_band_observed_vs_fitted.png', dpi=180); plt.close()
    print('\n===== 모델 적합 결과 (학습 데이터 내 평가) =====')
    print(metrics.to_string(index=False))
    print('\n===== 회귀 계수 ====='); print(coeff.to_string(index=False))
    print('\n===== 공식 피해 5건의 적합값 ====='); print(cases.to_string(index=False))
    print('\n===== 모델 적합 QA ====='); print(json.dumps(report, ensure_ascii=False, indent=2))
    print('\n저장 위치:', out)
    print('주의: 독립 사건 검증 또는 미래 예측 성능을 의미하지 않습니다.')


if __name__ == '__main__':
    main()
