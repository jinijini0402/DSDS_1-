"""Exploratory in-sample feature ablation and binary/count model comparison.
Run from project root after prepare_poisson_training_data.py. Existing results are untouched.
All zero labels are inferred. No independent positive-event validation is available.
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
from sklearn.linear_model import LogisticRegression, PoissonRegressor
from sklearn.metrics import (average_precision_score, roc_auc_score, log_loss,
                             brier_score_loss, mean_absolute_error,
                             mean_squared_error, mean_poisson_deviance,
                             precision_recall_curve)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

BASE = Path('data/gold/model_inputs/poisson_casualty_2024')
OUTPUT = Path('results/human_casualty/model_comparison_2024')
STATUS = 'EXPLORATORY_ASSUMED_ZERO_NOT_VALIDATED_FOR_GENERALIZATION'
TARGET = 'death_missing_count'
KEYS = ['admin_dong_code', 'observation_date']
DAILY = 'rain_daily_total_mm'
H1 = 'rain_1h_max_mm'
H24 = 'rain_24h_max_mm'
FEATURES = [DAILY, H1, H24]
SPECS = [
    ('M1', 'Poisson: daily total', [DAILY], 'poisson'),
    ('M2', 'Poisson: 1h max', [H1], 'poisson'),
    ('M3', 'Poisson: 24h max', [H24], 'poisson'),
    ('M4', 'Poisson: 1h + 24h', [H1, H24], 'poisson'),
    ('M5', 'Logistic: 1h + 24h', [H1, H24], 'logistic'),
]
EVAL = 'IN_SAMPLE_ASSUMED_ZERO'


def require(ok, message):
    if not ok:
        raise ValueError(message)


def load_data(folder):
    qa = json.loads((folder / 'poisson_training_2024.qa.json').read_text(encoding='utf-8'))
    require(qa['status'] == STATUS and qa['year'] == 2024 and qa['target'] == TARGET,
            'Unexpected input QA; review before fitting.')
    require(qa['model_features_prespecified'] == [H1, H24], 'Original feature specification changed.')
    cols = ['year', *KEYS, 'admin_dong_name', 'target_source', TARGET, *FEATURES,
            'hours_total', 'rain_valid_hours', 'rain_24h_valid_hours_of_day']
    df = pd.read_parquet(folder / 'poisson_training_2024_assumed_zero.parquet', columns=cols)
    df['observation_date'] = pd.to_datetime(df['observation_date'], errors='raise').dt.normalize()
    df['admin_dong_code'] = df['admin_dong_code'].astype('string').str.strip()
    require(not df[KEYS].isna().any().any() and not df.duplicated(KEYS).any(), 'Invalid dong-day keys.')
    require(df['year'].eq(2024).all() and df['observation_date'].dt.year.eq(2024).all(), 'Unexpected year.')
    require(not df['observation_date'].between('2024-09-19','2024-09-21').any(), 'Excluded period found.')
    y = df[TARGET].to_numpy(dtype=float)
    require(np.isin(y, [0, 1]).all(), 'Expected binary observed count support (0/1).')
    require(len(df) == qa['eligible_rows'] and (y == 1).sum() == qa['eligible_positive_rows'] == 5
            and (y == 0).sum() == qa['eligible_inferred_zero_rows'], 'QA counts changed.')
    require(df.loc[y == 1, 'target_source'].eq('official_observed').all() and
            df.loc[y == 0, 'target_source'].eq('inferred_zero_annual_register').all(), 'Invalid target provenance.')
    require(df[['hours_total','rain_valid_hours','rain_24h_valid_hours_of_day']].eq(24).all().all(),
            'Original weather quality filter not satisfied.')
    df[FEATURES] = df[FEATURES].astype(float)
    valid = np.isfinite(df[FEATURES].to_numpy()).all(axis=1) & df[FEATURES].ge(0).all(axis=1).to_numpy()
    # A common complete-case cohort for all models. Never drop rows separately per feature.
    excluded = int((~valid).sum())
    require(not ((~valid) & (y == 1)).any(), 'Additional feature screening would remove a positive. Stop and review.')
    df = df.loc[valid].reset_index(drop=True)
    require(df[FEATURES].std().gt(0).all(), 'Constant feature.')
    return df, qa, excluded


def fit_model(x, y, family):
    if family == 'poisson':
        estimator = PoissonRegressor(alpha=0.0, max_iter=2000, tol=1e-12)
    else:
        # No balancing/resampling: preserve the assumed population event rate.
        # C=inf disables regularization (scikit-learn >=1.2).
        estimator = LogisticRegression(C=np.inf, solver='lbfgs', max_iter=2000, tol=1e-12)
    model = Pipeline([('scaler', StandardScaler()), ('regressor', estimator)])
    with warnings.catch_warnings():
        warnings.simplefilter('error', ConvergenceWarning)
        model.fit(x, y)
    fitted = model.predict(x) if family == 'poisson' else model.predict_proba(x)[:, 1]
    require(np.isfinite(fitted).all() and (fitted >= 0).all(), 'Invalid fitted values.')
    if family == 'poisson':
        require((fitted > 0).all(), 'Nonpositive Poisson expectations.')
    else:
        require((fitted <= 1).all(), 'Invalid probabilities.')
    # A small averaged optimizer gradient is insufficient with only five positives.
    xs = model.named_steps['scaler'].transform(x)
    residual = fitted - y
    score = np.r_[residual.sum(), xs.T @ residual]
    relative_score = float(np.max(np.abs(score)) / y.sum())
    require(relative_score < 0.01, f'{family}: optimization score check failed ({relative_score:.6g}); do not interpret this fit.')
    return model, fitted, relative_score


def count_scores(y, expected):
    return {'MAE': float(mean_absolute_error(y, expected)),
            'RMSE': float(np.sqrt(mean_squared_error(y, expected))),
            'mean_poisson_deviance': float(mean_poisson_deviance(y, expected)),
            'observed_total': float(y.sum()), 'fitted_total': float(expected.sum()),
            'positive_mean_expected_count': float(expected[y > 0].mean())}


def event_scores(z, probability):
    # Threshold-free metrics: a 0.5 cutoff is unhelpful at this prevalence.
    # Include ties at the 99th-percentile boundary and report the actual fraction.
    cutoff = float(np.quantile(probability, .99))
    selected = probability >= cutoff
    return {'average_precision': float(average_precision_score(z, probability)),
            'ROC_AUC': float(roc_auc_score(z, probability)),
            'log_loss': float(log_loss(z, probability, labels=[0, 1])),
            'brier_score': float(brier_score_loss(z, probability)),
            'mean_fitted_event_probability': float(probability.mean()),
            'positive_mean_event_probability': float(probability[z == 1].mean()),
            'inferred_zero_mean_event_probability': float(probability[z == 0].mean()),
            'top_1pct_score_threshold': cutoff,
            'top_1pct_including_ties_rows': int(selected.sum()),
            'selected_fraction': float(selected.mean()),
            'positive_cases_in_selected': int(z[selected].sum()),
            'selected_recall': float(z[selected].sum() / z.sum())}


def poisson_event_probability(expected):
    return -np.expm1(-expected)


def run_comparison(df, qa, excluded, out):
    out.mkdir(parents=True, exist_ok=True)
    (out / 'models').mkdir(exist_ok=True)
    y = df[TARGET].to_numpy(dtype=float)
    z = (y > 0).astype(int)
    require(z.sum() == 5 and z.min() == 0, 'Expected five positive cases and nonempty background.')
    rate = float(y.mean())
    count_baseline = np.full(len(y), rate)
    # Separate intercept-only Bernoulli baseline for event metrics: exactly empirical prevalence.
    event_baseline = np.full(len(y), float(z.mean()))
    count_metrics = [{'model_id':'M0', 'model':'Intercept-only mean', 'features':'none',
                      'evaluation': EVAL, **count_scores(y,count_baseline)}]
    event_metrics = [{'model_id':'M0', 'model':'Constant event prevalence', 'features':'none',
                      'evaluation': EVAL, **event_scores(z,event_baseline)}]
    coefficients, diagnostics = [], []
    case_rows = df.loc[z == 1, [*KEYS,'admin_dong_name',TARGET,*FEATURES]].copy()
    case_rows['M0_expected_count'] = rate
    case_rows['M0_event_probability'] = float(z.mean())
    curves = {}
    for model_id, name, features, family in SPECS:
        print(f'[학습] {model_id} | {name}', flush=True)
        model, fitted, relative_score = fit_model(df[features], y if family == 'poisson' else z, family)
        if family == 'poisson':
            count_metrics.append({'model_id':model_id,'model':name,'features':' + '.join(features),
                                  'evaluation':EVAL, **count_scores(y,fitted)})
            probability = poisson_event_probability(fitted)
            case_rows[f'{model_id}_expected_count'] = fitted[z == 1]
        else:
            probability = fitted
        event_metrics.append({'model_id':model_id,'model':name,'features':' + '.join(features),
                              'evaluation':EVAL, **event_scores(z,probability)})
        case_rows[f'{model_id}_event_probability'] = probability[z == 1]
        if model_id in ('M4','M5'):
            precision, recall, _ = precision_recall_curve(z, probability)
            curves[model_id] = (recall,precision)
        scaler = model.named_steps['scaler']
        reg = model.named_steps['regressor']
        coef_std = np.asarray(reg.coef_).reshape(-1)
        beta = coef_std / scaler.scale_
        raw_intercept = float(np.asarray(reg.intercept_).reshape(-1)[0] - beta @ scaler.mean_)
        for f, a, b in zip(features,coef_std,beta):
            coefficients.append({'model_id':model_id,'model':name,'family':family,'feature':f,
                                 'standardized_coefficient':float(a),'coefficient_per_mm':float(b),
                                 'ratio_per_10mm':float(np.exp(10*b)),
                                 'ratio_meaning':'expected_count_ratio' if family=='poisson' else 'odds_ratio',
                                 'raw_intercept':raw_intercept})
        diagnostics.append({'model_id':model_id,'family':family,'relative_score_check':relative_score,
                            'iterations':int(np.asarray(reg.n_iter_).reshape(-1)[0]),'raw_intercept':raw_intercept})
        joblib.dump({'pipeline':model,'features':features,'target':TARGET if family=='poisson' else 'death_missing_occurred',
                     'target_definition':'official count' if family=='poisson' else 'death_missing_count > 0',
                     'status':STATUS,'evaluation':EVAL,'sklearn_version':sklearn.__version__},out/'models'/f'{model_id}_{family}.joblib')
    count = pd.DataFrame(count_metrics)
    event = pd.DataFrame(event_metrics)
    count['deviance_reduction_vs_M0_pct'] = 100*(1-count['mean_poisson_deviance']/count.loc[0,'mean_poisson_deviance'])
    event['AP_lift_vs_prevalence'] = event['average_precision']/float(z.mean())
    coefs = pd.DataFrame(coefficients)
    count.to_csv(out/'poisson_feature_comparison.csv',index=False)
    event.to_csv(out/'event_model_comparison.csv',index=False)
    coefs.to_csv(out/'model_coefficients.csv',index=False)
    case_rows.to_csv(out/'official_positive_case_comparison.csv',index=False,encoding='utf-8-sig')
    # Baseline is saved as parameters, not a fitted pipeline (no features).
    (out/'models'/'M0_constant.json').write_text(json.dumps({'expected_count':rate,'event_probability':float(z.mean()),'status':STATUS},indent=2),encoding='utf-8')
    report = {'status':STATUS,'evaluation':'IN_SAMPLE_ONLY','rows':len(df),'original_eligible_rows':qa['eligible_rows'],
              'additional_common_feature_exclusions':excluded,'positive_rows':int(z.sum()),
              'inferred_zero_rows':int((z==0).sum()),'event_prevalence':float(z.mean()),
              'features':FEATURES,'preprocessing':'common complete-case cohort; standardization for each fitted model',
              'class_weight':None,'resampling':False,'regularization':'none',
              'diagnostics':diagnostics,'sklearn_version':sklearn.__version__,
              'limits':qa['limitations']+['All zero labels are inferred from the annual register.',
                'Feature comparisons are exploratory, not feature selection validated on held-out data.',
                'AP, ROC-AUC, log loss, Brier score and top-score recall are all in-sample; only five positive cases.',
                'Poisson event probability is 1-exp(-lambda), logistic predicts P(count>0); fitted probabilities are not validated risk estimates.',
                'High ROC-AUC or AP lift is not predictive validation. No class-weighting or oversampling; no confidence intervals or significance claims.',
                'M0 count baseline uses mean count; M0 event baseline uses observed prevalence.',
                'Top-1% rankings include boundary ties; M0 therefore selects all rows.']}
    # Plots: separate units and tasks; AP is log-scaled because prevalence is tiny.
    ax = count.plot.bar(x='model_id',y='mean_poisson_deviance',legend=False,figsize=(8,4.5))
    ax.set_title('Poisson feature comparison (in-sample)'); ax.set_ylabel('Mean Poisson deviance (lower is better)'); ax.set_xlabel('Model'); plt.xticks(rotation=0); plt.tight_layout(); plt.savefig(out/'poisson_feature_deviance.png',dpi=180); plt.close()
    ax = event.plot.bar(x='model_id',y='average_precision',legend=False,figsize=(8,4.5),logy=True)
    ax.axhline(float(z.mean()),linestyle='--',label='Event prevalence'); ax.legend(); ax.set_title('Event ranking comparison (in-sample)'); ax.set_ylabel('Average precision (log scale)'); ax.set_xlabel('Model'); plt.xticks(rotation=0); plt.tight_layout(); plt.savefig(out/'event_average_precision.png',dpi=180); plt.close()
    fig,ax=plt.subplots(figsize=(8,5))
    for mid,(recall,precision) in curves.items():
        ax.step(recall,precision,where='post',label=mid)
    ax.axhline(float(z.mean()),linestyle='--',label='Event prevalence'); ax.set_yscale('log'); ax.set_ylim(float(z.mean())/2,1.05); ax.set_xlabel('Recall'); ax.set_ylabel('Precision (log scale)'); ax.set_title('Poisson vs logistic precision-recall (in-sample)'); ax.legend(); fig.tight_layout(); fig.savefig(out/'poisson_vs_logistic_pr.png',dpi=180); plt.close(fig)
    (out/'comparison_qa.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print('\n===== 포아송 Feature 조합별 적합도 ====='); print(count.to_string(index=False,float_format=lambda v:f'{v:.9g}'))
    print('\n===== 피해 발생 여부 모델 비교 ====='); print(event.to_string(index=False,float_format=lambda v:f'{v:.9g}'))
    print('\n===== 회귀계수 (로지스틱은 Odds Ratio) ====='); print(coefs.to_string(index=False))
    print('\n===== 실제 피해 5건의 모델별 적합값 ====='); print(case_rows.to_string(index=False,float_format=lambda v:f'{v:.9g}'))
    print('\n===== 모델 비교 QA ====='); print(json.dumps(report,ensure_ascii=False,indent=2))
    print('\n저장 위치:',out)
    print('주의: 모든 결과는 학습 데이터 내 탐색적 적합도입니다. 독립 사건 예측 성능이 아닙니다.')
    return count,event,report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir',type=Path,default=BASE)
    parser.add_argument('--output-dir',type=Path,default=OUTPUT)
    args=parser.parse_args()
    df,qa,excluded=load_data(args.input_dir)
    run_comparison(df,qa,excluded,args.output_dir)

if __name__=='__main__':
    main()
