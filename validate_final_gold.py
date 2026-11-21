"""Final Gold QA: missing, duplicate keys, numeric anomalies and label statuses.
Run: python validate_final_gold.py path/to/file.parquet
Requires: pandas, pyarrow, numpy
"""
import json
import sys
from pathlib import Path
import numpy as np
import pandas as pd

if len(sys.argv) < 2:
    raise SystemExit('Usage: python validate_final_gold.py <file.parquet>')
path=Path(sys.argv[1]); df=pd.read_parquet(path)
print(f'\nFile: {path}\nRows: {len(df):,}, Columns: {len(df.columns)}')
print('\nColumns:', ', '.join(map(str,df.columns)))

missing=pd.DataFrame({'column':df.columns, 'dtype':df.dtypes.astype(str).values,'missing_count':df.isna().sum().values,'missing_pct':(df.isna().mean()*100).round(2).values}).sort_values(['missing_count','column'],ascending=[False,True]); print('\n[MISSING]\n',missing.to_string(index=False))
print('\n[DUPLICATES] Exact row duplicates:',int(df.duplicated().sum()))
# Candidate names vary across sources; only report composite key if unequivocally identified
cols=set(df.columns)
code_candidates=['sgis_admin_dong_code','sgis_dong_code','admin_dong_code','adm_cd','dong_code','adm_code','admin_code','sgis_code','행정동코드']
date_candidates=['date','date_kst','observation_date_kst','day','ymd','base_date','damage_date','observation_date','날짜','일자']
code=next((c for c in code_candidates if c in cols),None)
date=next((c for c in date_candidates if c in cols),None)
if code and date:
    bad=df.duplicated(subset=[code,date],keep=False)
    print(f'Composite key [{code}, {date}]: {int(bad.sum())} rows involved, {int(df.duplicated(subset=[code,date]).sum())} extra duplicates')
    if bad.any(): print(df.loc[bad,[code,date]].head(20).to_string(index=False))
else:
    print('Composite key NOT auto-identified. Verify columns manually. Code/date candidates:',code,date)
print('\n[NUMERIC ANOMALIES]')
num=df.select_dtypes(include='number').columns
issues=[]
for c in num:
    x=pd.to_numeric(df[c],errors='coerce'); finite=x[np.isfinite(x)]
    nonfinite=int((~np.isfinite(x.to_numpy(dtype=float,na_value=np.nan)) & x.notna().to_numpy()).sum())
    # Only identify negative as suspicious for features semantically required nonnegative.
    nonneg=any(s in c.lower() for s in ('damage','loss','amount','cost','casual','death','missing_person','injur','rain','precip','rn','피해','강수','사망','부상')) and not any(s in c.lower() for s in ('rate','change','temp','ta','lat','lon','diff','delta','pressure'))
    neg=int((x<0).sum()) if nonneg else None
    q1=finite.quantile(.25) if len(finite) else np.nan
    q3=finite.quantile(.75) if len(finite) else np.nan
    iqr=q3-q1
    # IQR is not reliable for zero-inflated losses: report high quantiles rather than deleting values.
    issues.append({'column':c,'count':int(x.notna().sum()),'min':float(finite.min()) if len(finite) else None,'p99':float(finite.quantile(.99)) if len(finite) else None,'max':float(finite.max()) if len(finite) else None,'infinite_count':nonfinite,'negative_nonnegative_variable':neg})
print(pd.DataFrame(issues).to_string(index=False))
print('\n[CATEGORICAL LABEL STATUS]')
for c in df.columns:
    if ('status' in c.lower() or 'label' in c.lower() or 'official' in c.lower()) and (df[c].dtype=='object' or isinstance(df[c].dtype,pd.CategoricalDtype) or pd.api.types.is_string_dtype(df[c])):
        print(c,':',df[c].value_counts(dropna=False).head(20).to_dict())

out=path.parent/(path.stem+'_validation')
out.mkdir(exist_ok=True)
missing.to_csv(out/'missing_summary.csv',index=False,encoding='utf-8-sig')
pd.DataFrame(issues).to_csv(out/'numeric_anomalies.csv',index=False,encoding='utf-8-sig')
report={'file':str(path),'rows':len(df),'columns':len(df.columns),'exact_duplicate_rows':int(df.duplicated().sum()),'composite_key_columns':[code,date] if code and date else None,'composite_key_duplicates':int(df.duplicated(subset=[code,date]).sum()) if code and date else None,'remarks':['Unobserved human cases are NOT confirmed negatives.','Outliers should be reviewed before removing; extreme rain damage may be valid.','Verify damage amount units and label source.']}
(out/'summary.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
print('\nSaved reports:',out)
