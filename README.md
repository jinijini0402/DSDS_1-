# Heavy Rain Damage Prediction — 2024–2025 전국 호우 피해 데이터 파이프라인

> **과제 제출 준비용 브랜치** · Bronze → Silver → Gold · 행정동/시간 단위 기상 자료 및 일별 피해 Label  
> **상태:** 메달리온 주요 데이터 생성 완료 / 모델 학습·평가 및 완전 재현성 검증은 진행 필요

## 개요

기상청 AWS 관측자료와 재난 피해 기록을 수집해 **원본(Bronze) → 정제·행정동 정합(Silver) → 분석용 데이터(Gold)** 로 구성합니다. 기상 Feature는 시간별로 구축하고, 물적·인적 피해 Label은 **실제 관측된 일자/지역 단위**로 보존합니다. 일별 피해량을 시간별 정답으로 임의 분배하지 않습니다.

- 분석 범위: **2024–2025년**, 전국 행정동 기준
- 분석 목표: 사유시설 피해액, 공공시설 피해액, 총 피해액 회귀. 인적 피해는 별도의 상태·공식 확인 여부 기준으로 탐색
- 예측 방식: **사후** 관측 기상 정보 기반 피해 추정
- 주요 데이터 저장: [Google Drive DSDS_1](https://drive.google.com/drive/folders/1obNQDM00CxFSs1YIZKdxwqofie-rwoIE) (원천 재배포/권한 확인 필요)
- 로컬 프로젝트의 `data/`, `.env`, `backup/`은 기본적으로 GitHub 제출 대상에서 제외합니다.

## 데이터 계층과 산출물

| 계층 | 대표 산출물 | 목적 |
|---|---|---|
| Bronze | `data/bronze/weather/`, `labels/`, `boundaries/`, `manifests/` | 원본·경계·수집기록·체크섬 보존 |
| Silver | `data/silver/weather/station_hourly/nationwide_station_hourly_{2024,2025}.csv` | 전국 시간별 기상자료 표준화 |
| Silver | `data/silver/mappings/`, `labels/rain_damage/`, `labels/rain_casualty/` | 행정동 정합 및 원래 피해 해상도 보존 |
| Gold (시간) | `data/gold/weather/hourly/{2024,2025}/hourly_YYYY_MM.parquet` | 행정동별 시간 Feature |
| Gold (일) | `data/gold/weather/daily_from_hourly/{2024,2025}/dong_daily_weather_YYYY_MM.parquet` | 피해 일별 Label 결합용 기상 집계 |
| Gold (물적 피해) | `data/gold/model_inputs/property_damage_observed_daily_2024_2025_recovered_noksan124_PROVISIONAL.parquet` | 사유·공공 피해 + 기상 |
| Gold (물적+인적) | `data/gold/model_inputs/property_human_damage_observed_daily_2024_2025_recovered_noksan124_PROVISIONAL.parquet` | EDA 통합 데이터 |

마지막 통합 Gold의 QA 보고서 기준 **22,527행**, 관측된 인적 피해 **8행**, `no_record` **21,584행**, `unverified` **935행**입니다. `no_record`는 **확정된 피해 0이 아니며**, QA는 `PROVISIONAL_OBSERVED_CASES_NOT_TRAINING_READY` 상태입니다. 피해액 단위·결측 의미·공식 인적 피해 선택 규칙을 확인하기 전 학습 준비 완료로 표기하지 않습니다.

## 시작하기

```bash
python -m venv .venv
source .venv/bin/activate    # macOS/Linux
pip install -r requirements.txt
```

1. 원본·전처리 데이터는 Google Drive에서 **권한과 제공기관 이용조건을 확인한 후** 프로젝트의 `data/` 경로에 배치합니다.
2. API 인증키는 로컬 `.env`에만 보관하고 커밋하지 않습니다.
3. [전처리 실행 및 의존성](docs/PIPELINE.md)을 확인합니다. 순서가 확인되지 않은 스크립트를 전부 연속 실행하지 마세요.
4. [제출물 및 남은 작업](docs/SUBMISSION_CHECKLIST.md)을 확인합니다.

## GitHub에 올릴 Python 코드

이 브랜치에는 **제출 구조·문서·의존성 안내**를 먼저 정리합니다. 별도로 첨부해 검토한 28개 Python 파일 중 `main`에 없는 최신 파일은 **로컬 작업본을 확인하여 선별 업로드해야 합니다.** 현재 GitHub에 모든 28개가 업로드되어 있다고 가정하지 마세요.

핵심 파일명을 임의로 바꾸거나 위치를 이동하지 않습니다. 특히 `rebuild_noksan_124_pipeline.py`는 다른 `.py` 파일을 이름으로 호출합니다.

## 검증과 한계

- 시간별/일별 Gold가 모두 있어도 Label의 관측 상태와 모델 학습 가능성은 별도로 검증해야 합니다.
- `label_dong_code + label_day` 복합키 중복, 피해금액 단위, NULL/0 구분, 기상 극값, 누수 여부를 최종 확인합니다.
- 2024년 1–8월 Train, 9–12월 Validation, 2025년 Test는 **검토 중인 분할안**입니다. Validation에 양수 피해 표본이 충분한지 먼저 확인해야 합니다.
- 분석 산출물·Baseline·XGBoost 모델과 최종 평가 결과는 **아직 제출 준비가 완료되지 않았습니다.**
