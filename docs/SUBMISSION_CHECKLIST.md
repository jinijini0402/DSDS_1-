# 필수 제출물 점검표

과제의 필수 제출물 네 종류를 기준으로 **GitHub 코드·Google Drive 데이터·분석 보고서**를 별도 확인합니다.

| 필수 제출물 | 현재 상태 | 제출 전 확인 |
|---|---|---|
| 데이터 분석 산출물 | **미완료/저장소 미확인** | EDA의 기상·피해금액·인적피해 분포, 결측/중복/이상치, 시각화와 해석 |
| 시간(1시간)·공간(행정동) 정합 전처리 코드 | **부분 확보** | ZIP에 핵심 28개 Python 코드 확인. 최신 코드 GitHub 반영과 실행 의존성 검사 필요 |
| 원본 데이터셋 및 전처리 데이터셋 | **주요 데이터 확보** | Google Drive Bronze·Silver·Gold 폴더와 산출물 존재. 원본 계보/검증자료 점검 |
| 사용 모델 파일 및 학습 결과 | **미완료/저장소 미확인** | Baseline, XGBoost, 검증·테스트 지표, 학습 모델 파일과 재현 명령 필요 |

## 핵심 Python 파일 목록

**Bronze (4):**
- `first_request.py`
- `collect_nationwide_aws_hourly.py`
- `collect_safety_damage.py`
- `rebuild_bronze_manifest.py`

**Silver — 기상·공간 정합 (6):**
- `prepare_nationwide_weather_silver.py`
- `repair_nationwide_aws_coordinates.py`
- `repair_stations_by_observed_dates.py`
- `map_nationwide_stations_to_dong.py`
- `prepare_ndms_sgis_crosswalk_inputs.py`
- `validate_ndms_sgis_crosswalk.py`

**Silver — 피해·매핑 (8):**
- `filter_rain_damage_2024_2025.py`
- `build_private_rain_latest_history_review_v2.py`
- `map_private_rain_v2_to_sgis_review_fixed.py` (backup ZIP에서 복원)
- `audit_private_mapping_recovery_2025.py`
- `map_private_rain_v2_to_sgis_recovered_2025.py`
- `map_public_rain_to_sgis_review.py`
- `audit_noksan_2024_bronze_by_index.py`
- `combine_rain_property_provisional_sources_recovered_2025.py`

**Gold — 기상 (5):**
- `prepare_gold_hourly_2024.py`
- `prepare_gold_hourly_2025.py`
- `validate_and_build_daily_weather_2024.py`
- `validate_and_build_daily_weather_2025.py`
- `prepare_gold_nationwide_daily_features.py`

**Gold — 결합 (3):**
- `join_property_daily_weather_2024_2025.py`
- `join_rain_casualty_to_recovered_gold.py`
- `rebuild_noksan_124_pipeline.py`

**검증 (2):**
- `audit_rain_label_join_readiness.py`
- `validate_final_gold.py`

## GitHub 및 Google Drive 역할

- **GitHub:** Python 코드, README, 데이터 정의, 분석 보고서, 가벼운 검증 산출물
- **Google Drive:** 대용량 원본 및 Silver/Gold 데이터, manifest·QA
- **비공개 로컬:** `.env`, API 키, 재배포 제한 자료
