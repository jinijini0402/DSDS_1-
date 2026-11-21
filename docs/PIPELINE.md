# 전처리 파이프라인 및 실행 가이드

> 이 문서는 **코드 본문 검토와 ZIP 아카이브의 파일명 기준으로 작성한 실행/의존성 안내**입니다. 전체 원본부터 자동 재현하는 단일 실행기가 아니며, 로컬 `data/`의 선행 산출물 존재 여부를 점검해야 합니다.

## 1. Bronze: 원본 수집 및 계보

| 스크립트 | 목적 | 유의사항 |
|---|---|---|
| `first_request.py` | API 1회 또는 기간별 원본 요청 | `config/weather.json`, `config/labels.json` 및 `.env` 필요 |
| `collect_seoul_hourly.py` | AWS 시간별 관측 원본 수집 | **파일명은 서울이지만 최근 작업본은 전국 수집**; GitHub의 기존 버전과 로컬 최신판의 차이 검증 |
| `collect_safety_damage.py` | 안전데이터 피해 원본 수집 | API 접근권한·페이지 집계 검증 |
| `rebuild_bronze_manifest.py` | 원천 파일 manifest·체크섬 재구성 | 원본에 손대지 않고 실행 |

## 2. Silver: 전국 기상·행정동

| 권장 순서 | 스크립트 | 주요 산출물 |
|---|---|---|
| 1 | `prepare_nationwide_weather_silver.py` | `nationwide_station_hourly_2024.csv`, `nationwide_station_hourly_2025.csv` |
| 2 | `repair_nationwide_aws_coordinates.py` | 관측소 좌표 보정 |
| 3 | `repair_stations_by_observed_dates.py` | 관측 유효 기간을 고려한 보정; `repair_nationwide_aws_coordinates.py`를 import |
| 4 | `map_nationwide_stations_to_dong.py` | `dong_station_distance_YYYY.csv` 등 행정동/관측소 매핑 |
| 5 | `prepare_ndms_sgis_crosswalk_inputs.py` | NDMS–SGIS 코드 연계 자료 |
| 6 | `validate_ndms_sgis_crosswalk.py` | 연계표 검사 |

실행 전 **해당 연도의 관측소 좌표, 경계 파일, 코드표, 관측소→행정동 매핑 파일**의 경로와 버전을 확인합니다. 지역 코드·기준 연도가 다르면 무조건 join하지 않습니다.

## 3. Silver: 사유·공공 피해 Label

| 스크립트 | 역할 및 확인할 선행 산출물 |
|---|---|
| `filter_rain_damage_2024_2025.py` | 2024–2025 호우 피해 Label 추출 |
| `build_private_rain_latest_history_review_v2.py` | 사유시설 최신 이력 검토 자료 생성 |
| `map_private_rain_v2_to_sgis_review_fixed.py` | 최초 사유시설 SGIS 정합: `private_rain_latest_history_review_v2.parquet` + `official_admin10_to_sgis8_YYYY.csv` + **레거시** `dong_daily_features_YYYY.parquet` 요구 |
| `audit_private_mapping_recovery_2025.py` | 2025 미정합 252건 복구 후보 검증 |
| `map_private_rain_v2_to_sgis_recovered_2025.py` | 2025 복구 후보와 연도별 행정동 연계표 기반 재매핑 |
| `map_public_rain_to_sgis_review.py` | 공공시설 정합 |
| `audit_noksan_2024_bronze_by_index.py` | 2024 녹산동 원천 대조/복구 진단 |
| `combine_rain_property_provisional_sources_recovered_2025.py` | 사유·공공 피해 관측값 통합 |

**차단 요소 A:** `map_private_rain_v2_to_sgis_review_fixed.py`가 요구하는 과거 `data/gold/weather/dong_daily_features_2024.parquet` / `...2025.parquet`의 재현성 검증 또는 최신 월별 Gold로의 경로 수정 필요.

**차단 요소 B:** 인적 피해 Silver 최초 생성 코드가 제공된 두 ZIP에서 확인되지 않았습니다. 현재 `data/silver/labels/rain_casualty/rain_casualty_records.parquet` 및 `rain_casualty_dong_daily.parquet`는 선행 입력이며, 이 코드 묶음만으로 다시 만들 수 있다고 주장하면 안 됩니다.

## 4. Gold: 기상 Feature

1. `prepare_gold_hourly_2024.py`, `prepare_gold_hourly_2025.py`: Silver의 관측소별 1시간 관측과 연도별 `dong_station_distance_YYYY.csv`를 사용하여 `hourly_YYYY_MM.parquet` 생성
2. `validate_and_build_daily_weather_2024.py`, `validate_and_build_daily_weather_2025.py`: 12개월 시간별 Gold 품질 검사 및 `dong_daily_weather_YYYY_MM.parquet` 생성
3. `prepare_gold_nationwide_daily_features.py`: **별도의 과거 형식** `dong_daily_features_YYYY.parquet` 생성 코드. 최초 사유시설 매핑의 레거시 입력이 될 수 있으므로 최종 월별 일별 Gold와 혼동하지 않습니다.

## 5. Gold: 피해 결합 및 복구

| 스크립트 | 설명 |
|---|---|
| `join_property_daily_weather_2024_2025.py` | 일별 기상 + 물적 피해 결합 |
| `join_rain_casualty_to_recovered_gold.py` | 물적 Gold + 인적 피해 Silver 결합 |
| `rebuild_noksan_124_pipeline.py` | 2024 녹산동 124건 복구 및 최종 Gold 재생성·QA; 관련 결합 스크립트를 동적 로드 |

`rebuild_noksan_124_pipeline.py`는 연관 스크립트의 **원래 파일명과 위치를 가정**합니다. 따라서 GitHub 제출 단계에서 Python 파일 이름을 변경하거나 모두 `scripts/bronze` 등 하위 폴더로 이동하지 않습니다. 기존 출력 파일 덮어쓰기 조건도 실행 전 확인합니다.

최종 제출용 EDA 메인:
```
data/gold/model_inputs/property_human_damage_observed_daily_2024_2025_recovered_noksan124_PROVISIONAL.parquet
data/gold/model_inputs/property_human_damage_observed_daily_2024_2025_recovered_noksan124_PROVISIONAL.qa.json
```

## 6. 실행 전·후 검증

- `audit_rain_label_join_readiness.py`: 피해 Label 정합·결합 준비 상태 진단
- `validate_final_gold.py`: 최종 Gold 컬럼, NULL, 중복, 숫자 유효범위 진단
- QA `status`가 `PROVISIONAL_OBSERVED_CASES_NOT_TRAINING_READY`이면 모델 학습 준비 완료 상태가 아님
- `label_dong_code` + `label_day` 중복, 비정상적 강수량, 관측소 거리, 피해금액 원 단위 및 0/NULL 의미를 점검

**데이터 해상도:** 1시간 기상 Feature ≠ 매시간 피해 Label. 일별 피해 금액/인적 기록을 임의로 시간별 Label로 늘리지 않습니다. `no_record`와 `unverified` 인적 상태를 피해 0으로 변환하지 않습니다.

**공식 인적 피해 기준:** `human_all_officially_confirmed` 값과 인적 피해 원천의 정의에 따라 8건 중 비공식 2건을 어떻게 다룰지 문서화해야 합니다.

## 7. 제출/재현성에서 빠진 항목

- 인적 피해 Silver 최초 생성 코드 및 Label 수집/매핑 근거
- 레거시 일별 Gold/원천 경로 의존성 정리
- 실제 실행 순서별 입력·출력 존재·행 수·키 유일성·원천 checksum 확인
- EDA 결과, Baseline, XGBoost 저장 모델·평가 결과

앞선 여러 파일이 개발·복구 전용 코드이므로, **파일명만으로 모든 명령의 무조건적인 재실행을 권장하지 않습니다.** 데이터 원본의 보존과 모델 누수 여부까지 검증한 뒤 재현 완료를 선언합니다.
