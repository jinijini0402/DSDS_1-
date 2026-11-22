# Heavy Rain Damage Prediction — 2024–2025 전국 호우 피해 데이터 파이프라인

## 개요

기상청 AWS 관측자료와 재난 피해 기록을 수집해 **원본(Bronze) → 정제·행정동 정합(Silver) → 분석용 데이터(Gold)** 로 구성합니다. 기상 Feature는 시간별로 구축하고, 물적·인적 피해 Label은 **실제 관측된 일자/지역 단위**로 보존합니다. 일별 피해량을 시간별 정답으로 임의 분배하지 않습니다.

- 분석 범위: **2024–2025년**, 전국 행정동 기준
- 분석 목표: 사유시설 피해액, 공공시설 피해액, 총 피해액 회귀. 인적 피해는 별도의 상태·공식 확인 여부 기준으로 탐색
- 예측 방식: **사후** 관측 기상 정보 기반 피해 추정
- 주요 데이터 저장: [Google Drive DSDS_1](https://drive.google.com/drive/folders/1obNQDM00CxFSs1YIZKdxwqofie-rwoIE)
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

마지막 통합 Gold의 QA 보고서 기준 **22,527행**, 관측된 인적 피해 **8행**, `no_record` **21,584행**, `unverified` **935행**입니다.
