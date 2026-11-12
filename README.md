# DSDS_1-
Heavy Rain Damage Prediction using Weather and Disaster Data

## 데이터셋 안내

본 프로젝트는 서울특별시의 2021~2024년 기상 관측자료와 침수흔적도 데이터를 활용하여 행정동별 폭우 관련 데이터 파이프라인을 구축합니다.

대용량 원본 및 전처리 데이터는 GitHub에 직접 업로드하지 않고 별도의 Google Drive에서 관리합니다.

### 데이터 다운로드

[Google Drive 데이터 폴더](https://drive.google.com/drive/folders/1obNQDM00CxFSs1YIZKdxwqofie-rwoIE?usp=drive_link)

### 데이터 목록

| 데이터셋 | 기간 | 내용 | 저장 경로 |
|---|---|---|---|
| 서울 AWS 기상자료 | 2021~2024 | 관측소별 시간 단위 기상자료 | `data/bronze/weather/` |
| 서울 침수흔적도 | 확보 자료 기준 | 침수 발생일, 면적, 침수구역 도형 | `data/bronze/labels/` |
| 서울 행정동 경계 | 2021~2024 | 연도별 행정동 경계 SHP | `data/bronze/boundaries/` |
| 기상자료 Silver | 2021~2024 | 결측·이상값 검증 및 표준화 결과 | `data/silver/weather/` |

### 데이터 사용 방법

1. Google Drive에서 필요한 데이터셋을 다운로드합니다.
2. 저장소를 내려받은 뒤 각 데이터셋을 위 표의 경로에 배치합니다.
3. 필요한 Python 패키지를 설치합니다.
4. 각 전처리 스크립트를 실행하여 분석 결과를 재현합니다.

### 데이터 관리 원칙

- Bronze 원본 데이터는 수정하지 않습니다.
- Silver 데이터는 검증 및 전처리 결과를 저장합니다.
- Gold 데이터는 행정동별 1시간 단위 Feature를 저장합니다.
- API 인증키는 `.env`에 별도로 관리하고 공유하지 않습니다.
- 데이터의 출처, 수집기간, 기준연도, 결측 처리 방법은 문서로 기록합니다.

※ Google Drive 파일의 재배포 및 공개 범위는 원천 제공기관의 이용조건을 따릅니다.
