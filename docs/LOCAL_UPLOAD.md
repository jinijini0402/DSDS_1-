# 최신 Python 28개 코드 로컬 → GitHub 반영 가이드

이 브랜치는 **먼저 문서와 의존성 파일만 업데이트**했습니다. 과제 제출 전에는 로컬에서 최종 작업본을 GitHub에 반영하고 재현성 검사를 해야 합니다.

## 현재 준비된 코드

대화에서 검토한 `아카이브.zip`과 `backup.zip`으로 만든 `submission_python_28.zip`에는 현재 파일명 그대로 핵심 Python 스크립트 28개가 있습니다. 인적 피해 Silver **최초 생성 코드**는 포함되어 있지 않습니다.

**중요:** 로컬 `heavy-rain-prediction`에 있는 코드가 ZIP보다 최신일 수 있습니다. ZIP을 무조건 덮어쓰면 안 됩니다. `collect_seoul_hourly.py`처럼 `main`에 예전 버전이 있는 파일은 반드시 변경 내용을 비교합니다.

## 권장 절차 (macOS)

```bash
# 변경 사항 확인 (기존 로컬 수정/삭제/추적 중인 data 파일 포함)
git status --short

# 필요하면 현재 수정사항을 안전한 위치에 백업
# ZIP을 내려받은 뒤 원하는 폴더에 먼저 압축 해제 (프로젝트에 직접 덮어쓰지 않음)
mkdir -p /tmp/heavy_rain_submission
unzip submission_python_28.zip -d /tmp/heavy_rain_submission

# 기존 코드와 차이 확인
diff -u collect_seoul_hourly.py /tmp/heavy_rain_submission/collect_seoul_hourly.py || true
diff -u README.md README.md   # README는 기존 상태 확인 후 사용
```

28개 코드의 최신 버전을 확정한 다음, **이 저장소의 브랜치를 기준으로** 필요한 파일만 `git add 파일명.py`로 선택합니다. 기존 `data/` 트래킹 파일 삭제를 의도하지 않았다면 `git add .`로 한꺼번에 추가하지 마세요.

```bash
git diff --cached --name-status
git diff --cached --check
git commit -m "Add reviewed 2024-2025 medallion pipeline scripts"
git push -u origin submission/medallion-pipeline-2026
```

단, 위 과정의 `git commit`과 `git push`는 **선별 파일을 스테이징한 경우에만** 실행합니다. 이미 다른 로컬 변경이 있다면 먼저 별도 보존하고 브랜치를 전환해야 합니다.

## 최종 구조에 대한 원칙

- 기존 Python 파일명과 **루트 위치 유지**: 동적 스크립트 로드·상대경로 참조가 있음
- 탐색/테스트/레거시 코드는 로컬 `backup/`에 보존하며 기본 GitHub 제출 제외
- 원본 데이터 `data/`, API 키 `.env`는 원칙적으로 GitHub 새 업로드 제외
- 기존에 이미 Git이 추적하는 `data/` 경로는 별도 검토 없이는 삭제/추적 해제 금지
- README와 `docs/PIPELINE.md`에 단계별 실행·산출물·품질 제한 표시
