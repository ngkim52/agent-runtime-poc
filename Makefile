# Agent Platform POC - 편의 명령어

.PHONY: build up down logs test ps

# 이미지 빌드
build:
	docker compose build

# 전체 서비스 실행 (daemon)
up:
	docker compose up -d

# 전체 서비스 중지
down:
	docker compose down

# 로그 확인
logs:
	docker compose logs -f

# 서비스 상태 확인
ps:
	docker compose ps

# 테스트 실행 (로컬, Dapr 미설치 환경에서도 동작)
test:
	PYTHONIOENCODING=utf-8 python tests/test_loader.py
	PYTHONIOENCODING=utf-8 python tests/test_worker.py
	PYTHONIOENCODING=utf-8 python tests/test_engine.py
