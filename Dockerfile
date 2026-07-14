# Agent Platform POC - 공용 이미지
# docker build -t agent-platform-poc .

FROM python:3.12-slim

WORKDIR /app

# 의존성 설치 (캐시 레이어 분리)
COPY pyproject.toml .
RUN pip install --no-cache-dir -e . && \
    pip install --no-cache-dir setuptools

# 소스 코드 복사
COPY . .

# CMD는 docker-compose에서 서비스별로 오버라이드
CMD ["uvicorn", "orchestrator.main:app", "--host", "0.0.0.0", "--port", "8000"]
