# Agent Runtime POC

AI 기반 워크플로우 오케스트레이션 플랫폼. **Orchestrator → Dapr Workflow → Pub/Sub → Worker Agents** 아키텍처로 구성된 에이전트 런타임 시스템입니다.

## Architecture

```
┌─────────────┐    ┌───────────────┐    ┌──────────────────┐
│  Orchestrator │───▶│ Dapr Workflow │───▶│  Agent Workers   │
│  (FastAPI)    │    │   (FastAPI)   │    │  (FastAPI x N)   │
│  :8000        │    │   :8002       │    │  :8003, :8004     │
└──────┬───────┘    └──────┬────────┘    └────────┬─────────┘
       │                   │                      │
       │      ┌────────────┴────────────┐         │
       │      │     Message Queue       │         │
       │      │   RabbitMQ :5672        │◀────────┘
       │      └─────────────────────────┘
       │
       ▼
┌──────────────┐
│  PostgreSQL  │
│  :5432       │
└──────────────┘
```

### Components

| Component | Role | Tech |
|-----------|------|------|
| **Orchestrator** | 워크플로우 정의 관리, 실행 요청, Web UI 제공 | FastAPI + Jinja2 |
| **Dapr Workflow** | Dapr Workflow 런타임, 태스크 디스패치 | Dapr SDK + FastAPI |
| **Agent Workers** | 실제 AI 태스크 수행 (멀티워커) | LangGraph + OpenAI |
| **PostgreSQL** | 워크플로우 정의, 인스턴스 상태 저장 | SQLAlchemy ORM |
| **RabbitMQ** | 태스크 큐잉 (competing consumers) | Dapr PubSub |
| **Redis** | Dapr statestore / actor | Dapr StateStore |
| **Dapr** | Pub/Sub, State Management, Workflow, Actors | Dapr 1.18 |

## Quick Start

### Prerequisites

- Docker & Docker Compose
- OpenAI-compatible API key (or any LLM endpoint)

### Setup

```bash
# 1. 환경변수 설정
cp .env.example .env
# .env 파일을 열어서 API 키 등 실제 값 입력

# 2. 전체 서비스 실행
docker compose up -d

# 3. Web UI 접속
open http://localhost:8000
```

첫 실행 시 `init_db.py` 가 orchestrator startup 에서 자동 실행되어 DB 테이블이 생성됩니다.

### 서비스 상태 확인

```bash
docker compose ps
docker compose logs orchestrator -f
```

## URLs

| Service | URL | Description |
|---------|-----|-------------|
| **Web UI** | http://localhost:8000 | 메인 대시보드 |
| Workflows | http://localhost:8000/workflows | 워크플로우 목록 |
| Instances | http://localhost:8000/instances | 실행 이력 |
| Run | http://localhost:8000/run | 워크플로우 실행 |
| Health | http://localhost:8000/healthz | 헬스체크 |
| RabbitMQ Admin | http://localhost:15672 | guest / guest |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://postgres:changeme@postgres:5432/poc_workflow` | PostgreSQL 접속 문자열 |
| `OPENAI_API_KEY` | `sk-placeholder` | OpenAI 또는 호환 API 키 |
| `LLM_BASE_URL` | `https://api.openai.com/v1` | LLM API 엔드포인트 |
| `LLM_MODEL` | `gpt-4o` | LLM 모델명 |
| `TAVILY_API_KEY` | `tvly-placeholder` | Tavily Search API 키 (선택) |
| `LLM_TIMEOUT` | `180` | LLM 요청 타임아웃 (초) |
| `AGENT_TIMEOUT` | `120` | 에이전트 타임아웃 (초) |

## API Endpoints

### Workflows

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/workflows` | 워크플로우 목록 조회 |
| `GET` | `/api/workflows/{id}/{version}` | 특정 워크플로우 조회 |
| `POST` | `/api/workflows/{id}/{version}` | 워크플로우 저장 |
| `DELETE` | `/api/workflows/{id}/{version}` | 워크플로우 삭제 |
| `POST` | `/api/workflows/ai-design` | AI 워크플로우 설계 |
| `POST` | `/api/workflows/ai-design/stream` | AI 설계 (스트리밍) |

### Instances

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/instances/start` | 워크플로우 실행 |
| `GET` | `/api/instances` | 인스턴스 목록 |
| `GET` | `/api/instances/{id}` | 인스턴스 상세 |
| `POST` | `/api/instances/{id}/resume` | 인스턴스 재개 |

## Development

### 로컬에서 테스트 실행

```bash
# Docker 없이 테스트만 실행
make test
```

### DB 마이그레이션

```bash
# YAML 워크플로우를 DB에 적재
docker compose exec orchestrator python scripts/migrate_yaml_to_db.py
```

### 새 워크플로우 추가

1. `biz_workflows/` 디렉토리에 YAML 파일 생성
2. `scripts/migrate_yaml_to_db.py` 의 `ALL_WORKFLOWS` 리스트에 추가
3. 마이그레이션 실행

## Roadmap

### Planned Features

#### API Call Step
워크플로우 스텝(State)에서 **외부 REST API 호출**을 `task_type` 으로 지원할 예정입니다.

- REST API (GET/POST/PUT/DELETE) 호출
- 요청/응답 변수 매핑 (Jinja2 템플릿)
- 타임아웃 및 재시도 정책 설정
- 인증 정보 secrets 관리

#### Data Processing Step
수집된 데이터를 가공/변환하는 **데이터 처리 스텝**을 지원할 예정입니다.

- JSONata / JQ 기반 데이터 변환
- 필터, 매핑, 집계 연산
- 조건부 분기 로직
- 이전 스텝 출력값 참조 및 가공

#### Workflow Promotion (Dev → Production)
개발계에 등록된 워크플로우와 스텝을 운영계로 이관(Promotion)하는 기능을 지원할 예정입니다.

- **버전 비교** — 개발계/운영계 간 워크플로우 정의 diff 체크
- **변경 내역 조회** — 특정 버전의 변경 스텝, 트랜지션, 스키마 차이 확인
- **YAML 익스포트** — DB에 저장된 워크플로우를 YAML 파일로 추출하여 소스 관리
- **동기화** — 검증된 워크플로우를 대상 환경에 배포

이를 통해 Git 기반의 IaC(Infrastructure as Code) 워크플로우를 구성할 수 있으며, `biz_workflows/` 디렉토리의 YAML 파일과 DB 데이터 간의 양방향 동기화가 가능해집니다.

#### Why These Features
현재는 AI 에이전트(LangGraph) 기반 태스크만 지원합니다. 위 기능들이 추가되면:

1. **외부 시스템 연동** — 레거시 API, DB, SaaS 서비스와 직접 통합
2. **데이터 ETL** — 수집 → 정제 → 가공 → 저장 파이프라인 구성
3. **하이브리드 워크플로우** — AI 판단 + API 호출 + 데이터 변환 조합
4. **No-Code 에이전트** — YAML 선언만으로 복잡한 비즈니스 로직 구현
5. **Git 기반 운영** — DB ↔ YAML 동기화로 버전 관리 및 환경 간 프로모션

## Project Structure

```
.
├── agent_worker/          # AI Agent Workers
│   ├── handlers/          #  Task-specific handlers
│   └── universal/         #  LangGraph-based universal agent
├── biz_workflows/         # YAML 워크플로우 정의
├── components/            # Dapr component configs
├── dapr_workflow/         # Dapr Workflow layer
├── orchestrator/          # Web UI & API
├── scripts/               # 유틸리티 스크립트
├── shared/                # 공유 모델, DB, 로더
├── templates/             # Jinja2 HTML 템플릿
└── tests/                 # 테스트
```

## License

MIT
