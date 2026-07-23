# Agent Runtime POC

AI 기반 워크플로우 오케스트레이션 플랫폼. **Orchestrator → Dapr Workflow → Pub/Sub → Worker Agents** 아키텍처로 구성된 에이전트 런타임 시스템입니다.

두 가지 실행 모드:
- **Standalone** (기본값, Dapr 불필요): Orchestrator → DirectAgentClient → Worker (ThreadPool 기반 병렬 실행)
- **Dapr**: Orchestrator → DaprWorkflowClient → Dapr WF → Pub/Sub → Worker + Dapr sidecar (진정한 fan-out/fan-in)

`DAPR_WF_SERVICE_URL` 환경변수로 전환. `"none"` 또는 `"false"` 로 설정 시 Standalone 모드.

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
| Instance Output | `/instances/{id}/output` | MD 리포트 조회 |
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
| `DAPR_WF_SERVICE_URL` | `http://localhost:8002` | Dapr 모드 URL. `none` = Standalone |
| `AGENT_WORKER_URL` | `http://localhost:8003` | Worker 직접 호출 URL |

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
| `POST` | `/api/instances/start` | 워크플로우 실행 (비동기) |
| `GET` | `/api/instances` | 인스턴스 목록 |
| `GET` | `/api/instances/{id}` | 인스턴스 상세 |
| `POST` | `/api/instances/{id}/resume` | 인스턴스 재개 (실패 지점부터) |
| `POST` | `/api/test/run` | 워크플로우 실행 (동기, 테스트용) |

### Instance Pages (Web UI)

| Path | Description |
|------|-------------|
| `/instances/{id}` | Step별 실행 이력 |
| `/instances/{id}/progress` | SVG 순서도 + 진행률 |
| `/instances/{id}/output` | 최종 MD 리포트 뷰어 |

## Workflow System

### State Types (4종)

| Type | 설명 |
|------|------|
| `start` | 시작점 |
| `task` | 단일 작업 (ai_task / rest_api_call / data_processing) |
| `parallel` | 병렬 브랜치 실행 (fan-out/fan-in) |
| `end` | 종료점 |

### Task Types

| Task | 설명 |
|------|------|
| `ai_task` | LangGraph ReAct Agent (LLM + Tool calling) |
| `rest_api_call` | 외부 REST API 호출 (GET/POST, response_mapping, error_handling) |
| `data_processing` | 데이터 변환 (select/filter/sort/compute/merge transforms) |

### Parallel Execution (Fan-out / Fan-in)

`parallel` state type으로 여러 브랜치를 동시에 실행하고 결과를 취합합니다:

```yaml
- id: FETCH_INITIAL_DATA
  type: parallel
  branches:
    - id: GET_POLICY
      task: rest_api_call
      vars:
        policy_no: "$._input.policy_number"
      rest_api_config:
        method: GET
        url: "http://orchestrator:8000/api/mock/policies/{{ payload.policy_no }}"
        response_mapping:
          coverage_type: "$.data.coverage_type"
          max_benefit: "$.data.max_benefit"
          deductible: "$.data.deductible"
    - id: GET_CUSTOMER
      task: rest_api_call
      vars:
        customer_id: "$._input.customer_id"
      rest_api_config:
        method: GET
        url: "http://orchestrator:8000/api/mock/customers/{{ payload.customer_id }}"
        response_mapping:
          name: "$.data.name"
          claim_history: "$.data.claim_history"
          claim_count_3y: "$.data.claim_count_3y"
```

모든 브랜치가 완료되면 결과는 `FETCH_INITIAL_DATA.GET_POLICY`, `FETCH_INITIAL_DATA.GET_CUSTOMER` 형태로 취합되며, 이후 step에서 `vars: { coverage: "$.GET_POLICY", customer: "$.GET_CUSTOMER" }` 로 개별 접근 가능합니다.

### Data Processing (Transforms)

`data_processing` task는 체인 가능한 변환(transform) 파이프라인을 지원합니다:

| Transform | 설명 |
|-----------|------|
| `select` | 특정 필드만 추출 (dict → dict 또는 list → list) |
| `filter` | 조건 기반 필터링 |
| `sort` | 키 기준 정렬 |
| `compute` | 표현식 평가 |
| `merge` | 여러 source dict를 하나로 병합 |

```yaml
- id: PROCESS_RESULTS
  type: task
  task: data_processing
  vars:
    classification: "$.CLASSIFY_CLAIM"
    fraud: "$.FRAUD_CHECK"
  data_processing_config:
    transforms:
      - type: select
        source_var: classification
        fields:
          category: "item.claim_category"
          risk: "item.risk_score"
        result_var: selected
      - type: select
        source_var: fraud
        fields:
          score: "item.fraud_score"
          indicators: "item.fraud_indicators"
        result_var: fraud_info
      - type: merge
        sources: [selected, fraud_info]
        result_var: merged
    output_var: merged
```

### Variables & Data Flow

워크플로우 step 간 데이터 전달은 두 가지 방식을 지원합니다:

1. **vars (신규, 권장)**: 명시적 JSON path 매핑
   ```yaml
   vars:
     policy_no: "$._input.policy_number"
     coverage: "$.GET_POLICY.coverage_type"
   ```

2. **inputs (기존)**: step ID 기반 flat-merge
   ```yaml
   inputs: ["GET_POLICY", "GET_CUSTOMER"]
   ```

JSON path 표현식: `$.STATE_ID.field.subfield`, `$._input.field`, `$.STATE_ID` (전체 결과)

### Transition Conditions

`when` 필드로 조건부 분기 가능 (Python 표현식, `simpleeval`로 안전 실행):

```yaml
transitions:
  - from: FRAUD_CHECK
    to: MANUAL_REVIEW
    when: "result.FRAUD_CHECK.fraud_score > 70"
  - from: FRAUD_CHECK
    to: FINAL_DECISION
    when: "result.FRAUD_CHECK.fraud_score <= 70"
```

`when`이 없거나 비어있으면 unconditional (항상 매칭됨). First-match-wins 방식.
조건 표현식에서 `result.STATE_ID.field` 로 이전 step 결과에 접근 가능.

### Claim Basic Adjudication 예제

`biz_workflows/claim_basic_adjudication_v1.yaml` — 9개 state, 8개 transition으로 구성된 보험금 청구 심사 워크플로우:

```
START
  → FETCH_INITIAL_DATA (parallel: GET_POLICY + GET_CUSTOMER)
  → CLASSIFY_CLAIM (ai_task: AI 분류)
  → FRAUD_CHECK (rest_api_call: 사기탐지)
  → PROCESS_RESULTS (data_processing: 데이터 정제)
  → FINAL_DECISION (ai_task: AI 최종 결정)
  → SUBMIT_RESULT (rest_api_call: 코어API 전송)
  → GENERATE_REPORT (ai_task: MD 리포트 생성)
  → END
```

## Development

### 로컬에서 테스트 실행

```bash
# Docker 없이, Dapr 없이, API key 불필요 (mock handler 사용)
make test
# 또는:
PYTHONIOENCODING=utf-8 python tests/test_worker.py

# E2E 테스트 (pytest)
python -m pytest tests/test_e2e.py -v
python -m pytest tests/test_workflow_designer.py -v
```

### DB 마이그레이션

```bash
# YAML 워크플로우를 DB에 적재
docker compose exec orchestrator python scripts/migrate_yaml_to_db.py
```

### 새 워크플로우 추가

1. `biz_workflows/` 디렉토리에 `{workflow_id}_v{major}.yaml` 파일 생성
2. `scripts/migrate_yaml_to_db.py` 의 `ALL_WORKFLOWS` 리스트에 추가
3. 마이그레이션 실행

## Project Structure

```
.
├── agent_worker/           # AI Agent Workers
│   ├── handlers/           # Task-specific handlers (auto-register on import)
│   │   ├── base.py         # TaskHandler ABC (ok/fail helpers)
│   │   ├── data_processing.py  # Transform pipeline handler
│   │   ├── rest_api_call.py    # REST API call handler
│   │   └── __init__.py     # Side-effect imports trigger registration
│   ├── universal/          # LangGraph ReAct Agent (the "ai_task" handler)
│   │   ├── handler.py      # handle_task(), WORKFLOW_DESIGNER_INSTRUCTION
│   │   ├── langgraph_agent.py  # create_react_agent, run_agent()
│   │   └── tools.py        # Tool definitions
│   ├── registry.py         # HandlerRegistry: task_type → handler
│   ├── dispatcher.py       # dispatch → execute → return
│   └── main.py             # FastAPI app
├── biz_workflows/          # YAML 워크플로우 정의
│   ├── claim_basic_adjudication_v1.yaml  # 보험금 청구 심사 예제
│   └── poc_doc_generation_v4.yaml        # 문서 생성 예제
├── components/             # Dapr component configs
├── dapr_workflow/          # Dapr Workflow layer
├── orchestrator/           # Web UI & API
│   ├── main.py             # FastAPI app, UI routes, Mock API
│   ├── engine.py           # BizFlowEngine: state machine loop
│   ├── instance.py         # ClaimInstance, StepRecord
│   ├── dapr_client.py      # DirectAgentClient / DaprWorkflowClient
│   └── payload_builder.py  # Step payload assembly
├── scripts/                # 유틸리티 스크립트
├── shared/                 # 공유 모델, DB, 로더
│   ├── models.py           # BizWorkflowDef, StateDef, TaskRequest
│   ├── step_utils.py       # evaluate_vars, resolve_jsonpath
│   ├── workflow_loader.py  # BizWorkflowLoader, TransitionEvaluator
│   └── workflow_repository.py  # BizWorkflowRepository (DB CRUD)
├── templates/              # Jinja2 HTML 템플릿 (11 files)
├── tests/                  # 테스트
└── AGENTS.md               # Agent guide
```

## Roadmap

### Implemented Features

#### ✅ Parallel State (Fan-out/Fan-in)
여러 API 호출 또는 AI 태스크를 병렬로 실행하고 결과를 취합. `DirectAgentClient.execute_parallel()`은 ThreadPoolExecutor 기반 진정한 병렬 실행.

#### ✅ REST API Call Step
외부 REST API 호출을 `task_type: rest_api_call`로 지원. GET/POST, response_mapping, error_handling, Jinja2 템플릿 변수 치환.

#### ✅ Data Processing Step
`task_type: data_processing`로 체인 가능한 transform pipeline 지원 (select, filter, sort, compute, merge).

#### ✅ AI Workflow Designer
LangGraph 기반 AI 어시스턴트로 대화형 워크플로우 설계/수정. 7개 workflow 전용 툴 + 8개 일반 툴.

#### ✅ MD Report Viewer
GENERATE_REPORT step의 Markdown 출력을 `/instances/{id}/output`에서 렌더링. 다운로드/클립보드 복사 지원.

### Planned Features

#### Workflow Promotion (Dev → Production)
개발계에 등록된 워크플로우와 스텝을 운영계로 이관(Promotion)하는 기능을 지원할 예정입니다.

- **버전 비교** — 개발계/운영계 간 워크플로우 정의 diff 체크
- **변경 내역 조회** — 특정 버전의 변경 스텝, 트랜지션, 스키마 차이 확인
- **YAML 익스포트** — DB에 저장된 워크플로우를 YAML 파일로 추출하여 소스 관리
- **동기화** — 검증된 워크플로우를 대상 환경에 배포

이를 통해 Git 기반의 IaC(Infrastructure as Code) 워크플로우를 구성할 수 있으며, `biz_workflows/` 디렉토리의 YAML 파일과 DB 데이터 간의 양방향 동기화가 가능해집니다.

#### Why These Features
AI 에이전트(LangGraph) + REST API Call + Data Processing이 모두 지원되면:

1. **외부 시스템 연동** — 레거시 API, DB, SaaS 서비스와 직접 통합
2. **데이터 ETL** — 수집 → 정제 → 가공 → 저장 파이프라인 구성
3. **하이브리드 워크플로우** — AI 판단 + API 호출 + 데이터 변환 조합
4. **No-Code 에이전트** — YAML 선언만으로 복잡한 비즈니스 로직 구현
5. **Git 기반 운영** — DB ↔ YAML 동기화로 버전 관리 및 환경 간 프로모션

## License

MIT
