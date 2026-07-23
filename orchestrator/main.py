from __future__ import annotations
import json
import os
import sys
import logging
from contextlib import asynccontextmanager

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from typing import Any, Dict
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.responses import StreamingResponse
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel

from shared.db import Base, engine
from shared.workflow_loader import BizWorkflowLoader
from shared.workflow_repository import BizWorkflowRepository
from orchestrator.engine import BizFlowEngine
from orchestrator.dapr_client import DirectAgentClient, DaprWorkflowClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("orchestrator")

# ── Lifespan (startup/shutdown) ───────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    log.info("DB tables synced (%d tables)", len(Base.metadata.tables))
    yield

app = FastAPI(title="Biz Orchestrator (Claim Adjudication)", lifespan=lifespan)
_jinja_env = Environment(
    loader=FileSystemLoader(os.path.join(PROJECT_ROOT, "templates")),
)

# 한글 깨짐 방지: Jinja2 기본 tojson은 ensure_ascii=True (기본값)이므로 커스텀 필터 추가
def _tojson_unicode(value: Any, indent: int = 2) -> str:
    return json.dumps(value, indent=indent, ensure_ascii=False)

_jinja_env.filters["tojson_unicode"] = _tojson_unicode

# ── 싱글톤 ──────────────────────────────────────────────────
_db_repo = BizWorkflowRepository()
_loader = _db_repo
# DaprWorkflowClient 기본값 (DAPR_WF_SERVICE_URL=http://localhost:8002)
# DAPR_WF_SERVICE_URL=none 또는 false 로 설정 시 DirectAgentClient (standalone) 사용
_dapr_url = os.getenv("DAPR_WF_SERVICE_URL", "http://localhost:8002")
_dapr = DirectAgentClient() if _dapr_url.lower() in ("", "none", "0", "false") else DaprWorkflowClient(_dapr_url)
_engine = BizFlowEngine(loader=_loader, dapr_wf_client=_dapr, db_repo=_db_repo)


# ── helpers ──────────────────────────────────────────────────

def _render(name: str, **ctx) -> HTMLResponse:
    tmpl = _jinja_env.get_template(name)
    html = tmpl.render(**ctx)
    return HTMLResponse(html)


def _get_wf(workflow_id: str, version: str):
    try:
        return _db_repo.load(workflow_id, version)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Workflow not found")


# ═══════════════════════════════════════════════════════════════
# Claim 실행
# ═══════════════════════════════════════════════════════════════

class StartClaimRequest(BaseModel):
    claim_no: str
    policy_no: str
    insured_name: str
    insured_ssn: str
    account_no: str
    workflow_version: str = "1.0"


@app.post("/claims/start")
def start_claim(req: StartClaimRequest) -> Dict[str, Any]:
    inst = _engine.run(
        workflow_id="claim_adjudication",
        version=req.workflow_version,
        initial_input=req.model_dump(),
    )
    return {
        "instance_id": inst.instance_id,
        "final_state": inst.final_state,
        "history": [s.model_dump() for s in inst.history],
    }


# ═══════════════════════════════════════════════════════════════
# 범용 워크플로우 실행
# ═══════════════════════════════════════════════════════════════

class StartInstanceRequest(BaseModel):
    workflow_id: str
    version: str = "1.0"
    input: Dict[str, Any] = {}


@app.post("/api/instances/start")
def api_start_instance(req: StartInstanceRequest) -> Dict[str, Any]:
    """범용 워크플로우 실행 — 생성된 instance_id와 진행 페이지 URL 반환."""
    import threading
    from orchestrator.instance import ClaimInstance

    wf = _get_wf(req.workflow_id, req.version)

    # 1) Instance 생성 + DB 저장 (RUNNING)
    inst = ClaimInstance(
        workflow_id=req.workflow_id,
        workflow_version=req.version,
        initial_input=req.input,
    )
    wf_def_id = _db_repo._lookup_wf_def_id(req.workflow_id, req.version)
    _db_repo.save_instance(
        instance_id=inst.instance_id,
        wf_def_id=wf_def_id,
        workflow_id=req.workflow_id,
        workflow_version=req.version,
        initial_input=req.input,
    )
    start_id = _engine._get_start_state_id(wf)
    _db_repo.update_instance_state(inst.instance_id, current_state=start_id, status="RUNNING")

    # 2) Engine을 백그라운드 스레드에서 실행
    def _run_engine():
        try:
            _engine.run(
                workflow_id=req.workflow_id,
                version=req.version,
                initial_input=req.input,
                existing_instance=inst,
            )
        except Exception as e:
            log.exception("Background engine failed for %s", inst.instance_id)
            try:
                _db_repo.update_instance_state(
                    instance_id=inst.instance_id,
                    current_state="ERROR",
                    status="FAILED",
                )
            except Exception:
                pass

    thread = threading.Thread(target=_run_engine, daemon=True)
    thread.start()

    return {
        "instance_id": inst.instance_id,
        "status": "RUNNING",
        "progress_url": f"/instances/{inst.instance_id}/progress",
    }


# ═══════════════════════════════════════════════════════════════
# Resume (FAILED → 재시도)
# ═══════════════════════════════════════════════════════════════

class ResumeInstanceRequest(BaseModel):
    max_retries: int = 3


@app.post("/api/instances/{instance_id}/resume")
def api_resume_instance(instance_id: str, req: ResumeInstanceRequest = Body(...)) -> Dict[str, Any]:
    """FAILED 상태의 instance를 재시도. 재시도 횟수 제한 초과 시 409."""
    import threading
    from orchestrator.instance import ClaimInstance

    # 1) DB에서 instance 조회
    detail = _db_repo.get_instance_detail(instance_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Instance not found")

    if detail["status"] != "FAILED":
        raise HTTPException(status_code=409, detail=f"Instance is {detail['status']}, not FAILED")

    # 2) 재시도 횟수 검증 (state별 FAIL + SKIP 개수)
    failed_state_id = detail["current_state"]
    attempts = _db_repo.count_state_attempts(instance_id, failed_state_id)
    if attempts >= req.max_retries:
        raise HTTPException(
            status_code=409,
            detail=f"Retry limit exhausted for state '{failed_state_id}': {attempts}/{req.max_retries}",
        )

    # 3) 기존 FAIL result → SKIP 마킹
    _db_repo.skip_step_result(instance_id, failed_state_id)

    # 4) Instance 재구성
    reconstructed = _db_repo.reconstruct_instance(instance_id)
    if reconstructed is None:
        raise HTTPException(status_code=500, detail="Failed to reconstruct instance")

    r = reconstructed
    inst = ClaimInstance(
        instance_id=r["instance"]["instance_id"],
        workflow_id=r["instance"]["workflow_id"],
        workflow_version=r["instance"]["workflow_version"],
        initial_input=r["instance"]["initial_input"],
    )
    inst.current_state = r["instance"]["current_state"]
    inst.created_at = datetime.fromisoformat(r["instance"]["created_at"].replace("Z", "+00:00"))
    if r["instance"].get("finished_at"):
        inst.finished_at = datetime.fromisoformat(r["instance"]["finished_at"].replace("Z", "+00:00"))
    # history 복원
    for h in r["instance"]["history"]:
        from orchestrator.instance import StepRecord as StepRecordCls
        sr = StepRecordCls(
            state_id=h["state_id"],
            task_type=h.get("task_type"),
            wf_instance_id=h.get("wf_instance_id"),
            status=h["status"],
            result=h.get("result", {}),
            error=h.get("error"),
            started_at=datetime.fromisoformat(h["started_at"].replace("Z", "+00:00")),
            finished_at=datetime.fromisoformat(h["finished_at"].replace("Z", "+00:00")) if h.get("finished_at") else None,
        )
        inst.record(sr)

    # 5) DB 상태 업데이트: RUNNING
    _db_repo.update_instance_state(instance_id, current_state=failed_state_id, status="RUNNING")

    # 6) 백그라운드 Engine 실행
    def _run_engine():
        try:
            _engine.run(
                workflow_id=r["instance"]["workflow_id"],
                version=r["instance"]["workflow_version"],
                initial_input=r["instance"]["initial_input"],
                existing_instance=inst,
            )
        except Exception as e:
            log.exception("Background engine failed for %s", instance_id)
            try:
                _db_repo.update_instance_state(
                    instance_id=instance_id,
                    current_state=failed_state_id,
                    status="FAILED",
                )
            except Exception:
                pass

    thread = threading.Thread(target=_run_engine, daemon=True)
    thread.start()

    return {
        "instance_id": instance_id,
        "status": "RUNNING",
        "retry_attempt": attempts + 1,
        "max_retries": req.max_retries,
        "resumed_from_state": failed_state_id,
        "progress_url": f"/instances/{instance_id}/progress",
    }


# ═══════════════════════════════════════════════════════════════
# 워크플로우 정의 관리 (JSON API)
# ═══════════════════════════════════════════════════════════════

@app.get("/api/workflows")
def api_list_workflows():
    return _db_repo.list_workflows()


@app.post("/api/workflows/ai-design/stream")
def ai_workflow_design_stream(body: Dict[str, Any] = Body(...)):
    """SSE streaming endpoint for AI Workflow Designer. Yields progress events."""
    from agent_worker.universal.handler import WORKFLOW_DESIGNER_INSTRUCTION
    from agent_worker.universal.langgraph_agent import run_agent_stream
    import json as _json

    designer_tools = [
        "load_workflow_db", "save_workflow_db", "delete_workflow",
        "list_workflows", "validate_workflow", "load_workflow_yaml",
    ]

    payload = {
        "message": body.get("message", ""),
        "workflow_id": body.get("workflow_id"),
        "version": body.get("version"),
    }

    def event_generator():
        try:
            for event in run_agent_stream(
                instruction=WORKFLOW_DESIGNER_INSTRUCTION,
                payload=payload,
                tool_names=designer_tools,
                max_turns=25,
                timeout=120.0,
                max_tokens=4096,
            ):
                yield f"data: {_json.dumps(event, ensure_ascii=False)}\n\n"
        except Exception as e:
            log.exception("AI designer stream failed")
            yield f"data: {_json.dumps({'type': 'final', 'result': {'error': str(e)}}, ensure_ascii=False)}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/workflows/{workflow_id}/{version}")
def api_get_workflow(workflow_id: str, version: str = "1.0"):
    wf = _get_wf(workflow_id, version)
    return wf.model_dump(by_alias=True)


@app.post("/api/workflows/{workflow_id}/{version}")
def api_save_workflow(workflow_id: str, version: str, body: Dict[str, Any]):
    from shared.models import BizWorkflowDef
    try:
        wf_data = {"workflow_id": workflow_id, "version": version, **body}
        wf = BizWorkflowDef.model_validate(wf_data)
        def_id = _db_repo.save_workflow(wf)
        return {"id": def_id, "workflow_id": workflow_id, "version": version}
    except Exception as e:
        log.exception("Failed to save workflow %s v%s: %s", workflow_id, version, e)
        return JSONResponse(
            content={"error": f"Save failed: {e}"},
            status_code=500,
        )


@app.delete("/api/workflows/{workflow_id}/{version}")
def api_delete_workflow(workflow_id: str, version: str):
    """워크플로우 정의와 연결된 모든 데이터(인스턴스, 상태, 전이)를 삭제."""
    try:
        _db_repo.delete_workflow(workflow_id, version)
        return {"status": "deleted", "workflow_id": workflow_id, "version": version}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Workflow not found")


@app.get("/api/instances")
def api_list_instances(status: str = None, limit: int = 50):
    return _db_repo.list_instances(status=status, limit=limit)


@app.get("/api/instances/{instance_id}")
def api_get_instance(instance_id: str):
    detail = _db_repo.get_instance_detail(instance_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Instance not found")
    return detail


# ═══════════════════════════════════════════════════════════════
# UI Pages
# ═══════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
def index():
    return _render("index.html",
        workflows=_db_repo.list_workflows(),
        instances=_db_repo.list_instances(limit=10),
    )


@app.get("/workflows", response_class=HTMLResponse)
def workflow_list_page():
    return _render("workflows.html",
        workflows=_db_repo.list_workflows(),
    )


@app.get("/workflows/{workflow_id}/{version}", response_class=HTMLResponse)
def workflow_detail_page(workflow_id: str, version: str = "1.0"):
    wf = _get_wf(workflow_id, version)
    return _render("workflow_detail.html",
        wf=wf,
        wf_dict=wf.model_dump(by_alias=True),
    )


@app.get("/workflows/{workflow_id}/{version}/edit", response_class=HTMLResponse)
def workflow_edit_page(workflow_id: str, version: str = "1.0"):
    from shared.models import BizWorkflowDef
    try:
        wf = _get_wf(workflow_id, version)
        is_new = False
    except HTTPException:
        wf = BizWorkflowDef(
            workflow_id=workflow_id, version=version,
            description="", states=[], transitions=[],
        )
        is_new = True
    return _render("workflow_edit.html",
        wf=wf,
        wf_dict=wf.model_dump(by_alias=True),
        is_new=is_new,
    )


@app.get("/workflows/new", response_class=HTMLResponse)
def new_workflow_page():
    from shared.models import BizWorkflowDef
    wf = BizWorkflowDef(
        workflow_id="new_workflow", version="1.0",
        description="", states=[], transitions=[],
    )
    return _render("workflow_edit.html",
        wf=wf,
        wf_dict=wf.model_dump(by_alias=True),
        is_new=True,
    )


# ═══════════════════════════════════════════════════════════════
# AI Workflow Designer
# ═══════════════════════════════════════════════════════════════

@app.get("/workflows/ai-create", response_class=HTMLResponse)
def ai_workflow_create_page():
    return _render("workflow_ai_create.html",
        wf_id=None, wf_ver=None, initial_message=None,
    )


@app.get("/workflows/{workflow_id}/{version}/ai-edit", response_class=HTMLResponse)
def ai_workflow_edit_page(workflow_id: str, version: str = "1.0"):
    try:
        wf = _get_wf(workflow_id, version)
        return _render("workflow_ai_create.html",
            wf_id=workflow_id, wf_ver=version,
            initial_message=f"Loaded workflow '{workflow_id}' v{version} with {len(wf.states)} states and {len(wf.transitions)} transitions. How would you like to modify it?",
        )
    except HTTPException:
        return _render("workflow_ai_create.html",
            wf_id=workflow_id, wf_ver=version,
            initial_message=f"Workflow '{workflow_id}' v{version} not found. I can help you create a new one.",
        )


@app.post("/api/workflows/ai-design")
def ai_workflow_design(body: Dict[str, Any] = Body(...)):
    """Chat endpoint for AI Workflow Designer. Calls LangGraph agent with workflow tools."""
    from agent_worker.universal.handler import WORKFLOW_DESIGNER_INSTRUCTION
    from agent_worker.universal.langgraph_agent import run_agent

    designer_tools = [
        "load_workflow_db", "save_workflow_db", "delete_workflow",
        "list_workflows", "validate_workflow", "load_workflow_yaml",
    ]

    payload = {
        "message": body.get("message", ""),
        "workflow_id": body.get("workflow_id"),
        "version": body.get("version"),
    }

    try:
        result = run_agent(
            instruction=WORKFLOW_DESIGNER_INSTRUCTION,
            payload=payload,
            tool_names=designer_tools,
            max_turns=25,
            timeout=120.0,
            max_tokens=4096,
        )

        if isinstance(result, dict) and "error" in result:
            return JSONResponse(
                content={"message": f"Agent error: {result['error']}", "workflow": None, "action": None},
                status_code=500,
            )

        # Extract structured response from agent result
        message = result.get("message") or result.get("llm_output") or "Task completed."
        workflow = result.get("workflow", None)
        action = result.get("action", None)

        # If no structured action but we have a workflow in the result, derive action
        if not action and workflow:
            action = "show_preview"

        return {
            "message": message,
            "workflow": workflow,
            "action": action,
        }
    except Exception as e:
        log.exception("AI designer failed")
        return JSONResponse(
            content={"message": f"Error: {e}", "workflow": None, "action": None},
            status_code=500,
        )


@app.get("/instances", response_class=HTMLResponse)
def instances_page(status: str = None):
    return _render("instances.html",
        instances=_db_repo.list_instances(status=status, limit=100),
        status_filter=status,
    )


@app.get("/run", response_class=HTMLResponse)
def run_page():
    return _render("run.html",
        workflows=_db_repo.list_workflows(),
    )


@app.get("/instances/{instance_id}/progress", response_class=HTMLResponse)
def instance_progress_page(instance_id: str):
    detail = _db_repo.get_instance_detail(instance_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Instance not found")
    import json
    wf_def = None
    state_map = {}
    try:
        wf_def = _db_repo.load(detail.get("workflow_id", ""), detail.get("workflow_version", "1.0"))
        for s in wf_def.states:
            state_map[s.id] = {
                "inputs": s.inputs,
                "tools": s.tools,
                "output_schema": s.output_schema,
                "instruction": s.instruction[:200] if s.instruction else None,
                "description": s.description,
            }
    except Exception:
        pass

    # Build JSON data for SVG flowchart
    states_json = json.dumps([])
    transitions_json = json.dumps([])
    branches_json = json.dumps({})
    step_statuses_json = json.dumps({})
    total_steps = 0
    completed_steps = 0
    progress_percent = 0

    if wf_def is not None:
        states_json = json.dumps([
            {"id": s.id, "type": s.type, "description": s.description, "task_type": s.task}
            for s in wf_def.states
        ])
        transitions_json = json.dumps([
            {"from": t.from_, "to": t.to, "when": t.when}
            for t in wf_def.transitions
        ])
        branches_json = json.dumps({
            s.id: [
                {"id": b.id, "task": b.task, "description": getattr(b, "description", "")}
                for b in s.branches
            ]
            for s in wf_def.states if s.type == "parallel"
        })

        step_statuses = {}
        for step in detail.get("steps", []):
            step_statuses[step["state_id"]] = step["status"]
        step_statuses_json = json.dumps(step_statuses)

        task_states = [s for s in wf_def.states if s.type not in ("start", "end")]
        total_steps = len(task_states)
        completed_steps = sum(1 for s in step_statuses.values() if s in ("OK", "FAIL"))
        progress_percent = int(completed_steps / total_steps * 100) if total_steps > 0 else 0

    return _render("instance_progress.html",
        inst=detail, state_map=state_map,
        states_json=states_json,
        transitions_json=transitions_json,
        branches_json=branches_json,
        step_statuses_json=step_statuses_json,
        total_steps=total_steps,
        completed_steps=completed_steps,
        progress_percent=progress_percent,
    )


def _extract_md_from_result(result: Dict[str, Any]) -> str:
    """result에서 markdown content를 다양한 포맷으로부터 추출."""
    # 1) 직접 markdown 키 (poc_doc_generation)
    for key in ("markdown", "GENERATE_MD"):
        md = result.get(key, "")
        if md:
            return md

    # 2) llm_output fallback: JSON string 안에 markdown/document/content/GENERATE_MD 키
    llm = result.get("llm_output", "")
    if isinstance(llm, str) and llm.strip():
        text = llm.strip()
        if text.startswith("{"):
            try:
                parsed = json.loads(text)
                for key in ("markdown", "document", "content", "GENERATE_MD"):
                    val = parsed.get(key, "")
                    if val and isinstance(val, str):
                        return val
            except json.JSONDecodeError:
                # 3) Truncated JSON — markdown 값이 중간에 잘린 경우
                # {"title":"...","markdown":"# 내용..." 형태에서 markdown 값 추출
                import re
                # "markdown": 다음의 값 추출 (Truncated JSON이어도)
                m = re.search(r'"(?:markdown|document|content|GENERATE_MD)"\s*:\s*"(.+)', text, re.DOTALL)
                if m:
                    raw = m.group(1)
                    # Remove trailing unclosed string artifacts
                    raw = raw.rstrip('"}').rstrip('"')
                    # Unescape JSON escapes
                    raw = raw.replace('\\n', '\n').replace('\\t', '\t').replace('\\"', '"')
                    if len(raw) > 100:
                        return raw
        # 4) JSON이 아닌 raw 텍스트 자체가 markdown일 수 있음
        if text.startswith("#") or text.startswith("```"):
            return text

    return ""


def _extract_title_from_result(result: Dict[str, Any], default: str = "POC Documentation") -> str:
    """result에서 title 추출 (다양한 포맷)."""
    for key in ("title", "GENERATE_MD"):
        val = result.get(key, "")
        if val and isinstance(val, str):
            # If we got GENERATE_MD value, derive title from first line
            if key == "GENERATE_MD":
                first_line = val.strip().split("\n")[0]
                return first_line.lstrip("#").strip() or default
            return val
    llm = result.get("llm_output", "")
    if isinstance(llm, str) and llm.strip().startswith("{"):
        try:
            parsed = json.loads(llm)
            return parsed.get("title", default)
        except json.JSONDecodeError:
            pass
    return default


@app.get("/instances/{instance_id}/output", response_class=HTMLResponse)
def instance_output_page(instance_id: str):
    detail = _db_repo.get_instance_detail(instance_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Instance not found")
    # Find markdown output from the report-generating step
    md_content = ""
    title = "POC Documentation"
    md_state_ids = ("GENERATE_REPORT", "GENERATE_MD")
    for step in reversed(detail.get("steps", [])):
        if step.get("state_id") in md_state_ids and step.get("status") == "OK":
            result = step.get("result", {})
            md_content = _extract_md_from_result(result)
            title = _extract_title_from_result(result, title)
            break
    return _render("instance_output.html",
        inst=detail, md_content=md_content, doc_title=title)


@app.get("/instances/{instance_id}", response_class=HTMLResponse)
def instance_detail_page(instance_id: str):
    detail = _db_repo.get_instance_detail(instance_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Instance not found")
    return _render("instance_detail.html", inst=detail)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


# ═══════════════════════════════════════════════════════════════
# Mock API Endpoints (for testing claim_basic_adjudication)
# ═══════════════════════════════════════════════════════════════

_MOCK_POLICIES = {
    "POL-HEALTH-8842": {
        "customer_id": "CUST-001",
        "product_code": "HEALTH-GOLD",
        "coverage_type": "실손의료비",
        "max_benefit": 50000000,
        "deductible": 500000,
        "premium_status": "정상납입",
        "status": "active",
    },
    "POL-AUTO-1234": {
        "customer_id": "CUST-002",
        "product_code": "AUTO-STANDARD",
        "coverage_type": "자동차종합",
        "max_benefit": 100000000,
        "deductible": 200000,
        "premium_status": "정상납입",
        "status": "active",
    },
    "POL-LIFE-5678": {
        "customer_id": "CUST-003",
        "product_code": "LIFE-VIP",
        "coverage_type": "종신보험",
        "max_benefit": 200000000,
        "deductible": 0,
        "premium_status": "연체",
        "status": "lapse",
    },
}

_MOCK_CUSTOMERS = {
    "CUST-001": {
        "name": "홍길동",
        "birth_date": "1985-03-15",
        "phone": "010-1234-5678",
        "email": "hong@example.com",
        "refund_account": "국민은행 123-45-67890",
        "claim_history": ["CLM-2023-001", "CLM-2024-012"],
        "claim_count_3y": 2,
    },
    "CUST-002": {
        "name": "김영희",
        "birth_date": "1990-07-22",
        "phone": "010-9876-5432",
        "email": "kim@example.com",
        "refund_account": "신한은행 987-65-43210",
        "claim_history": [],
        "claim_count_3y": 0,
    },
    "CUST-003": {
        "name": "이철수",
        "birth_date": "1975-11-02",
        "phone": "010-5555-6666",
        "email": "lee@example.com",
        "refund_account": "우리은행 555-66-77777",
        "claim_history": ["CLM-2022-005", "CLM-2023-008", "CLM-2023-015", "CLM-2024-001", "CLM-2024-007"],
        "claim_count_3y": 5,
    },
}

_MOCK_CLAIM_RESULTS = {}


@app.get("/api/mock/policies/{policy_no}")
def mock_get_policy(policy_no: str):
    data = _MOCK_POLICIES.get(policy_no)
    if not data:
        return JSONResponse(status_code=404, content={"error": "Policy not found", "code": 404})
    return {"status": "ok", "data": data}


@app.get("/api/mock/customers/{customer_id}")
def mock_get_customer(customer_id: str):
    data = _MOCK_CUSTOMERS.get(customer_id)
    if not data:
        return JSONResponse(status_code=404, content={"error": "Customer not found", "code": 404})
    return {"status": "ok", "data": data}


@app.post("/api/mock/fraud/evaluate")
def mock_fraud_evaluate(body: dict = Body(...)):
    amount = int(float(body.get("amount", 0)))
    risk_score = int(body.get("risk_score", 0))
    claim_count = 2  # simplified

    score = min(100, amount // 200000 + risk_score // 2 + claim_count * 5)
    indicators = []
    if amount > 10000000:
        indicators.append("고액청구")
    if risk_score > 70:
        indicators.append("고위험군")
    if claim_count > 3:
        indicators.append("다건청구이력")

    action = "approve" if score < 40 else "review" if score < 70 else "block"

    return {
        "status": "ok",
        "data": {
            "score": score,
            "indicators": indicators if indicators else ["정상"],
            "action": action,
        },
    }


@app.post("/api/mock/claims/{claim_id}/result")
def mock_submit_claim_result(claim_id: str, body: dict = Body(...)):
    _MOCK_CLAIM_RESULTS[claim_id] = body
    return {
        "status": "ok",
        "data": {
            "reference_number": f"REF-{claim_id}-{len(_MOCK_CLAIM_RESULTS)}",
            "processed_at": datetime.now().isoformat(),
        },
    }


# ═══════════════════════════════════════════════════════════════
# Test: 워크플로우 등록 + 실행 (단일 엔드포인트)
# ═══════════════════════════════════════════════════════════════

class TestRunRequest(BaseModel):
    workflow_id: str
    version: str = "1.0"
    input: Dict[str, Any]


@app.post("/api/test/run")
def api_test_run(req: TestRunRequest) -> Dict[str, Any]:
    """
    1. YAML 파일을 찾아 DB에 등록 (이미 있으면 업데이트)
    2. 워크플로우 실행
    3. 결과 반환 (동기)
    """
    import threading
    from orchestrator.instance import ClaimInstance
    from shared.workflow_loader import BizWorkflowLoader

    # 1) YAML → DB 등록
    loader = BizWorkflowLoader()
    base_dir = os.path.join(PROJECT_ROOT, "biz_workflows")
    pattern = os.path.join(base_dir, f"{req.workflow_id}_v{req.version}.yaml")
    if os.path.exists(pattern):
        wf = loader.load(pattern)
        _db_repo.save_workflow(wf)
        log.info("Test: registered workflow %s v%s from YAML", req.workflow_id, req.version)
    else:
        wf = _get_wf(req.workflow_id, req.version)

    # 2) Instance 생성
    inst = ClaimInstance(
        workflow_id=req.workflow_id,
        workflow_version=req.version,
        initial_input=req.input,
    )
    wf_def_id = _db_repo._lookup_wf_def_id(req.workflow_id, req.version)
    _db_repo.save_instance(
        instance_id=inst.instance_id,
        wf_def_id=wf_def_id,
        workflow_id=req.workflow_id,
        workflow_version=req.version,
        initial_input=req.input,
    )
    _db_repo.update_instance_state(inst.instance_id, current_state="START", status="RUNNING")

    # 3) Engine 실행 (동기)
    try:
        _engine.run(
            workflow_id=req.workflow_id,
            version=req.version,
            initial_input=req.input,
            existing_instance=inst,
        )
        status = "completed"
    except Exception as e:
        log.exception("Test run failed for %s", inst.instance_id)
        _db_repo.update_instance_state(inst.instance_id, current_state="ERROR", status="FAILED")
        return {"instance_id": inst.instance_id, "status": "FAILED", "error": str(e)}

    # 4) 결과 조회
    detail = _db_repo.get_instance_detail(inst.instance_id)
    return {
        "instance_id": inst.instance_id,
        "status": status,
        "workflow_id": req.workflow_id,
        "version": req.version,
        "steps": [
            {
                "state_id": s.state_id,
                "task_type": s.task_type,
                "status": s.status,
                "result": s.result,
                "error": s.error,
            }
            for s in (inst.history if hasattr(inst, "history") else [])
        ],
        "output": detail.get("output") if isinstance(detail, dict) else None,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("APP_PORT", "8000")))
