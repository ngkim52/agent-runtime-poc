# dapr_workflow/main.py
from __future__ import annotations
import asyncio
import json
import os
import sys
import logging
from typing import Any, Dict, List, Optional

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dapr.ext.workflow import WorkflowRuntime

from dapr_workflow.workflows import execute_task_workflow, parallel_execute_workflow
from dapr_workflow.activities import publish_task_activity
from dapr_workflow.result_bridge import router as bridge_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("dapr_workflow")

# ── 1) Workflow Runtime 등록 ──
wf_runtime = WorkflowRuntime()
wf_runtime.register_workflow(execute_task_workflow)
wf_runtime.register_workflow(parallel_execute_workflow)
wf_runtime.register_activity(publish_task_activity)

# ── 2) FastAPI 앱 ──
app = FastAPI(title="Generic Dapr Workflow Layer")
app.include_router(bridge_router)

DAPR_HTTP_PORT = os.getenv("DAPR_HTTP_PORT", "3500")
WF_COMPONENT = os.getenv("DAPR_WF_COMPONENT", "dapr")
WF_NAME = "execute_task_workflow"
PARALLEL_WF_NAME = "parallel_execute_workflow"


class ExecuteTaskRequest(BaseModel):
    task_type: str
    payload: Dict[str, Any]
    instruction: Optional[str] = None
    tools: List[str] = []
    output_schema: Optional[Dict[str, Any]] = None
    timeout_sec: Optional[float] = None
    max_tokens: Optional[int] = None


class ExecuteParallelRequest(BaseModel):
    branches: List[Dict[str, Any]]


# ── 3) Workflow 실행 엔드포인트 (Orchestrator에서 호출) ──
@app.post("/workflows/execute")
async def execute_workflow(req: ExecuteTaskRequest):
    """
    단일 Task 실행. Orchestrator가 호출.
    """
    instance_id = await _start_workflow(WF_NAME, req.model_dump())
    log.info("started wf instance_id=%s task_type=%s timeout=%s",
             instance_id, req.task_type, req.timeout_sec)
    poll_timeout = int(req.timeout_sec or 360)
    output = await _poll_workflow(instance_id, timeout_sec=poll_timeout)
    return {"wf_instance_id": instance_id, "output": output}


@app.post("/workflows/execute-parallel")
async def execute_parallel(req: ExecuteParallelRequest):
    """
    병렬(Fan-out/Fan-in) Task 실행. Orchestrator가 호출.
    """
    instance_id = await _start_workflow(PARALLEL_WF_NAME, req.model_dump())
    # 브랜치 중 최대 timeout_sec 기준 polling
    max_timeout = max(
        (int(b.get("timeout_sec", 300)) for b in req.branches),
        default=300,
    )
    log.info("started parallel wf instance_id=%s branches=%d max_timeout=%ss",
             instance_id, len(req.branches), max_timeout)
    try:
        output = await _poll_workflow(instance_id, timeout_sec=max_timeout)
    except TimeoutError:
        log.warning("parallel wf %s timed out after %ss", instance_id, max_timeout)
        raise HTTPException(
            status_code=504,
            detail=f"parallel workflow {instance_id} timed out after {max_timeout}s",
        )
    return {"wf_instance_id": instance_id, "output": output}


async def _start_workflow(wf_name: str, body: Dict[str, Any]) -> str:
    import httpx
    url = f"http://localhost:{DAPR_HTTP_PORT}/v1.0-alpha1/workflows/{WF_COMPONENT}/{wf_name}/start"
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(url, json=body)
        r.raise_for_status()
        data = r.json()
    return data.get("instanceID") or data.get("instance_id") or data.get("id")


async def _poll_workflow(instance_id: str, timeout_sec: int = 120) -> Dict[str, Any]:
    import httpx
    import time

    url = f"http://localhost:{DAPR_HTTP_PORT}/v1.0-alpha1/workflows/{WF_COMPONENT}/{instance_id}"
    elapsed = 0.0
    poll_interval = 0.5
    async with httpx.AsyncClient(timeout=10.0) as client:
        while elapsed < timeout_sec:
            r = await client.get(url)
            # Dapr runtime이 가끔 일시적인 5xx를 반환 (초기화 중 등) — 경고만 남기고 계속 폴링
            if r.is_server_error:
                log.warning("Dapr runtime 5xx (%.0fs elapsed): %s %s", elapsed, r.status_code, url)
                await asyncio.sleep(poll_interval)
                elapsed += poll_interval
                continue
            r.raise_for_status()  # 4xx는 그대로 raise
            state = r.json()
            runtime_status = state.get("runtimeStatus") or state.get("runtime_status")
            if runtime_status in ("COMPLETED", "Completed"):
                raw_output = state.get("serializedOutput") or state.get("properties", {}).get("dapr.workflow.output")
                return _parse_output(raw_output)
            if runtime_status in ("FAILED", "TERMINATED", "Failed", "Terminated"):
                raise RuntimeError(f"workflow {instance_id} ended with status={runtime_status}")
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
    raise TimeoutError(f"workflow {instance_id} timed out after {timeout_sec}s")


@staticmethod
def _parse_output(raw: Any) -> Dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {"raw": raw}
    return {"raw": str(raw)}


@app.on_event("startup")
def startup():
    log.info("▶ Workflow runtime starting...")
    wf_runtime.start()
    log.info("✓ Workflow runtime started")


@app.on_event("shutdown")
def shutdown():
    log.info("▶ Workflow runtime shutting down...")
    wf_runtime.shutdown()


@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "dapr_workflow_layer"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("APP_PORT", "8002")))