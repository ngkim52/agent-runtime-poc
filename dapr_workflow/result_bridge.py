# dapr_workflow/result_bridge.py
"""
agent.results 토픽 구독 → Dapr Workflow의 raise_event 로 라우팅.

워커가 publish 한 TaskResult 는 workflow_instance_id 필드를 가지므로,
그 ID 의 WF 인스턴스에 "TASK_RESULT" 이벤트를 발생시킨다.
"""
from __future__ import annotations
import asyncio
import logging
import os
from typing import Any, Dict

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

log = logging.getLogger("result_bridge")

PUBSUB_NAME = os.getenv("DAPR_PUBSUB_NAME", "pubsub")
RESULT_TOPIC = os.getenv("AGENT_RESULT_TOPIC", "agent.results")
DAPR_HTTP_PORT = os.getenv("DAPR_HTTP_PORT", "3500")
WF_COMPONENT = os.getenv("DAPR_WF_COMPONENT", "dapr")   # Dapr WF 엔진 기본명

router = APIRouter()


@router.get("/dapr/subscribe")
def dapr_subscribe():
    return [
        {
            "pubsubname": PUBSUB_NAME,
            "topic": RESULT_TOPIC,
            "route": "/bridge/result",
        }
    ]


@router.post("/bridge/result")
async def on_result(request: Request):
    envelope: Dict[str, Any] = await request.json()
    data = envelope.get("data", envelope)

    instance_id = data.get("workflow_instance_id")
    if not instance_id:
        log.error("No workflow_instance_id in result: %s", data)
        return JSONResponse(status_code=200, content={"status": "DROP"})

    event_name = data.get("event_name", "TASK_RESULT")
    task_id = data.get("task_id", "?")
    log.info("📥 result received task_id=%s wf=%s status=%s event_name=%s",
             task_id, instance_id, data.get("status"), event_name)
    log.debug("result data keys=%s", list(data.keys()))

    url = (
        f"http://localhost:{DAPR_HTTP_PORT}"
        f"/v1.0-alpha1/workflows/{WF_COMPONENT}/{instance_id}/raiseEvent/{event_name}"
    )
    last_error = None
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.post(url, json=data)
                r.raise_for_status()
            log.info("→ raised event %s to wf=%s (attempt %d)", event_name, instance_id, attempt + 1)
            return {"status": "SUCCESS"}
        except httpx.HTTPStatusError as e:
            last_error = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
            if e.response.status_code in (404, 400):
                # 404 = workflow not found (maybe already completed/timed out)
                # 400 = bad request (maybe duplicate event)
                log.warning("raiseEvent %s wf=%s attempt %d non-retriable: %s",
                            event_name, instance_id, attempt + 1, last_error)
                return {"status": "SUCCESS"}  # non-retriable, don't retry
            log.warning("raiseEvent %s wf=%s attempt %d failed: %s (retrying...)",
                        event_name, instance_id, attempt + 1, last_error)
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            log.warning("raiseEvent %s wf=%s attempt %d exception: %s (retrying...)",
                        event_name, instance_id, attempt + 1, last_error)
        await asyncio.sleep(1)

    log.error("raiseEvent FAILED after 3 attempts wf=%s event=%s: %s",
              instance_id, event_name, last_error)
    return {"status": "SUCCESS"}