# agent_worker/main.py
from __future__ import annotations
import asyncio
import os
import sys
import json
import logging
from typing import Any, Dict

# sys.path: 프로젝트 루트 인식
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from shared.models import TaskRequest
from agent_worker import handlers  # noqa: F401  ← Registry 자동 등록
from agent_worker.dispatcher import TaskDispatcher
from agent_worker.registry import HandlerRegistry

# ─────────────────────────────────────────────
PUBSUB_NAME = os.getenv("DAPR_PUBSUB_NAME", "pubsub")
TASK_TOPIC = os.getenv("AGENT_TASK_TOPIC", "agent.tasks")
RESULT_TOPIC = os.getenv("AGENT_RESULT_TOPIC", "agent.results")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("agent_worker")

app = FastAPI(title="Generic Agent Worker")
dispatcher = TaskDispatcher()


# ── 1) Dapr Pub/Sub 구독 선언 ──
@app.get("/dapr/subscribe")
def dapr_subscribe():
    return [
        {
            "pubsubname": PUBSUB_NAME,
            "topic": TASK_TOPIC,
            "route": "/agent/task",
        }
    ]


# ── 2) 헬스/디버그 ──
@app.get("/healthz")
def healthz():
    return {"status": "ok", "registered_handlers": HandlerRegistry.list_registered()}


# ── 3) 메인 수신 엔드포인트 ──
@app.post("/agent/task")
async def on_task(request: Request):
    """
    Dapr Pub/Sub은 CloudEvents 포맷으로 메시지를 보냄.
    실제 페이로드는 envelope['data'] 안에 들어있음.
    """
    envelope: Dict[str, Any] = await request.json()
    data = envelope.get("data", envelope)  # Dapr이 아닌 직접 호출도 허용

    try:
        req = TaskRequest.model_validate(data)
    except Exception as e:
        log.error("Invalid TaskRequest: %s | raw=%s", e, data)
        # Dapr에 DROP 신호 (재시도 안 함)
        return JSONResponse(status_code=200, content={"status": "DROP"})

    result = await dispatcher.dispatch(req)
    result_dict = result.model_dump()

    # 결과 publish (Dapr 환경에서만 실제 publish)
    await _publish_result(result_dict)

    # 직접 호출(Dapr 없음) 시 result도 함께 반환
    return {"status": "SUCCESS", "result": result_dict}


# ── 4) 결과 publish ──
async def _publish_result(result_dict: Dict[str, Any]) -> None:
    """
    Dapr 가 떠 있으면 sidecar HTTP API로 publish.
    POC 초반(Dapr 없을 때)에는 콘솔에 찍기만 함.
    연결 실패 시에도 500을 반환하지 않도록 예외 처리.
    """
    dapr_port = os.getenv("DAPR_HTTP_PORT")
    if not dapr_port:
        log.info("[NO-DAPR] would publish to %s: %s", RESULT_TOPIC, json.dumps(result_dict, ensure_ascii=False))
        return

    import httpx
    url = f"http://localhost:{dapr_port}/v1.0/publish/{PUBSUB_NAME}/{RESULT_TOPIC}"
    for attempt in range(2):
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.post(url, json=result_dict)
                r.raise_for_status()
                log.info("→ published result task_id=%s (attempt %d)", result_dict.get("task_id"), attempt + 1)
                return
        except (httpx.ConnectTimeout, httpx.ConnectError) as e:
            log.warning("→ publish result failed attempt %d: %s (retrying...)", attempt + 1, e)
            if attempt == 0:
                await asyncio.sleep(1)
            else:
                log.error("→ publish result FAILED after 2 attempts task_id=%s: %s",
                          result_dict.get("task_id"), e)


if __name__ == "__main__":
    import uvicorn
    log.info("Registered handlers: %s", HandlerRegistry.list_registered())
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("APP_PORT", "8001")))