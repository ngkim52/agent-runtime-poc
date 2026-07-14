# orchestrator/dapr_client.py
"""
dapr_workflow 서비스의 /workflows/execute 엔드포인트를 호출하는 클라이언트.
Dapr 미사용 시 DirectAgentClient로 agent_worker를 직접 호출.
"""
from __future__ import annotations
import logging
import os
import uuid
from typing import Any, Dict, List

import httpx

log = logging.getLogger("dapr_client")

DAPR_WF_SERVICE_URL = os.getenv("DAPR_WF_SERVICE_URL", "http://localhost:8002")
AGENT_WORKER_URL = os.getenv("AGENT_WORKER_URL", "http://localhost:8003")


class DirectAgentClient:
    """
    Dapr 없이 agent_worker를 직접 HTTP 호출.
    테스트 환경(target=_MockDaprClient) 대신 실제 agent_worker와 통신.
    """

    def __init__(self, agent_url: str | None = None):
        self.agent_url = (agent_url or AGENT_WORKER_URL).rstrip("/")

    def execute_task(
        self,
        task_type: str,
        payload: Dict[str, Any],
        instruction: str | None = None,
        tools: list[str] | None = None,
        output_schema: Dict[str, Any] | None = None,
        timeout: float = 300.0,
        max_tokens: int | None = None,
    ) -> Dict[str, Any]:
        """agent_worker의 /agent/task 를 직접 호출하여 결과 반환."""
        url = f"{self.agent_url}/agent/task"
        body = {
            "task_type": task_type,
            "payload": payload,
            "instruction": instruction,
            "tools": tools or [],
            "output_schema": output_schema,
            "timeout_sec": timeout,
            "max_tokens": max_tokens,
            "task_id": f"direct-{uuid.uuid4().hex[:8]}",
            "workflow_instance_id": "direct",
        }
        log.info("→ DirectAgent: task_type=%s timeout=%ss → %s", task_type, timeout, url)
        with httpx.Client(timeout=timeout + 10) as client:
            r = client.post(url, json=body)
            r.raise_for_status()
            resp = r.json()

        result_data = resp.get("result", {})
        output = {
            "status": result_data.get("status", "OK"),
            "result": result_data.get("result", {}),
            "error": result_data.get("error"),
        }
        log.info("← DirectAgent: task_type=%s status=%s", task_type, output["status"])
        return {"output": output, "wf_instance_id": "direct"}

    def execute_parallel(
        self,
        branches: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """병렬 브랜치를 순차적으로 실행 (Dapr when_all 대체)."""
        log.info("→ DirectAgent parallel: %d branches", len(branches))
        branch_results = {}
        all_ok = True
        for b in branches:
            bid = b["id"]
            try:
                branch_timeout = float(b.get("timeout_sec", 300))
                result = self.execute_task(
                    task_type=b["task_type"],
                    payload=b["payload"],
                    instruction=b.get("instruction"),
                    tools=b.get("tools"),
                    output_schema=b.get("output_schema"),
                    timeout=branch_timeout,
                    max_tokens=b.get("max_tokens"),
                )
                branch_results[bid] = result["output"]
                if result["output"]["status"] != "OK":
                    all_ok = False
            except Exception as e:
                branch_results[bid] = {"status": "FAIL", "error": str(e)}
                all_ok = False
        return {
            "output": {
                "branches": branch_results,
                "status": "OK" if all_ok else "FAIL",
            },
            "wf_instance_id": "direct",
        }


class DaprWorkflowClient:
    """
    dapr_workflow 서비스의 /workflows/execute 및 /workflows/execute-parallel 호출.
    실제 WF 시작 + polling은 dapr_workflow 서비스가 내부 Dapr sidecar로 처리.
    """

    def __init__(self, wf_service_url: str | None = None):
        self.wf_service_url = (wf_service_url or DAPR_WF_SERVICE_URL).rstrip("/")

    def execute_task(
        self,
        task_type: str,
        payload: Dict[str, Any],
        instruction: str | None = None,
        tools: list[str] | None = None,
        output_schema: Dict[str, Any] | None = None,
        timeout: float = 300.0,
        max_tokens: int | None = None,
    ) -> Dict[str, Any]:
        """dapr_workflow 서비스에 task 실행을 요청하고 결과를 반환받음."""
        url = f"{self.wf_service_url}/workflows/execute"
        body = {
            "task_type": task_type,
            "payload": payload,
            "instruction": instruction,
            "tools": tools or [],
            "output_schema": output_schema,
            "timeout_sec": timeout,
            "max_tokens": max_tokens,
        }
        log.info("→ calling dapr_workflow: task_type=%s timeout=%ss", task_type, timeout)
        with httpx.Client(timeout=timeout + 10) as client:
            r = client.post(url, json=body)
            r.raise_for_status()
            result = r.json()
        log.info("← dapr_workflow responded wf_instance_id=%s", result.get("wf_instance_id"))
        return result

    def execute_parallel(
        self,
        branches: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """dapr_workflow 서비스에 병렬 task 실행을 요청하고 취합 결과를 반환받음."""
        url = f"{self.wf_service_url}/workflows/execute-parallel"
        body = {"branches": branches}
        # 브랜치 중 최대 timeout_sec 기준 + buffer
        max_branch_timeout = max(
            (float(b.get("timeout_sec", 300)) for b in branches),
            default=300,
        )
        log.info("→ calling dapr_workflow parallel: branches=%d max_timeout=%ss",
                 len(branches), max_branch_timeout)
        with httpx.Client(timeout=max_branch_timeout + 10) as client:
            r = client.post(url, json=body)
            r.raise_for_status()
            result = r.json()
        log.info("← dapr_workflow parallel responded wf_instance_id=%s", result.get("wf_instance_id"))
        return result