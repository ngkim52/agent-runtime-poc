# dapr_workflow/activities.py
from __future__ import annotations
import json
import logging
import os
from typing import Any, Dict

import httpx
from dapr.ext.workflow import WorkflowActivityContext

from shared.models import TaskRequest

log = logging.getLogger("dapr_wf.activity")

PUBSUB_NAME = os.getenv("DAPR_PUBSUB_NAME", "pubsub")
TASK_TOPIC = os.getenv("AGENT_TASK_TOPIC", "agent.tasks")


def publish_task_activity(ctx: WorkflowActivityContext, input_: Dict[str, Any]) -> Dict[str, Any]:
    """
    워커에게 task를 publish 하는 activity.
    input_:
        - task_id: str (= workflow instance id)
        - workflow_instance_id: str
        - task_type: str
        - payload: dict
        - instruction: optional str
        - tools: optional list[str]
        - output_schema: optional dict
    """
    req = TaskRequest(
        task_id=input_["task_id"],
        workflow_instance_id=input_["workflow_instance_id"],
        task_type=input_["task_type"],
        payload=input_.get("payload", {}),
        instruction=input_.get("instruction"),
        tools=input_.get("tools", []),
        output_schema=input_.get("output_schema"),
        timeout_sec=input_.get("timeout_sec"),
        event_name=input_.get("event_name", "TASK_RESULT"),
    )

    dapr_port = os.getenv("DAPR_HTTP_PORT", "3500")
    url = f"http://localhost:{dapr_port}/v1.0/publish/{PUBSUB_NAME}/{TASK_TOPIC}"

    body = req.model_dump()
    log.info("📤 publish task_id=%s type=%s → %s",
             req.task_id, req.task_type, TASK_TOPIC)

    with httpx.Client(timeout=5.0) as client:
        r = client.post(url, json=body)
        r.raise_for_status()

    return {"published": True, "task_id": req.task_id}