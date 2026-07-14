# dapr_workflow/workflows.py
from __future__ import annotations
import logging
from typing import Any, Dict, Generator, List

import dapr.ext.workflow as wf
from dapr.ext.workflow import DaprWorkflowContext

from .activities import publish_task_activity

log = logging.getLogger("dapr_wf.workflow")


def execute_task_workflow(
    ctx: DaprWorkflowContext,
    input_: Dict[str, Any],
) -> Generator[Any, Any, Dict[str, Any]]:
    """
    범용 Task 실행 워크플로우.
    1. publish → agent.tasks (instruction/tools/output_schema 포함)
    2. wait for external event "TASK_RESULT"
    3. 결과 반환
    """
    instance_id = ctx.instance_id
    task_type = input_["task_type"]
    payload = input_.get("payload", {})

    # 1) publish (task_id == workflow_instance_id)
    activity_input = {
        "task_id": instance_id,
        "workflow_instance_id": instance_id,
        "task_type": task_type,
        "payload": payload,
    }
    # instruction/tools/output_schema/timeout_sec/max_tokens가 있으면 전달
    for key in ("instruction", "tools", "output_schema", "timeout_sec", "max_tokens"):
        if key in input_:
            activity_input[key] = input_[key]

    yield ctx.call_activity(publish_task_activity, input=activity_input)

    # 2) wait for external event from result_bridge
    try:
        result_event: Dict[str, Any] = yield ctx.wait_for_external_event("TASK_RESULT")
    except Exception as e:
        log.error("execute_task wf %s failed: %s", instance_id, e)
        return {
            "status": "FAIL",
            "error": f"workflow error: {e}",
            "task_id": instance_id,
        }

    return result_event


def parallel_execute_workflow(
    ctx: DaprWorkflowContext,
    input_: Dict[str, Any],
) -> Generator[Any, Any, Dict[str, Any]]:
    """
    Fan-out/Fan-in 병렬 태스크 실행 워크플로우.
    1. 모든 브랜치 publish (병렬 - when_all)
    2. 모든 브랜치 결과 대기 (병렬 - when_all)
    3. {branch_id → result} 취합하여 반환
    """
    branches: List[Dict[str, Any]] = input_["branches"]
    instance_id = ctx.instance_id

    # ── Fan-out: 모든 브랜치 publish (병렬) ──
    pub_tasks = [
        ctx.call_activity(
            publish_task_activity,
            input={
                "task_id": f"{instance_id}/{b['id']}",
                "workflow_instance_id": instance_id,
                "task_type": b["task_type"],
                "payload": b.get("payload", {}),
                "instruction": b.get("instruction"),
                "tools": b.get("tools", []),
                "output_schema": b.get("output_schema"),
                "timeout_sec": b.get("timeout_sec"),
                "max_tokens": b.get("max_tokens"),
                "event_name": f"BRANCH_{b['id']}",
            },
        )
        for b in branches
    ]
    yield wf.when_all(pub_tasks)
    log.info("parallel wf %s: all %d branches published", instance_id, len(branches))

    # ── Fan-in: 모든 브랜치 결과 대기 (병렬) ──
    wait_tasks = [
        ctx.wait_for_external_event(f"BRANCH_{b['id']}")
        for b in branches
    ]
    try:
        branch_results: List[Dict[str, Any]] = yield wf.when_all(wait_tasks)
    except Exception as e:
        log.error("parallel wf %s failed: %s", instance_id, e)
        return {"status": "FAIL", "error": f"parallel workflow error: {e}"}

    log.info("parallel wf %s: all %d branches completed", instance_id, len(branches))

    # ── 취합: { branch_id → result } ──
    return {
        "branches": {
            b["id"]: result
            for b, result in zip(branches, branch_results)
        }
    }