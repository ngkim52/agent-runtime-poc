# agent_worker/handlers/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from shared.models import TaskRequest, TaskResult


class TaskHandler(ABC):
    task_type: str = ""

    @abstractmethod
    async def handle(self, req: TaskRequest) -> TaskResult:
        ...

    # 헬퍼 ──────────────────────────────────────
    def ok(self, req: TaskRequest, **result_fields) -> TaskResult:
        return TaskResult(
            task_id=req.task_id,
            workflow_instance_id=req.workflow_instance_id,
            status="OK",
            result=result_fields,
            event_name=req.event_name,
        )

    def fail(self, req: TaskRequest, error: str, **result_fields) -> TaskResult:
        return TaskResult(
            task_id=req.task_id,
            workflow_instance_id=req.workflow_instance_id,
            status="FAIL",
            result=result_fields,
            error=error,
            event_name=req.event_name,
        )