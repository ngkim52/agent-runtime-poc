# agent_worker/dispatcher.py
from __future__ import annotations
import logging
import traceback
from shared.models import TaskRequest, TaskResult
from .registry import HandlerRegistry

log = logging.getLogger("dispatcher")


class TaskDispatcher:
    async def dispatch(self, req: TaskRequest) -> TaskResult:
        log.info(
            "▶ dispatch task_id=%s wf=%s type=%s",
            req.task_id, req.workflow_instance_id, req.task_type,
        )
        try:
            handler = HandlerRegistry.get(req.task_type)
            if isinstance(handler, type):
                # 클래스 기반 핸들러
                handler = handler()
                result = await handler.handle(req)
            else:
                # 함수 기반 핸들러 (범용 AI Worker)
                import asyncio
                if asyncio.iscoroutinefunction(handler):
                    result = await handler(req)
                else:
                    result = handler(req)
            log.info("✓ done task_id=%s status=%s", req.task_id, result.status)
            return result
        except Exception as e:
            log.error("✗ failed task_id=%s err=%s", req.task_id, e)
            traceback.print_exc()
            return TaskResult(
                task_id=req.task_id,
                workflow_instance_id=req.workflow_instance_id,
                status="FAIL",
                error=f"{type(e).__name__}: {e}",
                event_name=req.event_name,
            )