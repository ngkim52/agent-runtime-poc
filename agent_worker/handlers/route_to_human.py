# agent_worker/handlers/route_to_human.py
from __future__ import annotations
from shared.models import TaskRequest, TaskResult
from ..registry import register
from .base import TaskHandler


@register("route_to_human")
class RouteToHumanHandler(TaskHandler):
    task_type = "route_to_human"

    async def handle(self, req: TaskRequest) -> TaskResult:
        return self.ok(req, queue_id="HUMAN_REVIEW_Q1", routed=True)