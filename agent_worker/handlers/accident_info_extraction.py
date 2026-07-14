# agent_worker/handlers/accident_info_extraction.py
from __future__ import annotations
import asyncio
from shared.models import TaskRequest, TaskResult
from ..registry import register
from .base import TaskHandler


@register("accident_info_extraction")
class AccidentInfoExtractionHandler(TaskHandler):
    task_type = "accident_info_extraction"

    async def handle(self, req: TaskRequest) -> TaskResult:
        await asyncio.sleep(0.2)
        fields = {
            "disease_code": "I63.9",         # 뇌경색 (예시)
            "accident_date": "2026-05-12",
            "hospital": "서울대학교병원",
            "doctor": "김의사",
        }
        return self.ok(req, extraction_confidence=0.88, fields=fields)