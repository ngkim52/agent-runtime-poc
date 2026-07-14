# agent_worker/handlers/image_data_extraction.py
from __future__ import annotations
import asyncio
from shared.models import TaskRequest, TaskResult
from ..registry import register
from .base import TaskHandler


@register("image_data_extraction")
class ImageDataExtractionHandler(TaskHandler):
    task_type = "image_data_extraction"

    async def handle(self, req: TaskRequest) -> TaskResult:
        await asyncio.sleep(0.2)
        # Mock: 청구서에서 추출했다고 가정
        fields = {
            "insured_name": "홍길동",
            "ssn": "900101-1******",
            "account_no": "110-123-456789",
        }
        return self.ok(req, extraction_confidence=0.92, fields=fields)