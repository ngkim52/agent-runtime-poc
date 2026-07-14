# agent_worker/handlers/image_download.py
from __future__ import annotations
import asyncio
from shared.models import TaskRequest, TaskResult
from ..registry import register
from .base import TaskHandler


@register("image_download")
class ImageDownloadHandler(TaskHandler):
    task_type = "image_download"

    async def handle(self, req: TaskRequest) -> TaskResult:
        claim_no = req.payload.get("claim_no", "")
        await asyncio.sleep(0.1)  # 외부 시스템 호출 흉내
        # Mock: 접수번호 끝자리가 '0'이면 0건, 아니면 5건
        count = 0 if claim_no.endswith("0") else 5
        refs = [f"s3://claims/{claim_no}/img_{i}.jpg" for i in range(count)]
        return self.ok(req, image_count=count, image_refs=refs)