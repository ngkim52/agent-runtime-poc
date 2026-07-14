# agent_worker/handlers/document_classification.py
from __future__ import annotations
import asyncio
from shared.models import TaskRequest, TaskResult
from ..registry import register
from .base import TaskHandler


@register("document_classification")
class DocumentClassificationHandler(TaskHandler):
    task_type = "document_classification"

    async def handle(self, req: TaskRequest) -> TaskResult:
        image_refs = req.payload.get("image_refs", [])
        await asyncio.sleep(0.1)
        docs = []
        has_claim_form = False
        for i, ref in enumerate(image_refs):
            doc_type = "CLAIM_FORM" if i == 0 else ("DIAGNOSIS" if i == 1 else "RECEIPT")
            if doc_type == "CLAIM_FORM":
                has_claim_form = True
            docs.append({"image_ref": ref, "doc_type": doc_type, "confidence": 0.95})
        return self.ok(req, has_claim_form=has_claim_form, documents=docs)