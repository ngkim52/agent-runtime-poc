# agent_worker/handlers/identity_verification.py
from __future__ import annotations
import asyncio
from shared.models import TaskRequest, TaskResult
from ..registry import register
from .base import TaskHandler


@register("identity_verification")
class IdentityVerificationHandler(TaskHandler):
    task_type = "identity_verification"

    async def handle(self, req: TaskRequest) -> TaskResult:
        await asyncio.sleep(0.1)
        extracted = req.payload.get("extracted_fields", {})
        master = req.payload.get("master_data", {})

        name_match = extracted.get("insured_name") == master.get("insured_name")
        ssn_match = extracted.get("ssn", "")[:6] == master.get("insured_ssn", "")[:6]
        acc_match = extracted.get("account_no") == master.get("account_no")

        reasons = []
        if not name_match: reasons.append("name_mismatch")
        if not ssn_match:  reasons.append("ssn_mismatch")
        if not acc_match:  reasons.append("account_mismatch")

        return self.ok(
            req,
            name_match=name_match,
            ssn_match=ssn_match,
            account_match=acc_match,
            mismatch_reasons=reasons,
        )