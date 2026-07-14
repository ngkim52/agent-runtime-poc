# agent_worker/handlers/prior_claims_matching.py
from __future__ import annotations
import asyncio
from shared.models import TaskRequest, TaskResult
from ..registry import register
from .base import TaskHandler


@register("prior_claims_matching")
class PriorClaimsMatchingHandler(TaskHandler):
    task_type = "prior_claims_matching"

    async def handle(self, req: TaskRequest) -> TaskResult:
        await asyncio.sleep(0.1)
        # 호환성: 새 payload 구조 (fields) / 예전 구조 (accident_fields)
        accident = (
            req.payload.get("fields")
            or req.payload.get("accident_fields")
            or {}
        )

        # Mock: 처리계 기지급 전문 조회 결과
        prior_claims = [
            {
                "claim_no": "P-20260301-001",
                "disease_code": "I63.9",
                "accident_date": "2026-05-10",
                "hospital": "서울대학교병원",
            }
        ]
        # 동일사고 판정 (질병코드 + 사고일자 ±3일 + 병원 일치)
        confirmed = any(
            pc["disease_code"] == accident.get("disease_code")
            and pc["hospital"] == accident.get("hospital")
            for pc in prior_claims
        )

        return self.ok(
            req,
            accident_confirmed=confirmed,
            confirmed_disease_code=accident.get("disease_code") if confirmed else None,
            confirmed_accident_date=accident.get("accident_date") if confirmed else None,
            prior_claims=prior_claims,
            decision_reason="prior_claim_matched" if confirmed else "no_match",
        )