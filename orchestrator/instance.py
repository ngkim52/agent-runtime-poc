# orchestrator/instance.py
from __future__ import annotations
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class StepRecord(BaseModel):
    """한 step 실행 이력"""
    state_id: str
    task_type: Optional[str] = None
    wf_instance_id: Optional[str] = None
    status: str                          # OK / FAIL / SKIP
    result: Dict[str, Any] = {}
    error: Optional[str] = None
    started_at: datetime
    finished_at: Optional[datetime] = None


class ClaimInstance(BaseModel):
    """비즈 워크플로우 1회 실행 인스턴스 (= 1건의 청구 심사)"""
    instance_id: str = Field(default_factory=lambda: f"CLM-WF-{uuid.uuid4().hex[:12]}")
    workflow_id: str
    workflow_version: str
    initial_input: Dict[str, Any]        # claim_no, policy_no, master_data 등
    current_state: str = "START"
    final_state: Optional[str] = None
    history: List[StepRecord] = []
    created_at: datetime = Field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None

    def record(self, step: StepRecord) -> None:
        self.history.append(step)

    def latest_result(self, state_id: str) -> Dict[str, Any]:
        """특정 state의 마지막 result 조회 (payload_builder에서 사용)

        우선 순위:
          1. 해당 state_id와 일치하는 step의 result (일반 task)
          2. parallel step의 브랜치 결과 중 state_id와 일치하는 브랜치의 result 필드
        """
        # 1) 정확한 state_id 매칭 (일반 task)
        for step in reversed(self.history):
            if step.state_id == state_id and step.status == "OK":
                return step.result
        # 2) parallel step의 브랜치 내부 검색
        for step in reversed(self.history):
            if step.task_type == "parallel" and step.status == "OK":
                branch = step.result.get(state_id)
                if branch and branch.get("status") == "OK":
                    return branch.get("result", {})
        return {}