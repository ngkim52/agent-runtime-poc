# shared/models.py
from __future__ import annotations
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


class BranchDef(BaseModel):
    """parallel state의 각 브랜치"""
    id: str
    task: str = "ai_task"
    timeout_sec: int = 300
    instruction: Optional[str] = None
    inputs: List[str] = []
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    tools: List[str] = []
    max_tokens: Optional[int] = None


class StateDef(BaseModel):
    id: str
    type: Literal["start", "task", "parallel", "end"]
    task: Optional[str] = None
    branches: List[BranchDef] = []
    timeout_sec: int = 300
    description: Optional[str] = None
    instruction: Optional[str] = None
    inputs: List[str] = []
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    tools: List[str] = []
    max_tokens: Optional[int] = None


class TransitionDef(BaseModel):
    from_: str = Field(..., alias="from")
    to: str
    when: Optional[str] = None

    class Config:
        populate_by_name = True


class BizWorkflowDef(BaseModel):
    workflow_id: str
    version: str
    description: Optional[str] = ""
    states: List[StateDef]
    transitions: List[TransitionDef]
    input_schema: Dict[str, Any] = {}

    def get_state(self, state_id: str) -> StateDef:
        for s in self.states:
            if s.id == state_id:
                return s
        raise KeyError(f"State not found: {state_id}")

    def next_transitions(self, current: str) -> List[TransitionDef]:
        return [t for t in self.transitions if t.from_ == current]


# ── Orchestrator ↔ Worker 공통 메시지 ──

class TaskRequest(BaseModel):
    task_id: str
    workflow_instance_id: str
    task_type: str
    payload: Dict[str, Any]
    event_name: str = "TASK_RESULT"
    instruction: Optional[str] = None        # AI 작업 지시문
    tools: List[str] = []                    # 사용 가능한 도구 목록
    output_schema: Optional[Dict[str, Any]] = None  # 출력 구조 정의
    timeout_sec: Optional[float] = None       # 에이전트 실행 전체 타임아웃 (초)
    max_tokens: Optional[int] = None          # LLM 응답 최대 토큰 수 (None=기본값 4096)


class TaskResult(BaseModel):
    task_id: str
    workflow_instance_id: str
    status: Literal["OK", "FAIL"]
    result: Dict[str, Any] = {}
    error: Optional[str] = None
    event_name: str = "TASK_RESULT"
