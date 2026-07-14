# shared/workflow_repository.py
"""
DB 기반 워크플로우 저장소.

YAML 로더(BizWorkflowLoader)를 대체하여 같은 BizWorkflowDef Pydantic 모델을 리턴합니다.
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from .db import SessionLocal
from .models import BizWorkflowDef, BranchDef, StateDef, TransitionDef
from .orm_models import (
    WorkflowDefinitionModel,
    WorkflowStateModel,
    WorkflowBranchModel,
    WorkflowTransitionModel,
    WorkflowInstanceModel,
    StepResultModel,
)

log = logging.getLogger("wf_repo")


class BizWorkflowRepository:
    """DB 기반 워크플로우 CRUD."""

    def __init__(self, session: Optional[Session] = None):
        self._session = session  # 외부 주입용 (테스트)
        self._cache: Dict[str, BizWorkflowDef] = {}

    # ── session ──────────────────────────────────────────────

    @property
    def session(self) -> Session:
        if self._session is not None:
            return self._session
        return SessionLocal()

    # ── Definition CRUD ─────────────────────────────────────

    def load(self, workflow_id: str, version: str = "1.0") -> BizWorkflowDef:
        """YAML 로더와 동일한 인터페이스 — BizWorkflowDef 리턴."""
        cache_key = f"{workflow_id}:{version}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        db = self.session
        try:
            row = db.execute(
                select(WorkflowDefinitionModel).where(
                    WorkflowDefinitionModel.workflow_id == workflow_id,
                    WorkflowDefinitionModel.version == version,
                )
            ).scalar_one_or_none()

            if row is None:
                raise FileNotFoundError(
                    f"Workflow not found: {workflow_id} v{version}"
                )

            wf = self._row_to_def(row)
            self._cache[cache_key] = wf
            return wf
        finally:
            if self._session is None:
                db.close()

    def list_workflows(self) -> List[Dict[str, Any]]:
        """등록된 워크플로우 목록 (요약)."""
        db = self.session
        try:
            rows = db.execute(
                select(WorkflowDefinitionModel).order_by(
                    WorkflowDefinitionModel.workflow_id,
                    WorkflowDefinitionModel.version.desc(),
                )
            ).scalars().all()
            return [
                {
                    "id": r.id,
                    "workflow_id": r.workflow_id,
                    "version": r.version,
                    "label": r.label,
                    "description": r.description,
                    "state_count": len(r.states),
                    "transition_count": len(r.transitions),
                    "input_schema": r.input_schema,
                    "created_at": r.created_at.isoformat(),
                    "updated_at": r.updated_at.isoformat(),
                }
                for r in rows
            ]
        finally:
            if self._session is None:
                db.close()

    def save_workflow(self, wf: BizWorkflowDef) -> int:
        """BizWorkflowDef를 DB에 저장. 기존 버전이 있으면 덮어씀 (CASCADE)."""
        db = self.session
        try:
            # 기존 row 확인
            existing = db.execute(
                select(WorkflowDefinitionModel).where(
                    WorkflowDefinitionModel.workflow_id == wf.workflow_id,
                    WorkflowDefinitionModel.version == wf.version,
                )
            ).scalar_one_or_none()

            if existing:
                # Delete referencing instances first to avoid FK violation
                old_instances = db.execute(
                    select(WorkflowInstanceModel).where(
                        WorkflowInstanceModel.wf_def_id == existing.id,
                    )
                ).scalars().all()
                for inst in old_instances:
                    db.delete(inst)
                db.flush()
                db.delete(existing)
                db.flush()

            row = WorkflowDefinitionModel(
                workflow_id=wf.workflow_id,
                version=wf.version,
                label=wf.description or wf.workflow_id,
                description=wf.description,
                input_schema=wf.input_schema,
            )
            db.add(row)
            db.flush()  # id 확보

            # states
            for idx, s in enumerate(wf.states):
                state_row = WorkflowStateModel(
                    wf_def_id=row.id,
                    state_id=s.id,
                    type=s.type,
                    task_type=s.task,
                    timeout_sec=s.timeout_sec,
                    description=s.description,
                    sort_order=idx,
                    instruction=s.instruction,
                    inputs=s.inputs or None,
                    input_schema=s.input_schema,
                    output_schema=s.output_schema,
                    tools=s.tools or None,
                    max_tokens=s.max_tokens,
                )
                db.add(state_row)
                db.flush()

                # branches (parallel)
                for bi, b in enumerate(s.branches):
                    br_row = WorkflowBranchModel(
                        state_id=state_row.id,
                        branch_id=b.id,
                        task_type=b.task,
                        timeout_sec=b.timeout_sec,
                        sort_order=bi,
                        instruction=b.instruction,
                        inputs=b.inputs or None,
                        input_schema=b.input_schema,
                        output_schema=b.output_schema,
                        tools=b.tools or None,
                        max_tokens=b.max_tokens,
                    )
                    db.add(br_row)

            # transitions
            for ti, t in enumerate(wf.transitions):
                tr_row = WorkflowTransitionModel(
                    wf_def_id=row.id,
                    from_state=t.from_,
                    to_state=t.to,
                    condition_expr=t.when,
                    sort_order=ti,
                )
                db.add(tr_row)

            db.commit()
            self._cache.pop(f"{wf.workflow_id}:{wf.version}", None)
            return row.id
        except Exception:
            db.rollback()
            raise
        finally:
            if self._session is None:
                db.close()

    def delete_workflow(self, workflow_id: str, version: str) -> None:
        """워크플로우 정의와 연결된 모든 데이터(상태, 전이, 인스턴스)를 삭제."""
        db = self.session
        try:
            row = db.execute(
                select(WorkflowDefinitionModel).where(
                    WorkflowDefinitionModel.workflow_id == workflow_id,
                    WorkflowDefinitionModel.version == version,
                )
            ).scalar_one_or_none()

            if row is None:
                raise FileNotFoundError(
                    f"Workflow not found: {workflow_id} v{version}"
                )

            # 연결된 인스턴스 먼저 삭제 (step_results는 CASCADE)
            instances = db.execute(
                select(WorkflowInstanceModel).where(
                    WorkflowInstanceModel.wf_def_id == row.id,
                )
            ).scalars().all()
            for inst in instances:
                db.delete(inst)

            db.flush()

            # 워크플로우 정의 삭제 (states, transitions, branches는 CASCADE)
            db.delete(row)
            db.commit()

            # 캐시 정리
            self._cache.pop(f"{workflow_id}:{version}", None)
            log.info("Deleted workflow %s v%s", workflow_id, version)
        except Exception:
            db.rollback()
            raise
        finally:
            if self._session is None:
                db.close()

    # ── Runtime Instance ────────────────────────────────────

    def save_instance(
        self,
        instance_id: str,
        wf_def_id: int,
        workflow_id: str,
        workflow_version: str,
        initial_input: Dict[str, Any],
    ) -> None:
        db = self.session
        try:
            row = WorkflowInstanceModel(
                instance_id=instance_id,
                wf_def_id=wf_def_id,
                workflow_id=workflow_id,
                workflow_version=workflow_version,
                initial_input=initial_input,
            )
            db.add(row)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            if self._session is None:
                db.close()

    def update_instance_state(
        self,
        instance_id: str,
        current_state: str,
        status: Optional[str] = None,
        final_state: Optional[str] = None,
    ) -> None:
        db = self.session
        try:
            row = db.execute(
                select(WorkflowInstanceModel).where(
                    WorkflowInstanceModel.instance_id == instance_id
                )
            ).scalar_one_or_none()
            if row is None:
                raise ValueError(f"Instance not found: {instance_id}")
            row.current_state = current_state
            if status is not None:
                row.status = status
            if final_state is not None:
                row.final_state = final_state
                row.finished_at = datetime.now(timezone.utc)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            if self._session is None:
                db.close()

    def save_step_result(
        self,
        instance_id: str,
        state_id: str,
        task_type: Optional[str],
        wf_instance_id: Optional[str],
        status: str,
        result: Dict[str, Any],
        error: Optional[str],
        started_at: datetime,
        finished_at: Optional[datetime],
        sort_order: int,
    ) -> None:
        db = self.session
        try:
            row = StepResultModel(
                instance_id=instance_id,
                state_id=state_id,
                task_type=task_type,
                wf_instance_id=wf_instance_id,
                status=status,
                result=result,
                error=error,
                started_at=started_at,
                finished_at=finished_at,
                sort_order=sort_order,
            )
            db.add(row)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            if self._session is None:
                db.close()

    def list_instances(
        self,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        db = self.session
        try:
            q = select(WorkflowInstanceModel)
            if status:
                q = q.where(WorkflowInstanceModel.status == status)
            q = q.order_by(WorkflowInstanceModel.created_at.desc()).limit(limit)
            rows = db.execute(q).scalars().all()
            return [
                {
                    "instance_id": r.instance_id,
                    "workflow_id": r.workflow_id,
                    "workflow_version": r.workflow_version,
                    "current_state": r.current_state,
                    "status": r.status,
                    "final_state": r.final_state,
                    "created_at": r.created_at.isoformat(),
                    "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                    "step_count": len(r.step_results),
                }
                for r in rows
            ]
        finally:
            if self._session is None:
                db.close()

    def get_instance_detail(self, instance_id: str) -> Optional[Dict[str, Any]]:
        db = self.session
        try:
            row = db.execute(
                select(WorkflowInstanceModel).where(
                    WorkflowInstanceModel.instance_id == instance_id
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            return {
                "instance_id": row.instance_id,
                "workflow_id": row.workflow_id,
                "workflow_version": row.workflow_version,
                "initial_input": row.initial_input,
                "current_state": row.current_state,
                "status": row.status,
                "final_state": row.final_state,
                "created_at": row.created_at.isoformat(),
                "finished_at": row.finished_at.isoformat() if row.finished_at else None,
                "steps": [
                    {
                        "state_id": sr.state_id,
                        "task_type": sr.task_type,
                        "wf_instance_id": sr.wf_instance_id,
                        "status": sr.status,
                        "result": sr.result,
                        "error": sr.error,
                        "started_at": sr.started_at.isoformat(),
                        "finished_at": sr.finished_at.isoformat() if sr.finished_at else None,
                        "sort_order": sr.sort_order,
                    }
                    for sr in row.step_results
                ],
            }
        finally:
            if self._session is None:
                db.close()

    # ── Resume / Retry ──────────────────────────────────────

    def count_state_attempts(self, instance_id: str, state_id: str) -> int:
        """특정 state의 FAIL + SKIP 시도 횟수를 반환 (재시도 제한 판단용)."""
        db = self.session
        try:
            rows = db.execute(
                select(StepResultModel).where(
                    StepResultModel.instance_id == instance_id,
                    StepResultModel.state_id == state_id,
                    StepResultModel.status.in_(["FAIL", "SKIP"]),
                )
            ).scalars().all()
            return len(rows)
        finally:
            if self._session is None:
                db.close()

    def skip_step_result(self, instance_id: str, state_id: str) -> None:
        """가장 최근 FAIL 상태의 step_result를 SKIP으로 마킹."""
        db = self.session
        try:
            row = db.execute(
                select(StepResultModel).where(
                    StepResultModel.instance_id == instance_id,
                    StepResultModel.state_id == state_id,
                    StepResultModel.status == "FAIL",
                ).order_by(StepResultModel.sort_order.desc())
            ).scalars().first()
            if row is not None:
                row.status = "SKIP"
                db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            if self._session is None:
                db.close()

    def reconstruct_instance(self, instance_id: str, default_max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        DB에서 FAILED instance를 읽어 resume에 필요한 데이터를 반환.
        
        Returns: {
            "instance": ClaimInstance-like dict,
            "max_retries": int,
        } or None if not found / not FAILED.
        """
        from orchestrator.instance import StepRecord as StepRecordModel  # local alias to avoid confusion
        detail = self.get_instance_detail(instance_id)
        if detail is None:
            return None
        if detail["status"] != "FAILED":
            return None

        steps = detail["steps"]
        # StepRecord 객체 목록 생성 (sort_order 기준 정렬)
        history = []
        for sr in sorted(steps, key=lambda x: x.get("sort_order", 0)):
            history.append({
                "state_id": sr["state_id"],
                "task_type": sr.get("task_type"),
                "wf_instance_id": sr.get("wf_instance_id"),
                "status": sr["status"],
                "result": sr.get("result", {}),
                "error": sr.get("error"),
                "started_at": sr.get("started_at"),
                "finished_at": sr.get("finished_at"),
            })

        return {
            "instance": {
                "instance_id": detail["instance_id"],
                "workflow_id": detail["workflow_id"],
                "workflow_version": detail["workflow_version"],
                "initial_input": detail["initial_input"],
                "current_state": detail["current_state"],
                "history": history,
                "created_at": detail["created_at"],
                "finished_at": detail["finished_at"],
                "final_state": detail.get("final_state"),
            },
            "max_retries": default_max_retries,
        }

    def _lookup_wf_def_id(self, workflow_id: str, version: str) -> int:
        db = self.session
        try:
            row = db.execute(
                select(WorkflowDefinitionModel).where(
                    WorkflowDefinitionModel.workflow_id == workflow_id,
                    WorkflowDefinitionModel.version == version,
                )
            ).scalar_one_or_none()
            if row is None:
                raise ValueError(f"Workflow not found: {workflow_id} v{version}")
            return row.id
        finally:
            if self._session is None:
                db.close()

    # ── internal ────────────────────────────────────────────

    @staticmethod
    def _row_to_def(row: WorkflowDefinitionModel) -> BizWorkflowDef:
        states: List[StateDef] = []
        for s in row.states:
            branches = [
                BranchDef(
                    id=b.branch_id, task=b.task_type, timeout_sec=b.timeout_sec,
                    instruction=b.instruction,
                    inputs=list(b.inputs) if b.inputs else [],
                    input_schema=b.input_schema,
                    output_schema=b.output_schema,
                    tools=list(b.tools) if b.tools else [],
                    max_tokens=b.max_tokens,
                )
                for b in s.branches
            ]
            states.append(StateDef(
                id=s.state_id,
                type=s.type,  # type: ignore
                task=s.task_type,
                branches=branches,
                timeout_sec=s.timeout_sec,
                description=s.description,
                instruction=s.instruction,
                inputs=list(s.inputs) if s.inputs else [],
                input_schema=s.input_schema,
                output_schema=s.output_schema,
                tools=list(s.tools) if s.tools else [],
                max_tokens=s.max_tokens,
            ))

        transitions = [
            TransitionDef(from_=t.from_state, to=t.to_state, when=t.condition_expr)
            for t in row.transitions
        ]

        return BizWorkflowDef(
            workflow_id=row.workflow_id,
            version=row.version,
            description=row.description or "",
            states=states,
            transitions=transitions,
            input_schema=row.input_schema,
        )
