# orchestrator/engine.py
from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from shared.models import BizWorkflowDef
from shared.workflow_loader import BizWorkflowLoader, TransitionEvaluator
from shared.workflow_repository import BizWorkflowRepository

from .instance import ClaimInstance, StepRecord
from .payload_builder import PayloadBuilder


@runtime_checkable
class DaprClientProtocol(Protocol):
    """BizFlowEngine이 Dapr 클라이언트에 요구하는 최소 인터페이스."""
    def execute_task(
        self,
        task_type: str,
        payload: Dict[str, Any],
        instruction: str | None = None,
        tools: list[str] | None = None,
        output_schema: dict[str, Any] | None = None,
        timeout: float = 300.0,
    ) -> Dict[str, Any]:
        ...
    def execute_parallel(self, branches: List[Dict[str, Any]]) -> Dict[str, Any]:
        ...

log = logging.getLogger("biz_engine")


class BizFlowEngine:
    """
    YAML(or DB) 기반 비즈 흐름을 순회하며 각 task를 Dapr WF로 위임 실행.
    """

    MAX_STEPS = 50   # 무한루프 방지 안전장치

    def __init__(
        self,
        loader: BizWorkflowLoader,
        dapr_wf_client: DaprClientProtocol,
        db_repo: Optional[BizWorkflowRepository] = None,
    ):
        self.loader = loader
        self.dapr = dapr_wf_client
        self.db_repo = db_repo

    @staticmethod
    def _get_start_state_id(wf) -> str:
        """Find the actual start state ID from workflow definition."""
        for s in wf.states:
            if s.type == "start":
                return s.id
        return "START"

    def _persist_step(
        self,
        inst: ClaimInstance,
        step_result: StepRecord,
        sort_order: int,
    ) -> None:
        if self.db_repo is None:
            return
        # naive datetime → aware (UTC)
        def _aware(dt: Optional[datetime]) -> Optional[datetime]:
            if dt is None or dt.tzinfo is not None:
                return dt
            return dt.replace(tzinfo=timezone.utc)
        self.db_repo.save_step_result(
            instance_id=inst.instance_id,
            state_id=step_result.state_id,
            task_type=step_result.task_type,
            wf_instance_id=step_result.wf_instance_id,
            status=step_result.status,
            result=step_result.result,
            error=step_result.error,
            started_at=_aware(step_result.started_at),
            finished_at=_aware(step_result.finished_at),
            sort_order=sort_order,
        )

    def run(
        self,
        workflow_id: str,
        version: str,
        initial_input: Dict[str, Any],
        existing_instance: Optional[ClaimInstance] = None,
    ) -> ClaimInstance:
        wf = self.loader.load(workflow_id, version)

        # Find the actual start state ID from workflow definition
        start_id = self._get_start_state_id(wf)

        if existing_instance:
            inst = existing_instance
            # history에서 실패한 state의 FAIL/SKIP 레코드는 제거
            inst.history = [s for s in inst.history
                            if not (s.state_id == inst.current_state and s.status in ("FAIL", "SKIP"))]
            sort_order = len(inst.history)
            # current_state가 시작 state면 트랜지션 평가
            if inst.current_state == "START" or inst.current_state == start_id:
                first = TransitionEvaluator.pick_next(wf.next_transitions(start_id), {})
                if first is not None:
                    inst.current_state = first.to
            log.info("▶ resume instance=%s from state=%s (history=%d entries)",
                     inst.instance_id, inst.current_state, sort_order)
        else:
            inst = ClaimInstance(
                workflow_id=workflow_id,
                workflow_version=version,
                initial_input=initial_input,
            )
            log.info("▶ start instance=%s wf=%s", inst.instance_id, workflow_id)

            # DB에 instance 생성
            if self.db_repo is not None:
                wf_def_id = self.db_repo._lookup_wf_def_id(workflow_id, version)
                self.db_repo.save_instance(
                    instance_id=inst.instance_id,
                    wf_def_id=wf_def_id,
                    workflow_id=workflow_id,
                    workflow_version=version,
                    initial_input=initial_input,
                )

            # 시작 state → 다음 state
            first = TransitionEvaluator.pick_next(wf.next_transitions(start_id), {})
            if first is None:
                raise RuntimeError(f"No transition from start state '{start_id}'")
            inst.current_state = first.to
            sort_order = 0

        # 메인 루프
        for step_no in range(self.MAX_STEPS):
            state_def = wf.get_state(inst.current_state)
            log.info("  [step %d] state=%s type=%s", step_no, state_def.id, state_def.type)

            if state_def.type == "end":
                inst.final_state = state_def.id
                inst.finished_at = datetime.utcnow()
                if self.db_repo is not None:
                    self.db_repo.update_instance_state(
                        instance_id=inst.instance_id,
                        current_state=state_def.id,
                        status="COMPLETED",
                        final_state=state_def.id,
                    )
                log.info("✓ instance=%s ended at %s", inst.instance_id, state_def.id)
                return inst

            # skip-already-OK: state가 이미 OK 결과를 가지면 재실행하지 않고 통과
            existing_ok = inst.latest_result(state_def.id)
            if existing_ok:
                log.info("  ⏭ skip state=%s (already OK in history)", state_def.id)
                # 현재 상태를 OK로 간주하고 transition 평가로 이동
                step_result = StepRecord(
                    state_id=state_def.id,
                    task_type="skip",
                    status="OK",
                    result=existing_ok,
                    started_at=datetime.utcnow(),
                    finished_at=datetime.utcnow(),
                )
                inst.record(step_result)
                sort_order += 1
                self._persist_step(inst, step_result, sort_order)

                # 다음 state 결정
                transitions = wf.next_transitions(state_def.id)
                nxt = TransitionEvaluator.pick_next(
                    transitions,
                    {**existing_ok, "status": "OK"},
                )
                if nxt is None:
                    inst.final_state = state_def.id
                    inst.finished_at = datetime.utcnow()
                    if self.db_repo is not None:
                        self.db_repo.update_instance_state(
                            instance_id=inst.instance_id,
                            current_state=state_def.id,
                            status="FAILED",
                            final_state=state_def.id,
                        )
                    log.error("✗ no matching transition from %s (skip)", state_def.id)
                    return inst

                log.info("  → next state: %s (skip)", nxt.to)
                inst.current_state = nxt.to
                if self.db_repo is not None:
                    self.db_repo.update_instance_state(
                        instance_id=inst.instance_id,
                        current_state=nxt.to,
                    )
                continue

            # task 실행 (task / parallel 분기)
            if state_def.type == "parallel":
                step_result = self._execute_parallel(inst, state_def)
            else:
                step_result = self._execute_task(inst, state_def, wf)
            inst.record(step_result)
            sort_order += 1
            self._persist_step(inst, step_result, sort_order)

            # 실패면 즉시 종료 (POC 정책)
            if step_result.status != "OK":
                inst.final_state = state_def.id
                inst.finished_at = datetime.utcnow()
                if self.db_repo is not None:
                    self.db_repo.update_instance_state(
                        instance_id=inst.instance_id,
                        current_state=state_def.id,
                        status="FAILED",
                        final_state=state_def.id,
                    )
                log.error("✗ instance=%s failed at %s: %s",
                          inst.instance_id, state_def.id, step_result.error)
                return inst

            # 다음 state 결정
            transitions = wf.next_transitions(state_def.id)
            nxt = TransitionEvaluator.pick_next(
                transitions,
                {**step_result.result, "status": step_result.status},
            )
            if nxt is None:
                inst.final_state = state_def.id
                inst.finished_at = datetime.utcnow()
                if self.db_repo is not None:
                    self.db_repo.update_instance_state(
                        instance_id=inst.instance_id,
                        current_state=state_def.id,
                        status="FAILED",
                        final_state=state_def.id,
                    )
                log.error("✗ no matching transition from %s", state_def.id)
                return inst

            log.info("  → next state: %s", nxt.to)
            inst.current_state = nxt.to

            # 매 state 변경 시 DB 업데이트
            if self.db_repo is not None:
                self.db_repo.update_instance_state(
                    instance_id=inst.instance_id,
                    current_state=nxt.to,
                )

        raise RuntimeError(f"MAX_STEPS({self.MAX_STEPS}) exceeded")

    # ── Schema validation helper ────────────────────────────

    @staticmethod
    def _validate_schema(result: Dict[str, Any], schema: Optional[Dict[str, Any]], llm_output: str = "") -> bool:
        """output_schema에 정의된 필드를 검증.
        - properties에 정의된 필드 중 1개 이상 매칭되면 partial match 인정
        - required 목록이 있으면 해당 필드들도 모두 존재해야 함
        - llm_output 안에 JSON이 파묻혀 있으면 추출 시도."""
        if not schema:
            return True
        props = schema.get("properties", {})
        if not props:
            return True

        # Helper: check a dict against props and required fields
        def _check(data: Dict[str, Any]) -> bool:
            # 1) Partial match: at least one defined property exists
            if not (set(props.keys()) & set(data.keys())):
                return False
            # 2) Required fields check: all required fields must be present (non-empty)
            required = schema.get("required", [])
            if required:
                missing = [f for f in required if not data.get(f)]
                if missing:
                    log.warning("Schema missing required fields: %s (present: %s)", missing, list(data.keys()))
                    return False
            return True

        # 1) 직접 매칭
        if _check(result):
            return True
        # 2) llm_output 안에 JSON이 있는지 확인
        if llm_output and llm_output.strip().startswith("{"):
            try:
                parsed = json.loads(llm_output)
                if _check(parsed):
                    return True
            except (json.JSONDecodeError, TypeError):
                pass
        return False

    # ── internal ──────────────────────────────────────────
    def _execute_task(self, inst: ClaimInstance, state_def, wf: BizWorkflowDef) -> StepRecord:
        started = datetime.utcnow()
        payload = PayloadBuilder.build(state_def, inst)
        max_retries = 2  # schema mismatch 시 재시도

        last_error: Optional[str] = None
        for attempt in range(max_retries):
            # Build instruction with previous failure context if retrying
            if attempt == 0:
                retry_instruction = state_def.instruction
            elif last_error:
                retry_instruction = (
                    f"{state_def.instruction}\n\n"
                    f"[PREVIOUS ATTEMPT FAILED — Retry {attempt}/{max_retries-1}]\n"
                    f"The previous attempt failed with error: {last_error}\n"
                    f"Please try a different approach and ensure the output matches the required JSON schema: "
                    f"{json.dumps(state_def.output_schema, ensure_ascii=False)}"
                )
            else:
                retry_instruction = (
                    f"{state_def.instruction}\n\n"
                    f"[SCHEMA RETRY {attempt}/{max_retries-1}]\n"
                    f"Previous response did not follow the required JSON schema. "
                    f"You MUST return EXACTLY this structure: "
                    f"{json.dumps(state_def.output_schema, ensure_ascii=False)}"
                )

            try:
                wf_out = self.dapr.execute_task(
                    task_type=state_def.task,
                    payload=payload,
                    instruction=retry_instruction,
                    tools=state_def.tools,
                    output_schema=state_def.output_schema,
                    timeout=float(state_def.timeout_sec or 60),
                    max_tokens=state_def.max_tokens,
                )
                output = wf_out["output"]
                result_data = output.get("result", {})
                llm_raw = result_data.get("llm_output", "")

                # Schema validation
                if output.get("status") == "OK" and state_def.output_schema:
                    if not self._validate_schema(result_data, state_def.output_schema, llm_raw):
                        if attempt < max_retries - 1:
                            log.warning("Schema mismatch at %s (attempt %d/%d): result keys=%s, expected=%s",
                                        state_def.id, attempt + 1, max_retries,
                                        list(result_data.keys()),
                                        list(state_def.output_schema.get("properties", {}).keys()))
                            last_error = f"Schema mismatch: expected keys {list(state_def.output_schema.get('properties', {}).keys())}, got {list(result_data.keys())}"
                            continue  # retry with corrected instruction
                        else:
                            log.warning("Schema mismatch at %s exhausted retries: result_keys=%s",
                                        state_def.id, list(result_data.keys()))

                return StepRecord(
                    state_id=state_def.id,
                    task_type=state_def.task,
                    wf_instance_id=wf_out.get("wf_instance_id"),
                    status=output.get("status", "OK"),
                    result=result_data,
                    error=output.get("error"),
                    started_at=started,
                    finished_at=datetime.utcnow(),
                )
            except Exception as e:
                if attempt < max_retries - 1:
                    log.warning("task exec failed at %s (attempt %d/%d): %s", state_def.id, attempt + 1, max_retries, e)
                    last_error = f"{type(e).__name__}: {e}"
                    continue
                log.exception("task exec failed at %s (all attempts)", state_def.id)
                return StepRecord(
                    state_id=state_def.id,
                    task_type=state_def.task,
                    status="FAIL",
                    error=f"{type(e).__name__}: {e}",
                    started_at=started,
                    finished_at=datetime.utcnow(),
                )
        # Shouldn't reach here, but safety
        return StepRecord(
            state_id=state_def.id,
            task_type=state_def.task,
            status="FAIL",
            error="Max retries exceeded",
            started_at=started,
            finished_at=datetime.utcnow(),
        )

    def _execute_parallel(self, inst: ClaimInstance, state_def) -> StepRecord:
        """배치별 payload 구성 → 병렬 실행 → 결과 취합"""
        started = datetime.utcnow()
        branches = []
        for b in state_def.branches:
            payload = PayloadBuilder.build(b, inst)  # branch도 StateDef-like (id, inputs, instruction, ...)
            branches.append({
                "id": b.id,
                "task_type": b.task,
                "payload": payload,
                "instruction": b.instruction,
                "tools": b.tools,
                "output_schema": b.output_schema,
                "timeout_sec": b.timeout_sec or 60,
                "max_tokens": b.max_tokens,
            })

        try:
            wf_out = self.dapr.execute_parallel(branches)
            branch_results = wf_out.get("output", {}).get("branches", {})

            # 전체 상태 판단: 모든 브랜치 OK면 OK
            all_ok = all(
                br.get("status") == "OK"
                for br in branch_results.values()
            )

            return StepRecord(
                state_id=state_def.id,
                task_type="parallel",
                wf_instance_id=wf_out.get("wf_instance_id"),
                status="OK" if all_ok else "FAIL",
                result=branch_results,
                error=None if all_ok else "one or more branches failed",
                started_at=started,
                finished_at=datetime.utcnow(),
            )
        except Exception as e:
            log.exception("parallel exec failed at %s", state_def.id)
            return StepRecord(
                state_id=state_def.id,
                task_type="parallel",
                status="FAIL",
                error=f"{type(e).__name__}: {e}",
                started_at=started,
                finished_at=datetime.utcnow(),
            )