# tests/test_e2e.py
"""
E2E 통합 테스트 — pytest discovery 기반.

실행:  cd poc && python -m pytest tests/test_e2e.py -v

커버리지:
  1. YAML 워크플로우 로딩
  2. Transition 조건 평가 (Happy + Unhappy + Parallel)
  3. Worker 핸들러 dispatch (7개 핸들러 + unknown)
  4. Engine 전체 파이프라인 (병렬 포함)
"""
from __future__ import annotations
import asyncio
import os
import sys
from typing import Any, Dict, List

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from shared.models import TaskRequest, BizWorkflowDef, TransitionDef
from shared.workflow_loader import BizWorkflowLoader, TransitionEvaluator
from agent_worker import handlers  # noqa: F401 — 핸들러 자동 등록
from agent_worker.dispatcher import TaskDispatcher
from agent_worker.registry import HandlerRegistry
from orchestrator.engine import BizFlowEngine
from orchestrator.instance import ClaimInstance

# ═══════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════

@pytest.fixture(scope="session")
def wf() -> BizWorkflowDef:
    """YAML 워크플로우 로딩"""
    loader = BizWorkflowLoader(base_dir=os.path.join(PROJECT_ROOT, "biz_workflows"))
    return loader.load("claim_adjudication", "1.0")


@pytest.fixture(scope="session")
def initial() -> Dict[str, str]:
    """기본 청구 입력 데이터"""
    return {
        "claim_no": "CLM-TEST-001",
        "policy_no": "POL-001",
        "insured_name": "홍길동",
        "insured_ssn": "900101-1******",
        "account_no": "110-123-456789",
    }


class _MockDaprClient:
    """Dapr WF 대신 worker dispatcher를 즉시 호출하는 Mock."""

    def __init__(self):
        self.dispatcher = TaskDispatcher()
        self.counter = 0

    def execute_task(
        self,
        task_type: str,
        payload: Dict[str, Any],
        instruction: str | None = None,
        tools: list[str] | None = None,
        output_schema: dict[str, Any] | None = None,
        timeout: float = 300.0,
        max_tokens: int | None = None,
    ) -> Dict[str, Any]:
        self.counter += 1
        fake_id = f"mock-wf-{self.counter:04d}"
        req = TaskRequest(
            task_id=fake_id,
            workflow_instance_id=fake_id,
            task_type=task_type,
            payload=payload,
            instruction=instruction,
            tools=tools or [],
            output_schema=output_schema,
            timeout_sec=timeout,
            max_tokens=max_tokens,
        )
        result = asyncio.run(self.dispatcher.dispatch(req))
        return {"wf_instance_id": fake_id, "output": result.model_dump()}

    def execute_parallel(self, branches: List[Dict[str, Any]]) -> Dict[str, Any]:
        self.counter += 1
        fake_id = f"mock-par-{self.counter:04d}"
        branch_results = {}
        for b in branches:
            req = TaskRequest(
                task_id=f"{fake_id}/{b['id']}",
                workflow_instance_id=fake_id,
                task_type=b["task_type"],
                payload=b.get("payload", {}),
                event_name=f"BRANCH_{b['id']}",
            )
            result = asyncio.run(self.dispatcher.dispatch(req))
            branch_results[b["id"]] = result.model_dump()
        return {"wf_instance_id": fake_id, "output": {"branches": branch_results}}


def _next(wf: BizWorkflowDef, current: str, result: dict) -> TransitionDef:
    nxt = TransitionEvaluator.pick_next(wf.next_transitions(current), result)
    assert nxt is not None, f"No transition matched: from={current!r}"
    return nxt


def _run_engine(claim_no: str) -> ClaimInstance:
    loader = BizWorkflowLoader(base_dir=os.path.join(PROJECT_ROOT, "biz_workflows"))
    engine = BizFlowEngine(loader=loader, dapr_wf_client=_MockDaprClient())
    return engine.run(
        workflow_id="claim_adjudication",
        version="1.0",
        initial_input={
            "claim_no": claim_no,
            "policy_no": "POL-001",
            "insured_name": "홍길동",
            "insured_ssn": "900101-1******",
            "account_no": "110-123-456789",
        },
    )


# ═══════════════════════════════════════════════════════════════
# 1. YAML 로딩
# ═══════════════════════════════════════════════════════════════

class TestWorkflowLoading:
    def test_loads_correct_id(self, wf: BizWorkflowDef):
        assert wf.workflow_id == "claim_adjudication"
        assert wf.version == "1.0"

    def test_has_all_states(self, wf: BizWorkflowDef):
        ids = {s.id for s in wf.states}
        expected = {"START", "DOWNLOAD_IMAGES", "CLASSIFY_DOCS", "EXTRACT_DATA",
                     "PARALLEL_CHECKS", "MATCH_PRIOR_CLAIMS", "MANUAL_REVIEW", "END"}
        assert ids == expected, f"Missing states: {expected - ids}"

    def test_parallel_state_has_branches(self, wf: BizWorkflowDef):
        parallel = wf.get_state("PARALLEL_CHECKS")
        assert parallel.type == "parallel"
        assert len(parallel.branches) == 2
        assert parallel.branches[0].id == "VERIFY_IDENTITY"
        assert parallel.branches[1].id == "EXTRACT_ACCIDENT_INFO"

    def test_has_transitions(self, wf: BizWorkflowDef):
        assert len(wf.transitions) >= 11


# ═══════════════════════════════════════════════════════════════
# 2. Transition 조건 평가
# ═══════════════════════════════════════════════════════════════

class TestTransitions:
    def test_happy_path_full_flow(self, wf: BizWorkflowDef):
        assert _next(wf, "START", {}).to == "DOWNLOAD_IMAGES"
        assert _next(wf, "DOWNLOAD_IMAGES",
                     {"status": "OK", "image_count": 5}).to == "CLASSIFY_DOCS"
        assert _next(wf, "CLASSIFY_DOCS",
                     {"status": "OK", "has_claim_form": True}).to == "EXTRACT_DATA"
        assert _next(wf, "EXTRACT_DATA",
                     {"status": "OK", "extraction_confidence": 0.92}).to == "PARALLEL_CHECKS"
        assert _next(wf, "PARALLEL_CHECKS",
                     {"VERIFY_IDENTITY": {"status": "OK"},
                      "EXTRACT_ACCIDENT_INFO": {"status": "OK"}}).to == "MATCH_PRIOR_CLAIMS"
        assert _next(wf, "MATCH_PRIOR_CLAIMS",
                     {"status": "OK", "accident_confirmed": True}).to == "END"

    def test_download_fails_falls_to_manual(self, wf: BizWorkflowDef):
        assert _next(wf, "DOWNLOAD_IMAGES",
                     {"status": "OK", "image_count": 0}).to == "MANUAL_REVIEW"

    def test_no_claim_form_to_manual(self, wf: BizWorkflowDef):
        assert _next(wf, "CLASSIFY_DOCS",
                     {"status": "OK", "has_claim_form": False}).to == "MANUAL_REVIEW"

    def test_low_confidence_to_manual(self, wf: BizWorkflowDef):
        assert _next(wf, "EXTRACT_DATA",
                     {"status": "OK", "extraction_confidence": 0.55}).to == "MANUAL_REVIEW"

    def test_parallel_branch_fails_to_manual(self, wf: BizWorkflowDef):
        assert _next(wf, "PARALLEL_CHECKS",
                     {"VERIFY_IDENTITY": {"status": "OK"},
                      "EXTRACT_ACCIDENT_INFO": {"status": "FAIL"}}).to == "MANUAL_REVIEW"

    def test_prior_claim_not_confirmed_to_manual(self, wf: BizWorkflowDef):
        assert _next(wf, "MATCH_PRIOR_CLAIMS",
                     {"status": "OK", "accident_confirmed": False}).to == "MANUAL_REVIEW"


# ═══════════════════════════════════════════════════════════════
# 3. Worker 핸들러
# ═══════════════════════════════════════════════════════════════

class TestHandlers:
    @pytest.fixture(scope="class")
    def dispatcher(self):
        return TaskDispatcher()

    @pytest.mark.asyncio
    async def test_all_handlers_registered(self):
        registered = HandlerRegistry.list_registered()
        expected = [
            "accident_info_extraction",
            "ai_task",
            "document_classification",
            "identity_verification",
            "image_data_extraction",
            "image_download",
            "prior_claims_matching",
            "route_to_human",
        ]
        assert registered == expected

    @pytest.mark.asyncio
    async def test_image_download(self, dispatcher: TaskDispatcher):
        r = await dispatcher.dispatch(TaskRequest(
            task_id="t-1", workflow_instance_id="w1",
            task_type="image_download", payload={"claim_no": "C1"},
        ))
        assert r.status == "OK"
        assert r.result["image_count"] == 5

    @pytest.mark.asyncio
    async def test_document_classification(self, dispatcher: TaskDispatcher):
        r = await dispatcher.dispatch(TaskRequest(
            task_id="t-2", workflow_instance_id="w1",
            task_type="document_classification",
            payload={"image_refs": ["a.jpg", "b.jpg"]},
        ))
        assert r.status == "OK"
        assert r.result["has_claim_form"] is True

    @pytest.mark.asyncio
    async def test_identity_verification_all_match(self, dispatcher: TaskDispatcher):
        r = await dispatcher.dispatch(TaskRequest(
            task_id="t-3", workflow_instance_id="w1",
            task_type="identity_verification",
            payload={
                "extracted_fields": {"insured_name": "홍길동", "ssn": "900101-1******",
                                     "account_no": "110-123-456789"},
                "master_data": {"insured_name": "홍길동", "insured_ssn": "900101-1******",
                                "account_no": "110-123-456789"},
            },
        ))
        assert r.status == "OK"
        assert all([r.result["name_match"], r.result["ssn_match"], r.result["account_match"]])

    @pytest.mark.asyncio
    async def test_prior_claims_matching(self, dispatcher: TaskDispatcher):
        r = await dispatcher.dispatch(TaskRequest(
            task_id="t-4", workflow_instance_id="w1",
            task_type="prior_claims_matching",
            payload={
                "accident_fields": {
                    "disease_code": "I63.9", "accident_date": "2026-05-12",
                    "hospital": "서울대학교병원", "doctor": "김의사",
                }
            },
        ))
        assert r.status == "OK"
        assert r.result["accident_confirmed"] is True

    @pytest.mark.asyncio
    async def test_unknown_task_returns_fail(self, dispatcher: TaskDispatcher):
        r = await dispatcher.dispatch(TaskRequest(
            task_id="t-unknown", workflow_instance_id="w1",
            task_type="unknown_task", payload={},
        ))
        assert r.status == "FAIL"

    @pytest.mark.asyncio
    async def test_event_name_propagated(self, dispatcher: TaskDispatcher):
        r = await dispatcher.dispatch(TaskRequest(
            task_id="t-ev", workflow_instance_id="w1",
            task_type="image_download", payload={"claim_no": "C1"},
            event_name="BRANCH_TEST",
        ))
        assert r.event_name == "BRANCH_TEST"


# ═══════════════════════════════════════════════════════════════
# 4. Engine 전체 파이프라인
# ═══════════════════════════════════════════════════════════════

class TestEngine:
    def test_happy_path_ends_with_end(self):
        inst = _run_engine("CLM-E2E-001")
        assert inst.final_state == "END"

    def test_happy_path_has_parallel_block(self):
        inst = _run_engine("CLM-E2E-002")
        states = [s.state_id for s in inst.history]
        assert "PARALLEL_CHECKS" in states
        assert "MATCH_PRIOR_CLAIMS" in states
        assert "MANUAL_REVIEW" not in states

    def test_parallel_block_contains_both_branches(self):
        inst = _run_engine("CLM-E2E-003")
        parallel = [s for s in inst.history if s.state_id == "PARALLEL_CHECKS"][0]
        assert parallel.status == "OK"
        assert "VERIFY_IDENTITY" in parallel.result
        assert "EXTRACT_ACCIDENT_INFO" in parallel.result
        assert parallel.result["VERIFY_IDENTITY"]["status"] == "OK"
        assert parallel.result["EXTRACT_ACCIDENT_INFO"]["status"] == "OK"

    def test_parallel_branches_have_correct_event_names(self):
        inst = _run_engine("CLM-E2E-004")
        parallel = [s for s in inst.history if s.state_id == "PARALLEL_CHECKS"][0]
        vi = parallel.result["VERIFY_IDENTITY"]
        eai = parallel.result["EXTRACT_ACCIDENT_INFO"]
        assert vi["event_name"] == "BRANCH_VERIFY_IDENTITY"
        assert eai["event_name"] == "BRANCH_EXTRACT_ACCIDENT_INFO"

    def test_downstream_uses_parallel_result(self):
        """MATCH_PRIOR_CLAIMS가 parallel 브랜치(EXTRACT_ACCIDENT_INFO)의 결과를 참조"""
        inst = _run_engine("CLM-E2E-005")
        prior = [s for s in inst.history if s.state_id == "MATCH_PRIOR_CLAIMS"][0]
        assert prior.status == "OK"
        assert prior.result["accident_confirmed"] is True
        assert prior.result["decision_reason"] == "prior_claim_matched"

    def test_unhappy_path_skips_parallel(self):
        """이미지 0건 → DOWNLOAD_IMAGES → MANUAL_REVIEW, parallel 미진입"""
        inst = _run_engine("CLM-E2E-000")
        assert inst.final_state == "END"
        states = [s.state_id for s in inst.history]
        assert "MANUAL_REVIEW" in states
        assert "PARALLEL_CHECKS" not in states

    def test_history_order(self):
        inst = _run_engine("CLM-E2E-006")
        states = [s.state_id for s in inst.history]
        expected_order = [
            "DOWNLOAD_IMAGES", "CLASSIFY_DOCS", "EXTRACT_DATA",
            "PARALLEL_CHECKS", "MATCH_PRIOR_CLAIMS",
        ]
        assert states == expected_order, f"Expected {expected_order}, got {states}"

    def test_step_count(self):
        inst = _run_engine("CLM-E2E-007")
        # 5 sequential steps + END는 history에 없음 → 5
        assert len(inst.history) == 5


# ═══════════════════════════════════════════════════════════════
# 5. Resume / Retry
# ═══════════════════════════════════════════════════════════════

class _FailingMockDaprClient(_MockDaprClient):
    """지정된 call index에서 FAIL을 반환하는 Mock."""

    def __init__(self, fail_on_call: int = 99, fail_branch: str = ""):
        super().__init__()
        self.fail_on_call = fail_on_call
        self.fail_branch = fail_branch

    def execute_task(
        self,
        task_type: str,
        payload: Dict[str, Any],
        instruction: str | None = None,
        tools: list[str] | None = None,
        output_schema: dict[str, Any] | None = None,
        timeout: float = 300.0,
        max_tokens: int | None = None,
    ) -> Dict[str, Any]:
        if self.counter + 1 == self.fail_on_call:
            self.counter += 1
            return {"wf_instance_id": "fail", "output": {"status": "FAIL", "error": "Simulated failure"}}
        return super().execute_task(task_type, payload, instruction, tools, output_schema, timeout, max_tokens)

    def execute_parallel(self, branches: List[Dict[str, Any]]) -> Dict[str, Any]:
        if self.fail_branch:
            self.counter += 1
            fake_id = f"mock-par-{self.counter:04d}"
            branch_results = {}
            for b in branches:
                if b["id"] == self.fail_branch:
                    branch_results[b["id"]] = {"status": "FAIL", "error": "Simulated branch failure", "result": {}}
                else:
                    req = TaskRequest(
                        task_id=f"{fake_id}/{b['id']}",
                        workflow_instance_id=fake_id,
                        task_type=b["task_type"],
                        payload=b.get("payload", {}),
                        event_name=f"BRANCH_{b['id']}",
                    )
                    result = asyncio.run(self.dispatcher.dispatch(req))
                    branch_results[b["id"]] = result.model_dump()
            return {"wf_instance_id": fake_id, "output": {"branches": branch_results}}
        return super().execute_parallel(branches)


class TestResume:
    """Engine resume-from-failure 기능 테스트."""

    def _run(self, client=None, existing=None, initial=None):
        loader = BizWorkflowLoader(base_dir=os.path.join(PROJECT_ROOT, "biz_workflows"))
        engine = BizFlowEngine(loader=loader, dapr_wf_client=client or _MockDaprClient())
        inp = initial or {
            "claim_no": "CLM-RSM-001",
            "policy_no": "POL-001",
            "insured_name": "홍길동",
            "insured_ssn": "900101-1******",
            "account_no": "110-123-456789",
        }
        return engine.run(
            workflow_id="claim_adjudication",
            version="1.0",
            initial_input=inp,
            existing_instance=existing,
        )

    def test_resume_single_failure(self):
        """단순 task 실패 → 재시도 → COMPLETED"""
        # 1st run: EXTRACT_DATA에서 실패 (claim_adjudication: step 2 → fail_on_call=3)
        client = _FailingMockDaprClient(fail_on_call=3)
        inst = self._run(client=client)
        assert inst.final_state == "EXTRACT_DATA"
        assert inst.history[-1].status == "FAIL"
        assert inst.history[-1].state_id == "EXTRACT_DATA"

        # 2nd run: 정상 클라이언트로 재시도
        inst2 = self._run(existing=inst)
        assert inst2.final_state == "END"
        # EXTRACT_DATA가 정확히 1번만 실행됨 (FAIL 레코드는 engine이 제거)
        extract = [s for s in inst2.history if s.state_id == "EXTRACT_DATA"]
        assert len(extract) == 1
        assert extract[0].status == "OK"

    def test_resume_parallel_partial(self):
        """parallel 브랜치 1개 실패 → 재시도 → 정상 완료"""
        client = _FailingMockDaprClient(fail_branch="EXTRACT_ACCIDENT_INFO")
        inst = self._run(client=client)
        assert inst.final_state == "PARALLEL_CHECKS"
        assert inst.history[-1].status == "FAIL"

        # 재시도
        inst2 = self._run(existing=inst)
        assert inst2.final_state == "END"
        parallel = [s for s in inst2.history if s.state_id == "PARALLEL_CHECKS"]
        assert len(parallel) == 1
        assert parallel[0].status == "OK"

    def test_resume_no_re_execute_ok(self):
        """OK 상태는 재실행되지 않음 — history에 각 state가 1번만 존재"""
        client = _FailingMockDaprClient(fail_on_call=5)  # MATCH_PRIOR_CLAIMS에서 실패
        inst = self._run(client=client)
        assert inst.final_state == "MATCH_PRIOR_CLAIMS"
        # 재시도 전 history: 4 OK states + 1 FAIL = 5
        assert len(inst.history) == 5

        inst2 = self._run(existing=inst)
        assert inst2.final_state == "END"
        # MATCH_PRIOR_CLAIMS의 FAIL 레코드 제거됨 → 1개 OK만 남음
        match = [s for s in inst2.history if s.state_id == "MATCH_PRIOR_CLAIMS"]
        assert len(match) == 1
        assert match[0].status == "OK"
        # 이전 OK state들은 여전히 1번씩
        assert len([s for s in inst2.history if s.state_id == "DOWNLOAD_IMAGES"]) == 1
        assert len([s for s in inst2.history if s.state_id == "EXTRACT_DATA"]) == 1

    def test_resume_two_failures_then_success(self):
        """연속 2회 실패 후 3회차 성공"""
        client = _FailingMockDaprClient(fail_on_call=3)
        inst = self._run(client=client)
        assert inst.history[-1].status == "FAIL"

        client2 = _FailingMockDaprClient(fail_on_call=3)
        inst2 = self._run(client=client2, existing=inst)
        assert inst2.history[-1].status == "FAIL"  # 두 번째도 실패

        inst3 = self._run(existing=inst2)
        assert inst3.final_state == "END"

    def test_resume_does_not_touch_prior_ok(self):
        """재시도 후에도 이전 OK 상태들의 result가 변경되지 않음"""
        client = _FailingMockDaprClient(fail_on_call=3)
        inst = self._run(client=client)

        download_ok = inst.history[0].result  # 실패 전 OK 결과 저장

        inst2 = self._run(existing=inst)
        assert inst2.final_state == "END"

        d2 = [s for s in inst2.history if s.state_id == "DOWNLOAD_IMAGES"][0]
        assert d2.result == download_ok
