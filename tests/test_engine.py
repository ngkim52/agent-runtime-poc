# tests/test_engine.py
import sys
import os
import asyncio
from typing import Any, Dict, List

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

print(">>> test_engine.py 시작", flush=True)

from shared.workflow_loader import BizWorkflowLoader
from orchestrator.engine import BizFlowEngine

# ─── Dapr WF를 흉내내는 Mock (워커를 직접 호출) ─────────────
from agent_worker import handlers  # 자동 등록
from agent_worker.dispatcher import TaskDispatcher
from shared.models import TaskRequest


class MockDaprWFClient:
    """Dapr WF 대신 worker dispatcher를 즉시 호출 (POC 검증용)"""

    def __init__(self):
        self.dispatcher = TaskDispatcher()
        self.counter = 0

    def execute_task(self, task_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.counter += 1
        fake_wf_id = f"mock-wf-{self.counter:04d}"
        req = TaskRequest(
            task_id=fake_wf_id,
            workflow_instance_id=fake_wf_id,
            task_type=task_type,
            payload=payload,
        )
        result = asyncio.run(self.dispatcher.dispatch(req))
        return {
            "wf_instance_id": fake_wf_id,
            "output": result.model_dump(),
        }

    def execute_parallel(self, branches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Mock parallel: 모든 브랜치를 순차 dispatch 후 취합 (동기)"""
        self.counter += 1
        fake_wf_id = f"mock-par-{self.counter:04d}"

        branch_results = {}
        for b in branches:
            req = TaskRequest(
                task_id=f"{fake_wf_id}/{b['id']}",
                workflow_instance_id=fake_wf_id,
                task_type=b["task_type"],
                payload=b.get("payload", {}),
                event_name=f"BRANCH_{b['id']}",
            )
            result = asyncio.run(self.dispatcher.dispatch(req))
            branch_results[b["id"]] = result.model_dump()

        return {
            "wf_instance_id": fake_wf_id,
            "output": {"branches": branch_results},
        }


def run_scenario(claim_no: str, label: str):
    print(f"\n========== 시나리오: {label} (claim_no={claim_no}) ==========")
    loader = BizWorkflowLoader(base_dir=os.path.join(PROJECT_ROOT, "biz_workflows"))
    engine = BizFlowEngine(loader=loader, dapr_wf_client=MockDaprWFClient())

    inst = engine.run(
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

    print(f"\n→ instance_id   : {inst.instance_id}")
    print(f"→ final_state   : {inst.final_state}")
    print(f"→ steps executed: {len(inst.history)}")
    for s in inst.history:
        print(f"   • {s.state_id:25s} status={s.status} task_type={s.task_type}")
    return inst


if __name__ == "__main__":
    # 1) Happy path: 끝자리가 0이 아닌 접수번호 → 정상 자동 처리 → END
    inst1 = run_scenario("CLM-20260623-001", "Happy path")
    assert inst1.final_state == "END"
    states1 = [s.state_id for s in inst1.history]
    assert "PARALLEL_CHECKS" in states1, f"PARALLEL_CHECKS not in {states1}"
    assert "MATCH_PRIOR_CLAIMS" in states1
    assert "MANUAL_REVIEW" not in states1

    # 병렬 블록 검증
    parallel_step = [s for s in inst1.history if s.state_id == "PARALLEL_CHECKS"][0]
    assert parallel_step.status == "OK"
    assert "VERIFY_IDENTITY" in parallel_step.result
    assert "EXTRACT_ACCIDENT_INFO" in parallel_step.result
    assert parallel_step.result["VERIFY_IDENTITY"]["status"] == "OK"
    assert parallel_step.result["EXTRACT_ACCIDENT_INFO"]["status"] == "OK"
    print("✅ Happy path OK (parallel VERIFY_IDENTITY + EXTRACT_ACCIDENT_INFO)")

    # 2) 이미지 0건 시나리오: 끝자리 0 → DOWNLOAD_IMAGES 결과 0건 → MANUAL_REVIEW
    inst2 = run_scenario("CLM-20260623-000", "이미지 0건 → 수기심사")
    assert inst2.final_state == "END"
    states2 = [s.state_id for s in inst2.history]
    assert "MANUAL_REVIEW" in states2
    assert "EXTRACT_DATA" not in states2
    assert "PARALLEL_CHECKS" not in states2  # EXTRACT_DATA 전에 차단
    print("✅ Unhappy path OK (parallel 진입 전 수기심사)")

    print("\n🎉 Fan-out/Fan-in 엔진 검증 완료")