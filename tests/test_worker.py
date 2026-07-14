# tests/test_worker.py
import sys
import os
import asyncio

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

print(">>> test_worker.py 시작", flush=True)

from shared.models import TaskRequest
from agent_worker import handlers  # 자동 등록
from agent_worker.dispatcher import TaskDispatcher
from agent_worker.registry import HandlerRegistry


async def run_one(task_type: str, payload: dict) -> dict:
    dispatcher = TaskDispatcher()
    req = TaskRequest(
        task_id=f"t-{task_type}",
        workflow_instance_id="wf-INST-001",
        task_type=task_type,
        payload=payload,
    )
    result = await dispatcher.dispatch(req)
    return result.model_dump()


async def main():
    print(f"등록된 handler: {HandlerRegistry.list_registered()}", flush=True)

    # 1) 이미지 다운로드
    r = await run_one("image_download", {"claim_no": "CLM-20260623-001"})
    assert r["status"] == "OK"
    assert r["result"]["image_count"] == 5
    print(f"✅ image_download → image_count={r['result']['image_count']}")

    # 2) 문서 분류
    r = await run_one("document_classification",
                      {"image_refs": ["a.jpg", "b.jpg", "c.jpg"]})
    assert r["result"]["has_claim_form"] is True
    print(f"✅ document_classification → has_claim_form={r['result']['has_claim_form']}")

    # 3) 데이터 추출
    r = await run_one("image_data_extraction", {})
    assert r["result"]["extraction_confidence"] >= 0.7
    print(f"✅ image_data_extraction → confidence={r['result']['extraction_confidence']}")

    # 4) 본인확인 (모두 일치)
    r = await run_one("identity_verification", {
        "extracted_fields": {
            "insured_name": "홍길동",
            "ssn": "900101-1******",
            "account_no": "110-123-456789",
        },
        "master_data": {
            "insured_name": "홍길동",
            "insured_ssn": "900101-1******",
            "account_no": "110-123-456789",
        },
    })
    assert r["result"]["name_match"] and r["result"]["ssn_match"] and r["result"]["account_match"]
    print(f"✅ identity_verification → all match")

    # 5) 사고정보 추출
    r = await run_one("accident_info_extraction", {})
    assert r["result"]["extraction_confidence"] >= 0.7
    print(f"✅ accident_info_extraction → fields={r['result']['fields']}")

    # 6) 기지급 전문 조회
    r = await run_one("prior_claims_matching", {
        "accident_fields": {
            "disease_code": "I63.9",
            "accident_date": "2026-05-12",
            "hospital": "서울대학교병원",
            "doctor": "김의사",
        }
    })
    assert r["result"]["accident_confirmed"] is True
    print(f"✅ prior_claims_matching → confirmed={r['result']['accident_confirmed']}")

    # 7) 수기심사 라우팅
    r = await run_one("route_to_human", {})
    assert r["result"]["routed"] is True
    print(f"✅ route_to_human → queue={r['result']['queue_id']}")

    # 8) 알 수 없는 task_type → FAIL
    r = await run_one("unknown_task", {})
    assert r["status"] == "FAIL"
    print(f"✅ unknown_task → FAIL (error={r['error']})")

    print("\n🎉 모든 handler 동작 확인 완료")


if __name__ == "__main__":
    asyncio.run(main())