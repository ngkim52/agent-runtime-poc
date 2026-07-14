# tests/test_loader.py
import sys
import os
import traceback

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

print(">>> test_loader.py 시작", flush=True)
print(f"    cwd          = {os.getcwd()}", flush=True)
print(f"    PROJECT_ROOT = {PROJECT_ROOT}", flush=True)

try:
    from shared.models import TransitionDef
    from shared.workflow_loader import BizWorkflowLoader, TransitionEvaluator
    print(">>> import OK", flush=True)
except Exception as e:
    print(f"❌ import 실패: {e}", flush=True)
    traceback.print_exc()
    sys.exit(1)


def _next(wf, current: str, result: dict) -> TransitionDef:
    nxt = TransitionEvaluator.pick_next(wf.next_transitions(current), result)
    assert nxt is not None, f"No transition matched: from={current!r}, result={result}"
    return nxt


def test_load_and_transition():
    loader = BizWorkflowLoader(base_dir=os.path.join(PROJECT_ROOT, "biz_workflows"))
    wf = loader.load("claim_adjudication", "1.0")

    print(f"✅ Loaded: {wf.workflow_id} v{wf.version}", flush=True)
    print(f"   states={len(wf.states)}, transitions={len(wf.transitions)}", flush=True)

    # ── Happy path ────────────────────────────────────────
    assert _next(wf, "START", {}).to == "DOWNLOAD_IMAGES"

    assert _next(wf, "DOWNLOAD_IMAGES",
                 {"status": "OK", "image_count": 5}).to == "CLASSIFY_DOCS"

    assert _next(wf, "CLASSIFY_DOCS",
                 {"status": "OK", "has_claim_form": True}).to == "EXTRACT_DATA"

    # Sequential → Parallel 진입
    assert _next(wf, "EXTRACT_DATA",
                 {"status": "OK", "extraction_confidence": 0.92}).to == "PARALLEL_CHECKS"

    # Parallel → 모든 브랜치 OK → MATCH_PRIOR_CLAIMS
    assert _next(wf, "PARALLEL_CHECKS",
                 {"VERIFY_IDENTITY": {"status": "OK"},
                  "EXTRACT_ACCIDENT_INFO": {"status": "OK"}}).to == "MATCH_PRIOR_CLAIMS"

    # Sequential: 기지급 전문 조회 → END
    assert _next(wf, "MATCH_PRIOR_CLAIMS",
                 {"status": "OK", "accident_confirmed": True}).to == "END"

    # ── Unhappy paths ─────────────────────────────────────
    assert _next(wf, "DOWNLOAD_IMAGES",
                 {"status": "OK", "image_count": 0}).to == "MANUAL_REVIEW"

    assert _next(wf, "CLASSIFY_DOCS",
                 {"status": "OK", "has_claim_form": False}).to == "MANUAL_REVIEW"

    assert _next(wf, "EXTRACT_DATA",
                 {"status": "OK", "extraction_confidence": 0.55}).to == "MANUAL_REVIEW"

    # Parallel → 일부 브랜치 실패 → MANUAL_REVIEW
    assert _next(wf, "PARALLEL_CHECKS",
                 {"VERIFY_IDENTITY": {"status": "OK"},
                  "EXTRACT_ACCIDENT_INFO": {"status": "FAIL"}}).to == "MANUAL_REVIEW"

    # 동일사고 미확정
    assert _next(wf, "MATCH_PRIOR_CLAIMS",
                 {"status": "OK", "accident_confirmed": False}).to == "MANUAL_REVIEW"

    print("✅ All transition rules OK (happy + unhappy paths)", flush=True)


if __name__ == "__main__":
    try:
        test_load_and_transition()
        print(">>> 테스트 종료 (성공)", flush=True)
    except AssertionError as e:
        print(f"❌ AssertionError: {e}", flush=True)
        traceback.print_exc()
        sys.exit(2)
    except Exception as e:
        print(f"❌ 예외 발생: {e}", flush=True)
        traceback.print_exc()
        sys.exit(3)