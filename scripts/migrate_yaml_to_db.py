# scripts/migrate_yaml_to_db.py
"""
YAML 워크플로우 정의를 PostgreSQL로 마이그레이션.

실행:  cd poc && python scripts/migrate_yaml_to_db.py
"""
from __future__ import annotations
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from shared.workflow_loader import BizWorkflowLoader
from shared.workflow_repository import BizWorkflowRepository

ALL_WORKFLOWS = [
    ("claim_adjudication", "1.0"),
    ("poc_doc_generation", "1.0"),
    ("poc_doc_generation", "2.1"),
    ("poc_doc_generation", "3.0"),
    ("poc_doc_generation", "4.1"),
]


def main():
    loader = BizWorkflowLoader(base_dir=os.path.join(PROJECT_ROOT, "biz_workflows"))
    repo = BizWorkflowRepository()

    for wf_id, wf_ver in ALL_WORKFLOWS:
        print(f"\nLoading YAML: {wf_id} v{wf_ver} ...")
        try:
            wf = loader.load(wf_id, wf_ver)
        except FileNotFoundError:
            print(f"  SKIP - file not found for {wf_id} v{wf_ver}")
            continue
        print(f"  states={len(wf.states)}, transitions={len(wf.transitions)}")

        print("  Saving to DB ...")
        def_id = repo.save_workflow(wf)
        print(f"  OK - workflow_definitions.id = {def_id}")

        # Verify
        wf2 = repo.load(wf_id, wf_ver)
        assert len(wf2.states) == len(wf.states), f"State count mismatch: {len(wf2.states)} != {len(wf.states)}"
        assert len(wf2.transitions) == len(wf.transitions), f"Transition count mismatch"
        print(f"  Verified: {len(wf2.states)} states, {len(wf2.transitions)} transitions OK")

    print("\nAll migrations complete.")


if __name__ == "__main__":
    main()
