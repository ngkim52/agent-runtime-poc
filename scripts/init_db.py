"""
DB 초기화 스크립트: ORM 모델 기반 테이블 생성.

실행:
  python scripts/init_db.py                           # 기본 DB_URL 사용
  DATABASE_URL="postgresql://..." python scripts/init_db.py  # 커스텀 DB_URL
"""
from __future__ import annotations
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from shared.db import engine, Base
from shared.orm_models import (  # noqa: F401 - 모델 import 로 Base 에 등록
    WorkflowDefinitionModel,
    WorkflowStateModel,
    WorkflowBranchModel,
    WorkflowTransitionModel,
    WorkflowInstanceModel,
    StepResultModel,
)


def main():
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:changeme@localhost:5432/poc_workflow")
    print(f"Connecting to: {db_url}")
    print("Creating tables ...")

    Base.metadata.create_all(bind=engine)

    created = sorted(Base.metadata.tables.keys())
    print(f"Done. {len(created)} tables created:")
    for t in created:
        print(f"  - {t}")


if __name__ == "__main__":
    main()
