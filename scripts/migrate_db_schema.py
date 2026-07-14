# scripts/migrate_db_schema.py
"""
DB 스키마 마이그레이션: max_tokens 컬럼 추가.

실행:  cd poc && python scripts/migrate_db_schema.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['DATABASE_URL'] = os.getenv('DATABASE_URL', 'postgresql://postgres:changeme@localhost:5432/poc_workflow')

from sqlalchemy import text
from shared.db import engine

with engine.connect() as conn:
    # Check if column already exists
    result = conn.execute(
        text("SELECT column_name FROM information_schema.columns "
             "WHERE table_name='workflow_states' AND column_name='max_tokens'")
    )
    if result.fetchone() is None:
        print("Adding max_tokens to workflow_states ...")
        conn.execute(text("ALTER TABLE workflow_states ADD COLUMN max_tokens INTEGER"))
        print("  OK")
    else:
        print("Column max_tokens already exists in workflow_states")

    # Check branches table
    result = conn.execute(
        text("SELECT column_name FROM information_schema.columns "
             "WHERE table_name='workflow_branches' AND column_name='max_tokens'")
    )
    if result.fetchone() is None:
        print("Adding max_tokens to workflow_branches ...")
        conn.execute(text("ALTER TABLE workflow_branches ADD COLUMN max_tokens INTEGER"))
        print("  OK")
    else:
        print("Column max_tokens already exists in workflow_branches")

    conn.commit()
    print("\nSchema migration complete.")
