# shared/db.py
from __future__ import annotations
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:changeme@localhost:5432/poc_workflow",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    pass


def get_session():
    """컨텍스트 매니저 용도로 사용하려면 SessionLocal() 직접 호출해도 됩니다."""
    return SessionLocal()
