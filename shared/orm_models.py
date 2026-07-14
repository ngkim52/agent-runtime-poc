# shared/orm_models.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    VARCHAR,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, BIGINT
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


def _utcnow():
    return datetime.now(timezone.utc)


# ── Workflow Definition ──────────────────────────────────────


class WorkflowDefinitionModel(Base):
    __tablename__ = "workflow_definitions"

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    workflow_id: Mapped[str] = mapped_column(VARCHAR(100), nullable=False)
    version: Mapped[str] = mapped_column(VARCHAR(20), nullable=False)
    label: Mapped[str] = mapped_column(VARCHAR(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    input_schema: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    __table_args__ = (
        UniqueConstraint("workflow_id", "version"),
    )

    states: Mapped[List["WorkflowStateModel"]] = relationship(
        back_populates="workflow", cascade="all, delete-orphan",
        order_by="WorkflowStateModel.sort_order",
    )
    transitions: Mapped[List["WorkflowTransitionModel"]] = relationship(
        back_populates="workflow", cascade="all, delete-orphan",
        order_by="WorkflowTransitionModel.sort_order",
    )


class WorkflowStateModel(Base):
    __tablename__ = "workflow_states"

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    wf_def_id: Mapped[int] = mapped_column(ForeignKey("workflow_definitions.id", ondelete="CASCADE"), nullable=False)
    state_id: Mapped[str] = mapped_column(VARCHAR(100), nullable=False)
    type: Mapped[str] = mapped_column(VARCHAR(20), nullable=False)
    task_type: Mapped[Optional[str]] = mapped_column(VARCHAR(100))
    timeout_sec: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    description: Mapped[Optional[str]] = mapped_column(Text)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    instruction: Mapped[Optional[str]] = mapped_column(Text)
    inputs: Mapped[Optional[List[str]]] = mapped_column(ARRAY(VARCHAR(100)))
    input_schema: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)
    output_schema: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)
    tools: Mapped[Optional[List[str]]] = mapped_column(ARRAY(VARCHAR(100)))
    max_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint("wf_def_id", "state_id"),
        CheckConstraint("type IN ('start', 'task', 'parallel', 'end')"),
    )

    workflow: Mapped["WorkflowDefinitionModel"] = relationship(back_populates="states")
    branches: Mapped[List["WorkflowBranchModel"]] = relationship(
        back_populates="state", cascade="all, delete-orphan",
        order_by="WorkflowBranchModel.sort_order",
    )


class WorkflowBranchModel(Base):
    __tablename__ = "workflow_branches"

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    state_id: Mapped[int] = mapped_column(ForeignKey("workflow_states.id", ondelete="CASCADE"), nullable=False)
    branch_id: Mapped[str] = mapped_column(VARCHAR(100), nullable=False)
    task_type: Mapped[str] = mapped_column(VARCHAR(100), nullable=False)
    timeout_sec: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    instruction: Mapped[Optional[str]] = mapped_column(Text)
    inputs: Mapped[Optional[List[str]]] = mapped_column(ARRAY(VARCHAR(100)))
    input_schema: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)
    output_schema: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)
    tools: Mapped[Optional[List[str]]] = mapped_column(ARRAY(VARCHAR(100)))
    max_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint("state_id", "branch_id"),
    )

    state: Mapped["WorkflowStateModel"] = relationship(back_populates="branches")


class WorkflowTransitionModel(Base):
    __tablename__ = "workflow_transitions"

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    wf_def_id: Mapped[int] = mapped_column(ForeignKey("workflow_definitions.id", ondelete="CASCADE"), nullable=False)
    from_state: Mapped[str] = mapped_column(VARCHAR(100), nullable=False)
    to_state: Mapped[str] = mapped_column(VARCHAR(100), nullable=False)
    condition_expr: Mapped[Optional[str]] = mapped_column(Text)
    label: Mapped[Optional[str]] = mapped_column(VARCHAR(200))
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    workflow: Mapped["WorkflowDefinitionModel"] = relationship(back_populates="transitions")


# ── Runtime Instances ────────────────────────────────────────


class WorkflowInstanceModel(Base):
    __tablename__ = "workflow_instances"

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    instance_id: Mapped[str] = mapped_column(VARCHAR(100), nullable=False, unique=True)
    wf_def_id: Mapped[int] = mapped_column(ForeignKey("workflow_definitions.id"), nullable=False)
    workflow_id: Mapped[str] = mapped_column(VARCHAR(100), nullable=False)
    workflow_version: Mapped[str] = mapped_column(VARCHAR(20), nullable=False)
    initial_input: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)
    current_state: Mapped[str] = mapped_column(VARCHAR(100), nullable=False, default="START")
    status: Mapped[str] = mapped_column(VARCHAR(20), nullable=False, default="RUNNING")
    final_state: Mapped[Optional[str]] = mapped_column(VARCHAR(100))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint("status IN ('RUNNING', 'COMPLETED', 'FAILED')"),
        Index("idx_instances_status", "status"),
        Index("idx_instances_created_at", created_at.desc()),
    )

    step_results: Mapped[List["StepResultModel"]] = relationship(
        back_populates="instance", cascade="all, delete-orphan",
        order_by="StepResultModel.sort_order",
    )


class StepResultModel(Base):
    __tablename__ = "workflow_step_results"

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    instance_id: Mapped[str] = mapped_column(ForeignKey("workflow_instances.instance_id", ondelete="CASCADE"), nullable=False)
    state_id: Mapped[str] = mapped_column(VARCHAR(100), nullable=False)
    task_type: Mapped[Optional[str]] = mapped_column(VARCHAR(100))
    wf_instance_id: Mapped[Optional[str]] = mapped_column(VARCHAR(100))
    status: Mapped[str] = mapped_column(VARCHAR(20), nullable=False)
    result: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    error: Mapped[Optional[str]] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        CheckConstraint("status IN ('OK', 'FAIL', 'SKIP')"),
        Index("idx_step_results_instance", "instance_id"),
    )

    instance: Mapped["WorkflowInstanceModel"] = relationship(back_populates="step_results")
