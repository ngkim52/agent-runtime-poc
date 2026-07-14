"""
Workflow Designer Agent — Tests for new CRUD tools + AI design endpoint.

Run:  cd poc && python -m pytest tests/test_workflow_designer.py -v
"""
from __future__ import annotations
import json
import pytest
from typing import Any, Dict

from agent_worker.universal.tools import (
    TOOL_DEFINITIONS,
    TOOL_REGISTRY,
    validate_workflow,
    list_workflows,
    load_workflow_yaml,
)
from agent_worker.universal.handler import WORKFLOW_DESIGNER_INSTRUCTION
from agent_worker.universal.langgraph_agent import _ALL_LC_TOOLS


class TestToolRegistration:
    """6개 신규 도구가 모두 등록되었는지 검증."""

    NEW_TOOL_NAMES = [
        "load_workflow_db",
        "save_workflow_db",
        "delete_workflow",
        "list_workflows",
        "validate_workflow",
        "load_workflow_yaml",
        "get_weather",
    ]

    def test_tool_definitions_has_all_new_tools(self):
        names = [t["function"]["name"] for t in TOOL_DEFINITIONS]
        for nt in self.NEW_TOOL_NAMES:
            assert nt in names, f"{nt} missing from TOOL_DEFINITIONS"

    def test_tool_registry_has_all_new_tools(self):
        for nt in self.NEW_TOOL_NAMES:
            assert nt in TOOL_REGISTRY, f"{nt} missing from TOOL_REGISTRY"

    def test_lc_tools_has_all_new_tools(self):
        lc_names = [t.name for t in _ALL_LC_TOOLS]
        for nt in self.NEW_TOOL_NAMES:
            assert nt in lc_names, f"{nt} missing from _ALL_LC_TOOLS"

    def test_total_tool_count(self):
        assert len(TOOL_DEFINITIONS) == 15
        assert len(TOOL_REGISTRY) == 15
        assert len(_ALL_LC_TOOLS) == 15


class TestValidateWorkflow:
    """validate_workflow 도구 테스트."""

    def test_valid_workflow(self):
        """정상적인 workflow JSON → valid=True"""
        wf_json = json.dumps({
            "workflow_id": "test_wf",
            "version": "1.0",
            "states": [
                {"id": "START", "type": "start"},
                {"id": "STEP_1", "type": "task", "task": "ai_task"},
                {"id": "END", "type": "end"},
            ],
            "transitions": [
                {"from": "START", "to": "STEP_1"},
                {"from": "STEP_1", "to": "END"},
            ],
        })
        result = json.loads(validate_workflow(wf_json))
        assert result["valid"] is True
        assert len(result["errors"]) == 0

    def test_invalid_workflow_missing_states(self):
        """states 필드 누락 → valid=False"""
        wf_json = json.dumps({
            "workflow_id": "test_wf",
            "version": "1.0",
            "transitions": [],
        })
        result = json.loads(validate_workflow(wf_json))
        assert result["valid"] is False
        assert len(result["errors"]) > 0

    def test_invalid_json(self):
        """JSON 파싱 불가 → valid=False"""
        result = json.loads(validate_workflow("not json"))
        assert result["valid"] is False
        assert len(result["errors"]) > 0

    def test_invalid_state_type(self):
        """존재하지 않는 state type → valid=False"""
        wf_json = json.dumps({
            "workflow_id": "test_wf",
            "version": "1.0",
            "states": [
                {"id": "START", "type": "invalid_type"},
            ],
            "transitions": [],
        })
        result = json.loads(validate_workflow(wf_json))
        assert result["valid"] is False

    def test_valid_parallel_workflow(self):
        """parallel state 포함 워크플로우 → valid=True"""
        wf_json = json.dumps({
            "workflow_id": "test_parallel",
            "version": "1.0",
            "states": [
                {"id": "START", "type": "start"},
                {"id": "PARALLEL", "type": "parallel", "branches": [
                    {"id": "BRANCH_A", "task": "ai_task"},
                    {"id": "BRANCH_B", "task": "ai_task"},
                ]},
                {"id": "END", "type": "end"},
            ],
            "transitions": [
                {"from": "START", "to": "PARALLEL"},
                {"from": "PARALLEL", "to": "END"},
            ],
        })
        result = json.loads(validate_workflow(wf_json))
        assert result["valid"] is True


class TestListWorkflows:
    """list_workflows 도구 테스트."""

    def test_returns_string(self):
        result = list_workflows()
        assert isinstance(result, str)
        # Should be valid JSON array
        parsed = json.loads(result)
        assert isinstance(parsed, list)


class TestLoadWorkflowYaml:
    """load_workflow_yaml 도구 테스트."""

    def test_load_known_workflow(self):
        """기존 YAML 파일 로드 (claim_adjudication v1.0)"""
        result = load_workflow_yaml("claim_adjudication", "1.0")
        parsed = json.loads(result)
        assert parsed["workflow_id"] == "claim_adjudication"
        assert parsed["version"] == "1.0"
        assert len(parsed["states"]) > 0
        assert len(parsed["transitions"]) > 0

    def test_load_doc_generation_v4(self):
        """poc_doc_generation v4.1 (4-way parallel)"""
        result = load_workflow_yaml("poc_doc_generation", "4.1")
        parsed = json.loads(result)
        assert parsed["workflow_id"] == "poc_doc_generation"
        assert "version" in parsed
        # Find parallel state
        parallel_states = [s for s in parsed["states"] if s["type"] == "parallel"]
        assert len(parallel_states) > 0
        # Should have 4+ branches
        assert len(parallel_states[0]["branches"]) >= 4

    def test_nonexistent_workflow(self):
        """존재하지 않는 워크플로우 → error"""
        result = load_workflow_yaml("nonexistent", "1.0")
        parsed = json.loads(result)
        assert "error" in parsed


class TestWorkflowDesignerInstruction:
    """WORKFLOW_DESIGNER_INSTRUCTION 내용 검증."""

    def test_instruction_contains_all_tool_names(self):
        tool_names = [
            "load_workflow_db", "save_workflow_db", "delete_workflow",
            "list_workflows", "validate_workflow", "load_workflow_yaml",
        ]
        for tn in tool_names:
            assert tn in WORKFLOW_DESIGNER_INSTRUCTION, f"{tn} not in instruction"

    def test_instruction_contains_hitl_rules(self):
        assert "confirm" in WORKFLOW_DESIGNER_INSTRUCTION.lower() or "ask" in WORKFLOW_DESIGNER_INSTRUCTION.lower()

    def test_instruction_requires_json_output(self):
        assert "JSON" in WORKFLOW_DESIGNER_INSTRUCTION
        assert "message" in WORKFLOW_DESIGNER_INSTRUCTION
        assert "action" in WORKFLOW_DESIGNER_INSTRUCTION

    def test_instruction_contains_state_types(self):
        for st in ("start", "task", "parallel", "end"):
            assert st in WORKFLOW_DESIGNER_INSTRUCTION

    def test_instruction_length(self):
        """충분히 상세한 instruction인지 확인"""
        assert len(WORKFLOW_DESIGNER_INSTRUCTION) > 500


class TestAIDesignEndpoint:
    """AI Design API 엔드포인트 테스트 (Mock LLM 사용)."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from orchestrator.main import app
        return TestClient(app)

    def test_ai_create_page(self, client):
        """GET /workflows/ai-create → 200"""
        resp = client.get("/workflows/ai-create")
        assert resp.status_code == 200
        assert "AI Workflow Designer" in resp.text

    def test_ai_edit_page_existing(self, client):
        """GET /workflows/poc_doc_generation/4.1/ai-edit → 200 (워크플로우 존재)"""
        resp = client.get("/workflows/poc_doc_generation/4.1/ai-edit")
        assert resp.status_code == 200
        assert "AI Workflow Designer" in resp.text

    def test_ai_edit_page_not_found(self, client):
        """GET /workflows/nonexistent/1.0/ai-edit → 200 (not found 메시지)"""
        resp = client.get("/workflows/nonexistent/1.0/ai-edit")
        assert resp.status_code == 200
        assert "AI Workflow Designer" in resp.text

    def test_ai_design_chat_without_api_key(self, client):
        """OPENAI_API_KEY 없이 채팅 → mock 응답 (message 필드 존재)"""
        resp = client.post("/api/workflows/ai-design", json={
            "message": "Create a simple workflow with start, task, and end states",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "message" in data
        # Without API key, the mock fallback should return a message
        assert isinstance(data["message"], str)

    def test_ai_design_chat_empty_message(self, client):
        """빈 메시지로 채팅 요청"""
        resp = client.post("/api/workflows/ai-design", json={
            "message": "",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "message" in data


class TestRunAgentStream:
    """run_agent_stream() generator 테스트."""

    def test_mock_fallback_yields_final(self):
        """Mock fallback yields single final event."""
        from agent_worker.universal.langgraph_agent import run_agent_stream
        events = list(run_agent_stream(
            instruction="Create a workflow",
            payload={"message": "hello"},
            tool_names=["list_workflows"],
        ))
        assert len(events) == 1
        assert events[0]["type"] == "final"
        assert "result" in events[0]

    def test_mock_final_has_message_key(self):
        from agent_worker.universal.langgraph_agent import run_agent_stream
        events = list(run_agent_stream(
            instruction="Create a workflow with states",
            payload={"message": "hello"},
            tool_names=["list_workflows"],
        ))
        result = events[0]["result"]
        assert "message" in result

    def test_mock_search_yields_events(self):
        from agent_worker.universal.langgraph_agent import run_agent_stream
        events = list(run_agent_stream(
            instruction="Search for information",
            payload={"message": "search"},
            tool_names=["web_search"],
        ))
        assert len(events) == 1
        assert events[0]["type"] == "final"

    def test_generator_is_generator(self):
        from agent_worker.universal.langgraph_agent import run_agent_stream
        gen = run_agent_stream(
            instruction="test",
            payload={},
            tool_names=[],
        )
        from typing import Generator
        assert isinstance(gen, Generator)


class TestAIDesignStreamEndpoint:
    """SSE streaming endpoint 테스트."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from orchestrator.main import app
        return TestClient(app)

    def test_stream_endpoint_exists(self, client):
        resp = client.post("/api/workflows/ai-design/stream", json={
            "message": "Create a workflow",
        })
        assert resp.status_code == 200
        assert resp.headers.get("content-type", "").startswith("text/event-stream")

    def test_stream_endpoint_returns_sse_format(self, client):
        resp = client.post("/api/workflows/ai-design/stream", json={
            "message": "Create a workflow",
        })
        text = resp.text
        assert text.startswith("data: ")
        assert text.strip().endswith("}")

    def test_stream_endpoint_final_event(self, client):
        resp = client.post("/api/workflows/ai-design/stream", json={
            "message": "Create a workflow",
        })
        events = []
        for line in resp.text.strip().split("\n"):
            line = line.strip()
            if line.startswith("data: "):
                events.append(json.loads(line[6:]))
        assert len(events) >= 1
        assert events[-1]["type"] == "final"
        assert isinstance(events[-1]["result"], dict)
