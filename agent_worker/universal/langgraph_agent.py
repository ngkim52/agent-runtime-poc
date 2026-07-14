"""
LangGraph ReAct Agent — Universal AI Worker의 LLM + Tool 실행 엔진.

create_react_agent(prebuilt)를 사용하여 tool calling 루프를 자동 처리:
  LLM → tool_call → execute → LLM → ... → final answer

최적화:
  - 그래프를 모듈 로드 시 1회만 컴파일 (create_react_agent 중복 호출 제거)
  - graph.invoke()에 전체 실행 타임아웃 적용
  - max_tokens 제한으로 LLM 응답 길이 제어
"""
from __future__ import annotations
import asyncio
import concurrent.futures
import json
import logging
import os
import threading
import time
from typing import Any, Dict, Generator, List, Optional, Sequence

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langchain_core.tools import tool as lc_tool
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import create_react_agent

from . import tools as worker_tools

log = logging.getLogger("worker.langgraph")

# ── LLM 설정 ──
LLM_API_KEY = os.getenv("OPENAI_API_KEY", "")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://api.openai.com/v1")

# 기본 에이전트 타임아웃 (초) — 이 시간이 지나면 강제 중단
_AGENT_TIMEOUT = float(os.getenv("AGENT_TIMEOUT", "300"))

# 개별 LLM API 호출 타임아웃 (초) — 느린 LLM 응답 대응
_LLM_TIMEOUT = float(os.getenv("LLM_TIMEOUT", "180"))


def _build_tools(tool_names: List[str]) -> List:
    """주어진 tool 이름 목록에 해당하는 LangChain tool 객체 리스트 반환."""
    name_set = set(tool_names)
    return [t for t in _ALL_LC_TOOLS if t.name in name_set]


# ── LangChain Tool 정의 ──
# 기존 worker_tools의 함수들을 LangChain @tool 데코레이터로 감싼 래퍼


@lc_tool
def web_search(query: str, num_results: int = 5) -> str:
    """Search the web for current information on a topic. Returns search results with titles and snippets."""
    log.info("web_search: query=%s", query)
    return worker_tools.web_search(query=query, num_results=num_results)


@lc_tool
def web_fetch(url: str) -> str:
    """Fetch and extract the main content from a URL. Returns the page title and text content."""
    log.info("web_fetch: url=%s", url)
    return worker_tools.web_fetch(url=url)


@lc_tool
def calculator(expression: str) -> str:
    """Evaluate a mathematical expression. Returns the result."""
    return worker_tools.calculator(expression=expression)


@lc_tool
def read_file(path: str) -> str:
    """Read the contents of a file from the project source directory. Returns the file content as text. Path is relative to the project root."""
    return worker_tools.read_file(path=path)


@lc_tool
def read_files(paths: str) -> str:
    """Read MULTIPLE files at once. Provide paths as a comma-separated string. Max 6 files per call. Returns each file's content with a header. Much faster than calling read_file individually."""
    return worker_tools.read_files(paths=paths)


@lc_tool
def glob(pattern: str) -> str:
    """Search for files matching a glob pattern in the project directory. Returns a list of matching file paths."""
    return worker_tools.glob(pattern=pattern)


@lc_tool
def grep(pattern: str, path: str = ".", max_results: int = 50) -> str:
    """Search for text or regex patterns inside file contents. Returns matching lines with file paths and line numbers."""
    return worker_tools.grep(pattern=pattern, path=path, max_results=max_results)


@lc_tool
def list_directory(path: str = ".") -> str:
    """List all entries in a directory. Returns files and subdirectories with type indicators."""
    return worker_tools.list_directory(path=path)


@lc_tool
def load_workflow_db(workflow_id: str, version: str = "1.0") -> str:
    """Load a workflow definition from the database by workflow_id and version. Returns the full workflow definition with states and transitions."""
    return worker_tools.load_workflow_db(workflow_id=workflow_id, version=version)


@lc_tool
def save_workflow_db(workflow_id: str, version: str, workflow_json: str) -> str:
    """Save a workflow definition to the database. Replaces existing version if it exists. IMPORTANT: Only call this AFTER the user has confirmed."""
    return worker_tools.save_workflow_db(workflow_id=workflow_id, version=version, workflow_json=workflow_json)


@lc_tool
def delete_workflow(workflow_id: str, version: str = "1.0") -> str:
    """Delete a workflow definition and all its instances from the database. IMPORTANT: Only call this AFTER the user has confirmed."""
    return worker_tools.delete_workflow(workflow_id=workflow_id, version=version)


@lc_tool
def list_workflows() -> str:
    """List all workflow definitions registered in the database. Returns a summary of each workflow."""
    return worker_tools.list_workflows()


@lc_tool
def validate_workflow(workflow_json: str) -> str:
    """Validate a workflow definition JSON string against the schema. Returns valid=True or valid=False with error details."""
    return worker_tools.validate_workflow(workflow_json=workflow_json)


@lc_tool
def load_workflow_yaml(workflow_id: str, version: str = "1.0") -> str:
    """Load a workflow definition from YAML file (biz_workflows/) by workflow_id and version. Returns the full workflow definition."""
    return worker_tools.load_workflow_yaml(workflow_id=workflow_id, version=version)


@lc_tool
def get_weather(latitude: float, longitude: float) -> str:
    """Get current weather and 7-day forecast for a location using Open-Meteo API (free, no API key required). Provide latitude and longitude coordinates. Returns temperature, precipitation, humidity, wind speed, and weather condition data."""
    return worker_tools.get_weather(latitude=latitude, longitude=longitude)


_ALL_LC_TOOLS = [
    web_search,
    web_fetch,
    calculator,
    read_file,
    read_files,
    glob,
    grep,
    list_directory,
    load_workflow_db,
    save_workflow_db,
    delete_workflow,
    list_workflows,
    validate_workflow,
    load_workflow_yaml,
    get_weather,
]


# ── 싱글턴 모델 + 그래프 캐시 ──

_DEFAULT_MAX_TOKENS = 4096


def _create_model(max_tokens: int = _DEFAULT_MAX_TOKENS) -> ChatOpenAI:
    """ChatOpenAI 모델을 생성 (timeout=LLM_TIMEOUT, max_tokens=설정가능, thinking mode disabled)."""
    return ChatOpenAI(
        model=LLM_MODEL,
        api_key=LLM_API_KEY,
        base_url=LLM_BASE_URL,
        temperature=0.3,
        max_tokens=max_tokens,
        timeout=_LLM_TIMEOUT,   # 개별 LLM 호출 타임아웃 (env LLM_TIMEOUT)
        max_retries=1,          # 재시도 1회로 제한
        extra_body={"thinking": {"type": "disabled"}},  # deepseek 사고 모드 끄기
    )


# ── 그래프 캐시: tool set별로 create_react_agent 결과를 재사용 ──
_graph_cache: Dict[frozenset, Any] = {}
_graph_cache_lock = threading.Lock()


def _get_graph(tool_names: List[str], max_tokens: int = _DEFAULT_MAX_TOKENS):
    """tool set별 캐시된 compiled graph 반환. 없으면 생성 후 캐시."""
    key = frozenset([*tool_names, f"tok:{max_tokens}"])
    if key not in _graph_cache:
        with _graph_cache_lock:
            if key not in _graph_cache:  # double-check
                enabled = _build_tools(tool_names)
                _graph_cache[key] = create_react_agent(
                    model=_create_model(max_tokens=max_tokens),
                    tools=enabled,
                    # prompt= 는 invoke 시점에 SystemMessage로 직접 전달하므로 생략
                )
                log.info("compiled new graph for %d tools (max_tokens=%d): %s", len(enabled), max_tokens, list(tool_names))
    return _graph_cache[key]


def _extract_balanced_json(text: str) -> Optional[Dict[str, Any]]:
    """Find the first balanced JSON object in text using brace counting."""
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    in_str = False
    escaped = False
    for i in range(start, len(text)):
        ch = text[i]
        if escaped:
            escaped = False
            continue
        if ch == "\\" and in_str:
            escaped = True
            continue
        if ch == '"' and not escaped:
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = text[start:i+1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError:
                    start = text.find("{", i + 1)
                    if start == -1:
                        return None
                    depth = 0
                    i = start - 1
                    continue
    return None


def _derive_message(result: Dict[str, Any]) -> str:
    """Derive a human-readable message from parsed result when 'message' key is missing."""
    if "llm_output" in result:
        return result["llm_output"][:500]
    if "error" in result:
        return f"Error: {result['error']}"
    if "summary" in result:
        return result["summary"]
    if result.get("workflow"):
        states = result["workflow"].get("states", [])
        return f"Designed workflow with {len(states)} state(s). Review the preview below."
    if result.get("action") == "ask_question":
        return result.get("message", "Let me ask you a question.")
    keys = [k for k in result if isinstance(result[k], str) and len(result[k]) > 10]
    if keys:
        return result[keys[0]][:500]
    return "I've processed your request."


def run_agent(
    instruction: str,
    payload: Dict[str, Any],
    tool_names: List[str],
    max_turns: int = 25,
    timeout: float | None = None,
    max_tokens: int | None = None,
) -> Dict[str, Any]:
    """
    LangGraph ReAct Agent 실행.

    Args:
        instruction: System prompt (LLM 역할 지시)
        payload: LLM에 전달할 컨텍스트 (이전 step 결과 등)
        tool_names: 사용 가능한 tool 이름 목록
        max_turns: 최대 턴 수 (safety limit)
        timeout: 전체 에이전트 실행 타임아웃 (초). 기본값: _AGENT_TIMEOUT
        max_tokens: LLM 응답 최대 토큰 수. 기본값: _DEFAULT_MAX_TOKENS (4096)

    Returns:
        최종 LLM 응답 (파싱된 JSON dict)
    """
    if not LLM_API_KEY:
        log.warning("OPENAI_API_KEY not set — falling back to mock response")
        # Check if this is a designer task (very long instruction with "Workflow Designer")
        if "workflow designer" in instruction.lower() or len(instruction) > 500:
            user_msg = (payload or {}).get("message", "")
            return {
                "message": f"I'll create a workflow based on: {user_msg or 'your request'}",
                "workflow": {
                    "workflow_id": (user_msg.split()[0] if user_msg else "demo") + "_workflow",
                    "version": "1.0",
                    "description": f"Auto-generated: {user_msg}",
                    "states": [
                        {"id": "START", "type": "start"},
                        {"id": "PROCESS", "type": "task", "task": "ai_task",
                         "instruction": f"You are an AI assistant. Process: {user_msg or 'the user request'}",
                         "inputs": ["_input"],
                         "output_schema": {"type": "object", "properties": {"result": {"type": "string"}}},
                         "timeout_sec": 300},
                        {"id": "END", "type": "end"},
                    ],
                    "transitions": [
                        {"from": "START", "to": "PROCESS"},
                        {"from": "PROCESS", "to": "END"},
                    ],
                },
                "action": "show_preview",
            }
        # Generic task mock (used by workflow execution, not designer)
        low = instruction.lower()
        if "source" in low or "analyze" in low:
            return {"result": "Analysis complete.", "architecture": {"layers": [{"name": "Orchestrator", "role": "WF engine"}]}, "data_flow_summary": "Orchestrator -> Dapr WF -> Worker"}
        if "search" in low or "web" in low:
            return {"result": "Search complete.", "facts": [{"title": "Dapr", "description": "Durable execution"}], "summary": "Dapr is a distributed app runtime."}
        if "slide" in low or "presentation" in low:
            return {"result": "Presentation created.", "slides": [{"slide_number": 1, "title": "POC", "bullets": ["Dapr-based"]}]}
        if "review" in low or "quality" in low:
            return {"result": "Review complete.", "scores": {"completeness": 8, "accuracy": 9}, "overall_score": 8.25, "issues": [], "verdict": "approved"}
        return {"message": "Task completed.", "workflow": None, "action": None}

    timeout = timeout or _AGENT_TIMEOUT
    # LLM TIMEOUT보다 step timeout이 작으면 LLM TIMEOUT 사용 (truncation 방지)
    if timeout < _LLM_TIMEOUT:
        log.warning("timeout=%s < LLM_TIMEOUT=%s: using LLM_TIMEOUT to avoid truncation", timeout, _LLM_TIMEOUT)
        timeout = _LLM_TIMEOUT
    effective_max_tokens = max_tokens or _DEFAULT_MAX_TOKENS

    # 1) 캐시된 그래프 로드 (tool set + max_tokens별)
    graph = _get_graph(tool_names, max_tokens=effective_max_tokens)

    # 2) instruction → SystemMessage, payload → HumanMessage
    input_messages: List[BaseMessage] = [
        SystemMessage(content=instruction),
        HumanMessage(content=json.dumps(payload, indent=2, ensure_ascii=False)),
    ]

    # LLM input 로그 (첫 1000자만)
    log.info("=== LLM INPUT ===")
    log.info("instruction[:1000]: %s", instruction[:1000])
    log.info("payload keys=%s size=%d", list(payload.keys()), len(json.dumps(payload)))

    # 3) 실행 (전체 타임아웃 적용, Cloudflare 524 재시도 포함)
    _MAX_524_RETRIES = 2  # 524 오류 최대 재시도 횟수
    _524_BACKOFF = 30     # 524 재시도 대기 시간 (초) — Cloudflare 권장 120s+ 실제로는 짧은 백오프로도 충분

    last_error: str | None = None
    for attempt in range(1 + _MAX_524_RETRIES):
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    graph.invoke,
                    {"messages": input_messages},
                    {"recursion_limit": max_turns + 5},
                )
                result = future.result(timeout=timeout)
                break  # 성공 → 루프 탈출
        except concurrent.futures.TimeoutError:
            log.error("LangGraph agent timed out after %ss", timeout)
            return {"error": f"Agent timed out after {timeout}s", "llm_output": None}
        except Exception as e:
            estr = str(e)
            # Cloudflare 524 — 재시도 가능 (retryable=True)
            if "524" in estr and attempt < _MAX_524_RETRIES:
                log.warning(
                    "Cloudflare 524 (attempt %d/%d), backing off %ds: %s",
                    attempt + 1, _MAX_524_RETRIES + 1, _524_BACKOFF, estr[:200],
                )
                time.sleep(_524_BACKOFF)
                last_error = estr
                continue
            # 그 외 오류 → 즉시 실패
            log.error("LangGraph agent failed: %s", e)
            return {"error": f"LangGraph agent failed: {e}", "llm_output": None}

    if last_error:
        log.error("LangGraph agent failed after %d retries: %s", _MAX_524_RETRIES, last_error)
        return {"error": f"LangGraph agent failed (524): {last_error}", "llm_output": None}

    # LLM output 로그 — 전체 메시지 history에서 AI 응답 + Tool 호출 추적
    log.info("=== LLM OUTPUT (%d messages) ===", len(result["messages"]))
    for i, msg in enumerate(result["messages"]):
        if isinstance(msg, AIMessage) and msg.content:
            log.info("[turn %d] LLM response (%d chars): %s", i, len(msg.content), msg.content[:500])
        if isinstance(msg, AIMessage) and msg.tool_calls:
            for tc in msg.tool_calls:
                log.info("[turn %d] tool_call: %s(%s)", i, tc["name"], json.dumps(tc["args"]))
        if isinstance(msg, ToolMessage):
            log.info("[turn %d] tool_result (%d chars): %s", i, len(msg.content), msg.content[:300])

    # 4) 최종 메시지 추출
    final_msg = result["messages"][-1]
    content = final_msg.content if hasattr(final_msg, "content") else str(final_msg)

    # 5) JSON 파싱 시도 — brace matching 기반 (중첩 ```` 무시)
    parsed = _extract_balanced_json(content)
    if parsed is not None:
        return parsed
    return {"llm_output": content}


def run_agent_stream(
    instruction: str,
    payload: Dict[str, Any],
    tool_names: List[str],
    max_turns: int = 25,
    timeout: float | None = None,
    max_tokens: int | None = None,
) -> Generator[Dict[str, Any], None, None]:
    """
    Streaming variant of run_agent(). Yields SSE-friendly events during execution.

    Yields:
        {"type": "thinking", "content": str} — LLM reasoning text
        {"type": "tool_call", "name": str, "args": dict} — Tool invocation
        {"type": "tool_result", "name": str, "content_preview": str} — Tool result
        {"type": "final", "result": dict} — Final parsed output
    """
    if not LLM_API_KEY:
        # Mock fallback: yield single final event
        log.warning("OPENAI_API_KEY not set — fallback to mock streaming")
        user_msg = (payload or {}).get("message", "").lower()
        # Check user's message (NOT instruction) for intent
        wf_states = []
        wf_transitions = []
        if "analyze" in user_msg or "source" in user_msg or "research" in user_msg:
            wf_name = "analysis_workflow"
            desc = "Code analysis workflow"
            wf_states = [
                {"id": "START", "type": "start"},
                {"id": "ANALYZE_CODE", "type": "task", "task": "ai_task",
                 "instruction": "You are a code analysis expert. Analyze source code and identify key patterns, architecture, and data flow. Return findings as JSON with 'architecture' and 'data_flow_summary' keys.",
                 "inputs": ["_input"],
                 "output_schema": {"type": "object", "properties": {"architecture": {"type": "object"}, "data_flow_summary": {"type": "string"}}},
                 "timeout_sec": 300},
                {"id": "END", "type": "end"},
            ]
            wf_transitions = [{"from": "START", "to": "ANALYZE_CODE"}, {"from": "ANALYZE_CODE", "to": "END"}]
            message = "Analysis workflow created! I'll set up a code analysis pipeline."
        elif "search" in user_msg or "web" in user_msg or "research" in user_msg:
            wf_name = "research_workflow"
            desc = "Web research workflow"
            wf_states = [
                {"id": "START", "type": "start"},
                {"id": "RESEARCH", "type": "task", "task": "ai_task",
                 "instruction": "You are a research assistant. Search the web for relevant information and compile findings. Return JSON with 'results' (array) and 'summary' (string).",
                 "inputs": ["_input"],
                 "tools": ["web_search", "web_fetch"],
                 "output_schema": {"type": "object", "properties": {"results": {"type": "array", "items": {"type": "object"}}, "summary": {"type": "string"}}},
                 "timeout_sec": 300},
                {"id": "END", "type": "end"},
            ]
            wf_transitions = [{"from": "START", "to": "RESEARCH"}, {"from": "RESEARCH", "to": "END"}]
            message = "Research workflow created! Includes web search and fetch tools."
        else:
            wf_name = (user_msg.split()[0] if user_msg else "demo") + "_workflow"
            desc = f"Workflow for: {user_msg or 'general task'}"
            step_instruction = f"You are an AI assistant. Process the following task and return structured results: {user_msg}" if user_msg else "You are an AI assistant. Process the user's request and return results."
            wf_states = [
                {"id": "START", "type": "start"},
                {"id": "PROCESS", "type": "task", "task": "ai_task",
                 "instruction": step_instruction,
                 "inputs": ["_input"],
                 "output_schema": {"type": "object", "properties": {"result": {"type": "string"}}},
                 "timeout_sec": 300},
                {"id": "END", "type": "end"},
            ]
            wf_transitions = [{"from": "START", "to": "PROCESS"}, {"from": "PROCESS", "to": "END"}]
            message = f"Workflow created! I set up a basic pipeline with 1 step."
        result = {
            "message": message,
            "workflow": {
                "workflow_id": wf_name,
                "version": "1.0",
                "description": desc,
                "states": wf_states,
                "transitions": wf_transitions,
            },
            "action": "show_preview",
        }
        yield {"type": "final", "result": result}
        return

    effective_timeout = timeout or _AGENT_TIMEOUT
    if effective_timeout < _LLM_TIMEOUT:
        effective_timeout = _LLM_TIMEOUT
    effective_max_tokens = max_tokens or _DEFAULT_MAX_TOKENS

    graph = _get_graph(tool_names, max_tokens=effective_max_tokens)

    input_messages: List[BaseMessage] = [
        SystemMessage(content=instruction),
        HumanMessage(content=json.dumps(payload, indent=2, ensure_ascii=False)),
    ]

    # Track final messages across events
    final_messages: List[BaseMessage] | None = None

    try:
        # Use graph.stream() for step-by-step events
        for event in graph.stream(
            {"messages": input_messages},
            {"recursion_limit": max_turns + 5},
        ):
            # Find messages in event (event format: {"agent": {"messages": [...]}})
            event_msgs = None
            for node_key in ("agent", "tools"):
                if node_key in event and isinstance(event[node_key], dict):
                    msgs = event[node_key].get("messages")
                    if msgs is not None:
                        event_msgs = msgs
                        final_messages = msgs

            # Handle agent node events
            if "agent" in event and event_msgs:
                for msg in event_msgs:
                    if isinstance(msg, AIMessage):
                        if msg.tool_calls:
                            for tc in msg.tool_calls:
                                yield {"type": "tool_call", "name": tc["name"], "args": tc["args"]}
                        if msg.content and not msg.tool_calls:
                            yield {"type": "thinking", "content": msg.content[:500]}
            # Handle tools node events
            elif "tools" in event and event_msgs:
                for msg in event_msgs:
                    if isinstance(msg, ToolMessage):
                        preview = (msg.content or "")[:300]
                        yield {"type": "tool_result", "name": msg.name or "", "content_preview": preview}

    except Exception as e:
        log.error("LangGraph stream failed: %s", e)
        yield {"type": "final", "result": {"error": f"Stream failed: {e}"}}
        return

    # Extract final response from tracked messages
    if final_messages:
        final_msg = final_messages[-1]
        content = final_msg.content if hasattr(final_msg, "content") else str(final_msg)
        parsed = _extract_balanced_json(content)
        if parsed is not None:
            # Always include a 'message' key so frontend never shows '(no response)'
            if "message" not in parsed:
                parsed["message"] = parsed.get("llm_output") or _derive_message(parsed) or "Task completed."
            yield {"type": "final", "result": parsed}
        else:
            yield {"type": "final", "result": {"message": "I processed your request.", "llm_output": content}}
    else:
        yield {"type": "final", "result": {"message": "Agent completed with no output.", "workflow": None, "action": None}}
