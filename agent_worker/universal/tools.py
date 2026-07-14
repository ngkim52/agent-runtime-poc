# agent_worker/universal/tools.py
"""
Universal AI Worker 내장 도구 모음.
각 도구는 LLM function calling을 통해 호출됩니다.
"""
from __future__ import annotations
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List

import httpx

log = logging.getLogger("worker.tools")

# ── 파일 시스템 도구의 베이스 경로 (보안) ──
BASE_PATH = os.environ.get("FS_BASE_PATH", "/app")


def _safe_path(path: str) -> str:
    """경로가 BASE_PATH 이내인지 보안 검증."""
    full = os.path.abspath(os.path.join(BASE_PATH, path))
    if not full.startswith(os.path.abspath(BASE_PATH)):
        raise ValueError(f"Access denied: path '{path}' is outside project directory")
    return full


# ── Tool definitions (OpenAI function calling format) ──

TOOL_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Search the web for current information on a topic. Uses Tavily AI search - returns structured results with titles, URLs, and content snippets.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query string",
                    },
                    "num_results": {
                        "type": "integer",
                        "description": "Number of results to return (max 10)",
                        "default": 5,
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "web_fetch",
            "description": "Fetch and extract the main content from a URL. Returns the page title and text content.",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "The URL to fetch content from",
                    },
                },
                "required": ["url"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read the contents of a file from the project source directory. Returns the file content as text. Path is relative to the project root.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Relative path to the file (e.g. 'orchestrator/main.py' or 'shared/models.py')",
                    },
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_files",
            "description": "Read MULTIPLE files at once. Provide paths as a comma-separated string. Max 6 files per call. Returns each file's content with a header. Much faster than calling read_file individually.",
            "parameters": {
                "type": "object",
                "properties": {
                    "paths": {
                        "type": "string",
                        "description": "Comma-separated relative paths (e.g. 'orchestrator/main.py, orchestrator/engine.py, shared/models.py'). Max 6 paths.",
                    },
                },
                "required": ["paths"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "glob",
            "description": "Search for files matching a glob pattern in the project directory. Returns a list of matching file paths.",
            "parameters": {
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Glob pattern to search (e.g. '**/*.py' finds all Python files, 'orchestrator/**/*.py' finds orchestrator Python files)",
                    },
                },
                "required": ["pattern"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "grep",
            "description": "Search for text or regex patterns inside file contents. Returns matching lines with file paths and line numbers. Use this to find where specific functions, classes, or patterns are used in the codebase.",
            "parameters": {
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Regex pattern to search for (e.g. 'def handle_' finds all functions starting with handle_)",
                    },
                    "path": {
                        "type": "string",
                        "description": "Relative path to search in (default: '.' for entire project)",
                        "default": ".",
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum results to return (default: 50)",
                        "default": 50,
                    },
                },
                "required": ["pattern"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_directory",
            "description": "List all entries in a directory. Returns files and subdirectories with type indicators. Use this to explore the project structure.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Relative path to the directory (default: '.' for project root)",
                        "default": ".",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "calculator",
            "description": "Evaluate a mathematical expression. Returns the result.",
            "parameters": {
                "type": "object",
                "properties": {
                    "expression": {
                        "type": "string",
                        "description": "The mathematical expression to evaluate (e.g. '2 + 2', '150 * 3.5')",
                    },
                },
                "required": ["expression"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "load_workflow_db",
            "description": "Load a workflow definition from the database by workflow_id and version. Returns the full workflow definition with states and transitions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "workflow_id": {
                        "type": "string",
                        "description": "The workflow identifier (e.g. 'poc_doc_generation')",
                    },
                    "version": {
                        "type": "string",
                        "description": "Version string (e.g. '4.1')",
                        "default": "1.0",
                    },
                },
                "required": ["workflow_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "save_workflow_db",
            "description": "Save a workflow definition to the database. Replaces existing version if it exists. IMPORTANT: Only call this AFTER the user has confirmed they want to save.",
            "parameters": {
                "type": "object",
                "properties": {
                    "workflow_id": {
                        "type": "string",
                        "description": "The workflow identifier",
                    },
                    "version": {
                        "type": "string",
                        "description": "Version string",
                    },
                    "workflow_json": {
                        "type": "string",
                        "description": "JSON string containing the workflow definition (states, transitions, description, input_schema)",
                    },
                },
                "required": ["workflow_id", "version", "workflow_json"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "delete_workflow",
            "description": "Delete a workflow definition and all its instances from the database. IMPORTANT: Only call this AFTER the user has confirmed they want to delete.",
            "parameters": {
                "type": "object",
                "properties": {
                    "workflow_id": {
                        "type": "string",
                        "description": "The workflow identifier to delete",
                    },
                    "version": {
                        "type": "string",
                        "description": "Version string",
                        "default": "1.0",
                    },
                },
                "required": ["workflow_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_workflows",
            "description": "List all workflow definitions registered in the database. Returns a summary of each workflow including ID, version, state count, and transition count.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "validate_workflow",
            "description": "Validate a workflow definition JSON string against the BizWorkflowDef schema. Checks required fields, correct state types, valid transitions, and overall structure.",
            "parameters": {
                "type": "object",
                "properties": {
                    "workflow_json": {
                        "type": "string",
                        "description": "JSON string containing the workflow definition to validate",
                    },
                },
                "required": ["workflow_json"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "load_workflow_yaml",
            "description": "Load a workflow definition from YAML file (biz_workflows/ directory) by workflow_id and version. Returns the full workflow definition. Use this to inspect existing YAML workflow files.",
            "parameters": {
                "type": "object",
                "properties": {
                    "workflow_id": {
                        "type": "string",
                        "description": "The workflow identifier (e.g. 'poc_doc_generation')",
                    },
                    "version": {
                        "type": "string",
                        "description": "Version string (e.g. '4.1')",
                        "default": "1.0",
                    },
                },
                "required": ["workflow_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get current weather and 7-day forecast for a location using Open-Meteo API (free, no API key required). Returns temperature, precipitation, humidity, wind speed, and weather condition codes. Use latitude/longitude coordinates.",
            "parameters": {
                "type": "object",
                "properties": {
                    "latitude": {
                        "type": "number",
                        "description": "Latitude coordinate (e.g. 37.5665 for Seoul)",
                    },
                    "longitude": {
                        "type": "number",
                        "description": "Longitude coordinate (e.g. 126.9780 for Seoul)",
                    },
                },
                "required": ["latitude", "longitude"],
            },
        },
    },
]

# ── Tool execution ──

TOOL_REGISTRY: Dict[str, Any] = {}


def register_tool(fn):
    TOOL_REGISTRY[fn.__name__] = fn
    return fn


@register_tool
def web_search(query: str, num_results: int = 5) -> str:
    """Search the web using Tavily AI Search API. Returns structured results with titles, URLs, and content."""
    log.info("web_search: query=%s, num_results=%d", query, num_results)
    try:
        from tavily import TavilyClient
        api_key = os.getenv("TAVILY_API_KEY", "")
        if not api_key:
            return "Search error: TAVILY_API_KEY not configured"
        client = TavilyClient(api_key=api_key)
        response = client.search(
            query=query,
            search_depth="basic",
            max_results=num_results,
        )
        results = response.get("results", [])
        lines = []
        for r in results:
            title = r.get("title", "")
            url = r.get("url", "")
            content = r.get("content", "")
            snippet = content[:500] if content else ""
            lines.append(f"- **{title}**: {url}\n  {snippet}")
        output = "\n".join(lines) if lines else "No results found."
        log.info("web_search: %d results from Tavily", len(results))
        return output
    except Exception as e:
        log.warning("web_search failed: %s", e)
        return f"Search error: {e}"


@register_tool
def web_fetch(url: str) -> str:
    """Fetch page content from a URL and extract text."""
    log.info("web_fetch: url=%s", url)
    try:
        resp = httpx.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15.0)
        resp.raise_for_status()
        import re
        html = resp.text
        # Basic text extraction
        text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        # Limit to first 5000 chars
        text = text[:5000]
        return text
    except Exception as e:
        log.warning("web_fetch failed: %s", e)
        return f"Fetch error: {e}"


@register_tool
def calculator(expression: str) -> str:
    """Evaluate a math expression safely."""
    log.info("calculator: %s", expression)
    allowed = set("0123456789+-*/.()% ")
    if not all(c in allowed for c in expression):
        return "Error: invalid characters in expression"
    try:
        result = eval(expression, {"__builtins__": {}}, {})
        return str(result)
    except Exception as e:
        return f"Calculation error: {e}"


@register_tool
def read_file(path: str) -> str:
    """Read a file from the project directory."""
    log.info("read_file: path=%s", path)
    try:
        safe = _safe_path(path)
        with open(safe, "r", encoding="utf-8") as f:
            content = f.read()
        log.info("read_file: %d bytes", len(content))
        return content
    except Exception as e:
        return f"Error reading file '{path}': {e}"


@register_tool
def read_files(paths: str) -> str:
    """Read MULTIPLE files at once. Provide paths as a comma-separated string. Max 6 files per call. Max 1200 lines total."""
    log.info("read_files: paths=%s", paths)
    try:
        path_list = [p.strip() for p in paths.split(",") if p.strip()]
        results = []
        max_total_lines = 1200
        total_lines = 0
        for p in path_list[:6]:
            safe = _safe_path(p)
            with open(safe, "r", encoding="utf-8") as f:
                content = f.read()
            lines = content.count("\n") + 1
            total_lines += lines
            results.append(f"=== {p} ({lines} lines) ===\n{content}")
            if total_lines > max_total_lines:
                remaining = len(path_list) - path_list.index(p) - 1
                results.append(f"... (truncated after {max_total_lines} lines, {remaining} file(s) skipped)")
                break
        log.info("read_files: %d files, %d bytes", len(path_list[:6]), sum(len(r) for r in results))
        return "\n\n".join(results)
    except Exception as e:
        return f"Error reading files '{paths}': {e}"


@register_tool
def glob(pattern: str) -> str:
    """Find files matching a glob pattern."""
    log.info("glob: pattern=%s", pattern)
    try:
        import glob as glob_mod
        matches = glob_mod.glob(os.path.join(BASE_PATH, pattern), recursive=True)
        rel_matches = [os.path.relpath(m, BASE_PATH).replace("\\", "/") for m in sorted(matches)]
        result = "\n".join(rel_matches) if rel_matches else "No files found."
        log.info("glob: %d matches", len(rel_matches))
        return result
    except Exception as e:
        return f"Glob error: {e}"


@register_tool
def grep(pattern: str, path: str = ".", max_results: int = 50) -> str:
    """Search file contents with a regex pattern."""
    log.info("grep: pattern=%s, path=%s", pattern, path)
    try:
        safe_path = _safe_path(path)
        compiled = re.compile(pattern)
        matches = []
        search_root = safe_path if os.path.isdir(safe_path) else os.path.dirname(safe_path)
        for root, _dirs, files in os.walk(search_root):
            for fname in files:
                fpath = os.path.join(root, fname)
                try:
                    with open(fpath, "r", encoding="utf-8") as fh:
                        for i, line in enumerate(fh, 1):
                            if compiled.search(line):
                                rel = os.path.relpath(fpath, BASE_PATH).replace("\\", "/")
                                matches.append(f"{rel}:{i}: {line.rstrip()[:200]}")
                                if len(matches) >= max_results:
                                    break
                except (UnicodeDecodeError, PermissionError):
                    continue
                if len(matches) >= max_results:
                    break
        result = "\n".join(matches) if matches else "No matches found."
        log.info("grep: %d matches", len(matches))
        return result
    except Exception as e:
        return f"Grep error: {e}"


@register_tool
def list_directory(path: str = ".") -> str:
    """List directory contents."""
    log.info("list_directory: path=%s", path)
    try:
        safe = _safe_path(path)
        entries = os.listdir(safe)
        result_lines = []
        for e in sorted(entries):
            full = os.path.join(safe, e)
            suffix = "/" if os.path.isdir(full) else ""
            result_lines.append(f"{e}{suffix}")
        result = "\n".join(result_lines) if result_lines else "(empty)"
        log.info("list_directory: %d entries", len(entries))
        return result
    except Exception as e:
        return f"List directory error: {e}"


# ── Open-Meteo Weather Tool ──

@register_tool
def get_weather(latitude: float, longitude: float) -> str:
    """Get current weather and 7-day forecast for a location using Open-Meteo API (free, no API key required). Returns temperature, precipitation, wind speed, and weather conditions."""
    log.info("get_weather: lat=%s, lon=%s", latitude, longitude)
    try:
        import httpx as _httpx
        import json as _json
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m",
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,weather_code,wind_speed_10m_max",
            "timezone": "auto",
            "forecast_days": 7,
        }
        resp = _httpx.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=10.0)
        resp.raise_for_status()
        data = resp.json()
        return _json.dumps(data, indent=2, ensure_ascii=False)
    except Exception as e:
        log.warning("get_weather failed: %s", e)
        return f"Weather fetch error: {e}"


# ── Workflow CRUD tools ──

_WF_DEFS_DIR: str | None = None


def _get_biz_workflows_dir() -> str:
    """biz_workflows/ 디렉토리 경로를 결정 (__file__ 기준)."""
    global _WF_DEFS_DIR
    if _WF_DEFS_DIR is None:
        _WF_DEFS_DIR = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "biz_workflows",
        )
    return _WF_DEFS_DIR


@register_tool
def load_workflow_db(workflow_id: str, version: str = "1.0") -> str:
    """Load a workflow definition from the database."""
    log.info("load_workflow_db: %s v%s", workflow_id, version)
    try:
        from shared.workflow_repository import BizWorkflowRepository
        repo = BizWorkflowRepository()
        wf = repo.load(workflow_id, version)
        return json.dumps(wf.model_dump(by_alias=True), indent=2, ensure_ascii=False)
    except FileNotFoundError:
        return json.dumps({"error": f"Workflow not found: {workflow_id} v{version}"})
    except Exception as e:
        log.warning("load_workflow_db failed: %s", e)
        return json.dumps({"error": f"Load failed: {e}"})


@register_tool
def save_workflow_db(workflow_id: str, version: str, workflow_json: str) -> str:
    """Save a workflow definition to the database. Ask user confirmation first."""
    log.info("save_workflow_db: %s v%s", workflow_id, version)
    try:
        from shared.models import BizWorkflowDef
        from shared.workflow_repository import BizWorkflowRepository
        import json as _json
        wf_data = _json.loads(workflow_json)
        full_data = {"workflow_id": workflow_id, "version": version, **wf_data}
        wf = BizWorkflowDef.model_validate(full_data)
        repo = BizWorkflowRepository()
        def_id = repo.save_workflow(wf)
        return json.dumps({"id": def_id, "status": "saved", "workflow_id": workflow_id, "version": version})
    except Exception as e:
        log.warning("save_workflow_db failed: %s", e)
        return json.dumps({"error": f"Save failed: {e}"})


@register_tool
def delete_workflow(workflow_id: str, version: str = "1.0") -> str:
    """Delete a workflow definition. Ask user confirmation first."""
    log.info("delete_workflow: %s v%s", workflow_id, version)
    try:
        from shared.workflow_repository import BizWorkflowRepository
        repo = BizWorkflowRepository()
        repo.delete_workflow(workflow_id, version)
        return json.dumps({"status": "deleted", "workflow_id": workflow_id, "version": version})
    except FileNotFoundError:
        return json.dumps({"error": f"Workflow not found: {workflow_id} v{version}"})
    except Exception as e:
        log.warning("delete_workflow failed: %s", e)
        return json.dumps({"error": f"Delete failed: {e}"})


@register_tool
def list_workflows() -> str:
    """List all workflow definitions in the database."""
    log.info("list_workflows")
    try:
        from shared.workflow_repository import BizWorkflowRepository
        repo = BizWorkflowRepository()
        workflows = repo.list_workflows()
        return json.dumps(workflows, indent=2, ensure_ascii=False)
    except Exception as e:
        log.warning("list_workflows failed: %s", e)
        return json.dumps({"error": f"List failed: {e}"})


@register_tool
def validate_workflow(workflow_json: str) -> str:
    """Validate a workflow definition JSON against the schema."""
    log.info("validate_workflow: %d chars", len(workflow_json))
    try:
        from shared.models import BizWorkflowDef
        import json as _json
        wf_data = _json.loads(workflow_json)
        # Inject dummy workflow_id/version for schema validation if missing
        if "workflow_id" not in wf_data:
            wf_data["workflow_id"] = "_validation_only"
        if "version" not in wf_data:
            wf_data["version"] = "1.0"
        BizWorkflowDef.model_validate(wf_data)
        return json.dumps({"valid": True, "errors": []})
    except Exception as e:
        err_msg = str(e)
        log.warning("validate_workflow failed: %s", err_msg[:200])
        return json.dumps({"valid": False, "errors": [err_msg]})


@register_tool
def load_workflow_yaml(workflow_id: str, version: str = "1.0") -> str:
    """Load a workflow definition from YAML file."""
    log.info("load_workflow_yaml: %s v%s", workflow_id, version)
    try:
        from shared.workflow_loader import BizWorkflowLoader
        base_dir = _get_biz_workflows_dir()
        loader = BizWorkflowLoader(base_dir=base_dir)
        wf = loader.load(workflow_id, version)
        return json.dumps(wf.model_dump(by_alias=True), indent=2, ensure_ascii=False)
    except FileNotFoundError:
        return json.dumps({"error": f"Workflow YAML not found: {workflow_id} v{version}"})
    except Exception as e:
        log.warning("load_workflow_yaml failed: %s", e)
        return json.dumps({"error": f"Load YAML failed: {e}"})


def get_enabled_tools(tool_names: List[str]) -> List[Dict[str, Any]]:
    """주어진 이름 목록에 해당하는 tool definitions만 반환."""
    name_set = set(tool_names)
    return [t for t in TOOL_DEFINITIONS if t["function"]["name"] in name_set]


def execute_tool(name: str, args: Dict[str, Any]) -> str:
    """도구 이름과 인자로 실제 실행."""
    fn = TOOL_REGISTRY.get(name)
    if fn is None:
        return f"Error: unknown tool '{name}'"
    try:
        return fn(**args)
    except Exception as e:
        return f"Tool error ({name}): {e}"
