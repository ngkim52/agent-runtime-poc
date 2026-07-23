# shared/step_utils.py
"""
워크플로우 스텝 간 데이터 처리 유틸리티.
"""
from __future__ import annotations
from typing import Any, Dict


def resolve_jsonpath(data: Any, path: str) -> Any:
    """
    '$.' 로 시작하는 단순 JSON path 해석.
    예:
      $.STEP_3.items        → data["STEP_3"]["items"]
      $.STEP_3.user.name    → data["STEP_3"]["user"]["name"]
      $.STEP_3.items.0      → data["STEP_3"]["items"][0]
      $                     → data (전체 반환)
    """
    if not path or path == "$":
        return data
    parts = path.lstrip("$.").split(".")
    current = data
    for part in parts:
        if not part:
            continue
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, (list, tuple)):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return current


def evaluate_vars(
    vars_def: Dict[str, str] | None,
    history: list,
    initial_input: Dict[str, Any],
) -> Dict[str, Any]:
    """
    vars: { var_name: "$.STATE_ID.field" } 정의를 평가하여 값 dict 반환.

    표현식 형식:
      $.STATE_ID.field       → history에서 STATE_ID의 result에서 field 추출
      $.STATE_ID             → history에서 STATE_ID의 전체 result 반환
      $._input.field         → initial_input에서 field 추출
      그 외 문자열            → 리터럴 값으로 처리
    """
    if not vars_def:
        return {}

    # history를 state_id → result dict로 변환
    # parallel state의 브랜치 결과도 branch_id → result로 평탄화
    history_map: Dict[str, Any] = {}
    for step in history:
        if step.status == "OK":
            history_map[step.state_id] = step.result
            # parallel step의 브랜치 결과도 최상위에서 접근 가능하게
            if getattr(step, 'task_type', None) == 'parallel':
                for branch_id, branch_data in step.result.items():
                    if isinstance(branch_data, dict) and branch_data.get("status") == "OK":
                        history_map[branch_id] = branch_data.get("result", {})

    # 초기 입력도 history_map에 포함
    history_map["_input"] = initial_input

    result: Dict[str, Any] = {}
    for var_name, expression in vars_def.items():
        if not isinstance(expression, str):
            result[var_name] = expression
            continue

        if expression.startswith("$"):
            # JSON path 표현식
            value = resolve_jsonpath(history_map, expression)
            result[var_name] = value
        else:
            # 리터럴 값
            result[var_name] = expression

    return result
