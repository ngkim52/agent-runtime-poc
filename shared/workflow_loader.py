# shared/workflow_loader.py
from __future__ import annotations
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional

from .models import BizWorkflowDef, TransitionDef


class BizWorkflowLoader:
    """POC: 파일 기반. 추후 DB 기반(BizWorkflowRepository)으로 교체 예정."""

    def __init__(self, base_dir: str | Path = "biz_workflows"):
        self.base_dir = Path(base_dir)
        self._cache: Dict[str, BizWorkflowDef] = {}

    def load(self, workflow_id: str, version: str = "1.0") -> BizWorkflowDef:
        cache_key = f"{workflow_id}:{version}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # 파일명 규칙: {workflow_id}_v{major}.yaml
        major = version.split(".")[0]
        path = self.base_dir / f"{workflow_id}_v{major}.yaml"
        if not path.exists():
            raise FileNotFoundError(f"Workflow file not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        wf = BizWorkflowDef.model_validate(data)
        self._cache[cache_key] = wf
        return wf


class _DotDict(dict):
    """result.status 처럼 점 접근 가능하게.
    중첩 dict의 키도 재귀 탐색: result.verdict → data.data.verdict 등
    """
    def __getattr__(self, item):
        # 1) 직접 키가 있으면 즉시 반환
        if item in self:
            v = self[item]
            return _DotDict(v) if isinstance(v, dict) else v
        # 2) dict 값 중 하나라도 item을 키로 가지면 재귀 반환
        for v in self.values():
            if isinstance(v, dict) and item in v:
                found = v[item]
                return _DotDict(found) if isinstance(found, dict) else found
        # 3) 모든 dict 값을 재귀 탐색 (깊은 중첩 처리)
        for v in self.values():
            if isinstance(v, dict):
                sub = _DotDict(v)
                try:
                    return getattr(sub, item)
                except AttributeError:
                    continue
        raise AttributeError(item)


class TransitionEvaluator:
    """전이 조건(when 표현식) 평가기.
    POC: 단순 eval 사용. 추후 안전한 expression engine(asteval/simpleeval)으로 교체.
    """

    SAFE_BUILTINS = {"True": True, "False": False, "None": None}

    @classmethod
    def evaluate(cls, expr: Optional[str], result: Dict[str, Any]) -> bool:
        if expr is None or expr.strip() == "":
            return True
        ns = {"result": _DotDict(result), **cls.SAFE_BUILTINS}
        try:
            return bool(eval(expr, {"__builtins__": {}}, ns))
        except Exception as e:
            raise ValueError(f"Transition expr eval failed: {expr!r} → {e}")

    @classmethod
    def pick_next(
        cls,
        transitions: List[TransitionDef],
        result: Dict[str, Any],
    ) -> Optional[TransitionDef]:
        for t in transitions:
            if cls.evaluate(t.when, result):
                return t
        return None