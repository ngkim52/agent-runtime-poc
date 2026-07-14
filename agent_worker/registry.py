# agent_worker/registry.py
from __future__ import annotations
from typing import Any, Callable, Dict, List, Type
from .handlers.base import TaskHandler


class HandlerRegistry:
    """task_type → TaskHandler 클래스/함수 매핑"""
    _registry: Dict[str, Any] = {}

    @classmethod
    def register(cls, task_type: str) -> Callable[[Type[TaskHandler]], Type[TaskHandler]]:
        def deco(handler_cls: Type[TaskHandler]) -> Type[TaskHandler]:
            if task_type in cls._registry:
                raise ValueError(f"Duplicate handler for task_type={task_type!r}")
            cls._registry[task_type] = handler_cls
            return handler_cls
        return deco

    @classmethod
    def register_func(cls, task_type: str, handler_fn: Callable) -> None:
        """함수형 핸들러 등록 (클래스 기반 decorator 대신)."""
        if task_type in cls._registry:
            raise ValueError(f"Duplicate handler for task_type={task_type!r}")
        cls._registry[task_type] = handler_fn

    @classmethod
    def get(cls, task_type: str):
        if task_type not in cls._registry:
            raise KeyError(f"No handler registered for task_type={task_type!r}")
        return cls._registry[task_type]

    @classmethod
    def list_registered(cls) -> List[str]:
        return sorted(cls._registry.keys())


# 짧은 별칭
register = HandlerRegistry.register