# orchestrator/payload_builder.py
"""
각 state(=task)별로 워커에게 넘길 payload를 동적으로 구성.
state_def.inputs에 선언된 이전 step 결과를 평탄화(merge)하여 전달.
"""
from __future__ import annotations
from typing import Any, Dict, List

from .instance import ClaimInstance


class PayloadBuilder:
    """
    state_def.inputs에 지정된 이전 step들의 결과를 조회하여 payload 생성.

    - payload["_input"]: 최초 입력 데이터 (항상 포함)
    - payload["{input_state_id}"]: input 결과 전체 (handler가 특정 step 직접 참조)
    - 그 외 input result의 모든 키는 payload 최상위에 병합 (flat 접근)
    - inputs가 빈 리스트이면 _input + initial_input 필드만 전달
    """

    @staticmethod
    def build(
        state_or_branch: Any,
        instance: ClaimInstance,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}

        # 1) initial_input은 항상 포함
        payload["_input"] = instance.initial_input
        # flat key 접근 (예: claim_no, policy_no 등을 직접 참조)
        for k, v in instance.initial_input.items():
            if k not in payload:
                payload[k] = v

        # 2) inputs에 명시된 이전 step 결과 조회
        input_ids: List[str] = getattr(state_or_branch, "inputs", []) or []
        for src in input_ids:
            result = instance.latest_result(src)
            if result:
                payload[src] = result
                # result 내부 키도 최상위에 병합 (handler 호환성)
                for k, v in result.items():
                    if k not in payload:
                        payload[k] = v

        return payload
