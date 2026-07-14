# agent_worker/handlers/__init__.py
"""
handler가 자동으로 Registry에 등록
"""
from . import image_download                # noqa: F401
from . import document_classification       # noqa: F401
from . import image_data_extraction         # noqa: F401
from . import identity_verification         # noqa: F401
from . import accident_info_extraction      # noqa: F401
from . import prior_claims_matching         # noqa: F401
from . import route_to_human                # noqa: F401

# 범용 AI Worker (함수형 핸들러 등록)
from ..registry import HandlerRegistry
from ..universal.handler import handle_task
HandlerRegistry.register_func("ai_task", handle_task)