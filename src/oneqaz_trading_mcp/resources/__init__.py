# -*- coding: utf-8 -*-
"""
Market MCP Resources
====================
?뺤쟻/二쇨린 媛깆떊 ?ㅻ깄???곗씠???쒓났

Resource??"?꾩옱 ?곹깭"瑜?諛섑솚?섎ŉ, LLM??而⑦뀓?ㅽ듃濡??ъ슜?⑸땲??
"""

from .global_regime import register_global_regime_resources
from .market_status import register_market_status_resources
from .market_structure import register_market_structure_resources
from .indicators import register_indicator_resources
from .signal_system import register_signal_resources
from .external_context import register_external_context_resources
from .unified_context import register_unified_context_resources
from .derived_signals import register_derived_signals_resources

__all__ = [
    "register_global_regime_resources",
    "register_market_status_resources",
    "register_market_structure_resources",
    "register_indicator_resources",
    "register_signal_resources",
    "register_external_context_resources",
    "register_unified_context_resources",
    "register_derived_signals_resources",
]

