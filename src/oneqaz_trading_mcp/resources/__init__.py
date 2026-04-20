# -*- coding: utf-8 -*-
"""OneQAZ Trading MCP — Resources.

Static / periodic-refresh state read through `market://...` URIs.
"""

from .derived_signals import register_derived_signals_resources
from .external_context import register_external_context_resources
from .global_regime import register_global_regime_resources
from .indicators import register_indicator_resources
from .market_status import register_market_status_resources
from .market_structure import register_market_structure_resources
from .signal_system import register_signal_resources
from .unified_context import register_unified_context_resources

__all__ = [
    "register_derived_signals_resources",
    "register_external_context_resources",
    "register_global_regime_resources",
    "register_indicator_resources",
    "register_market_status_resources",
    "register_market_structure_resources",
    "register_signal_resources",
    "register_unified_context_resources",
]
