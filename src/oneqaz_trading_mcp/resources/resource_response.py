# -*- coding: utf-8 -*-
"""Response utilities, error builders, and explanation contracts.

Public-package equivalent of internal `mcps/resources/resource_response.py`.
Stripped of `api.insight.schema` dependency (monorepo-only) — uses plain dicts
for explanation payloads instead of pydantic models.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional


def _json_default(value: Any) -> str | float:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    return str(value)


def to_resource_text(data: Any) -> str:
    """Serialize data to JSON string for FastMCP resource responses."""
    return json.dumps(data, ensure_ascii=False, default=_json_default)


# ── AX: Actionable Error Response ──


class MCPErrorCode(str, Enum):
    """Standardized MCP error codes."""
    SIGNAL_STALE = "signal_stale"
    REGIME_UNAVAILABLE = "regime_unavailable"
    SYMBOL_NOT_FOUND = "symbol_not_found"
    DATA_COLLECTION_LAG = "data_collection_lag"
    DB_NOT_FOUND = "db_not_found"
    TABLE_NOT_FOUND = "table_not_found"
    TIMEOUT = "timeout"
    MARKET_NOT_FOUND = "market_not_found"
    NO_DATA = "no_data"
    INTERNAL_ERROR = "internal_error"


class MCPErrorAction(str, Enum):
    """Recommended follow-up actions for AI consumers."""
    RETRY = "retry_after_seconds"
    FALLBACK = "use_fallback"
    CHECK = "check_availability"


def mcp_error(
    code: MCPErrorCode,
    reason: str,
    *,
    action: MCPErrorAction = MCPErrorAction.CHECK,
    action_value: str = "",
    fallback_tool: Optional[str] = None,
    fallback_note: Optional[str] = None,
    **extra_context,
) -> dict:
    """Actionable error response.

    Replaces the plain `{"error": ...}` pattern. Returns a dict with:
    - error_code: classification
    - reason: human-readable detail
    - action: recommended follow-up (retry / fallback / check)
    - fallback_tool: alternate tool/resource name (optional)
    """
    d: dict[str, Any] = {
        "error": True,
        "error_code": code.value,
        "reason": reason,
        "action": action.value,
        "action_value": action_value,
    }
    if fallback_tool:
        d["fallback_tool"] = fallback_tool
    if fallback_note:
        d["fallback_note"] = fallback_note
    if extra_context:
        d.update(extra_context)
    return d


# ── AX: AI Summary Wrapper ──


# Default TTL by resource type (seconds). Used when wrap_with_ai_summary is
# called without an explicit TTL override.
_AI_SUMMARY_TTL_DEFAULTS: dict[str, int] = {
    "market_status": 60,
    "global_regime": 120,
    "market_structure": 180,
    "signal_system": 60,
    "derived_signals": 90,
    "external_context": 120,
    "unified_context": 90,
    "indicators": 60,
}


def wrap_with_ai_summary(
    data: dict,
    resource_type: str,
    summary_fn: Callable[[dict], str],
) -> dict:
    """Wrap response with `ai_summary + full_data` structure.

    - full_data: original response (backwards compatible)
    - ai_summary: 1-line summary (LLM can grasp essence without parsing full_data)
    - ai_summary_ttl_seconds: summary validity window
    """
    ttl = _AI_SUMMARY_TTL_DEFAULTS.get(resource_type, 120)
    try:
        ai_summary = summary_fn(data)
    except Exception:
        ai_summary = ""
    return {
        "ai_summary": ai_summary,
        "ai_summary_generated_at": datetime.utcnow().isoformat() + "Z",
        "ai_summary_ttl_seconds": ttl,
        "full_data": data,
        "_llm_summary": data.get("_llm_summary", ai_summary),
    }


# ── Explanation Contract Builder ──


def build_resource_explanation(
    *,
    market: str,
    entity_type: str,
    explanation_type: str,
    as_of_time: str | None = None,
    symbol: str | None = None,
    headline: str,
    why_text: str,
    bullet_points: list[str] | None = None,
    market_context_refs: dict[str, Any] | None = None,
    confidence: float = 0.5,
    note: str | None = None,
) -> dict[str, Any]:
    """Build a structured explanation payload for a resource."""
    if hasattr(as_of_time, "isoformat"):
        as_of_time = as_of_time.isoformat()
    as_of_time = as_of_time or datetime.utcnow().isoformat()
    bullet_points = bullet_points or []
    market_context_refs = market_context_refs or {}

    return {
        "schema_version": "1.0",
        "entity": {
            "market": market,
            "symbol": symbol,
            "entity_type": entity_type,
        },
        "as_of_time": as_of_time,
        "explanation_type": explanation_type,
        "assessment": {
            "driver": {"primary": "unknown", "secondary": None, "confidence": confidence},
            "time_phase": "event",
            "relation_status": "watching",
            "movement_classification": "unconfirmed",
            "news_timing": "no_news_detected",
            "lead_lag": None,
            "explanation_status": "candidate",
        },
        "evidence": {
            "source_event_ids": [],
            "related_relation_ids": [],
            "signal_keys": [],
            "market_context_refs": market_context_refs,
            "state_refs": {},
            "memory_refs": {},
            "confidence": confidence,
        },
        "summary": {
            "headline": headline,
            "why_text": why_text,
            "short_reason": bullet_points[0] if bullet_points else None,
            "bullet_points": bullet_points,
            "market_details": {},
            "uncertainty_flags": [],
            "risk_assessment": {},
        },
        "provenance": {"schema_version": "1.0", "note": note},
    }


def with_explanation_contract(
    data: dict[str, Any],
    *,
    resource_type: str,
    explanation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Attach an explanation contract to resource data."""
    enriched = dict(data)
    if explanation:
        enriched["_contract"] = {
            "schema_version": "1.0",
            "resource_type": resource_type,
            "explanation": explanation,
        }
        if not enriched.get("_llm_summary"):
            summary = explanation.get("summary", {})
            enriched["_llm_summary"] = summary.get("why_text", "")
    else:
        enriched["_contract"] = {
            "schema_version": "1.0",
            "resource_type": resource_type,
        }
    return enriched


__all__ = [
    "to_resource_text",
    "MCPErrorCode",
    "MCPErrorAction",
    "mcp_error",
    "wrap_with_ai_summary",
    "build_resource_explanation",
    "with_explanation_contract",
]
