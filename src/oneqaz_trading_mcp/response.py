# -*- coding: utf-8 -*-
"""Response utilities for MCP resource serialization."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


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
