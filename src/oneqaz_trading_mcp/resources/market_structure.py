# -*- coding: utf-8 -*-
"""
Market Structure Resource
=========================
ETF/basket/dominance structure analysis.

Data sources:
- market_structure_summary.json: Per-market structure summary
- group_analysis.db: Per-group detailed analysis
"""

from __future__ import annotations

import json
import sqlite3
import logging
from typing import Any, Dict, List

from oneqaz_trading_mcp.config import (
    ANALYSIS_DB_PATHS,
    get_structure_summary_path,
    get_analysis_db_path,
)
from oneqaz_trading_mcp.response import to_resource_text

logger = logging.getLogger("MarketMCP")


def _load_structure_summary(market_id: str) -> Dict[str, Any]:
    path = get_structure_summary_path(market_id)
    if not path or not path.exists():
        return {"error": f"Structure summary not found for {market_id}"}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        data["_llm_summary"] = _generate_structure_summary_text(data)
        return data
    except Exception as e:
        logger.error("Failed to load structure summary for %s: %s", market_id, e)
        return {"error": str(e)}


def _generate_structure_summary_text(data: Dict[str, Any]) -> str:
    try:
        market = data.get("market", "unknown")
        overall = data.get("overall", {})
        groups = data.get("groups", {})
        lines = [
            f"[{market} Market Structure] Updated: {data.get('updated_at', 'N/A')}",
            f"- Overall: {overall.get('regime', 'N/A')} (score: {overall.get('score', 0):.2f})",
        ]
        for gid, ginfo in groups.items():
            lines.append(f"- {gid}: {ginfo.get('regime_dominant', 'N/A')}, avg={ginfo.get('regime_avg', 0):.2f}, confidence={ginfo.get('confidence', 0):.2f}")
            timing = ginfo.get("timing_signal", "")
            if timing:
                lines.append(f"  timing: {timing}")
        for key, label in [("concentration", "Concentration"), ("index_spread", "Index Spread"), ("dominance", "Dominance"), ("breadth", "Breadth"), ("alt_strength", "Alt Strength")]:
            extra = data.get(key)
            if isinstance(extra, dict):
                lines.append(f"- {label}: {extra.get('state', 'N/A')}")
        return "\n".join(lines)
    except Exception as e:
        return f"Summary generation failed: {e}"


def _load_group_analysis(market_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    key = market_id.replace("_stock", "_structure").replace("crypto", "coin_structure")
    db_path = get_analysis_db_path(key)
    if not db_path or not db_path.exists():
        return []
    try:
        with sqlite3.connect(str(db_path), timeout=20) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute(
                "SELECT symbol, interval, timestamp, regime_stage, regime_label, sentiment, sentiment_label, integrated_direction FROM analysis ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            ).fetchall()]
    except Exception as e:
        logger.error("Group analysis query failed for %s: %s", market_id, e)
        return []


def read_market_structure(market_id: str) -> str:
    return to_resource_text(_load_structure_summary(market_id))


def read_market_structure_group(market_id: str, group_id: str) -> str:
    data = _load_structure_summary(market_id)
    group_data = data.get("groups", {}).get(group_id)
    if not group_data:
        return to_resource_text({"error": f"Group '{group_id}' not found in {market_id}"})
    return to_resource_text(group_data)


def read_all_market_structures() -> str:
    result: Dict[str, Any] = {}
    for key in ("us_stock", "kr_stock", "crypto"):
        data = _load_structure_summary(key)
        if "error" not in data:
            result[key] = {
                "overall": data.get("overall"),
                "groups": {gid: {"regime_dominant": g.get("regime_dominant"), "regime_avg": g.get("regime_avg"), "confidence": g.get("confidence"), "timing_signal": g.get("timing_signal")} for gid, g in data.get("groups", {}).items()},
            }
    return to_resource_text(result)


def register_market_structure_resources(mcp, cache):
    """Register market structure MCP resources."""

    @mcp.resource("market://structure/all")
    def get_all_structures() -> str:
        """All markets ETF/basket structure summary (US/KR/Crypto)."""
        cache_key = "all_market_structures"
        cached = cache.get(cache_key, ttl=60)
        if cached:
            return cached
        result = read_all_market_structures()
        cache.set(cache_key, result)
        return result

    @mcp.resource("market://{market_id}/structure")
    def get_market_structure(market_id: str) -> str:
        """Per-market structure analysis. Returns: ETF/basket/dominance structure."""
        cache_key = f"structure_{market_id}"
        cached = cache.get(cache_key, ttl=60)
        if cached:
            return cached
        result = read_market_structure(market_id)
        cache.set(cache_key, result)
        return result

    @mcp.resource("market://{market_id}/structure/group/{group_id}")
    def get_structure_group(market_id: str, group_id: str) -> str:
        """Detailed analysis for a specific structure group (sector, thematic, dominance, etc.)."""
        return read_market_structure_group(market_id, group_id)

    logger.info("  Market Structure resources registered")
