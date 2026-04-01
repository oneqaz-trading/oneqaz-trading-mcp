# -*- coding: utf-8 -*-
"""
Global Regime Resource
======================
Macro regime data for commodities, bonds, forex, VIX, credit, etc.

Data sources:
- global_regime_summary.json: Overall summary
- *_analysis.db: Per-category detailed analysis
"""

from __future__ import annotations

import json
import sqlite3
import logging
from typing import Any, Dict, List

from oneqaz_trading_mcp.config import (
    GLOBAL_REGIME_SUMMARY_JSON,
    ANALYSIS_DB_PATHS,
    CACHE_TTL_GLOBAL_REGIME,
    get_analysis_db_path,
)
from oneqaz_trading_mcp.response import to_resource_text

logger = logging.getLogger("MarketMCP")


def _load_global_regime_summary() -> Dict[str, Any]:
    """Load global_regime_summary.json."""
    try:
        if not GLOBAL_REGIME_SUMMARY_JSON.exists():
            return {"error": "Global regime summary not found", "path": str(GLOBAL_REGIME_SUMMARY_JSON)}
        with open(GLOBAL_REGIME_SUMMARY_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        data["_llm_summary"] = _generate_regime_summary_text(data)
        return data
    except Exception as e:
        logger.error(f"Failed to load global regime summary: {e}")
        return {"error": str(e)}


def _generate_regime_summary_text(data: Dict[str, Any]) -> str:
    """Generate LLM-friendly summary text."""
    try:
        overall = data.get("overall", {})
        categories = data.get("categories", {})
        lines = [
            f"[Global Regime Summary] Updated: {data.get('updated_at', 'N/A')}",
            f"- Overall regime: {overall.get('regime', 'N/A')} (score: {overall.get('score', 0):.2f})",
        ]
        for cat, info in categories.items():
            regime = info.get("regime_dominant", "N/A")
            sentiment = info.get("sentiment_avg", 0)
            symbols = info.get("symbols", 0)
            lines.append(f"- {cat}: {regime}, sentiment={sentiment:.2f}, symbols={symbols}")

        mtf = data.get("mtf_summary", {})
        if mtf:
            aligned = mtf.get("aligned_symbols", 0)
            misaligned = mtf.get("misaligned_symbols", 0)
            lines.append(f"- MTF aligned/misaligned: {aligned}/{misaligned}")
        return "\n".join(lines)
    except Exception as e:
        return f"Summary generation failed: {e}"


def _load_category_analysis(category: str, limit: int = 20) -> Dict[str, Any]:
    """Load per-category analysis from its SQLite DB."""
    db_path = get_analysis_db_path(category)
    if not db_path or not db_path.exists():
        return {
            "error": f"Analysis DB not found for category: {category}",
            "available_categories": list(ANALYSIS_DB_PATHS.keys()),
        }
    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT symbol, interval, timestamp,
                       regime_stage, regime_label,
                       sentiment, sentiment_label,
                       integrated_direction, volatility_level, risk_level,
                       regime_confidence, regime_transition_prob
                FROM analysis
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,))
            rows = [dict(row) for row in cursor.fetchall()]

            by_symbol: Dict[str, List] = {}
            for row in rows:
                by_symbol.setdefault(row["symbol"], []).append(row)

            return {
                "category": category,
                "db_path": str(db_path),
                "total_rows": len(rows),
                "by_symbol": by_symbol,
                "_llm_summary": _generate_category_summary_text(category, by_symbol),
            }
    except Exception as e:
        logger.error(f"Failed to load {category} analysis: {e}")
        return {"error": str(e), "category": category}


def _generate_category_summary_text(category: str, by_symbol: Dict[str, List]) -> str:
    """Generate per-category LLM summary."""
    try:
        lines = [f"[{category.upper()} Analysis Summary]"]
        for symbol, data_list in list(by_symbol.items())[:5]:
            if not data_list:
                continue
            latest = data_list[0]
            lines.append(
                f"- {symbol}: regime={latest.get('regime_label', 'N/A')}, "
                f"sentiment={latest.get('sentiment_label', 'N/A')}, "
                f"direction={latest.get('integrated_direction', 'N/A')}, "
                f"risk={latest.get('risk_level', 'N/A')}"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"Summary generation failed: {e}"


def register_global_regime_resources(mcp, cache):
    """Register global regime MCP resources."""

    @mcp.resource("market://global/summary")
    def get_global_regime_summary() -> str:
        """
        Global regime summary (overall macro environment).
        Aggregated regime data across commodities, bonds, forex, VIX, credit, etc.
        Returns: Overall regime, per-category details, MTF alignment info.
        """
        cache_key = "global_regime_summary"
        cached = cache.get(cache_key, ttl=CACHE_TTL_GLOBAL_REGIME)
        if cached:
            return to_resource_text(cached)
        data = _load_global_regime_summary()
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://global/category/{category}")
    def get_category_analysis(category: str) -> str:
        """
        Per-category detailed analysis (bonds, commodities, forex, vix, credit, liquidity, inflation).
        Returns: Per-symbol regime, sentiment, direction, and risk data.
        """
        cache_key = f"category_analysis_{category}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_GLOBAL_REGIME)
        if cached:
            return to_resource_text(cached)
        data = _load_category_analysis(category)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://global/categories")
    def list_categories() -> str:
        """
        List available global analysis categories and their DB status.
        Returns: Category list with DB existence status.
        """
        return to_resource_text({
            "categories": [
                {"id": cat, "db_path": str(db_path), "exists": db_path.exists()}
                for cat, db_path in ANALYSIS_DB_PATHS.items()
            ]
        })

    logger.info("  Global Regime resources registered")
