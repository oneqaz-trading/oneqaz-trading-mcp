# -*- coding: utf-8 -*-
"""
Indicator Resources
===================
Fear & Greed Index and market regime analysis.

Data sources:
- Alternative.me API: Fear & Greed Index
- Cached regime data (no external module dependency)
"""

from __future__ import annotations

import json
import logging
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict

from oneqaz_trading_mcp.config import (
    CACHE_TTL_FEAR_GREED,
    CACHE_TTL_MARKET_STATUS,
    DATA_ROOT,
)
from oneqaz_trading_mcp.response import to_resource_text

logger = logging.getLogger("MarketMCP")


def _fetch_fear_greed_from_api() -> Dict[str, Any]:
    """Fetch Fear & Greed Index from Alternative.me API with file cache."""
    cache_path = DATA_ROOT / "market" / "coin_market" / "data_storage" / "fear_greed_cache.json"

    # Reuse file cache if fresh (< 5 min)
    try:
        if cache_path.exists():
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if time.time() - cached.get("cache_timestamp", 0) < 300 and cached.get("error") is None:
                return cached
    except Exception:
        pass

    # API call
    try:
        url = "https://api.alternative.me/fng/?limit=1&format=json"
        req = urllib.request.Request(url, headers={"User-Agent": "OneqazTradingMCP/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read().decode())
        entry = body.get("data", [{}])[0]
        result = {
            "value": int(entry.get("value", 50)),
            "classification": entry.get("value_classification", "Neutral"),
            "timestamp": int(entry.get("timestamp", 0)),
            "cache_timestamp": time.time(),
            "error": None,
        }
    except Exception as e:
        logger.warning(f"Fear & Greed API call failed: {e}")
        try:
            if cache_path.exists():
                return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            pass
        return {"value": 50, "classification": "Neutral", "error": str(e)}

    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass
    return result


def _get_fear_greed_adjustment(fg_value: int) -> Dict[str, Any]:
    """Compute strategy adjustment coefficients based on Fear & Greed value."""
    if fg_value <= 20:
        return {"threshold_adj": -0.15, "position_mult": 0.5, "strategy_hint": "defensive"}
    elif fg_value <= 35:
        return {"threshold_adj": -0.08, "position_mult": 0.7, "strategy_hint": "cautious"}
    elif fg_value >= 80:
        return {"threshold_adj": 0.10, "position_mult": 0.6, "strategy_hint": "take_profit"}
    elif fg_value >= 65:
        return {"threshold_adj": 0.05, "position_mult": 0.85, "strategy_hint": "trend"}
    else:
        return {"threshold_adj": 0.0, "position_mult": 1.0, "strategy_hint": "neutral"}


def _interpret_fear_greed(value: int, classification: str) -> str:
    """Return a textual interpretation of the Fear & Greed value."""
    if value <= 25:
        return "Extreme fear zone. Possible contrarian buy opportunity, but further downside risk exists."
    elif value <= 45:
        return "Fear-dominant. Cautious approach recommended. Set tight stop-losses."
    elif value >= 75:
        return "Extreme greed zone. Overheating warning. Consider early profit-taking or avoid new entries."
    elif value >= 55:
        return "Greed-dominant. Consider taking profits. Trend-following still valid."
    else:
        return "Neutral zone. Base decisions on technical analysis."


def _load_fear_greed_index() -> Dict[str, Any]:
    """Load Fear & Greed Index with interpretation and strategy adjustment."""
    try:
        fg_data = _fetch_fear_greed_from_api()
        fg_value = fg_data.get("value", 50)
        fg_class = fg_data.get("classification", "Neutral")
        adjustment = _get_fear_greed_adjustment(fg_value)
        result = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "value": fg_value,
            "classification": fg_class,
            "adjustment": adjustment,
            "interpretation": _interpret_fear_greed(fg_value, fg_class),
        }
        result["_llm_summary"] = (
            f"[Fear & Greed Index]\n"
            f"- Value: {fg_value} ({fg_class})\n"
            f"- Interpretation: {result['interpretation']}\n"
            f"- Strategy adjustment: threshold_adj={adjustment.get('threshold_adj', 0):+.2f}, "
            f"position_mult={adjustment.get('position_mult', 1.0):.2f}x, hint={adjustment.get('strategy_hint', 'neutral')}"
        )
        return result
    except Exception as e:
        logger.error(f"Failed to load Fear & Greed: {e}")
        return {"error": str(e)}


def _load_market_context() -> Dict[str, Any]:
    """Combined market context (Fear & Greed only — no external module dependency)."""
    fear_greed = _load_fear_greed_index()
    fg_value = fear_greed.get("value", 50)

    if fg_value <= 30:
        alignment_note = "Fear zone. Contrarian buy possible."
    elif fg_value >= 70:
        alignment_note = "Greed zone. Consider profit-taking."
    else:
        alignment_note = "Normal range. Use technical analysis."

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fear_greed": fear_greed,
        "alignment_note": alignment_note,
    }
    result["_llm_summary"] = (
        f"[Market Context]\n"
        f"- Fear & Greed: {fg_value} ({fear_greed.get('classification', 'N/A')})\n"
        f"- Note: {alignment_note}"
    )
    return result


def register_indicator_resources(mcp, cache):
    """Register indicator MCP resources."""

    @mcp.resource("market://indicators/fear-greed")
    def get_fear_greed() -> str:
        """
        Fear & Greed Index (market sentiment indicator).
        0-100 scale: lower = fear, higher = greed.
        Returns: Value, classification, strategy adjustment, interpretation.
        """
        cache_key = "fear_greed"
        cached = cache.get(cache_key, ttl=CACHE_TTL_FEAR_GREED)
        if cached:
            return to_resource_text(cached)
        data = _load_fear_greed_index()
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://indicators/context")
    def get_market_context() -> str:
        """
        Combined market context (Fear & Greed + alignment analysis).
        Returns: Sentiment data, alignment note.
        """
        cache_key = "market_context"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = _load_market_context()
        cache.set(cache_key, data)
        return to_resource_text(data)

    logger.info("  Indicator resources registered")
