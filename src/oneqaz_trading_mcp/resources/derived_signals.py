# -*- coding: utf-8 -*-
"""
Derived Signals Resources
=========================
High-value derived signal resources (5 types).

1. event_leading_scores    — Event leading scores
2. regime_transition_probs — Regime transition probabilities
3. cross_market_decoupling — Cross-market decoupling index
4. news_reaction_speed     — News reaction speed
5. strategy_fitness        — Strategy fitness scores
"""

from __future__ import annotations

import sqlite3
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from oneqaz_trading_mcp.config import (
    CACHE_TTL_MARKET_STATUS,
    EXTERNAL_CONTEXT_DATA_DIR,
    PROJECT_ROOT,
    get_external_db_path,
)
from oneqaz_trading_mcp.response import (
    build_resource_explanation,
    to_resource_text,
    with_explanation_contract,
)

logger = logging.getLogger("MarketMCP")


# -- Common helpers --

def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _fetch_all(conn: sqlite3.Connection, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def _get_news_db_path():
    return EXTERNAL_CONTEXT_DATA_DIR / "news" / "external_context.db"


def _get_agent_history_db_path(market_id: str):
    """Get agent_history DB path (coin/kr_stock/us_stock). Disabled — returns None."""
    return None


# ===================================================================
# 1. Event leading scores
# ===================================================================

def _load_event_leading_scores() -> Dict[str, Any]:
    """Load event leading scores from the news DB."""
    db_path = _get_news_db_path()
    if not db_path.exists():
        return {"error": "news DB not found", "scores": []}

    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            if not _table_exists(conn, "event_leading_scores"):
                return {"scores": [], "note": "Table not yet created (awaiting first cycle)"}

            scores = _fetch_all(conn, """
                SELECT event_type, market_id, news_type,
                       leading_score, avg_lead_time_minutes,
                       avg_anticipation_ratio, avg_precursor_score,
                       sample_count, accuracy_pct, computed_at
                FROM event_leading_scores
                ORDER BY leading_score DESC
            """)

            summary = _build_leading_score_summary(scores)
            result = {"scores": scores, "_llm_summary": summary}

            explanation = build_resource_explanation(
                market="global",
                entity_type="market",
                explanation_type="mcp_event_leading_scores",
                as_of_time=scores[0]["computed_at"] if scores else None,
                headline="Event leading scores snapshot",
                why_text=summary,
                bullet_points=[
                    f"Analysis groups={len(scores)}",
                    f"Top leading score={scores[0]['leading_score']:.2f}" if scores else "No data",
                ],
            )
            return with_explanation_contract(result, resource_type="event_leading_scores", explanation=explanation)
    except Exception as e:
        logger.warning("Failed to load event leading scores: %s", e)
        return {"error": str(e), "scores": []}


def _build_leading_score_summary(scores: List[Dict]) -> str:
    if not scores:
        return "[Event leading scores] No data"
    lines = ["[Event leading scores]"]
    for s in scores[:5]:
        lines.append(
            f"- {s['event_type']}({s['news_type']}): "
            f"leading={s['leading_score']:.2f}, "
            f"avg_lead_time={s.get('avg_lead_time_minutes', 0):.0f}min, "
            f"accuracy={s.get('accuracy_pct', 0) or 0:.0%}"
        )
    if len(scores) > 5:
        lines.append(f"  ... and {len(scores) - 5} more")
    return "\n".join(lines)


# ===================================================================
# 2. Regime transition probabilities
# ===================================================================

def _load_regime_transition_probs(market_id: str) -> Dict[str, Any]:
    """Load per-market regime transition probabilities."""
    ec_map = {
        "crypto": "coin_market", "coin": "coin_market", "coin_market": "coin_market",
        "kr_stock": "kr_market", "kr": "kr_market", "kr_market": "kr_market",
        "us_stock": "us_market", "us": "us_market", "us_market": "us_market",
    }
    ec_mid = ec_map.get(market_id.lower(), market_id.lower())
    db_path = EXTERNAL_CONTEXT_DATA_DIR / ec_mid / "external_context.db"

    if not db_path.exists():
        return {"error": f"DB not found for {ec_mid}", "market_id": market_id, "transitions": []}

    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            if not _table_exists(conn, "regime_transition_probs"):
                return {"market_id": market_id, "transitions": [], "note": "Table not yet created"}

            rows = _fetch_all(conn, """
                SELECT current_regime, next_regime, probability,
                       avg_duration_hours, cross_market_boost, confidence, computed_at
                FROM regime_transition_probs
                WHERE market_id = ?
                ORDER BY probability DESC
            """, (ec_mid,))

            current = rows[0]["current_regime"] if rows else None
            summary = _build_transition_summary(market_id, current, rows)
            result = {
                "market_id": market_id,
                "current_regime": current,
                "transitions": rows,
                "_llm_summary": summary,
            }

            explanation = build_resource_explanation(
                market=market_id,
                entity_type="market",
                explanation_type="mcp_regime_transition_probs",
                as_of_time=rows[0]["computed_at"] if rows else None,
                headline=f"{market_id} regime transition probabilities",
                why_text=summary,
                bullet_points=[
                    f"Current regime={current or 'unknown'}",
                    f"Transition paths={len(rows)}",
                ],
            )
            return with_explanation_contract(result, resource_type="regime_transition_probs", explanation=explanation)
    except Exception as e:
        logger.warning("Failed to load regime transition probs [%s]: %s", market_id, e)
        return {"error": str(e), "market_id": market_id, "transitions": []}


def _build_transition_summary(market_id: str, current: str | None, rows: List[Dict]) -> str:
    if not rows:
        return f"[{market_id} regime transition] No data"
    lines = [f"[{market_id} regime transition] current={current or '?'}"]
    for r in rows[:5]:
        boost = f", cross_market+{r['cross_market_boost']:.2f}" if r.get("cross_market_boost") else ""
        lines.append(f"- -> {r['next_regime']}: {r['probability']:.1%} (confidence={r['confidence']:.2f}{boost})")
    return "\n".join(lines)


# ===================================================================
# 3. Cross-market decoupling index
# ===================================================================

def _load_cross_market_decoupling() -> Dict[str, Any]:
    """Load cross-market decoupling index from the news DB."""
    db_path = _get_news_db_path()
    if not db_path.exists():
        return {"error": "news DB not found", "pairs": []}

    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            if not _table_exists(conn, "cross_market_decoupling"):
                return {"pairs": [], "note": "Table not yet created"}

            pairs = _fetch_all(conn, """
                SELECT source_market, target_market, decoupling_index,
                       correlation_breakdown, regime_divergence,
                       timing_lag_divergence, computed_at
                FROM cross_market_decoupling
                ORDER BY decoupling_index DESC
            """)

            avg_idx = sum(p["decoupling_index"] for p in pairs) / len(pairs) if pairs else 0
            summary = _build_decoupling_summary(pairs, avg_idx)
            result = {
                "pairs": pairs,
                "overall_decoupling": round(avg_idx, 4),
                "_llm_summary": summary,
            }

            explanation = build_resource_explanation(
                market="global",
                entity_type="market",
                explanation_type="mcp_cross_market_decoupling",
                as_of_time=pairs[0]["computed_at"] if pairs else None,
                headline="Cross-market decoupling snapshot",
                why_text=summary,
                bullet_points=[
                    f"Analyzed pairs={len(pairs)}",
                    f"Average decoupling={avg_idx:.2f}",
                ],
            )
            return with_explanation_contract(result, resource_type="cross_market_decoupling", explanation=explanation)
    except Exception as e:
        logger.warning("Failed to load decoupling index: %s", e)
        return {"error": str(e), "pairs": []}


def _build_decoupling_summary(pairs: List[Dict], avg: float) -> str:
    if not pairs:
        return "[Decoupling index] No data"
    level = "high (divergent)" if avg > 0.6 else "moderate" if avg > 0.3 else "low (correlated)"
    lines = [f"[Decoupling index] Overall level={level} ({avg:.2f})"]
    for p in pairs:
        lines.append(
            f"- {p['source_market']}<->{p['target_market']}: "
            f"{p['decoupling_index']:.2f} "
            f"(corr_breakdown={p.get('correlation_breakdown', 0) or 0:.2f}, "
            f"regime_divergence={p.get('regime_divergence', 0) or 0:.2f})"
        )
    return "\n".join(lines)


# ===================================================================
# 4. News reaction speed
# ===================================================================

def _load_news_reaction_speed() -> Dict[str, Any]:
    """Load news reaction speed from the news DB."""
    db_path = _get_news_db_path()
    if not db_path.exists():
        return {"error": "news DB not found", "speed_bands": []}

    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            if not _table_exists(conn, "news_reaction_speed"):
                return {"speed_bands": [], "note": "Table not yet created"}

            bands = _fetch_all(conn, """
                SELECT news_type, market_id, speed_band, count,
                       avg_reaction_score, avg_lag_minutes,
                       absorption_time_minutes, direction_accuracy, computed_at
                FROM news_reaction_speed
                ORDER BY news_type, speed_band
            """)

            summary = _build_reaction_speed_summary(bands)
            result = {"speed_bands": bands, "_llm_summary": summary}

            explanation = build_resource_explanation(
                market="global",
                entity_type="market",
                explanation_type="mcp_news_reaction_speed",
                as_of_time=bands[0]["computed_at"] if bands else None,
                headline="News reaction speed classification",
                why_text=summary,
                bullet_points=[f"Classification groups={len(bands)}"],
            )
            return with_explanation_contract(result, resource_type="news_reaction_speed", explanation=explanation)
    except Exception as e:
        logger.warning("Failed to load news reaction speed: %s", e)
        return {"error": str(e), "speed_bands": []}


def _build_reaction_speed_summary(bands: List[Dict]) -> str:
    if not bands:
        return "[News reaction speed] No data"
    lines = ["[News reaction speed]"]
    for b in bands:
        acc = f", direction_accuracy={b['direction_accuracy']:.0%}" if b.get("direction_accuracy") else ""
        lines.append(
            f"- {b['news_type']}/{b['speed_band']}: "
            f"avg_lag={b.get('avg_lag_minutes', 0) or 0:.0f}min, "
            f"absorption={b.get('absorption_time_minutes', 0) or 0:.0f}min{acc} "
            f"(N={b['count']})"
        )
    return "\n".join(lines)


# ===================================================================
# 5. Strategy fitness scores
# ===================================================================

def _load_strategy_fitness(market_id: str) -> Dict[str, Any]:
    """Load strategy fitness scores. Returns empty results (agent_history dependency removed)."""
    return {"market_id": market_id, "fitness": [], "prefer_count": 0, "avoid_count": 0,
            "note": "agent_history module not available",
            "_llm_summary": f"[{market_id} strategy fitness] No data (agent_history not available)"}


def _build_fitness_summary(market_id: str, fitness: List[Dict], prefer: List, avoid: List) -> str:
    if not fitness:
        return f"[{market_id} strategy fitness] No data"
    lines = [f"[{market_id} strategy fitness] Total {len(fitness)} (prefer={len(prefer)}, avoid={len(avoid)})"]
    for f in fitness[:6]:
        lines.append(
            f"- {f['regime']}/{f['strategy_role']}: "
            f"fitness={f['fitness_score']:.2f} ({f['recommended_action']}) "
            f"WR={f.get('historical_win_rate', 0) or 0:.0%}, "
            f"PF={f.get('historical_pf', 0) or 0:.2f}"
        )
    return "\n".join(lines)


# ===================================================================
# Combined: All 5 signals at once
# ===================================================================

def _load_all_derived_signals(market_id: str) -> Dict[str, Any]:
    """Load all 5 derived signals in a single batch."""
    result = {
        "market_id": market_id,
        "event_leading_scores": _load_event_leading_scores(),
        "regime_transition_probs": _load_regime_transition_probs(market_id),
        "cross_market_decoupling": _load_cross_market_decoupling(),
        "news_reaction_speed": _load_news_reaction_speed(),
        "strategy_fitness": _load_strategy_fitness(market_id),
    }

    # Combined LLM summary
    summaries = []
    for key in ("event_leading_scores", "regime_transition_probs", "cross_market_decoupling",
                "news_reaction_speed", "strategy_fitness"):
        s = result[key].get("_llm_summary", "")
        if s:
            summaries.append(s)
    result["_llm_summary"] = "\n\n".join(summaries)

    explanation = build_resource_explanation(
        market=market_id,
        entity_type="market",
        explanation_type="mcp_derived_signals_all",
        headline=f"{market_id} all 5 derived signals combined",
        why_text=result["_llm_summary"][:500],
        bullet_points=[
            "event_leading / regime_transition / decoupling / reaction_speed / strategy_fitness",
        ],
    )
    return with_explanation_contract(result, resource_type="derived_signals_all", explanation=explanation)


# ===================================================================
# Registration
# ===================================================================

def register_derived_signals_resources(mcp, cache):
    """Register derived signal resources."""

    @mcp.resource("market://derived/event-leading")
    def get_event_leading_scores():
        cache_key = "derived_event_leading"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = _load_event_leading_scores()
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/derived/regime-transitions")
    def get_regime_transition_probs(market_id: str):
        cache_key = f"derived_regime_trans_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = _load_regime_transition_probs(market_id)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://derived/cross-decoupling")
    def get_cross_market_decoupling():
        cache_key = "derived_cross_decoupling"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = _load_cross_market_decoupling()
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://derived/reaction-speed")
    def get_news_reaction_speed():
        cache_key = "derived_reaction_speed"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = _load_news_reaction_speed()
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/derived/strategy-fitness")
    def get_strategy_fitness(market_id: str):
        cache_key = f"derived_strategy_fitness_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = _load_strategy_fitness(market_id)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/derived/all")
    def get_all_derived_signals(market_id: str):
        cache_key = f"derived_all_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = _load_all_derived_signals(market_id)
        cache.set(cache_key, data)
        return to_resource_text(data)

    logger.info("  Derived Signals resources registered")
