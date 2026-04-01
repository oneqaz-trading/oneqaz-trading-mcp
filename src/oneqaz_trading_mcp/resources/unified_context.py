# -*- coding: utf-8 -*-
"""
Unified Context Resource
========================
Core layer that merges internal (market technical analysis) and external
(external_context macro/news) data.

3 Levels:
  Level 1 - Symbol-level:  BTC technical indicators + BTC-related news/events
  Level 2 - Market-level:  Impact of global events (e.g. FOMC) on the entire crypto market
  Level 3 - Cross-market:  Gold up + BTC down = risk-off, Bond yields up + Stocks down = rate sensitive

Data Sources:
  - market/{market_id}/data_storage/ -> Technical analysis (internal)
  - external_context/data_storage/   -> News/events/macro (external)
  - market/global_regime/            -> Global regime (cross-market basis)
"""

from __future__ import annotations

import json
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

from oneqaz_trading_mcp.config import (
    PROJECT_ROOT,
    CACHE_TTL_MARKET_STATUS,
    get_market_db_path,
    get_external_db_path,
    get_analysis_db_path,
    get_signals_dir,
    get_structure_summary_path,
    GLOBAL_REGIME_SUMMARY_JSON,
)
from oneqaz_trading_mcp.response import (
    build_resource_explanation,
    to_resource_text,
    with_explanation_contract,
)

logger = logging.getLogger("MarketMCP")

CACHE_TTL_UNIFIED = 120

# Market ID -> global_regime category mapping (for cross-analysis)
MARKET_TO_REGIME_CATEGORY = {
    "crypto": None,
    "kr_stock": None,
    "us_stock": None,
    "commodity": "commodities",
    "commodities": "commodities",
    "forex": "forex",
    "bond": "bonds",
    "bonds": "bonds",
    "vix": "vix",
    "credit": "credit",
    "liquidity": "liquidity",
    "inflation": "inflation",
}

ALL_MARKETS = ["crypto", "kr_stock", "us_stock"]
ALL_CATEGORIES = ["commodities", "forex", "bonds", "vix", "credit", "liquidity", "inflation"]
ALL_STRUCTURE_MARKETS = ["us_stock", "kr_stock", "crypto"]

# --- Cross-market correlation pattern definitions ---
CROSS_MARKET_PATTERNS = [
    {
        "id": "gold_btc_inverse",
        "name": "Gold up BTC down (risk aversion)",
        "pair": ("GC=F", "BTC"),
        "source_market": ("commodities", "crypto"),
        "expected": "inverse",
        "interpretation": "risk_off",
        "description": "When gold rises and BTC falls, risk-off sentiment is strong",
    },
    {
        "id": "gold_btc_aligned",
        "name": "Gold up BTC up (inflation hedge)",
        "pair": ("GC=F", "BTC"),
        "source_market": ("commodities", "crypto"),
        "expected": "aligned",
        "interpretation": "inflation_hedge",
        "description": "When gold and BTC rise together, inflation hedge demand is present",
    },
    {
        "id": "dxy_em_inverse",
        "name": "Dollar up Emerging markets down",
        "pair": ("DX-Y.NYB", None),
        "source_market": ("forex", "kr_stock"),
        "expected": "inverse",
        "interpretation": "dollar_strength",
        "description": "Dollar strength is negative for emerging markets",
    },
    {
        "id": "bond_yield_stock_inverse",
        "name": "Bond yields up Stocks down (rate sensitive)",
        "pair": ("^TNX", None),
        "source_market": ("bonds", "us_stock"),
        "expected": "inverse",
        "interpretation": "rate_sensitivity",
        "description": "Rising bond yields put downward pressure on equities",
    },
    {
        "id": "oil_inflation",
        "name": "Oil up (inflation pressure)",
        "pair": ("CL=F", None),
        "source_market": ("commodities", None),
        "expected": "signal",
        "interpretation": "inflation_pressure",
        "description": "Rising oil prices create inflation pressure affecting all financial markets",
    },
    {
        "id": "vix_risk",
        "name": "VIX up (fear spreading)",
        "pair": ("^VIX", None),
        "source_market": ("vix", None),
        "expected": "signal",
        "interpretation": "fear_spike",
        "description": "VIX spike is a risk-off signal across all markets",
    },
    {
        "id": "vxn_tech_fear",
        "name": "VXN up (tech fear)",
        "pair": ("^VXN", None),
        "source_market": ("vix", None),
        "expected": "signal",
        "interpretation": "tech_fear",
        "description": "NASDAQ volatility spike signals simultaneous decline in tech stocks and crypto",
    },
]

# Global event keywords -> affected markets
GLOBAL_EVENT_IMPACT = {
    "FOMC": ["crypto", "kr_stock", "us_stock", "bonds", "forex", "liquidity", "credit"],
    "interest rate": ["crypto", "kr_stock", "us_stock", "bonds", "forex", "liquidity", "credit"],
    "CPI": ["crypto", "kr_stock", "us_stock", "bonds", "commodities", "inflation"],
    "PCE": ["crypto", "us_stock", "bonds", "inflation"],
    "employment": ["us_stock", "bonds", "forex"],
    "inflation": ["crypto", "kr_stock", "us_stock", "bonds", "commodities", "inflation"],
    "GDP": ["kr_stock", "us_stock", "bonds"],
    "OPEC": ["commodities", "kr_stock", "us_stock", "inflation"],
    "Fed": ["crypto", "kr_stock", "us_stock", "bonds", "forex", "liquidity"],
    "BOJ": ["forex", "kr_stock", "bonds", "liquidity"],
    "ECB": ["forex", "us_stock", "bonds", "liquidity"],
    "tariff": ["kr_stock", "us_stock", "commodities", "inflation"],
    "war": ["crypto", "kr_stock", "us_stock", "commodities", "forex", "bonds", "vix", "credit"],
    "sanctions": ["crypto", "commodities", "forex", "credit"],
    "volatility": ["crypto", "kr_stock", "us_stock", "vix", "credit"],
    "fear": ["crypto", "kr_stock", "us_stock", "vix", "credit"],
    "liquidity": ["crypto", "kr_stock", "us_stock", "bonds", "liquidity"],
    "credit": ["bonds", "credit", "vix"],
}


# --- DB Utilities ---

def _safe_connect(db_path: Path) -> Optional[sqlite3.Connection]:
    if not db_path or not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(str(db_path), timeout=10)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def _fetch_rows(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> List[Dict]:
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    except Exception:
        return []


# --- Level 1: Symbol-level internal+external merge ---

def _load_internal_symbol_snapshot(market_id: str, symbol: str) -> Dict[str, Any]:
    """Internal technical data: latest analysis from signal DB"""
    from oneqaz_trading_mcp.config import get_signal_db_path
    db_path = get_signal_db_path(market_id, symbol)
    if not db_path or not db_path.exists():
        return {}
    conn = _safe_connect(db_path)
    if not conn:
        return {}
    try:
        rows = _fetch_rows(conn, """
            SELECT * FROM signals ORDER BY timestamp DESC LIMIT 1
        """)
        return rows[0] if rows else {}
    finally:
        conn.close()


def _load_external_symbol_snapshot(market_id: str, symbol: str) -> Dict[str, Any]:
    """External context data: news/events/fundamentals for the symbol"""
    db_path = get_external_db_path(market_id)
    conn = _safe_connect(db_path)
    if not conn:
        return {}
    try:
        result = {}
        sym = symbol.upper()
        if _table_exists(conn, "news_events"):
            result["news"] = _fetch_rows(conn, """
                SELECT title, sentiment_score, sentiment_label, sentiment_confidence,
                       sentiment_model, risk_flags, impact_level, published_at
                FROM news_events
                WHERE UPPER(symbol) = ? OR symbol = 'GLOBAL'
                ORDER BY COALESCE(published_at, created_at) DESC LIMIT 5
            """, (sym,))
        if _table_exists(conn, "scheduled_events"):
            result["events"] = _fetch_rows(conn, """
                SELECT title, expected_impact, scheduled_at, status
                FROM scheduled_events
                WHERE (UPPER(symbol) = ? OR symbol = 'GLOBAL') AND status = 'upcoming'
                ORDER BY scheduled_at ASC LIMIT 3
            """, (sym,))
        if _table_exists(conn, "fundamentals"):
            rows = _fetch_rows(conn, """
                SELECT market_cap, per, pbr, roe, sector, industry
                FROM fundamentals WHERE UPPER(symbol) = ? LIMIT 1
            """, (sym,))
            result["fundamentals"] = rows[0] if rows else {}
        return result
    finally:
        conn.close()


def _merge_symbol_context(market_id: str, symbol: str) -> Dict[str, Any]:
    """Merge symbol-level internal + external context"""
    internal = _load_internal_symbol_snapshot(market_id, symbol)
    external = _load_external_symbol_snapshot(market_id, symbol)

    volatility_spike = False
    if internal:
        vol = str(internal.get("volatility_level") or internal.get("risk_level") or "").lower()
        volatility_spike = vol in ("high", "very_high", "critical")

    external_trigger = False
    top_news = external.get("news", [])
    for n in top_news:
        score = n.get("sentiment_score")
        if score is not None and abs(float(score)) >= 0.5:
            external_trigger = True
            break

    correlation_type = "none"
    if volatility_spike and external_trigger:
        correlation_type = "both_active"
    elif volatility_spike:
        correlation_type = "internal_trigger"
    elif external_trigger:
        correlation_type = "external_trigger"

    return {
        "symbol": symbol,
        "internal": internal,
        "external": external,
        "volatility_spike": volatility_spike,
        "external_trigger": external_trigger,
        "correlation_type": correlation_type,
    }


# --- Level 2: Market-level (global event impact) ---

def _load_global_events_affecting(market_id: str) -> List[Dict[str, Any]]:
    """Collect global events affecting market_id from all external context DBs"""
    affecting = []
    news_db = get_external_db_path("news")
    conn = _safe_connect(news_db)
    if conn:
        try:
            if _table_exists(conn, "news_events"):
                rows = _fetch_rows(conn, """
                    SELECT title, summary, sentiment_score, impact_level, published_at
                    FROM news_events
                    ORDER BY COALESCE(published_at, created_at) DESC LIMIT 20
                """)
                for row in rows:
                    title = (row.get("title") or "").lower()
                    for keyword, markets in GLOBAL_EVENT_IMPACT.items():
                        if keyword.lower() in title and market_id in markets:
                            affecting.append({
                                **row,
                                "trigger_keyword": keyword,
                                "affected_markets": markets,
                            })
                            break
        finally:
            conn.close()

    for cat in ALL_CATEGORIES:
        cat_db = get_external_db_path(cat)
        cat_conn = _safe_connect(cat_db)
        if not cat_conn:
            continue
        try:
            if _table_exists(cat_conn, "scheduled_events"):
                rows = _fetch_rows(cat_conn, """
                    SELECT title, expected_impact, scheduled_at, status
                    FROM scheduled_events WHERE status = 'upcoming'
                    ORDER BY scheduled_at ASC LIMIT 5
                """)
                for row in rows:
                    title = (row.get("title") or "").lower()
                    for keyword, markets in GLOBAL_EVENT_IMPACT.items():
                        if keyword.lower() in title and market_id in markets:
                            affecting.append({
                                **row,
                                "type": "scheduled",
                                "category": cat,
                                "trigger_keyword": keyword,
                                "affected_markets": markets,
                            })
                            break
        finally:
            cat_conn.close()

    return affecting


def _load_market_internal_summary(market_id: str) -> Dict[str, Any]:
    """Market-wide internal summary (trading_system.db)"""
    db_path = get_market_db_path(market_id)
    conn = _safe_connect(db_path)
    if not conn:
        return {"error": f"No trading DB for {market_id}"}
    try:
        result = {}
        try:
            rows = _fetch_rows(conn, "SELECT key, value FROM system_status")
            status = {r["key"]: r["value"] for r in rows}
            result["regime"] = status.get("market_regime", "Unknown")
            result["scanning"] = status.get("scanning_coins", "")
        except Exception:
            result["regime"] = "Unknown"

        try:
            rows = _fetch_rows(conn, """
                SELECT COUNT(*) as cnt,
                       AVG(profit_loss_pct) as avg_pnl,
                       SUM(CASE WHEN profit_loss_pct > 0 THEN 1 ELSE 0 END) as wins
                FROM virtual_positions
            """)
            if rows:
                result["positions_count"] = rows[0].get("cnt", 0)
                result["avg_pnl"] = round(rows[0].get("avg_pnl") or 0, 2)
                result["winning_count"] = rows[0].get("wins", 0)
        except Exception:
            pass
        return result
    finally:
        conn.close()


def _load_market_external_summary(market_id: str) -> Dict[str, Any]:
    """Market-wide external summary"""
    db_path = get_external_db_path(market_id)
    conn = _safe_connect(db_path)
    if not conn:
        return {}
    try:
        result = {}
        if _table_exists(conn, "news_events"):
            rows = _fetch_rows(conn, """
                SELECT title, sentiment_score, sentiment_label, sentiment_confidence,
                       sentiment_model, risk_flags, impact_level, published_at, symbol
                FROM news_events
                ORDER BY COALESCE(published_at, created_at) DESC LIMIT 10
            """)
            result["recent_news"] = rows
            high_impact = [r for r in rows if r.get("impact_level") == "high"]
            result["high_impact_news_count"] = len(high_impact)
            risk_news = [r for r in rows if r.get("risk_flags") and r["risk_flags"] != "[]"]
            result["risk_flagged_news_count"] = len(risk_news)

        if _table_exists(conn, "scheduled_events"):
            rows = _fetch_rows(conn, """
                SELECT title, expected_impact, scheduled_at, symbol
                FROM scheduled_events WHERE status = 'upcoming'
                ORDER BY scheduled_at ASC LIMIT 5
            """)
            result["upcoming_events"] = rows

        if _table_exists(conn, "market_metrics"):
            rows = _fetch_rows(conn, """
                SELECT symbol, metric_type, value
                FROM market_metrics
                ORDER BY timestamp DESC LIMIT 20
            """)
            result["metrics"] = rows

        return result
    finally:
        conn.close()


def _build_unified_market_context(market_id: str) -> Dict[str, Any]:
    """Market-level unified context (Level 1+2)"""
    internal = _load_market_internal_summary(market_id)
    external = _load_market_external_summary(market_id)
    global_events = _load_global_events_affecting(market_id)

    alert_level = "normal"
    if len(global_events) >= 3:
        alert_level = "high"
    elif len(global_events) >= 1:
        alert_level = "elevated"

    high_impact = external.get("high_impact_news_count", 0)
    if high_impact >= 2:
        alert_level = "high"

    return {
        "market_id": market_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "internal": internal,
        "external": external,
        "global_events_affecting": global_events,
        "alert_level": alert_level,
        "_llm_summary": _build_unified_llm_summary(market_id, internal, external, global_events),
    }


def _build_unified_llm_summary(
    market_id: str,
    internal: Dict,
    external: Dict,
    global_events: List[Dict],
) -> str:
    lines = [f"[{market_id.upper()} Unified Context]"]

    regime = internal.get("regime", "Unknown")
    pos_count = internal.get("positions_count", 0)
    avg_pnl = internal.get("avg_pnl", 0)
    lines.append(f"Internal: regime={regime}, positions={pos_count}, avg_pnl={avg_pnl:+.2f}%")

    news_count = len(external.get("recent_news", []))
    hi_count = external.get("high_impact_news_count", 0)
    event_count = len(external.get("upcoming_events", []))
    lines.append(f"External: news={news_count}(high_impact={hi_count}), upcoming_events={event_count}")

    if global_events:
        lines.append(f"Global event impact: {len(global_events)} events")
        for ev in global_events[:3]:
            title = ev.get("title", "")[:60]
            kw = ev.get("trigger_keyword", "")
            lines.append(f"  - [{kw}] {title}")

    risk_count = external.get("risk_flagged_news_count", 0)
    if risk_count:
        lines.append(f"Risk-flagged news: {risk_count}")

    top_news = external.get("recent_news", [])[:3]
    if top_news:
        lines.append("Recent key news:")
        for n in top_news:
            title = (n.get("title") or "")[:60]
            score = n.get("sentiment_score")
            label = n.get("sentiment_label", "")
            model = n.get("sentiment_model", "")
            conf = n.get("sentiment_confidence")
            model_tag = f"[{model}]" if model and model != "keyword_fallback" else ""
            score_str = ""
            if isinstance(score, (int, float)):
                conf_str = f", confidence={conf:.0%}" if isinstance(conf, (int, float)) and conf > 0 else ""
                score_str = f" ({label}={score:+.2f}{conf_str}) {model_tag}"
            risk_raw = n.get("risk_flags", "")
            risk_str = ""
            if risk_raw and risk_raw not in ("[]", "null", ""):
                risk_str = f" RISK:{risk_raw}"
            lines.append(f"  - {title}{score_str}{risk_str}")

    upcoming = external.get("upcoming_events", [])[:2]
    if upcoming:
        lines.append("Upcoming events:")
        for e in upcoming:
            title = (e.get("title") or "")[:50]
            sched = (e.get("scheduled_at") or "")[:10]
            lines.append(f"  - {title} ({sched})")

    return "\n".join(lines)


# --- Level 3: Cross-market correlations ---

def _load_regime_directions() -> Dict[str, Dict[str, Any]]:
    """Load direction per category/symbol from global regime"""
    directions: Dict[str, Dict[str, Any]] = {}

    for category in ALL_CATEGORIES:
        db_path = get_analysis_db_path(category)
        conn = _safe_connect(db_path)
        if not conn:
            continue
        try:
            rows = _fetch_rows(conn, """
                SELECT symbol, regime_label, sentiment_label, integrated_direction,
                       regime_stage, volatility_level
                FROM analysis
                ORDER BY timestamp DESC LIMIT 50
            """)
            seen = set()
            for row in rows:
                sym = row.get("symbol", "")
                if sym in seen:
                    continue
                seen.add(sym)
                direction = _classify_direction(row)
                directions[sym] = {
                    "category": category,
                    "direction": direction,
                    "regime": row.get("regime_label") or row.get("regime_stage", ""),
                    "sentiment": row.get("sentiment_label", ""),
                    "volatility": row.get("volatility_level", ""),
                    "raw_direction": row.get("integrated_direction", ""),
                }
        finally:
            conn.close()

    for market_id in ALL_MARKETS:
        db_path = get_market_db_path(market_id)
        conn = _safe_connect(db_path)
        if not conn:
            continue
        try:
            status_rows = _fetch_rows(conn, "SELECT key, value FROM system_status")
            status = {r["key"]: r["value"] for r in status_rows}
            regime = (status.get("market_regime") or "").lower()
            d = "neutral"
            if any(k in regime for k in ("bull", "up", "risk_on", "risk-on")):
                d = "up"
            elif any(k in regime for k in ("bear", "down", "risk_off", "risk-off")):
                d = "down"
            directions[f"_market_{market_id}"] = {
                "category": market_id,
                "direction": d,
                "regime": status.get("market_regime", "Unknown"),
            }
        finally:
            conn.close()

    return directions


def _classify_direction(row: Dict) -> str:
    d = (row.get("integrated_direction") or "").lower()
    if any(k in d for k in ("bull", "up", "long")):
        return "up"
    if any(k in d for k in ("bear", "down", "short")):
        return "down"
    regime = (row.get("regime_label") or row.get("regime_stage") or "").lower()
    if any(k in regime for k in ("bull", "risk_on", "risk-on")):
        return "up"
    if any(k in regime for k in ("bear", "risk_off", "risk-off")):
        return "down"
    return "neutral"


def _detect_cross_market_correlations(directions: Dict[str, Dict]) -> List[Dict[str, Any]]:
    """Detect cross-market correlation patterns"""
    detected = []

    for pattern in CROSS_MARKET_PATTERNS:
        sym_a, sym_b = pattern["pair"]
        src_a, src_b = pattern["source_market"]

        dir_a = None
        if sym_a and sym_a in directions:
            dir_a = directions[sym_a]
        elif sym_a:
            for key, val in directions.items():
                if sym_a.upper() in key.upper() and val.get("category") == src_a:
                    dir_a = val
                    break

        dir_b = None
        if sym_b and sym_b in directions:
            dir_b = directions[sym_b]
        elif sym_b:
            for key, val in directions.items():
                if sym_b.upper() in key.upper():
                    dir_b = val
                    break
        if dir_b is None and src_b:
            market_key = f"_market_{src_b}"
            if market_key in directions:
                dir_b = directions[market_key]

        if pattern["expected"] == "signal":
            if dir_a and dir_a["direction"] != "neutral":
                strength = dir_a.get("volatility", "")
                score = 0.7 if strength in ("high", "very_high") else 0.4
                detected.append({
                    "pattern_id": pattern["id"],
                    "pattern_name": pattern["name"],
                    "interpretation": pattern["interpretation"],
                    "description": pattern["description"],
                    "direction_a": dir_a["direction"],
                    "direction_b": None,
                    "score": score,
                    "active": True,
                })
            continue

        if not dir_a or not dir_b:
            continue

        a_dir = dir_a["direction"]
        b_dir = dir_b["direction"]

        if a_dir == "neutral" and b_dir == "neutral":
            continue

        is_inverse = (a_dir == "up" and b_dir == "down") or (a_dir == "down" and b_dir == "up")
        is_aligned = (a_dir == b_dir) and a_dir != "neutral"

        active = False
        if pattern["expected"] == "inverse" and is_inverse:
            active = True
        elif pattern["expected"] == "aligned" and is_aligned:
            active = True

        score = 0.8 if active else 0.3

        detected.append({
            "pattern_id": pattern["id"],
            "pattern_name": pattern["name"],
            "interpretation": pattern["interpretation"],
            "description": pattern["description"],
            "direction_a": a_dir,
            "direction_b": b_dir,
            "score": score,
            "active": active,
        })

    return detected


def _build_cross_market_context() -> Dict[str, Any]:
    """Full cross-market analysis (Level 3)"""
    directions = _load_regime_directions()
    correlations = _detect_cross_market_correlations(directions)

    active = [c for c in correlations if c.get("active")]
    dominant_interpretation = None
    if active:
        interp_counts: Dict[str, float] = {}
        for c in active:
            interp = c["interpretation"]
            interp_counts[interp] = interp_counts.get(interp, 0) + c["score"]
        dominant_interpretation = max(interp_counts, key=interp_counts.get)

    regime_summary = _load_global_regime_brief()

    structure_brief = _load_all_structure_briefs()

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "directions": {
            k: {"direction": v["direction"], "regime": v.get("regime", ""), "category": v.get("category", "")}
            for k, v in directions.items()
        },
        "correlations": correlations,
        "active_patterns": active,
        "active_count": len(active),
        "dominant_interpretation": dominant_interpretation,
        "global_regime": regime_summary,
        "market_structure": structure_brief,
        "_llm_summary": _build_cross_market_llm_summary(directions, active, dominant_interpretation, regime_summary),
    }
    return result


def _load_all_structure_briefs() -> Dict[str, Any]:
    """Load structure summary briefs for all markets"""
    result: Dict[str, Any] = {}
    for market_id in ALL_STRUCTURE_MARKETS:
        path = get_structure_summary_path(market_id)
        if not path or not path.exists():
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            overall = data.get("overall", {})
            groups_brief = {}
            for gid, ginfo in data.get("groups", {}).items():
                if isinstance(ginfo, dict):
                    groups_brief[gid] = {
                        "regime_dominant": ginfo.get("regime_dominant"),
                        "regime_avg": ginfo.get("regime_avg"),
                        "timing_signal": ginfo.get("timing_signal"),
                        "confidence": ginfo.get("confidence"),
                    }
            result[market_id] = {
                "overall_regime": overall.get("regime", "unknown"),
                "overall_score": overall.get("score", 0),
                "groups": groups_brief,
                "updated_at": data.get("updated_at"),
            }
        except Exception:
            continue
    return result


def _load_global_regime_brief() -> Dict[str, Any]:
    try:
        if GLOBAL_REGIME_SUMMARY_JSON.exists():
            with open(GLOBAL_REGIME_SUMMARY_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {
                "overall_regime": data.get("overall", {}).get("regime", "Unknown"),
                "overall_score": data.get("overall", {}).get("score", 0),
                "updated_at": data.get("updated_at"),
            }
    except Exception:
        pass
    return {"overall_regime": "Unknown", "overall_score": 0}


def _build_cross_market_llm_summary(
    directions: Dict,
    active_patterns: List[Dict],
    dominant: Optional[str],
    regime: Dict,
) -> str:
    lines = ["[Cross-Market Correlation Analysis]"]
    lines.append(f"Global regime: {regime.get('overall_regime', '?')} (score: {regime.get('overall_score', 0):.2f})")

    market_dirs = {k: v for k, v in directions.items() if k.startswith("_market_")}
    if market_dirs:
        lines.append("Market directions:")
        for k, v in market_dirs.items():
            name = k.replace("_market_", "")
            lines.append(f"  - {name}: {v['direction']} ({v.get('regime', '')})")

    if active_patterns:
        lines.append(f"\nActive patterns ({len(active_patterns)}):")
        for p in active_patterns:
            lines.append(f"  - {p['pattern_name']}: {p['description']}")
        if dominant:
            INTERP_EN = {
                "risk_off": "Risk-off sentiment dominant",
                "risk_on": "Risk-on sentiment dominant",
                "inflation_hedge": "Inflation hedge demand",
                "dollar_strength": "Dollar strength impact",
                "rate_sensitivity": "Rate-sensitive phase",
                "inflation_pressure": "Inflation pressure",
                "fear_spike": "Fear spike",
            }
            lines.append(f"\nDominant interpretation: {INTERP_EN.get(dominant, dominant)}")
    else:
        lines.append("\nNo active cross-market patterns - no clear inter-market correlation signals detected")

    return "\n".join(lines)


# --- Integrated endpoints: combine everything ---

def _load_feature_gate_summary(market_id: str) -> Dict[str, Any]:
    """Load Feature Gate status (agent_history removed - returns empty)"""
    return {}


def _load_agent_history_rag(market_id: str, regime: str = "") -> str:
    """Load agent_history RAG context (agent_history removed - returns empty)"""
    return ""


def _load_inference_watchlist(market_id: str) -> List[Dict[str, Any]]:
    """Load event propagation inference watchlist"""
    market_map = {"crypto": "coin_market", "kr_stock": "kr_market", "us_stock": "us_market"}
    search_markets = ["news"]
    mapped = market_map.get(market_id)
    if mapped:
        search_markets.append(mapped)

    items = []
    for mid in search_markets:
        db_path = get_external_db_path(mid)
        conn = _safe_connect(db_path)
        if not conn:
            continue
        try:
            if not _table_exists(conn, "inference_candidates"):
                continue
            rows = _fetch_rows(conn, """
                SELECT candidate_symbol, candidate_market, source_title,
                       relation_type, relation_detail, adjusted_confidence,
                       coverage_penalty, created_at
                FROM inference_candidates
                WHERE status = 'watching'
                ORDER BY adjusted_confidence DESC, created_at DESC
                LIMIT 5
            """)
            items.extend(rows)
        finally:
            conn.close()
    return items[:5]


def _load_regime_flow_summary(market_id: str) -> str:
    """Load regime flow analysis results as LLM summary text"""
    market_map = {"crypto": "coin_market", "kr_stock": "kr_market", "us_stock": "us_market",
                  "commodity": "commodities", "forex": "forex", "bond": "bonds", "vix": "vix"}
    mapped = market_map.get(market_id, market_id)
    db_path = get_external_db_path(mapped)
    conn = _safe_connect(db_path)
    if not conn:
        return ""

    parts = []
    try:
        # Market-wide regime flow
        if _table_exists(conn, "market_regime_flow"):
            mf = _fetch_rows(conn, """
                SELECT dominant_regime, avg_regime_stage, avg_volume_ratio,
                       bullish_pct, bearish_pct, neutral_pct,
                       volume_trend, regime_trend, snapshot_at
                FROM market_regime_flow
                WHERE market_id = ?
                ORDER BY snapshot_at DESC LIMIT 1
            """, (mapped,))
            if mf:
                m = mf[0]
                parts.append(
                    f"[Market Regime Flow] regime={m.get('dominant_regime','?')} "
                    f"(stage={m.get('avg_regime_stage','?'):.1f}), "
                    f"volume_ratio={m.get('avg_volume_ratio',1):.2f}, "
                    f"bullish={m.get('bullish_pct',0):.0%}/bearish={m.get('bearish_pct',0):.0%}/neutral={m.get('neutral_pct',0):.0%}, "
                    f"volume_trend={m.get('volume_trend','?')}, regime_trend={m.get('regime_trend','?')}"
                )

        # Sector-level regime flow (top entries)
        if _table_exists(conn, "sector_regime_flow"):
            sf = _fetch_rows(conn, """
                SELECT sector, dominant_regime, avg_volume_ratio,
                       volume_trend, regime_trend, symbol_count
                FROM sector_regime_flow
                WHERE market_id = ?
                ORDER BY snapshot_at DESC, avg_volume_ratio DESC
                LIMIT 8
            """, (mapped,))
            if sf:
                sect_lines = ["[Sector Regime]"]
                for s in sf:
                    sect_lines.append(
                        f"  {s.get('sector','?')}({s.get('symbol_count',0)} symbols): "
                        f"{s.get('dominant_regime','?')}, vol={s.get('avg_volume_ratio',1):.2f}"
                        f"({s.get('volume_trend','?')}), trend={s.get('regime_trend','?')}"
                    )
                parts.append("\n".join(sect_lines))

        # Regime transition prediction (top 3)
        if _table_exists(conn, "regime_transitions"):
            current_regime = ""
            if _table_exists(conn, "market_regime_flow"):
                cr = conn.execute(
                    "SELECT dominant_regime FROM market_regime_flow WHERE market_id = ? ORDER BY snapshot_at DESC LIMIT 1",
                    (mapped,),
                ).fetchone()
                if cr:
                    current_regime = cr[0]

            if current_regime:
                trans = conn.execute(
                    """
                    SELECT to_regime, volume_condition, count
                    FROM regime_transitions
                    WHERE market_id = ? AND scope = 'market' AND from_regime = ?
                    ORDER BY count DESC LIMIT 5
                    """,
                    (mapped, current_regime),
                ).fetchall()
                if trans:
                    total = sum(t[2] for t in trans)
                    pred_lines = [f"[Regime Transition Prediction] current={current_regime}"]
                    for t in trans[:3]:
                        prob = t[2] / total if total > 0 else 0
                        pred_lines.append(
                            f"  -> {t[0]} (probability={prob:.0%}, volume_condition={t[1]}, samples={t[2]})"
                        )
                    parts.append("\n".join(pred_lines))

        # Cross-market signals
        news_db = _safe_connect(get_external_db_path("news"))
        if news_db:
            try:
                if _table_exists(news_db, "cross_market_correlation"):
                    cross = news_db.execute(
                        """
                        SELECT source_market, source_regime_change, lag_hours,
                               correlation_score, count
                        FROM cross_market_correlation
                        WHERE target_market = ? AND correlation_score >= 0.4 AND count >= 2
                        ORDER BY correlation_score DESC LIMIT 5
                        """,
                        (mapped,),
                    ).fetchall()
                    if cross:
                        cross_lines = ["[Cross-Market Regime Correlation]"]
                        for c in cross:
                            cross_lines.append(
                                f"  {c[0]}({c[1]}) -> this market impact: "
                                f"avg {c[2]:.0f}h lag, correlation={c[3]:.2f}, observations={c[4]}"
                            )
                        parts.append("\n".join(cross_lines))
            finally:
                news_db.close()

    except Exception:
        pass
    finally:
        conn.close()

    return "\n".join(parts)


def _build_full_unified_context(market_id: str) -> Dict[str, Any]:
    """Full unified context for a specific market (Level 1+2+3 + Feature Gate + Inference)"""
    market_ctx = _build_unified_market_context(market_id)
    cross_market = _build_cross_market_context()

    current_regime = market_ctx.get("internal", {}).get("regime", "")

    feature_gate = _load_feature_gate_summary(market_id)
    history_rag = _load_agent_history_rag(market_id, current_regime)
    inference_watchlist = _load_inference_watchlist(market_id)
    regime_flow = _load_regime_flow_summary(market_id)

    full_summary_parts = []
    if market_ctx.get("_llm_summary"):
        full_summary_parts.append(market_ctx["_llm_summary"])
    if cross_market.get("_llm_summary"):
        full_summary_parts.append(cross_market["_llm_summary"])
    if history_rag:
        full_summary_parts.append(history_rag)

    if regime_flow:
        full_summary_parts.append(regime_flow)

    if inference_watchlist:
        wl_lines = ["[Event Propagation Inference Watchlist]"]
        for item in inference_watchlist:
            sym = item.get("candidate_symbol", "?")
            src = (item.get("source_title") or "")[:50]
            rel = item.get("relation_type", "related")
            conf = item.get("adjusted_confidence", 0)
            penalty = item.get("coverage_penalty", 0)
            detail = (item.get("relation_detail") or "")[:40]
            conf_val = float(conf) if conf else 0.0
            penalty_val = float(penalty) if penalty else 0.0
            wl_lines.append(
                f"  {sym} ({rel}): '{src}' "
                f"[confidence={conf_val:.2f}, penalty={penalty_val:.2f}] {detail}"
            )
        full_summary_parts.append("\n".join(wl_lines))

    data = {
        "market_id": market_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level_1_2": market_ctx,
        "level_3": cross_market,
        "feature_gate": feature_gate,
        "inference_watchlist": inference_watchlist,
        "regime_flow": regime_flow,
        "alert_level": market_ctx.get("alert_level", "normal"),
        "active_cross_patterns": cross_market.get("active_count", 0),
        "dominant_interpretation": cross_market.get("dominant_interpretation"),
        "_llm_summary": "\n\n---\n\n".join(full_summary_parts),
    }
    explanation = build_resource_explanation(
        market=market_id,
        entity_type="market",
        explanation_type="mcp_unified_context",
        as_of_time=data["timestamp"],
        headline=f"{market_id} unified context snapshot",
        why_text=data.get("_llm_summary") or f"{market_id} unified context available",
        bullet_points=[
            f"alert_level={data.get('alert_level', 'normal')}",
            f"active_cross_patterns={data.get('active_cross_patterns', 0)}",
            f"dominant_interpretation={data.get('dominant_interpretation') or 'none'}",
        ],
        market_context_refs={
            "alert_level": data.get("alert_level", "normal"),
            "active_cross_patterns": data.get("active_cross_patterns", 0),
            "dominant_interpretation": data.get("dominant_interpretation"),
        },
        note="derived from unified MCP resource",
    )
    return with_explanation_contract(
        data,
        resource_type="unified_context",
        explanation=explanation,
    )


# --- Resource registration ---

def register_unified_context_resources(mcp, cache):
    """Register unified context resources"""

    @mcp.resource("market://{market_id}/unified")
    def get_unified_context(market_id: str) -> Dict[str, Any]:
        """
        Market-level unified context (internal + external + cross-market combined)

        Merges internal technical analysis + external news/events/macro +
        cross-market correlations into a single response. Allows LLM agents
        to receive all levels of context in a single call.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock, commodity, forex, bond)

        Returns:
            Unified context (level_1_2: symbol+market, level_3: cross-market, _llm_summary)
        """
        cache_key = f"unified_context_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_UNIFIED)
        if cached:
            return to_resource_text(cached)
        data = _build_full_unified_context(market_id)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://unified/cross-market")
    def get_cross_market_correlations() -> Dict[str, Any]:
        """
        Cross-market correlation analysis (Level 3)

        Detects inter-market correlation patterns such as
        Gold vs BTC, Bond yields vs Stocks, Dollar vs Emerging markets, etc.

        Returns:
            Cross-market correlations (directions, active_patterns, dominant_interpretation)
        """
        cache_key = "cross_market_correlations"
        cached = cache.get(cache_key, ttl=CACHE_TTL_UNIFIED)
        if cached:
            return to_resource_text(cached)
        raw = _build_cross_market_context()
        explanation = build_resource_explanation(
            market="global",
            entity_type="market",
            explanation_type="mcp_cross_market_context",
            as_of_time=raw.get("timestamp"),
            headline="cross market correlation snapshot",
            why_text=raw.get("_llm_summary") or "cross market context available",
            bullet_points=[
                f"active_count={raw.get('active_count', 0)}",
                f"dominant_interpretation={raw.get('dominant_interpretation') or 'none'}",
            ],
            market_context_refs={
                "active_count": raw.get("active_count", 0),
                "dominant_interpretation": raw.get("dominant_interpretation"),
            },
            note="derived from cross-market MCP resource",
        )
        data = with_explanation_contract(
            raw,
            resource_type="cross_market_context",
            explanation=explanation,
        )
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/unified/symbol/{symbol}")
    def get_unified_symbol_context(market_id: str, symbol: str) -> Dict[str, Any]:
        """
        Symbol-level unified context (Level 1)

        Merges a specific symbol's technical analysis (internal) with
        news/events (external).

        Args:
            market_id: Market ID
            symbol: Symbol code

        Returns:
            Symbol unified context (internal, external, correlation_type)
        """
        cache_key = f"unified_symbol_{market_id}_{symbol}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_UNIFIED)
        if cached:
            return to_resource_text(cached)
        raw = _merge_symbol_context(market_id, symbol)
        explanation = build_resource_explanation(
            market=market_id,
            symbol=symbol,
            entity_type="symbol",
            explanation_type="mcp_unified_symbol_context",
            as_of_time=datetime.now(timezone.utc).isoformat(),
            headline=f"{symbol} unified symbol context",
            why_text=raw.get("_llm_summary") or f"{symbol} symbol context available",
            bullet_points=[
                f"correlation_type={raw.get('correlation_type', 'none')}",
                f"external_trigger={raw.get('external_trigger', False)}",
                f"volatility_spike={raw.get('volatility_spike', False)}",
            ],
            market_context_refs={
                "correlation_type": raw.get("correlation_type", "none"),
                "external_trigger": raw.get("external_trigger", False),
                "volatility_spike": raw.get("volatility_spike", False),
            },
            note="derived from unified symbol MCP resource",
        )
        data = with_explanation_contract(
            raw,
            resource_type="unified_symbol_context",
            explanation=explanation,
        )
        cache.set(cache_key, data)
        return to_resource_text(data)

    logger.info("  Unified Context resources registered (Level 1/2/3)")
