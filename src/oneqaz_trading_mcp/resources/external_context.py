# -*- coding: utf-8 -*-
"""
External Context Resources
==========================
Resource for querying external_context DB (per-market external data).
"""

from __future__ import annotations

import sqlite3
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from oneqaz_trading_mcp.config import CACHE_TTL_MARKET_STATUS, get_external_db_path
from oneqaz_trading_mcp.response import (
    build_resource_explanation,
    to_resource_text,
    with_explanation_contract,
)

logger = logging.getLogger("MarketMCP")


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


def _latest_updated_at(conn: sqlite3.Connection) -> str | None:
    if not _table_exists(conn, "pipeline_runs"):
        return None
    row = conn.execute(
        "SELECT fetched_at FROM pipeline_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def _load_external_summary(market_id: str) -> Dict[str, Any]:
    """Load external context summary for the given market."""
    db_path = get_external_db_path(market_id)
    if not db_path or not db_path.exists():
        return {
            "error": f"external_context DB not found for market: {market_id}",
            "market_id": market_id,
            "db_path": str(db_path) if db_path else None,
        }

    try:
        with sqlite3.connect(str(db_path), timeout=20) as conn:
            symbol_master = _fetch_all(
                conn,
                """
                SELECT symbol, name_ko, name_en, sector, industry, is_active, updated_at
                FROM symbol_master
                ORDER BY is_active DESC, symbol ASC
                LIMIT 300
                """,
            ) if _table_exists(conn, "symbol_master") else []

            fundamentals = _fetch_all(
                conn,
                """
                SELECT symbol, market_cap, per, pbr, roe, tvl, active_addresses, dominance,
                       sector, industry, updated_at
                FROM fundamentals
                ORDER BY COALESCE(market_cap, 0) DESC, symbol ASC
                LIMIT 200
                """,
            ) if _table_exists(conn, "fundamentals") else []

            news_events = _fetch_all(
                conn,
                """
                SELECT id, symbol, title, summary, sentiment_score,
                       sentiment_label, sentiment_confidence, sentiment_model, risk_flags,
                       impact_level, status, source, url, published_at, created_at,
                       sector, event_type, lifecycle_stage
                FROM news_events
                ORDER BY COALESCE(published_at, created_at) DESC
                LIMIT 50
                """,
            ) if _table_exists(conn, "news_events") else []

            now_iso = datetime.now(timezone.utc).isoformat()
            scheduled_events = _fetch_all(
                conn,
                """
                SELECT id, symbol, event_type, title, status, confidence, expected_impact,
                       scheduled_at, source, details, created_at
                FROM scheduled_events
                WHERE status = 'upcoming' OR scheduled_at >= ?
                ORDER BY scheduled_at ASC
                LIMIT 30
                """,
                (now_iso,),
            ) if _table_exists(conn, "scheduled_events") else []

            market_metrics = _fetch_all(
                conn,
                """
                SELECT symbol, metric_type, value, timestamp, details
                FROM market_metrics
                ORDER BY timestamp DESC
                LIMIT 100
                """,
            ) if _table_exists(conn, "market_metrics") else []

            news_reaction_history = _fetch_all(
                conn,
                """
                SELECT news_id, market_id, window_minutes, reaction_score, confidence,
                       lag_minutes, sample_size, direction, news_published_at, computed_at, details
                FROM news_reaction_history
                ORDER BY computed_at DESC
                LIMIT 120
                """,
            ) if _table_exists(conn, "news_reaction_history") else []

            latest_updated = _latest_updated_at(conn)

            sector_counts: Dict[str, int] = {}
            for row in symbol_master:
                sector = (row.get("sector") or "UNKNOWN").strip() or "UNKNOWN"
                sector_counts[sector] = sector_counts.get(sector, 0) + 1

            summary = {
                "market_id": market_id,
                "db_path": str(db_path),
                "updated_at": latest_updated,
                "counts": {
                    "symbol_master": len(symbol_master),
                    "fundamentals": len(fundamentals),
                    "news_events": len(news_events),
                    "scheduled_events": len(scheduled_events),
                    "market_metrics": len(market_metrics),
                    "news_reaction_history": len(news_reaction_history),
                },
                "sectors": dict(sorted(sector_counts.items(), key=lambda x: x[1], reverse=True)[:20]),
                "symbol_master": symbol_master,
                "fundamentals": fundamentals,
                "news_events": news_events,
                "scheduled_events": scheduled_events,
                "market_metrics": market_metrics,
                "news_reaction_history": news_reaction_history,
            }
            summary["_llm_summary"] = _to_llm_summary(summary)
            explanation = build_resource_explanation(
                market=market_id,
                entity_type="market",
                explanation_type="mcp_external_summary",
                as_of_time=latest_updated or datetime.now(timezone.utc).isoformat(),
                headline=f"{market_id} external context snapshot",
                why_text=summary["_llm_summary"],
                bullet_points=[
                    f"news_events={len(news_events)}",
                    f"scheduled_events={len(scheduled_events)}",
                    f"market_metrics={len(market_metrics)}",
                ],
                market_context_refs={
                    "counts": summary["counts"],
                    "updated_at": latest_updated,
                },
                note="derived from external_context MCP resource",
            )
            return with_explanation_contract(
                summary,
                resource_type="external_summary",
                explanation=explanation,
            )
    except Exception as e:
        logger.error("Failed to load external summary for %s: %s", market_id, e)
        return {"error": str(e), "market_id": market_id}


def _load_external_symbol(market_id: str, symbol: str) -> Dict[str, Any]:
    """Load external context for a specific symbol within a market."""
    base = _load_external_summary(market_id)
    if base.get("error"):
        return base
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"error": "symbol is required", "market_id": market_id}

    def _match(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for row in rows:
            row_sym = (row.get("symbol") or "").strip().upper()
            if row_sym == sym:
                out.append(row)
        return out

    result = {
        "market_id": market_id,
        "symbol": sym,
        "updated_at": base.get("updated_at"),
        "symbol_master": _match(base.get("symbol_master", [])),
        "fundamentals": _match(base.get("fundamentals", [])),
        "news_events": _match(base.get("news_events", [])),
        "scheduled_events": _match(base.get("scheduled_events", [])),
        "market_metrics": _match(base.get("market_metrics", [])),
    }
    result["_llm_summary"] = _to_llm_symbol_summary(result)
    explanation = build_resource_explanation(
        market=market_id,
        symbol=sym,
        entity_type="symbol",
        explanation_type="mcp_external_symbol",
        as_of_time=result.get("updated_at") or datetime.now(timezone.utc).isoformat(),
        headline=f"{sym} external context snapshot",
        why_text=result["_llm_summary"],
        bullet_points=[
            f"fundamentals={len(result.get('fundamentals', []))}",
            f"news_events={len(result.get('news_events', []))}",
            f"scheduled_events={len(result.get('scheduled_events', []))}",
        ],
        market_context_refs={
            "updated_at": result.get("updated_at"),
        },
        note="derived from external symbol MCP resource",
    )
    return with_explanation_contract(
        result,
        resource_type="external_symbol",
        explanation=explanation,
    )


def _to_llm_summary(data: Dict[str, Any]) -> str:
    """Convert external context summary to LLM-friendly text."""
    counts = data.get("counts", {})
    sectors = data.get("sectors", {})
    top_sectors = ", ".join([f"{k}:{v}" for k, v in list(sectors.items())[:5]]) if sectors else "N/A"

    lines = [
        f"[{data.get('market_id')} external_context]",
        f"- symbols: {counts.get('symbol_master', 0)}",
        f"- fundamentals: {counts.get('fundamentals', 0)}",
        f"- confirmed_news: {counts.get('news_events', 0)}",
        f"- upcoming_events: {counts.get('scheduled_events', 0)}",
        f"- news_reactions: {counts.get('news_reaction_history', 0)}",
        f"- top_sectors: {top_sectors}",
        f"- updated_at: {data.get('updated_at')}",
    ]

    news = data.get("news_events") or []
    top_news = [n for n in news if n.get("title")][:5]
    if top_news:
        lines.append("- Recent news:")
        for n in top_news:
            title = (n.get("title") or "")[:80]
            score = n.get("sentiment_score")
            label = n.get("sentiment_label", "")
            model = n.get("sentiment_model", "")
            impact = n.get("impact_level", "")
            risk_raw = n.get("risk_flags", "")
            tag = f" [{impact}]" if impact else ""
            score_tag = ""
            if isinstance(score, (int, float)):
                model_tag = f" [{model}]" if model and model != "keyword_fallback" else ""
                score_tag = f" ({label}={score:+.2f}){model_tag}"
            risk_tag = ""
            if risk_raw and risk_raw not in ("[]", "null", "", None):
                risk_tag = f" ⚠{risk_raw}"
            lines.append(f"  * {title}{tag}{score_tag}{risk_tag}")

    events = data.get("scheduled_events") or []
    upcoming = [e for e in events if e.get("title") and e.get("status") == "upcoming"][:3]
    if upcoming:
        lines.append("- Upcoming events:")
        for e in upcoming:
            title = (e.get("title") or "")[:60]
            scheduled = e.get("scheduled_at", "")
            date_tag = f" ({scheduled[:10]})" if scheduled else ""
            lines.append(f"  * {title}{date_tag}")

    return "\n".join(lines)


def _to_llm_symbol_summary(data: Dict[str, Any]) -> str:
    """Convert symbol-level external context to LLM-friendly text."""
    f_count = len(data.get("fundamentals", []))
    n_count = len(data.get("news_events", []))
    e_count = len(data.get("scheduled_events", []))
    sm = data.get("symbol_master", [])
    name = ""
    if sm:
        name = sm[0].get("name_ko") or sm[0].get("name_en") or ""
    return (
        f"[{data.get('market_id')}:{data.get('symbol')} external]\n"
        f"- name: {name or data.get('symbol')}\n"
        f"- fundamentals_rows: {f_count}\n"
        f"- confirmed_news_rows: {n_count}\n"
        f"- upcoming_events_rows: {e_count}"
    )


def _load_causality_summary(market_id: str) -> Dict[str, Any]:
    """Load news-market causality analysis results."""
    # Query causality analysis results for the market from the news DB
    news_db_path = get_external_db_path("news")
    if not news_db_path or not news_db_path.exists():
        return {"error": "news DB not found", "market_id": market_id}

    try:
        with sqlite3.connect(str(news_db_path), timeout=10) as conn:
            if not _table_exists(conn, "news_causality_analysis"):
                return {"market_id": market_id, "causality": [], "type_distribution": {}, "message": "no data yet"}

            # Recent analysis results
            causality_rows = _fetch_all(
                conn,
                """
                SELECT nca.news_id, nca.market_id, nca.news_type,
                       nca.anticipation_ratio, nca.lead_time_minutes,
                       nca.precursor_score,
                       nca.precursor_global_regime, nca.precursor_etf_flow, nca.precursor_stock_signal,
                       nca.cascade_macro_lead_min, nca.cascade_sector_lead_min, nca.cascade_stock_lead_min,
                       nca.is_calendar_event, nca.confidence, nca.sample_size,
                       nca.pre_240m_delta, nca.pre_1440m_delta,
                       nca.post_5m_delta, nca.post_30m_delta, nca.post_240m_delta,
                       nca.computed_at,
                       ne.title AS news_title, ne.event_type AS news_event_type,
                       ne.published_at
                FROM news_causality_analysis nca
                LEFT JOIN news_events ne ON nca.news_id = ne.id
                WHERE nca.market_id = ?
                ORDER BY nca.computed_at DESC
                LIMIT 30
                """,
                (market_id,),
            )

            # Type distribution
            type_dist = _fetch_all(
                conn,
                """
                SELECT news_type, COUNT(*) as cnt,
                       AVG(anticipation_ratio) as avg_ratio,
                       AVG(precursor_score) as avg_precursor
                FROM news_causality_analysis
                WHERE market_id = ?
                GROUP BY news_type
                """,
                (market_id,),
            )

            type_distribution = {}
            for row in type_dist:
                nt = row.get("news_type", "unknown")
                type_distribution[nt] = {
                    "count": row.get("cnt", 0),
                    "avg_anticipation_ratio": round(float(row.get("avg_ratio") or 0), 3),
                    "avg_precursor_score": round(float(row.get("avg_precursor") or 0), 3),
                }

        result = {
            "market_id": market_id,
            "total_analyses": len(causality_rows),
            "type_distribution": type_distribution,
            "causality": causality_rows,
        }
        result["_llm_summary"] = _to_causality_llm_summary(result)

        explanation = build_resource_explanation(
            market=market_id,
            entity_type="market",
            explanation_type="mcp_causality_summary",
            as_of_time=causality_rows[0]["computed_at"] if causality_rows else datetime.now(timezone.utc).isoformat(),
            headline=f"{market_id} news-market causality analysis",
            why_text=result["_llm_summary"],
            bullet_points=[
                f"total={len(causality_rows)}",
                f"types={list(type_distribution.keys())}",
            ],
            market_context_refs={"type_distribution": type_distribution},
            note="derived from news_causality_analysis",
        )
        return with_explanation_contract(
            result,
            resource_type="causality_summary",
            explanation=explanation,
        )
    except Exception as e:
        logger.error("Failed to load causality summary for %s: %s", market_id, e)
        return {"error": str(e), "market_id": market_id}


def _to_causality_llm_summary(data: Dict[str, Any]) -> str:
    """Convert causality analysis results to LLM-friendly text."""
    market_id = data.get("market_id", "?")
    type_dist = data.get("type_distribution", {})
    causality = data.get("causality", [])

    lines = [f"[{market_id} News-market causality]"]

    # Type distribution
    if type_dist:
        lines.append("- News type distribution:")
        for nt, info in type_dist.items():
            cnt = info.get("count", 0)
            ratio = info.get("avg_anticipation_ratio", 0)
            label = {"anticipated": "anticipated", "surprise": "surprise", "surprise_with_precursor": "surprise_with_precursor"}.get(nt, nt)
            lines.append(f"  * {label}: {cnt} items (anticipation_ratio={ratio:.2f})")

    # Recent analyses (top 5)
    recent = [c for c in causality if c.get("news_title")][:5]
    if recent:
        lines.append("- Recent analyses:")
        for c in recent:
            title = (c.get("news_title") or "")[:60]
            nt = c.get("news_type", "?")
            conf = c.get("confidence", 0)
            pre_score = c.get("precursor_score", 0)
            tag = f" [precursor={pre_score:.1f}]" if nt == "surprise_with_precursor" else ""
            lines.append(f"  * [{nt}] {title} (confidence={conf:.2f}){tag}")

    return "\n".join(lines)


def _load_macro_events() -> Dict[str, Any]:
    """Load active macro event lifecycle list."""
    news_db_path = get_external_db_path("news")
    if not news_db_path or not news_db_path.exists():
        return {"error": "news DB not found", "events": []}

    try:
        with sqlite3.connect(str(news_db_path), timeout=10) as conn:
            if not _table_exists(conn, "macro_event_narratives"):
                return {"events": [], "message": "macro_event_narratives table not found"}

            # Active events (excluding RESOLVED)
            events = _fetch_all(
                conn,
                """
                SELECT event_id, title, title_en, category, lifecycle_state,
                       current_sensitivity, peak_sensitivity,
                       key_entities, keywords, macro_categories,
                       affected_markets, escalation_count,
                       article_count, cluster_count,
                       first_detected_at, last_development_at, resolved_at,
                       half_life_hours, updated_at
                FROM macro_event_narratives
                ORDER BY
                    CASE lifecycle_state
                        WHEN 'RESOLVED' THEN 1 ELSE 0
                    END,
                    current_sensitivity DESC,
                    last_development_at DESC
                LIMIT 20
                """,
            )

            # Recent developments for each event
            for evt in events:
                event_id = evt.get("event_id", "")
                developments = _fetch_all(
                    conn,
                    """
                    SELECT timestamp, from_state, to_state,
                           sensitivity_before, sensitivity_after,
                           description, trigger_type
                    FROM macro_event_developments
                    WHERE event_id = ?
                    ORDER BY timestamp DESC
                    LIMIT 5
                    """,
                    (event_id,),
                )
                evt["recent_developments"] = developments

                # Latest sensitivity per market
                market_impact = _fetch_all(
                    conn,
                    """
                    SELECT market_id, sensitivity, snapshot_at
                    FROM macro_event_market_impact
                    WHERE event_id = ?
                    ORDER BY snapshot_at DESC
                    LIMIT 9
                    """,
                    (event_id,),
                )
                # Keep only the latest per market
                seen = {}
                for mi in market_impact:
                    mid = mi.get("market_id", "")
                    if mid not in seen:
                        seen[mid] = mi
                evt["market_sensitivities"] = seen

            active_count = sum(1 for e in events if e.get("lifecycle_state") != "RESOLVED")
            result = {
                "total_events": len(events),
                "active_events": active_count,
                "events": events,
            }
            result["_llm_summary"] = _to_macro_events_llm_summary(result)

            explanation = build_resource_explanation(
                entity_type="system",
                explanation_type="mcp_macro_events",
                as_of_time=datetime.now(timezone.utc).isoformat(),
                headline="Macro event lifecycle status",
                why_text=result["_llm_summary"],
                bullet_points=[
                    f"active={active_count}",
                    f"total={len(events)}",
                ],
                market_context_refs={},
                note="derived from macro_event_narratives",
            )
            return with_explanation_contract(
                result,
                resource_type="macro_events",
                explanation=explanation,
            )
    except Exception as e:
        logger.error("Failed to load macro events: %s", e)
        return {"error": str(e)}


def _to_macro_events_llm_summary(data: Dict[str, Any]) -> str:
    """Convert macro events to LLM-friendly text."""
    events = data.get("events", [])
    if not events:
        return "[Macro events] No active events"

    lines = [f"[Macro events status] active={data.get('active_events', 0)}"]

    # State label mapping
    state_label_map = {
        "EMERGING": "Detected",
        "ESCALATING": "Escalating",
        "PEAK_IMPACT": "Peak Impact",
        "ADAPTING": "Adapting",
        "DORMANT": "Dormant",
        "RE_ESCALATION": "Re-escalation",
        "RESOLVING": "Resolving",
        "RESOLVED": "Resolved",
    }

    for evt in events[:5]:
        title = (evt.get("title") or "")[:50]
        state = evt.get("lifecycle_state", "?")
        state_label = state_label_map.get(state, state)
        sensitivity = evt.get("current_sensitivity", 0)
        esc_count = evt.get("escalation_count", 0)
        category = evt.get("category", "")

        line = f"- [{state_label}] {title} (sensitivity={sensitivity:.2f}, category={category}"
        if esc_count > 0:
            line += f", re-escalations={esc_count}"
        line += ")"
        lines.append(line)

        # Per-market sensitivity
        ms = evt.get("market_sensitivities", {})
        if ms:
            parts = []
            for mid, info in ms.items():
                s = info.get("sensitivity", 0)
                short = mid.replace("_market", "")
                parts.append(f"{short}={s:.2f}")
            lines.append(f"  By market: {', '.join(parts)}")

    return "\n".join(lines)


def register_external_context_resources(mcp, cache):
    """Register external_context resources."""

    @mcp.resource("market://{market_id}/external/summary")
    def get_external_summary(market_id: str) -> Dict[str, Any]:
        cache_key = f"external_summary_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = _load_external_summary(market_id)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/external/symbol/{symbol}")
    def get_external_symbol(market_id: str, symbol: str) -> Dict[str, Any]:
        cache_key = f"external_symbol_{market_id}_{symbol}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = _load_external_symbol(market_id, symbol)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/external/causality")
    def get_causality_summary(market_id: str) -> Dict[str, Any]:
        """News-market causality analysis results (3-type classification)."""
        cache_key = f"causality_summary_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = _load_causality_summary(market_id)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://global/macro_events")
    def get_macro_events() -> Dict[str, Any]:
        """Active macro event lifecycle status (wars, financial crises, and other long-running events)."""
        cache_key = "macro_events_global"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = _load_macro_events()
        cache.set(cache_key, data)
        return to_resource_text(data)

    logger.info("  External Context resources registered")
