# -*- coding: utf-8 -*-
"""
External Context Resources
==========================
external_context DB(시장별 외부 데이터) 조회 Resource.
"""

from __future__ import annotations

import asyncio
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from mcps.config import CACHE_TTL_MARKET_STATUS, get_external_db_path
from mcps.resources.resource_response import (
    build_resource_explanation,
    to_resource_text,
    with_explanation_contract,
    mcp_error,
    MCPErrorCode,
    MCPErrorAction,
    wrap_with_ai_summary,
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
    db_path = get_external_db_path(market_id)
    if not db_path or not db_path.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"external_context DB not found for market: {market_id}",
            action=MCPErrorAction.CHECK,
            action_value=f"market://{market_id}/status",
            market_id=market_id,
            db_path=str(db_path) if db_path else None,
        )

    try:
        with sqlite3.connect(str(db_path), timeout=20) as conn:
            symbol_master = _fetch_all(
                conn,
                """
                SELECT symbol, name_ko, name_en, sector, industry, is_active, updated_at
                FROM symbol_master
                WHERE is_active = 1
                ORDER BY symbol ASC
                LIMIT 100
                """,
            ) if _table_exists(conn, "symbol_master") else []

            fundamentals = _fetch_all(
                conn,
                """
                SELECT symbol, market_cap, per, pbr, roe, tvl, active_addresses, dominance,
                       sector, industry, updated_at
                FROM fundamentals
                ORDER BY COALESCE(market_cap, 0) DESC, symbol ASC
                LIMIT 50
                """,
            ) if _table_exists(conn, "fundamentals") else []

            news_events = _fetch_all(
                conn,
                """
                SELECT id, symbol, title, sentiment_score,
                       sentiment_label, sentiment_confidence, sentiment_model, risk_flags,
                       impact_level, status, source, published_at,
                       sector, event_type, lifecycle_stage
                FROM news_events
                ORDER BY COALESCE(published_at, created_at) DESC
                LIMIT 30
                """,
            ) if _table_exists(conn, "news_events") else []

            now_iso = datetime.now(timezone.utc).isoformat()
            scheduled_events = _fetch_all(
                conn,
                """
                SELECT id, symbol, event_type, title, status, confidence, expected_impact,
                       scheduled_at, source, created_at
                FROM scheduled_events
                WHERE status = 'upcoming' OR scheduled_at >= ?
                ORDER BY scheduled_at ASC
                LIMIT 20
                """,
                (now_iso,),
            ) if _table_exists(conn, "scheduled_events") else []

            market_metrics = _fetch_all(
                conn,
                """
                SELECT symbol, metric_type, value, timestamp
                FROM market_metrics
                ORDER BY timestamp DESC
                LIMIT 50
                """,
            ) if _table_exists(conn, "market_metrics") else []

            news_reaction_history = _fetch_all(
                conn,
                """
                SELECT news_id, market_id, window_minutes, reaction_score, confidence,
                       lag_minutes, sample_size, direction, news_published_at, computed_at
                FROM news_reaction_history
                ORDER BY computed_at DESC
                LIMIT 30
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
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            str(e),
            action=MCPErrorAction.RETRY,
            action_value="30",
            market_id=market_id,
        )


def _load_external_symbol(market_id: str, symbol: str) -> Dict[str, Any]:
    base = _load_external_summary(market_id)
    if base.get("error"):
        return base
    sym = (symbol or "").strip().upper()
    if not sym:
        return mcp_error(
            MCPErrorCode.SYMBOL_NOT_FOUND,
            "symbol is required",
            action=MCPErrorAction.CHECK,
            market_id=market_id,
        )

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
        lines.append("- 최근 뉴스:")
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
        lines.append("- 예정 이벤트:")
        for e in upcoming:
            title = (e.get("title") or "")[:60]
            scheduled = e.get("scheduled_at", "")
            date_tag = f" ({scheduled[:10]})" if scheduled else ""
            lines.append(f"  * {title}{date_tag}")

    return "\n".join(lines)


def _to_llm_symbol_summary(data: Dict[str, Any]) -> str:
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
    """뉴스-시장 인과관계 분석 결과를 로드한다."""
    # news DB에서 해당 마켓의 causality 분석 결과 조회
    news_db_path = get_external_db_path("news")
    if not news_db_path or not news_db_path.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            "news DB not found",
            action=MCPErrorAction.CHECK,
            market_id=market_id,
        )

    try:
        with sqlite3.connect(str(news_db_path), timeout=10) as conn:
            if not _table_exists(conn, "news_causality_analysis"):
                return mcp_error(
                    MCPErrorCode.NO_DATA,
                    "news_causality_analysis 테이블 없음 — 데이터 축적 필요",
                    action=MCPErrorAction.CHECK,
                    market_id=market_id,
                )

            # 최근 분석 결과
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

            # 타입 분포
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
            headline=f"{market_id} 뉴스-시장 인과관계 분석",
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
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            str(e),
            action=MCPErrorAction.RETRY,
            action_value="30",
            market_id=market_id,
        )


def _to_causality_llm_summary(data: Dict[str, Any]) -> str:
    """인과관계 분석 결과를 LLM 친화적 텍스트로 변환."""
    market_id = data.get("market_id", "?")
    type_dist = data.get("type_distribution", {})
    causality = data.get("causality", [])

    lines = [f"[{market_id} 뉴스-시장 인과관계]"]

    # 타입 분포
    if type_dist:
        lines.append("- 뉴스 타입 분포:")
        for nt, info in type_dist.items():
            cnt = info.get("count", 0)
            ratio = info.get("avg_anticipation_ratio", 0)
            label = {"anticipated": "예견", "surprise": "서프라이즈", "surprise_with_precursor": "전조있는 서프라이즈"}.get(nt, nt)
            lines.append(f"  * {label}: {cnt}건 (선행비율={ratio:.2f})")

    # 최근 분석 (상위 5건)
    recent = [c for c in causality if c.get("news_title")][:5]
    if recent:
        lines.append("- 최근 분석:")
        for c in recent:
            title = (c.get("news_title") or "")[:60]
            nt = c.get("news_type", "?")
            conf = c.get("confidence", 0)
            pre_score = c.get("precursor_score", 0)
            tag = f" [전조={pre_score:.1f}]" if nt == "surprise_with_precursor" else ""
            lines.append(f"  * [{nt}] {title} (신뢰={conf:.2f}){tag}")

    return "\n".join(lines)


def _load_macro_events() -> Dict[str, Any]:
    """활성 매크로 이벤트 라이프사이클 목록 로드."""
    news_db_path = get_external_db_path("news")
    if not news_db_path or not news_db_path.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            "news DB not found",
            action=MCPErrorAction.CHECK,
        )

    try:
        with sqlite3.connect(str(news_db_path), timeout=10) as conn:
            if not _table_exists(conn, "macro_event_narratives"):
                return mcp_error(
                    MCPErrorCode.NO_DATA,
                    "macro_event_narratives 테이블 없음 — 이벤트 데이터 축적 필요",
                    action=MCPErrorAction.CHECK,
                )

            # 활성 이벤트 (RESOLVED 제외)
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

            # 각 이벤트의 최근 전개
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

                # 시장별 최신 민감도
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
                # 시장별 최신만
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
                headline="매크로 이벤트 라이프사이클 현황",
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
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            str(e),
            action=MCPErrorAction.RETRY,
            action_value="30",
        )


def _to_macro_events_llm_summary(data: Dict[str, Any]) -> str:
    """매크로 이벤트를 LLM 친화적 텍스트로 변환."""
    events = data.get("events", [])
    if not events:
        return "[매크로 이벤트] 활성 이벤트 없음"

    lines = [f"[매크로 이벤트 현황] 활성={data.get('active_events', 0)}개"]

    # 상태 한국어 매핑
    state_ko = {
        "EMERGING": "감지됨",
        "ESCALATING": "확대중",
        "PEAK_IMPACT": "최고영향",
        "ADAPTING": "시장적응중",
        "DORMANT": "소강상태",
        "RE_ESCALATION": "재충격",
        "RESOLVING": "해결중",
        "RESOLVED": "해결됨",
    }

    for evt in events[:5]:
        title = (evt.get("title") or "")[:50]
        state = evt.get("lifecycle_state", "?")
        state_label = state_ko.get(state, state)
        sensitivity = evt.get("current_sensitivity", 0)
        esc_count = evt.get("escalation_count", 0)
        category = evt.get("category", "")

        line = f"- [{state_label}] {title} (민감도={sensitivity:.2f}, 카테고리={category}"
        if esc_count > 0:
            line += f", 재충격={esc_count}회"
        line += ")"
        lines.append(line)

        # 시장별 민감도
        ms = evt.get("market_sensitivities", {})
        if ms:
            parts = []
            for mid, info in ms.items():
                s = info.get("sensitivity", 0)
                short = mid.replace("_market", "")
                parts.append(f"{short}={s:.2f}")
            lines.append(f"  시장별: {', '.join(parts)}")

    return "\n".join(lines)


def _ai_summary_external(data: dict) -> str:
    counts = data.get("counts", {})
    news = counts.get("news_events", 0)
    fund = counts.get("fundamentals", 0)
    events = counts.get("scheduled_events", 0)
    market_id = data.get("market_id", "")
    news_list = data.get("news_events", [])
    headline = ""
    if news_list:
        high_impact = [n for n in news_list if n.get("impact_level") == "high"]
        if high_impact:
            headline = f" 고영향: {high_impact[0].get('title', '')[:30]}."
    return f"{market_id} 외부: 뉴스 {news}건, 펀더멘탈 {fund}건, 예정이벤트 {events}건.{headline}"


def register_external_context_resources(mcp, cache):
    """external_context Resource 등록."""

    @mcp.resource("market://{market_id}/external/summary")
    async def get_external_summary(market_id: str) -> Dict[str, Any]:
        """[역할] 시장별 외부 컨텍스트(뉴스, 펀더멘탈, 예정이벤트, 메트릭) 요약. [호출 시점] 기술적 분석 외 뉴스/이벤트 영향 파악 시. [선행 조건] 없음. [후속 추천] market://{market_id}/external/symbol/{symbol}, market://{market_id}/external/causality. [주의] TTL=60초. [출력 스키마] ai_summary 래핑. full_data: market_id(str), counts{symbol_master,fundamentals,news_events,scheduled_events,market_metrics,news_reaction_history}, sectors{sector→count}, news_events[{title,sentiment_score,sentiment_label,impact_level,source}], scheduled_events[{event_type,title,status,scheduled_at}], _contract{...}, _llm_summary(str)."""
        cache_key = f"external_summary_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = await asyncio.to_thread(_load_external_summary, market_id)
        if not data.get("error"):
            data = wrap_with_ai_summary(data, "external_summary", _ai_summary_external)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/external/symbol/{symbol}")
    async def get_external_symbol(market_id: str, symbol: str) -> Dict[str, Any]:
        """[역할] 특정 종목의 펀더멘탈/뉴스/예정이벤트. [호출 시점] 특정 종목 외부 요인 분석 시. [선행 조건] external/summary 권장. [후속 추천] get_signal_detail과 교차 분석, market://{market_id}/unified/symbol/{symbol}. [주의] 종목별 데이터 없을 수 있음. [출력 스키마] _contract 포함. market_id(str), symbol(str), symbol_master[{name_ko,sector,industry}], fundamentals[{market_cap,per,pbr}], news_events[{title,sentiment_score,impact_level}], scheduled_events[{event_type,status}], _llm_summary(str)."""
        cache_key = f"external_symbol_{market_id}_{symbol}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = await asyncio.to_thread(_load_external_symbol, market_id, symbol)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/external/causality")
    async def get_causality_summary(market_id: str) -> Dict[str, Any]:
        """[역할] 뉴스-시장 인과관계 분석(예상/서프라이즈/전조서프라이즈 3분류). [호출 시점] 뉴스의 시장 인과관계 분석 시. [선행 조건] external/summary 후. [후속 추천] market://derived/event-leading, market://derived/reaction-speed. [주의] 데이터 축적 필요. [출력 스키마] _contract 포함. market_id(str), total_analyses(int), type_distribution{news_type→{count,avg_anticipation_ratio,avg_precursor_score}}, causality[{news_type,anticipation_ratio,lead_time_minutes,precursor_score,confidence,title,published_at}], _llm_summary(str)."""
        cache_key = f"causality_summary_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = await asyncio.to_thread(_load_causality_summary, market_id)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://global/macro_events")
    async def get_macro_events() -> Dict[str, Any]:
        """[역할] 활성 매크로 이벤트의 라이프사이클 상태(전쟁/금융위기 등 장기 이벤트). [호출 시점] 진행 중인 글로벌 매크로 이벤트 확인 시. [선행 조건] market://global/summary 권장. [후속 추천] market://unified/cross-market. [주의] 이벤트 없으면 빈 배열. [출력 스키마] _contract 포함. total_events(int), active_events(int), events[{event_id,title,category,lifecycle_state,current_sensitivity,peak_sensitivity,affected_markets,recent_developments[],market_sensitivities{}}], _llm_summary(str)."""
        cache_key = "macro_events_global"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = await asyncio.to_thread(_load_macro_events)
        cache.set(cache_key, data)
        return to_resource_text(data)

    logger.info("  🌍 External Context resources registered")

