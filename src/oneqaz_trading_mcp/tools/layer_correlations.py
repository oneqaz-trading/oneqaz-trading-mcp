# -*- coding: utf-8 -*-
"""
Layer-Internal Correlations Tools
=================================
agent_history Stage 2 산출물 (sector/macro/peer 학습기) 의 결과를
B2AI 트러스트 레이어 도구로 노출한다.

테이블:
- agent_history.sector_correlation_matrix : ETF/그룹 pair correlation + cluster_id
- agent_history.sector_clusters           : 클러스터 메타 (members, intra_corr)
- agent_history.macro_causality_graph     : 거시 카테고리 lag-aware causality
- agent_history.symbol_peer_links         : 종목 lead-lag

각 도구는 ai_summary 래핑 + full_data 패턴 사용.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.resources.resource_response import wrap_tool_response
# Phase B (2026-05-07): module-level 함수에서 호출. lazy import 로 circular 회피.
def _next_actions_imports():
    from oneqaz_trading_mcp.tools.next_actions import (
        for_sector_correlations, for_macro_causality_graph,
        for_symbol_peer_links, for_feature_governance_status_tool,
    )
    return {
        "sector": for_sector_correlations,
        "macro_causality": for_macro_causality_graph,
        "peer_links": for_symbol_peer_links,
        "governance_status": for_feature_governance_status_tool,
    }
# Phase D-E (2026-05-07): user-facing followup questions.
def _followup_imports():
    from oneqaz_trading_mcp.tools.followup_questions import (
        fq_sector_correlations, fq_macro_causality_graph,
        fq_symbol_peer_links, fq_feature_governance_status_tool,
    )
    return {
        "sector": fq_sector_correlations,
        "macro_causality": fq_macro_causality_graph,
        "peer_links": fq_symbol_peer_links,
        "governance_status": fq_feature_governance_status_tool,
    }

logger = logging.getLogger("MarketMCP")

_VALID_MARKETS = ("coin", "kr_stock", "us_stock")


def _open_pg():
    """agent_history 연결 (PG 직결, read-only 분석 도구).

    [2026-07-06] AWS 배포 제거 완료 — SQLite fallback 경로 hint
    (layer_correlations.db export) 삭제. 홈 PG 단일 경로.
    """
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    return open_schema_connection("agent_history", readonly=True)


# ─── Sector / ETF Co-movement ────────────────────────────────────────

async def get_sector_correlations(market_id: str = "us_stock",
                                  top_k: int = 20) -> Dict[str, Any]:
    """[역할] 시장 내 ETF/그룹 상관 행렬 + 클러스터.
    [호출 시점] 분산 매매 / 비슷한 그룹 회피 결정 시.
    [선행 조건] 없음.
    [후속 추천] get_symbol_peer_links (특정 그룹 내 종목별 lead-lag).
    [주의] 데이터는 6시간마다 갱신. 60일 lookback 기본.
    [출력 스키마] full_data: market_id, top_pairs[{a,b,corr,cluster_id,n}],
                  clusters[{cluster_id, size, intra_corr, members}], total_pairs(int)."""
    if market_id not in _VALID_MARKETS:
        return wrap_tool_response(
            {"error": f"invalid_market:{market_id}"},
            ai_summary=f"market_id 는 {_VALID_MARKETS} 중 하나",
        )

    pg = _open_pg()
    try:
        # top pairs
        rows = pg.execute(
            """
            SELECT symbol_a, symbol_b, correlation, sample_size, cluster_id
            FROM sector_correlation_matrix
            WHERE market = ?
            ORDER BY ABS(correlation) DESC
            LIMIT ?
            """,
            (market_id, top_k),
        ).fetchall()
        pairs = [{
            "a": r["symbol_a"], "b": r["symbol_b"],
            "corr": float(r["correlation"]),
            "n": int(r["sample_size"] or 0),
            "cluster_id": r["cluster_id"],
        } for r in rows]

        # clusters
        cluster_rows = pg.execute(
            """
            SELECT cluster_id, member_count, avg_intra_corr, members
            FROM sector_clusters
            WHERE market = ?
            ORDER BY member_count DESC
            LIMIT 10
            """,
            (market_id,),
        ).fetchall()
        clusters = []
        for r in cluster_rows:
            members_raw = r["members"]
            if isinstance(members_raw, list):
                members = members_raw
            else:
                import json
                try:
                    members = json.loads(members_raw) if members_raw else []
                except Exception:
                    members = []
            clusters.append({
                "cluster_id": int(r["cluster_id"]),
                "size": int(r["member_count"] or 0),
                "intra_corr": float(r["avg_intra_corr"] or 0.0),
                "members": members[:15],
            })

        # 총 pair 수
        total_row = pg.execute(
            "SELECT COUNT(*) AS c FROM sector_correlation_matrix WHERE market = ?",
            (market_id,),
        ).fetchall()
        total_pairs = int(total_row[0]["c"] or 0) if total_row else 0
    finally:
        try:
            pg.close()
        except Exception:
            pass

    summary_bits = [f"{market_id} sector graph: {total_pairs} pairs"]
    if pairs:
        top = pairs[0]
        summary_bits.append(f"strongest {top['a']}↔{top['b']} ρ={top['corr']:.2f}")
    if clusters:
        summary_bits.append(f"{len(clusters)} clusters (largest size={clusters[0]['size']})")

    _summary = " / ".join(summary_bits)
    return wrap_tool_response(
        {
            "market_id": market_id,
            "top_pairs": pairs,
            "clusters": clusters,
            "total_pairs": total_pairs,
        },
        "trust_layer",
        lambda _d: _summary,
        lambda _d: f"{market_id} 섹터 그래프 ({total_pairs}쌍)",
        next_actions_fn=_next_actions_imports()["sector"],
        followup_questions_fn=_followup_imports()["sector"],
    )


# ─── Macro-Macro Causality ────────────────────────────────────────────

async def get_macro_causality_graph(min_abs_corr: float = 0.15,
                                    max_p_value: float = 0.05) -> Dict[str, Any]:
    """[역할] 거시 카테고리 (bonds/vix/forex/credit/inflation/liquidity) 간 lag-aware 인과.
    [호출 시점] 거시 사건 발생 시 다른 카테고리에 미치는 영향 사전 평가.
    [선행 조건] 없음.
    [후속 추천] get_macro_influence_map (시장 영향).
    [주의] Pearson 기반 상관. p-value < 0.05, |corr| >= 0.15 이 의미있는 신호.
    [출력 스키마] full_data: edges[{cause,effect,lag_days,corr,p,n}],
                  total_edges, significant_count."""
    pg = _open_pg()
    try:
        rows = pg.execute(
            """
            SELECT cause_category, effect_category, lag_days,
                   correlation, p_value, sample_size
            FROM macro_causality_graph
            WHERE ABS(correlation) >= ? AND (p_value IS NULL OR p_value < ?)
            ORDER BY ABS(correlation) DESC
            LIMIT 50
            """,
            (min_abs_corr, max_p_value),
        ).fetchall()
        edges = [{
            "cause": r["cause_category"],
            "effect": r["effect_category"],
            "lag_days": int(r["lag_days"]),
            "corr": float(r["correlation"]),
            "p": float(r["p_value"]) if r["p_value"] is not None else None,
            "n": int(r["sample_size"] or 0),
        } for r in rows]

        total_row = pg.execute(
            "SELECT COUNT(*) AS c FROM macro_causality_graph"
        ).fetchall()
        total = int(total_row[0]["c"] or 0) if total_row else 0
    finally:
        try:
            pg.close()
        except Exception:
            pass

    summary_bits = [f"macro causality: {total} edges, {len(edges)} significant"]
    if edges:
        top = edges[0]
        summary_bits.append(
            f"top {top['cause']}→{top['effect']} lag={top['lag_days']}d ρ={top['corr']:+.2f}"
        )

    _summary = " / ".join(summary_bits)
    return wrap_tool_response(
        {"edges": edges, "total_edges": total, "significant_count": len(edges)},
        "trust_layer",
        lambda _d: _summary,
        lambda _d: f"거시 인과 그래프 ({len(edges)}개 유의)",
        next_actions_fn=_next_actions_imports()["macro_causality"],
        followup_questions_fn=_followup_imports()["macro_causality"],
    )


# ─── Symbol Peer Lead-Lag ─────────────────────────────────────────────

async def get_symbol_peer_links(market_id: str = "us_stock",
                                symbol: Optional[str] = None,
                                top_k: int = 20) -> Dict[str, Any]:
    """[역할] 종목 간 lead-lag 관계. symbol 지정 시 그 종목이 lead 또는 follow 인 peer 만.
    [호출 시점] 매매 결정 시 peer 의 선행 신호 활용.
    [선행 조건] symbol 은 옵션. 없으면 시장 전체 top.
    [후속 추천] get_signal_detail (peer 의 시그널 세부).
    [주의] 14일 lookback, 15분 봉 기반. lag 는 분 단위.
    [출력 스키마] full_data: market_id, focus_symbol(str?), as_lead[{follow,lag_min,corr,n}],
                  as_follow[{lead,lag_min,corr,n}], total_links."""
    if market_id not in _VALID_MARKETS:
        return wrap_tool_response(
            {"error": f"invalid_market:{market_id}"},
            ai_summary=f"market_id 는 {_VALID_MARKETS} 중 하나",
        )

    pg = _open_pg()
    try:
        if symbol:
            sym = symbol.upper()
            lead_rows = pg.execute(
                """
                SELECT symbol_follow, lag_minutes, correlation, sample_size
                FROM symbol_peer_links
                WHERE market = ? AND symbol_lead = ?
                ORDER BY ABS(correlation) DESC LIMIT ?
                """,
                (market_id, sym, top_k),
            ).fetchall()
            follow_rows = pg.execute(
                """
                SELECT symbol_lead, lag_minutes, correlation, sample_size
                FROM symbol_peer_links
                WHERE market = ? AND symbol_follow = ?
                ORDER BY ABS(correlation) DESC LIMIT ?
                """,
                (market_id, sym, top_k),
            ).fetchall()
            as_lead = [{
                "follow": r["symbol_follow"],
                "lag_min": int(r["lag_minutes"]),
                "corr": float(r["correlation"]),
                "n": int(r["sample_size"] or 0),
            } for r in lead_rows]
            as_follow = [{
                "lead": r["symbol_lead"],
                "lag_min": int(r["lag_minutes"]),
                "corr": float(r["correlation"]),
                "n": int(r["sample_size"] or 0),
            } for r in follow_rows]
        else:
            rows = pg.execute(
                """
                SELECT symbol_lead, symbol_follow, lag_minutes, correlation, sample_size
                FROM symbol_peer_links
                WHERE market = ?
                ORDER BY ABS(correlation) DESC LIMIT ?
                """,
                (market_id, top_k),
            ).fetchall()
            # symbol 미지정 시 lead 관점으로만 표시
            as_lead = [{
                "lead": r["symbol_lead"],
                "follow": r["symbol_follow"],
                "lag_min": int(r["lag_minutes"]),
                "corr": float(r["correlation"]),
                "n": int(r["sample_size"] or 0),
            } for r in rows]
            as_follow = []

        total_row = pg.execute(
            "SELECT COUNT(*) AS c FROM symbol_peer_links WHERE market = ?",
            (market_id,),
        ).fetchall()
        total = int(total_row[0]["c"] or 0) if total_row else 0
    finally:
        try:
            pg.close()
        except Exception:
            pass

    summary_bits = [f"{market_id} peer links: {total}"]
    if symbol:
        summary_bits.append(f"focus={symbol} lead_count={len(as_lead)} follow_count={len(as_follow)}")
    elif as_lead:
        summary_bits.append(
            f"top {as_lead[0].get('lead','?')}→{as_lead[0].get('follow','?')} "
            f"lag={as_lead[0]['lag_min']}min ρ={as_lead[0]['corr']:+.2f}"
        )

    _summary = " / ".join(summary_bits)
    _human = (f"{market_id} {symbol or ''} peer 관계 ({len(as_lead)+len(as_follow)}개)").strip()
    return wrap_tool_response(
        {
            "market_id": market_id,
            "focus_symbol": symbol,
            "as_lead": as_lead,
            "as_follow": as_follow,
            "total_links": total,
        },
        "trust_layer",
        lambda _d: _summary,
        lambda _d: _human,
        next_actions_fn=_next_actions_imports()["peer_links"],
        followup_questions_fn=_followup_imports()["peer_links"],
    )


# ─── Feature Governance Status (보강) ─────────────────────────────────

async def get_feature_governance_status() -> Dict[str, Any]:
    """[역할] 피처 거버넌스 현황 (OBSERVATION → CONDITIONAL → ACTIVE 분포 + 최근 승격).
    [호출 시점] 신뢰도 평가 / 어떤 피처가 활성인지 확인 시.
    [선행 조건] 없음.
    [후속 추천] get_feature_governance_state (기존 도구, 상세 lifecycle).
    [주의] 1시간마다 자동 promoter 가 갱신.
    [출력 스키마] full_data: by_status, recent_transitions[{feature_id, old, new, evaluated_at}],
                  total."""
    pg = _open_pg()
    try:
        status_rows = pg.execute(
            "SELECT status, COUNT(*) AS c FROM feature_governance GROUP BY status"
        ).fetchall()
        by_status = {(r["status"] or "null"): int(r["c"] or 0) for r in status_rows}

        # 2026-05-08 fix: NOW() - INTERVAL '7 days' 는 PG 전용 — AWS SQLite 에서
        # syntax error. ISO string lexicographic 비교로 변환 (timestamptz / TEXT
        # 양쪽 모두 정확히 동작 — ISO 8601 의 자연 정렬 속성).
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        cutoff_iso = (_dt.now(_tz.utc) - _td(days=7)).isoformat()
        recent_rows = pg.execute(
            """
            SELECT feature_id, old_status, new_status, evaluated_at, delta_pf
            FROM feature_evaluation_log
            WHERE evaluated_at > ?
            ORDER BY evaluated_at DESC LIMIT 20
            """,
            (cutoff_iso,),
        ).fetchall()
        transitions = [{
            "feature_id": r["feature_id"],
            "old": r["old_status"],
            "new": r["new_status"],
            "evaluated_at": str(r["evaluated_at"]),
            "delta_pf": float(r["delta_pf"] or 0.0),
        } for r in recent_rows]

        total = sum(by_status.values())
    finally:
        try:
            pg.close()
        except Exception:
            pass

    active_cond = by_status.get("ACTIVE", 0) + by_status.get("CONDITIONAL", 0)
    summary = (f"governance: total={total} ACTIVE+CONDITIONAL={active_cond} "
               f"(OBS={by_status.get('OBSERVATION', 0)}, DEPRECATED={by_status.get('DEPRECATED', 0)}). "
               f"recent_transitions={len(transitions)}")

    _summary = summary
    _user = f"피처 거버넌스: 총 {total}개 (활성+조건부 {active_cond})"
    return wrap_tool_response(
        {"by_status": by_status,
         "recent_transitions": transitions,
         "total": total},
        "trust_layer",
        lambda _d: _summary,
        lambda _d: _user,
        next_actions_fn=_next_actions_imports()["governance_status"],
        followup_questions_fn=_followup_imports()["governance_status"],
    )


# ─── 등록 ────────────────────────────────────────────────────────────

def register_layer_correlation_tools(mcp, cache=None):
    """Stage 2 산출물 노출 (sector / macro / peer / governance status)."""

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_sector_correlations_tool(market_id: str = "us_stock",
                                            top_k: int = 20) -> Dict[str, Any]:
        """
        Purpose: Intra-market ETF / group correlation matrix and auto-cluster output.
            Quantifies structural co-movement (e.g. ARKK <-> QQQ) for diversification
            and sector-avoidance reasoning.
        Triggers (casual questions too): "which sectors move together?", "어떤 섹터끼리 같이 움직여?",
            "am I too concentrated?", "ETF 상관관계 보여줘", "is tech basically one trade right now?".
        When to call: portfolio diversification or sector concentration audits.
        Prerequisites: none.
        Next steps: get_symbol_peer_links_tool for per-symbol lead-lag inside a sector.
        Caveats: refreshed every 6 hours; 60-day lookback.

        Args:
            market_id: coin / kr_stock / us_stock
            top_k: Number of top pairs to return

        Disclaimer: Information only, not investment advice.
        """
        return await get_sector_correlations(market_id=market_id, top_k=top_k)

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_macro_causality_graph_tool(min_abs_corr: float = 0.15,
                                              max_p_value: float = 0.05) -> Dict[str, Any]:
        """
        Purpose: Lag-aware causal graph between macro categories
            (bonds / vix / forex / credit / inflation / liquidity / commodities).
            Returns only statistically significant lead-lag pairs
            (e.g. forex -> vix 7d rho=-0.41).
        Triggers (casual questions too): "what happens to VIX when bonds move?", "금리 오르면 뭐가 움직여?",
            "which macro leads which?", "거시 지표끼리 인과관계 있어?", "does the dollar lead volatility?".
        When to call: assess pre-emptive cross-category impact after a macro event.
        Prerequisites: none.
        Next steps: get_macro_influence_map for category -> market impact.
        Caveats: Pearson-based; requires >= 30 samples; p < 0.05 filter.

        Args:
            min_abs_corr: Minimum |corr| (default 0.15)
            max_p_value: Maximum p-value (default 0.05)

        Disclaimer: Information only, not investment advice.
        """
        return await get_macro_causality_graph(min_abs_corr=min_abs_corr,
                                                max_p_value=max_p_value)

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_symbol_peer_links_tool(market_id: str = "us_stock",
                                          symbol: Optional[str] = None,
                                          top_k: int = 20) -> Dict[str, Any]:
        """
        Purpose: Symbol-level lead-lag links (e.g. META -> AMZN, lag=15m, rho=+0.53).
            When `symbol` is set, only peers that lead or follow that symbol are returned.
        Triggers (casual questions too): "what moves before NVDA?", "이 종목보다 먼저 움직이는 종목 있어?",
            "which stocks follow AAPL?", "선행 종목 알려줘", "any early-warning peers for this ticker?".
        When to call: incorporate peer leading signals into single-symbol reasoning.
        Prerequisites: none.
        Next steps: get_signal_detail for the peer's signal context.
        Caveats: 14-day lookback, 15-minute bars.

        Args:
            market_id: coin / kr_stock / us_stock
            symbol: Optional. When set, peers are anchored to this symbol.
            top_k: Number of top links to return

        Disclaimer: Information only, not investment advice.
        """
        return await get_symbol_peer_links(market_id=market_id, symbol=symbol, top_k=top_k)

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_feature_governance_status_tool() -> Dict[str, Any]:
        """
        Purpose: Feature governance snapshot — OBSERVATION / CONDITIONAL / ACTIVE / DEPRECATED
            distribution + last 7-day transitions. Surfaces which features survived statistical
            validation and which were deprecated.
        Triggers (casual questions too): "which features are actually used?", "어떤 피처가 살아있어?",
            "any features promoted recently?", "피처 검증 현황 어때?", "did anything get deprecated?".
        When to call: trust evaluation, "which features are live right now?".
        Prerequisites: none.
        Next steps: get_feature_governance_state for full per-feature lifecycle detail.
        Caveats: promoter cycle runs hourly.

        Disclaimer: Information only, not investment advice.
        """
        return await get_feature_governance_status()
