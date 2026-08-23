# -*- coding: utf-8 -*-
"""
포지션 조회 Tool
================
시장별 현재 포지션을 조건에 맞게 조회

파라미터:
- market_id: 시장 (crypto, kr_stock, us_stock)
- min_roi, max_roi: 수익률 필터
- strategy: 전략 필터
- sort_by: 정렬 기준
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Literal

from oneqaz_trading_mcp.config import (
    CACHE_TTL_POSITIONS,
    get_market_db_path,
    connect_readonly,
)
from oneqaz_trading_mcp.resources.resource_response import mcp_error, MCPErrorCode, MCPErrorAction, wrap_tool_response

# FastMCP introspects the function return annotation to build outputSchema.
try:
    from oneqaz_trading_mcp.schemas import PositionsEnvelope
except ImportError:  # pragma: no cover
    PositionsEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]

logger = logging.getLogger("MarketMCP")

_MARKET_LABEL_KO = {"crypto": "암호화폐", "kr_stock": "한국 주식", "us_stock": "미국 주식"}


def _market_label(market_id: str) -> str:
    return _MARKET_LABEL_KO.get(market_id, market_id)


# ---------------------------------------------------------------------------
# AI / 사용자 요약
# ---------------------------------------------------------------------------

def _positions_total(data: dict) -> int:
    stats = data.get("stats", {}) or {}
    # _get_positions 는 total_positions, 단축 호출은 길이 기반. 둘 다 대응.
    if "total_positions" in stats:
        return int(stats.get("total_positions") or 0)
    if "total" in stats:
        return int(stats.get("total") or 0)
    return len(data.get("positions", []) or [])


def _ai_summary_positions(data: dict) -> str:
    market_id = data.get("market_id", "")
    stats = data.get("stats", {}) or {}
    total = _positions_total(data)
    return (
        f"{market_id} 포지션 {total}개 — "
        f"이익 {stats.get('profitable', 0)} / 손실 {stats.get('losing', 0)}, "
        f"avg {stats.get('avg_pnl', 0):+.2f}%"
    )


def _user_summary_positions(data: dict) -> str:
    market_id = data.get("market_id", "")
    label = _market_label(market_id)
    stats = data.get("stats", {}) or {}
    total = _positions_total(data)
    if total == 0:
        return f"{label} 시장에 현재 보유 중인 포지션이 없습니다."
    profitable = stats.get("profitable", 0)
    avg = stats.get("avg_pnl", 0) or 0
    win_pct = round(profitable / total * 100) if total else 0
    return (
        f"{label} 시장에서 현재 {total}개 종목을 보유 중이며, 그 중 {profitable}개({win_pct}%)가 이익 상태이고 "
        f"평균 수익률은 {avg:+.2f}%입니다."
    )


def _ai_summary_position_detail(data: dict) -> str:
    coin = data.get("coin", "?")
    pos = data.get("position", {}) or {}
    return (
        f"{coin} 포지션 — 수익률 {pos.get('profit_loss_pct', 0):+.2f}%, "
        f"전략 {pos.get('current_strategy', '?')}, AI 점수 {pos.get('ai_score', 0):.2f}"
    )


def _user_summary_position_detail(data: dict) -> str:
    coin = data.get("coin", "?")
    pos = data.get("position", {}) or {}
    pnl = pos.get("profit_loss_pct", 0) or 0
    holding = pos.get("holding_time_str", "?")
    state = "이익" if pnl > 0 else ("손실" if pnl < 0 else "보합")
    return f"{coin} 종목은 현재 {pnl:+.2f}% {state} 상태이며 {holding} 동안 보유 중입니다."


def _ai_summary_strategy_dist(data: dict) -> str:
    dist = data.get("distribution", []) or []
    if not dist:
        return f"{data.get('market_id', '')} 전략 분포 — 비어있음"
    top = dist[0]
    return (
        f"{data.get('market_id', '')} 전략 {len(dist)}종 — "
        f"top: {top.get('current_strategy', '?')} {top.get('count', 0)}개 (avg {top.get('avg_pnl', 0):+.2f}%, "
        f"승률 {top.get('win_rate', 0):.1f}%)"
    )


def _user_summary_strategy_dist(data: dict) -> str:
    label = _market_label(data.get("market_id", ""))
    dist = data.get("distribution", []) or []
    if not dist:
        return f"{label} 시장에 현재 활성화된 전략이 없습니다."
    top = dist[0]
    return f"{label} 시장에서 가장 많이 쓰이는 전략은 '{top.get('current_strategy', '?')}'이며 {top.get('count', 0)}개 종목에 적용되어 있습니다."


# ---------------------------------------------------------------------------
# 포지션 조회 함수
# ---------------------------------------------------------------------------

def _get_positions(
    market_id: str,
    min_roi: Optional[float] = None,
    max_roi: Optional[float] = None,
    strategy: Optional[str] = None,
    sort_by: str = "profit_loss_pct",
    sort_order: str = "desc",
    limit: int = 1000,
) -> Dict[str, Any]:
    """포지션 조회 (필터링 및 정렬 지원)"""
    db_path = get_market_db_path(market_id)
    logger.info("[MCP] get_positions market_id=%s db_path=%s", market_id, db_path)
    # [2026-08-10] .exists() 게이트 제거 — db_path 는 PG 라우팅용 논리 키
    # (connect_readonly 가 PG 스키마로 라우팅, 실파일 안 엶 — trade_history.py 동일 수리)
    if not db_path:
        return mcp_error(MCPErrorCode.DB_NOT_FOUND, f"Trading DB not found for market: {market_id}", fallback_tool="market://all/summary", available_markets=["crypto", "kr_stock", "us_stock"])

    try:
        with connect_readonly(db_path) as conn:

            # 테이블에 존재하는 컬럼만 SELECT (시장별 스키마 차이 대응)
            cursor_info = conn.execute(
                "SELECT name FROM pragma_table_info('virtual_positions')"
            )
            existing_columns = {row[0] for row in cursor_info.fetchall()}
            wanted = [
                "symbol", "entry_price", "current_price", "profit_loss_pct",
                "quantity", "entry_timestamp", "holding_duration",
                "target_price", "stop_loss_price", "max_profit_pct",
                "ai_score", "ai_reason", "entry_strategy", "current_strategy",
                "strategy_match", "strategy_switch_count", "evolution_level",
                "fractal_score", "mtf_score", "cross_score", "entry_confidence",
            ]
            select_cols = [c for c in wanted if c in existing_columns]
            if not select_cols:
                return mcp_error(MCPErrorCode.TABLE_NOT_FOUND, "virtual_positions has no expected columns", action=MCPErrorAction.CHECK, action_value="market://info", market_id=market_id)
            cols_str = ", ".join(select_cols)
            query = f"""
                SELECT {cols_str}
                FROM virtual_positions
                WHERE 1=1
            """
            params: List[Any] = []
            
            # 수익률 필터
            if min_roi is not None:
                query += " AND profit_loss_pct >= ?"
                params.append(min_roi)
            if max_roi is not None:
                query += " AND profit_loss_pct <= ?"
                params.append(max_roi)
            
            # 전략 필터
            if strategy:
                query += " AND (current_strategy = ? OR entry_strategy = ?)"
                params.append(strategy)
                params.append(strategy)
            
            # 정렬
            valid_sort_cols = ["profit_loss_pct", "entry_timestamp", "holding_duration", "ai_score"]
            if sort_by not in valid_sort_cols:
                sort_by = "profit_loss_pct"
            order = "DESC" if sort_order.lower() == "desc" else "ASC"
            query += f" ORDER BY {sort_by} {order} LIMIT ?"
            params.append(limit)
            
            cursor = conn.execute(query, params)
            positions = []
            
            for row in cursor.fetchall():
                pos = dict(row)
                # symbol 우선 (coin 컬럼 제거됨)
                sym = pos.get("symbol") or pos.get("coin", "?")
                pos["symbol"] = sym
                
                # 보유 시간 포맷
                duration = pos.get("holding_duration", 0) or 0
                hours = duration // 3600
                mins = (duration % 3600) // 60
                pos["holding_time_str"] = f"{hours}h {mins}m"
                
                # 진입 시간 포맷
                if pos.get("entry_timestamp"):
                    pos["entry_time_str"] = datetime.fromtimestamp(
                        pos["entry_timestamp"]
                    ).strftime("%m/%d %H:%M")
                
                # 목표 달성률 계산
                target = pos.get("target_price", 0) or 0
                current = pos.get("current_price", 0) or 0
                entry = pos.get("entry_price", 0) or 0
                
                if target > entry and entry > 0:
                    progress = (current - entry) / (target - entry) * 100
                    pos["target_progress"] = round(min(progress, 100), 1)
                else:
                    pos["target_progress"] = None
                
                positions.append(pos)
            
            # 통계 계산
            if positions:
                total_pnl = sum(p.get("profit_loss_pct", 0) for p in positions)
                profitable = sum(1 for p in positions if p.get("profit_loss_pct", 0) > 0)
                avg_pnl = total_pnl / len(positions)
                avg_ai_score = sum(p.get("ai_score", 0) or 0 for p in positions) / len(positions)
                
                stats = {
                    "total_positions": len(positions),
                    "profitable": profitable,
                    "losing": len(positions) - profitable,
                    "total_pnl": round(total_pnl, 2),
                    "avg_pnl": round(avg_pnl, 2),
                    "avg_ai_score": round(avg_ai_score, 2),
                }
            else:
                stats = {
                    "total_positions": 0,
                    "profitable": 0,
                    "losing": 0,
                    "total_pnl": 0,
                    "avg_pnl": 0,
                    "avg_ai_score": 0,
                }
            
            # LLM용 요약
            summary_lines = [
                f"[{market_id.upper()} 포지션] {len(positions)}개 보유",
                f"- 이익/손실: {stats['profitable']}개 / {stats['losing']}개",
                f"- 평균 수익률: {stats['avg_pnl']:+.2f}%",
                f"- 평균 AI 점수: {stats['avg_ai_score']:.2f}",
            ]
            
            for p in positions[:5]:
                sym = p.get("symbol") or p.get("coin", "?")
                pnl = p.get("profit_loss_pct", 0)
                strategy = p.get("current_strategy", "?")
                progress = p.get("target_progress")
                progress_str = f", 목표 {progress}%" if progress else ""
                summary_lines.append(f"  - {sym}: {pnl:+.2f}% ({strategy}{progress_str})")
            
            return {
                "market_id": market_id,
                "positions": positions,
                "stats": stats,
                "filters": {
                    "min_roi": min_roi,
                    "max_roi": max_roi,
                    "strategy": strategy,
                    "sort_by": sort_by,
                    "sort_order": sort_order,
                    "limit": limit,
                },
                "_llm_summary": "\n".join(summary_lines),
            }
            
    except Exception as e:
        logger.error(f"Failed to get positions for {market_id}: {e}")
        return mcp_error(MCPErrorCode.NO_DATA, str(e), market_id=market_id)

def _get_position_detail(market_id: str, coin: str) -> Dict[str, Any]:
    """특정 코인의 포지션 상세 정보"""
    db_path = get_market_db_path(market_id)

    # [2026-08-10] .exists() 게이트 제거 — 위 _get_positions 와 동일한 이유
    if not db_path:
        return mcp_error(MCPErrorCode.DB_NOT_FOUND, f"DB not found for market: {market_id}", fallback_tool="market://all/summary", available_markets=["crypto", "kr_stock", "us_stock"])

    try:
        with connect_readonly(db_path) as conn:

            # 포지션 정보 (virtual_positions는 symbol 컬럼 사용)
            cursor = conn.execute("""
                SELECT * FROM virtual_positions WHERE symbol = ?
            """, (coin.upper(),))
            
            row = cursor.fetchone()
            if not row:
                return mcp_error(MCPErrorCode.SYMBOL_NOT_FOUND, f"Position not found for {coin} in {market_id}", action=MCPErrorAction.FALLBACK, action_value=f"get_positions(market_id='{market_id}')", fallback_tool=f"get_signals(market_id='{market_id}', coin='{coin}')", market_id=market_id)
            
            position = dict(row)
            
            # 보유 시간 포맷
            duration = position.get("holding_duration", 0) or 0
            hours = duration // 3600
            mins = (duration % 3600) // 60
            position["holding_time_str"] = f"{hours}h {mins}m"
            
            # 최근 거래 내역 (해당 코인)
            cursor = conn.execute("""
                SELECT action, profit_loss_pct, exit_timestamp, ai_reason
                FROM virtual_trade_history
                WHERE symbol = ?
                ORDER BY exit_timestamp DESC
                LIMIT 5
            """, (coin.upper(),))
            
            recent_trades = [dict(r) for r in cursor.fetchall()]
            
            # 최근 매매 판단
            cursor = conn.execute("""
                SELECT decision, ai_reason, regime_name, timestamp
                FROM virtual_trade_decisions
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT 3
            """, (coin.upper(),))
            
            recent_decisions = [dict(r) for r in cursor.fetchall()]
            
            # LLM 요약
            pnl = position.get("profit_loss_pct", 0)
            strategy = position.get("current_strategy", "?")
            ai_score = position.get("ai_score", 0)
            ai_reason = position.get("ai_reason", "")
            
            summary_lines = [
                f"[{coin.upper()} 포지션 상세]",
                f"- 현재 수익률: {pnl:+.2f}%",
                f"- 전략: {strategy}",
                f"- AI 점수: {ai_score:.2f}",
                f"- AI 판단: {ai_reason[:100] if ai_reason else 'N/A'}",
                f"- 보유 시간: {position['holding_time_str']}",
            ]
            
            if recent_trades:
                summary_lines.append(f"- 최근 {len(recent_trades)}건 거래 이력 있음")
            
            return {
                "market_id": market_id,
                "coin": coin.upper(),
                "position": position,
                "recent_trades": recent_trades,
                "recent_decisions": recent_decisions,
                "_llm_summary": "\n".join(summary_lines),
            }
            
    except Exception as e:
        logger.error(f"Failed to get position detail for {coin} in {market_id}: {e}")
        return mcp_error(MCPErrorCode.NO_DATA, str(e), market_id=market_id, symbol=coin)

def _get_strategy_distribution(market_id: str) -> Dict[str, Any]:
    """전략별 포지션 분포"""
    db_path = get_market_db_path(market_id)

    # [2026-08-10] .exists() 게이트 제거 — 위 _get_positions 와 동일한 이유
    if not db_path:
        return mcp_error(MCPErrorCode.DB_NOT_FOUND, f"DB not found for market: {market_id}", fallback_tool="market://all/summary", available_markets=["crypto", "kr_stock", "us_stock"])

    try:
        with connect_readonly(db_path) as conn:

            cursor = conn.execute("""
                SELECT current_strategy,
                       COUNT(*) as count,
                       AVG(profit_loss_pct) as avg_pnl,
                       SUM(CASE WHEN profit_loss_pct > 0 THEN 1 ELSE 0 END) as wins
                FROM virtual_positions
                GROUP BY current_strategy
                ORDER BY count DESC
            """)
            
            distribution = []
            for row in cursor.fetchall():
                d = dict(row)
                d["avg_pnl"] = round(d["avg_pnl"] or 0, 2)
                d["win_rate"] = round(d["wins"] / d["count"] * 100, 1) if d["count"] > 0 else 0
                distribution.append(d)
            
            # LLM 요약
            summary_lines = [f"[{market_id.upper()} 전략 분포]"]
            for d in distribution:
                strategy = d.get("current_strategy", "unknown")
                count = d.get("count", 0)
                avg_pnl = d.get("avg_pnl", 0)
                win_rate = d.get("win_rate", 0)
                summary_lines.append(f"- {strategy}: {count}개, 평균 {avg_pnl:+.2f}%, 승률 {win_rate:.1f}%")
            
            return {
                "market_id": market_id,
                "distribution": distribution,
                "_llm_summary": "\n".join(summary_lines),
            }
            
    except Exception as e:
        logger.error(f"Failed to get strategy distribution for {market_id}: {e}")
        return mcp_error(MCPErrorCode.NO_DATA, str(e), market_id=market_id)

# ---------------------------------------------------------------------------
# Tool 등록 함수
# ---------------------------------------------------------------------------

def register_position_tools(mcp, cache):
    """포지션 관련 Tool 등록"""
    # Phase 5+B (2026-05-07): 응답 데이터 기반 _next_actions 추천.
    from oneqaz_trading_mcp.tools.next_actions import (
        for_positions, for_position_detail, for_profitable_positions,
        for_losing_positions, for_strategy_distribution,
    )
    # Phase D-E (2026-05-07): user-facing followup questions.
    from oneqaz_trading_mcp.tools.followup_questions import (
        fq_positions, fq_position_detail, fq_profitable_positions,
        fq_losing_positions, fq_strategy_distribution,
    )
    
    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_positions(
        market_id: str,
        min_roi: Optional[float] = None,
        max_roi: Optional[float] = None,
        strategy: Optional[str] = None,
        sort_by: str = "profit_loss_pct",
        sort_order: str = "desc",
        limit: int = 1000,
    ) -> PositionsEnvelope:
        """
        Purpose: List current paper-trading positions, with dynamic filters (ROI / strategy / sort).
        Triggers (casual questions too): "what are you holding?", "current positions?",
            "뭐 들고 있어?", "what's the exposure / portfolio?", "any winners / losers right now?",
            "how's the book doing?". Paper-trading positions (NOT real money).
        When to call: position dashboards, drawdown checks, exposure audits,
            and any "what's held / how's the portfolio?" question.
        Prerequisites: market://{market_id}/status recommended for context.
        Next steps: get_position_detail, get_strategy_distribution.
        Caveats: paper-trading data only. Positions are not real money holdings.
        Disclaimer: Information only, not investment advice.


        Args:
            market_id: Market ID (crypto, kr_stock, us_stock)
            min_roi: Min ROI % filter (e.g., -5.0)
            max_roi: Max ROI % filter (e.g., 10.0)
            strategy: Strategy filter (e.g., trend, scalping)
            sort_by: Sort field (profit_loss_pct, entry_timestamp, holding_duration, ai_score)
            sort_order: Sort direction (desc, asc)
            limit: Max results (default 1000)
        """
        cache_key = f"positions_{market_id}_{min_roi}_{max_roi}_{strategy}_{sort_by}_{sort_order}_{limit}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_POSITIONS)
        if cached:
            logger.debug(f"Cache hit: {cache_key}")
            return cached

        limit = min(max(1, limit), 1000)

        result = await asyncio.to_thread(
            _get_positions,
            market_id=market_id,
            min_roi=min_roi,
            max_roi=max_roi,
            strategy=strategy,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
        )

        wrapped = wrap_tool_response(
            result, "positions",
            _ai_summary_positions, _user_summary_positions,
            next_actions_fn=for_positions,
            followup_questions_fn=fq_positions,
        )
        cache.set(cache_key, wrapped)
        return wrapped

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_position_detail(
        market_id: str,
        coin: Optional[str] = None,
        symbol: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Purpose: Per-symbol paper position deep-dive (position + recent trades + decisions).
        Triggers (casual questions too): "how's the BTC position doing?", "삼성전자 얼마나 벌고 있어?",
            "why are you holding X?", "그 종목 지금 수익률 어때?", "tell me about the AAPL position".
        When to call: full context for one ticker.
        Prerequisites: confirm the symbol holds a position via get_positions.
        Next steps: get_signal_detail, get_role_analysis.
        Caveats: returns an error envelope when no position exists for the symbol.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock; aliases coin/kr/us accepted)
            symbol: Asset identifier (preferred; e.g., BTC, ETH, AAPL)
            coin: Legacy alias of symbol (kept for backward compatibility)

        Disclaimer: Information only, not investment advice.
        """
        _symbol = symbol or coin
        if not _symbol:
            return mcp_error(
                MCPErrorCode.MISSING_REQUIRED_FIELD,
                "Provide 'symbol' (preferred) or legacy alias 'coin'",
                action=MCPErrorAction.FALLBACK,
                action_value=f"get_positions(market_id='{market_id}')",
                fallback_tool="get_positions",
            )
        cache_key = f"position_detail_{market_id}_{_symbol}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_POSITIONS)
        if cached:
            return cached

        result = await asyncio.to_thread(_get_position_detail, market_id, _symbol)
        wrapped = wrap_tool_response(
            result, "positions",
            _ai_summary_position_detail, _user_summary_position_detail,
            next_actions_fn=for_position_detail,
            followup_questions_fn=fq_position_detail,
        )
        cache.set(cache_key, wrapped)
        return wrapped
    
    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_profitable_positions(
        market_id: str,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """
        Purpose: Profitable paper positions (ROI > 0). Convenience wrapper around get_positions(min_roi=0.01).
        Triggers (casual questions too): "what's winning right now?", "지금 뭐가 수익 나고 있어?",
            "show me the green ones", "best open positions?", "어떤 종목이 잘 가고 있어?".
        When to call: quickly surface winning tickers.
        Prerequisites: none.
        Next steps: get_position_detail for full context.
        Caveats: paper-trading data only.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock; aliases coin/kr/us accepted)
            limit: Max results (default 20)

        Disclaimer: Information only, not investment advice.
        """
        result = await asyncio.to_thread(
            _get_positions,
            market_id=market_id,
            min_roi=0.01,
            sort_by="profit_loss_pct",
            sort_order="desc",
            limit=limit,
        )
        return wrap_tool_response(
            result, "positions",
            _ai_summary_positions, _user_summary_positions,
            next_actions_fn=for_profitable_positions,
            followup_questions_fn=fq_profitable_positions,
        )

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_losing_positions(
        market_id: str,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """
        Purpose: Losing paper positions (ROI < 0). Convenience wrapper around get_positions(max_roi=-0.01).
        Triggers (casual questions too): "what's underwater?", "지금 뭐가 물려 있어?",
            "show me the red ones", "any positions in trouble?", "얼마나 손실 중이야?".
        When to call: drawdown / risk review.
        Prerequisites: none.
        Next steps: get_position_detail, get_role_analysis.
        Caveats: paper-trading data only.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock; aliases coin/kr/us accepted)
            limit: Max results (default 20)

        Disclaimer: Information only, not investment advice.
        """
        result = await asyncio.to_thread(
            _get_positions,
            market_id=market_id,
            max_roi=-0.01,
            sort_by="profit_loss_pct",
            sort_order="asc",
            limit=limit,
        )
        return wrap_tool_response(
            result, "positions",
            _ai_summary_positions, _user_summary_positions,
            next_actions_fn=for_losing_positions,
            followup_questions_fn=fq_losing_positions,
        )

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_strategy_distribution(market_id: str) -> Dict[str, Any]:
        """
        Purpose: Per-strategy breakdown across current paper positions (count, avg P&L, win rate per strategy).
        Triggers (casual questions too): "what strategies are you running?", "무슨 전략 돌리고 있어?",
            "which strategy holds the most positions?", "전략별 성적 어때?", "is one strategy dominating?".
        When to call: diversification audit, per-strategy performance check.
        Prerequisites: get_positions recommended for raw rows.
        Next steps: market://{market_id}/derived/strategy-fitness, signals/feedback.
        Caveats: empty distribution when no positions are open.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock; aliases coin/kr/us accepted)

        Disclaimer: Information only, not investment advice.
        """
        cache_key = f"strategy_distribution_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_POSITIONS)
        if cached:
            return cached

        result = await asyncio.to_thread(_get_strategy_distribution, market_id)
        wrapped = wrap_tool_response(
            result, "positions",
            _ai_summary_strategy_dist, _user_summary_strategy_dist,
            next_actions_fn=for_strategy_distribution,
            followup_questions_fn=fq_strategy_distribution,
        )
        cache.set(cache_key, wrapped)
        return wrapped
    
    logger.info("  [OK] Position tools registered")
