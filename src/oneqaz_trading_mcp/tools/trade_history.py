# -*- coding: utf-8 -*-
"""
거래 내역 조회 Tool
===================
시장별 거래 내역을 조건에 맞게 조회

파라미터:
- market_id: 시장 (crypto, kr_stock, us_stock)
- limit: 조회 개수
- action: 필터 (buy, sell, all)
- min_pnl, max_pnl: 수익률 필터
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.config import (
    CACHE_TTL_TRADE_HISTORY,
    get_market_db_path,
    connect_readonly,
)

# FastMCP 2.14 introspects function return annotations to build outputSchema.
# Importing the pinned envelope here gives external clients the precise shape
# of `full_data.trades / stats / filters` instead of a free-form `object`.
try:
    from oneqaz_trading_mcp.schemas import TradeHistoryEnvelope, TradeAnalysisEnvelope
except ImportError:  # pragma: no cover
    TradeHistoryEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    TradeAnalysisEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
from oneqaz_trading_mcp.resources.resource_response import mcp_error, MCPErrorCode, MCPErrorAction, wrap_tool_response

logger = logging.getLogger("MarketMCP")

_MARKET_LABEL_KO = {"crypto": "암호화폐", "kr_stock": "한국 주식", "us_stock": "미국 주식"}


def _market_label_th(market_id: str) -> str:
    return _MARKET_LABEL_KO.get(market_id, market_id)


def _ai_summary_trades(data: dict) -> str:
    market_id = data.get("market_id", "")
    stats = data.get("stats", {}) or {}
    return (
        f"{market_id} 거래 {stats.get('total_trades', 0)}건 — "
        f"승률 {stats.get('win_rate', 0):.1f}%, total {stats.get('total_pnl', 0):+.2f}%, "
        f"avg {stats.get('avg_pnl', 0):+.2f}%"
    )


def _user_summary_trades(data: dict) -> str:
    label = _market_label_th(data.get("market_id", ""))
    stats = data.get("stats", {}) or {}
    total = stats.get("total_trades", 0)
    if total == 0:
        return f"{label} 시장에 조건에 맞는 거래 내역이 없습니다."
    return (
        f"{label} 시장에서 {total}건의 거래가 있었으며 승률은 {stats.get('win_rate', 0):.1f}%, "
        f"평균 수익률은 {stats.get('avg_pnl', 0):+.2f}%입니다."
    )


def _ai_summary_analysis(data: dict) -> str:
    market_id = data.get("market_id", "")
    days = data.get("days", 0)
    total = data.get("total_trades", 0)
    return f"{market_id} 최근 {days}일 분석 — 총 {total}건"


def _user_summary_analysis(data: dict) -> str:
    label = _market_label_th(data.get("market_id", ""))
    days = data.get("days", 0)
    total = data.get("total_trades", 0)
    if total == 0:
        return f"{label} 시장에 최근 {days}일 동안 거래가 없었습니다."
    return f"{label} 시장에서 최근 {days}일 동안 총 {total}건의 거래가 분석되었습니다."


# ---------------------------------------------------------------------------
# 거래 내역 조회 함수
# ---------------------------------------------------------------------------

def _get_trade_history(
    market_id: str,
    limit: int = 1000,
    action_filter: Optional[str] = None,
    min_pnl: Optional[float] = None,
    max_pnl: Optional[float] = None,
    hours_back: Optional[int] = None,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    """거래 내역 조회"""
    db_path = get_market_db_path(market_id)
    logger.info("[MCP] get_trade_history market_id=%s db_path=%s", market_id, db_path)
    # [2026-08-10] .exists() 게이트 제거 — db_path 는 PG 라우팅용 논리 키일 뿐이라
    # (connect_readonly 가 market_coin 등 PG 스키마로 라우팅, SQLite 안 엶)
    # 레거시 백업 파일을 archive 하는 순간 PG 정상인데 DB_NOT_FOUND 가 되는 지뢰였다.
    # 미등록 시장은 get_market_db_path 가 None, 미등록 경로는 connect_readonly fail-loud.
    if not db_path:
        return mcp_error(MCPErrorCode.DB_NOT_FOUND, f"Trading DB not found for market: {market_id}", fallback_tool="market://all/summary", available_markets=["crypto", "kr_stock", "us_stock"])
    
    try:
        with connect_readonly(db_path, timeout=10) as conn:
            # 테이블에 존재하는 컬럼만 SELECT (시장별 스키마 차이 대응)
            cursor_info = conn.execute(
                "SELECT name FROM pragma_table_info('virtual_trade_history')"
            )
            existing_columns = {row[0] for row in cursor_info.fetchall()}
            wanted = [
                "symbol", "action", "profit_loss_pct", "entry_price", "exit_price",
                "entry_timestamp", "exit_timestamp", "holding_duration",
                "ai_score", "ai_reason", "signal_pattern",
                "created_at", "entry_confidence",
                "fractal_score", "mtf_score", "cross_score",
            ]
            select_cols = [c for c in wanted if c in existing_columns]
            if not select_cols:
                return mcp_error(MCPErrorCode.TABLE_NOT_FOUND, "virtual_trade_history has no expected columns", action=MCPErrorAction.CHECK, action_value="market://info", market_id=market_id)
            cols_str = ", ".join(select_cols)
            query = f"""
                SELECT {cols_str}
                FROM virtual_trade_history
                WHERE 1=1
            """
            params: List[Any] = []

            # symbol 필터 (AX: 외부 AI 가 "BTC 체결만" 요청할 때 필요)
            if symbol and "symbol" in existing_columns:
                query += " AND UPPER(symbol) = UPPER(?)"
                params.append(symbol)

            # 액션 필터
            if action_filter and action_filter.lower() != "all":
                if action_filter.lower() == "buy":
                    query += " AND action LIKE 'buy%'"
                elif action_filter.lower() in ("sell", "close", "exit"):
                    query += " AND action NOT LIKE 'buy%'"
            
            # 수익률 필터
            if min_pnl is not None:
                query += " AND profit_loss_pct >= ?"
                params.append(min_pnl)
            if max_pnl is not None:
                query += " AND profit_loss_pct <= ?"
                params.append(max_pnl)
            
            # 시간 필터
            if hours_back is not None:
                import time
                cutoff = int(time.time()) - (hours_back * 3600)
                query += " AND exit_timestamp >= ?"
                params.append(cutoff)
            
            query += " ORDER BY exit_timestamp DESC LIMIT ?"
            params.append(limit)
            
            cursor = conn.execute(query, params)
            trades = []
            col_names = [d[0] for d in cursor.description] if cursor.description else select_cols

            for row in cursor.fetchall():
                if isinstance(row, dict):
                    trade = dict(row)
                else:
                    trade = {col_names[i]: row[i] for i in range(len(col_names))}
                # symbol 우선 (coin 컬럼 제거됨)
                sym = trade.get("symbol") or trade.get("coin", "UNKNOWN")
                trade["symbol"] = sym

                # 보유 시간 포맷
                duration = trade.get("holding_duration", 0) or 0
                hours = duration // 3600
                mins = (duration % 3600) // 60
                trade["holding_time_str"] = f"{hours}h {mins}m"
                
                # 타임스탬프 포맷
                if trade.get("exit_timestamp"):
                    trade["exit_time_str"] = datetime.fromtimestamp(
                        trade["exit_timestamp"]
                    ).strftime("%Y-%m-%d %H:%M")
                
                # created_at 포맷 (대시보드용)
                created_at = trade.get("created_at")
                if created_at:
                    if 'T' in str(created_at):
                        trade["time"] = str(created_at).split('T')[1][:5]
                    elif ' ' in str(created_at):
                        trade["time"] = str(created_at).split(' ')[1][:5]
                    else:
                        trade["time"] = str(created_at)[:5]
                elif trade.get("exit_timestamp"):
                    trade["time"] = datetime.fromtimestamp(
                        trade["exit_timestamp"]
                    ).strftime("%H:%M")
                else:
                    trade["time"] = ""
                
                # entry_confidence 확신도 레벨
                conf = trade.get("entry_confidence", 0) or 0
                if conf >= 0.8:
                    trade["confidence_level"] = "High"
                elif conf >= 0.5:
                    trade["confidence_level"] = "Medium"
                else:
                    trade["confidence_level"] = "Low"
                
                # None 값 기본값 처리
                trade["fractal_score"] = trade.get("fractal_score") or 0.5
                trade["mtf_score"] = trade.get("mtf_score") or 0.5
                trade["cross_score"] = trade.get("cross_score") or 0.5
                trade["ai_score"] = trade.get("ai_score") or 0.0
                
                trades.append(trade)
            
            # 통계 계산
            if trades:
                total_pnl = sum(t.get("profit_loss_pct", 0) for t in trades)
                wins = sum(1 for t in trades if t.get("profit_loss_pct", 0) > 0)
                losses = len(trades) - wins
                avg_pnl = total_pnl / len(trades)
                
                stats = {
                    "total_trades": len(trades),
                    "wins": wins,
                    "losses": losses,
                    "win_rate": round(wins / len(trades) * 100, 1) if trades else 0,
                    "total_pnl": round(total_pnl, 2),
                    "avg_pnl": round(avg_pnl, 2),
                }
            else:
                stats = {
                    "total_trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "win_rate": 0,
                    "total_pnl": 0,
                    "avg_pnl": 0,
                }
            
            # LLM 요약
            summary_lines = [
                f"[{market_id.upper()} 거래 내역] {len(trades)}건 조회",
                f"- 승률: {stats['win_rate']}% ({stats['wins']}승 {stats['losses']}패)",
                f"- 평균 수익률: {stats['avg_pnl']:+.2f}%",
            ]

            for t in trades[:5]:
                sym = t.get("symbol") or t.get("coin", "?")
                pnl = t.get("profit_loss_pct", 0)
                action = t.get("action", "?")
                summary_lines.append(f"  - {sym}: {pnl:+.2f}% ({action})")
            
            return {
                "market_id": market_id,
                "trades": trades,
                "stats": stats,
                "filters": {
                    "action": action_filter,
                    "min_pnl": min_pnl,
                    "max_pnl": max_pnl,
                    "hours_back": hours_back,
                    "limit": limit,
                },
                "_llm_summary": "\n".join(summary_lines),
            }
            
    except Exception as e:
        logger.error(f"Failed to get trade history for {market_id}: {e}")
        return mcp_error(MCPErrorCode.NO_DATA, str(e), market_id=market_id)

def _get_trade_analysis(market_id: str, days: int = 7) -> Dict[str, Any]:
    """거래 분석 (일별/패턴별 통계)"""
    db_path = get_market_db_path(market_id)

    # [2026-08-10] .exists() 게이트 제거 — 위 _get_trade_history 와 동일한 이유
    if not db_path:
        return mcp_error(MCPErrorCode.DB_NOT_FOUND, f"DB not found for market: {market_id}", fallback_tool="market://all/summary", available_markets=["crypto", "kr_stock", "us_stock"])
    
    try:
        import time
        from collections import defaultdict
        
        cutoff = int(time.time()) - (days * 86400)
        
        with connect_readonly(db_path, timeout=10) as conn:
            cursor_info = conn.execute("SELECT name FROM pragma_table_info('virtual_trade_history')")
            cols = {r[0] for r in cursor_info.fetchall()}
            sym_col = "symbol" if "symbol" in cols else "coin"
            cursor = conn.execute(f"""
                SELECT {sym_col}, action, profit_loss_pct, exit_timestamp,
                       signal_pattern, ai_score
                FROM virtual_trade_history
                WHERE exit_timestamp >= ?
                ORDER BY exit_timestamp
            """, (cutoff,))
            
            trades = [dict(row) for row in cursor.fetchall()]
            
            if not trades:
                return {
                    "market_id": market_id,
                    "days": days,
                    "analysis": {},
                    "_llm_summary": f"[{market_id.upper()} 분석] 최근 {days}일 거래 내역 없음",
                }
            
            # 일별 통계
            daily_stats = defaultdict(lambda: {"trades": 0, "pnl": 0, "wins": 0})
            pattern_stats = defaultdict(lambda: {"trades": 0, "pnl": 0, "wins": 0})
            coin_stats = defaultdict(lambda: {"trades": 0, "pnl": 0, "wins": 0})
            
            for t in trades:
                # 일별
                ts = t.get("exit_timestamp", 0)
                day = datetime.fromtimestamp(ts).strftime("%Y-%m-%d") if ts else "unknown"
                daily_stats[day]["trades"] += 1
                daily_stats[day]["pnl"] += t.get("profit_loss_pct", 0)
                if t.get("profit_loss_pct", 0) > 0:
                    daily_stats[day]["wins"] += 1
                
                # 패턴별
                pattern = t.get("signal_pattern", "unknown") or "unknown"
                pattern_stats[pattern]["trades"] += 1
                pattern_stats[pattern]["pnl"] += t.get("profit_loss_pct", 0)
                if t.get("profit_loss_pct", 0) > 0:
                    pattern_stats[pattern]["wins"] += 1
                
                # 종목별
                sym = t.get("symbol") or t.get("coin", "unknown")
                coin_stats[sym]["trades"] += 1
                coin_stats[sym]["pnl"] += t.get("profit_loss_pct", 0)
                if t.get("profit_loss_pct", 0) > 0:
                    coin_stats[sym]["wins"] += 1
            
            # 상위 5개 코인
            top_coins = sorted(
                coin_stats.items(),
                key=lambda x: x[1]["pnl"],
                reverse=True
            )[:5]
            
            # 상위 5개 패턴
            top_patterns = sorted(
                pattern_stats.items(),
                key=lambda x: x[1]["pnl"],
                reverse=True
            )[:5]
            
            # LLM 요약
            summary_lines = [
                f"[{market_id.upper()} 거래 분석] 최근 {days}일",
                f"- 총 거래: {len(trades)}건",
            ]
            
            total_pnl = sum(t.get("profit_loss_pct", 0) for t in trades)
            total_wins = sum(1 for t in trades if t.get("profit_loss_pct", 0) > 0)
            summary_lines.append(f"- 총 수익률: {total_pnl:+.2f}%, 승률: {total_wins/len(trades)*100:.1f}%")
            
            summary_lines.append("- 상위 코인:")
            for coin, stats in top_coins[:3]:
                summary_lines.append(f"  - {coin}: {stats['pnl']:+.2f}% ({stats['trades']}건)")
            
            return {
                "market_id": market_id,
                "days": days,
                "total_trades": len(trades),
                "daily_stats": dict(daily_stats),
                "pattern_stats": dict(pattern_stats),
                "coin_stats": dict(coin_stats),
                "top_coins": top_coins,
                "top_patterns": top_patterns,
                "_llm_summary": "\n".join(summary_lines),
            }
            
    except Exception as e:
        logger.error(f"Failed to analyze trades for {market_id}: {e}")
        return mcp_error(MCPErrorCode.NO_DATA, str(e), market_id=market_id)

# ---------------------------------------------------------------------------
# Tool 등록 함수
# ---------------------------------------------------------------------------

def register_trade_history_tools(mcp, cache):
    """거래 내역 관련 Tool 등록"""
    # Phase 5+B (2026-05-07): 응답 데이터 기반 _next_actions 추천.
    from oneqaz_trading_mcp.tools.next_actions import (
        for_trade_history, for_analyze_trades,
        for_winning_trades, for_losing_trades,
    )
    # Phase D-E (2026-05-07): user-facing followup questions.
    from oneqaz_trading_mcp.tools.followup_questions import (
        fq_trade_history, fq_analyze_trades,
        fq_winning_trades, fq_losing_trades,
    )

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_trade_history(
        market_id: str,
        limit: int = 1000,
        action_filter: str = "all",
        min_pnl: Optional[float] = None,
        max_pnl: Optional[float] = None,
        hours_back: Optional[int] = None,
        symbol: Optional[str] = None,
    ) -> TradeHistoryEnvelope:
        """
        Purpose: Query paper-trading history with dynamic filters (action / P&L / time / symbol).
        Triggers (casual questions too): "what trades happened lately?", "최근 거래 내역 보여줘",
            "how did the BTC trades go?", "승률 어때?", "show me the trade log",
            "how many trades won this week?".
        When to call: past trade review, single-symbol post-mortem, win-rate audits.
        Prerequisites: none.
        Next steps: analyze_trades, market://{market_id}/signals/feedback.
        Caveats: paper-trading data only (not real money). limit capped at 1000.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock; aliases coin/kr/us accepted)
            limit: Max results (default 1000)
            action_filter: Filter by action (all, buy, sell)
            min_pnl: Min P&L % filter (e.g., -5.0)
            max_pnl: Max P&L % filter (e.g., 10.0)
            hours_back: Only trades within last N hours
            symbol: Filter by ticker symbol (e.g., "BTC", "AAPL"); case-insensitive

        Disclaimer: Information only, not investment advice.
        """
        # 캐시 키 생성
        cache_key = f"trade_history_{market_id}_{limit}_{action_filter}_{min_pnl}_{max_pnl}_{hours_back}_{symbol}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_TRADE_HISTORY)
        if cached:
            logger.debug(f"Cache hit: {cache_key}")
            return cached

        # 파라미터 검증
        limit = min(max(1, limit), 1000)

        result = await asyncio.to_thread(
            _get_trade_history,
            market_id=market_id,
            limit=limit,
            action_filter=action_filter,
            min_pnl=min_pnl,
            max_pnl=max_pnl,
            hours_back=hours_back,
            symbol=symbol,
        )

        wrapped = wrap_tool_response(
            result, "trade_history",
            _ai_summary_trades, _user_summary_trades,
            next_actions_fn=for_trade_history,
            followup_questions_fn=fq_trade_history,
        )
        cache.set(cache_key, wrapped)
        return wrapped

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def analyze_trades(
        market_id: str,
        days: int = 7,
    ) -> TradeAnalysisEnvelope:
        """
        Purpose: Aggregate paper trades by day / pattern / symbol.
        Triggers (casual questions too): "how's the week been?", "이번 주 매매 성적 어때?",
            "which patterns are working?", "어떤 종목이 제일 잘 벌었어?", "break down the trades",
            "daily P&L summary?".
        When to call: pattern audits, period-over-period performance review.
        Prerequisites: get_trade_history recommended for raw rows first.
        Next steps: market://{market_id}/signals/feedback for the upstream signals.
        Caveats: max 30 days; empty result when no trades in the window.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock; aliases coin/kr/us accepted)
            days: Analysis period in days (default 7, max 30)

        Disclaimer: Information only, not investment advice.
        """
        cache_key = f"trade_analysis_{market_id}_{days}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_TRADE_HISTORY * 3)  # 분석은 좀 더 캐싱
        if cached:
            return cached

        days = min(max(1, days), 30)
        result = await asyncio.to_thread(_get_trade_analysis, market_id, days)

        wrapped = wrap_tool_response(
            result, "trade_history",
            _ai_summary_analysis, _user_summary_analysis,
            next_actions_fn=for_analyze_trades,
            followup_questions_fn=fq_analyze_trades,
        )
        cache.set(cache_key, wrapped)
        return wrapped
    
    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_winning_trades(
        market_id: str,
        limit: int = 10,
    ) -> TradeHistoryEnvelope:
        """
        Purpose: Winning paper trades only (P&L > 0). Convenience wrapper around get_trade_history(min_pnl=0.01).
        Triggers (casual questions too): "what worked?", "뭐가 제일 잘 벌었어?",
            "show me the winners", "best trades lately?", "수익 난 거래 보여줘".
        When to call: success-pattern review.
        Prerequisites: none.
        Next steps: analyze_trades for breakdowns.
        Caveats: paper-trading data only.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock; aliases coin/kr/us accepted)
            limit: Max results (default 10)

        Disclaimer: Information only, not investment advice.
        """
        result = await asyncio.to_thread(
            _get_trade_history,
            market_id=market_id,
            limit=limit,
            min_pnl=0.01,
        )
        return wrap_tool_response(
            result, "trade_history",
            _ai_summary_trades, _user_summary_trades,
            next_actions_fn=for_winning_trades,
            followup_questions_fn=fq_winning_trades,
        )

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_losing_trades(
        market_id: str,
        limit: int = 10,
    ) -> TradeHistoryEnvelope:
        """
        Purpose: Losing paper trades only (P&L < 0). Convenience wrapper around get_trade_history(max_pnl=-0.01).
        Triggers (casual questions too): "어디서 잃었어?", "show me the losses",
            "what went wrong?", "worst trades?", "손실 난 거래 뭐야?".
        When to call: failure-pattern review.
        Prerequisites: none.
        Next steps: analyze_trades for breakdowns.
        Caveats: paper-trading data only.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock; aliases coin/kr/us accepted)
            limit: Max results (default 10)

        Disclaimer: Information only, not investment advice.
        """
        result = await asyncio.to_thread(
            _get_trade_history,
            market_id=market_id,
            limit=limit,
            max_pnl=-0.01,
        )
        return wrap_tool_response(
            result, "trade_history",
            _ai_summary_trades, _user_summary_trades,
            next_actions_fn=for_losing_trades,
            followup_questions_fn=fq_losing_trades,
        )

    logger.info("  [OK] Trade History tools registered")
