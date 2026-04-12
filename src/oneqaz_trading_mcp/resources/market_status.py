# -*- coding: utf-8 -*-
"""
시장 상태 Resource
==================
코인/한국주식/미국주식 시장의 현재 상태 제공

데이터 소스:
- trading_system.db: 가상매매 시스템 상태
  - virtual_positions: 현재 포지션
  - virtual_performance_stats: 성과 통계
  - system_status: 시장 레짐, 스캔 상태
"""

from __future__ import annotations

import asyncio
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from mcps.config import (
    MARKET_DB_PATHS,
    CACHE_TTL_MARKET_STATUS,
    get_market_db_path,
)
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

# ---------------------------------------------------------------------------
# AI Summary
# ---------------------------------------------------------------------------

def _ai_summary_status(data: dict) -> str:
    regime = data.get("market_regime", "unknown")
    pos = data.get("positions_summary", {})
    total = pos.get("total", 0)
    profitable = pos.get("profitable", 0)
    avg_pnl = pos.get("avg_pnl", 0)
    perf = data.get("performance", {})
    win_rate = perf.get("win_rate", 0)
    r24 = data.get("recent_24h", {})
    trades_24h = r24.get("trades", 0)
    market_id = data.get("market_id", "")
    return f"{market_id} 시장: 레짐={regime}, 포지션 {total}개(이익 {profitable}개, avg {avg_pnl:.1f}%), 승률 {win_rate:.1f}%, 24h {trades_24h}건."

# ---------------------------------------------------------------------------
# 데이터 로더 함수
# ---------------------------------------------------------------------------

def _load_market_status(market_id: str) -> Dict[str, Any]:
    """시장 상태 로드 (포지션 요약, 성과, 레짐)"""
    db_path = get_market_db_path(market_id)

    if not db_path or not db_path.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Trading DB not found for market: {market_id}",
            action=MCPErrorAction.CHECK,
            action_value="market://all/summary",
            available_markets=["crypto", "kr_stock", "us_stock"],
            market_id=market_id,
        )

    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row

            result = {
                "market_id": market_id,
                "db_path": str(db_path),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            # 1. 시스템 상태 (레짐, 스캔 상태, thinking_log)
            try:
                cursor = conn.execute("SELECT key, value FROM system_status")
                system_status = {row["key"]: row["value"] for row in cursor.fetchall()}
                result["system_status"] = system_status
                result["market_regime"] = system_status.get("market_regime", "Unknown")
                result["scanning_coins"] = system_status.get("scanning_coins", "")
                result["thinking_log"] = system_status.get("thinking_log", "")
            except Exception as e:
                result["system_status"] = {"error": str(e)}
                result["market_regime"] = "Unknown"
                result["thinking_log"] = ""

            # 1-1. thinking_log가 없으면 system_logs에서 최신 1건 조회
            if not result.get("thinking_log"):
                try:
                    cursor = conn.execute("""
                        SELECT message FROM system_logs
                        WHERE component IN ('Executor', 'RiskManager', 'Strategy')
                        ORDER BY id DESC LIMIT 1
                    """)
                    row = cursor.fetchone()
                    if row:
                        result["thinking_log"] = row["message"]
                except Exception:
                    pass  # system_logs 테이블이 없을 수 있음

            # 2. 포지션 요약
            try:
                cursor = conn.execute("""
                    SELECT
                        COUNT(*) as total_positions,
                        SUM(CASE WHEN profit_loss_pct > 0 THEN 1 ELSE 0 END) as profitable,
                        SUM(CASE WHEN profit_loss_pct <= 0 THEN 1 ELSE 0 END) as losing,
                        AVG(profit_loss_pct) as avg_pnl,
                        MAX(profit_loss_pct) as max_pnl,
                        MIN(profit_loss_pct) as min_pnl
                    FROM virtual_positions
                """)
                row = cursor.fetchone()
                if row:
                    result["positions_summary"] = {
                        "total": row["total_positions"] or 0,
                        "profitable": row["profitable"] or 0,
                        "losing": row["losing"] or 0,
                        "avg_pnl": round(row["avg_pnl"] or 0, 2),
                        "max_pnl": round(row["max_pnl"] or 0, 2),
                        "min_pnl": round(row["min_pnl"] or 0, 2),
                    }
            except Exception as e:
                result["positions_summary"] = {"error": str(e)}

            # 3. 성과 통계 (최신)
            try:
                cursor = conn.execute("""
                    SELECT total_trades, winning_trades, losing_trades,
                           win_rate, total_profit_pct, active_positions, timestamp
                    FROM virtual_performance_stats
                    ORDER BY timestamp DESC LIMIT 1
                """)
                row = cursor.fetchone()
                if row:
                    result["performance"] = {
                        "total_trades": row["total_trades"],
                        "winning_trades": row["winning_trades"],
                        "losing_trades": row["losing_trades"],
                        "win_rate": round(row["win_rate"] or 0, 2),
                        "total_profit_pct": round(row["total_profit_pct"] or 0, 2),
                        "active_positions": row["active_positions"],
                    }
            except Exception as e:
                result["performance"] = {"error": str(e)}

            # 4. 최근 거래 요약 (24시간)
            try:
                import time
                one_day_ago = int(time.time()) - 86400
                cursor = conn.execute("""
                    SELECT
                        COUNT(*) as trades_24h,
                        SUM(CASE WHEN profit_loss_pct > 0 THEN 1 ELSE 0 END) as wins_24h,
                        AVG(profit_loss_pct) as avg_pnl_24h
                    FROM virtual_trade_history
                    WHERE exit_timestamp > ?
                """, (one_day_ago,))
                row = cursor.fetchone()
                if row:
                    result["recent_24h"] = {
                        "trades": row["trades_24h"] or 0,
                        "wins": row["wins_24h"] or 0,
                        "avg_pnl": round(row["avg_pnl_24h"] or 0, 2),
                    }
            except Exception as e:
                result["recent_24h"] = {"error": str(e)}

            # LLM용 요약 텍스트
            result["_llm_summary"] = _generate_market_status_text(market_id, result)
            explanation = build_resource_explanation(
                market=market_id,
                entity_type="market",
                explanation_type="mcp_market_status",
                as_of_time=result["timestamp"],
                headline=f"{market_id} market status snapshot",
                why_text=result["_llm_summary"],
                bullet_points=[
                    f"market_regime={result.get('market_regime', 'Unknown')}",
                    f"positions={result.get('positions_summary', {}).get('total', 0)}",
                    f"win_rate={result.get('performance', {}).get('win_rate', 0)}",
                ],
                market_context_refs={
                    "market_regime": result.get("market_regime", "Unknown"),
                    "positions_summary": result.get("positions_summary", {}),
                    "performance": result.get("performance", {}),
                },
                note="derived from market status MCP resource",
            )
            return with_explanation_contract(
                result,
                resource_type="market_status",
                explanation=explanation,
            )

    except Exception as e:
        logger.error(f"Failed to load market status for {market_id}: {e}")
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Failed to load market status for {market_id}: {e}",
            market_id=market_id,
        )

def _generate_market_status_text(market_id: str, data: Dict[str, Any]) -> str:
    """LLM이 이해하기 쉬운 시장 상태 요약 텍스트"""
    try:
        lines = [f"[{market_id.upper()} 시장 상태]"]

        # 레짐
        regime = data.get("market_regime", "Unknown")
        lines.append(f"- 시장 레짐: {regime}")

        # 포지션 요약
        pos = data.get("positions_summary", {})
        if not pos.get("error"):
            total = pos.get("total", 0)
            profitable = pos.get("profitable", 0)
            avg_pnl = pos.get("avg_pnl", 0)
            lines.append(f"- 보유 포지션: {total}개 (이익 {profitable}개, 평균 수익률 {avg_pnl:+.2f}%)")

        # 성과
        perf = data.get("performance", {})
        if not perf.get("error"):
            win_rate = perf.get("win_rate", 0)
            total_pnl = perf.get("total_profit_pct", 0)
            lines.append(f"- 전체 성과: 승률 {win_rate:.1f}%, 총 수익률 {total_pnl:+.2f}%")

        # 24시간
        recent = data.get("recent_24h", {})
        if not recent.get("error"):
            trades = recent.get("trades", 0)
            wins = recent.get("wins", 0)
            lines.append(f"- 최근 24시간: {trades}건 거래, {wins}건 성공")

        return "\n".join(lines)

    except Exception as e:
        return f"요약 생성 실패: {e}"

def _load_positions_snapshot(market_id: str, limit: int = 10) -> Dict[str, Any]:
    """현재 포지션 스냅샷"""
    db_path = get_market_db_path(market_id)

    if not db_path or not db_path.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"DB not found for market: {market_id}",
            action=MCPErrorAction.CHECK,
            action_value="market://all/summary",
            available_markets=["crypto", "kr_stock", "us_stock"],
            market_id=market_id,
        )

    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row

            # virtual_positions는 symbol 컬럼 사용 (coin 없을 수 있음)
            cursor = conn.execute("""
                SELECT symbol, entry_price, current_price, profit_loss_pct,
                       entry_timestamp, holding_duration, target_price,
                       ai_score, ai_reason, current_strategy
                FROM virtual_positions
                ORDER BY profit_loss_pct DESC
                LIMIT ?
            """, (limit,))

            positions = []
            for row in cursor.fetchall():
                pos = dict(row)
                sym = pos.get("symbol") or pos.get("coin", "?")
                pos["symbol"] = sym
                # 보유 시간 포맷
                duration = pos.get("holding_duration", 0)
                hours = duration // 3600
                mins = (duration % 3600) // 60
                pos["holding_time_str"] = f"{hours}h {mins}m"
                positions.append(pos)

            # LLM용 요약
            summary_lines = [f"[{market_id.upper()} 포지션 스냅샷] 총 {len(positions)}개"]
            for p in positions[:5]:
                sym = p.get("symbol") or p.get("coin", "?")
                pnl = p.get("profit_loss_pct", 0)
                strategy = p.get("current_strategy", "?")
                summary_lines.append(f"- {sym}: {pnl:+.2f}% ({strategy})")

            result = {
                "market_id": market_id,
                "positions": positions,
                "count": len(positions),
                "_llm_summary": "\n".join(summary_lines),
            }
            explanation = build_resource_explanation(
                market=market_id,
                entity_type="market",
                explanation_type="mcp_positions_snapshot",
                as_of_time=datetime.now(timezone.utc).isoformat(),
                headline=f"{market_id} positions snapshot",
                why_text=result["_llm_summary"],
                bullet_points=[f"count={len(positions)}"],
                market_context_refs={"count": len(positions)},
                note="derived from positions snapshot MCP resource",
            )
            return with_explanation_contract(
                result,
                resource_type="positions_snapshot",
                explanation=explanation,
            )

    except Exception as e:
        logger.error(f"Failed to load positions for {market_id}: {e}")
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Failed to load positions for {market_id}: {e}",
            market_id=market_id,
        )

# ---------------------------------------------------------------------------
# Resource 등록 함수
# ---------------------------------------------------------------------------

def register_market_status_resources(mcp, cache):
    """시장 상태 관련 Resource 등록"""

    @mcp.resource("market://{market_id}/status")
    async def get_market_status(market_id: str) -> Dict[str, Any]:
        """
        [역할] 특정 시장의 현재 상태(레짐, 포지션 요약, 성과, 24h 거래).
        [호출 시점] 특정 시장 현황 빠르게 파악 시.
        [선행 조건] 없음 (시장별 최상위).
        [후속 추천] get_positions(market_id), market://{market_id}/signals/summary.
        [주의] TTL=60초. market_id=crypto,kr_stock,us_stock.
        [출력 스키마] ai_summary 래핑. full_data: market_id(str), market_regime(str), positions_summary{total,profitable,losing,avg_pnl,max_pnl,min_pnl}, performance{total_trades,winning_trades,win_rate,total_profit_pct}, recent_24h{trades,wins,avg_pnl}, _contract{...}, _llm_summary(str).

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock)
        """
        cache_key = f"market_status_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            logger.debug(f"Cache hit: {cache_key}")
            return to_resource_text(cached)

        data = await asyncio.to_thread(_load_market_status, market_id)
        if not data.get("error"):
            data = wrap_with_ai_summary(data, "market_status", _ai_summary_status)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/positions/snapshot")
    async def get_positions_snapshot(market_id: str) -> Dict[str, Any]:
        """
        [역할] 시장의 현재 포지션을 수익률순 스냅샷.
        [호출 시점] 보유 포지션 전체 현황 확인 시.
        [선행 조건] market://{market_id}/status 권장.
        [후속 추천] get_position_detail, get_strategy_distribution.
        [주의] TTL=60초. 최대 10개.
        [출력 스키마] _contract 포함. market_id(str), positions[{symbol,entry_price,current_price,profit_loss_pct,holding_time_str,ai_score,ai_reason,current_strategy}], count(int), _llm_summary(str).

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock)
        """
        cache_key = f"positions_snapshot_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            logger.debug(f"Cache hit: {cache_key}")
            return to_resource_text(cached)

        data = await asyncio.to_thread(_load_positions_snapshot, market_id)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://all/summary")
    async def get_all_markets_summary() -> Dict[str, Any]:
        """
        [역할] crypto+kr_stock+us_stock 3개 시장 상태를 한번에 반환.
        [호출 시점] 전체 포트폴리오 개요 시 가장 먼저 호출.
        [선행 조건] 없음.
        [후속 추천] market://{market_id}/status, market://unified/cross-market.
        [주의] TTL=60초. 3개 시장 순차 조회.
        [출력 스키마] timestamp(str), markets{market_id→{market_regime,positions_summary{total,profitable,avg_pnl},performance{win_rate,total_profit_pct},recent_24h{trades,wins,avg_pnl}}}, _llm_summary(str).
        """
        cache_key = "all_markets_summary"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)

        markets = ["crypto", "kr_stock", "us_stock"]
        result = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "markets": {},
        }

        summary_lines = ["[전체 시장 요약]"]

        for market_id in markets:
            status = await asyncio.to_thread(_load_market_status, market_id)
            result["markets"][market_id] = status

            # 요약 텍스트
            if not status.get("error"):
                regime = status.get("market_regime", "?")
                pos_count = status.get("positions_summary", {}).get("total", 0)
                avg_pnl = status.get("positions_summary", {}).get("avg_pnl", 0)
                summary_lines.append(f"- {market_id}: {regime}, 포지션 {pos_count}개, 평균 {avg_pnl:+.2f}%")
            else:
                summary_lines.append(f"- {market_id}: 데이터 없음")

        result["_llm_summary"] = "\n".join(summary_lines)

        cache.set(cache_key, result)
        return to_resource_text(result)

    logger.info("  📈 Market Status resources registered")
