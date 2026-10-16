# -*- coding: utf-8 -*-
"""
시장 상태 Resource
==================
코인/한국주식/미국주식 시장의 현재 상태 제공

데이터 소스 (PG market_{m} 스키마 — 구 trading_system.db 논리 키):
- virtual_positions: 현재 포지션
- virtual_performance_stats: 성과 통계
- system_status: 레짐 · 스캔 상태 (⚠️ 2026-10-05 실측 3시장 0행 — writer 호출자 0)
- ⚠️ 레짐 · 성과 원천(system_status, virtual_performance_stats)이 비면 값을 지어내지 않고
  `market_regime_status` / `performance.unavailable` 로 명시한다 (반대편 스틸맨 C2).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.config import (
    MARKET_DB_PATHS,
    CACHE_TTL_MARKET_STATUS,
    get_market_db_path,
    connect_readonly,
)
from oneqaz_trading_mcp.resources.resource_response import (
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

def _block_missing(block: Any) -> bool:
    """[2026-10-05 D12] 로더의 실패 표식은 {"unavailable": True} 인데 요약부는 "error" 만
    검사해서, 실패 블록이 0 값으로 사실처럼 출력됐다 (백로그 fix_now-14). 둘 다 본다."""
    return not isinstance(block, dict) or not block or bool(block.get("error")) or bool(block.get("unavailable"))


def _ai_summary_status(data: dict) -> str:
    regime = data.get("market_regime", "unknown")
    pos = data.get("positions_summary", {})
    if _block_missing(pos):
        pos_txt = "포지션 N/A"
    else:
        pos_txt = (f"포지션 {pos.get('total', 0)}개(이익 {pos.get('profitable', 0)}개, "
                   f"avg {pos.get('avg_pnl', 0):.1f}%)")
    perf = data.get("performance", {})
    win_rate = perf.get("win_rate") if not _block_missing(perf) else None
    # [2026-10-05] 값이 없으면 0.0% 로 지어내지 않는다.
    win_rate_txt = f"{win_rate:.1f}%" if isinstance(win_rate, (int, float)) else "N/A"
    r24 = data.get("recent_24h", {})
    trades_txt = "N/A" if _block_missing(r24) else f"{r24.get('trades', 0)}건"
    market_id = data.get("market_id", "")
    return f"{market_id} 시장: 레짐={regime}, {pos_txt}, 승률 {win_rate_txt}, 24h {trades_txt}."


def _user_summary_status(data: dict) -> str:
    """인간 사용자에게 보여줄 1줄 (jargon-free, 한국어)."""
    market_id = data.get("market_id", "")
    market_label = {"crypto": "암호화폐", "kr_stock": "한국 주식", "us_stock": "미국 주식"}.get(market_id, market_id)
    pos = data.get("positions_summary", {})
    total = pos.get("total", 0)
    profitable = pos.get("profitable", 0)
    if total == 0:
        return f"{market_label} 시장에 현재 진행 중인 포지션이 없습니다."
    win_pct = round(profitable / total * 100) if total else 0
    return f"{market_label} 시장에서 현재 {total}개 종목을 가상 운용 중이며, 그 중 {profitable}개({win_pct}%)가 이익 상태입니다."

# ---------------------------------------------------------------------------
# 데이터 로더 함수
# ---------------------------------------------------------------------------

def _load_market_status(market_id: str) -> Dict[str, Any]:
    """시장 상태 로드 (포지션 요약, 성과, 레짐)"""
    db_path = get_market_db_path(market_id)

    if not db_path:  # [2026-07-03] PG 논리 키 — 파일 존재 검사 제거 (레거시 파일 삭제 시 DB_NOT_FOUND 오탐 함정)
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Trading DB not found for market: {market_id}",
            action=MCPErrorAction.CHECK,
            action_value="market://all/summary",
            available_markets=["crypto", "kr_stock", "us_stock"],
            market_id=market_id,
        )

    try:
        # Read-only URI: writer(virtual_trade_executor)와 lock contention 없이 동시 read
        with connect_readonly(db_path) as conn:

            result = {
                "market_id": market_id,
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
                logger.warning("system_status load failed for %s: %s", market_id, e)
                result["system_status"] = {"unavailable": True, "reason": "데이터 수집 중"}
                result["market_regime"] = "Unknown"
                result["thinking_log"] = ""

            # 1-0. [2026-10-05 스틸맨 C2] 레짐 원천이 비면 지어내지 않고 이유를 명시한다.
            #   한때 external_context.market_regime_flow 의 1d 다수결 라벨로 채웠으나, 그것은
            #   엔진이 쓰는 레짐(coin BTC 앵커 / kr·us 최근 30분 평균 stage — trade/core/trading.py)
            #   과 다른 개념이었고, 함께 실은 regime_trend 는 인터벌 혼합 인공물이었다.
            if result.get("market_regime") in (None, "", "Unknown"):
                result["market_regime"] = "Unknown"
                result["market_regime_status"] = {
                    "available": False,
                    "reason": (
                        "system_status has no rows (its writer has no callers); the engine's "
                        "own regime is not exposed by this resource."
                    ),
                }

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
                logger.warning("positions_summary load failed for %s: %s", market_id, e)
                result["positions_summary"] = {"unavailable": True, "reason": "데이터 수집 중"}

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
                else:
                    # [2026-10-05 D12 · 스틸맨 C2] virtual_performance_stats 가 3시장 0행이다
                    # (writer save_performance_stats 의 호출자 0). 종전엔 performance 키가 없어
                    # 요약이 기본값으로 "승률 0.0%" 를 사실처럼 냈다. 원장 파생을 한때 시도했으나
                    # 진입 행(exit_timestamp=0)까지 손실로 세어 승률을 절반으로 냈다 → 백로그 D12
                    # 원처방대로 '없음' 을 명시한다. 청산 기준 통계는 전용 tool 이 창을 명시해 계산한다.
                    result["performance"] = {
                        "unavailable": True,
                        "reason": (
                            "virtual_performance_stats has no rows (its writer has no callers); "
                            "use analyze_trades / get_trade_history for closed-trade statistics."
                        ),
                    }
            except Exception as e:
                logger.warning("performance load failed for %s: %s", market_id, e)
                result["performance"] = {"unavailable": True, "reason": "데이터 수집 중"}

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
                logger.warning("recent_24h load failed for %s: %s", market_id, e)
                result["recent_24h"] = {"unavailable": True, "reason": "데이터 수집 중"}

            # LLM용 요약 텍스트
            result["_llm_summary"] = _generate_market_status_text(market_id, result)
            explanation = build_resource_explanation(
                market=market_id,
                entity_type="market",
                explanation_type="mcp_market_status",
                as_of_time=result["timestamp"],
                headline=f"{market_id} market status snapshot",
                why_text=result["_llm_summary"],
                # [2026-10-05 D12] 값이 없을 때 0 을 기본값으로 박지 않는다 (None 그대로).
                bullet_points=[
                    f"market_regime={result.get('market_regime', 'Unknown')}",
                    f"positions={result.get('positions_summary', {}).get('total')}",
                    f"win_rate={result.get('performance', {}).get('win_rate')}",
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

        # [2026-10-05 D12] 실패 블록({"unavailable": True})은 0 이 아니라 N/A 로 적는다.
        # 포지션 요약
        pos = data.get("positions_summary", {})
        if _block_missing(pos):
            lines.append("- 보유 포지션: N/A (데이터 수집 중)")
        else:
            total = pos.get("total", 0)
            profitable = pos.get("profitable", 0)
            avg_pnl = pos.get("avg_pnl", 0)
            lines.append(f"- 보유 포지션: {total}개 (이익 {profitable}개, 평균 수익률 {avg_pnl:+.2f}%)")

        # 성과
        perf = data.get("performance", {})
        if _block_missing(perf):
            lines.append("- 전체 성과: N/A (데이터 수집 중)")
        else:
            win_rate = perf.get("win_rate")
            total_pnl = perf.get("total_profit_pct")
            # [2026-10-05] 값이 없으면 0 으로 지어내지 않는다.
            wr_txt = f"{win_rate:.1f}%" if isinstance(win_rate, (int, float)) else "N/A"
            tp_txt = f"{total_pnl:+.2f}%" if isinstance(total_pnl, (int, float)) else "N/A"
            lines.append(f"- 전체 성과: 승률 {wr_txt}, 총 수익률 {tp_txt}")

        # 24시간
        recent = data.get("recent_24h", {})
        if _block_missing(recent):
            lines.append("- 최근 24시간: N/A (데이터 수집 중)")
        else:
            trades = recent.get("trades", 0)
            wins = recent.get("wins", 0)
            lines.append(f"- 최근 24시간: {trades}건 거래, {wins}건 성공")

        return "\n".join(lines)

    except Exception as e:
        return f"요약 생성 실패: {e}"

def _load_positions_snapshot(market_id: str, limit: int = 10) -> Dict[str, Any]:
    """현재 포지션 스냅샷"""
    db_path = get_market_db_path(market_id)

    if not db_path:  # [2026-07-03] PG 논리 키 — 파일 존재 검사 제거 (레거시 파일 삭제 시 DB_NOT_FOUND 오탐 함정)
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"DB not found for market: {market_id}",
            action=MCPErrorAction.CHECK,
            action_value="market://all/summary",
            available_markets=["crypto", "kr_stock", "us_stock"],
            market_id=market_id,
        )

    try:
        # Read-only URI: writer(virtual_trade_executor)와 lock contention 없이 동시 read
        with connect_readonly(db_path) as conn:

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
            data = wrap_with_ai_summary(data, "market_status", _ai_summary_status, _user_summary_status)
        cache.set(cache_key, data)
        return to_resource_text(data)

    async def _positions_snapshot_impl(market_id: str):
        cache_key = f"positions_snapshot_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            logger.debug(f"Cache hit: {cache_key}")
            return to_resource_text(cached)

        data = await asyncio.to_thread(_load_positions_snapshot, market_id)
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
        return await _positions_snapshot_impl(market_id)

    @mcp.resource("market://{market_id}/positions")
    async def get_positions_alias(market_id: str) -> Dict[str, Any]:
        """
        [역할] `market://{market_id}/positions/snapshot` 의 별칭. 외부 가이드에서 더 자연스러운 URI.
        """
        return await _positions_snapshot_impl(market_id)

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
