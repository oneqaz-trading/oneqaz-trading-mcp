# -*- coding: utf-8 -*-
"""
매매 결정 조회 Tool
===================
시장별 최신 매매 결정 및 분석 근거 조회

데이터소스:
- virtual_trade_decisions: 매매 결정 이력
  - symbol, decision, signal_score, timestamp, reason, ai_score, ai_reason
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.config import (
    CACHE_TTL_POSITIONS,
    get_market_db_path,
    connect_readonly,
)
from oneqaz_trading_mcp.resources.resource_response import mcp_error, MCPErrorCode, MCPErrorAction, wrap_tool_response

# FastMCP introspects the function return annotation to build outputSchema.
try:
    from oneqaz_trading_mcp.schemas import LatestDecisionsEnvelope, LlmTradingDecisionsEnvelope
except ImportError:  # pragma: no cover
    LatestDecisionsEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    LlmTradingDecisionsEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]

logger = logging.getLogger("MarketMCP")

_MARKET_LABEL_KO = {"crypto": "암호화폐", "kr_stock": "한국 주식", "us_stock": "미국 주식"}


def _market_label(market_id: str) -> str:
    return _MARKET_LABEL_KO.get(market_id, market_id)


# ---------------------------------------------------------------------------
# AI / 사용자 요약
# ---------------------------------------------------------------------------

def _ai_summary_decisions(data: dict) -> str:
    market_id = data.get("market_id", "")
    stats = data.get("stats", {}) or {}
    return (
        f"{market_id} 매매 결정 {stats.get('total', 0)}건 — "
        f"buy {stats.get('buy_count', 0)} / sell {stats.get('sell_count', 0)} / hold {stats.get('hold_count', 0)}"
    )


def _user_summary_decisions(data: dict) -> str:
    market_id = data.get("market_id", "")
    label = _market_label(market_id)
    stats = data.get("stats", {}) or {}
    total = stats.get("total", 0)
    if total == 0:
        return f"{label} 시장에 최근 매매 결정 이력이 없습니다."
    buy = stats.get("buy_count", 0)
    sell = stats.get("sell_count", 0)
    return f"{label} 시장에서 최근 {total}건의 매매 결정이 있었으며, 매수 {buy}건·매도 {sell}건이 기록되어 있습니다."


def _ai_summary_llm_decisions(data: dict) -> str:
    market_id = data.get("market_id", "")
    stats = data.get("stats", {}) or {}
    return (
        f"{market_id} LLM 판단 {stats.get('total', 0)}건 — "
        f"buy {stats.get('buy_count', 0)} / sell {stats.get('sell_count', 0)} / hold {stats.get('hold_count', 0)}"
    )


def _user_summary_llm_decisions(data: dict) -> str:
    market_id = data.get("market_id", "")
    label = _market_label(market_id)
    stats = data.get("stats", {}) or {}
    total = stats.get("total", 0)
    if total == 0:
        return f"{label} 시장에 최근 AI 판단 이력이 없습니다."
    return f"{label} 시장에서 AI 에이전트가 최근 {total}건의 매매 판단을 내렸습니다."


# ---------------------------------------------------------------------------
# 결정 이력 조회 함수
# ---------------------------------------------------------------------------

def _get_latest_decisions(
    market_id: str,
    limit: int = 10,
    decision_filter: Optional[str] = None,
    hours_back: Optional[int] = None,
) -> Dict[str, Any]:
    """최신 매매 결정 조회"""
    db_path = get_market_db_path(market_id)

    # [2026-08-10] .exists() 게이트 제거 — db_path 는 PG 라우팅용 논리 키
    # (connect_readonly 가 PG 스키마로 라우팅, 실파일 안 엶 — trade_history.py 동일 수리)
    if not db_path:
        return mcp_error(MCPErrorCode.DB_NOT_FOUND, f"Trading DB not found for market: {market_id}", fallback_tool="market://all/summary", available_markets=["crypto", "kr_stock", "us_stock"])
    
    try:
        with connect_readonly(db_path) as conn:

            # 테이블 존재 여부 확인
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='virtual_trade_decisions'"
            )
            if not cursor.fetchone():
                return mcp_error(MCPErrorCode.TABLE_NOT_FOUND, "virtual_trade_decisions table not found", action=MCPErrorAction.CHECK, action_value="market://info", market_id=market_id)
            
            # 컬럼 확인
            cursor = conn.execute("PRAGMA table_info(virtual_trade_decisions)")
            columns = [col[1] for col in cursor.fetchall()]
            
            # 기본 쿼리 구성 (symbol 컬럼 사용)
            sym_col = "symbol" if "symbol" in columns else "coin"
            select_cols = [sym_col, "decision", "signal_score", "timestamp", "reason"]
            if "ai_score" in columns:
                select_cols.append("ai_score")
            if "ai_reason" in columns:
                select_cols.append("ai_reason")
            
            query = f"SELECT {', '.join(select_cols)} FROM virtual_trade_decisions WHERE 1=1"
            params: List[Any] = []
            
            # 결정 필터 (buy, sell, hold 등)
            if decision_filter:
                query += " AND decision = ?"
                params.append(decision_filter)
            
            # 시간 필터
            if hours_back is not None:
                import time
                cutoff = int(time.time()) - (hours_back * 3600)
                query += " AND timestamp >= ?"
                params.append(cutoff)
            
            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)
            
            cursor = conn.execute(query, params)
            decisions = []
            
            for row in cursor.fetchall():
                decision = dict(row)
                
                # 타임스탬프 포맷
                ts = decision.get("timestamp")
                if ts:
                    try:
                        if isinstance(ts, (int, float)):
                            decision["timestamp_str"] = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            decision["timestamp_str"] = str(ts)
                    except:
                        decision["timestamp_str"] = str(ts)
                
                decisions.append(decision)
            
            # 통계
            stats = {
                "total": len(decisions),
                "buy_count": sum(1 for d in decisions if d.get("decision") == "buy"),
                "sell_count": sum(1 for d in decisions if d.get("decision") == "sell"),
                "hold_count": sum(1 for d in decisions if d.get("decision") == "hold"),
            }
            
            # LLM 요약
            llm_summary = _generate_decisions_summary(market_id, decisions, stats)
            
            return {
                "market_id": market_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "decisions": decisions,
                "stats": stats,
                "_llm_summary": llm_summary,
            }
            
    except Exception as e:
        logger.error(f"Failed to get decisions for {market_id}: {e}")
        return mcp_error(MCPErrorCode.NO_DATA, str(e), market_id=market_id)


def _generate_decisions_summary(market_id: str, decisions: List[Dict], stats: Dict) -> str:
    """LLM용 결정 요약 텍스트"""
    if not decisions:
        return f"[{market_id.upper()} 매매 결정] 최근 결정 이력 없음"
    
    lines = [f"[{market_id.upper()} 최근 매매 결정]"]
    lines.append(f"- 총 {stats['total']}건: 매수 {stats['buy_count']}, 매도 {stats['sell_count']}, 홀드 {stats['hold_count']}")
    
    # 최신 3건 요약
    for d in decisions[:3]:
        sym = d.get("symbol") or d.get("coin", "?")
        decision = d.get("decision", "?")
        reason = d.get("ai_reason") or d.get("reason") or ""
        if len(reason) > 50:
            reason = reason[:47] + "..."
        lines.append(f"  - {sym}: {decision} - {reason}")
    
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LLM 매매 판단 조회 (conversation.db → llm_trading_decisions)
# ---------------------------------------------------------------------------

def _get_conversation_db_path() -> Optional[str]:
    project_root = str(get_market_db_path("crypto") or "").rsplit("market", 1)[0]
    if not project_root:
        import pathlib
        project_root = str(pathlib.Path(__file__).resolve().parents[2])
    path = os.path.join(project_root, "llm_factory", "store", "conversation.db")
    return path if os.path.exists(path) else None


def _get_llm_trading_decisions(
    market_id: str,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    """conversation.db의 llm_trading_decisions 테이블에서 LLM 매매 판단 조회"""
    db_path = _get_conversation_db_path()
    if not db_path:
        return mcp_error(MCPErrorCode.DB_NOT_FOUND, "conversation.db not found", fallback_tool="get_latest_decisions", fallback_note="Use Track B decisions instead")

    try:
        with connect_readonly(db_path, timeout=5) as conn:

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='llm_trading_decisions'"
            )
            if not cursor.fetchone():
                return mcp_error(MCPErrorCode.TABLE_NOT_FOUND, "llm_trading_decisions table not found", action=MCPErrorAction.CHECK, action_value="market://info", fallback_tool="get_latest_decisions")

            if symbol:
                rows = conn.execute(
                    "SELECT * FROM llm_trading_decisions WHERE market_id = ? AND symbol = ?",
                    (market_id, symbol),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM llm_trading_decisions WHERE market_id = ? ORDER BY confidence DESC",
                    (market_id,),
                ).fetchall()

            decisions = [dict(r) for r in rows]

            buy_count = sum(1 for d in decisions if d.get("action") == "buy")
            sell_count = sum(1 for d in decisions if d.get("action") == "sell")
            hold_count = sum(1 for d in decisions if d.get("action") == "hold")

            llm_summary = f"[{market_id.upper()} LLM 매매 판단] {len(decisions)}건: 매수 {buy_count}, 매도 {sell_count}, 홀드 {hold_count}"

            return {
                "market_id": market_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "decisions": decisions,
                "stats": {
                    "total": len(decisions),
                    "buy_count": buy_count,
                    "sell_count": sell_count,
                    "hold_count": hold_count,
                },
                "_llm_summary": llm_summary,
            }
    except Exception as e:
        logger.error(f"Failed to get LLM decisions for {market_id}: {e}")
        return mcp_error(MCPErrorCode.NO_DATA, str(e), market_id=market_id)


# ---------------------------------------------------------------------------
# Tool 등록 함수
# ---------------------------------------------------------------------------

import os

def register_decision_tools(mcp, cache):
    """결정 관련 Tool 등록"""
    # Phase 5+B (2026-05-07): 응답 데이터 기반 _next_actions 추천.
    from oneqaz_trading_mcp.tools.next_actions import (
        for_latest_decisions, for_llm_trading_decisions,
    )
    # Phase D-E (2026-05-07): user-facing followup questions.
    from oneqaz_trading_mcp.tools.followup_questions import (
        fq_latest_decisions, fq_llm_trading_decisions,
    )

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_latest_decisions(
        market_id: str,
        limit: int = 10,
        decision_filter: str = None,
        hours_back: int = None,
    ) -> LatestDecisionsEnvelope:
        """
        Purpose: Track-B (signal-driven) paper-trading decision log
            (Track B = the signal-engine decision path — indicator/Thompson-sampling driven;
            Track A = the LLM judgement path, see get_llm_trading_decisions).
        Triggers (casual questions too): "what did the system decide?", "최근에 뭐 샀어? 팔았어?",
            "why did you buy X?", "show recent buy/sell calls", "오늘 매매 판단 뭐 했어?",
            "any trades triggered today?".
        When to call: review recent automated decisions and their outcomes.
        Prerequisites: market://{market_id}/status recommended for context.
        Next steps: get_trade_history, get_signals.
        Caveats: paper-trading decisions only — no real-money order routing.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock; aliases coin/kr/us accepted)
            limit: Max results (default 10)
            decision_filter: Filter by decision (buy, sell, hold)
            hours_back: Only decisions within last N hours

        Disclaimer: Information only, not investment advice.
        """
        result = await asyncio.to_thread(
            _get_latest_decisions,
            market_id=market_id,
            limit=limit,
            decision_filter=decision_filter,
            hours_back=hours_back,
        )
        return wrap_tool_response(
            result, "decisions",
            _ai_summary_decisions, _user_summary_decisions,
            next_actions_fn=for_latest_decisions,
            followup_questions_fn=fq_latest_decisions,
        )

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_llm_trading_decisions(
        market_id: str,
        symbol: str = None,
    ) -> LlmTradingDecisionsEnvelope:
        """
        Purpose: Track-A (LLM-driven) paper-trading judgement log
            (Track A = the LLM judgement path, applied to trading only as a capped bias
            on top of engine signals; Track B = the signal-engine path, see get_latest_decisions).
        Triggers (casual questions too): "what does the AI think?", "AI는 뭘 사라고 해?",
            "show the LLM's trade calls", "AI 판단 근거 보여줘", "does the AI agree with the signals?".
        When to call: inspect LLM-generated reasoning and trade calls.
        Prerequisites: none.
        Next steps: get_latest_decisions to compare with Track B.
        Caveats: paper-trading only.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock, commodity, forex, bond)
            symbol: Specific symbol (optional; omit for entire market)

        Disclaimer: Information only, not investment advice.
        """
        result = await asyncio.to_thread(_get_llm_trading_decisions, market_id=market_id, symbol=symbol)
        return wrap_tool_response(
            result, "decisions",
            _ai_summary_llm_decisions, _user_summary_llm_decisions,
            next_actions_fn=for_llm_trading_decisions,
            followup_questions_fn=fq_llm_trading_decisions,
        )
    
    logger.info("  [OK] Decision Tools registered (+ LLM Trading Decisions)")
