# -*- coding: utf-8 -*-
"""
매매 결정 조회 Tool
===================
시장별 최신 매매 결정 및 분석 근거 조회

데이터소스:
- virtual_trade_decisions: 매매 결정 이력
  - symbol, decision, signal_score, timestamp, reason, ai_score, ai_reason
- llm_factory.llm_trading_decisions: Track A(LLM) 시장 단위 판단 (symbol='MARKET', PG read-only)
  - market_id, symbol, action, confidence, regime, risk_level, reason, decided_at, details
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Annotated, Any, Dict, List, Optional

from pydantic import Field

from oneqaz_trading_mcp.config import (
    CACHE_TTL_POSITIONS,
    get_market_db_path,
    connect_readonly,
)
from oneqaz_trading_mcp.resources.resource_response import (
    mcp_error, MCPErrorCode, MCPErrorAction, wrap_tool_response, normalize_choice,
)
# [2026-09-27] T06-F4 — llm_trading_decisions 는 llm_factory PG 스키마에서 읽는다
# (trade/core/llm_trade_reader.py 와 같은 경로). SQLite conversation.db 경로 폐기.
from oneqaz_trading_mcp.shared.db.compat import open_llm_factory_connection

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
    # [2026-09-27] 정직화 — 시장 단위 판단이며 action 은 엔진 시그널과 정합 검증된 방향이다.
    # 매매 도달 가중(llm_weight)·confidence 출처는 _llm_summary 에 숫자로 실린다.
    return (
        f"{market_id} LLM 판단 {stats.get('total', 0)}건(시장 단위, 엔진 시그널 정합 검증 통과분) — "
        f"buy {stats.get('buy_count', 0)} / sell {stats.get('sell_count', 0)} / hold {stats.get('hold_count', 0)}; "
        "매매 반영 가중·confidence 출처는 _llm_summary 참조"
    )


def _user_summary_llm_decisions(data: dict) -> str:
    market_id = data.get("market_id", "")
    label = _market_label(market_id)
    stats = data.get("stats", {}) or {}
    total = stats.get("total", 0)
    if total == 0:
        return f"{label} 시장에 최근 AI 판단 이력이 없습니다."
    # [2026-09-27] 정직화 — "AI 가 매매 판단을 내렸다" 는 표현은 이 원장이 매매 결정인 것처럼
    # 읽힌다. 실제로는 시그널 엔진과 방향을 대조 검증한 시장 단위 판단 기록이다.
    return (
        f"{label} 시장에 대한 AI 의 시장 단위 판단 {total}건입니다. "
        "이 판단은 시그널 엔진과 방향을 대조해 검증한 기록이며, 매매 결정 자체는 아닙니다."
    )


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
# LLM 매매 판단 조회 (llm_factory.llm_trading_decisions — Track A 판단 원장)
# ---------------------------------------------------------------------------
# [2026-09-27] T06-F4 수리. 종전 `_get_conversation_db_path()` 는
# get_market_db_path("crypto") 를 `.rsplit("market", 1)` 로 절단해 프로젝트 루트를 얻고
# llm_factory/store/conversation.db 를 os.path.exists 로 찾았다 — Wave G(PG 이관)·
# Wave I(SQLite fallback 폐기) 이후 그 파일은 없으므로 항상 DB_NOT_FOUND
# (09-20 이후 +13건, 누적 143/144 = 5개월 dead 공개 tool). 경로가 맞았어도
# sqlite_master 조회가 한 층 더 죽어 있었다. 정석 = trade/core/llm_trade_reader.py
# (open_llm_factory_connection(readonly=True) + 명시 컬럼 zip). SQLite fallback 없음(금지항목 2).

_LLM_DECISION_COLS = (
    "market_id", "symbol", "action", "confidence",
    "regime", "risk_level", "reason", "decided_at", "details",
)
# 기록기(llm_factory/orchestrator/router.py::_upsert_llm_trading_decision)는 시장 단위
# 판단만 symbol='MARKET' 로 upsert 한다 — 종목별 행은 현재 생산자가 없다.
_LLM_MARKET_SYMBOL = "MARKET"
# 기록기가 agent 결과에 confidence 가 없을 때 넣는 고정 기본값(router.py `confidence = 0.5`).
# MarketAgent._parse_response 는 `confidence` 키를 복사하지 않으므로 실측상 전 행이 이 값이다
# (llm_trading_decisions_history 09-20 이후 3,105행 DISTINCT confidence = 1, 2026-10-04).
# [W2A T06-F1] 2026-10-05 부터 모델 산출 원값은 **details.llm_confidence_raw** 에 섀도우 기록된다
# (컬럼은 여전히 0.5 상수 — U-9 판정 입력일 뿐 매매·strength 비접촉). 응답에서는 컬럼 값을
# "LLM 이 산출한 확률" 로 방송하지 않는다 — 원값은 그대로 두고 출처를 명시한다.
_LLM_WRITER_DEFAULT_CONFIDENCE = 0.5


def _llm_row_to_dict(row: Any) -> Dict[str, Any]:
    """명시 컬럼 zip 으로 dict 화 (llm_trade_reader._row_to_dict 패턴).

    SELECT 가 `_LLM_DECISION_COLS` 순서 그대로이므로 shim 의 행 타입(_RowView/tuple)에
    의존하지 않는다. decided_at 은 PG timestamptz → datetime 이므로 ISO(UTC) 문자열로 직렬화.
    """
    d = dict(zip(_LLM_DECISION_COLS, tuple(row)))
    ts = d.get("decided_at")
    if isinstance(ts, datetime):
        d["decided_at"] = ts.astimezone(timezone.utc).isoformat()
    return d


def _get_track_a_weight(market_id: str) -> Optional[float]:
    """market_{m}.llm_weight_tracker 의 현재 llm_weight — Track A 판단이 매매에 닿는 가중.

    행이 없으면 None. (trade/core/llm_weight_manager.get_llm_weight 는 이때
    _INITIAL_WEIGHT=0.05 를 돌려주지만 외부 노출 응답에서 그 기본값은 거짓이다.)
    """
    db_path = get_market_db_path(market_id)
    if not db_path:
        return None
    with connect_readonly(db_path, timeout=5) as conn:
        row = conn.execute(
            "SELECT llm_weight FROM llm_weight_tracker WHERE market_id = ?",
            (market_id,),
        ).fetchone()
    if not row or row[0] is None:
        return None
    return float(row[0])


def _get_llm_trading_decisions(
    market_id: str,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    """llm_factory.llm_trading_decisions 에서 Track A(LLM) 시장 판단 조회 (PG read-only)."""
    market_id, _err = normalize_choice("market", "market_id", market_id)
    if _err:
        return _err
    if not market_id:
        return mcp_error(
            MCPErrorCode.INVALID_MARKET, "market_id is required (crypto, kr_stock, us_stock)",
            action_value="crypto", param="market_id",
        )
    symbol = (symbol or "").strip().upper() or None

    try:
        with open_llm_factory_connection(readonly=True, logical_name="conversation") as conn:
            cols = ", ".join(_LLM_DECISION_COLS)
            if symbol:
                rows = conn.execute(
                    f"SELECT {cols} FROM llm_trading_decisions "
                    "WHERE market_id = ? AND symbol = ?",
                    (market_id, symbol),
                ).fetchall()
            else:
                # 종전 ORDER BY confidence DESC 는 confidence 가 상수라 무의미 — 최신순.
                rows = conn.execute(
                    f"SELECT {cols} FROM llm_trading_decisions "
                    "WHERE market_id = ? ORDER BY decided_at DESC, symbol",
                    (market_id,),
                ).fetchall()
        decisions = [_llm_row_to_dict(r) for r in rows]
    except Exception as e:
        logger.error("Failed to get LLM decisions for %s: %s", market_id, e, exc_info=True)
        return mcp_error(MCPErrorCode.NO_DATA, str(e), market_id=market_id)

    buy_count = sum(1 for d in decisions if d.get("action") == "buy")
    sell_count = sum(1 for d in decisions if d.get("action") == "sell")
    hold_count = sum(1 for d in decisions if d.get("action") == "hold")

    # Track A 가중은 보조 맥락이라 조회 실패가 본 페이로드를 죽이면 안 된다(명시적 사유).
    # 실패는 warning 으로 남기고 요약에는 "미확인" 으로 적는다(가짜 기본값 금지).
    track_a_weight: Optional[float] = None
    weight_lookup_failed = False
    try:
        track_a_weight = _get_track_a_weight(market_id)
    except Exception as e:
        weight_lookup_failed = True
        logger.warning("llm_weight_tracker 조회 실패 (%s): %s", market_id, e, exc_info=True)

    llm_summary = _build_llm_decisions_summary(
        market_id, symbol, decisions, buy_count, sell_count, hold_count,
        track_a_weight, weight_lookup_failed,
    )

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


def _build_llm_decisions_summary(
    market_id: str,
    symbol: Optional[str],
    decisions: List[Dict[str, Any]],
    buy_count: int,
    sell_count: int,
    hold_count: int,
    track_a_weight: Optional[float],
    weight_lookup_failed: bool,
) -> str:
    """AI 소비자용 서술 — 판단의 출처와 매매 도달 여부를 숫자와 함께 적는다(정직화)."""
    lines = [
        f"[{market_id.upper()} LLM 매매 판단] {len(decisions)}건: "
        f"매수 {buy_count}, 매도 {sell_count}, 홀드 {hold_count}"
    ]
    if not decisions:
        if symbol and symbol != _LLM_MARKET_SYMBOL:
            lines.append(
                f"- symbol={symbol} 행 없음 — 기록기는 시장 단위(symbol={_LLM_MARKET_SYMBOL})만 "
                "기록한다. symbol 을 생략하면 시장 판단이 반환된다"
            )
        else:
            lines.append("- 기록 없음 (LLM 분석 사이클 미실행 또는 grounding 검증 미통과)")
    else:
        latest = decisions[0]
        ts = latest.get("decided_at")
        age_txt = "시각 미상"
        if isinstance(ts, str):
            try:
                age_min = (datetime.now(timezone.utc) - datetime.fromisoformat(ts)).total_seconds() / 60
                age_txt = f"{age_min:.0f}분 전"
            except ValueError:
                pass
        lines.append(
            f"- 최신: symbol={latest.get('symbol')} action={latest.get('action')} "
            f"regime={latest.get('regime')} risk={latest.get('risk_level')} ({age_txt}, {ts})"
        )
        lines.append(
            "- 출처: action 은 LLM 방향(details.direction)이 엔진 시그널과 grounding 검증을 "
            "통과한 결과 — 시그널과 독립된 예측이 아니다"
        )
        if {d.get("confidence") for d in decisions} == {_LLM_WRITER_DEFAULT_CONFIDENCE}:
            lines.append(
                f"- confidence {_LLM_WRITER_DEFAULT_CONFIDENCE} = 기록기 고정 기본값"
                "(에이전트가 confidence 를 산출하지 않음) — 캘리브레이션된 확률이 아니다"
            )
    if weight_lookup_failed:
        lines.append("- 매매 반영 가중(llm_weight): 조회 실패 — 미확인")
    elif track_a_weight is None:
        lines.append("- 매매 반영 가중(llm_weight): 기록 없음 — 미확인")
    elif track_a_weight == 0:
        lines.append("- 매매 반영 가중(llm_weight)=0 — 현재 이 판단은 매매 결정에 닿지 않는다")
    else:
        lines.append(
            f"- 매매 반영 가중(llm_weight)={track_a_weight:.3f} — 엔진 시그널 위에 캡 범위의 bias 로만 반영"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tool 등록 함수
# ---------------------------------------------------------------------------

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

    @mcp.tool(title='Latest Decisions', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_latest_decisions(
        market_id: Annotated[str, Field(
            description='Market ID (crypto, kr_stock, us_stock; aliases coin/kr/us accepted)', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )],
        limit: Annotated[int, Field(
            description='Max results (default 10)'
        )] = 10,
        decision_filter: Annotated[Optional[str], Field(
            description='Filter by decision (buy, sell, hold)'
        )] = None,
        hours_back: Annotated[Optional[int], Field(
            description='Only decisions within last N hours'
        )] = None,
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

    @mcp.tool(title='LLM Trading Decisions', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_llm_trading_decisions(
        market_id: Annotated[str, Field(
            description='Market ID (crypto, kr_stock, us_stock). Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )],
        symbol: Annotated[Optional[str], Field(
            description='Specific symbol (optional; omit for entire market)'
        )] = None,
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

        """
        result = await asyncio.to_thread(_get_llm_trading_decisions, market_id=market_id, symbol=symbol)
        return wrap_tool_response(
            result, "decisions",
            _ai_summary_llm_decisions, _user_summary_llm_decisions,
            next_actions_fn=for_llm_trading_decisions,
            followup_questions_fn=fq_llm_trading_decisions,
        )
    
    logger.info("  [OK] Decision Tools registered (+ LLM Trading Decisions)")
