# -*- coding: utf-8 -*-
"""
Entity Context Tools (2026-10-15, req_mcp_entity_context Stage A 1차 묶음)
==========================================================================
get_symbol_context · get_group_context · get_market_context — 진입 엔티티를 받아 근거가 붙은 연결과
빈 구간(gaps)을 공통 스키마로 돌려준다. 조립 = `mcps.entity.context`, envelope = `wrap_entity_response`.

description 라우팅 규칙(req 6.1): 첫 문장 "Use when the question is about …" · 둘째 = 출력 · 셋째 = "Not for …".
공통 문구는 3분 확률이 없으므로 req 원문의 "probability" 를 "signal-score … measured hit rates" 로 정정했다
(제안서 §1 정정 항목, 2026-10-15 승인).
"""

from __future__ import annotations

import logging
from typing import Annotated, Any, Dict, Optional

from pydantic import Field

from oneqaz_trading_mcp.resources.resource_response import normalize_choice, wrap_entity_response

logger = logging.getLogger("MarketMCP")

try:
    from oneqaz_trading_mcp.schemas import EntityContextEnvelope
except ImportError:  # pragma: no cover
    EntityContextEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]

_COMMON = (
    "Provides regime, signal-score and linkage context with sample sizes and measured hit rates; "
    "every link carries its evidence and every empty section is listed in gaps. "
    "Not a real-time quote, fundamentals, or full-text news source."
)
_ANNOTATIONS = {"readOnlyHint": True, "idempotentHint": True, "openWorldHint": False}

_DESC_SYMBOL = (
    "Use when the question is about one stock or coin — its current signal state, the OneQAZ structure group "
    "it belongs to, and which assets have tracked lead-lag links to it. "
    "Returns self-contained headline claims first, then the symbol's observed state, upstream links (its group, "
    "macro/ETF/basket instruments and symbols that move before it), peers (symbols that follow it), tagged news, "
    "and gaps that say what is not tracked. "
    "Not for sector-wide questions (use get_group_context) or whole-market questions (use get_market_context). "
    + _COMMON
    + " Accepts tickers, KRX codes or names; ambiguous inputs return candidates instead of a guess."
)
_DESC_GROUP = (
    "Use when the question is about a sector, theme, ETF basket or index basket — its structure state and its "
    "members. "
    "Returns headline claims, the group's regime state per interval, its market, groups that co-move with it, "
    "member symbols with their latest signal bias, symbols the basket leads, and gaps. "
    "Not for a single stock (use get_symbol_context) or a whole market (use get_market_context). "
    + _COMMON
)
_DESC_MARKET = (
    "Use when the question is about a whole market (KR, US, CRYPTO) — its structure regime and how its groups "
    "are positioned. "
    "Returns headline claims, the market's structure and global macro regime, the macro categories assumed to "
    "affect it (with tuned lags), its groups ordered by distance from the middle regime stage, market-tagged "
    "news, and gaps. "
    "Not for one sector or stock (use get_group_context or get_symbol_context). "
    + _COMMON
)


# req 6.8 원문 그대로. 응답 로직에 쓰지 않는다(E7) — 기록 여부는 mcps/entity/calls_log 의 스위치가 정한다(기본 off).
_QUESTION_DESC = (
    "Optional. The user's original question, used only for service improvement analytics. "
    "Does not change the response."
)


def _finish(tool: str, payload: dict, nav: list, *, input_value, market_param, limit, question) -> dict:
    """응답을 먼저 확정하고, 그 요약만 호출 원장 후보로 넘긴다 — question 은 응답 조립에 들어가지 않는다."""
    resp = wrap_entity_response(payload, nav)
    from oneqaz_trading_mcp.entity.calls_log import build_entry, stash_entry
    try:
        stash_entry(build_entry(tool, resp, input_value=input_value, market_param=market_param,
                                limit=limit, question=question))
    except Exception:  # 원장 요약 실패가 응답을 깨면 안 된다 — 코드 결함이라 크게 남긴다
        logger.error("entity calls: 원장 요약 실패 (%s)", tool, exc_info=True)
    return resp


def _market_arg(market_id: Optional[str]):
    if market_id in (None, ""):
        return None, None
    return normalize_choice("market", "market_id", market_id)


def register_entity_context_tools(mcp, cache=None):
    """엔티티 컨텍스트 1차 묶음 3종 등록."""

    @mcp.tool(name="get_symbol_context", title="Symbol Context", description=_DESC_SYMBOL,
              annotations=_ANNOTATIONS)
    async def entity_symbol_context(
        symbol: Annotated[str, Field(
            description="Ticker (NVDA), KRX 6-digit code (005930), coin base ticker (BTC) or name "
                        "(삼성전자, Samsung Electronics). Ambiguous names return candidates, not a guess."
        )],
        market_id: Annotated[Optional[str], Field(
            description="Optional. crypto / kr_stock / us_stock (aliases coin/kr/us). Narrows name resolution.",
            json_schema_extra={"enum": ["crypto", "kr_stock", "us_stock"]},
        )] = None,
        limit: Annotated[int, Field(description="Max links per section (default 8, max 50).")] = 8,
        question: Annotated[Optional[str], Field(
            description=_QUESTION_DESC,
        )] = None,
    ) -> EntityContextEnvelope:
        """description = _DESC_* (위)."""
        from oneqaz_trading_mcp.entity.context import symbol_context
        m, err = _market_arg(market_id)
        if err:
            return err
        payload, nav = symbol_context(symbol, m, limit)
        return _finish("get_symbol_context", payload, nav, input_value=symbol, market_param=m,
                       limit=limit, question=question)

    @mcp.tool(name="get_group_context", title="Group Context", description=_DESC_GROUP,
              annotations=_ANNOTATIONS)
    async def entity_group_context(
        group: Annotated[str, Field(
            description="OneQAZ structure group id (kr_semiconductor, technology, broad_index), a sector word "
                        "(반도체, energy) or an ETF ticker that belongs to a group (QQQ)."
        )],
        market_id: Annotated[Optional[str], Field(
            description="Optional. crypto / kr_stock / us_stock (aliases coin/kr/us). Group ids repeat across markets.",
            json_schema_extra={"enum": ["crypto", "kr_stock", "us_stock"]},
        )] = None,
        limit: Annotated[int, Field(description="Max links per section (default 8, max 50; members default 10).")] = 8,
        question: Annotated[Optional[str], Field(
            description=_QUESTION_DESC,
        )] = None,
    ) -> EntityContextEnvelope:
        """description = _DESC_* (위)."""
        from oneqaz_trading_mcp.entity.context import group_context
        m, err = _market_arg(market_id)
        if err:
            return err
        payload, nav = group_context(group, m, limit)
        return _finish("get_group_context", payload, nav, input_value=group, market_param=m,
                       limit=limit, question=question)

    @mcp.tool(name="get_market_context", title="Market Context", description=_DESC_MARKET,
              annotations=_ANNOTATIONS)
    async def entity_market_context(
        market: Annotated[str, Field(
            description="crypto / kr_stock / us_stock, or a market word (KR, 국장, 코스피, US, 미장, 나스닥, 코인)."
        )],
        limit: Annotated[int, Field(description="Max links per section (default 8, max 50).")] = 8,
        question: Annotated[Optional[str], Field(
            description=_QUESTION_DESC,
        )] = None,
    ) -> EntityContextEnvelope:
        """description = _DESC_* (위)."""
        from oneqaz_trading_mcp.entity.context import market_context
        payload, nav = market_context(market, limit)
        return _finish("get_market_context", payload, nav, input_value=market, market_param=None,
                       limit=limit, question=question)
