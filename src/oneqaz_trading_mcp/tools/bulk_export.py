# -*- coding: utf-8 -*-
"""
Bulk Export Tools (2026-07-08)
==============================
파이프라인 소비자(자율 리서치 에이전트)용 벌크 표면 — B2AI 감사에서
"백테스트를 돌릴 데이터를 받을 수 없다"(cursor/벌크 export 0개)가
상시 편입의 관문 블로커로 지적된 것의 해소.

get_trade_outcomes_bulk: 예측→거래→결과 3단 체인을 cursor 페이지네이션으로 제공.
- 거래(virtual_trade_history) 원장 행 + 그 진입 직전의 시그널 예측
  (signal_predictions, 동일 심볼·진입 전 2h 창의 최근접)을 LATERAL 조인.
- 링크는 FK 가 아니라 시간창 최근접 매칭(best-effort) — meta 에 정직하게 명시.
- envelope 는 페이지당 1회 (대량 소비 시 보일러플레이트 절약).
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.resources.resource_response import (
    MCPErrorCode,
    mcp_error,
    wrap_tool_response,
)

logger = logging.getLogger("MarketMCP")

_MAX_PAGE = 500
_LINK_WINDOW_SEC = 7200  # 진입 전 2h 안의 최근접 예측을 링크

_MARKET_SCHEMAS = {
    "crypto": "market_coin",
    "coin": "market_coin",
    "kr_stock": "market_kr",
    "kr": "market_kr",
    "us_stock": "market_us",
    "us": "market_us",
}


def _get_trade_outcomes_bulk(
    market: str = "crypto",
    cursor: int = 0,
    limit: int = 200,
    days: int = 30,
) -> Dict[str, Any]:
    schema = _MARKET_SCHEMAS.get((market or "").lower())
    if not schema:
        return mcp_error(
            MCPErrorCode.INVALID_MARKET,
            f"unknown market '{market}' — use crypto / kr_stock / us_stock",
        )
    limit = max(1, min(int(limit), _MAX_PAGE))
    days = max(1, min(int(days), 120))
    try:
        cursor = max(0, int(cursor))
    except (TypeError, ValueError):
        return mcp_error(MCPErrorCode.MISSING_REQUIRED_FIELD, "cursor must be an integer id")

    since = int(time.time()) - days * 86400
    try:
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
        conn = open_schema_connection(schema, readonly=True)
        try:
            rows = conn.execute(
                """
                SELECT t.id, t.symbol, t.action, t.entry_price, t.exit_price, t.quantity,
                       t.profit_loss_pct, t.entry_timestamp, t.exit_timestamp,
                       t.holding_duration, t.entry_signal_score, t.entry_confidence,
                       t.signal_pattern, t.market_regime, t.volatility_regime,
                       t.policy_version, t.sizing_mode, t.sizing_multiplier,
                       t.exec_mode, t.exec_status,
                       p.id AS pred_id, p.pred_interval, p.pred_ts,
                       p.predicted_direction, p.pred_signal_score,
                       p.pred_is_correct, p.pred_strategy_id
                FROM virtual_trade_history t
                LEFT JOIN LATERAL (
                    SELECT sp.id, sp."interval" AS pred_interval, sp.timestamp AS pred_ts,
                           sp.predicted_direction, sp.signal_score AS pred_signal_score,
                           sp.is_correct AS pred_is_correct, sp.strategy_id AS pred_strategy_id
                    FROM signal_predictions sp
                    WHERE sp.symbol = t.symbol
                      AND sp.timestamp <= t.entry_timestamp
                      AND sp.timestamp > t.entry_timestamp - %s
                    ORDER BY sp.timestamp DESC
                    LIMIT 1
                ) p ON TRUE
                WHERE t.id > %s AND t.exit_timestamp >= %s
                ORDER BY t.id
                LIMIT %s
                """,
                (_LINK_WINDOW_SEC, cursor, since, limit + 1),
            ).fetchall()
        finally:
            conn.close()

        has_more = len(rows) > limit
        trades: List[Dict[str, Any]] = []
        linked = 0
        for r in rows[:limit]:
            d = dict(r)
            pred = None
            if d.get("pred_id") is not None:
                linked += 1
                pred = {
                    "prediction_id": d.pop("pred_id"),
                    "interval": d.pop("pred_interval"),
                    "predicted_at": d.pop("pred_ts"),
                    "predicted_direction": d.pop("predicted_direction"),
                    "signal_score": d.pop("pred_signal_score"),
                    "is_correct": d.pop("pred_is_correct"),
                    "strategy_id": d.pop("pred_strategy_id"),
                }
            else:
                for k in ("pred_id", "pred_interval", "pred_ts", "predicted_direction",
                          "pred_signal_score", "pred_is_correct", "pred_strategy_id"):
                    d.pop(k, None)
            d["linked_prediction"] = pred
            trades.append(d)

        next_cursor = trades[-1]["id"] if (trades and has_more) else None
        return {
            "market": market,
            "trades": trades,
            "count": len(trades),
            "linked_prediction_count": linked,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "window_days": days,
            "meta": {
                "source_tables": f"{schema}.virtual_trade_history + {schema}.signal_predictions",
                "linkage": (
                    f"linked_prediction is a best-effort temporal match (same symbol, most recent "
                    f"prediction within {_LINK_WINDOW_SEC // 3600}h before entry) — NOT a foreign key. "
                    "Treat as probable, not guaranteed, causal linkage."
                ),
                "retention_note": (
                    "signal_predictions raw rows roll off after ~30 days; daily immutable aggregates "
                    "persist in signal_predictions_daily_archive. Trades are kept long-term."
                ),
                "interpretation": (
                    "Prediction -> trade -> outcome chain for offline backtesting: entry/exit prices "
                    "and profit_loss_pct are the realized paper outcomes of the system's own signals, "
                    "with entry-time regime and sizing metadata."
                ),
            },
        }
    except Exception as exc:
        logger.exception("get_trade_outcomes_bulk failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


def _ai_summary_bulk(data: Dict[str, Any]) -> str:
    n = data.get("count", 0)
    linked = data.get("linked_prediction_count", 0)
    more = " (more pages)" if data.get("has_more") else ""
    return (
        f"{n} paper trades with realized outcomes, {linked} linked to their pre-entry "
        f"prediction{more} — cursor-paginated bulk export for offline verification."
    )


def _user_summary_bulk(data: Dict[str, Any]) -> str:
    return f"가상매매 결과 {data.get('count', 0)}건(예측 링크 {data.get('linked_prediction_count', 0)}건)을 벌크로 반환했습니다."


def _na_bulk(data: Dict[str, Any]) -> list:
    actions = []
    if data.get("has_more") and data.get("next_cursor"):
        actions.append({
            "intent": "continue bulk export",
            "tool": "get_trade_outcomes_bulk",
            "args": {"market": data.get("market", "crypto"), "cursor": data["next_cursor"], "limit": 500},
            "rationale": "Cursor pagination — fetch the next page.",
            "priority": "high",
        })
    actions.append({
        "intent": "verify the prediction ledger these trades acted on",
        "tool": "get_resolved_predictions",
        "args": {"limit": 200},
        "rationale": "Cross-check trade-linked predictions against the tamper-evident ledger.",
        "priority": "normal",
    })
    return actions


def register_bulk_export_tools(mcp, cache):
    """벌크 export tool 등록."""

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_trade_outcomes_bulk(
        market: str = "crypto",
        cursor: int = 0,
        limit: int = 200,
        days: int = 30,
    ) -> Dict[str, Any]:
        """
        Purpose: Cursor-paginated bulk export of the prediction -> trade -> outcome chain —
            paper trades with realized P&L, each linked (best-effort, same-symbol 2h window)
            to the signal prediction that preceded entry. Built for pipeline consumers who
            need offline backtesting data, not conversational snippets.
        Triggers: "give me your full trade history for backtesting", "bulk export trades",
            "예측이 실제 매매 성과로 이어졌는지 원데이터로 검증하고 싶다", "download outcomes".
        When to call: offline verification, periodic ingestion into a research pipeline,
            or auditing whether signals translate into realized outcomes.
        Prerequisites: none. For the prediction ledger itself use get_resolved_predictions.
        Next steps: follow next_cursor until has_more=false; get_resolved_predictions to
            cross-check linked predictions against the tamper-evident ledger.
        Caveats: linkage is temporal matching, NOT a foreign key (see meta.linkage).
            Paper trading only — envelope carries the standard disclaimer once per page.
        Output: full_data { market, trades[] {id, symbol, action, entry/exit price+ts,
            profit_loss_pct, holding_duration, entry_signal_score, regime fields,
            policy_version, sizing fields, linked_prediction{...}|null}, count,
            linked_prediction_count, next_cursor, has_more, meta }.

        Args:
            market: "crypto" (default) / "kr_stock" / "us_stock"
            cursor: last trade id from previous page (0 = start)
            limit: page size (max 500)
            days: exit-time window in days (max 120)

        Disclaimer: Information only, not investment advice.
        """
        result = await asyncio.to_thread(_get_trade_outcomes_bulk, market, cursor, limit, days)
        return wrap_tool_response(
            result, "trade_outcomes_bulk",
            _ai_summary_bulk, _user_summary_bulk,
            next_actions_fn=_na_bulk,
        )

    logger.info("  [OK] Bulk Export Tools registered (1 tool)")
