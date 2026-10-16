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
import os
import time
from typing import Annotated, Any, Dict, List, Optional

from pydantic import Field

from oneqaz_trading_mcp.resources.resource_response import (
    MCPErrorCode,
    mcp_error,
    wrap_tool_response,
)

logger = logging.getLogger("MarketMCP")

# [2026-09-23 P0-B] pinned outputSchema — FastMCP 가 반환 annotation 을
# introspect 해 outputSchema 를 만든다. pydantic 부재 시 Dict 로 격하.
try:
    from oneqaz_trading_mcp.schemas import TradeOutcomesBulkEnvelope
except ImportError:  # pragma: no cover
    TradeOutcomesBulkEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]



_MAX_PAGE = 500
# [2026-09-22] 기본 200 → 50. 이 tool 은 이미 cursor 관용구(has_more/next_cursor)를
# 갖고 있어 잘림 노출이 완비돼 있다 — 기본값만 내리면 되고, 전량이 필요한 호출자는
# next_cursor 로 이어받으면 손실이 없다. (trade_history/positions 와 달리 집계
# 통계가 없어 표본/페이지 분리도 불필요하다.)
_BULK_PAGE_DEFAULT = 50
_LINK_WINDOW_SEC = 7200  # 진입 전 2h 안의 최근접 예측을 링크
# [2026-09-21] 원장 보존일수 — 엔진과 같은 env 키를 읽는다 (retention_note 용).
_LEDGER_RETENTION_DAYS = int(
    int(os.environ.get("ACCURACY_LEDGER_RETENTION_SEC", 30 * 86400)) / 86400)

_MARKET_SCHEMAS = {
    "crypto": "market_coin",
    "coin": "market_coin",
    "kr_stock": "market_kr",
    "kr": "market_kr",
    "us_stock": "market_us",
    "us": "market_us",
}

# [2026-10-11] 기본 호출 지연 수리 (Mac 요청서 2026-10-10 #8 — get_trade_outcomes_bulk({}) p50 8.5s · 최대 39s).
#   원인 = 09-22 에 넣은 실보유 범위 메타 쿼리 `SELECT MIN("timestamp"), MAX("timestamp") FROM signal_predictions`.
#   조건 없는 MIN/MAX 라 coin 1,300만 행 인덱스를 끝까지 훑고 힙 가시성 확인을 213만 번 한다(버퍼 2.1M) —
#   데워진 캐시에서 1.3s, 크롤러가 오는 6시간 간격이면 캐시가 밀려 5~15s. 09-21 까지 같은 호출은 0.4~1.6s 였다.
#   ⇒ 심볼별 (symbol, timestamp) 인덱스 양 끝만 찍는 loose index scan 으로 같은 값을 얻는다(coin 475 심볼 ·
#   버퍼 5,895 · 14ms). symbol·timestamp 는 NOT NULL 이라 전체 MIN/MAX 와 정확히 같다(3시장 실측 일치).
#   ⛔ 다시 `MIN/MAX(...) FROM signal_predictions` 로 되돌리지 말 것 — tests/validation/test_bulk_export_query.py 가 버퍼 상한으로 막는다.
_AVAILABLE_RANGE_SQL = """
    WITH RECURSIVE s AS (
        (SELECT symbol FROM signal_predictions ORDER BY symbol LIMIT 1)
        UNION ALL
        SELECT (SELECT sp.symbol FROM signal_predictions sp
                 WHERE sp.symbol > s.symbol ORDER BY sp.symbol LIMIT 1)
        FROM s WHERE s.symbol IS NOT NULL
    )
    SELECT MIN(lo) AS oldest, MAX(hi) AS newest FROM (
        SELECT (SELECT sp."timestamp" FROM signal_predictions sp WHERE sp.symbol = s.symbol
                 ORDER BY sp."timestamp" ASC LIMIT 1) AS lo,
               (SELECT sp."timestamp" FROM signal_predictions sp WHERE sp.symbol = s.symbol
                 ORDER BY sp."timestamp" DESC LIMIT 1) AS hi
        FROM s WHERE s.symbol IS NOT NULL
    ) x
"""

# [2026-10-11] 같은 수리 — 거래 페이지를 **먼저** 자르고 그 행에만 LATERAL 링크를 붙인다.
#   종전(조인 후 ORDER BY id LIMIT)은 virtual_trade_history 통계가 낡아(30일 창을 14행으로 추정, 실제 20,995)
#   창 안 거래 전부에 LATERAL 을 돌린 뒤 정렬해 51행만 썼다(coin 0.35s · 버퍼 107k → 55ms · 22k).
#   LIMIT 이 든 서브쿼리는 풀어 올려지지 않으므로 계획과 무관하게 LATERAL 은 페이지 크기만큼만 돈다.
#   LEFT JOIN LATERAL ... LIMIT 1 은 행을 늘리지 않아 결과·순서가 종전과 같다(3시장 × 9 인자 조합 실측 일치).
#   파라미터 순서: (cursor, since, limit + 1, link_window_sec).
_OUTCOMES_SQL = """
    SELECT t.id, t.symbol, t.action, t.entry_price, t.exit_price, t.quantity,
           t.profit_loss_pct, t.entry_timestamp, t.exit_timestamp,
           t.holding_duration, t.entry_signal_score, t.entry_confidence,
           t.signal_pattern, t.market_regime, t.volatility_regime,
           t.policy_version, t.sizing_mode, t.sizing_multiplier,
           t.exec_mode, t.exec_status,
           p.id AS pred_id, p.pred_interval, p.pred_ts,
           p.predicted_direction, p.pred_signal_score,
           p.pred_is_correct, p.pred_strategy_id
    FROM (
        SELECT id, symbol, action, entry_price, exit_price, quantity,
               profit_loss_pct, entry_timestamp, exit_timestamp,
               holding_duration, entry_signal_score, entry_confidence,
               signal_pattern, market_regime, volatility_regime,
               policy_version, sizing_mode, sizing_multiplier,
               exec_mode, exec_status
        FROM virtual_trade_history
        WHERE id > %s AND exit_timestamp >= %s
        ORDER BY id
        LIMIT %s
    ) t
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
    ORDER BY t.id
"""


def _get_trade_outcomes_bulk(
    market: str = "crypto",
    cursor: int = 0,
    limit: int = _BULK_PAGE_DEFAULT,
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
    # [2026-09-22] 실보유 범위 실측 — 정책값과 분리해 둘 다 응답에 싣는다(아래 retention_note 참조).
    #   실패해도 본 응답을 막지 않는다(부가 메타).
    _available_days: Optional[float] = None
    _available_from: Optional[str] = None
    try:
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection as _osc
        _c = _osc(schema, readonly=True)
        try:
            _r = _c.execute(_AVAILABLE_RANGE_SQL).fetchone()
            if _r and _r[0] and _r[1]:
                _available_days = round((int(_r[1]) - int(_r[0])) / 86400.0, 1)
                _available_from = time.strftime(
                    "%Y-%m-%d", time.gmtime(int(_r[0]))
                )
        finally:
            try:
                _c.close()
            except Exception:
                pass
    except Exception as _avail_err:  # 부가 메타 실패는 본 응답을 막지 않는다 (사유 명시)
        logger.warning("bulk_export 실보유 범위 조회 실패(메타만 누락): %s", _avail_err)

    try:
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
        conn = open_schema_connection(schema, readonly=True)
        try:
            rows = conn.execute(
                _OUTCOMES_SQL, (cursor, since, limit + 1, _LINK_WINDOW_SEC),
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
                # [2026-09-21] 보존기간을 env 에서 계산한다. 종전엔 "~30 days" 가 하드코딩이라
                # 보존창을 365일로 늘린 뒤에는 **외부 호출자에게 나가는 거짓 서술**이 된다.
                #
                # [2026-09-22 정정] 🔴 **그 수리가 새 거짓을 만들었다.**
                #   정책값(365)을 그대로 내보내니 "raw rows roll off after ~365 days" 가 됐는데,
                #   보존창 연장은 **소급되지 않는다** — 실제 보유는 **30.7일**(최고령 2026-08-22,
                #   2026-09-22 실측)이고 365일이 차는 데 11개월이 걸린다.
                #   외부 호출자가 `days=120` 을 요청하면 **31일치만 받으면서 설명은 365일**이라고
                #   읽는다. "하드코딩이 정책에서 어긋남" 을 고치다가 "정책이 실물에서 어긋남" 을
                #   만든 것이다.
                #   ⇒ **정책과 실물을 분리해 둘 다 내보낸다.** 둘 중 하나만 내보내면 언제든 거짓이 된다.
                "retention_policy_days": _LEDGER_RETENTION_DAYS,
                "raw_rows_available_days": _available_days,
                "raw_rows_available_from": _available_from,
                "retention_note": (
                    f"Retention POLICY is ~{_LEDGER_RETENTION_DAYS} days, but the policy is NOT "
                    f"retroactive: rows older than the previous window were already deleted. "
                    f"ACTUAL raw history currently available is ~{_available_days} days"
                    + (f" (from {_available_from})" if _available_from else "")
                    + ". Requests beyond that return fewer rows than asked — use "
                    "raw_rows_available_days, not the policy, to size backtest windows. "
                    "Daily immutable aggregates persist in signal_predictions_daily_archive. "
                    "Trades are kept long-term."
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

    @mcp.tool(title='Trade Outcomes (Bulk Export)', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_trade_outcomes_bulk(
        market: Annotated[str, Field(
            description='"crypto" (default) / "kr_stock" / "us_stock"'
        )] = 'crypto',
        cursor: Annotated[int, Field(
            description='last trade id from previous page (0 = start)'
        )] = 0,
        limit: Annotated[int, Field(
            description=('Page size (default 50, max 500). Cursor-paginated: follow '
                         'next_cursor while has_more is true to retrieve everything.')
        )] = _BULK_PAGE_DEFAULT,
        days: Annotated[int, Field(
            description='exit-time window in days (max 120)'
        )] = 30,
    ) -> TradeOutcomesBulkEnvelope:
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

        """
        result = await asyncio.to_thread(_get_trade_outcomes_bulk, market, cursor, limit, days)
        return wrap_tool_response(
            result, "trade_outcomes_bulk",
            _ai_summary_bulk, _user_summary_bulk,
            next_actions_fn=_na_bulk,
        )

    logger.info("  [OK] Bulk Export Tools registered (1 tool)")
