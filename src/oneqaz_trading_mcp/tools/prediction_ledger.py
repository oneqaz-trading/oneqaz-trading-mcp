# -*- coding: utf-8 -*-
"""
Prediction Ledger Tools (2026-07-08)
====================================
예측 원장의 "검증 루프를 닫는" 도구 2종 — B2AI 감사 판정에서 3자 공통
인용거절 1순위였던 "사후 수정 불가 증명 부재"의 해소.

- get_resolved_predictions: 개별 예측의 전체 생애(created→resolved→outcome)를
  원시 행 그대로 커서 페이지네이션으로 노출. 집계(24셀)만 보이던 resolved 33k건을
  외부 AI 가 직접 재검증할 수 있게 한다.
- get_ledger_integrity: 일별 SHA-256 해시 체인 서빙. 외부 관찰자가 해시를
  아카이브해두면 이후의 조용한 원장 수정이 재계산 불일치로 드러난다.

데이터 소스: market_global.macro_regime_predictions (PG 라우팅),
             mcp_analytics.prediction_ledger_hashes (해시 체인).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Annotated, Any, Dict, List, Optional

from pydantic import Field

from oneqaz_trading_mcp.config import GLOBAL_REGIME_DIR
from oneqaz_trading_mcp.resources.resource_response import (
    normalize_choice,
    MCPErrorCode,
    mcp_error,
    wrap_tool_response,
)

logger = logging.getLogger("MarketMCP")

# [2026-09-23 P0-B] pinned outputSchema — FastMCP 가 반환 annotation 을
# introspect 해 outputSchema 를 만든다. pydantic 부재 시 Dict 로 격하.
try:
    from oneqaz_trading_mcp.schemas import LedgerIntegrityEnvelope, ResolvedPredictionsEnvelope
except ImportError:  # pragma: no cover
    LedgerIntegrityEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    ResolvedPredictionsEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]



_MAX_PAGE = 500


def _open_predictions_ro():
    from oneqaz_trading_mcp.shared.db.compat import connect_readonly
    return connect_readonly(str(GLOBAL_REGIME_DIR / "global_predictions.db"), timeout=10)


# ---------------------------------------------------------------------------
# get_resolved_predictions
# ---------------------------------------------------------------------------

def _get_resolved_predictions(
    target_market: Optional[str] = None,
    source_category: Optional[str] = None,
    day: Optional[str] = None,
    status: str = "all",
    cursor: int = 0,
    limit: int = 100,
) -> Dict[str, Any]:
    # [2026-09-22] 열거형 파라미터 검증 — 종전에는 미지 시장값이 WHERE 절에
    # 그대로 들어가 **조용히 0건**을 냈다. 0건은 "해당 데이터가 없다" 라는
    # 유효한 답과 구분되지 않아 호출자가 오타를 영영 모른다.
    target_market, _err = normalize_choice('market', 'target_market', target_market)
    if _err:
        return _err

    limit = max(1, min(int(limit), _MAX_PAGE))
    try:
        cursor = max(0, int(cursor))
    except (TypeError, ValueError):
        return mcp_error(MCPErrorCode.MISSING_REQUIRED_FIELD, "cursor must be an integer id")

    try:
        conn = _open_predictions_ro()
        try:
            where: List[str] = ["id > ?"]
            params: List[Any] = [cursor]
            if target_market:
                where.append("target_market = ?")
                # [2026-10-07] 테이블 값은 *_market — 정본값 그대로 넣으면 조용한 0건 (trust_layer._macro_target 참조)
                from oneqaz_trading_mcp.tools.trust_layer import _macro_target
                params.append(_macro_target(target_market))
            if source_category:
                where.append("source_category = ?")
                params.append(source_category)
            if day:
                where.append("substr(created_at, 1, 10) = ?")
                params.append(day[:10])
            if status == "resolved":
                where.append("outcome IS NOT NULL")
            elif status == "open":
                where.append("outcome IS NULL")

            rows = conn.execute(
                "SELECT id, source_category, source_regime_change, target_market, "
                "       predicted_regime_shift, lag_hours, confidence, created_at, "
                "       resolved_at, outcome, actual_regime_shift, "
                # [2026-10-07] additive — 섀도우(강등 셀) 표식·판정 버전. 해시 recipe v1 범위 밖
                "       lifecycle_state_at_creation, judge_version "
                "FROM macro_regime_predictions "
                f"WHERE {' AND '.join(where)} ORDER BY id LIMIT ?",
                (*params, limit + 1),
            ).fetchall()
        finally:
            conn.close()

        has_more = len(rows) > limit
        page = [dict(r) for r in rows[:limit]]
        next_cursor = page[-1]["id"] if (page and has_more) else None

        return {
            "predictions": page,
            "count": len(page),
            "next_cursor": next_cursor,
            "has_more": has_more,
            "filters": {
                "target_market": target_market,
                "source_category": source_category,
                "day": day,
                "status": status,
            },
            "meta": {
                "source_table": "market_global.macro_regime_predictions",
                "append_only_contract": (
                    "Rows are INSERTed at prediction time; resolution fills resolved_at/outcome/"
                    "actual_regime_shift once and is never legitimately edited afterwards. "
                    "Verify independently: recompute the daily hash from these raw rows and "
                    "compare with get_ledger_integrity (see its recipe field)."
                ),
                "interpretation": (
                    "Full lifecycle of every macro regime prediction — created_at is the "
                    "on-record timestamp, outcome is the post-hoc verdict. This is the raw "
                    "evidence behind get_prediction_accuracy's aggregate cells."
                ),
            },
        }
    except Exception as exc:
        logger.exception("get_resolved_predictions failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


def _ai_summary_resolved(data: Dict[str, Any]) -> str:
    n = data.get("count", 0)
    more = " (more pages available)" if data.get("has_more") else ""
    return f"{n} prediction lifecycle rows served with on-record timestamps{more}; verify against get_ledger_integrity hashes."


def _user_summary_resolved(data: Dict[str, Any]) -> str:
    return f"예측 원장 원시 기록 {data.get('count', 0)}건 (생성→검증 전체 생애)을 반환했습니다."


def _na_resolved(data: Dict[str, Any]) -> list:
    actions = [{
        "intent": "verify ledger immutability",
        "tool": "get_ledger_integrity",
        "args": {"days": 30},
        "rationale": "Recompute daily hashes from these raw rows and compare with the published chain.",
        "priority": "high",
    }]
    if data.get("has_more") and data.get("next_cursor"):
        actions.append({
            "intent": "fetch next page",
            "tool": "get_resolved_predictions",
            "args": {"cursor": data["next_cursor"], "limit": 200},
            "rationale": "Cursor pagination — continue the bulk read.",
            "priority": "normal",
        })
    return actions


# ---------------------------------------------------------------------------
# get_ledger_integrity
# ---------------------------------------------------------------------------

def _get_ledger_integrity(days: int = 30) -> Dict[str, Any]:
    days = max(1, min(int(days), 400))
    try:
        from oneqaz_trading_mcp.ledger_integrity import get_chain
        return get_chain(days)
    except Exception as exc:
        logger.exception("get_ledger_integrity failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Chain query failed: {exc}")


def _ai_summary_ledger(data: Dict[str, Any]) -> str:
    return (
        f"Daily SHA-256 hash chain over the prediction ledger: {data.get('chain_length', 0)} days "
        f"({data.get('first_day')}..{data.get('last_day')}). Archive a chain_hash now to detect any future tampering."
    )


def _user_summary_ledger(data: Dict[str, Any]) -> str:
    return f"예측 원장 무결성 해시 체인 {data.get('chain_length', 0)}일치를 반환했습니다 (사후 수정 감지용)."


def _na_ledger(data: Dict[str, Any]) -> list:
    return [{
        "intent": "audit raw rows behind a day's hash",
        "tool": "get_resolved_predictions",
        "args": {"day": data.get("last_day") or "", "limit": 200},
        "rationale": "Fetch the raw rows for a day and recompute its hash using the recipe field.",
        "priority": "high",
    }]


# ---------------------------------------------------------------------------
# Register
# ---------------------------------------------------------------------------

def register_prediction_ledger_tools(mcp, cache):
    """예측 원장 검증 도구 2종 등록."""

    @mcp.tool(title='Resolved Predictions', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_resolved_predictions(
        target_market: Annotated[Optional[str], Field(
            description='filter e.g. "coin_market" / "kr_market" / "us_market". Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = None,
        source_category: Annotated[Optional[str], Field(
            description='filter e.g. "vix", "bonds", "commodities"'
        )] = None,
        day: Annotated[Optional[str], Field(
            description='filter by created day "YYYY-MM-DD" (UTC, string prefix of created_at)'
        )] = None,
        status: Annotated[str, Field(
            description='"all" | "resolved" | "open"'
        )] = 'all',
        cursor: Annotated[int, Field(
            description='last id from previous page (0 = start)'
        )] = 0,
        limit: Annotated[int, Field(
            description='page size (max 500)'
        )] = 100,
    ) -> ResolvedPredictionsEnvelope:
        """
        Purpose: Raw, row-level prediction ledger — every macro regime prediction's full
            lifecycle (created_at -> resolved_at -> outcome). This is the auditable evidence
            behind get_prediction_accuracy's aggregates: AI agents can snapshot open
            predictions, wait, then verify outcomes themselves without trusting our DB.
        Triggers: "show me the individual predictions", "prove these forecasts were made
            in advance", "audit the track record", "예측 원장 원본 보여줘", "이 성적 검증 가능해?".
        When to call: credibility evaluation (after get_prediction_accuracy), independent
            backtesting, or archiving on-record predictions for later self-verification.
        Prerequisites: none. Pairs with get_ledger_integrity for tamper-evidence.
        Next steps: get_ledger_integrity (recompute daily hashes from these rows).
        Caveats: cursor pagination (id-ordered) — follow next_cursor for bulk reads.
            Paper-research forecasts, not investment advice.
        Output: full_data { predictions[] {id, source_category, source_regime_change,
            target_market, predicted_regime_shift, lag_hours, confidence, created_at,
            resolved_at, outcome, actual_regime_shift}, count, next_cursor, has_more, meta }.

        """
        result = await asyncio.to_thread(
            _get_resolved_predictions, target_market, source_category, day, status, cursor, limit
        )
        return wrap_tool_response(
            result, "resolved_predictions",
            _ai_summary_resolved, _user_summary_resolved,
            next_actions_fn=_na_resolved,
        )

    @mcp.tool(title='Prediction Ledger Integrity', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_ledger_integrity(
        days: Annotated[int, Field(
            description='how many most-recent chain entries to return (max 400)'
        )] = 30,
    ) -> LedgerIntegrityEnvelope:
        """
        Purpose: Tamper-evidence for the prediction ledger — a daily SHA-256 hash chain
            over all created/resolved prediction rows, with the exact canonical recipe
            published so any third party can recompute and verify. Archive a chain_hash
            today; if history is ever silently edited, recomputation will not match.
        Triggers: "how do I know these predictions weren't backfilled?", "is the track
            record tamper-proof?", "예측 조작 안 했다는 증거 있어?", "verify ledger integrity".
        When to call: FIRST STEP of any serious credibility audit, and periodically to
            re-anchor (each entry commits to all prior history via prev_chain_hash).
        Prerequisites: none. Raw rows for recomputation: get_resolved_predictions.
        Next steps: get_resolved_predictions (fetch a day's raw rows, recompute its hash).
        Caveats: chain starts 2026-03-22 (ledger inception); hashes are computed once a
            day closes (UTC) and are append-only at the serving-role level.
        Output: full_data { recipe_version, recipe, chain_length, first_day, last_day,
            entries[] {day, created_count, resolved_count, created_hash, resolved_hash,
            prev_chain_hash, chain_hash, computed_at}, verification_hint }.

        """
        result = await asyncio.to_thread(_get_ledger_integrity, days)
        return wrap_tool_response(
            result, "ledger_integrity",
            _ai_summary_ledger, _user_summary_ledger,
            next_actions_fn=_na_ledger,
        )

    logger.info("  [OK] Prediction Ledger Tools registered (2 tools)")
