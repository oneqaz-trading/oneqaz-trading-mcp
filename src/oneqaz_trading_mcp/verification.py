# -*- coding: utf-8 -*-
"""
Verification Status (검증 상태 표식)
===================================
외부 노출 응답에 "이 지표가 방법론 검증을 통과했는가" 를 명시하는 단일 창구.

배경 (2026-09-16 PM 관측 / Ticket A):
    `get_news_leading_indicator_performance` 가 "뉴스보다 평균 732분 먼저 감지" 를
    free tier 에 노출하고 있었으나, anticipated / surprise 두 그룹의
    anticipation_ratio 차이가 거의 없어(0.85~0.92 vs 0.80~0.83) 측정 자체가
    lookback window 안에서 아무 시점이나 '선행' 으로 잡아내는 구조일 가능성이 있다.
    placebo 검증(Ticket B) 전까지 성과 주장으로 노출하지 않는다.

정책:
    - 상태 enum 은 UNVERIFIED / VERIFIED / INVALIDATED 3종.
    - 상태 전환은 **PM 승인 사안**이다. 코드가 자동으로 승격하지 않는다.
    - raw 수치 비노출: MCP 소비자는 LLM 이므로 summary 를 중립화해도 raw 필드가
      남아 있으면 호출한 AI 가 스스로 "평균 12시간 선행" 을 서술한다.
      DB 원본은 건드리지 않고 **응답에서만** 제거한다.

관련 문서:
    - ai_brain/07_mcp_llm_api/mcp_trust_layer_design.md
    - exposure_inventory.md (Ticket A 산출물)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Sequence

logger = logging.getLogger("MarketMCP")

# ── 상태 enum ───────────────────────────────────────────────────────────────
VERIFICATION_UNVERIFIED = "UNVERIFIED"
VERIFICATION_VERIFIED = "VERIFIED"
VERIFICATION_INVALIDATED = "INVALIDATED"

VERIFICATION_STATUSES: tuple[str, ...] = (
    VERIFICATION_UNVERIFIED,
    VERIFICATION_VERIFIED,
    VERIFICATION_INVALIDATED,
)

# ── 뉴스 선행 지표 계열 ─────────────────────────────────────────────────────
NEWS_LEAD_VERIFICATION_NOTE = (
    "Methodology under audit. Not evidence of predictive or anticipatory capability."
)

NEWS_LEAD_USER_NOTE_KO = (
    "이 지표는 계산 방법 검증 중(UNVERIFIED)이며, 뉴스 선행 능력의 근거로 사용할 수 없습니다."
)

# A-2.3 이 지정한 비노출 대상 raw 필드 (event_leading_scores 집계 행).
NEWS_LEAD_METRIC_FIELDS: tuple[str, ...] = (
    "leading_score",
    "avg_lead_time_minutes",
    "avg_anticipation_ratio",
    "accuracy_pct",
)

# 파생 시그널 resource(market://derived/event-leading)는 같은 집계 행에
# avg_precursor_score 를 함께 싣는다. 동일 성격이라 같이 가린다.
NEWS_LEAD_DERIVED_METRIC_FIELDS: tuple[str, ...] = NEWS_LEAD_METRIC_FIELDS + (
    "avg_precursor_score",
)

# news_causality_analysis 집계(get_news_causality_breakdown)의 선행 주장 성분.
NEWS_CAUSALITY_METRIC_FIELDS: tuple[str, ...] = (
    "avg_lead_time_minutes",
    "avg_anticipation_ratio",
)


def mark_unverified(
    response: Dict[str, Any],
    note: str = NEWS_LEAD_VERIFICATION_NOTE,
) -> Dict[str, Any]:
    """응답 최상위에 verification_status / verification_note 를 박는다.

    envelope(wrap_tool_response) 이후에 호출해서 full_data 안이 아니라
    최상위에 오도록 한다 — 첫 줄만 읽는 AI 가 놓치지 않게.

    에러 응답에도 동일하게 박는다 (상태 없음이 '검증됨' 으로 오독되지 않도록).
    """
    if not isinstance(response, dict):
        return response
    response["verification_status"] = VERIFICATION_UNVERIFIED
    response["verification_note"] = note
    return response


def strip_metric_fields(
    rows: Iterable[Any],
    fields: Sequence[str] = NEWS_LEAD_METRIC_FIELDS,
) -> List[Any]:
    """집계 행 리스트에서 미검증 수치 필드를 제거한 새 리스트를 반환.

    DB 원본은 수정하지 않는다. 호출자가 만든 dict 사본에서만 키를 지운다.
    dict 가 아닌 원소는 그대로 통과시킨다.
    """
    cleaned: List[Any] = []
    for row in rows or []:
        if not isinstance(row, dict):
            cleaned.append(row)
            continue
        cleaned.append({k: v for k, v in row.items() if k not in fields})
    return cleaned


def strip_metric_fields_in_mapping(
    mapping: Dict[str, Any],
    fields: Sequence[str] = NEWS_CAUSALITY_METRIC_FIELDS,
) -> Dict[str, Any]:
    """{key: {metric: value}} 형태 집계(breakdown/type_distribution)에서 필드 제거."""
    if not isinstance(mapping, dict):
        return mapping
    return {
        key: ({k: v for k, v in val.items() if k not in fields} if isinstance(val, dict) else val)
        for key, val in mapping.items()
    }


# ── 회귀 방지용 금지 표현 (harness probe 가 참조) ───────────────────────────
# public 응답 본문(요약/설명/next_actions/followup)에 이 표현이 다시 나타나면
# 선행 성과 주장이 재노출된 것이다.
FORBIDDEN_PUBLIC_PHRASES: tuple[str, ...] = (
    "anticipatory",
    "before news",
    "BEFORE news",
    "detect events before",
    "systematic discrimination",
    "statistical significance",
    "먼저 가격 움직임",
    "먼저 감지",
    "뉴스보다",
    "선행 감지",
    "분 선행",
)
