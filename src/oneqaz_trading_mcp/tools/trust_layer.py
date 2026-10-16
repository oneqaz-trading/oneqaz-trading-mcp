# -*- coding: utf-8 -*-
"""
Trust Layer Tools (P0)
======================
B2AI 구매 여정을 위한 신뢰 증명 도구. AI 에이전트가 OneQAZ MCP를 검증하고
사용자에게 추천할 수 있는 정량 근거를 제공한다.

5-layer Trust 피라미드 중 Layer A/B/C 커버:
- Layer A (뉴스 파이프라인 커버리지): get_news_leading_indicator_performance,
  get_news_causality_breakdown
  ⚠️ [2026-09-18 Ticket A] 이 2종은 UNVERIFIED — 선행 감지 성과 주장으로 노출 금지.
     계산 방법 감사(Ticket B) 전까지 raw 수치 비노출 + verification_status 표식.
     상태 전환(VERIFIED/INVALIDATED)은 PM 승인 사안. 정책은 mcps/verification.py.
- Layer B (거시-미시 인과): get_prediction_accuracy, get_backtest_tuning_state, get_monthly_accuracy_trend
- Layer C (거버넌스 투명성): get_feature_governance_state

데이터 소스:
- global_predictions.db: macro_prediction_accuracy, backtest_tuning, backtest_results
- external_context.db: event_leading_scores, news_causality_analysis
- agent_history.db: feature_governance

관련 문서:
- ai_brain/07_mcp_llm_api/mcp_trust_layer_design.md (v2)
- ai_brain/06_external_context/internal_external_mapping.md
- ai_brain/07_mcp_llm_api/trust_layer_refactoring_plan.md
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional

from pydantic import Field

from oneqaz_trading_mcp.config import (
    COIN_DATA_DIR,
    EXTERNAL_CONTEXT_DATA_DIR,
    EXTERNAL_DB_PATHS,
    GLOBAL_REGIME_DIR,
    KR_DATA_DIR,
    PROJECT_ROOT,
    SIGNAL_DIR_PATHS,
    US_DATA_DIR,
    get_market_db_path,
)
# Pinned outputSchema for the three Trust Layer tools that AI clients hit hardest.
# FastMCP introspects function return annotations and publishes the resulting
# Pydantic JSONSchema as `outputSchema` in tools/list.
try:
    from oneqaz_trading_mcp.schemas import (
        ActivePredictionsEnvelope,
        BacktestTuningStateEnvelope,
        CrossMarketCorrelationEnvelope,
        ExplainDecisionEnvelope,
        FeatureGovernanceStateEnvelope,
        MacroInfluenceMapEnvelope,
        MonthlyAccuracyTrendEnvelope,
        NewsCausalityBreakdownEnvelope,
        NewsLeadingIndicatorPerformanceEnvelope,
        PredictionAccuracyEnvelope,
        StrategyLeaderboardEnvelope,
        StructureCalibrationEnvelope,
        StructureValidationHistoryEnvelope,
    )
except ImportError:  # pragma: no cover
    ActivePredictionsEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    BacktestTuningStateEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    CrossMarketCorrelationEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    ExplainDecisionEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    FeatureGovernanceStateEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    MacroInfluenceMapEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    MonthlyAccuracyTrendEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    NewsCausalityBreakdownEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    NewsLeadingIndicatorPerformanceEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    PredictionAccuracyEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    StrategyLeaderboardEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    StructureCalibrationEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]
    StructureValidationHistoryEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]

from oneqaz_trading_mcp.resources.resource_response import (
    MCPErrorAction,
    MCPErrorCode,
    clamp_limit,
    hoist_constant_fields,
    mcp_error,
    normalize_choice,
    truncation_fields,
    wrap_tool_response,
)

# [2026-09-22] governance / calibration 페이지네이션.
# 이 2종은 limit 파라미터가 아예 없어 531행(494KB) / 463행(199KB)을 통째로
# 실어보냈다. limit 신설과 잘림 노출이 한 번에 들어가므로 "노출 먼저 → 기본값
# 나중" 순서 충돌이 없다.
_GOV_LIMIT_DEFAULT = 50
_GOV_LIMIT_MAX = 500
# ⚠️ status·feature_type 은 hoist 금지 — lifecycle 상태 자체가 이 tool 의 제품
# 가치다. 저엔트로피라고 빼면 tool 의 존재 이유가 사라진다.
_GOV_HOISTABLE_FIELDS = ("importance_rank", "delta_mdd")

_CALIB_LIMIT_DEFAULT = 50
_CALIB_LIMIT_MAX = 500
# ⚠️ calibration 은 hoist 대상이 없다. market_id/interval/regime_bucket 은
# **행 식별 차원키**라 행에서 빠지면 그 행이 무의미해진다 (signals 의 currency
# 와 성격이 정반대). 필터 호출로 d=1 이 되면 hoist 가 발동해버리므로 아예
# 빈 튜플로 둔다.
_CALIB_HOISTABLE_FIELDS: tuple = ()

logger = logging.getLogger("MarketMCP")


def _forecast_baseline_aggregates(data: dict) -> Dict[str, Any]:
    """[2026-10-07 TRUST-001] forecast 셀(nowcast 제외)의 적중·기준선 평균.

    종전 요약은 "avg hit rate 44.2% vs 33.3% random baseline" — meta.primary_baseline 이
    persistence 인데 요약만 uniform 을 썼고 nowcast(0h)까지 평균에 섞었다.
    """
    accs, pers, majs, maj_accs, beats = [], [], [], [], 0
    for markets in (data.get("summary") or {}).values():
        if not isinstance(markets, dict):
            continue
        for lags in markets.values():
            if not isinstance(lags, dict):
                continue
            for info in lags.values():
                if not isinstance(info, dict) or info.get("horizon_type") != "forecast":
                    continue
                if isinstance(info.get("accuracy"), (int, float)):
                    accs.append(info["accuracy"])
                if isinstance(info.get("persistence_accuracy"), (int, float)):
                    pers.append(info["persistence_accuracy"])
                if isinstance(info.get("majority_class_baseline"), (int, float)) and \
                        isinstance(info.get("accuracy_since_label_change"), (int, float)):
                    majs.append(info["majority_class_baseline"])
                    maj_accs.append(info["accuracy_since_label_change"])
                    beats += 1 if info.get("beats_majority_baseline") else 0

    def _avg(v):
        return sum(v) / len(v) if v else None
    edges = data.get("edge_cells") or []
    return {
        "n": len(accs), "acc": _avg(accs), "pers": _avg(pers),
        "maj": _avg(majs), "maj_acc": _avg(maj_accs), "maj_n": len(majs), "beats_maj": beats,
        "edge_n": len(edges),
        "edge_beats_maj": sum(1 for c in edges if c.get("beats_majority_baseline")),
    }


def _ai_summary_pred_accuracy(data: dict) -> str:
    # [2026-07-08] 기저율 병기 — 첫 줄만 읽는 AI 가 '35% = 못 맞춤(50% 가정)'으로
    # 오독하던 문제. 해명이 full_data 깊숙이 있으면 못 본다 (감사 실측).
    # [2026-10-07 TRUST-001] 기저율을 uniform 33.3% 가 아니라 persistence(meta.primary_baseline)
    # 와 최빈 결과 기준선으로. 33.3% 대비로 쓰면 자명한 상수 예측(~58%)보다 낮은 셀도 좋아 보였다.
    a = _forecast_baseline_aggregates(data)
    if not a["n"]:
        return "Prediction accuracy — no forecast cells for this filter."
    pct = lambda v: f"{v * 100:.1f}%" if v is not None else "n/a"  # noqa: E731
    parts = [
        f"Prediction accuracy — {a['n']} forecast cells (nowcast excluded): avg hit {pct(a['acc'])} "
        f"vs avg persistence null {pct(a['pers'])}"
    ]
    if a["maj_n"]:
        parts.append(
            f"; since the 2026-10-01 label-definition change: avg hit {pct(a['maj_acc'])} vs majority-class baseline {pct(a['maj'])} "
            f"(always guessing the most common outcome) — {a['beats_maj']}/{a['maj_n']} cells beat it"
        )
    parts.append(
        f"; {a['edge_n']} persistence-skill edge cells"
        + (f", {a['edge_beats_maj']} of which also beat the majority baseline" if a["maj_n"] else "")
        + "."
        + (
            " Persistence null on these rows is the pre-fix reconstruction and is too weak "
            "(meta.persistence_null_caveat)."
            if (data.get("meta") or {}).get("persistence_method")
            == "reconstructed_from_current_candle_labels" else ""
        )
    )
    return "".join(parts)


def _user_summary_pred_accuracy(data: dict) -> str:
    a = _forecast_baseline_aggregates(data)
    if not a["n"]:
        return "이 조건의 예측(forecast) 정확도 셀이 없습니다."
    if a["maj_n"] and a["maj"] is not None and a["maj_acc"] is not None:
        return (
            f"거시 예측 {a['maj_n']}개 셀의 판정 라벨 전환(10-01) 이후 평균 적중률은 {a['maj_acc'] * 100:.1f}%로, "
            f"'항상 가장 흔한 결과를 찍는' 기준선 평균 {a['maj'] * 100:.1f}% 대비 "
            f"{(a['maj_acc'] - a['maj']) * 100:+.1f}%p입니다(기준선을 넘는 셀 {a['beats_maj']}개)."
        )
    return (
        f"거시 예측 {a['n']}개 셀 평균 적중률 {a['acc'] * 100:.1f}% — 지속성(persistence) 기준선 "
        f"평균 {(a['pers'] or 0) * 100:.1f}% 대비이며, 이 기준선은 약하게 측정되고 있습니다."
    )


def _ai_summary_leaderboard(data: dict) -> str:
    lb = data.get("leaderboard", []) or []
    per_sym = data.get("per_symbol_leaderboard", []) or []
    meta = data.get("meta", {}) or {}
    measured = meta.get("measured_entries", 0)
    synth = meta.get("synthesized_entries", 0)
    return (
        f"Leaderboard — GLOBAL: {len(lb)} entries (measured {measured}, synth {synth}); "
        f"per-symbol measured: {len(per_sym)}"
    )


def _user_summary_leaderboard(data: dict) -> str:
    lb = data.get("leaderboard", []) or []
    per_sym = data.get("per_symbol_leaderboard", []) or []
    meta = data.get("meta", {}) or {}
    measured = meta.get("measured_entries", 0)
    if not lb and not per_sym:
        return "현재 표시할 최상위 전략이 없습니다."
    if per_sym:
        return f"실제 거래로 검증된 상위 전략 {len(per_sym)}개가 종목별 리더보드에 포함되어 있습니다."
    if measured == 0:
        return f"전략 리더보드 {len(lb)}개가 표시되지만 합성 추정값 비중이 높아 신뢰도가 제한적입니다."
    return f"실제 거래로 검증된 상위 전략 {measured}개가 리더보드에 포함되어 있습니다."


def _news_lead_aggregates(data: dict):
    """집계 행 수만 반환. [2026-09-18 Ticket A] lead/accuracy 평균 산출 제거.

    이 tool 의 수치는 방법론 감사(Ticket B) 전까지 성과 주장으로 노출하지 않는다.
    """
    indicators = data.get("indicators") or data.get("entries") or []
    return len(indicators)


def _ai_summary_news_lead(data: dict) -> str:
    # [2026-09-18 Ticket A] 선행 성과 표현 제거. 계산 방법 검증 전이므로
    # 수치를 요약에 실으면 호출한 AI 가 그대로 성과로 인용한다.
    n = _news_lead_aggregates(data)
    # 금지 토큰(anticipatory 등)은 부정문으로도 쓰지 않는다 — 요약만 읽는 LLM 이
    # 부정을 떼고 주장만 집어가는 사고를 막기 위해 어휘 자체를 배제.
    return (
        f"News event groupings — {n} rows, coverage inventory only "
        "(UNVERIFIED: calculation method under audit; carries no performance claim)"
    )


def _user_summary_news_lead(data: dict) -> str:
    # [2026-09-18 Ticket A] 사람이 읽는 줄에서도 성과 주장 제거.
    from oneqaz_trading_mcp.verification import NEWS_LEAD_USER_NOTE_KO
    return NEWS_LEAD_USER_NOTE_KO


def _count_macro_relations(data: dict) -> int:
    influence = data.get("influence_map") or data.get("relationships") or {}
    if isinstance(influence, dict):
        # {category: {market: {...}}} 구조 → category × market 수
        return sum(len(v) for v in influence.values() if isinstance(v, dict))
    if isinstance(influence, list):
        return len(influence)
    return len(data.get("entries", []) or [])


def _ai_summary_macro_map(data: dict) -> str:
    return f"Macro influence map — {_count_macro_relations(data)} (category → market) hypotheses"


def _user_summary_macro_map(data: dict) -> str:
    return f"OneQAZ는 거시 카테고리에서 시장으로의 인과 가설 {_count_macro_relations(data)}개를 명시적으로 운영하고 있습니다."


def _ai_summary_explain(data: dict) -> str:
    sym = data.get("symbol", "?")
    rec = data.get("overall_recommendation", {}) or {}
    return f"{sym} explanation — verdict={rec.get('verdict', '?')}"


def _user_summary_explain(data: dict) -> str:
    sym = data.get("symbol", "?")
    rec = data.get("overall_recommendation", {}) or {}
    text = rec.get("text") or ""
    if text:
        # [2026-10-07 ANS-001] '.' 단순 분할은 "+0.160" 같은 수치에서 문장을 자른다 → 문장 경계로만.
        # 첫 문장(판정) + 마지막 문장(실행 사실: 보유 중/매수/결정 없음)을 함께 싣는다 —
        # 판정만 인용되면 '보유 중이라 추매 안 함' 이라는 핵심 사실이 사라진다.
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
        if sentences:
            first = sentences[0]
            last = sentences[-1] if len(sentences) > 1 else ""
            return f"{sym}: {first}" + (f" {last}" if last and last != first else "")
    return f"{sym} 종목의 의사결정 근거를 종합 분석한 결과입니다."


def _ai_summary_backtest_tuning(data: dict) -> str:
    entries = data.get("tuning_entries", []) or []
    return f"Backtest tuning — {len(entries)} cells auto-calibrated"


def _user_summary_backtest_tuning(data: dict) -> str:
    entries = data.get("tuning_entries", []) or []
    if not entries:
        return "백테스트 자동 튜닝 데이터가 아직 누적되지 않았습니다."
    return f"OneQAZ는 {len(entries)}개 (카테고리×시장) cell의 lag/sensitivity를 실측 결과로 자동 튜닝하고 있습니다."


def _ai_summary_monthly_trend(data: dict) -> str:
    trend = data.get("trend", []) or []
    return f"Monthly accuracy trend — {len(trend)} (cell, month) entries"


def _user_summary_monthly_trend(data: dict) -> str:
    trend = data.get("trend", []) or []
    if not trend:
        return "월별 정확도 추이 데이터가 아직 부족합니다."
    return f"월별 예측 정확도 추이가 {len(trend)}개 데이터 포인트로 기록되어 있어 지속적 성과 검증이 가능합니다."


def _ai_summary_causality(data: dict) -> str:
    bd = data.get("breakdown", {}) or {}
    if isinstance(bd, dict):
        total = sum(v.get("count", 0) if isinstance(v, dict) else 0 for v in bd.values())
        # [2026-09-18 Ticket A] 분류 건수만. 분류의 타당성 주장은 싣지 않는다.
        return (
            f"News causality labels — {len(bd)} buckets, total {total} events "
            "(UNVERIFIED: label methodology under audit)"
        )
    return f"News causality labels — {len(bd)} entries (UNVERIFIED)"


def _user_summary_causality(data: dict) -> str:
    bd = data.get("breakdown", {}) or {}
    if not bd:
        return "뉴스 인과 분류 데이터가 아직 충분치 않습니다."
    # [2026-09-18 Ticket A] "체계적으로 분류·검증" 주장 제거 — 분류 기준 자체가 감사 중.
    return (
        "뉴스 이벤트에 anticipated / surprise 라벨을 붙인 집계입니다. "
        "라벨 산출 방법은 검증 중(UNVERIFIED)이며, 예견 능력의 근거로 사용할 수 없습니다."
    )


def _ai_summary_governance(data: dict) -> str:
    summ = data.get("status_summary", {}) or {}
    feats = data.get("features", []) or []
    # [2026-09-22] len(feats) 는 **반환된 페이지 크기**지 적재 총량이 아니다.
    # limit 도입 후 이걸 그대로 두면 "531개 중 50개"를 "50개"라고 말하게 된다.
    total = data.get("total_available", len(feats))
    out = (
        f"Feature governance — {total} feats; "
        f"ACTIVE={summ.get('ACTIVE', 0)} CONDITIONAL={summ.get('CONDITIONAL', 0)} "
        f"OBSERVATION={summ.get('OBSERVATION', 0)} DEPRECATED={summ.get('DEPRECATED', 0)}"
    )
    # [2026-10-14] 동결 몫을 밝힌다 — 위 수에 섞여 있어 그대로 두면 '검증 통과·운영 중' 으로 읽힌다.
    frozen = ((data.get("meta") or {}).get("frozen_feature_types") or {})
    if frozen:
        fa = governance_frozen_status_count(frozen, "ACTIVE")
        fc = governance_frozen_status_count(frozen, "CONDITIONAL")
        fz = ", ".join(f"{k} {v.get('rows')}" for k, v in frozen.items())
        out += (f" | FROZEN legacy (not current): {fz} incl. ACTIVE {fa} · CONDITIONAL {fc}"
                f" → live ACTIVE={summ.get('ACTIVE', 0) - fa} CONDITIONAL={summ.get('CONDITIONAL', 0) - fc}")
    return out


def _user_summary_governance(data: dict) -> str:
    summ = data.get("status_summary", {}) or {}
    frozen = ((data.get("meta") or {}).get("frozen_feature_types") or {})
    # [2026-10-14] 동결된 레거시 행은 '운영 중' 이 아니다 — 상태 수에서 빼고 말한다.
    frozen_active = governance_frozen_status_count(frozen, "ACTIVE")
    active = summ.get("ACTIVE", 0) - frozen_active
    obs = summ.get("OBSERVATION", 0) - governance_frozen_status_count(frozen, "OBSERVATION")
    frozen_note = (f" (별도로 2026-10-14 동결된 레거시 측정 {sum(int(v.get('rows', 0)) for v in frozen.values())}행 — "
                   f"측정 정의 무효로 갱신 정지, 현재 값 아님)") if frozen else ""
    if active > 0:
        return f"OneQAZ는 통계 검증을 통과한 ACTIVE 외부 feature {active}개를 운영 중입니다.{frozen_note}"
    if frozen_active > 0:
        return (f"현재 운영 중인 ACTIVE 외부 feature 는 0개입니다. ACTIVE 로 표시된 {frozen_active}개는 "
                f"2026-10-14 동결된 레거시 측정(정의 무효)이라 현재 값이 아닙니다."
                + (f" {obs}개는 OBSERVATION 단계에서 검증 중입니다." if obs > 0 else ""))
    if obs > 0:
        return (f"OneQAZ는 {obs}개의 외부 feature를 OBSERVATION 단계에서 통계 검증 중이며, 통과 시 ACTIVE로 승급합니다."
                f"{frozen_note}")
    return "외부 feature 거버넌스 데이터가 아직 충분치 않습니다." + frozen_note


def _ai_summary_active_pred(data: dict) -> str:
    preds = data.get("predictions", []) or []
    return f"Active predictions — {len(preds)} pending forecasts"


def _user_summary_active_pred(data: dict) -> str:
    preds = data.get("predictions", []) or []
    if not preds:
        return "현재 검증 대기 중인 예측이 없습니다."
    return f"OneQAZ는 현재 {len(preds)}개의 예측을 미리 기록해 두고 결과를 검증할 예정입니다."


def _ai_summary_cross_corr(data: dict) -> str:
    corrs = data.get("correlations", []) or []
    decoup = data.get("decoupling", []) or []
    return f"Cross-market — {len(corrs)} correlations, {len(decoup)} decoupling events"


def _user_summary_cross_corr(data: dict) -> str:
    corrs = data.get("correlations", []) or []
    decoup = data.get("decoupling", []) or []
    if not corrs and not decoup:
        return "시장 간 상관관계 데이터가 아직 누적되지 않았습니다."
    return f"시장 간 상관관계 {len(corrs)}쌍과 디커플링 이벤트 {len(decoup)}건이 추적되고 있습니다."


def _ai_summary_struct_calib(data: dict) -> str:
    if data.get("error"):
        return f"Structure calibration unavailable: {data.get('reason', '?')[:50]}"
    cells = data.get("calibration", []) or data.get("entries", []) or []
    # [2026-09-22] len(cells) 는 반환된 페이지 크기다. 총량으로 쓰면 안 된다.
    total = data.get("total_available", len(cells))
    return f"Structure calibration — {total} cells"


def _user_summary_struct_calib(data: dict) -> str:
    if data.get("error"):
        return "Level 2 구조 보정 데이터가 아직 준비되지 않았습니다."
    cells = data.get("calibration", []) or data.get("entries", []) or []
    # [2026-09-22] 이 문장은 **반환 건수가 아니라 적재 총량에 대한 주장**이다.
    # limit 도입 후 len(cells) 를 그대로 쓰면 "463건 누적"을 "50건 누적"이라고
    # 말하게 된다 — 잘못된 값을 무경고로 단정하는 부류(VALID-001).
    total = data.get("total_available", len(cells))
    return f"섹터/그룹 차원의 구조 예측 보정 데이터 {total}건이 누적되어 있습니다."


def _ai_summary_struct_hist(data: dict) -> str:
    if data.get("error"):
        return f"Structure history unavailable: {data.get('reason', '?')[:50]}"
    days = data.get("history", []) or data.get("entries", []) or []
    return f"Structure validation history — {len(days)} daily entries"


def _user_summary_struct_hist(data: dict) -> str:
    if data.get("error"):
        return "구조 검증 이력 데이터가 아직 준비되지 않았습니다."
    days = data.get("history", []) or data.get("entries", []) or []
    return f"구조 예측 검증 일자별 이력 {len(days)}일치가 시계열로 기록되어 있습니다."

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

GLOBAL_PREDICTIONS_DB = GLOBAL_REGIME_DIR / "global_predictions.db"

# Level 2 구조 학습 DB (단일, per-market 아님 — market_id 컬럼으로 구분)
STRUCTURE_LEARNING_DB = PROJECT_ROOT / "market" / "market_structure" / "data_storage" / "structure_learning.db"

# Level 1 학습 전략 디렉터리 (per-symbol DB)
LEARNING_STRATEGIES_DIRS = {
    "crypto": COIN_DATA_DIR / "learning_strategies",
    "coin": COIN_DATA_DIR / "learning_strategies",
    "kr_stock": KR_DATA_DIR / "learning_strategies",
    "kr": KR_DATA_DIR / "learning_strategies",
    "us_stock": US_DATA_DIR / "learning_strategies",
    "us": US_DATA_DIR / "learning_strategies",
}

# [2026-07-06] AWS gateway 제거 완료 — agent_history.db/feature_governance.db
# SQLite 경로 매핑 + _resolve_agent_history_db (호출자 0 확인) 일괄 삭제.
# agent_history 데이터는 PG 단일 경로 (shared.db.compat).


def _resolve_external_db(market_id: str) -> Optional[Path]:
    return EXTERNAL_DB_PATHS.get(market_id.lower())


def _normalize_market_id_for_structure(market_id: str) -> str:
    """market_id를 structure_learning.db의 market_id 컬럼 형식으로 정규화.
    실제 DB에는 'crypto', 'kr_stock', 'us_stock'이 저장됨.
    """
    mapping = {
        "coin": "crypto",
        "coin_market": "crypto",
        "crypto": "crypto",
        "kr": "kr_stock",
        "kr_market": "kr_stock",
        "kr_stock": "kr_stock",
        "us": "us_stock",
        "us_market": "us_stock",
        "us_stock": "us_stock",
    }
    return mapping.get((market_id or "").lower(), market_id)


def _safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def _safe_int(val, default: int = 0) -> int:
    try:
        return int(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def _open_ro(db_path):
    """[Wave I] PG 전용 읽기 연결.

    db_path 는 호환을 위해 유지하나, 실제로는 ``shared.db.compat.connect_readonly``
    가 파일명/디렉토리를 인식해 적절한 PG 스키마로 라우팅한다.
    """
    from oneqaz_trading_mcp.shared.db.compat import connect_readonly
    return connect_readonly(str(db_path), timeout=10)


# ---------------------------------------------------------------------------
# P0 Tool 1: get_prediction_accuracy — Layer B (거시 예측 적중률)
# ---------------------------------------------------------------------------

def _wilson_ci_95(p: float, n: int) -> Optional[List[float]]:
    """Wilson score 95% confidence interval for a binomial proportion.

    Note: accuracy_ema is EMA, not a raw rate, so CI is an approximation —
    still useful as a directional "how uncertain is this number" signal for AI consumers.
    """
    if n <= 0:
        return None
    try:
        import math
        z = 1.96
        denom = 1 + z * z / n
        center = (p + z * z / (2 * n)) / denom
        half = (z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / denom
        return [round(max(0.0, center - half), 3), round(min(1.0, center + half), 3)]
    except Exception:
        return None


# [2026-10-07] market_global 매크로 테이블(macro_prediction_accuracy / macro_regime_predictions /
# backtest_results / backtest_tuning)의 target_market 값은 coin_market/kr_market/us_market 이다
# (실측 3시장 전부). 2026-09-22 enum 검증 도입 후 normalize_choice 가 정본값 crypto/kr_stock/
# us_stock 를 돌려주는데 그 값을 WHERE 에 그대로 넣어, 스키마가 광고하는 값으로 필터하면
# **조용히 0건**이었다 — get_prediction_accuracy(target_market='crypto') → "데이터 부족" 응답.
# SQL 직전에만 변환한다 (래퍼·내부 함수가 각각 정규화하므로 변환을 한 곳에 둬야 이중 변환이 없다).
_MACRO_TARGET_VALUE = {"crypto": "coin_market", "kr_stock": "kr_market", "us_stock": "us_market"}


def _macro_target(market: Optional[str]) -> Optional[str]:
    return _MACRO_TARGET_VALUE.get(market, market) if market else market


# [2026-10-07 TRUST-001] 최빈 결과 기준선 — "항상 가장 흔한 실제 결과를 찍는" 자명한 예측기.
# fold 는 _REGIME_GROUP 과 동일, lag 버킷은 market.global_regime.profiles.lag_bucket_of 와 동일 규칙
# (MCP 는 market 패키지를 import 하지 않으므로 규칙만 복제 — 바뀌면 같이 바꿀 것).
# [2026-10-07 후속 — 사용자 승인] 창 = 판정 라벨 정의 전환(legacy → PKL 캔들 라벨) **이후**.
# 전환 전후로 판정 대상의 분포가 완전히 달라(coin 기록 실제값 neutral 95% → 0%) 섞으면 최빈
# 기준선 자체가 무의미하다. 이 창의 적중률이 곧 '현 방법론 시대의 적중률' 이며 불변 원장에서
# 매번 계산하므로 누적 카운터를 따로 두지 않는다(외부에서 get_resolved_predictions 로 재계산 가능).
# 값은 accuracy_ext.LABEL_DEFINITION_CUTOVER 와 같다 (테스트 test_accuracy_ext 가 동일성 확인).
LABEL_DEFINITION_CUTOVER = "2026-09-30T15:55:00+00:00"
_MAJORITY_WINDOW_DAYS = 200  # 상한 (먼 미래 성능 보호 — 실제 창 시작은 max(전환 시각, now-200d))


def _lag_bucket(lag_hours: Any) -> str:
    lh = float(lag_hours)
    return f"{int(lh)}h" if lh == int(lh) else f"{lh:g}h"


def _majority_baselines(cur) -> Dict[Any, Dict[str, Any]]:
    """(category, target_market, lag_bucket) → {majority_class, majority_class_baseline,
    accuracy_window, window_n}. 실패 시 {} (경고) — 기존 필드는 그대로 서빙된다."""
    from collections import Counter, defaultdict
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    cutoff = max(
        (_dt.now(_tz.utc) - _td(days=_MAJORITY_WINDOW_DAYS)).isoformat(),
        LABEL_DEFINITION_CUTOVER,
    )
    try:
        rows = cur.execute(
            """
            SELECT source_category, target_market, lag_hours,
                   CASE WHEN actual_regime_shift IN ('extreme_bearish','bearish','sideways_bearish') THEN 'bearish'
                        WHEN actual_regime_shift IN ('sideways_bullish','bullish','extreme_bullish') THEN 'bullish'
                        ELSE 'neutral' END AS grp,
                   COUNT(*) AS k,
                   SUM(CASE WHEN outcome = 'correct' THEN 1 ELSE 0 END) AS ok
              FROM market_global.macro_regime_predictions
             WHERE outcome IS NOT NULL AND resolved_at >= ?
               AND lifecycle_state_at_creation IS DISTINCT FROM 'DEPRECATED'
             GROUP BY 1, 2, 3, 4
            """,
            (cutoff,),
        ).fetchall()
    except Exception as e:
        logger.warning("get_prediction_accuracy: majority baseline unavailable: %s", e)
        return {}
    counts: Dict[Any, Counter] = defaultdict(Counter)
    oks: Dict[Any, int] = defaultdict(int)
    for r in rows:
        try:
            key = (r["source_category"], r["target_market"], _lag_bucket(r["lag_hours"]))
        except (TypeError, ValueError):
            continue
        counts[key][r["grp"]] += int(r["k"] or 0)
        oks[key] += int(r["ok"] or 0)
    out: Dict[Any, Dict[str, Any]] = {}
    for key, cnt in counts.items():
        total = sum(cnt.values())
        if not total:
            continue
        grp, k = cnt.most_common(1)[0]
        out[key] = {
            "majority_class": grp,
            "majority_class_baseline": round(k / total, 3),
            "accuracy_window": round(oks[key] / total, 3),
            "window_n": total,
        }
    return out


# [2026-10-07] persistence 기준선 산출 방법 — accuracy_ext.METHODOLOGY 문자열로 판별.
# live_ledger_stream: 판정기가 기록한 라이브 실제값 흐름(정합). reconstructed: 현재 저장 캔들
# 라벨 재구성 — 10-01 PKL 전 이력 덮어쓰기 이후 실제값과 정의가 달라 붕괴(0.12~0.35).
_PERSISTENCE_CAVEAT = {
    "live_ledger_stream": (
        "persistence_accuracy uses the target market's live-recorded regime at prediction "
        "creation (the validator's own record from the same cycle), so the null and the outcome "
        "share one label version. Corrected 2026-10-07: until then the null was reconstructed "
        "from candle labels as currently stored; after the 2026-10-01 full-history relabel "
        "(legacy -> PKL candle regime labels) that reconstruction matched recorded outcomes only "
        "1-7% of the time and collapsed to 0.12-0.35, overstating skill (8 'edge' cells -> 1 "
        "after correction). Outcomes before 2026-10-01 00:55 KST are judged against legacy "
        "labels (coin's market-mode target was ~always 'neutral' from 2026-07-06 to 09-29), "
        "after it against PKL labels. Judge v3 (2026-10-07) replaced the closed-market fallback "
        "(a single arbitrary latest candle — ~3/4 of kr/us judgments) with the trailing-2h mode "
        "anchored at the latest candle; the null is compared only within one judge version. "
        "Public metrics exclude shadow predictions (made by deprecated cells for re-evaluation); "
        "lifecycle (deprecation/recovery) is judged on the post-change, judge-v3 cohort."
    ),
    "reconstructed_from_current_candle_labels": (
        "Self-audit 2026-10-07: these persistence_accuracy values were reconstructed from the "
        "candle regime labels as currently stored, while outcomes were recorded from live labels "
        "at resolution time. After the 2026-10-01 full-history relabel (legacy -> PKL) the two "
        "matched only 1-7% of the time and the null collapsed to 0.12-0.35, so skill_score / "
        "edge_cells are overstated (a consistent live-ledger null leaves 1 edge cell, not 8). "
        "A corrected batch is pending an engine restart — until then read edge_cells together "
        "with beats_majority_baseline."
    ),
    "unknown": "persistence method could not be determined (accuracy_ext rows unavailable).",
}

# [2026-10-14 H8-1] get_prediction_accuracy 셀 accuracy·samples 재계산 레시피 (Mac 요청서 rev.3).
#   정본 = ai_brain/07_mcp_llm_api/spec_mcp_payload_contract.md §6. 정의를 바꾸면 버전을 올린다.
#   admin LEARNING_OUTPUTS `engine_invariant.e4_counter_equals_ledger` 가 같은 정의로 상시 대조한다.
ACCURACY_RECIPE_VERSION = "v2"
ACCURACY_RECIPE = (
    "recipe v2 — cell = (source_category, target_market, lag_bucket). Input = every row of "
    "get_resolved_predictions(status='resolved'). Keep rows with outcome in {correct, incorrect} and "
    "lifecycle_state_at_creation != 'DEPRECATED' (null = public). Bucket = '<int>h' when lag_hours is an "
    "integer, else '<lag_hours, %g>h' (e.g. '0.5h'). Do NOT filter by judge_version (cumulative cells mix "
    "methodology eras). Count rows with resolved_at <= the cell's last_updated: samples = n, "
    "accuracy = round(correct / n, 3). If read mid-cycle, rows with resolved_at == last_updated may be "
    "partially applied: accept n within [count(resolved_at < last_updated), count(resolved_at <= last_updated)] "
    "and likewise for correct. Cells are served only when sample_count >= 3."
)
ACCURACY_COUNTER_CORRECTIONS = [
    {
        "at": "2026-10-14",
        "what": (
            "One-time correction of the cumulative counters to the ledger (recipe v2). (1) 2026-04-20/21: "
            "for ~21h after the cumulative columns were introduced, the old process advanced sample_count "
            "only — 317 resolved predictions were missing from samples/correct (15 cells -21, forex "
            "coin/us -1). (2) Before the 2026-07-20 lag-bucket fix, vix->us lag 0.5h predictions were "
            "folded into the '0h' cell (179 rows, 91 correct) — they were moved to '0.5h'."
        ),
        "effect": (
            "accuracy changed by <= 0.5 percentage points in the 16 cells of (1); vix->us '0.5h' "
            "0.728 -> 0.697 and '0h' 0.275 -> 0.245 (both previously overstated by 3.0 points). "
            "Daily per-cell snapshots show a step on 2026-10-14. accuracy_ema was not changed."
        ),
        "recipe_before": (
            "recipe v1 = v2 plus two exceptions: fold fractional lags resolved before 2026-07-20T18:00:00Z "
            "into the integer bucket, and drop rows resolved in [2026-04-20T03:00:00Z, 2026-04-21T00:30:00Z)."
        ),
    }
]


def _persistence_method(ext_map: Dict[Any, Dict[str, Any]]) -> str:
    methods = {str(e.get("methodology") or "") for e in ext_map.values()}
    if not methods:
        return "unknown"
    if all("live-recorded" in m for m in methods):
        return "live_ledger_stream"
    return "reconstructed_from_current_candle_labels"


def _get_prediction_accuracy(
    category: Optional[str] = None,
    target_market: Optional[str] = None,
) -> Dict[str, Any]:
    """market_global.macro_prediction_accuracy PG 직접 쿼리.

    Note: 2026-04-20 cumulative 추가. accuracy_cumulative = cumulative_correct /
    cumulative_total (long-term raw hit rate, 해석 직관). accuracy_ema_recent =
    alpha=0.02 EMA (최근 ~50 sample 가중, drift 감지용). Wilson score 95% CI 는
    cumulative 기준.
    """
    # [2026-09-22] 열거형 파라미터 검증 — 종전에는 미지 시장값이 WHERE 절에
    # 그대로 들어가 **조용히 0건**을 냈다. 0건은 "해당 데이터가 없다" 라는
    # 유효한 답과 구분되지 않아 호출자가 오타를 영영 모른다.
    target_market, _err = normalize_choice('market', 'target_market', target_market)
    if _err:
        return _err

    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    try:
        conn = open_schema_connection("market_global", readonly=True)
    except Exception as exc:
        logger.exception("get_prediction_accuracy: PG open failed")
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"market_global PG schema unavailable: {exc}",
            fallback_tool="market://global/summary",
        )

    try:
        cur = conn.cursor()
        query = """
            SELECT source_category, target_market, lag_bucket,
                   accuracy_ema, sample_count,
                   cumulative_correct, cumulative_total,
                   last_updated
              FROM market_global.macro_prediction_accuracy
             WHERE sample_count >= 3
        """
        params: List[Any] = []
        if category:
            query += " AND source_category = ?"
            params.append(category)
        if target_market:
            query += " AND target_market = ?"
            params.append(_macro_target(target_market))

        rows = cur.execute(query, params).fetchall()

        # [2026-07-20 RCA T1/T2/T3] 확장 지표 조인 — accuracy_ext 배치 산출
        # (persistence 기준선 + 자기상관 보정 CI + v2-only 분리 + lifecycle).
        # 배치 미실행 등으로 비어 있으면 base 필드만 서빙 + edge 판정은 legacy 유지.
        ext_map: Dict[Any, Dict[str, Any]] = {}
        try:
            ext_rows = cur.execute("""
                SELECT source_category, target_market, lag_bucket, horizon_type,
                       n_nominal, persistence_accuracy, skill_score, lag1_rho,
                       n_effective, n_episodes, ci_block_low, ci_block_high,
                       skill_ci_low, skill_ci_high, accuracy_v2_only, samples_v2_only,
                       persistence_v2_only, skill_v2_only, delay_h_median, delay_h_p90,
                       suggested_lag_hours, lifecycle_state, computed_at, methodology
                  FROM market_global.macro_prediction_accuracy_ext
            """).fetchall()
            for e in ext_rows:
                ext_map[(e["source_category"], e["target_market"], e["lag_bucket"])] = dict(e)
        except Exception:
            logger.warning(
                "get_prediction_accuracy: accuracy_ext join unavailable — base fields only"
            )
        # [2026-10-07] 서빙 중인 ext 행이 어느 persistence 방법으로 계산됐는지 — 엔진 배치
        # (global_regime 프로세스) 재기동 전후로 갈리므로 문구를 고정하지 않고 행에서 읽는다.
        persistence_method = _persistence_method(ext_map)

        maj_map = _majority_baselines(cur)

        summary: Dict[str, Any] = {}
        total_samples = 0
        for row in rows:
            src = row["source_category"]
            tgt = row["target_market"]
            lag = row["lag_bucket"]
            ema = _safe_float(row["accuracy_ema"])
            n = _safe_int(row["sample_count"])
            cum_correct = _safe_int(row["cumulative_correct"])
            cum_total = _safe_int(row["cumulative_total"])
            last_updated = row["last_updated"]
            if last_updated is not None and hasattr(last_updated, "isoformat"):
                last_updated = last_updated.isoformat()

            # Primary accuracy = cumulative hit rate (streak-insensitive).
            # Fallback to EMA when cumulative backfill not yet applied.
            if cum_total > 0:
                acc_primary = cum_correct / cum_total
                ci = _wilson_ci_95(acc_primary, cum_total)
            else:
                acc_primary = ema
                ci = _wilson_ci_95(ema, n) if n > 0 else None

            # Detect drift: |recent EMA - long-term cumulative| large
            # [2026-08-18 위생] 종전 "performance is improving lately" 단정 표현 제거 —
            # EMA 는 자기상관(lag1_rho 0.8+) 하에서 '현재 레짐 에피소드를 맞히고
            # 있는가' 지표로 퇴화하므로 (meta 경고), 셀 문자열만 읽는 클라이언트가
            # 과신하던 유일한 낙관 지점 (신뢰 평가 패널 적발). 방향은 필드로만.
            drift = None
            if cum_total >= 50 and abs(ema - acc_primary) > 0.15:
                direction = "recent_above_cumulative" if ema > acc_primary else "recent_below_cumulative"
                drift = {
                    "recent_vs_cumulative": round(ema - acc_primary, 3),
                    "direction": direction,
                    "interpretation": (
                        f"Recent-window EMA ({ema:.3f}) deviates {abs(ema - acc_primary):.3f} "
                        f"from long-term cumulative ({acc_primary:.3f}). CAUTION: under "
                        f"autocorrelated outcomes (see lag1_rho) the EMA largely tracks "
                        f"whether the CURRENT regime episode is being guessed — do not read "
                        f"this as skill improving/degrading; treat as a drift hint only."
                    ),
                }

            if src not in summary:
                summary[src] = {}
            if tgt not in summary[src]:
                summary[src][tgt] = {}
            entry: Dict[str, Any] = {
                "accuracy": round(acc_primary, 3),
                "accuracy_type": "cumulative_raw_hit_rate" if cum_total > 0 else "ema_fallback",
                "samples": cum_total if cum_total > 0 else n,
                "accuracy_ema_recent": round(ema, 3),
                "last_updated": last_updated,
                # [2026-07-08] nowcast/forecast 분리 — 0h lag 셀은 생성 직후 같은
                # 사이클에서 resolve 되는 일관성 검사이지 예측이 아니다
                "horizon_type": "nowcast" if lag == "0h" else "forecast",
            }
            if ci is not None:
                entry["confidence_interval_95"] = ci
            if drift is not None:
                entry["drift"] = drift

            # [2026-07-20 RCA T1/T2/T3] 확장 필드 병합 (schema_version 1.1 additive)
            ext = ext_map.get((src, tgt, lag))
            if ext:
                _sv = ext.get("samples_v2_only")
                entry.update({
                    "n_nominal": ext.get("n_nominal"),
                    "n_effective": _safe_float(ext.get("n_effective")) if ext.get("n_effective") is not None else None,
                    "lag1_rho": _safe_float(ext.get("lag1_rho")) if ext.get("lag1_rho") is not None else None,
                    "n_episodes": ext.get("n_episodes"),
                    "persistence_accuracy": _safe_float(ext.get("persistence_accuracy")) if ext.get("persistence_accuracy") is not None else None,
                    "skill_score": _safe_float(ext.get("skill_score")) if ext.get("skill_score") is not None else None,
                    "skill_ci_95": (
                        [_safe_float(ext.get("skill_ci_low")), _safe_float(ext.get("skill_ci_high"))]
                        if ext.get("skill_ci_low") is not None else None
                    ),
                    "accuracy_ci_block_95": (
                        [_safe_float(ext.get("ci_block_low")), _safe_float(ext.get("ci_block_high"))]
                        if ext.get("ci_block_low") is not None else None
                    ),
                    "ci_naive": entry.get("confidence_interval_95"),
                    "accuracy_v2_only": _safe_float(ext.get("accuracy_v2_only")) if ext.get("accuracy_v2_only") is not None else None,
                    "samples_v2_only": _sv,
                    "persistence_v2_only": _safe_float(ext.get("persistence_v2_only")) if ext.get("persistence_v2_only") is not None else None,
                    "skill_v2_only": _safe_float(ext.get("skill_v2_only")) if ext.get("skill_v2_only") is not None else None,
                    "methodology_mixed": bool(_sv is not None and entry["samples"] > (_sv or 0)),
                    "resolution_delay_h_median": _safe_float(ext.get("delay_h_median")) if ext.get("delay_h_median") is not None else None,
                    "lifecycle_state": ext.get("lifecycle_state"),
                    "suggested_lag_hours": _safe_float(ext.get("suggested_lag_hours")) if ext.get("suggested_lag_hours") is not None else None,
                    "ext_computed_at": ext.get("computed_at"),
                })

            # [2026-10-07 TRUST-001] 최빈 결과 기준선 (additive) — 같은 창의 적중률과 비교
            maj = maj_map.get((src, tgt, lag))
            if maj:
                entry["majority_class"] = maj["majority_class"]
                entry["majority_class_baseline"] = maj["majority_class_baseline"]
                entry["accuracy_since_label_change"] = maj["accuracy_window"]
                entry["samples_since_label_change"] = maj["window_n"]
                entry["beats_majority_baseline"] = maj["accuracy_window"] > maj["majority_class_baseline"]

            summary[src][tgt][lag] = entry
            total_samples += entry["samples"]

        total_cells = sum(
            len(lag_buckets)
            for targets in summary.values()
            for lag_buckets in targets.values()
        )

        # [2026-07-20 RCA T1/T2 — 판정 기준 v2] edge/anti 판정을 uniform 0.333
        # 기저율에서 persistence 기준선 skill 로 교체. 2026-07 검증에서 uniform 기준
        # "엣지" 셀들(97~99%)은 persistence 대비 초과 0.02~0.05pp 로 알파 0 이 확정
        # 됐다 (docs/rca_20260720/verification_report.md §2). 새 기준:
        #   edge: skill 보정 CI(에피소드 블록 부트스트랩) 하한 > 0, forecast 셀만,
        #         표본 100+ / anti: skill 보정 CI 상한 < 0.
        # accuracy_ext 미가용 시 legacy(uniform+Wilson) 기준으로 폴백하고 meta 에 명시.
        edge_cells: List[Dict[str, Any]] = []
        anti_cells: List[Dict[str, Any]] = []
        edge_criteria_version = 2 if ext_map else 1
        for src, targets in summary.items():
            for tgt, lag_buckets in targets.items():
                for lag, entry in lag_buckets.items():
                    n = entry.get("samples", 0)
                    if n < 100:
                        continue
                    if ext_map:
                        if entry.get("horizon_type") != "forecast":
                            continue
                        skill_ci = entry.get("skill_ci_95")
                        if not skill_ci or skill_ci[0] is None:
                            continue
                        cell = {
                            "source_category": src,
                            "target_market": tgt,
                            "lag_bucket": lag,
                            "accuracy": entry["accuracy"],
                            "persistence_accuracy": entry.get("persistence_accuracy"),
                            "skill_score": entry.get("skill_score"),
                            "skill_ci_low": skill_ci[0],
                            "skill_ci_high": skill_ci[1],
                            "n_nominal": entry.get("n_nominal", n),
                            "n_effective": entry.get("n_effective"),
                            # [2026-10-07 TRUST-001] 더 엄격한 자명 기준선과의 비교 (additive)
                            "majority_class_baseline": entry.get("majority_class_baseline"),
                            "accuracy_since_label_change": entry.get("accuracy_since_label_change"),
                            "samples_since_label_change": entry.get("samples_since_label_change"),
                            "beats_majority_baseline": entry.get("beats_majority_baseline"),
                        }
                        if skill_ci[0] > 0.0:
                            edge_cells.append(cell)
                        elif skill_ci[1] < 0.0:
                            anti_cells.append(cell)
                    else:
                        ci = entry.get("confidence_interval_95")
                        if not ci:
                            continue
                        cell = {
                            "source_category": src,
                            "target_market": tgt,
                            "lag_bucket": lag,
                            "accuracy": entry["accuracy"],
                            "samples": n,
                            "ci_low": ci[0],
                            "ci_high": ci[1],
                        }
                        if cell["ci_low"] > 0.333:
                            edge_cells.append(cell)
                        elif cell["ci_high"] < 0.333:
                            anti_cells.append(cell)
        if ext_map:
            edge_cells.sort(key=lambda c: -(c["skill_ci_low"] or 0))
            anti_cells.sort(key=lambda c: (c["skill_ci_high"] or 0))
        else:
            edge_cells.sort(key=lambda c: -c["ci_low"])
            anti_cells.sort(key=lambda c: c["ci_high"])

        meta: Dict[str, Any] = {
            "total_category_target_lag_cells": total_cells,
            "total_samples": total_samples,
            "sample_count_filter": "sample_count >= 3 (statistical significance)",
            "source": "macro_prediction_accuracy_table + macro_prediction_accuracy_ext",
            "schema_version": "1.1",
            # [2026-06-13] 3-class 예측이므로 uniform random baseline = 1/3.
            # [2026-07-20 RCA T1] 다만 레짐은 sticky 시계열이라 uniform 이 아닌
            # persistence 가 올바른 null model — baseline_accuracy 는 하위호환으로
            # 유지하되(=uniform_baseline), 스킬 판정은 persistence_accuracy 기준.
            "baseline_accuracy": 0.333,
            "uniform_baseline": 0.333,
            "primary_baseline": "persistence_accuracy (per cell)",
            "prediction_classes": 3,
            "ci_method": (
                "episode-block bootstrap (episodes = runs of identical folded actual regime; "
                "B=1000, fixed seed) — corrects for outcome autocorrelation. Wilson CI kept "
                "as ci_naive for transparency; it assumes independent Bernoulli trials, which "
                "measured lag-1 rho of 0.80-0.90 (kr/us cells) violates badly."
            ),
            "interpretation": (
                "Predictions are 3-class (bearish/neutral/bullish). Regimes are sticky, so "
                "the honest null model is PERSISTENCE (the regime observed at prediction "
                "creation persists), not the uniform 0.333: in 2026-07 the 97-99% coin cells "
                "sat within 0.05pp of pure persistence (zero alpha). Judge each cell by "
                "skill_score = (accuracy - persistence_accuracy) / (1 - persistence_accuracy) "
                "with its skill_ci_95 (autocorrelation-corrected). n_effective << n_nominal "
                "means outcomes are batch-correlated; never treat n_nominal as independent "
                "evidence. accuracy_v2_only covers resolutions on/after the 2026-07-08 "
                "methodology-v2 cutover; methodology_mixed=true marks cells whose cumulative "
                "figures mix v1 and v2 judging. accuracy_ema_recent (alpha=0.02) degrades "
                "into an 'is the current regime episode being guessed right' indicator under "
                "autocorrelation — use for drift hints only."
            ),
            "edge_cells_criteria": (
                "criteria v2 (2026-07-20): edge_cells = forecast cells (horizon_type='forecast') "
                "with skill_ci_95 lower bound > 0 and samples >= 100 — i.e. beats the "
                "persistence null with autocorrelation-corrected significance. "
                "anti_predictive_cells = skill_ci_95 upper bound < 0 (systematically worse "
                "than persistence; shown for honesty). Cells in neither group are "
                "indistinguishable from persistence. v1 criteria (Wilson low > uniform 0.333) "
                "was retired after verification showed it certifies persistence, not alpha "
                "(docs/rca_20260720)."
            ),
            "edge_criteria_version": edge_criteria_version,
            # [2026-10-07 TRUST-001] 자체 감사 공개 — persistence 기준선이 약하다는 실측.
            "majority_class_baseline_note": (
                "majority_class_baseline = hit rate of always predicting the most frequent recorded "
                "outcome group for that cell, over the same window as accuracy_since_label_change "
                "(compare those two, not the all-time `accuracy`). beats_majority_baseline=false means "
                "the cell does not outperform a constant guess. Shadow predictions are excluded."
            ),
            # [2026-10-07 후속] 판정 라벨 정의 전환 — 시대별 적중률 분리
            "label_definition_change": {
                "at": LABEL_DEFINITION_CUTOVER,
                "what": (
                    "Candle regime labels (the judging target) switched from the legacy to the PKL "
                    "causal definition on 2026-10-01 (full-history relabel 00:06-00:47 KST, live from "
                    "00:55 KST). Outcomes before and after are judged against different label "
                    "definitions — coin's recorded outcome was 'neutral' ~95% of the time before and "
                    "0% after."
                ),
                "fields": (
                    "accuracy_since_label_change / samples_since_label_change / majority_class_baseline "
                    "are computed from the immutable ledger for predictions resolved after the change "
                    "(current-methodology era; small samples early on — read with "
                    "samples_since_label_change). `accuracy` (cumulative) and accuracy_ema_recent mix "
                    "both eras."
                ),
            },
            "persistence_method": persistence_method,
            "persistence_null_caveat": _PERSISTENCE_CAVEAT[persistence_method],
            # [2026-10-14 H8-1] 서빙 accuracy·samples 를 원장 원행으로 재계산하는 레시피 — get_ledger_integrity 와 같은 형식.
            #   10-14 카운터 1회 정정(H8-2) 이후 예외 없는 v2 로 25셀 전부 일치(admin E4 프로브가 상시 감시).
            "verification_recipe": {
                "version": ACCURACY_RECIPE_VERSION,
                "recipe": ACCURACY_RECIPE,
                "applies_to": "per-cell `accuracy` and `samples`",
                "not_applies_to": (
                    "n_nominal / accuracy_v2_only / samples_v2_only / skill_* / persistence_* come from a "
                    "separate daily batch over the last 200 days and use a different definition."
                ),
            },
            "counter_corrections": ACCURACY_COUNTER_CORRECTIONS,
            "changelog_1_1": (
                "schema_version 1.1 (2026-07-20): added per-cell persistence_accuracy, "
                "skill_score, skill_ci_95, accuracy_ci_block_95, n_nominal, n_effective, "
                "lag1_rho, n_episodes, accuracy_v2_only, samples_v2_only, methodology_mixed, "
                "resolution_delay_h_median, lifecycle_state, suggested_lag_hours. "
                "edge/anti judgment switched to persistence-skill basis. All 1.0 fields "
                "preserved unchanged."
            ),
            # [2026-07-08] 방법론 공개 — 감사 AI 가 물어볼 것을 먼저 답한다
            "actual_regime_methodology": (
                "actual regime = mode of regime_label over the target market's candles in the "
                "trailing 2h window at resolution time (v2, 2026-07-08; falls back to the "
                "latest labeled candle when the market is closed). Cells with lag_bucket=0h "
                "are resolved within the same analyzer cycle they were created — these are "
                "NOWCAST consistency checks, not forecasts (see horizon_type per cell). "
                "Filter horizon_type='forecast' when evaluating predictive skill. "
                "v1 (before 2026-07-08) judged against a single arbitrary symbol's latest "
                "candle — cumulative cells mix both methodologies across that cutover."
            ),
        }

        # [2026-08-18 schema 1.2 additive] 허브 심화 3블록 — 30d 실사용에서 이 도구가
        # 외부 세션의 90% 관문임이 실측돼(1,477/1,639) 신뢰 답변을 여기서 깊게 한다:
        # signal_gauge_v2(시그널측 수수료 차감 잣대)·known_biases(자기 결함 공개)·
        # ledger_health(채점 파이프 생존 뱃지). 전부 fail-open + TTL 캐시 (trust_extras).
        meta["schema_version"] = "1.2"
        meta["changelog_1_2"] = (
            "schema_version 1.2 (2026-08-18): added signal_gauge_v2 (signal-level "
            "fee-net honest gauge, single-stream), known_biases (self-measured defect "
            "disclosures with coordinates and mitigation policy tags), ledger_health "
            "(grading-pipe liveness badge). All 1.0/1.1 fields preserved unchanged."
        )
        extras: Dict[str, Any] = {}
        try:
            from oneqaz_trading_mcp.tools.trust_extras import get_trust_extras
            extras = get_trust_extras()
        except Exception:
            logger.warning("trust_extras unavailable — serving base response only")

        return {
            "summary": summary,
            "edge_cells": edge_cells[:10],
            "anti_predictive_cells": anti_cells[:10],
            "meta": meta,
            **extras,
        }
    except Exception as exc:
        logger.exception("get_prediction_accuracy failed")
        return mcp_error(
            MCPErrorCode.INTERNAL_ERROR,
            f"Failed to fetch prediction accuracy: {exc}",
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# P0 Tool 2: get_backtest_tuning_state — Layer B (자기 보정 증명)
# ---------------------------------------------------------------------------

def _get_backtest_tuning_state(
    category: Optional[str] = None,
    target_market: Optional[str] = None,
) -> Dict[str, Any]:
    """market_global.backtest_tuning PG 쿼리 — 자기 보정 상태."""
    # [2026-09-22] 열거형 파라미터 검증 — 미지 시장값이 조용히 통과해
    # 필터가 무시되거나 0건이 나왔다 (오타와 실제 공백이 구분 불가).
    target_market, _err = normalize_choice('market', 'target_market', target_market)
    if _err:
        return _err

    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    try:
        conn = open_schema_connection("market_global", readonly=True)
    except Exception as exc:
        logger.exception("get_backtest_tuning_state: PG open failed")
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"market_global PG schema unavailable: {exc}",
        )

    try:
        cur = conn.cursor()
        query = (
            "SELECT category, target_market, tuned_lag_hours, tuned_sensitivity, "
            "       confidence, sample_count, last_backtest "
            "FROM market_global.backtest_tuning WHERE 1=1"
        )
        params: List[Any] = []
        if category:
            query += " AND category = ?"
            params.append(category)
        if target_market:
            query += " AND target_market = ?"
            params.append(_macro_target(target_market))
        query += " ORDER BY last_backtest DESC NULLS LAST"

        rows = cur.execute(query, params).fetchall()

        # [2026-07-20 RCA T6] 라이프사이클 조인 (있으면) — 셀별 거버넌스 상태 노출
        lifecycle_map: Dict[Any, str] = {}
        try:
            for e in cur.execute(
                "SELECT source_category, target_market, lifecycle_state "
                "FROM market_global.macro_prediction_accuracy_ext "
                "WHERE horizon_type = 'forecast'"
            ).fetchall():
                lifecycle_map[(e["source_category"], e["target_market"])] = e["lifecycle_state"]
        except Exception:
            logger.warning("get_backtest_tuning_state: lifecycle join unavailable")

        from datetime import datetime as _dt, timezone as _tz
        _now = _dt.now(_tz.utc)
        stale_count = 0

        entries: List[Dict[str, Any]] = []
        for row in rows:
            last_backtest = row["last_backtest"]
            if last_backtest is not None and hasattr(last_backtest, "isoformat"):
                last_backtest = last_backtest.isoformat()
            # [2026-07-20 RCA T6] 48h 초과 스테일 플래그 — vix×us 동결(13일 방치)이
            # 외부 감사에서야 발견된 재발 방지 (verification_report §5)
            stale = None
            if last_backtest:
                try:
                    _lb = _dt.fromisoformat(str(last_backtest).replace("Z", "+00:00"))
                    if _lb.tzinfo is None:
                        _lb = _lb.replace(tzinfo=_tz.utc)
                    stale = (_now - _lb).total_seconds() > 48 * 3600
                except (ValueError, TypeError):
                    stale = None
            if stale:
                stale_count += 1
                logger.warning("[tuning] stale cell (>48h): %s→%s last_backtest=%s",
                               row["category"], row["target_market"], last_backtest)
            entries.append({
                "category": row["category"],
                "target_market": row["target_market"],
                "tuned_lag_hours": _safe_float(row["tuned_lag_hours"]),
                "tuned_sensitivity": _safe_float(row["tuned_sensitivity"]),
                "confidence": _safe_float(row["confidence"]),
                "sample_count": _safe_int(row["sample_count"]),
                "last_backtest": last_backtest,
                "stale": stale,
                "lifecycle_state": lifecycle_map.get(
                    (row["category"], row["target_market"])
                ),
            })

        return {
            "tuning_entries": entries,
            "meta": {
                "total_entries": len(entries),
                "stale_entries": stale_count,
                "stale_definition": "last_backtest older than 48h",
                "source": "backtest_tuning_table",
                "interpretation": (
                    "Each entry shows how our system auto-tuned lag_hours and sensitivity "
                    "based on real backtest results. confidence (v2, 2026-07) = Wilson 95% "
                    "lower bound of the cell's hit rate, normalized against the 3-class "
                    "random baseline (0.333), scaled by sample maturity (n/300, cap 1.0) — "
                    "values near 0 mean no statistically-backed skill yet, NOT missing data. "
                    "(v1 was min(1, n/100): a sample-count proxy that saturated every cell "
                    "at 1.000 — replaced because it carried zero information.) Values before "
                    "the next backtest cycle may still show the saturated v1 constant."
                ),
            },
        }
    except Exception as exc:
        logger.exception("get_backtest_tuning_state failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# P0 Tool 3: get_monthly_accuracy_trend — Layer B (월별 성과 시계열)
# ---------------------------------------------------------------------------

def _get_monthly_accuracy_trend(
    category: Optional[str] = None,
    target_market: Optional[str] = None,
) -> Dict[str, Any]:
    """market_global.backtest_results PG 쿼리 — 월별 정확도 시계열.

    Note: 2026-04-20 PG cutover. 데이터 수집이 2026-03 에 시작됨 (이전 월 없음).
    AI 소비자에게 data_collection_started, months_available 명시로 "단기 관측"
    투명성 제공.
    """
    # [2026-09-22] 열거형 파라미터 검증 — 종전에는 미지 시장값이 WHERE 절에
    # 그대로 들어가 **조용히 0건**을 냈다. 0건은 "해당 데이터가 없다" 라는
    # 유효한 답과 구분되지 않아 호출자가 오타를 영영 모른다.
    target_market, _err = normalize_choice('market', 'target_market', target_market)
    if _err:
        return _err

    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    try:
        conn = open_schema_connection("market_global", readonly=True)
    except Exception as exc:
        logger.exception("get_monthly_accuracy_trend: PG open failed")
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"market_global PG schema unavailable: {exc}",
        )

    try:
        cur = conn.cursor()
        query = (
            "SELECT category, target_market, lag_bucket, accuracy, sample_count, month, updated_at "
            "FROM market_global.backtest_results "
            "WHERE month IS NOT NULL AND month != 'all'"
        )
        params: List[Any] = []
        if category:
            query += " AND category = ?"
            params.append(category)
        if target_market:
            query += " AND target_market = ?"
            params.append(_macro_target(target_market))
        query += " ORDER BY month ASC, category ASC, target_market ASC"

        rows = cur.execute(query, params).fetchall()

        trend: List[Dict[str, Any]] = []
        for row in rows:
            trend.append({
                "month": row["month"],
                "category": row["category"],
                "target_market": row["target_market"],
                "lag_bucket": row["lag_bucket"],
                "accuracy": _safe_float(row["accuracy"]),
                "sample_count": _safe_int(row["sample_count"]),
            })

        months_available = sorted({r["month"] for r in trend}) if trend else []
        data_collection_started = months_available[0] if months_available else None

        # Also check earliest prediction for absolute start marker
        earliest_prediction: Optional[str] = None
        try:
            r_earliest = cur.execute(
                "SELECT MIN(created_at) AS earliest FROM market_global.macro_regime_predictions"
            ).fetchone()
            if r_earliest and r_earliest["earliest"]:
                v = r_earliest["earliest"]
                earliest_prediction = v.isoformat() if hasattr(v, "isoformat") else str(v)
        except Exception:
            pass

        return {
            "trend": trend,
            "meta": {
                "total_points": len(trend),
                "months_available": months_available,
                "months_covered_count": len(months_available),
                "data_collection_started": data_collection_started,
                "earliest_prediction_recorded": earliest_prediction,
                "source": "backtest_results_table",
                # [2026-07-20 RCA T1/T3] 월별 트렌드 오독 방지 — 검증에서 "개선 추세"
                # (42→61→99%)가 매월 persistence 를 소수점까지 추적함이 확정됨
                "methodology_note": (
                    "Months up to 2026-07 mix judging methodologies (v1 single-symbol "
                    "before 2026-07-08, v2 market-mode after). Raw monthly accuracy also "
                    "tracks market regime STICKINESS, not model improvement: verified "
                    "2026-07 that rising monthly accuracy moved in lockstep with the "
                    "persistence baseline (docs/rca_20260720). Judge improvement by "
                    "skill_score trends in get_prediction_accuracy, not by this series. "
                    # [2026-10-07] 판정 라벨 정의 전환 — 시대 경계
                    "Label-definition change on 2026-10-01 00:55 KST (legacy -> PKL candle "
                    "regime labels): months up to 2026-09 were judged against legacy labels and "
                    "are frozen at their last computation; from 2026-10 only post-change "
                    "resolutions are counted. The two eras are not comparable."
                ),
                "interpretation": (
                    "Monthly accuracy evolution. data_collection_started marks when the "
                    "evaluation pipeline began recording outcomes — months before this date "
                    "have no data (no backfill available, not a gap). For long-horizon "
                    "validation, wait for more months or use accuracy_ema from "
                    "get_prediction_accuracy (which covers the full collection window). "
                    "See methodology_note before treating trends as model improvement."
                ),
            },
        }
    except Exception as exc:
        logger.exception("get_monthly_accuracy_trend failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# P0 Tool 4: get_news_leading_indicator_performance — Layer A (예견 능력)
# ---------------------------------------------------------------------------

def _get_news_leading_indicator_performance(
    market_id: Optional[str] = None,
    min_sample_count: int = 3,
) -> Dict[str, Any]:
    """news/external_context.db::event_leading_scores에서 뉴스 선행 지표 성과 조회.

    데이터는 글로벌 news DB에 저장되며 market_id 컬럼으로 구분. market_id 미지정 시
    전체 시장 집계 반환.
    """
    # market_id 정규화 (None 허용 — 전체 조회)
    # [2026-09-22] 열거형 파라미터 검증 — 종전에는 미지 시장값이 WHERE 절에
    # 그대로 들어가 **조용히 0건**을 냈다. 0건은 "해당 데이터가 없다" 라는
    # 유효한 답과 구분되지 않아 호출자가 오타를 영영 모른다.
    market_id, _err = normalize_choice('market', 'market_id', market_id)
    if _err:
        return _err

    mid_normalized = None
    if market_id:
        mid_normalized = {
            "crypto": "coin_market",
            "coin": "coin_market",
            "coin_market": "coin_market",
            "kr": "kr_market",
            "kr_stock": "kr_market",
            "kr_market": "kr_market",
            "us": "us_market",
            "us_stock": "us_market",
            "us_market": "us_market",
        }.get(market_id.lower(), market_id)

    try:
        from external_context.core.db_utils import resolve_db_path
        news_db = resolve_db_path("news", "event_leading_scores")
    except ImportError:
        news_db = EXTERNAL_CONTEXT_DATA_DIR / "news" / "external_context.db"
    # SQLite 파일은 PG 이관 후 부재할 수 있음. _open_ro 가 PG 라우팅 처리.

    try:
        with _open_ro(news_db) as conn:

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='event_leading_scores'"
            )
            if not cursor.fetchone():
                return {
                    "indicators": [],
                    "meta": {
                        "message": "event_leading_scores table not populated yet",
                        "source_table": "news/external_context.db::event_leading_scores",
                    },
                }

            cursor = conn.execute("PRAGMA table_info(event_leading_scores)")
            columns = {col[1] for col in cursor.fetchall()}

            select_cols = []
            for opt in (
                "event_type",
                "market_id",
                "news_type",
                "leading_score",
                "avg_lead_time_minutes",
                "avg_anticipation_ratio",
                "accuracy_pct",
                "sample_count",
                "updated_at",
            ):
                if opt in columns:
                    select_cols.append(opt)

            if not select_cols:
                return {
                    "indicators": [],
                    "meta": {"message": "event_leading_scores has no recognized columns"},
                }

            query = f"SELECT {', '.join(select_cols)} FROM event_leading_scores WHERE 1=1"
            params: List[Any] = []
            if "sample_count" in columns:
                query += " AND sample_count >= ?"
                params.append(min_sample_count)
            if mid_normalized and "market_id" in columns:
                query += " AND market_id = ?"
                params.append(mid_normalized)
            if "leading_score" in columns:
                query += " ORDER BY leading_score DESC"

            rows = conn.execute(query, params).fetchall()

            # [2026-09-18 Ticket A] 미검증 수치는 public 응답에서 제거한다.
            # DB 원본은 그대로 두고 응답 사본에서만 가린다 — admin 진단 경로는
            # 이 함수를 거치지 않고 테이블을 직접 읽는다.
            from oneqaz_trading_mcp.verification import NEWS_LEAD_METRIC_FIELDS, strip_metric_fields
            indicators = strip_metric_fields(
                [dict(row) for row in rows], NEWS_LEAD_METRIC_FIELDS
            )

            return {
                "indicators": indicators,
                "meta": {
                    "total_indicators": len(indicators),
                    "market_id": mid_normalized,
                    "market_id_original": market_id,
                    "min_sample_count": min_sample_count,
                    "source_table": "news/external_context.db::event_leading_scores",
                    "interpretation": (
                        "Row inventory only: which (event_type, news_type) groupings the news "
                        "pipeline has accumulated, and how many samples each holds. "
                        "The underlying lead/score metrics are withheld while the calculation "
                        "method is under audit — they are not evidence of predictive or "
                        "anticipatory capability. "
                        "Pass market_id=None to see cross-market groupings."
                    ),
                    "withheld_fields": list(NEWS_LEAD_METRIC_FIELDS),
                },
            }
    except Exception as exc:
        logger.exception("get_news_leading_indicator_performance failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P0 Tool 5: get_news_causality_breakdown — Layer A (예견 vs 돌발 3분류)
# ---------------------------------------------------------------------------

def _get_news_causality_breakdown(
    market_id: str = "coin_market",
    days: int = 7,
) -> Dict[str, Any]:
    """news/external_context.db::news_causality_analysis에서 news_type 집계.

    주의: 뉴스 인과 데이터는 per-market DB가 아니라 news/external_context.db (글로벌)에
    저장되며, market_id 컬럼으로 구분된다. 따라서 market_id는 필터 조건으로 사용된다.

    market_id 정규화: "crypto", "coin" → "coin_market", "kr" → "kr_market", etc.
    """
    # market_id 정규화
    # [2026-09-22] 열거형 파라미터 검증 — 종전에는 미지 시장값이 WHERE 절에
    # 그대로 들어가 **조용히 0건**을 냈다. 0건은 "해당 데이터가 없다" 라는
    # 유효한 답과 구분되지 않아 호출자가 오타를 영영 모른다.
    market_id, _err = normalize_choice('market', 'market_id', market_id)
    if _err:
        return _err

    mid_normalized = {
        "crypto": "coin_market",
        "coin": "coin_market",
        "coin_market": "coin_market",
        "kr": "kr_market",
        "kr_stock": "kr_market",
        "kr_market": "kr_market",
        "us": "us_market",
        "us_stock": "us_market",
        "us_market": "us_market",
    }.get((market_id or "").lower(), market_id)

    # 뉴스 DB는 글로벌
    try:
        from external_context.core.db_utils import resolve_db_path
        news_db = resolve_db_path("news", "news_causality_analysis")
    except ImportError:
        news_db = EXTERNAL_CONTEXT_DATA_DIR / "news" / "external_context.db"
    # SQLite 파일은 PG 이관 후 부재할 수 있음. _open_ro 가 PG 라우팅 처리.

    try:
        with _open_ro(news_db) as conn:

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='news_causality_analysis'"
            )
            if not cursor.fetchone():
                return {
                    "breakdown": {},
                    "meta": {
                        "message": "news_causality_analysis table not populated yet",
                        "source_table": "news/external_context.db::news_causality_analysis",
                    },
                }

            cursor = conn.execute("PRAGMA table_info(news_causality_analysis)")
            columns = {col[1] for col in cursor.fetchall()}

            from datetime import datetime, timedelta, timezone
            cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)
            cutoff_iso = cutoff_dt.isoformat()

            wheres = []
            params: List[Any] = []
            if "market_id" in columns:
                wheres.append("market_id = ?")
                params.append(mid_normalized)
            if "computed_at" in columns:
                # PG 의 computed_at 은 TIMESTAMP 타입이라 ''(empty string) 과의 COALESCE
                # 비교가 type cast 실패. 문자열 컬럼에서만 공백 fallback 이 의미 있으므로
                # PG 에서는 단순 비교로 충분.
                wheres.append("computed_at >= ?")
                params.append(cutoff_iso)
            where_clause = ("WHERE " + " AND ".join(wheres)) if wheres else ""

            query = f"""
                SELECT news_type, COUNT(*) as cnt,
                       AVG(CASE WHEN anticipation_ratio IS NOT NULL THEN anticipation_ratio ELSE 0 END) as avg_anticipation,
                       AVG(CASE WHEN lead_time_minutes IS NOT NULL THEN lead_time_minutes ELSE 0 END) as avg_lead_time,
                       AVG(CASE WHEN confidence IS NOT NULL THEN confidence ELSE 0 END) as avg_confidence,
                       AVG(CASE WHEN precursor_score IS NOT NULL THEN precursor_score ELSE 0 END) as avg_precursor_score
                FROM news_causality_analysis
                {where_clause}
                GROUP BY news_type
                ORDER BY cnt DESC
            """
            rows = conn.execute(query, params).fetchall()

            breakdown = {
                row["news_type"] or "UNKNOWN": {
                    "count": _safe_int(row["cnt"]),
                    "avg_anticipation_ratio": round(_safe_float(row["avg_anticipation"]), 3),
                    "avg_lead_time_minutes": round(_safe_float(row["avg_lead_time"]), 1),
                    "avg_confidence": round(_safe_float(row["avg_confidence"]), 3),
                    "avg_precursor_score": round(_safe_float(row["avg_precursor_score"]), 3),
                }
                for row in rows
            }

            total = sum(v["count"] for v in breakdown.values())

            # [2026-09-18 Ticket A] 선행 주장 성분(lead time / anticipation ratio)은
            # 계산 방법 감사 전까지 public 집계에서 제거.
            from oneqaz_trading_mcp.verification import (
                NEWS_CAUSALITY_METRIC_FIELDS,
                strip_metric_fields_in_mapping,
            )
            breakdown = strip_metric_fields_in_mapping(
                breakdown, NEWS_CAUSALITY_METRIC_FIELDS
            )

            return {
                "breakdown": breakdown,
                "meta": {
                    "total_analyzed_news": total,
                    "window_days": days,
                    "market_id": mid_normalized,
                    "market_id_original": market_id,
                    "source_table": "news/external_context.db::news_causality_analysis",
                    "interpretation": (
                        "Counts of news items by the pipeline's internal label: 'anticipated' "
                        "(matched to a scheduled/calendar event) vs 'surprise' (no such match). "
                        "These are pipeline labels, not validated classifications — the labelling "
                        "rule and the lead/anticipation metrics behind it are under audit and are "
                        "withheld from this response."
                    ),
                    "withheld_fields": list(NEWS_CAUSALITY_METRIC_FIELDS),
                },
            }
    except Exception as exc:
        logger.exception("get_news_causality_breakdown failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P0 Tool 6: get_feature_governance_state — Layer C (거버넌스 투명성)
# ---------------------------------------------------------------------------

# [2026-10-14] 레거시 측정 동결 표기 (Mac 제안 · 사용자 승인). 동결 대상 목록·해제 env 의 정본은
# agent_history/core/feature_promoter.py `_frozen_types()`(env NEWS_FEATURE_LEGACY_FREEZE, 기본 동결) — 여기서 다시 정의하지 않는다.
# ⚠️ env 는 이 프로세스(mcp 컨테이너) 것을 읽는다 — 해제할 때는 intel 과 mcp 양쪽에 같은 값을 줄 것.
# 동결 전엔 응답이 멈춘 행을 현재 값처럼 내줘 외부 AI 가 그대로 인용하거나 stale 로 오지적할 수 있었다.
GOVERNANCE_FROZEN_REASON = (
    "Legacy measurement frozen on 2026-10-14: its definition was found invalid ('news direction' was the sign "
    "of the market-wide signal score on the first bar after the news, so every news item in a bar shared one "
    "value, and the sample window was misaligned). Status and counts no longer update — do not read them as "
    "current. A redefined measurement runs in shadow and is not exposed."
)


def governance_frozen_types() -> tuple:
    """현재 동결된 feature_type 목록 (정본 = feature_promoter._frozen_types)."""
    from oneqaz_trading_mcp.agent_history.core.feature_promoter import _frozen_types

    return tuple(_frozen_types())


def governance_frozen_summary(rows: List[Dict[str, Any]], frozen_types: tuple) -> Dict[str, Any]:
    """{feature_type: {rows, values_as_of, reason}} — values_as_of = 그 행들의 마지막 실제 평가 시각(max last_evaluated_at)."""
    out: Dict[str, Any] = {}
    for ft in frozen_types:
        sel = [r for r in rows if r.get("feature_type") == ft]
        if not sel:
            continue
        stamps = [
            v.isoformat() if hasattr(v, "isoformat") else str(v)       # 두 tool 이 같은 ISO 형식을 내도록
            for v in (r.get("last_evaluated_at") for r in sel) if v
        ]
        by_status: Dict[str, int] = {}
        for r in sel:
            if "status" in r:
                st = r.get("status") or "UNKNOWN"
                by_status[st] = by_status.get(st, 0) + 1
        out[ft] = {
            "rows": len(sel),
            # 상태별 — 집계(status_summary·by_status)에 섞인 동결 몫을 빼서 '현재 운영' 수를 말하기 위해
            "by_status": by_status,
            "values_as_of": max(stamps) if stamps else None,
            "reason": GOVERNANCE_FROZEN_REASON,
        }
    return out


def governance_frozen_status_count(frozen_summary: Dict[str, Any], status: str) -> int:
    """동결 피처 중 해당 상태의 행 수 합계."""
    return sum(int((v.get("by_status") or {}).get(status, 0)) for v in (frozen_summary or {}).values())


def _get_feature_governance_state(
    market_id: Optional[str] = None,
    status_filter: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """agent_history PG 스키마 feature_governance 테이블 조회.

    Note: agent_history 는 SQLite → PG 로 2026-04-17 cutover
    (project_external_agent_pg_done.md). feature_governance 는 global pool
    이라 market_id 컬럼이 없음 — market_id 파라미터는 호환성만 유지하고
    meta 에 "global pool" 임을 명시한다.
    """
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection

    # [2026-09-22] market_id 는 이 tool 에서 필터로 쓰이지 않지만(global pool),
    # meta.market_id_requested 로 되울려주므로 미지값을 그대로 받으면 "받아들여진
    # 필터" 처럼 보인다. 스키마에 enum 을 광고한 이상 서버가 지켜야 한다.
    market_id, _err = normalize_choice("market", "market_id", market_id)
    if _err:
        return _err

    # [2026-09-22] status_filter='YOLO' 가 WHERE status=? 에 그대로 들어가
    # 조용히 0건을 냈다 — "그런 상태의 feature 가 없다" 와 구분 불가였다.
    status_filter, _err = normalize_choice("governance_status", "status_filter", status_filter)
    if _err:
        return _err

    try:
        conn = open_schema_connection("agent_history", readonly=True)
    except Exception as exc:
        logger.exception("get_feature_governance_state: PG open failed")
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"agent_history PG schema unavailable: {exc}",
        )

    try:
        cur = conn.cursor()
        select_cols = [
            "feature_id", "feature_type", "source", "status",
            "delta_pf", "delta_mdd", "importance_rank",
            "oos_win_count", "oos_total_count",
            "last_evaluated_at", "updated_at", "description",
        ]
        query = f"SELECT {', '.join(select_cols)} FROM agent_history.feature_governance"
        params: List[Any] = []
        if status_filter:
            query += " WHERE status = ?"
            params.append(status_filter)
        query += " ORDER BY updated_at DESC NULLS LAST"

        rows = cur.execute(query, params).fetchall()
        features = [dict(row) for row in rows]

        # datetime → ISO string (JSON 직렬화용)
        for f in features:
            for k in ("last_evaluated_at", "updated_at"):
                v = f.get(k)
                if v is not None and hasattr(v, "isoformat"):
                    f[k] = v.isoformat()

        # [2026-10-14] 동결 표기 — 행마다 frozen(불리언, 전 행) + meta.frozen_feature_types(사유·마지막 실제 평가 시각)
        frozen_types = governance_frozen_types()
        for f in features:
            f["frozen"] = f.get("feature_type") in frozen_types
        frozen_summary = governance_frozen_summary(features, frozen_types)

        # 상태별 카운트
        status_counts: Dict[str, int] = {}
        # 평가 pending(표본 부족) vs 실제 평가됨 구분 — AI 가 "ACTIVE=0 이 고장인지
        # 단순히 축적중인지" 를 판단할 수 있게 메타를 풍부히 한다.
        # feature_gate_evaluator 기준: MIN_OOS_WINDOWS=3, SAMPLE_MIN=5.
        _MIN_OOS_WINDOWS = 3
        pending_eval = 0
        evaluated = 0
        for f in features:
            st = f.get("status", "UNKNOWN") or "UNKNOWN"
            status_counts[st] = status_counts.get(st, 0) + 1
            if (f.get("oos_total_count") or 0) < _MIN_OOS_WINDOWS:
                pending_eval += 1
            else:
                evaluated += 1

        active_n = status_counts.get("ACTIVE", 0)
        conditional_n = status_counts.get("CONDITIONAL", 0)
        deprecated_n = status_counts.get("DEPRECATED", 0)
        observation_n = status_counts.get("OBSERVATION", 0)

        # 데이터 기반 해석 문장 — 평가가 돌기 전인지, 실패한건지 구분.
        if active_n + conditional_n > 0:
            interp = (
                f"{active_n} features passed independent p-value validation "
                f"(ACTIVE) and {conditional_n} are regime-conditional. "
                f"{deprecated_n} features failed and were deprecated. "
                f"{pending_eval} of {len(features)} are still accumulating "
                f"OOS samples (min {_MIN_OOS_WINDOWS} required)."
            )
        elif evaluated > 0:
            interp = (
                f"No feature has currently passed the ACTIVE threshold "
                f"(delta_accuracy > 0 AND win_ratio >= 0.6 AND p < 0.10). "
                f"{evaluated} features were evaluated (sufficient OOS samples), "
                f"{deprecated_n} deprecated, {observation_n} staying in OBSERVATION. "
                f"{pending_eval} more are still accumulating samples. "
                f"This reflects statistical rigor — features are not promoted "
                f"just because samples exist."
            )
        else:
            interp = (
                f"{pending_eval} of {len(features)} features are still "
                f"accumulating OOS samples (min {_MIN_OOS_WINDOWS} required). "
                f"Evaluation has not produced any ACTIVE/DEPRECATED verdicts yet. "
                f"This is expected for newly registered features — check back "
                f"after more backtest cycles complete."
            )
        if frozen_summary:
            _fz = ", ".join(f"{ft} ({v['rows']} rows, values as of {v['values_as_of']})"
                            for ft, v in frozen_summary.items())
            _fa = governance_frozen_status_count(frozen_summary, "ACTIVE")
            _fc = governance_frozen_status_count(frozen_summary, "CONDITIONAL")
            interp += (
                f" FROZEN: {_fz} — legacy measurement with an invalid definition; their status and counts are "
                f"included above but no longer update (see meta.frozen_feature_types). Excluding them, live "
                f"ACTIVE = {active_n - _fa} and live CONDITIONAL = {conditional_n - _fc}."
            )

        # [2026-09-22] 자르기는 **집계가 전부 끝난 뒤**에 한다. status_summary /
        # interp / meta 카운트는 전체 531행 기준이어야 하고, 페이지 기준으로
        # 계산하면 "ACTIVE=0" 같은 거짓 진술이 나온다.
        eff_limit = clamp_limit(limit, _GOV_LIMIT_DEFAULT, _GOV_LIMIT_MAX)
        features_page = features[:eff_limit]
        common = hoist_constant_fields(features_page, _GOV_HOISTABLE_FIELDS)
        trunc = truncation_fields(
            returned=len(features_page),
            total=len(features),
            limit=eff_limit,
            maximum=_GOV_LIMIT_MAX,
            hint="status_filter 로 범위를 좁히세요 (status_summary 는 전체 기준입니다).",
        )

        out: Dict[str, Any] = {
            "features": features_page,
            "status_summary": status_counts,
            **trunc,
            "meta": {
                "total_features": len(features),
                "evaluated_count": evaluated,
                "pending_evaluation_count": pending_eval,
                "evaluation_thresholds": {
                    "MIN_OOS_WINDOWS": _MIN_OOS_WINDOWS,
                    "SAMPLE_MIN": 5,
                    "ACTIVE_WIN_RATIO": 0.6,
                    "P_VALUE_MAX": 0.10,
                    "DECAY_THRESHOLD": -0.02,
                    "DEPRECATED_LOSE_STREAK": 3,
                },
                "scope": "global pool (market_id not discriminated at storage)",
                "market_id_requested": market_id,
                "source_table": "agent_history.feature_governance (PG)",
                # [2026-10-14] 동결 피처 — 행의 frozen=true 와 짝. 비어 있으면 동결 없음.
                "frozen_feature_types": frozen_summary,
                "interpretation": interp,
                "lifecycle_rules": (
                    "OBSERVATION → (CONDITIONAL|ACTIVE) requires delta_accuracy>0, "
                    "win_ratio >= 0.6, p-value < 0.10, oos_total >= 3, sample >= 5. "
                    "ACTIVE → DEPRECATED on delta_accuracy <= -0.02 or 3 consecutive "
                    "OOS losses."
                ),
            },
        }
        if common:
            out["common"] = common
            out["common_note"] = (
                "common 의 값은 features 의 모든 행에 적용됩니다(행에서는 생략). "
                "행에 같은 키가 있으면 행 값이 우선합니다."
            )
        return out
    except Exception as exc:
        logger.exception("get_feature_governance_state failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# P1 Tool 7: get_structure_calibration — Layer D (섹터/바스켓 적중률)
# ---------------------------------------------------------------------------

_STRUCT_SCHEMA_BY_NORM = {
    "crypto": "market_coin_struct",
    "kr_stock": "market_kr_struct",
    "us_stock": "market_us_struct",
}
# 검증 지평 수리(STRUCT-VALIDATION r3) 이전 구간은 1w/1M 이 2일 지평으로 판정돼
# 수익률이 정확히 0.000(중립 오판) — 기준선 계산에서 제외한다.
_STRUCT_BASELINE_FLOOR_TS = 1785196800.0  # 2026-07-26 UTC


def _structure_skill_baseline(mid_norm: Optional[str]) -> Dict[str, Any]:
    """[2026-09-18 P1] 시장별 (모델 적중률, 다수클래스 기준선, 스킬) — 해석용 동봉.

    hit_rate_ema 만 주면 3-class 기준선(38~53%)과 대조할 수 없어 과신을 부른다.
    NaN 채점분과 지평 수리 이전 구간은 제외 — 둘 다 'neutral' 로 둔갑해 성적을 부풀린다.
    """
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection

    targets = ([mid_norm] if mid_norm else list(_STRUCT_SCHEMA_BY_NORM))
    out: Dict[str, Any] = {
        "metric": "majority_class_baseline",
        "note": (
            "skill_pp = accuracy - majority_class_baseline. Excludes NaN-scored rows and "
            "pre-2026-07-26 rows (2-day horizon mis-labeled as neutral). "
            "Negative skill_pp means the model is worse than always predicting the most "
            "common outcome."
        ),
        "by_market": {},
    }
    for norm in targets:
        schema = _STRUCT_SCHEMA_BY_NORM.get(norm)
        if not schema:
            continue
        try:
            with open_schema_connection(schema, readonly=True) as conn:
                row = conn.cursor().execute(
                    # 지평 컷오프는 **1w/1M 에만** 적용한다. 2일 지평 오판은 그 두
                    # 인터벌만의 문제였고(1d 는 0-수익률 오염 0.0~0.1%), 1d 까지 잘라내면
                    # 표본이 3~4배 줄어 기준선 추정이 불필요하게 흔들린다.
                    f"WITH v AS (SELECT actual_direction, is_correct "
                    f"             FROM {schema}.structure_predictions "
                    f"            WHERE is_correct IS NOT NULL "
                    f"              AND actual_avg_return::text <> 'NaN' "
                    f"              AND (\"interval\" = '1d' "
                    f"                   OR prediction_ts >= {_STRUCT_BASELINE_FLOOR_TS}) ), "
                    f"     b AS (SELECT COUNT(*) n FROM v GROUP BY actual_direction) "
                    f"SELECT (SELECT COUNT(*) FROM v) AS n, "
                    f"       (SELECT AVG(is_correct::float) FROM v) AS acc, "
                    f"       (SELECT MAX(n)::float / NULLIF((SELECT COUNT(*) FROM v),0) FROM b) AS base"
                ).fetchone()
            d = dict(row) if row is not None and not isinstance(row, dict) else (row or {})
            n = int(d.get("n") or 0)
            if n <= 0:
                continue
            acc = float(d.get("acc") or 0.0)
            base = float(d.get("base") or 0.0)
            out["by_market"][norm] = {
                "n": n,
                "accuracy": round(acc, 4),
                "majority_class_baseline": round(base, 4),
                "skill_pp": round((acc - base) * 100, 2),
            }
        except Exception:  # noqa: BLE001
            logger.warning("structure baseline 계산 실패 (schema=%s)", schema)
    return out


def _get_structure_calibration(
    market_id: Optional[str] = None,
    group_name: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """structure_calibration 테이블에서 섹터/바스켓 예측 적중률 집계."""
    if not STRUCTURE_LEARNING_DB.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"structure_learning.db not found at {STRUCTURE_LEARNING_DB}",
        )

    # [2026-09-22] 구조 tool 2종만 market_id 미지값에 에러를 내지 않았다 —
    # _normalize_market_id_for_structure 가 알 수 없는 값을 그대로 통과시켜
    # WHERE market_id=? 가 0건을 냈다. 다른 tool 은 스키마 맵 조회에서 이미
    # INVALID_MARKET 을 낸다.
    market_id, _err = normalize_choice("market", "market_id", market_id)
    if _err:
        return _err
    mid_norm = _normalize_market_id_for_structure(market_id) if market_id else None

    try:
        with _open_ro(STRUCTURE_LEARNING_DB) as conn:

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='structure_calibration'"
            )
            if not cursor.fetchone():
                return {
                    "calibration": [],
                    "meta": {
                        "message": "structure_calibration table not yet populated",
                        "source_table": "structure_learning.db::structure_calibration",
                    },
                }

            query = """
                SELECT market_id, group_name, interval, regime_bucket,
                       hit_rate_ema, avg_return_ema, sample_count, updated_at
                FROM structure_calibration
                WHERE sample_count >= 1
            """
            params: List[Any] = []
            if mid_norm:
                query += " AND market_id = ?"
                params.append(mid_norm)
            if group_name:
                query += " AND group_name = ?"
                params.append(group_name)
            # [2026-09-18 P1] 정렬 반전: hit_rate_ema DESC 는 **n=1 짜리 1.000 셀을
            # 목록 맨 위**에 올렸다 (실측: us 상위가 1.0/0.954/0.948… 이고 1위는 n=1).
            # 응답을 앞에서 잘라 읽는 클라이언트는 "섹터 예측 90~100% 적중" 으로 오독한다.
            # 표본 많은 셀을 먼저 — 적중률로 줄 세우지 않는다.
            query += " ORDER BY sample_count DESC, hit_rate_ema DESC"

            rows = conn.execute(query, params).fetchall()

            calibration = [
                {
                    "market_id": row["market_id"],
                    "group_name": row["group_name"],
                    "interval": row["interval"],
                    "regime_bucket": row["regime_bucket"],
                    "hit_rate_ema": round(_safe_float(row["hit_rate_ema"]), 3),
                    "avg_return_ema": round(_safe_float(row["avg_return_ema"]), 4),
                    "sample_count": _safe_int(row["sample_count"]),
                    "updated_at": row["updated_at"],
                }
                for row in rows
            ]

            # [2026-09-22] total_samples 는 **자르기 전 전체 기준**이다.
            # 페이지 기준으로 재면 "누적 표본" 이라는 말 자체가 거짓이 된다.
            total_samples = sum(c["sample_count"] for c in calibration)

            # 자르기는 집계·baseline 이 끝난 뒤. baseline 은 해석의 기준선이므로
            # 절대 표본(페이지) 기준으로 계산하지 않는다.
            eff_limit = clamp_limit(limit, _CALIB_LIMIT_DEFAULT, _CALIB_LIMIT_MAX)
            calibration_page = calibration[:eff_limit]
            # ⚠️ hoist 하지 않는다 (_CALIB_HOISTABLE_FIELDS = ()).
            # market_id/interval/regime_bucket 은 행 식별 차원키라, 필터 호출로
            # d=1 이 됐다고 common 으로 올리면 행만 읽는 소비자가 어느 시장·어느
            # 인터벌 얘긴지 모른 채 hit_rate 를 인용한다.
            trunc = truncation_fields(
                returned=len(calibration_page),
                total=len(calibration),
                limit=eff_limit,
                maximum=_CALIB_LIMIT_MAX,
                hint="market_id / group_name 으로 범위를 좁히세요 (baseline 과 meta 는 전체 기준입니다).",
            )

            return {
                "calibration": calibration_page,
                **trunc,
                # [2026-09-18 P1] 기준선 동봉 — 이게 없으면 hit_rate_ema 는 해석 불가다.
                # 3-class 예측이라 "다수 클래스만 찍기" 기준선이 38~53% 인데 종전 응답은
                # 절대 적중률만 줬다. macro 쪽 get_prediction_accuracy 는 2026-08-18 에
                # 같은 함정을 이미 수리했고(persistence 기준선 + skill), 구조 쪽만 남아 있었다.
                "baseline": _structure_skill_baseline(mid_norm),
                "meta": {
                    "total_entries": len(calibration),
                    "total_samples": total_samples,
                    "market_id": mid_norm,
                    "group_name_filter": group_name,
                    "source_table": "structure_learning.db::structure_calibration",
                    "sort": "sample_count DESC (NOT hit_rate — high-rate rows are often n=1)",
                    "pagination": (
                        "calibration[] is a page; total_available is the whole-set count. "
                        "baseline, total_entries and total_samples are whole-set."
                    ),
                    "interpretation": (
                        "Level 2 (ETF/basket/sector) prediction calibration. Each row shows "
                        "hit_rate_ema (EMA hit rate) per (market, group, interval, regime_bucket). "
                        "avg_return_ema is exponentially-weighted average return. "
                        "CAUTION: hit_rate_ema alone is NOT evidence of edge. This is a 3-class "
                        "problem whose majority-class baseline is ~38-53%; judge against "
                        "`baseline.skill_pp` (accuracy minus majority-class baseline), not the "
                        "raw rate. Many cells have sample_count 1-5, where hit_rate_ema is 0 or 1 "
                        "by construction. As of 2026-09-18 the measured skill is NEGATIVE in all "
                        "three markets (see baseline)."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_structure_calibration failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P1 Tool 8: get_structure_validation_history — Layer D (일별 검증 트렌드)
# ---------------------------------------------------------------------------

def _get_structure_validation_history(
    market_id: Optional[str] = None,
    days: int = 90,
) -> Dict[str, Any]:
    """structure_validation_history 테이블에서 일별 검증 결과 시계열 조회."""
    if not STRUCTURE_LEARNING_DB.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"structure_learning.db not found at {STRUCTURE_LEARNING_DB}",
        )

    # [2026-09-22] 구조 tool 2종만 market_id 미지값에 에러를 내지 않았다 —
    # _normalize_market_id_for_structure 가 알 수 없는 값을 그대로 통과시켜
    # WHERE market_id=? 가 0건을 냈다. 다른 tool 은 스키마 맵 조회에서 이미
    # INVALID_MARKET 을 낸다.
    market_id, _err = normalize_choice("market", "market_id", market_id)
    if _err:
        return _err
    mid_norm = _normalize_market_id_for_structure(market_id) if market_id else None

    try:
        with _open_ro(STRUCTURE_LEARNING_DB) as conn:

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='structure_validation_history'"
            )
            if not cursor.fetchone():
                return {
                    "history": [],
                    "meta": {
                        "message": "structure_validation_history table not yet populated",
                        "source_table": "structure_learning.db::structure_validation_history",
                    },
                }

            from datetime import datetime, timedelta, timezone
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

            query = """
                SELECT market_id, date, total_predictions, total_validated,
                       total_correct, hit_rate, avg_return
                FROM structure_validation_history
                WHERE date >= ?
            """
            params: List[Any] = [cutoff]
            if mid_norm:
                query += " AND market_id = ?"
                params.append(mid_norm)
            query += " ORDER BY date DESC, market_id ASC"

            rows = conn.execute(query, params).fetchall()

            # [2026-10-13 E2] 검증 0건인 날은 적중률·평균수익이 **없다(null)** — 종전 _safe_float 가
            #   NULL·0.0 을 0.0 으로 내보내 '전부 틀림' 으로 읽혔다(DB 에 0.0 으로 박힌 구행 39개도 여기서 가린다).
            def _rate(row: Any, col: str, nd: int) -> Optional[float]:
                if _safe_int(row["total_validated"]) <= 0 or row[col] is None:
                    return None
                return round(_safe_float(row[col]), nd)

            history = [
                {
                    "market_id": row["market_id"],
                    "date": row["date"],
                    "total_predictions": _safe_int(row["total_predictions"]),
                    "total_validated": _safe_int(row["total_validated"]),
                    "total_correct": _safe_int(row["total_correct"]),
                    "hit_rate": _rate(row, "hit_rate", 3),
                    "avg_return": _rate(row, "avg_return", 4),
                }
                for row in rows
            ]

            # 집계 통계 (overall 트렌드)
            # [2026-10-13] 분모 = **검증된** 예측. 종전 분모(total_predictions)는 아직 판정 안 된 예측을
            #   실패로 셌다(coin 30일 실측: 0.236 → 검증 기준 0.283).
            total_predictions_all = sum(h["total_predictions"] for h in history)
            total_validated_all = sum(h["total_validated"] for h in history)
            total_correct_all = sum(h["total_correct"] for h in history)
            overall_hit_rate = (
                round(total_correct_all / total_validated_all, 3)
                if total_validated_all > 0
                else None
            )

            return {
                "history": history,
                "summary": {
                    "window_days": days,
                    "total_predictions": total_predictions_all,
                    "total_validated": total_validated_all,
                    "total_correct": total_correct_all,
                    "overall_hit_rate": overall_hit_rate,
                    "overall_hit_rate_basis": "sum(total_correct) / sum(total_validated) over daily rows",
                    "counting_note": (
                        "Each daily row is a trailing-30-day aggregate written once a day, so the "
                        "summary sums overlapping windows: totals are a weighting, not counts of "
                        "distinct predictions. hit_rate/avg_return are null on days with 0 validated "
                        "predictions (no sample — not a 0% hit rate)."
                    ),
                },
                "meta": {
                    "total_days": len(history),
                    "market_id": mid_norm,
                    "source_table": "structure_learning.db::structure_validation_history",
                    "interpretation": (
                        "Daily Level 2 prediction validation history. Use to verify sustained "
                        "performance over time. No degradation = consistent edge."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_structure_validation_history failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P1 Tool 9: get_strategy_leaderboard — Layer E (Level 1 실제 수익)
# ---------------------------------------------------------------------------

_PG_PARTITION_BY_MARKET = {
    "crypto": "rl_pipeline.strategies_coin",
    "coin": "rl_pipeline.strategies_coin",
    "kr_stock": "rl_pipeline.strategies_kr",
    "kr": "rl_pipeline.strategies_kr",
    "us_stock": "rl_pipeline.strategies_us",
    "us": "rl_pipeline.strategies_us",
}


def _query_per_symbol_leaderboard(
    market_id: str,
    top_n: int,
    min_trades: int,
) -> List[Dict[str, Any]]:
    """rl_pipeline.strategies_{market} 파티션에서 per-symbol top 전략 직접 조회.

    PG 파티션 직접 쿼리 (partial index 로 12ms 수준).
    [2026-07-06] AWS 배포 제거 완료 — _global_predictions.db SQLite 폴백 삭제.
    [2026-07-06] quality_grade='F' 제외 — 외부 리더보드에 F등급(가짜/미검증 우위)
    전략이 top 으로 노출되던 문제 (라이브 실측: WAXP PF 3304, grade F).
    """
    partition = _PG_PARTITION_BY_MARKET.get(market_id.lower())
    if not partition:
        return []
    try:
        from oneqaz_trading_mcp.shared.db.pg_pool import get_pool
        pool = get_pool("rl_pipeline")
        sql = f"""
            SELECT symbol, "interval", strategy_type, regime, profit, profit_factor,
                   win_rate, trades_count, quality_grade, league
            FROM {partition}
            WHERE trades_count >= %s
              AND win_rate > 0
              AND win_rate NOT IN (0.4, 0.5, 1.0)
              AND is_staging = FALSE
              AND profit_factor IS NOT NULL
              AND COALESCE(quality_grade, '') <> 'F'
            ORDER BY profit_factor DESC NULLS LAST
            LIMIT %s
        """
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (min_trades, top_n))
                cols = [d.name for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.warning("per-symbol PG query failed for %s: %s", market_id, exc)
        return []


def _get_strategy_leaderboard(
    market_id: str = "crypto",
    top_n: int = 20,
    min_trades: int = 10,
    include_per_symbol: bool = True,
) -> Dict[str, Any]:
    """_global_predictions.db::global_strategies + per-symbol PG partition 두 갈래 조회.

    GLOBAL pool: 미리 median 합성된 글로벌 전략 (~20-100 rows per market).
    Per-symbol: rl_pipeline.strategies_{m} 파티션 직접. partial index 로 빠름.

    [Bug #3 fix 2026-04-27]: synthesizer가 win_rate=0.4/0.5/1.0/0 sentinel을 hardcode 하는
    경우가 많아서 (실측 ratio 98% sentinel, 측정값 2%), 각 row에 `win_rate_is_synthesized` 플래그
    표시 + meta에 합성/측정 비율 노출.
    [P1-C 2026-04-28]: per-symbol PG 파티션 병행 쿼리 추가. partial index 로 14M 정렬 회피.
    """
    strategies_dir = LEARNING_STRATEGIES_DIRS.get(market_id.lower())
    if not strategies_dir or not strategies_dir.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"learning_strategies dir not found for market: {market_id}",
            available_markets=list(LEARNING_STRATEGIES_DIRS.keys()),
        )

    global_db = strategies_dir / "_global_predictions.db"
    if not global_db.exists():
        return {
            "leaderboard": [],
            "meta": {
                "message": "_global_predictions.db not found yet",
                "source_table": "learning_strategies/_global_predictions.db::global_strategies",
            },
        }

    # [Wave J] _global_predictions.db 는 rl_pipeline PG 스키마로 라우팅됨.
    # market_id + is_staging 필터는 _MarketStagingFilterConnectionShim 이 자동 주입.
    try:
        conn = _open_ro(global_db)
        try:

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='global_strategies'"
            )
            if not cursor.fetchone():
                return {
                    "leaderboard": [],
                    "meta": {
                        "message": "global_strategies table not yet populated",
                        "source_table": "_global_predictions.db::global_strategies",
                    },
                }

            pragma = conn.execute("PRAGMA table_info(global_strategies)")
            cols = {c[1] for c in pragma.fetchall()}

            # 컬럼 가변성 대응: 있는 것만 select
            select_cols = []
            for opt in (
                "symbol",
                "interval",
                "strategy_type",
                "regime",
                "market_condition",
                "profit",
                "profit_factor",
                "win_rate",
                "trades_count",
                "quality_grade",
                "league",
                # [2026-09-22] direction_accuracy 제거 — 동어반복(항상 1.0).
                "volatility_accuracy",
                "description",
                "created_at",
            ):
                if opt in cols:
                    select_cols.append(opt)

            if not select_cols:
                return {
                    "leaderboard": [],
                    "meta": {"message": "global_strategies has no recognized columns"},
                }

            # profit_factor가 ranking 기준. 없으면 profit, 없으면 win_rate
            order_col = None
            for c in ("profit_factor", "profit", "win_rate"):
                if c in cols:
                    order_col = c
                    break

            wheres = [f"{order_col} IS NOT NULL"] if order_col else []
            params: List[Any] = []
            if "trades_count" in cols:
                wheres.append("trades_count >= ?")
                params.append(min_trades)
            where_clause = ("WHERE " + " AND ".join(wheres)) if wheres else ""

            order_clause = f"ORDER BY {order_col} DESC" if order_col else ""

            query = f"""
                SELECT {', '.join(select_cols)}
                FROM global_strategies
                {where_clause}
                {order_clause}
                LIMIT ?
            """
            params.append(top_n)

            rows = conn.execute(query, params).fetchall()
            # synthesizer 가 사용하는 sentinel/default win_rate 값.
            # 이 값들은 실측이 아니라 padding 일 가능성이 매우 높음 (실측 ratio: ~2%만 비-sentinel).
            _SENTINEL_WIN_RATES = {0.0, 0.4, 0.5, 1.0}
            leaderboard: List[Dict[str, Any]] = []
            synthesized_count = 0
            for row in rows:
                entry = dict(row)
                wr = entry.get("win_rate")
                tc = entry.get("trades_count")
                if wr is not None:
                    is_synth = float(wr) in _SENTINEL_WIN_RATES
                    entry["win_rate_is_synthesized"] = is_synth
                    if is_synth:
                        synthesized_count += 1
                if wr is not None and tc is not None:
                    try:
                        ci = _wilson_ci_95(float(wr), int(tc))
                        if ci is not None:
                            entry["win_rate_ci_95"] = ci
                    except Exception:
                        pass
                leaderboard.append(entry)

            measured_count = len(leaderboard) - synthesized_count

            per_symbol_rows: List[Dict[str, Any]] = []
            if include_per_symbol:
                per_symbol_rows = _query_per_symbol_leaderboard(market_id, top_n, min_trades)
                # Wilson CI for per-symbol rows
                for entry in per_symbol_rows:
                    wr = entry.get("win_rate")
                    tc = entry.get("trades_count")
                    if wr is not None and tc is not None:
                        try:
                            ci = _wilson_ci_95(float(wr), int(tc))
                            if ci is not None:
                                entry["win_rate_ci_95"] = ci
                        except Exception:
                            pass

            return {
                "leaderboard": leaderboard,
                "per_symbol_leaderboard": per_symbol_rows,
                "meta": {
                    "total_entries": len(leaderboard),
                    "synthesized_entries": synthesized_count,
                    "measured_entries": measured_count,
                    "per_symbol_entries": len(per_symbol_rows),
                    "market_id": market_id,
                    "min_trades_filter": min_trades,
                    "ranking_by": order_col or "unordered",
                    "baseline_win_rate": 0.5,
                    "source_table": "learning_strategies/_global_predictions.db::global_strategies (PG: rl_pipeline)",
                    "per_symbol_source": _PG_PARTITION_BY_MARKET.get(market_id.lower()),
                    "interpretation": (
                        "Two views: (1) `leaderboard` = GLOBAL pool synthesized from per-symbol strategies "
                        "via median (each row's `win_rate_is_synthesized` flags synthesizer defaults). "
                        "(2) `per_symbol_leaderboard` = real measurements from rl_pipeline.strategies_{m} "
                        "partition with sentinel win_rate (0.4/0.5/1.0) excluded. Use per-symbol for "
                        "edge verification, GLOBAL for pattern existence evidence. Ranked by profit_factor. "
                        "win_rate_ci_95 is Wilson score 95% CI."
                    ),
                    "_caveat": (
                        f"{synthesized_count}/{len(leaderboard)} GLOBAL entries have synthesized win_rate. "
                        "Treat synthesized rows as 'pattern existence evidence', not 'edge measurement'. "
                        "Cross-reference with `per_symbol_leaderboard` for measured edges."
                    ) if synthesized_count > 0 else None,
                },
            }
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as exc:
        logger.exception("get_strategy_leaderboard failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P1 Tool 10: get_active_predictions — Layer B/C (현재 검증 대기 중)
# ---------------------------------------------------------------------------

def _get_active_predictions(
    target_market: Optional[str] = None,
    limit: int = 20,
) -> Dict[str, Any]:
    """macro_regime_predictions 테이블에서 검증 대기 중인 (outcome IS NULL) 예측 조회."""
    # [2026-09-22] 열거형 파라미터 검증 — 미지 시장값이 조용히 통과해
    # 필터가 무시되거나 0건이 나왔다 (오타와 실제 공백이 구분 불가).
    target_market, _err = normalize_choice('market', 'target_market', target_market)
    if _err:
        return _err

    if not GLOBAL_PREDICTIONS_DB.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"global_predictions.db not found at {GLOBAL_PREDICTIONS_DB}",
        )

    try:
        with _open_ro(GLOBAL_PREDICTIONS_DB) as conn:

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='macro_regime_predictions'"
            )
            if not cursor.fetchone():
                return {
                    "predictions": [],
                    "meta": {
                        "message": "macro_regime_predictions table not populated yet",
                        "source_table": "global_predictions.db::macro_regime_predictions",
                    },
                }

            query = """
                SELECT source_category, source_regime_change, target_market,
                       predicted_regime_shift, lag_hours, confidence, created_at
                FROM macro_regime_predictions
                WHERE outcome IS NULL
                  AND lifecycle_state_at_creation IS DISTINCT FROM 'DEPRECATED'  -- [2026-10-07] 섀도우 제외
            """
            params: List[Any] = []
            if target_market:
                query += " AND target_market = ?"
                params.append(_macro_target(target_market))
            query += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)

            rows = conn.execute(query, params).fetchall()

            predictions = [
                {
                    "source_category": row["source_category"],
                    "regime_change": row["source_regime_change"],
                    "target_market": row["target_market"],
                    "predicted_shift": row["predicted_regime_shift"],
                    "lag_hours": _safe_float(row["lag_hours"]),
                    "confidence": _safe_float(row["confidence"]),
                    "created_at": row["created_at"],
                }
                for row in rows
            ]

            return {
                "predictions": predictions,
                "meta": {
                    "total_active": len(predictions),
                    "limit": limit,
                    "target_market_filter": target_market,
                    "source_table": "global_predictions.db::macro_regime_predictions (outcome IS NULL)",
                    "interpretation": (
                        "Currently pending predictions waiting for validation. Shows that OneQAZ "
                        "is actively making forecasts right now. Combined with get_prediction_accuracy "
                        "this proves we don't just show past wins — we're on the record for future outcomes."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_active_predictions failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P2 Tool 11: get_macro_influence_map — Transparency (인과 가설 노출)
# ---------------------------------------------------------------------------



def _get_macro_influence_map(
    market_id: Optional[str] = None,
) -> Dict[str, Any]:
    """_MACRO_INFLUENCE_MAP을 직렬화하여 반환. 시장 필터 지원.

    [2026-07-06] AWS gateway(코드 없는 배포) 제거 완료 — inline fallback 사본
    삭제. live 맵은 backtest_tuning 으로 계속 보정되는데 하드코딩 사본을 남기면
    import 회귀 시 낡은 인과 가설을 신선한 것처럼 서빙하게 된다 (겉보기 신선함
    병소). import 실패는 에러로 드러낸다.
    """
    # [2026-09-22] 열거형 파라미터 검증 — 종전에는 미지 시장값이 WHERE 절에
    # 그대로 들어가 **조용히 0건**을 냈다. 0건은 "해당 데이터가 없다" 라는
    # 유효한 답과 구분되지 않아 호출자가 오타를 영영 모른다.
    market_id, _err = normalize_choice('market', 'market_id', market_id)
    if _err:
        return _err

    # [2026-10-05 B4] "(live)" 표기 제거 — 서빙 값은 정적 사전값이다 (아래 interpretation).
    source_label = "market/global_regime/profiles.py::_MACRO_INFLUENCE_MAP (static prior)"
    try:
        from market.global_regime.profiles import _MACRO_INFLUENCE_MAP
    except Exception as exc:
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"macro influence map unavailable (profiles import failed): {exc}",
        )

    # 정규화: 'coin'/'crypto' → 'coin_market', etc.
    target_filter = None
    if market_id:
        target_filter = {
            "coin": "coin_market",
            "crypto": "coin_market",
            "coin_market": "coin_market",
            "kr": "kr_market",
            "kr_stock": "kr_market",
            "kr_market": "kr_market",
            "us": "us_market",
            "us_stock": "us_market",
            "us_market": "us_market",
        }.get(market_id.lower(), market_id)

    # [2026-10-05 스틸맨 C4 · 백로그 D15 본 처방] 엔진 실효값 병기.
    #   엔진은 profiles.get_macro_influence() → get_tuned_influence() 로 셀마다 backtest_tuning
    #   값을 우선 쓴다 (게이트 = sample_count >= MIN_SAMPLES_FOR_TUNING 이고 tuned_lag_hours > 0).
    #   그 조회는 실패를 except: pass 로 삼켜 "튜닝 없음" 과 구분되지 않으므로 재사용하지 않고,
    #   같은 게이트(상수는 import — 베끼지 않는다)로 직접 읽되 실패는 숨기지 않는다.
    effective_status = "ok"
    tuned: Dict[Any, Any] = {}
    try:
        from market.global_regime.learning.historical_backtester import MIN_SAMPLES_FOR_TUNING
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
        conn = open_schema_connection("market_global", readonly=True)
        try:
            for r in conn.execute(
                "SELECT category, target_market, tuned_lag_hours, tuned_sensitivity, "
                "confidence, sample_count FROM market_global.backtest_tuning "
                "WHERE sample_count >= ? AND tuned_lag_hours > 0",
                (MIN_SAMPLES_FOR_TUNING,),
            ).fetchall():
                tuned[(r["category"], r["target_market"])] = r
        finally:
            conn.close()
    except Exception as exc:
        logger.error("macro influence map: tuned-value lookup failed: %s", exc, exc_info=True)
        effective_status = f"unavailable ({type(exc).__name__})"
        tuned = {}

    # dict 직렬화 (JSON-serializable 보장)
    serialized: Dict[str, Any] = {}
    n_cells = 0
    n_tuned = 0
    for category, targets in _MACRO_INFLUENCE_MAP.items():
        if not isinstance(targets, dict):
            continue
        entry: Dict[str, Any] = {}
        for tgt_market, params in targets.items():
            if target_filter and tgt_market != target_filter:
                continue
            if isinstance(params, dict):
                cell: Dict[str, Any] = {
                    "lag_hours": _safe_float(params.get("lag_hours")),
                    "sensitivity": _safe_float(params.get("sensitivity")),
                }
                n_cells += 1
                if effective_status != "ok":
                    cell["effective"] = None
                elif (category, tgt_market) in tuned:
                    t = tuned[(category, tgt_market)]
                    n_tuned += 1
                    cell["effective"] = {
                        "lag_hours": _safe_float(t["tuned_lag_hours"]),
                        "sensitivity": _safe_float(t["tuned_sensitivity"]),
                        "source": "backtest_tuning",
                        "sample_count": int(t["sample_count"] or 0),
                        "confidence": _safe_float(t["confidence"]),
                    }
                else:
                    cell["effective"] = {
                        "lag_hours": cell["lag_hours"],
                        "sensitivity": cell["sensitivity"],
                        "source": "static_prior",
                    }
                entry[tgt_market] = cell
        if entry:
            serialized[category] = entry

    return {
        "influence_map": serialized,
        "meta": {
            "category_count": len(serialized),
            "target_market_filter": target_filter,
            "source": source_label,
            # [2026-10-05 B4 → 스틸맨 C4] 상위 lag_hours/sensitivity 는 정적 사전값이고, 셀마다
            # 엔진이 실제로 쓰는 값은 effective 블록이다 (2026-10-05 실측: 24/24 셀이 튜닝값).
            "values_kind": "static_prior",
            "effective_status": effective_status,
            "effective_tuned_cells": f"{n_tuned}/{n_cells}",
            "interpretation": (
                "Causal hypothesis map: each macro category (bonds, forex, vix, etc.) mapped to "
                "a target market with lag_hours (time to propagate) and sensitivity (strength "
                "0-1). Top-level lag_hours/sensitivity are OneQAZ's pre-defined STATIC PRIOR "
                "values. Each cell's `effective` block is what the engine actually uses at "
                "runtime: the backtest_tuning value whenever the cell passes the tuning gate "
                "(enough samples and a positive tuned lag), otherwise the static prior. "
                "meta.effective_tuned_cells shows how many cells are tuned."
            ),
            "related_tools": [
                "get_backtest_tuning_state (to see runtime calibration)",
                "get_prediction_accuracy (to see accuracy per category)",
            ],
        },
    }


# ---------------------------------------------------------------------------
# P2 Tool 12: explain_decision — Multi-source explanation
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# explain_decision helpers — narrative + meta builders
# ---------------------------------------------------------------------------

def _build_signal_narrative(sig: Dict[str, Any]) -> str:
    """시그널 dict를 사용자 친화 텍스트로 변환."""
    if not sig or sig.get("_error"):
        return "Signal data not available."
    action = (sig.get("action") or "unknown").lower()
    score = _safe_float(sig.get("signal_score"))
    confidence = _safe_float(sig.get("confidence"))
    pattern = sig.get("pattern_type") or ""
    cond = sig.get("market_condition") or ""

    # score_trace에서 alignment 추출
    alignment = 0.0
    trace = sig.get("score_trace")
    if isinstance(trace, dict):
        stage2 = trace.get("stage2_combined")
        if isinstance(stage2, dict):
            alignment = _safe_float(stage2.get("alignment"))

    parts = [f"Action: {action.upper()} (score={score:.3f}, confidence={confidence:.0%})."]
    if pattern:
        parts.append(f"Pattern: {pattern.replace('_', ' ')}.")
    if cond and cond != pattern:
        parts.append(f"Market condition: {cond}.")
    if isinstance(trace, dict):
        if abs(alignment) < 0.2:
            parts.append("Multi-timeframe disagreement (alignment near 0) — direction unclear.")
        elif alignment > 0.5:
            parts.append("Strong multi-timeframe alignment supporting the signal.")
        elif alignment < -0.5:
            parts.append("Strong multi-timeframe alignment against the signal — caution.")
    return " ".join(parts)


def _build_decisions_meta(decisions: List[Dict[str, Any]], signal: Dict[str, Any]) -> Dict[str, Any]:
    """recent_decisions가 비어있을 때 그 의미를 명시적으로 설명.

    핵심: virtual_trade_decisions는 sparse — 매매 조건이 충족된 경우만 row가 생긴다.
    빈 배열 = 시스템이 모니터링 중이지만 매매 안 함 (정상).
    """
    count = len(decisions or [])
    if count > 0:
        return {
            "count": count,
            "reason": "recent_trades_present",
            "interpretation": (
                f"System executed {count} recent trade decisions for this symbol. "
                "Each entry shows the decision (buy/sell/hold), thompson_score, and reason."
            ),
        }

    # 빈 경우 — signal action에 따라 의미 부여
    sig_action = (signal or {}).get("action", "") if isinstance(signal, dict) else ""
    sig_action = (sig_action or "").lower()

    if sig_action == "hold":
        reason = "no_trade_consistent_with_hold_signal"
        # [2026-08-11-r1] hold 라벨과 체결의 관계를 정확히 서술 — 실행 엔진은 라벨이
        # 아니라 점수 기반 후보 경로를 쓰므로, hold 라벨이어도 점수가 충분하면 매수될 수
        # 있다 (모순 서사 방지: 종전 문구는 'hold = 무거래 보장'으로 읽혔음).
        interp = (
            "No recent trade decisions for this symbol. The current signal label is 'hold'. "
            "Note that entries in this system are score-driven rather than label-driven: a "
            "'hold'-labeled signal with a sufficient score can still enter the buy-candidate "
            "path, so the hold label alone does not guarantee absence of trades — here, no "
            "entry/exit condition happened to be met. This is normal for sideways or "
            "consolidation markets."
        )
    elif sig_action in ("buy", "sell"):
        reason = "signal_present_no_trade_executed"
        interp = (
            f"Signal is '{sig_action}' but no recent trade was executed. "
            "Possible reasons: position limit reached, risk gate triggered, "
            "Thompson sampling weighted the signal too low, or signal too recent. "
            "Check positions/limits before assuming an issue."
        )
    elif not sig_action:
        reason = "no_signal_data"
        interp = "No signal data found for this symbol; cannot interpret decision absence."
    else:
        reason = "no_recent_activity"
        interp = (
            f"No recent trade decisions for this symbol. Current signal action: '{sig_action}'."
        )

    return {"count": 0, "reason": reason, "interpretation": interp}


def _build_historical_narrative(
    pattern: Optional[Dict[str, Any]],
    fingerprint: Dict[str, Any],
) -> str:
    """Phase 16-E explanation_patterns 결과를 한 줄 설명으로."""
    role = (fingerprint.get("role_hint") if fingerprint else None) or "unknown"
    grade = (fingerprint.get("match_grade") if fingerprint else None) or "ALL"
    if not pattern:
        return (
            f"No historical pattern stats yet for role={role}, grade={grade}. "
            f"Either insufficient samples (n<10) or this combo hasn't been seen before — "
            f"early-stage learning."
        )
    n = int(pattern.get("sample_count") or 0)
    hits = int(pattern.get("hit_count") or 0)
    hit_rate = float(pattern.get("hit_rate") or 0.0)
    avg_p = float(pattern.get("avg_profit_pct") or 0.0)
    regime = pattern.get("regime") or "any"
    # [2026-10-07 NEWS-001] 풀링 범위 명시 — BTC·ETH 가 같은 n=306,852 를 받아 '이 종목의
    # 과거 적중률' 로 오인됐다. 이 수치는 같은 role/regime/grade/interval 조합의 전 종목 합산이다.
    parts = [
        f"Past combo (role={role}, regime={regime}, grade={grade}, n={n}; pooled across all "
        f"symbols sharing this combo — not this symbol's own record): "
        f"{hits} hits, hit_rate={hit_rate * 100:.1f}%, avg_profit={avg_p:+.2f}%."
    ]
    cls = _classify_history(pattern)
    if cls == "untrusted":
        parts.append("Sample below trust threshold (n<10) — treat as directional only.")
    elif cls == "positive":
        parts.append("Historically positive — supports following the signal.")
    elif cls == "weak":
        parts.append("Historically weak — caution warranted.")
    else:
        parts.append("Mixed history — neutral.")
    return " ".join(parts)


def _classify_history(pattern: Optional[Dict[str, Any]]) -> str:
    """explanation_patterns 한 행 → none / untrusted / positive / weak / mixed.

    [2026-10-07 ANS-001] historical_narrative 와 verdict 가 **같은 판정**을 쓰도록 단일화.
    종전 verdict 는 이 판정을 아예 보지 않아, narrative 가 "Historically weak" 인 NVDA/AAPL
    에도 STRONG_BUY 를 냈다. 임계는 기존 narrative 값(≥0.6 & >0 / <0.4 or <0) 그대로.
    """
    if not pattern:
        return "none"
    if not bool(pattern.get("is_trustable")):
        return "untrusted"
    hit_rate = float(pattern.get("hit_rate") or 0.0)
    avg_p = float(pattern.get("avg_profit_pct") or 0.0)
    if hit_rate >= 0.6 and avg_p > 0:
        return "positive"
    if hit_rate < 0.4 or avg_p < 0:
        return "weak"
    return "mixed"


def _build_news_narrative(news: List[Dict[str, Any]]) -> str:
    """최근 뉴스 인과 데이터를 사용자 친화 요약으로 변환."""
    if not news:
        return "No recent news causality data for this market."
    anticipated = sum(1 for n in news if (n.get("news_type") or "").lower() == "anticipated")
    surprise = sum(1 for n in news if (n.get("news_type") or "").lower() == "surprise")
    valid_lead = [
        _safe_float(n.get("lead_time_minutes"))
        for n in news
        if n.get("lead_time_minutes") is not None
    ]
    # [2026-10-07 NEWS-001] 범위·검증 상태 명시 — 이 5건은 종목 필터 없는 '시장 전체 최신'
    # 이라 BTC·ETH·NVDA 가 같은 news_id(DART 공시 포함)를 받았다. 뉴스 인과 파이프라인은
    # get_news_causality_breakdown 에서 스스로 UNVERIFIED 로 표시돼 있다.
    # [2026-10-13] 시장 범위로는 거른다(news_events.target_markets) — 종목 필터는 여전히 없다.
    parts = [
        f"Market-scoped news context (this market's news scope), not filtered to this symbol "
        f"({len(news)} latest items): {anticipated} anticipated, {surprise} surprise."
    ]
    if valid_lead:
        avg_lead = sum(valid_lead) / len(valid_lead)
        parts.append(
            f"Average pre-event lead time: {avg_lead:.0f} minutes "
            "(computed per market — the same news id can differ across markets)."
        )
    else:
        parts.append("Lead time data not available for these events in this market.")
    parts.append(
        "The news-causality classification is UNVERIFIED (method under audit) — "
        "do not cite it as evidence for this symbol; it is not used in overall_recommendation."
    )
    return " ".join(parts)


# [2026-10-07 ANS-001] '강한' 신호 강도 임계 — combined |signal_score| 기준.
# 실측(2026-10-07, 시장별 최근 combined 4만 행): buy 행 |score| p50 0.13~0.17 · p75 0.20~0.25
# (coin 0.21 / kr 0.25 / us 0.20). 3시장 공통 p75 하한인 0.20 을 '강함' 으로 둔다.
# raw confidence 는 0.70~0.74 에 몰려 변별력이 없어 강도 판정에 쓰지 않는다(basis 에만 노출).
_STRONG_SCORE_ABS = 0.20


def _latest_decision(decisions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """최근 결정 1건 요약 (virtual_trade_decisions 는 timestamp DESC 로 읽힌다)."""
    if not decisions:
        return {}
    d = decisions[0] or {}
    decision = str(d.get("decision") or "").lower()
    reason = str(d.get("reason") or "")
    # '이미 보유 중 (추매 불가)' — hold 결정 중 '보유 중이라 추가 매수 안 함' 을 구분한다.
    holding = decision == "hold" and ("보유" in reason or "holding" in reason.lower())
    return {
        "decision": decision or None,
        "reason": reason or None,
        "timestamp": d.get("timestamp"),
        "already_holding": holding,
    }


def _build_overall_recommendation(
    signal: Dict[str, Any],
    decisions: List[Dict[str, Any]],
    news: List[Dict[str, Any]],
    historical_pattern: Optional[Dict[str, Any]] = None,
    market_id: Optional[str] = None,
) -> Dict[str, Any]:
    """신호·결정·과거 결과를 결합한 verdict.

    [2026-10-07 ANS-001] 종전 판정은 `action=='buy' and len(decisions)>0` 이면 STRONG_BUY 였다.
    decision 값을 보지 않아 '이미 보유 중 (추매 불가)' hold 결정만 있어도 "The system is actively
    buying — high conviction" 을 냈고(ETH/NVDA/AAPL 실호출), historical 이 weak(39.6%, −0.34%)
    여도 STRONG 이었다. meta.interpretation 이 이 verdict 를 그대로 인용하라고 안내하므로
    면책 문구가 붙어도 사실이 아닌 문장이 사용자에게 닿았다.

    규칙:
      - STRONG_{BUY,SELL} = 방향 신호 + |score| ≥ _STRONG_SCORE_ABS + historical 'positive'
        (같은 판정 함수 _classify_history — narrative 와 verdict 가 갈리지 않는다).
        그 외 방향 신호는 WEAK_{BUY,SELL} 이며 text 가 어느 다리가 비었는지 말한다.
      - "buying/selling" 같은 실행 서술은 최근 결정이 실제로 buy/sell 일 때만 쓴다.
        보유 중 hold 는 사실대로 "Already holding — no additional buy".
      - 뉴스는 verdict 에 쓰지 않는다 — 종목 필터 없는 시장 전체 뉴스이고 파이프라인이
        UNVERIFIED 다(NEWS-001). 종전 WAIT_NEWS 분기는 이 뉴스가 결정했다 → WAIT_SIDEWAYS 로 흡수.
      - basis 에 판정 입력을 전부 싣는다 — 외부 검증자가 같은 모순을 자동 검출할 수 있게.
    """
    sig = signal if isinstance(signal, dict) else {}
    sig_action = (sig.get("action") or "").lower()
    score = sig.get("signal_score")
    try:
        score_f = float(score) if score is not None else None
    except (TypeError, ValueError):
        score_f = None
    cal = None
    try:
        from oneqaz_trading_mcp.confidence_calibration import calibrated_confidence
        cal = calibrated_confidence(market_id, "combined", sig.get("confidence"))
    except Exception:
        cal = None

    dec = _latest_decision(decisions)
    decision = dec.get("decision")
    hist_cls = _classify_history(historical_pattern)
    hp = historical_pattern or {}

    strong_score = score_f is not None and abs(score_f) >= _STRONG_SCORE_ABS
    history_ok = hist_cls == "positive"

    # 실행 사실 서술 (verdict 와 독립 — 결정 로그에 있는 것만 말한다)
    if decision == "buy":
        exec_txt = "The system's latest decision for this symbol was a paper BUY."
    elif decision == "sell":
        exec_txt = "The system's latest decision for this symbol was a paper SELL."
    elif dec.get("already_holding"):
        exec_txt = "Already holding — no additional buy (the latest decision was hold: position already open)."
    elif decision == "hold":
        exec_txt = "The latest decision was hold — no new trade executed."
    else:
        exec_txt = "No recent trade decision for this symbol."

    hist_txt = {
        "positive": "Historical outcomes for this signal type are positive",
        "weak": "Historical outcomes for this signal type are weak — caution warranted",
        "mixed": "Historical outcomes for this signal type are mixed (neutral)",
        "untrusted": "Historical sample is below the trust threshold",
        "none": "No historical outcome stats for this signal type yet",
    }[hist_cls]
    if hist_cls in ("positive", "weak", "mixed") and hp:
        try:
            hist_txt += (
                f" (pooled hit rate {float(hp.get('hit_rate') or 0) * 100:.1f}%, "
                f"avg {float(hp.get('avg_profit_pct') or 0):+.2f}%, n={int(hp.get('sample_count') or 0)})"
            )
        except (TypeError, ValueError):
            pass
    hist_txt += "."
    score_txt = f"score {score_f:+.3f}" if score_f is not None else "score n/a"

    if sig_action in ("buy", "sell"):
        side = sig_action.upper()
        if strong_score and history_ok:
            verdict = f"STRONG_{side}"
            lead = f"{side.title()} signal with a strong {score_txt} and supportive history."
        else:
            verdict = f"WEAK_{side}"
            why = []
            if not strong_score:
                why.append(f"{score_txt} is below the strong threshold (|score| ≥ {_STRONG_SCORE_ABS})")
            if not history_ok:
                why.append("history does not confirm it")
            lead = f"{side.title()} signal, but {' and '.join(why)}."
        text = f"{lead} {hist_txt} {exec_txt}"
    elif sig_action == "hold":
        verdict = "WAIT_SIDEWAYS"
        text = (
            f"No directional signal (hold, {score_txt}). No clear edge — the research signal "
            f"suggests waiting until a directional signal emerges. {exec_txt}"
        )
    else:
        verdict = "UNCLEAR"
        text = "Insufficient signal data for a clear recommendation."

    basis = {
        "signal_action": sig_action or None,
        "signal_score": round(score_f, 4) if score_f is not None else None,
        "signal_timestamp": sig.get("timestamp"),
        "confidence_raw": sig.get("confidence"),
        "confidence_calibrated": cal,
        "latest_decision": decision,
        "latest_decision_reason": dec.get("reason"),
        "latest_decision_timestamp": dec.get("timestamp"),
        "already_holding": bool(dec.get("already_holding")),
        "historical_class": hist_cls,
        "historical_hit_rate": hp.get("hit_rate"),
        "historical_avg_profit_pct": hp.get("avg_profit_pct"),
        "historical_sample_count": hp.get("sample_count"),
        "historical_scope": "pooled across all symbols with the same role/regime/grade/interval",
        "news_flag": "market_wide_unverified_not_used",
        "rules": {
            "strong_requires": f"|signal_score| >= {_STRONG_SCORE_ABS} AND historical_class == 'positive'",
            "historical_positive": "is_trustable AND hit_rate >= 0.60 AND avg_profit_pct > 0",
            "historical_weak": "hit_rate < 0.40 OR avg_profit_pct < 0",
            "execution_wording": "buy/sell execution is stated only when latest_decision is buy/sell",
        },
    }
    return {"verdict": verdict, "text": text, "basis": basis}


def _explain_decision(
    market_id: str,
    symbol: str,
) -> Dict[str, Any]:
    """특정 심볼의 최근 시그널/결정/뉴스 인과를 조합해서 설명.

    원천:
    - signals.score_trace (JSON) — 지표별 기여도
    - virtual_trade_decisions — thompson_score + regime_score + reason
    - news_causality_analysis — 해당 심볼 관련 최근 뉴스 (news DB)
    """
    import json as json_lib

    # [2026-10-07 VALID-001/LAT-001] 시장 정규화 + 종목 커버리지 확인을 맨 앞에.
    # 종전에는 없는 종목도 무거운 조회를 전부 돈 뒤(30.3s — decision_fingerprint 전체 역스캔이
    # statement_timeout 에 걸릴 때까지) verdict=UNCLEAR 로 끝났다.
    market_id, _err = normalize_choice("market", "market_id", market_id)
    if _err:
        return _err
    from oneqaz_trading_mcp.symbol_coverage import check_symbol, normalize_symbol
    _nf = check_symbol(market_id, symbol, tool="explain_decision")
    if _nf:
        return _nf
    symbol = normalize_symbol(symbol)

    # 1) per-symbol signal DB에서 최신 시그널
    sig_dir = SIGNAL_DIR_PATHS.get(market_id.lower())
    if not sig_dir:
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Signal dir not found for market: {market_id}",
            available_markets=list(SIGNAL_DIR_PATHS.keys()),
        )

    # signal DB는 symbol 소문자_signal.db — PG 라우팅용 논리 키
    # [2026-08-10] .exists() 게이트 제거: SQLite 동결 후 상장 심볼(파일 부재)도
    # PG 에는 시그널이 있다. 미존재 심볼은 PG 쿼리가 0행을 돌려 자연 처리되고,
    # 라우팅이 심볼을 upper 정규화하므로 대소문자 hint 도 불필요해짐.
    sig_db = sig_dir / f"{symbol.lower()}_signal.db"

    signal_info: Dict[str, Any] = {}
    try:
        with _open_ro(sig_db) as conn:

            # 실제 컬럼 파악 (스키마 가변성 대응)
            pragma = conn.execute("PRAGMA table_info(signals)")
            sig_cols = {c[1] for c in pragma.fetchall()}

            wanted = [
                "timestamp",
                "signal_score",
                "confidence",
                "action",
                "reason",
                "score_trace",
            ]
            for opt in ("pattern_type", "market_regime", "market_condition", "recommended_strategy", "behavior_action"):
                if opt in sig_cols:
                    wanted.append(opt)

            select_clause = ", ".join(c for c in wanted if c in sig_cols)
            # [2026-07-08] score_trace 보유 행 우선 — combined 행만 trace 를 탑재하는데
            # per-interval 행이 같은 timestamp 로 저장돼 "최신 1행"이 상시 NULL 행을
            # 집었다 (explain_decision 의 score_trace 레이어가 죽어 보이던 실증상).
            # [2026-10-07 LAT-001] 그 의도를 `interval='combined'` 로 직접 표현한다. 종전
            # `ORDER BY (score_trace IS NOT NULL) DESC, timestamp DESC` 는 식 정렬이라 인덱스를
            # 못 타 심볼 전 행을 읽었다(coin ETH 8.9만 행·7만 버퍼, 콜드 27.4s = crypto 첫 호출
            # 13~18s 의 실체). combined 는 (symbol, interval, timestamp) PK 조기 종료로 수 ms,
            # 3시장 combined 행 score_trace 보유율 100%(2026-10-07 실측 각 4만 행).
            row = conn.execute(
                f"SELECT {select_clause} FROM signals "
                f"WHERE interval = 'combined' ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            if row is None:  # combined 미생성 심볼 — 아무 interval 최신 행 (인덱스 사용 가능)
                row = conn.execute(
                    f"SELECT {select_clause} FROM signals ORDER BY timestamp DESC LIMIT 1"
                ).fetchone()
            if row:
                signal_info = {k: (_safe_float(row[k]) if k in ("signal_score", "confidence") else row[k]) for k in row.keys() if k != "score_trace"}
                # score_trace JSON 파싱 (있으면)
                # [2026-07-08] PG jsonb 는 psycopg 가 이미 dict 로 반환 — loads(dict)가
                # TypeError 로 떨어져 raw 문자열 폴백만 서빙되던 결함 수리.
                if "score_trace" in row.keys():
                    raw_trace = row["score_trace"]
                    if isinstance(raw_trace, (dict, list)):
                        signal_info["score_trace"] = raw_trace
                    elif raw_trace:
                        try:
                            signal_info["score_trace"] = json_lib.loads(raw_trace)
                        except Exception:
                            signal_info["score_trace_raw"] = str(raw_trace)[:500]
    except Exception as exc:
        logger.debug(f"signal DB read failed: {exc}")
        signal_info["_error"] = str(exc)

    # 2) trading_system.db::virtual_trade_decisions 최근 결정
    trading_db = get_market_db_path(market_id)
    decision_info: List[Dict[str, Any]] = []
    # [2026-08-10] .exists() 제거 — PG 라우팅 논리 키 (백업 파일 archive 시에도 PG 서빙).
    # 연결 실패는 아래 except 가 debug 로 흡수 — 기존 silent skip 과 동일 종착.
    if trading_db:
        try:
            with _open_ro(trading_db) as conn:
                # 테이블 존재 확인
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='virtual_trade_decisions'"
                )
                if cursor.fetchone():
                    pragma = conn.execute("PRAGMA table_info(virtual_trade_decisions)")
                    cols = {c[1] for c in pragma.fetchall()}
                    sym_col = "symbol" if "symbol" in cols else "coin"
                    select_cols = [sym_col, "decision", "signal_score", "timestamp", "reason"]
                    for opt in ("thompson_score", "regime_score", "regime_name", "ai_score", "ai_reason"):
                        if opt in cols:
                            select_cols.append(opt)
                    rows = conn.execute(
                        f"""
                        SELECT {', '.join(select_cols)}
                        FROM virtual_trade_decisions
                        WHERE {sym_col} = ? OR {sym_col} = ?
                        ORDER BY timestamp DESC
                        LIMIT 3
                        """,
                        (symbol, symbol.upper()),
                    ).fetchall()
                    decision_info = [dict(r) for r in rows]
        except Exception as exc:
            logger.debug(f"decisions read failed: {exc}")

    # 3) news_causality_analysis에서 해당 시장의 최근 ANTICIPATED 뉴스 3건
    try:
        from external_context.core.db_utils import resolve_db_path
        news_db = resolve_db_path("news", "news_causality_analysis")
    except ImportError:
        news_db = EXTERNAL_CONTEXT_DATA_DIR / "news" / "external_context.db"
    news_info: List[Dict[str, Any]] = []
    if True:  # PG routed via _open_ro — always attempt
        try:
            mid_norm = {
                "coin": "coin_market", "crypto": "coin_market", "coin_market": "coin_market",
                "kr": "kr_market", "kr_stock": "kr_market", "kr_market": "kr_market",
                "us": "us_market", "us_stock": "us_market", "us_market": "us_market",
            }.get(market_id.lower(), market_id)

            with _open_ro(news_db) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='news_causality_analysis'"
                )
                if cursor.fetchone():
                    pragma = conn.execute("PRAGMA table_info(news_causality_analysis)")
                    cols = {c[1] for c in pragma.fetchall()}
                    select_bits = ["news_id", "news_type"]
                    for opt in (
                        "anticipation_ratio",
                        "lead_time_minutes",
                        "precursor_score",
                        "confidence",
                        "computed_at",
                    ):
                        if opt in cols:
                            select_bits.append(opt)
                    # [2026-10-13 NEWS-001] 이 시장 범위의 뉴스만 — 뉴스 인과 분석은 모든 뉴스를 모든
                    #   시장에 대해 계산하므로 market_id 만으로는 BTC·ETH 근거에 DART 공시가 섞였다.
                    #   news_events.target_markets(external_context/core/news_scope.py)와 조인해 거른다.
                    #   인과 분석 엔진·통계는 건드리지 않는다(읽기 시점 필터).
                    rows = conn.execute(
                        f"""
                        SELECT {', '.join('c.' + b for b in select_bits)}
                        FROM news_causality_analysis c
                        JOIN news_events n ON n.id = c.news_id
                        WHERE c.market_id = ?
                          AND ? = ANY(n.target_markets)
                        ORDER BY c.computed_at DESC
                        LIMIT 5
                        """,
                        (mid_norm, mid_norm),
                    ).fetchall()
                    news_info = [dict(r) for r in rows]
        except Exception as exc:
            # [2026-10-13] debug → warning: 근거 뉴스가 조용히 비면 narrative 가 '데이터 없음' 으로 읽힌다
            logger.warning(f"news causality read failed: {exc}")

    # 4) Phase 16-B/E: decision_fingerprint × explanation_patterns
    #    역사적으로 (role_hint, regime, match_grade) 조합이 어떻게 끝났는지 근거.
    historical_pattern: Optional[Dict[str, Any]] = None
    fingerprint_info: Dict[str, Any] = {}
    # [2026-08-10] .exists() 제거 — 위 2) 와 동일한 이유
    if trading_db:
        try:
            import json as json_lib
            with _open_ro(trading_db) as conn:
                # [2026-10-07 LAT-001] 종전 `WHERE symbol=? ORDER BY created_at DESC LIMIT 1` 은
                # 플래너가 idx_df_created 역방향 스캔을 골라, 그 심볼 행이 없거나 오래됐으면
                # 테이블 전체(us 4.7GB / 178만 행)를 훑다 statement_timeout 에 걸렸다(실측 >60s,
                # 응답 30.3s 의 실체 — 예외는 아래 except 가 삼켜 조용히 빈 지문).
                # ① 최신 combined 시그널의 signal_id 로 PK 조회(수 ms) ② 실패 시 심볼 인덱스로
                # 먼저 거른 뒤 정렬(OFFSET 0 = 서브쿼리 평탄화 차단, 심볼당 수천 행 · 콜드 ~0.7s).
                _fp_cols = (
                    "signal_id, interval, role_hint, match_grade, role_match_count, "
                    "mtf_pass_count, final_score, action, confidence, mtf_context_json, created_at"
                )
                fp_row = None
                _sig_ts = signal_info.get("timestamp")
                if _sig_ts:
                    fp_row = conn.execute(
                        f"SELECT {_fp_cols} FROM decision_fingerprint WHERE signal_id = ?",
                        (f"{symbol}_combined_{int(_sig_ts)}_live",),
                    ).fetchone()
                if fp_row is None:
                    fp_row = conn.execute(
                        f"SELECT * FROM (SELECT {_fp_cols} FROM decision_fingerprint "
                        f"WHERE symbol = ? OFFSET 0) t ORDER BY created_at DESC LIMIT 1",
                        (symbol,),
                    ).fetchone()
                if fp_row:
                    fingerprint_info = dict(fp_row)
                    role_hint = fingerprint_info.get("role_hint") or "unknown"
                    match_grade = fingerprint_info.get("match_grade") or "ALL"
                    interval_v = fingerprint_info.get("interval") or "ALL"
                    regime_key = "any"
                    try:
                        mtf_raw = fingerprint_info.get("mtf_context_json")
                        if mtf_raw:
                            mtf_dict = (
                                mtf_raw if isinstance(mtf_raw, dict)
                                else json_lib.loads(mtf_raw)
                            )
                            regime_key = str(mtf_dict.get("regime") or "any")
                    except Exception:
                        pass

                    ep_row = conn.execute(
                        """
                        SELECT role_hint, regime, match_grade, interval,
                               sample_count, hit_count, miss_count,
                               hit_rate, avg_profit_pct, median_profit_pct,
                               avg_signal_score, avg_confidence,
                               is_trustable, last_updated
                        FROM explanation_patterns
                        WHERE role_hint = ? AND regime = ?
                          AND match_grade = ? AND interval = ?
                        LIMIT 1
                        """,
                        (role_hint, regime_key, match_grade, interval_v),
                    ).fetchone()
                    if ep_row is None:
                        ep_row = conn.execute(
                            """
                            SELECT role_hint, regime, match_grade, interval,
                                   sample_count, hit_count, miss_count,
                                   hit_rate, avg_profit_pct, median_profit_pct,
                                   avg_signal_score, avg_confidence,
                                   is_trustable, last_updated
                            FROM explanation_patterns
                            WHERE role_hint = ? AND regime = 'any'
                              AND match_grade = 'ALL' AND interval = 'ALL'
                            LIMIT 1
                            """,
                            (role_hint,),
                        ).fetchone()
                    if ep_row:
                        historical_pattern = dict(ep_row)
        except Exception as exc:
            logger.debug(f"explanation_patterns read failed: {exc}")

    historical_narrative = _build_historical_narrative(
        historical_pattern, fingerprint_info
    )

    # Narrative + meta builders (사용자 친화 텍스트 + 빈 응답 의미 부여)
    signal_narrative = _build_signal_narrative(signal_info)
    decisions_meta = _build_decisions_meta(decision_info, signal_info)
    news_narrative = _build_news_narrative(news_info)
    overall_recommendation = _build_overall_recommendation(
        signal_info, decision_info, news_info,
        historical_pattern=historical_pattern, market_id=market_id,
    )
    from oneqaz_trading_mcp.verification import VERIFICATION_UNVERIFIED

    return {
        "symbol": symbol,
        "market_id": market_id,
        "signal": signal_info,
        "signal_narrative": signal_narrative,
        "recent_decisions": decision_info,
        "recent_decisions_meta": decisions_meta,
        "recent_news_causality": news_info,
        # [2026-10-07 NEWS-001] additive — 뉴스 블록의 범위·검증 상태를 필드로도 명시
        # [2026-10-13 NEWS-001] 시장 범위(news_events.target_markets)로 거른 최신 5건 — 종목 필터는 아님
        "recent_news_causality_scope": "market_scoped_latest (news_events.target_markets; not filtered to this symbol)",
        "recent_news_causality_verification": VERIFICATION_UNVERIFIED,
        "news_narrative": news_narrative,
        "decision_fingerprint": fingerprint_info,
        "historical_pattern": historical_pattern,
        "historical_pattern_scope": (
            "pooled across all symbols sharing the same role_hint/regime/match_grade/interval — "
            "not this symbol's own track record"
        ),
        "historical_narrative": historical_narrative,
        "overall_recommendation": overall_recommendation,
        "meta": {
            "sources": [
                f"signals/{symbol.lower()}_signal.db::signals (score_trace JSON)",
                "trading_system.db::virtual_trade_decisions",
                "trading_system.db::decision_fingerprint",
                "trading_system.db::explanation_patterns",
                "news/external_context.db::news_causality_analysis",
            ],
            "interpretation": (
                "Multi-layer explanation combining: (1) technical indicators via score_trace, "
                "(2) Thompson + regime scores from decision log, (3) historical pattern stats "
                "(decision_fingerprint × explanation_patterns: how this role/regime/grade combo "
                "ended in past trades), (4) news causality context (market-scoped, not symbol-filtered, UNVERIFIED — not "
                "used in the verdict). `overall_recommendation.basis` lists every input the verdict "
                "used (signal score, latest decision, historical class) and the rules applied; "
                "STRONG_* requires both a strong score and positive pooled history. "
                "Use `overall_recommendation.verdict` for a quick verdict, or read the narrative "
                "fields (signal_narrative / news_narrative / historical_narrative / "
                "recent_decisions_meta.interpretation) to present to end users without further "
                "LLM processing. Empty `historical_pattern` means insufficient samples (n<10) — "
                "early-stage learning. Empty `recent_decisions` is normal — it means the symbol "
                "is being monitored but no trade conditions met."
            ),
        },
    }


# ---------------------------------------------------------------------------
# P2 Tool 13: get_cross_market_correlation
# ---------------------------------------------------------------------------

def _get_cross_market_correlation(
    source_market: Optional[str] = None,
    target_market: Optional[str] = None,
) -> Dict[str, Any]:
    """cross_market_correlation + cross_market_decoupling 테이블에서 크로스 마켓 관계 조회.

    데이터는 news/external_context.db에 있다.
    """
    # [2026-09-22] 열거형 파라미터 검증 — 미지 시장값이 조용히 통과해
    # 필터가 무시되거나 0건이 나왔다 (오타와 실제 공백이 구분 불가).
    source_market, _err = normalize_choice('market', 'source_market', source_market)
    if _err:
        return _err
    target_market, _err = normalize_choice('market', 'target_market', target_market)
    if _err:
        return _err

    news_db = EXTERNAL_CONTEXT_DATA_DIR / "news" / "external_context.db"
    # SQLite 파일은 PG 이관 후 부재할 수 있음. _open_ro 가 PG 라우팅 처리.

    try:
        with _open_ro(news_db) as conn:

            # cross_market_correlation
            correlations: List[Dict[str, Any]] = []
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='cross_market_correlation'"
            )
            if cursor.fetchone():
                query = """
                    SELECT source_market, source_scope, target_market, target_scope,
                           source_regime_change, target_regime_change,
                           lag_hours, count, correlation_score, last_seen_at, updated_at
                    FROM cross_market_correlation
                    WHERE 1=1
                """
                params: List[Any] = []
                if source_market:
                    query += " AND source_market = ?"
                    params.append(_macro_target(source_market))
                if target_market:
                    query += " AND target_market = ?"
                    params.append(_macro_target(target_market))
                query += " ORDER BY ABS(correlation_score) DESC LIMIT 100"

                for row in conn.execute(query, params).fetchall():
                    correlations.append({
                        "source_market": row["source_market"],
                        "source_scope": row["source_scope"],
                        "target_market": row["target_market"],
                        "target_scope": row["target_scope"],
                        "source_regime_change": row["source_regime_change"],
                        "target_regime_change": row["target_regime_change"],
                        "lag_hours": _safe_float(row["lag_hours"]),
                        "count": _safe_int(row["count"]),
                        "correlation_score": round(_safe_float(row["correlation_score"]), 3),
                        "last_seen_at": row["last_seen_at"],
                    })

            # cross_market_decoupling
            decoupling: List[Dict[str, Any]] = []
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='cross_market_decoupling'"
            )
            if cursor.fetchone():
                query = """
                    SELECT source_market, target_market, decoupling_index,
                           correlation_breakdown, regime_divergence, timing_lag_divergence, computed_at
                    FROM cross_market_decoupling
                    WHERE 1=1
                """
                params: List[Any] = []
                if source_market:
                    query += " AND source_market = ?"
                    params.append(_macro_target(source_market))
                if target_market:
                    query += " AND target_market = ?"
                    params.append(_macro_target(target_market))
                query += " ORDER BY computed_at DESC LIMIT 50"

                for row in conn.execute(query, params).fetchall():
                    decoupling.append({
                        "source_market": row["source_market"],
                        "target_market": row["target_market"],
                        "decoupling_index": round(_safe_float(row["decoupling_index"]), 3),
                        "correlation_breakdown": _safe_float(row["correlation_breakdown"]),
                        "regime_divergence": row["regime_divergence"],
                        "timing_lag_divergence": _safe_float(row["timing_lag_divergence"]),
                        "computed_at": row["computed_at"],
                    })

            return {
                "correlations": correlations,
                "decoupling": decoupling,
                "meta": {
                    "correlation_count": len(correlations),
                    "decoupling_count": len(decoupling),
                    "source_market_filter": source_market,
                    "target_market_filter": target_market,
                    "source_tables": [
                        "news/external_context.db::cross_market_correlation",
                        "news/external_context.db::cross_market_decoupling",
                    ],
                    "interpretation": (
                        "Cross-market lead-lag relationships and decoupling events. "
                        "correlation_score > 0 = co-movement, < 0 = inverse. "
                        "decoupling_index tracks divergence (BTC up + stocks down scenarios). "
                        "Key evidence for 'we understand how markets influence each other'."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_cross_market_correlation failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# Tool 등록 함수
# ---------------------------------------------------------------------------

def register_trust_layer_tools(mcp, cache):
    """Trust Layer P0 Tool 등록 (6개)"""
    # Phase 3-5 (2026-05-07): 응답 데이터 기반 _next_actions 추천 함수 import.
    # 각 tool 의 wrap_tool_response 호출에 next_actions_fn 인자로 전달.
    from oneqaz_trading_mcp.tools.next_actions import (
        for_prediction_accuracy,
        for_backtest_tuning,
        for_monthly_trend,
        for_news_lead,
        for_news_causality,
        for_governance,
        for_structure_calibration,
        for_structure_history,
        for_strategy_leaderboard,
        for_active_predictions,
        for_macro_influence_map,
        for_explain_decision,
        for_cross_market_correlation,
    )
    # Phase D-E (2026-05-07): user-facing followup questions.
    from oneqaz_trading_mcp.tools.followup_questions import (
        fq_prediction_accuracy, fq_backtest_tuning, fq_monthly_trend,
        fq_news_lead, fq_news_causality, fq_governance,
        fq_structure_calibration, fq_structure_history,
        fq_strategy_leaderboard, fq_active_predictions,
        fq_macro_influence_map, fq_explain_decision, fq_cross_market_correlation,
    )

    @mcp.tool(title='Prediction Accuracy', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_prediction_accuracy(
        category: Annotated[Optional[str], Field(
            description='Optional macro category filter (bonds, forex, vix, commodities, credit, liquidity, inflation, energy)'
        )] = None,
        target_market: Annotated[Optional[str], Field(
            description='Optional target market filter (coin_market, kr_market, us_market). Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = None,
    ) -> PredictionAccuracyEnvelope:
        """
        Purpose: Per-cell (category, target_market, lag_bucket) hit rates WITH the honest
            baselines (schema 1.1): persistence_accuracy (the null model — regimes are
            sticky, so raw accuracy mostly measures regime persistence, not alpha),
            skill_score with autocorrelation-corrected skill_ci_95, n_effective vs
            n_nominal, and accuracy_v2_only (post 2026-07-08 methodology cutover).
            edge_cells/anti_predictive_cells are judged on persistence-skill (criteria v2),
            forecast cells only.
        Triggers (casual questions too): "how accurate are your predictions?",
            "예측 잘 맞아?", "track record 있어?", "can I trust these forecasts?",
            "적중률 보여줘", "does macro actually predict these markets?".
        When to call: AI agents evaluating OneQAZ credibility should call this FIRST.
        Prerequisites: none.
        Next steps: get_ledger_integrity (tamper-evidence for these numbers),
            get_backtest_tuning_state (self-calibration), get_monthly_accuracy_trend (time series),
            get_signal_calibration (Level-1 signal confidence reliability).
        Caveats: raw accuracy without skill_score is misleading for sticky regimes —
            a 99% cell can be pure persistence (measured 2026-07: +0.05pp over null).
            Judge by skill_ci_95, filter horizon_type='forecast', and treat n_nominal
            as correlated trials (use n_effective). Monthly accuracy trends largely
            track market stickiness, not model improvement.

        """
        # [2026-07-08] wrap(내레이션 포함)까지 스레드로 — 이벤트루프 위 동기 vLLM
        # 대기(최대 35s)가 서버 전체를 정지시키던 문제의 방어 (daily_brief 와 동일 패턴)
        def _build():
            result = _get_prediction_accuracy(
                category=category,
                target_market=target_market,
            )
            return wrap_tool_response(
                result, "trust_layer",
                _ai_summary_pred_accuracy, _user_summary_pred_accuracy,
                next_actions_fn=for_prediction_accuracy,
                followup_questions_fn=fq_prediction_accuracy,
                narrative_context="prediction_accuracy",
            )

        return await asyncio.to_thread(_build)

    @mcp.tool(title='Backtest Tuning State', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_backtest_tuning_state(
        category: Annotated[Optional[str], Field(
            description='Optional category filter'
        )] = None,
        target_market: Annotated[Optional[str], Field(
            description='Optional target market filter. Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = None,
    ) -> BacktestTuningStateEnvelope:
        """
        Purpose: Continuous self-calibration evidence. Each entry shows the auto-tuned
            lag_hours and sensitivity per cell, derived from real backtest outcomes.
            Proves the system adapts to measured reality rather than static heuristics.
        Triggers (casual questions too): "does the system self-correct?", "시스템이 스스로 보정해?",
            "how is it calibrated?", "튜닝 상태 보여줘", "is it adapting to what actually happened?".
        When to call: after get_prediction_accuracy, to show the system updates itself.
        Prerequisites: get_prediction_accuracy recommended for context.
        Next steps: get_monthly_accuracy_trend.
        Caveats: `last_backtest` timestamp indicates tuning freshness.

        """
        result = await asyncio.to_thread(
            _get_backtest_tuning_state,
            category=category,
            target_market=target_market,
        )
        return wrap_tool_response(
            result, "trust_layer",
            _ai_summary_backtest_tuning, _user_summary_backtest_tuning,
            next_actions_fn=for_backtest_tuning,
            followup_questions_fn=fq_backtest_tuning,
        )

    @mcp.tool(title='Monthly Accuracy Trend', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_monthly_accuracy_trend(
        category: Annotated[Optional[str], Field(
            description='Optional category filter'
        )] = None,
        target_market: Annotated[Optional[str], Field(
            description='Optional target market filter. Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = None,
    ) -> MonthlyAccuracyTrendEnvelope:
        """
        Purpose: Monthly accuracy time series per (category, target_market, lag_bucket).
            Use to verify sustained performance and detect recent degradation.
        Triggers (casual questions too): "is accuracy improving?", "적중률이 좋아지고 있어?",
            "monthly performance trend?", "최근에 예측 성능 떨어졌어?", "show accuracy over time".
        When to call: after get_prediction_accuracy and get_backtest_tuning_state — completes the trust chain.
        Prerequisites: get_prediction_accuracy recommended.
        Next steps: none (trust chain complete).
        Caveats: excludes the 'all' month aggregate; empty when backtest_results is unpopulated.

        """
        result = await asyncio.to_thread(
            _get_monthly_accuracy_trend,
            category=category,
            target_market=target_market,
        )
        return wrap_tool_response(
            result, "trust_layer",
            _ai_summary_monthly_trend, _user_summary_monthly_trend,
            next_actions_fn=for_monthly_trend,
            followup_questions_fn=fq_monthly_trend,
        )

    @mcp.tool(title='News Leading-Indicator Performance', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_news_leading_indicator_performance(
        market_id: Annotated[str, Field(
            description='Market identifier (crypto, kr_stock, us_stock, etc.). Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = 'crypto',
        target_market: Annotated[Optional[str], Field(
            description='Alias for market_id (backward compat)', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = None,
        min_sample_count: Annotated[int, Field(
            description='Minimum rows-per-grouping cutoff (default 3). A coverage filter only — it confers no statistical validity.'
        )] = 3,
    ) -> NewsLeadingIndicatorPerformanceEnvelope:
        """
        UNVERIFIED — methodology under audit. Do not cite as evidence of predictive capability.

        Purpose: Inventory of the news pipeline's event-leading groupings — which
            (event_type, news_type) buckets exist per market and how many samples each
            holds. The lead/score metrics themselves are withheld from this response
            while the calculation method is being audited.
        Triggers: "what news event groupings does OneQAZ track?",
            "뉴스 이벤트 분류 어떤 게 있어?", "how many news samples per event type?".
        When to call: when inspecting news pipeline coverage. This tool does NOT answer
            questions about predicting or anticipating news — it carries no such evidence.
        Prerequisites: none.
        Next steps: get_news_causality_breakdown for the label counts.
        Caveats: empty when no news events processed in the recent window. Sample counts
            are coverage figures only; they do not imply statistical validity.

        """
        target_market, _err = normalize_choice("market", "target_market", target_market)
        if _err:
            return _err
        if target_market and not market_id:
            market_id = target_market
        elif target_market:
            market_id = target_market
        result = await asyncio.to_thread(
            _get_news_leading_indicator_performance,
            market_id=market_id,
            min_sample_count=min_sample_count,
        )
        # [2026-09-18 Ticket A] 검증 상태를 최상위에 명시. envelope 이후에 박아야
        # full_data 안이 아니라 응답 첫 층에 온다.
        from oneqaz_trading_mcp.verification import mark_unverified
        return mark_unverified(wrap_tool_response(
            result, "trust_layer",
            _ai_summary_news_lead, _user_summary_news_lead,
            next_actions_fn=for_news_lead,
            followup_questions_fn=fq_news_lead,
        ))

    @mcp.tool(title='News Causality Breakdown', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_news_causality_breakdown(
        market_id: Annotated[str, Field(
            description='Market identifier. Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = 'crypto',
        days: Annotated[int, Field(
            description='Lookback window in days (default 7)'
        )] = 7,
    ) -> NewsCausalityBreakdownEnvelope:
        """
        UNVERIFIED — methodology under audit. Do not cite as evidence of predictive capability.

        Purpose: Counts of news items per internal label the pipeline assigns.
            ANTICIPATED = the item matched a scheduled/calendar event.
            SURPRISE_WITH_PRECURSOR = the item was flagged by the cascade-anomaly heuristic
            (macro -> ETF -> stock). SURPRISE = neither matched.
            These are pipeline labels, not validated classifications; the labelling rule
            and its lead/anticipation metrics are under audit and withheld here.
        Triggers: "how many news items per category this week?",
            "뉴스 라벨 분포 어때?", "how many calendar-matched events?".
        When to call: when inspecting news label coverage. This tool does NOT establish
            that the market did or did not see an event coming.
        Prerequisites: none.
        Next steps: market://{market_id}/external/causality for raw causality rows.
        Caveats: window limited to recent days.

        """
        result = await asyncio.to_thread(
            _get_news_causality_breakdown,
            market_id=market_id,
            days=days,
        )
        # [2026-09-18 Ticket A] 라벨 산출 방법도 같은 감사 대상 — 동일 표식.
        from oneqaz_trading_mcp.verification import mark_unverified
        return mark_unverified(wrap_tool_response(
            result, "trust_layer",
            _ai_summary_causality, _user_summary_causality,
            next_actions_fn=for_news_causality,
            followup_questions_fn=fq_news_causality,
        ))

    @mcp.tool(title='Feature Governance State', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_feature_governance_state(
        market_id: Annotated[Optional[str], Field(
            description='Optional market filter (defaults to coin). Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = None,
        target_market: Annotated[Optional[str], Field(
            description='Alias for market_id (backward compat)', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = None,
        status_filter: Annotated[Optional[str], Field(
            description='Optional status filter (OBSERVATION, CONDITIONAL, ACTIVE, DEPRECATED)', json_schema_extra={'enum': ['OBSERVATION', 'CONDITIONAL', 'ACTIVE', 'DEPRECATED']}
        )] = None,
        limit: Annotated[int, Field(description='Max results (default 50, max 500). Rows are truncated; see `total_available` and `truncated` in the response. `status_summary` and `meta` counts stay whole-set.')] = _GOV_LIMIT_DEFAULT,
    ) -> FeatureGovernanceStateEnvelope:
        """
        Purpose: Current lifecycle state of external features (news, events) under 3-track
            statistical validation. Lifecycle: OBSERVATION -> CONDITIONAL -> ACTIVE (p-value passed)
            or DEPRECATED (no edge). Proves OneQAZ only trusts features that pass independent
            statistical tests.
        Triggers (casual questions too): "do you validate your own inputs?", "피처 검증은 어떻게 해?",
            "which signals passed testing?", "통계 검증 통과한 피처 뭐야?", "how do you avoid junk features?".
        When to call: meta-level trust audit ("do they validate their own inputs?").
        Prerequisites: none.
        Next steps: none (meta evidence).
        Caveats: empty when feature_gate_evaluator has not yet run cycles.
            Rows are paginated — read `total_available` (not `len(features)`) for the
            whole-set size. `status_summary`, `meta.*` and `interpretation` are always
            computed over the whole set, never over the returned page.
            Rows with `frozen=true` (see `meta.frozen_feature_types`: reason, values_as_of) are a
            legacy measurement frozen on 2026-10-14 — their status/counts no longer update;
            do not quote them as current.

        """
        target_market, _err = normalize_choice("market", "target_market", target_market)
        if _err:
            return _err
        if target_market and not market_id:
            market_id = target_market
        result = await asyncio.to_thread(
            _get_feature_governance_state,
            market_id=market_id,
            status_filter=status_filter,
            limit=limit,
        )
        return wrap_tool_response(
            result, "trust_layer",
            _ai_summary_governance, _user_summary_governance,
            next_actions_fn=for_governance,
            followup_questions_fn=fq_governance,
        )

    # =======================================================================
    # P1 Tools (Layer D + E + active forecasts)
    # =======================================================================

    @mcp.tool(title='Structure Calibration (Level 2)', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_structure_calibration(
        market_id: Annotated[Optional[str], Field(
            description='Optional market filter (crypto, kr_stock, us_stock). Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = None,
        group_name: Annotated[Optional[str], Field(
            description='Optional group/sector filter (e.g., layer1, defi, sector, broad_index)'
        )] = None,
        limit: Annotated[int, Field(description='Max results (default 50, max 500). Rows are truncated; see `total_available` and `truncated` in the response. `baseline` and `meta` counts stay whole-set.')] = _CALIB_LIMIT_DEFAULT,
    ) -> StructureCalibrationEnvelope:
        """
        Purpose: Level 2 (ETF / basket / sector granularity — Level 1 is individual symbols)
            prediction calibration. Returns hit_rate_ema per (market, group, interval,
            regime_bucket) with sample counts, **plus the majority-class baseline needed to
            interpret them**. This is measurement, NOT a claim of edge — as of 2026-09-18 the
            measured skill (accuracy minus baseline) is negative in all three markets.
        Triggers (casual questions too): "how good are your sector calls?", "섹터 예측 잘 맞아?",
            "sector rotation accuracy?", "그룹 단위 적중률 보여줘", "can you time sector moves?".
        When to call: when an AI wants to see Layer D evidence (Layer D = sector-structure
            tier of the 5-layer trust pyramid).
        Prerequisites: none.
        Next steps: get_structure_validation_history for the daily trend.
        Caveats: empty until structure-learning cycles complete.
            Rows are paginated — read `total_available` (not `len(calibration)`) for the
            whole-set size. `baseline`, `meta.total_entries` and `meta.total_samples` are
            always whole-set. Rows keep their dimension keys (market_id / interval /
            regime_bucket); they are never hoisted out of the row.

        """
        result = await asyncio.to_thread(
            _get_structure_calibration,
            market_id=market_id,
            group_name=group_name,
            limit=limit,
        )
        return wrap_tool_response(
            result, "trust_layer",
            _ai_summary_struct_calib, _user_summary_struct_calib,
            next_actions_fn=for_structure_calibration,
            followup_questions_fn=fq_structure_calibration,
        )

    @mcp.tool(title='Structure Validation History', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_structure_validation_history(
        market_id: Annotated[Optional[str], Field(
            description='Optional market filter. Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = None,
        days: Annotated[int, Field(
            description='Lookback window in days (default 90)'
        )] = 90,
    ) -> StructureValidationHistoryEnvelope:
        """
        Purpose: Daily validation history of Level 2 structure predictions (Level 2 =
            ETF / basket / sector granularity). Each row shows the hit_rate for a specific day,
            enabling time-series verification of sustained performance.
        Triggers (casual questions too): "sector accuracy over time?", "구조 예측 매일 검증해?",
            "daily hit-rate trend?", "요즘 섹터 예측 성적 어때?", "is the sector edge holding up?".
        When to call: after get_structure_calibration.
        Prerequisites: none.
        Next steps: get_monthly_accuracy_trend for the macro-level comparison.
        Caveats: returns an overall_hit_rate summary across the window.

        """
        result = await asyncio.to_thread(
            _get_structure_validation_history,
            market_id=market_id,
            days=days,
        )
        return wrap_tool_response(
            result, "trust_layer",
            _ai_summary_struct_hist, _user_summary_struct_hist,
            next_actions_fn=for_structure_history,
            followup_questions_fn=fq_structure_history,
        )

    @mcp.tool(title='Strategy Leaderboard', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_strategy_leaderboard(
        market_id: Annotated[str, Field(
            description='Market identifier (crypto, kr_stock, us_stock). Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = 'crypto',
        target_market: Annotated[Optional[str], Field(
            description='Alias for market_id (backward compat)', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = None,
        top_n: Annotated[int, Field(
            description='Top N strategies to return (default 20)'
        )] = 20,
        limit: Annotated[Optional[int], Field(
            description='Alias for top_n (client-compat)'
        )] = None,
        min_trades: Annotated[int, Field(
            description='Minimum trades count for inclusion (default 10)'
        )] = 10,
        include_per_symbol: Annotated[bool, Field(
            description='Include per-symbol PG partition results (default True)'
        )] = True,
    ) -> StrategyLeaderboardEnvelope:
        """
        Purpose: Top RL-learned research strategies — GLOBAL pool + per-symbol partition.
            Layer E evidence (Layer E = strategy-performance tier of the 5-layer trust pyramid).
            The GLOBAL pool may include synthesized win_rate values, so per_symbol_leaderboard
            is the primary measured-edge surface for trust auditing.
        Triggers (casual questions too): "what are the best strategies?", "제일 잘 버는 전략 뭐야?",
            "top strategies?", "전략 순위 보여줘", "which strategy has the best win rate?".
        When to call: final trust-validation step.
        Prerequisites: none.
        Next steps: market://{market_id}/signals/summary for live signals.
        Caveats: `min_trades` filter enforces statistical validity. Strategies are paper-tested,
            not real-money executed.

        """
        # [2026-09-22] 이 wrapper 들은 target_market 를 market_id 로 병합한 뒤
        # 헬퍼에 넘기므로, 병합 **전**에 검증해야 오타가 조용히 삼켜지지 않는다.
        target_market, _err = normalize_choice("market", "target_market", target_market)
        if _err:
            return _err
        if target_market and not market_id:
            market_id = target_market
        if limit is not None:
            top_n = limit
        result = await asyncio.to_thread(
            _get_strategy_leaderboard,
            market_id=market_id,
            top_n=top_n,
            min_trades=min_trades,
            include_per_symbol=include_per_symbol,
        )
        return wrap_tool_response(
            result, "trust_layer",
            _ai_summary_leaderboard, _user_summary_leaderboard,
            next_actions_fn=for_strategy_leaderboard,
            followup_questions_fn=fq_strategy_leaderboard,
        )

    @mcp.tool(title='Active Predictions', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_active_predictions(
        target_market: Annotated[Optional[str], Field(
            description='Optional target market filter (coin_market, kr_market, us_market). Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = None,
        limit: Annotated[int, Field(
            description='Max active predictions to return (default 20)'
        )] = 20,
    ) -> ActivePredictionsEnvelope:
        """
        Purpose: Currently pending predictions (outcome IS NULL). Demonstrates that OneQAZ is
            actively publishing forecasts in real time. Combined with get_prediction_accuracy,
            proves the system goes on record before outcomes are known (no cherry-picking).
        Triggers (casual questions too): "what are you predicting right now?", "지금 어떤 예측 걸려 있어?",
            "current forecasts?", "예측을 미리 기록해 두는 거야?", "anything on the record before it resolves?".
        When to call: to verify ongoing prediction activity.
        Prerequisites: none.
        Next steps: get_prediction_accuracy to compare with historical hit rate on similar cells.
        Caveats: returns most recent first.

        """
        result = await asyncio.to_thread(
            _get_active_predictions,
            target_market=target_market,
            limit=limit,
        )
        return wrap_tool_response(
            result, "trust_layer",
            _ai_summary_active_pred, _user_summary_active_pred,
            next_actions_fn=for_active_predictions,
            followup_questions_fn=fq_active_predictions,
        )

    # =======================================================================
    # P2 Tools (Transparency / Explanation)
    # =======================================================================

    @mcp.tool(title='Macro Influence Map', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_macro_influence_map(
        market_id: Annotated[Optional[str], Field(
            description='Optional target market filter (coin_market, kr_market, us_market). Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = None,
    ) -> MacroInfluenceMapEnvelope:
        """
        Purpose: Expose OneQAZ's pre-defined causal hypothesis map. Each macro category
            (bonds, forex, vix, credit, liquidity, inflation, commodities, energy) is mapped
            to a target market with lag_hours + sensitivity (static prior) and an
            `effective` block with the value the engine actually uses (backtest_tuning
            when the cell passes the tuning gate).
        Triggers (casual questions too): "how do rates affect crypto?", "금리가 코인에 어떻게 영향 줘?",
            "what's your causal model?", "예측 논리가 뭐야?", "which macro drives which market?".
        When to call: when an AI wants to understand WHY we make certain predictions.
        Prerequisites: none.
        Next steps: get_backtest_tuning_state for runtime calibration of these hypotheses.
        Caveats: top-level values are static priors — cite each cell's `effective` block
            for what the engine uses (meta.effective_status reports lookup failures).

        """
        result = await asyncio.to_thread(
            _get_macro_influence_map,
            market_id=market_id,
        )
        return wrap_tool_response(
            result, "trust_layer",
            _ai_summary_macro_map, _user_summary_macro_map,
            next_actions_fn=for_macro_influence_map,
            followup_questions_fn=fq_macro_influence_map,
        )

    @mcp.tool(title='Explain a Decision', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def explain_decision(
        market_id: Annotated[str, Field(
            description='Market identifier (crypto, kr_stock, us_stock; aliases coin/kr/us)', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )],
        symbol: Annotated[str, Field(
            description='Symbol to explain (e.g., btc, eth, 005930)'
        )],
    ) -> ExplainDecisionEnvelope:
        """
        Purpose: Multi-layer explanation for a single symbol's recent research signal.
            Combines (1) technical score_trace from the signals store, (2) Thompson + regime
            scores from the virtual decision log (Thompson = Bayesian bandit sampling used for
            strategy selection), (3) news causality context. Use this when an AI must present
            a structured "why" rather than a raw verdict.
        Triggers (casual questions too): "why is BTC bullish?", "왜 이 종목이 매수야?",
            "explain that signal", "판단 근거 설명해줘", "walk me through the reasoning".
        When to call: when the user asks "why is this signal bullish/bearish?".
        Prerequisites: identify the symbol via get_signals or get_latest_decisions first.
        Next steps: none (this completes the explanation chain).
        Caveats: `symbol` must match the per-symbol signal store filename (lowercase).
            Output is research evidence, NOT a buy or sell recommendation.

        """
        # [2026-07-08] wrap(내레이션 포함)까지 스레드로 (daily_brief 와 동일 패턴)
        def _build():
            result = _explain_decision(
                market_id=market_id,
                symbol=symbol,
            )
            return wrap_tool_response(
                result, "trust_layer",
                _ai_summary_explain, _user_summary_explain,
                next_actions_fn=for_explain_decision,
                followup_questions_fn=fq_explain_decision,
                narrative_context="explain_decision",
            )

        return await asyncio.to_thread(_build)

    @mcp.tool(title='Cross Market Correlation', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_cross_market_correlation(
        source_market: Annotated[Optional[str], Field(
            description='Optional source market filter. Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = None,
        target_market: Annotated[Optional[str], Field(
            description='Optional target market filter. Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )] = None,
    ) -> CrossMarketCorrelationEnvelope:
        """
        Purpose: Cross-market lead-lag relationships and decoupling events. Shows how
            markets influence each other (correlations) and when they diverge (decoupling,
            e.g. BTC up while stocks down).
        Triggers (casual questions too): "do crypto and stocks move together?", "코인이랑 주식이 따로 노나?",
            "any decoupling lately?", "시장끼리 상관관계 어때?", "is BTC tracking the Nasdaq?".
        When to call: when analyzing macro regime changes or divergent signals.
        Prerequisites: none.
        Next steps: get_macro_influence_map for the static causal hypotheses.
        Caveats: correlation data may be empty until enough regime changes accumulate.

        """
        result = await asyncio.to_thread(
            _get_cross_market_correlation,
            source_market=source_market,
            target_market=target_market,
        )
        return wrap_tool_response(
            result, "trust_layer",
            _ai_summary_cross_corr, _user_summary_cross_corr,
            next_actions_fn=for_cross_market_correlation,
            followup_questions_fn=fq_cross_market_correlation,
        )

    logger.info("  [OK] Trust Layer Tools registered (6 P0 + 4 P1 + 3 P2 = 13 tools)")
