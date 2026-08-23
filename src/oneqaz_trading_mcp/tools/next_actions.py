# -*- coding: utf-8 -*-
"""
Next Actions Functions
======================
Trust Layer tool 응답을 보고 "다음에 호출하면 좋은 tool" 을 조건부로 추천.

Phase 3-4 (2026-05-07):
    글로벌 dependency_graph (mcps/server.py 의 TOOL_META) 와는 다른 역할.
    - dependency_graph: 정적 — "이 tool 다음엔 보통 이걸 부른다"
    - next_actions:    동적 — "이 응답을 보니 이걸 부르면 정확히 뭐를 검증할 수 있다"

설계 원칙:
    1. LLM 호출 0. 응답 데이터 패턴만 보고 결정.
    2. 각 함수는 list[dict] 반환. 실패해도 빈 list (절대 raise 하지 않음).
    3. 항목 형식: {intent, tool, args, rationale, priority}
       - intent: "verify_drift" / "investigate_weak_category" 같이 의도 라벨
       - tool: 추천할 다음 tool 이름
       - args: 그 tool 에 넘길 인자 (예측 정확도 카테고리 등)
       - rationale: 왜 이 tool 인지 (한국어 OK, AI 가 사용자한테 인용 가능)
       - priority: "high" / "normal" / "low" — AI 의 호출 순서 결정에 도움
    4. 최대 3개. wrap_with_ai_summary 에서 자동 truncate.
    5. data 가 wrap_tool_response 에 들어가는 _내부_ payload 임에 유의.
       즉 here 에서 받는 data 는 full_data 자체. ("error" 키 없음 — error 는 wrapping 안 됨)
"""

from __future__ import annotations

from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# 공통 헬퍼
# ---------------------------------------------------------------------------

def _action(
    intent: str,
    tool: str,
    rationale: str,
    *,
    args: Dict[str, Any] | None = None,
    priority: str = "normal",
) -> Dict[str, Any]:
    return {
        "intent": intent,
        "tool": tool,
        "args": args or {},
        "rationale": rationale,
        "priority": priority,
    }


def _safe_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


# ---------------------------------------------------------------------------
# get_prediction_accuracy
# ---------------------------------------------------------------------------

def for_prediction_accuracy(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """약한 카테고리 / drift 신호 / 부족 sample 별 다음 단계 추천."""
    actions: List[Dict[str, Any]] = []
    summary = data.get("summary") or {}
    if not isinstance(summary, dict) or not summary:
        # 데이터 없음 → backtest_tuning 으로 자기보정 활성 여부 먼저 확인
        actions.append(_action(
            "verify_self_calibration_active",
            "get_backtest_tuning_state",
            "macro_prediction_accuracy 가 비어있음. 시스템이 학습 사이클을 돌고 있는지 확인 필요.",
            priority="high",
        ))
        return actions

    # 약한 카테고리 + drift 탐색
    weakest = None  # (acc, category, target)
    drift_hit = None  # (category, target, direction)
    for category, targets in summary.items():
        if not isinstance(targets, dict):
            continue
        for target, lags in targets.items():
            if not isinstance(lags, dict):
                continue
            for lag_key, info in lags.items():
                if not isinstance(info, dict):
                    continue
                acc = _safe_float(info.get("accuracy"))
                samples = info.get("samples") or 0
                # 약한 카테고리: accuracy < 0.5 + 표본 30 이상
                if samples >= 30 and acc < 0.5:
                    if weakest is None or acc < weakest[0]:
                        weakest = (acc, category, target)
                # drift 신호 있으면 우선
                if isinstance(info.get("drift"), dict):
                    drift_hit = (category, target, info["drift"].get("direction"))

    if drift_hit is not None:
        cat, tgt, direction = drift_hit
        actions.append(_action(
            "investigate_drift",
            "get_monthly_accuracy_trend",
            f"{cat}×{tgt} 에서 drift 감지({direction}). 월별 시계열로 추세 검증 필요.",
            args={"category": cat, "target_market": tgt},
            priority="high",
        ))

    if weakest is not None:
        acc, cat, tgt = weakest
        actions.append(_action(
            "investigate_weak_category",
            "get_backtest_tuning_state",
            f"{cat}×{tgt} accuracy={acc:.2f} (sub-50%). 자기보정이 lag/sensitivity 를 어떻게 조정했는지 확인.",
            args={"category": cat, "target_market": tgt},
            priority="high",
        ))

    # 일반 흐름 — 큰 문제 없으면 시계열로 sustained edge 검증
    if not actions:
        actions.append(_action(
            "verify_sustained_edge",
            "get_monthly_accuracy_trend",
            "전반적 정확도 양호. 월별 시계열로 최근 degradation 없는지 확인 권장.",
        ))
    return actions


# ---------------------------------------------------------------------------
# get_backtest_tuning_state
# ---------------------------------------------------------------------------

def for_backtest_tuning(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """튜닝 freshness + 적용된 카테고리 수 기반."""
    actions: List[Dict[str, Any]] = []
    entries = data.get("entries") or data.get("tuning_state") or []
    if not entries:
        actions.append(_action(
            "verify_active_predictions",
            "get_active_predictions",
            "튜닝 엔트리가 비어있음. 현재 활성 예측이 있는지 별도 확인.",
            priority="high",
        ))
        return actions

    actions.append(_action(
        "verify_outcome_track_record",
        "get_prediction_accuracy",
        "튜닝된 카테고리들의 실제 적중률 확인. 튜닝이 효과 있었는지 검증.",
    ))
    actions.append(_action(
        "see_open_predictions",
        "get_active_predictions",
        "현재 outcome 대기중 예측 — 미래 검증 가능한 record on-the-line.",
    ))
    return actions


# ---------------------------------------------------------------------------
# get_monthly_accuracy_trend
# ---------------------------------------------------------------------------

def for_monthly_trend(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    series = data.get("series") or data.get("trends") or []
    if not series:
        actions.append(_action(
            "fallback_to_lifetime",
            "get_prediction_accuracy",
            "월별 시계열 비어있음. 누적 정확도라도 확인.",
        ))
        return actions
    actions.append(_action(
        "drill_into_strategies",
        "get_strategy_leaderboard",
        "정확도 시계열 확인 후 실제 거래 결과 (전략 leaderboard) 와 일치 여부 검증.",
    ))
    return actions


# ---------------------------------------------------------------------------
# get_news_leading_indicator_performance
# ---------------------------------------------------------------------------

def for_news_lead(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    indicators = data.get("indicators") or data.get("entries") or []
    if not indicators:
        actions.append(_action(
            "check_news_pipeline",
            "get_news_causality_breakdown",
            "leading_indicator 비어있음. 인과 분석 자체가 도는지 확인.",
            priority="high",
        ))
        return actions

    leads = [i.get("avg_lead_time_minutes") for i in indicators if isinstance(i, dict)]
    leads_pos = [x for x in leads if x is not None and x > 0]

    if leads_pos:
        actions.append(_action(
            "verify_anticipation_classification",
            "get_news_causality_breakdown",
            f"{len(leads_pos)}개 event type 에서 양의 lead time. anticipated vs surprise 분류로 신호 결 검증.",
            priority="high",
        ))
    actions.append(_action(
        "verify_governance",
        "get_feature_governance_state",
        "선행 감지가 통계 검증 통과한 feature 인지 확인 (3-track p-value).",
    ))
    return actions


# ---------------------------------------------------------------------------
# get_news_causality_breakdown
# ---------------------------------------------------------------------------

def for_news_causality(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    breakdown = data.get("breakdown") or data.get("classification") or {}
    if not breakdown:
        return [_action(
            "fallback_lead_indicator",
            "get_news_leading_indicator_performance",
            "causality breakdown 비어있음. 시스템이 lead time 자체는 측정하는지 확인.",
        )]

    # anticipated 비율이 surprise 보다 높으면 OneQAZ 의 강점 → governance 로 검증
    actions.append(_action(
        "verify_with_governance",
        "get_feature_governance_state",
        "분류 결과가 어떤 feature 에 의존하는지 governance 에서 cross-check.",
    ))
    actions.append(_action(
        "see_macro_alignment",
        "get_macro_influence_map",
        "anticipated 분류가 정적 macro influence 가설과 일관되는지 비교.",
    ))
    return actions


# ---------------------------------------------------------------------------
# get_feature_governance_state
# ---------------------------------------------------------------------------

def for_governance(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    features = data.get("features") or data.get("entries") or []
    if not features:
        return [_action(
            "fallback_to_active_predictions",
            "get_active_predictions",
            "governance 데이터 비어있음. 현재 활성 예측은 어떤 feature 로 도출되는지 확인.",
        )]

    # ACTIVE 상태 비율 보고 → 비율 낮으면 데이터 누적 부족 신호
    active_count = sum(1 for f in features if isinstance(f, dict) and (f.get("status") == "ACTIVE"))
    total = len(features)
    if total > 0 and active_count / total < 0.3:
        actions.append(_action(
            "investigate_low_active_rate",
            "get_prediction_accuracy",
            f"ACTIVE feature 비율 {active_count}/{total} 낮음. 데이터 누적 부족인지 정확도로 확인.",
            priority="high",
        ))

    actions.append(_action(
        "verify_strategy_leaderboard",
        "get_strategy_leaderboard",
        "governance 통과한 feature 가 실제 거래 전략에 반영되는지 leaderboard 로 확인.",
    ))
    return actions


# ---------------------------------------------------------------------------
# get_structure_calibration
# ---------------------------------------------------------------------------

def for_structure_calibration(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    rows = data.get("calibration") or data.get("entries") or []
    if not rows:
        return [_action(
            "fallback_macro",
            "get_macro_influence_map",
            "Level 2 calibration 비어있음. Level 1 (macro) 로 fallback.",
        )]
    actions.append(_action(
        "verify_history",
        "get_structure_validation_history",
        "calibration snapshot 외에 일별 추세 확인.",
    ))
    actions.append(_action(
        "compare_to_macro_layer",
        "get_prediction_accuracy",
        "Level 2 적중률을 Level 1 (macro) 적중률과 비교해서 layer 별 edge 확인.",
    ))
    return actions


# ---------------------------------------------------------------------------
# get_structure_validation_history
# ---------------------------------------------------------------------------

def for_structure_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        _action(
            "compare_macro_trend",
            "get_monthly_accuracy_trend",
            "Level 2 일별 추세를 Level 1 월별 추세와 비교.",
        ),
        _action(
            "verify_strategies",
            "get_strategy_leaderboard",
            "구조 예측이 실제 거래 전략에 반영되어 PnL 로 이어지는지 확인.",
        ),
    ]


# ---------------------------------------------------------------------------
# get_strategy_leaderboard
# ---------------------------------------------------------------------------

def for_strategy_leaderboard(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    meta = data.get("meta", {}) or {}
    measured = meta.get("measured_entries", 0) or 0
    synth = meta.get("synthesized_entries", 0) or 0

    if synth > measured:
        actions.append(_action(
            "verify_with_lifetime_accuracy",
            "get_prediction_accuracy",
            f"GLOBAL pool 에 synthesized({synth}) > measured({measured}). 실제 macro accuracy 로 trust 검증 필요.",
            priority="high",
        ))

    per_sym = data.get("per_symbol_leaderboard") or []
    if per_sym:
        actions.append(_action(
            "verify_with_active_predictions",
            "get_active_predictions",
            "per-symbol 검증된 전략들이 현재 어떤 예측을 내고 있는지 확인.",
        ))
    actions.append(_action(
        "see_news_lead",
        "get_news_leading_indicator_performance",
        "전략 우위가 뉴스 선행 감지에서 오는지 확인 (Layer A evidence).",
    ))
    return actions


# ---------------------------------------------------------------------------
# get_active_predictions
# ---------------------------------------------------------------------------

def for_active_predictions(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    preds = data.get("predictions") or data.get("entries") or []
    if not preds:
        return [_action(
            "fallback_history",
            "get_prediction_accuracy",
            "활성 예측 없음. 과거 누적 적중률로 trust 검증.",
        )]

    actions.append(_action(
        "verify_track_record",
        "get_prediction_accuracy",
        f"현재 {len(preds)} 활성 예측. 같은 카테고리 과거 적중률로 trust 검증.",
        priority="high",
    ))
    actions.append(_action(
        "see_macro_basis",
        "get_macro_influence_map",
        "활성 예측이 어떤 macro 인과 가설에 기반하는지 확인.",
    ))
    return actions


# ---------------------------------------------------------------------------
# get_macro_influence_map
# ---------------------------------------------------------------------------

def for_macro_influence_map(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        _action(
            "verify_calibration",
            "get_backtest_tuning_state",
            "정적 가설맵의 lag/sensitivity 가 런타임에 어떻게 자기보정됐는지 확인.",
            priority="high",
        ),
        _action(
            "verify_with_outcomes",
            "get_prediction_accuracy",
            "이 가설들이 실제로 적중하는지 카테고리별 정확도로 검증.",
        ),
    ]


# ---------------------------------------------------------------------------
# explain_decision
# ---------------------------------------------------------------------------

def for_explain_decision(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    market = data.get("market_id") or data.get("market") or "crypto"
    symbol = data.get("symbol") or "BTC"

    actions.append(_action(
        "see_role_breakdown",
        "get_role_analysis",
        "이 심볼이 어떤 strategy role 에 매칭되는지 확인.",
        args={"market_id": market, "coin": symbol},
    ))
    actions.append(_action(
        "see_recent_signal",
        "get_signal_detail",
        "최근 시그널의 score_trace 와 일치하는지 확인.",
        args={"market_id": market, "coin": symbol},
    ))
    return actions


# ---------------------------------------------------------------------------
# get_cross_market_correlation
# ---------------------------------------------------------------------------

def for_cross_market_correlation(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        _action(
            "see_static_macro_map",
            "get_macro_influence_map",
            "크로스 시장 동조/디커플링이 정적 macro 가설과 일치하는지 비교.",
        ),
        _action(
            "verify_decoupling_history",
            "get_structure_validation_history",
            "디커플링 이벤트가 Level 2 구조 예측과 어떻게 연결되는지 확인.",
        ),
    ]


# ===========================================================================
# Phase B (2026-05-07): 18개 non-trust tool next_actions
# Strategy 2 — depth 확장. trust_layer 13개 → 31개 전체로 확대.
# ===========================================================================


# ---------------------------------------------------------------------------
# positions.py (5)
# ---------------------------------------------------------------------------

def for_positions(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """get_positions: 손실 비율 / 전략 다각화 / 개별 deep-dive."""
    actions: List[Dict[str, Any]] = []
    positions = data.get("positions") or data.get("entries") or []
    if not positions:
        return [_action(
            "fallback_trade_history",
            "get_trade_history",
            "현재 보유 포지션 없음. 최근 거래 내역으로 활동 패턴 확인.",
        )]

    market = data.get("market_id") or "crypto"
    losing = [p for p in positions if isinstance(p, dict) and _safe_float(p.get("roi", 0)) < 0]
    if len(positions) >= 5:
        actions.append(_action(
            "check_diversification",
            "get_strategy_distribution",
            f"{len(positions)}개 포지션 보유. 전략 다각화 여부 확인.",
            args={"market_id": market},
        ))
    if losing and len(losing) / len(positions) > 0.3:
        actions.append(_action(
            "investigate_losses",
            "get_losing_positions",
            f"손실 포지션 비율 {len(losing)}/{len(positions)} ({len(losing)/len(positions)*100:.0f}%) 높음. 손실 패턴 분석 필요.",
            args={"market_id": market},
            priority="high",
        ))
    actions.append(_action(
        "see_recent_decisions",
        "get_latest_decisions",
        "현재 포지션의 시그널 기반 매매 결정 이력 확인.",
        args={"market_id": market},
    ))
    return actions


def for_position_detail(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """get_position_detail: 시그널/role/macro 컨텍스트로 deep-dive."""
    market = data.get("market_id") or "crypto"
    symbol = data.get("symbol") or data.get("coin")
    if not symbol:
        return []
    return [
        _action(
            "see_role_match",
            "get_role_analysis",
            f"{symbol} 의 strategy role 매칭 점검.",
            args={"market_id": market, "coin": symbol},
            priority="high",
        ),
        _action(
            "see_signal_history",
            "get_signal_detail",
            f"{symbol} 의 최근 시그널 + 이력 확인.",
            args={"market_id": market, "coin": symbol},
        ),
        _action(
            "explain_decision_chain",
            "explain_decision",
            f"{symbol} 의 매매 결정 다층 설명 (technical + Thompson + news).",
            args={"market_id": market, "symbol": symbol},
        ),
    ]


def for_profitable_positions(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    market = data.get("market_id") or "crypto"
    positions = data.get("positions") or []
    if not positions:
        return [_action(
            "fallback_winning_trades",
            "get_winning_trades",
            "수익 포지션 없음. 과거 수익 거래 패턴 확인.",
            args={"market_id": market},
        )]
    return [
        _action(
            "verify_strategy_pattern",
            "get_strategy_distribution",
            f"수익 포지션 {len(positions)}개의 전략 분포 확인.",
            args={"market_id": market},
        ),
        _action(
            "see_winning_trades",
            "get_winning_trades",
            "과거 수익 거래와 패턴 비교.",
            args={"market_id": market},
        ),
    ]


def for_losing_positions(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    market = data.get("market_id") or "crypto"
    positions = data.get("positions") or []
    if not positions:
        return [_action(
            "good_state",
            "get_strategy_leaderboard",
            "손실 포지션 없음. 어떤 전략이 작동중인지 확인.",
            args={"market_id": market},
        )]
    actions = [_action(
        "analyze_losing_pattern",
        "analyze_trades",
        f"손실 포지션 {len(positions)}개의 공통 패턴 분석.",
        args={"market_id": market, "days": 30},
        priority="high",
    )]
    actions.append(_action(
        "see_losing_trades",
        "get_losing_trades",
        "포지션이 손실 거래로 이어지는 패턴 검증.",
        args={"market_id": market},
    ))
    return actions


def for_strategy_distribution(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    dist = data.get("distribution") or data.get("strategies") or {}
    if not dist:
        return [_action(
            "fallback_leaderboard",
            "get_strategy_leaderboard",
            "전략 분포 비어있음. 학습된 전략 leaderboard 로 fallback.",
        )]
    if isinstance(dist, dict) and len(dist) <= 2:
        actions.append(_action(
            "concentration_warning",
            "get_strategy_leaderboard",
            f"전략 {len(dist)}개로만 집중. 다른 학습된 전략 확인 권장.",
            priority="high",
        ))
    actions.append(_action(
        "see_role_breakdown",
        "get_role_analysis",
        "전략별 role 정렬도 확인.",
    ))
    return actions


# ---------------------------------------------------------------------------
# signals.py (3)
# ---------------------------------------------------------------------------

def for_signals(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """get_signals: 강한 시그널 발견 → deep-dive 추천."""
    actions: List[Dict[str, Any]] = []
    signals = data.get("signals") or data.get("entries") or []
    if not signals:
        return [_action(
            "verify_freshness",
            "get_latest_decisions",
            "시그널 결과 비어있음. 시그널 파이프라인이 도는지 결정 이력으로 확인.",
            priority="high",
        )]

    market = data.get("market_id") or "crypto"
    # confidence 가 높은 시그널 식별
    strong = [s for s in signals if isinstance(s, dict) and _safe_float(s.get("confidence", 0)) > 0.7]
    if strong:
        top = strong[0]
        sym = top.get("symbol") or top.get("coin")
        if sym:
            actions.append(_action(
                "deep_dive_strong_signal",
                "get_signal_detail",
                f"{sym} confidence={_safe_float(top.get('confidence', 0)):.2f} 강한 시그널. 상세 + 이력 확인.",
                args={"market_id": market, "coin": sym},
                priority="high",
            ))
            actions.append(_action(
                "explain_strong_signal",
                "explain_decision",
                f"{sym} 시그널의 다층 설명 (technical + Thompson + news).",
                args={"market_id": market, "symbol": sym},
            ))
    actions.append(_action(
        "see_role_distribution",
        "get_role_analysis",
        "전체 시그널의 role 별 분포 확인.",
        args={"market_id": market},
    ))
    return actions


def for_signal_detail(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """get_signal_detail: confidence 낮으면 explain, 높으면 position 확인."""
    actions: List[Dict[str, Any]] = []
    market = data.get("market_id") or data.get("market") or "crypto"
    symbol = data.get("symbol") or data.get("coin")
    if not symbol:
        return []

    latest = data.get("latest_signal") or data.get("signal") or {}
    confidence = _safe_float(latest.get("confidence", 0)) if isinstance(latest, dict) else 0

    if confidence < 0.5 and confidence > 0:
        actions.append(_action(
            "low_confidence_explain",
            "explain_decision",
            f"{symbol} confidence={confidence:.2f} 낮음. score_trace 로 왜 약한지 확인.",
            args={"market_id": market, "symbol": symbol},
            priority="high",
        ))
    else:
        actions.append(_action(
            "see_position_state",
            "get_position_detail",
            f"{symbol} 현재 포지션 상태 확인.",
            args={"market_id": market, "coin": symbol},
        ))

    actions.append(_action(
        "see_role_match",
        "get_role_analysis",
        f"{symbol} 의 strategy role 매칭 확인.",
        args={"market_id": market, "coin": symbol},
    ))
    return actions


def for_role_analysis(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    market = data.get("market_id") or data.get("market") or "crypto"
    symbol = data.get("symbol") or data.get("coin")
    actions = [_action(
        "see_unified_context",
        "get_position_detail",
        f"{symbol} 포지션 + 거래 이력 + 결정 결합 확인." if symbol else "포지션 상태 확인.",
        args={"market_id": market, "coin": symbol} if symbol else {"market_id": market},
    )]
    actions.append(_action(
        "explain_role_decision",
        "explain_decision",
        "role 매칭에서 매매 결정으로 이어지는 다층 설명.",
        args={"market_id": market, "symbol": symbol} if symbol else {"market_id": market},
    ))
    return actions


# ---------------------------------------------------------------------------
# decisions.py (2)
# ---------------------------------------------------------------------------

def for_latest_decisions(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """get_latest_decisions: 결정 → 시그널 / trade 결과 확인."""
    actions: List[Dict[str, Any]] = []
    decisions = data.get("decisions") or data.get("entries") or []
    market = data.get("market_id") or "crypto"
    if not decisions:
        return [_action(
            "fallback_signals",
            "get_signals",
            "결정 이력 비어있음. 시그널 파이프라인이 신호를 만드는지 직접 확인.",
            args={"market_id": market},
            priority="high",
        )]
    actions.append(_action(
        "verify_outcomes",
        "get_trade_history",
        f"최근 결정 {len(decisions)}개가 실제 거래로 이어졌는지 확인.",
        args={"market_id": market},
        priority="high",
    ))
    actions.append(_action(
        "see_strategy_distribution",
        "get_strategy_distribution",
        "현재 결정이 어떤 전략 분포에서 오는지 확인.",
        args={"market_id": market},
    ))
    return actions


def for_llm_trading_decisions(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """get_llm_trading_decisions (Track A): LLM 판단 → 실제 신호와 비교."""
    market = data.get("market_id") or "crypto"
    return [
        _action(
            "compare_with_signals",
            "get_latest_decisions",
            "Track A (LLM) 판단과 Track B (signal-based) 결정 비교.",
            args={"market_id": market},
            priority="high",
        ),
        _action(
            "verify_outcomes",
            "get_trade_history",
            "LLM 판단이 실제 trade 로 어떻게 이어졌는지 확인.",
            args={"market_id": market},
        ),
    ]


# ---------------------------------------------------------------------------
# trade_history.py (4)
# ---------------------------------------------------------------------------

def for_trade_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    trades = data.get("trades") or data.get("entries") or []
    market = data.get("market_id") or "crypto"
    if not trades:
        return [_action(
            "fallback_positions",
            "get_positions",
            "거래 이력 비어있음. 현재 보유 포지션이라도 확인.",
            args={"market_id": market},
        )]

    actions.append(_action(
        "analyze_pattern",
        "analyze_trades",
        f"{len(trades)}개 거래의 패턴/통계 분석.",
        args={"market_id": market, "days": 7},
        priority="high",
    ))
    # 손실 거래 비율 검출
    losing = [t for t in trades if isinstance(t, dict) and _safe_float(t.get("pnl", t.get("roi", 0))) < 0]
    if losing and len(losing) / len(trades) > 0.5:
        actions.append(_action(
            "investigate_losses",
            "get_losing_trades",
            f"손실 비율 {len(losing)}/{len(trades)} ({len(losing)/len(trades)*100:.0f}%) 높음.",
            args={"market_id": market},
            priority="high",
        ))
    return actions


def for_analyze_trades(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    market = data.get("market_id") or "crypto"
    return [
        _action(
            "verify_strategies",
            "get_strategy_leaderboard",
            "거래 분석 결과를 학습된 전략 leaderboard 와 비교.",
            args={"market_id": market},
        ),
        _action(
            "see_predictions",
            "get_active_predictions",
            "현재 활성 예측이 있다면 어떻게 거래로 이어질지 확인.",
        ),
    ]


def for_winning_trades(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    market = data.get("market_id") or "crypto"
    trades = data.get("trades") or []
    if not trades:
        return [_action(
            "fallback_history",
            "get_trade_history",
            "수익 거래 없음. 전체 이력으로 fallback.",
            args={"market_id": market},
        )]
    return [
        _action(
            "find_pattern",
            "analyze_trades",
            f"수익 {len(trades)} 거래의 공통 패턴 분석.",
            args={"market_id": market, "days": 30},
        ),
        _action(
            "verify_strategy",
            "get_strategy_distribution",
            "수익 거래의 전략 분포 확인 — 어떤 전략이 PnL 기여.",
            args={"market_id": market},
        ),
    ]


def for_losing_trades(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    market = data.get("market_id") or "crypto"
    trades = data.get("trades") or []
    if not trades:
        return [_action(
            "good_state",
            "get_strategy_leaderboard",
            "손실 거래 없음. 작동중인 전략 확인.",
            args={"market_id": market},
        )]
    return [
        _action(
            "find_failure_mode",
            "analyze_trades",
            f"손실 {len(trades)} 거래의 패턴 분석 (어떤 시점/전략이 약한지).",
            args={"market_id": market, "days": 30},
            priority="high",
        ),
        _action(
            "see_governance",
            "get_feature_governance_state",
            "손실 패턴이 deprecated feature 와 연관되는지 확인.",
        ),
    ]


# ---------------------------------------------------------------------------
# layer_correlations.py (4)
# ---------------------------------------------------------------------------

def for_sector_correlations(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    market = data.get("market_id") or "us_stock"
    pairs = data.get("top_pairs") or []
    actions: List[Dict[str, Any]] = []
    if pairs:
        top = pairs[0] if isinstance(pairs, list) else None
        if top and isinstance(top, dict):
            sym = top.get("a")
            if sym:
                actions.append(_action(
                    "deep_dive_top_pair",
                    "get_symbol_peer_links_tool",
                    f"가장 강한 pair 의 lead-lag 관계 확인 ({top.get('a')}↔{top.get('b')}).",
                    args={"market_id": market, "symbol": sym},
                    priority="high",
                ))
    actions.append(_action(
        "see_macro_layer",
        "get_macro_causality_graph_tool",
        "섹터 상관 → 매크로 인과로 layer 상승.",
    ))
    return actions


def for_macro_causality_graph(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        _action(
            "see_market_impact",
            "get_macro_influence_map",
            "매크로 카테고리 간 인과 → 시장 영향 가설로 연결.",
            priority="high",
        ),
        _action(
            "verify_outcomes",
            "get_prediction_accuracy",
            "이 인과 그래프 기반 예측의 실제 적중률 확인.",
        ),
    ]


def for_symbol_peer_links(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    market = data.get("market_id") or "us_stock"
    symbol = data.get("symbol")
    actions: List[Dict[str, Any]] = []
    if symbol:
        actions.append(_action(
            "see_symbol_signal",
            "get_signal_detail",
            f"{symbol} 의 시그널과 peer 의 시그널 비교.",
            args={"market_id": market, "coin": symbol},
            priority="high",
        ))
    actions.append(_action(
        "see_sector_context",
        "get_sector_correlations_tool",
        "이 종목이 속한 섹터의 클러스터 구조 확인.",
        args={"market_id": market},
    ))
    return actions


def for_feature_governance_status_tool(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """layer_correlations 의 governance status (전체 분포 + transitions)."""
    return [
        _action(
            "see_full_governance",
            "get_feature_governance_state",
            "전체 feature 의 lifecycle 상세 보기.",
            priority="high",
        ),
        _action(
            "verify_outcomes",
            "get_prediction_accuracy",
            "ACTIVE feature 들이 실제 정확도에 기여하는지 확인.",
        ),
    ]
