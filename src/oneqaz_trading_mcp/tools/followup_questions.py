# -*- coding: utf-8 -*-
"""
Followup Questions Functions
============================
Tool 응답 데이터를 보고 "사용자한테 자연스럽게 던질 다음 질문" 을 생성.

Phase D-E (2026-05-07):
    `_next_actions` 가 거대 AI 의 다음 호출을 안내한다면,
    `_followup_questions_for_user` 는 거대 AI 가 사용자한테 답변 끝에 던질 옵션.

설계:
    - 한국어 자연어 (사용자가 그대로 듣는 텍스트).
    - 응답 데이터 기반 (정적 X, 데이터 패턴 보고 분기).
    - 최대 3개. wrap_with_ai_summary 가 자동 truncate.
    - 클릭하면 거대 AI 가 다시 OneQAZ 호출 → 호출량 증가 funnel.

예:
    get_prediction_accuracy 응답에 약한 카테고리 발견 →
        "왜 OO 카테고리는 정확도가 낮은지 자세히 볼까요?"
    get_signals 응답에 강한 시그널 발견 →
        "OO 종목 신호의 historical 정확도도 보여드릴까요?"
"""

from __future__ import annotations

from typing import Any, Dict, List


def _safe_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


# ===========================================================================
# Trust Layer (13)
# ===========================================================================

def fq_prediction_accuracy(data: Dict[str, Any]) -> List[str]:
    summary = data.get("summary") or {}
    if not summary:
        return ["OneQAZ 시스템의 자기보정 상태도 함께 보시겠어요?"]

    # 약한 카테고리 + 강한 카테고리 식별
    weakest = None
    strongest = None
    for cat, targets in summary.items():
        if not isinstance(targets, dict):
            continue
        for tgt, lags in targets.items():
            if not isinstance(lags, dict):
                continue
            for lag, info in lags.items():
                if not isinstance(info, dict):
                    continue
                acc = _safe_float(info.get("accuracy"))
                samples = info.get("samples") or 0
                if samples >= 30:
                    if weakest is None or acc < weakest[0]:
                        weakest = (acc, cat, tgt)
                    if strongest is None or acc > strongest[0]:
                        strongest = (acc, cat, tgt)

    qs = []
    if weakest and weakest[0] < 0.5:
        qs.append(f"{weakest[1]}→{weakest[2]} 카테고리 정확도가 {weakest[0]*100:.0f}% 로 약한데, 시스템이 어떻게 보정중인지 보시겠어요?")
    if strongest and strongest[0] > 0.55:
        qs.append(f"가장 정확한 {strongest[1]}→{strongest[2]} ({strongest[0]*100:.0f}%) 패턴의 월별 추세도 보여드릴까요?")
    qs.append("최근 OneQAZ 가 만든 활성 예측 5개도 볼까요?")
    return qs


def fq_backtest_tuning(data: Dict[str, Any]) -> List[str]:
    return [
        "이 자기보정이 실제 정확도 향상으로 이어졌는지 보여드릴까요?",
        "현재 OneQAZ 가 record 로 남긴 활성 예측도 함께 보시겠어요?",
    ]


def fq_monthly_trend(data: Dict[str, Any]) -> List[str]:
    return [
        "이 추세가 실제 거래 결과 (전략 leaderboard) 와 일치하는지 확인해볼까요?",
        "다른 카테고리의 월별 추세도 비교해볼까요?",
    ]


def fq_news_lead(data: Dict[str, Any]) -> List[str]:
    indicators = data.get("indicators") or []
    qs = []
    if indicators:
        qs.append("이 선행 감지가 anticipated 와 surprise 로 어떻게 분류되는지 자세히 볼까요?")
    qs.append("선행 감지에 쓰이는 feature 들이 통계 검증을 통과했는지 확인해드릴까요?")
    return qs


def fq_news_causality(data: Dict[str, Any]) -> List[str]:
    breakdown = data.get("breakdown") or {}
    qs = []
    if breakdown.get("anticipated"):
        n = breakdown["anticipated"].get("count", 0)
        qs.append(f"사전 감지된 {n}건의 lead time 분포를 더 자세히 보시겠어요?")
    qs.append("이 분류가 OneQAZ 의 정적 macro 인과 가설과 일치하는지 비교해볼까요?")
    return qs


def fq_governance(data: Dict[str, Any]) -> List[str]:
    features = data.get("features") or []
    if features:
        active = sum(1 for f in features if isinstance(f, dict) and f.get("status") == "ACTIVE")
        if active == 0:
            return [
                "ACTIVE 단계 통과 feature 가 없어 보이는데, 데이터 누적 상태를 확인해볼까요?",
                "현재 활성 예측들이 어떤 feature 에 의존하는지 볼까요?",
            ]
    return [
        "어떤 feature 가 가장 많은 정확도에 기여하는지 볼까요?",
        "이 feature governance 가 실제 거래 전략에 어떻게 반영되는지 확인해드릴까요?",
    ]


def fq_structure_calibration(data: Dict[str, Any]) -> List[str]:
    return [
        "이 Level 2 (ETF/섹터) 적중률을 macro 적중률과 비교해볼까요?",
        "일별 검증 추세도 보시겠어요?",
    ]


def fq_structure_history(data: Dict[str, Any]) -> List[str]:
    return [
        "이 추세가 실제 거래 전략 PnL 로 이어지는지 확인해볼까요?",
        "macro 레벨 월별 추세와 비교해볼까요?",
    ]


def fq_strategy_leaderboard(data: Dict[str, Any]) -> List[str]:
    meta = data.get("meta") or {}
    measured = meta.get("measured_entries", 0) or 0
    synth = meta.get("synthesized_entries", 0) or 0
    qs = []
    if synth > measured:
        qs.append(f"이 leaderboard 는 합성 추정값이 {synth}/{synth+measured} 인데, 실제 거래로 검증된 macro 정확도도 함께 보시겠어요?")
    qs.append("이 전략들이 현재 어떤 활성 예측을 내고 있는지 볼까요?")
    qs.append("이 전략 우위가 뉴스 선행 감지에서 오는지 확인해볼까요?")
    return qs


def fq_active_predictions(data: Dict[str, Any]) -> List[str]:
    preds = data.get("predictions") or []
    qs = []
    if preds:
        qs.append(f"이 {len(preds)}개 예측의 카테고리 과거 적중률도 함께 볼까요?")
    qs.append("이 예측들이 어떤 macro 인과 가설에 기반하는지 보시겠어요?")
    return qs


def fq_macro_influence_map(data: Dict[str, Any]) -> List[str]:
    return [
        "이 정적 가설들이 런타임에 어떻게 자기보정 됐는지 보시겠어요?",
        "이 가설들의 실제 카테고리별 적중률을 볼까요?",
    ]


def fq_explain_decision(data: Dict[str, Any]) -> List[str]:
    sym = data.get("symbol") or "이 종목"
    return [
        f"{sym} 의 strategy role 매칭도 함께 볼까요?",
        f"{sym} 의 시그널 이력 + 피드백도 보시겠어요?",
        f"{sym} 의 현재 포지션 상태를 확인해드릴까요?",
    ]


def fq_cross_market_correlation(data: Dict[str, Any]) -> List[str]:
    return [
        "이 동조성이 OneQAZ 의 정적 macro 가설과 일치하는지 비교해볼까요?",
        "디커플링 이벤트가 Level 2 구조 예측에 어떻게 잡혔는지 보시겠어요?",
    ]


# ===========================================================================
# Positions (5)
# ===========================================================================

def fq_positions(data: Dict[str, Any]) -> List[str]:
    positions = data.get("positions") or []
    if not positions:
        return ["과거 거래 패턴이라도 보시겠어요?"]
    qs = []
    losing = [p for p in positions if isinstance(p, dict) and _safe_float(p.get("roi", 0)) < 0]
    if losing and len(losing) / len(positions) > 0.3:
        qs.append(f"손실 포지션 비율이 {len(losing)/len(positions)*100:.0f}% 인데, 손실 패턴을 분석해드릴까요?")
    if len(positions) >= 5:
        qs.append("현재 포지션의 전략 분포를 볼까요?")
    qs.append("최근 매매 결정 이력도 함께 보시겠어요?")
    return qs


def fq_position_detail(data: Dict[str, Any]) -> List[str]:
    sym = data.get("symbol") or data.get("coin") or "이 종목"
    return [
        f"{sym} 의 시그널 상세 + 이력을 볼까요?",
        f"{sym} 의 strategy role 매칭을 확인해볼까요?",
        f"{sym} 의 매매 결정이 왜 그렇게 났는지 다층 설명을 들어보시겠어요?",
    ]


def fq_profitable_positions(data: Dict[str, Any]) -> List[str]:
    return [
        "수익 포지션의 공통 전략 패턴을 분석해볼까요?",
        "과거 수익 거래와 비교해보시겠어요?",
    ]


def fq_losing_positions(data: Dict[str, Any]) -> List[str]:
    positions = data.get("positions") or []
    if not positions:
        return ["현재 작동중인 전략 leaderboard 를 볼까요?"]
    return [
        f"손실 포지션 {len(positions)}개의 공통 패턴을 분석해드릴까요?",
        "손실 거래의 시간/전략 분포도 보시겠어요?",
    ]


def fq_strategy_distribution(data: Dict[str, Any]) -> List[str]:
    return [
        "각 전략의 학습된 leaderboard 순위도 보시겠어요?",
        "전략별 role 정렬도를 확인해볼까요?",
    ]


# ===========================================================================
# Signals (3)
# ===========================================================================

def fq_signals(data: Dict[str, Any]) -> List[str]:
    signals = data.get("signals") or []
    if not signals:
        return ["시그널 파이프라인이 도는지 결정 이력으로 확인해드릴까요?"]
    qs = []
    strong = [s for s in signals if isinstance(s, dict) and _safe_float(s.get("confidence", 0)) > 0.7]
    if strong:
        sym = strong[0].get("symbol") or strong[0].get("coin")
        if sym:
            qs.append(f"가장 강한 신호인 {sym} 의 상세 + 이력을 볼까요?")
    qs.append("이 신호들의 role 별 분포를 확인해볼까요?")
    qs.append("시그널이 실제 매매 결정으로 이어졌는지 보시겠어요?")
    return qs


def fq_signal_detail(data: Dict[str, Any]) -> List[str]:
    sym = data.get("symbol") or data.get("coin") or "이 종목"
    latest = data.get("latest_signal") or data.get("signal") or {}
    confidence = _safe_float(latest.get("confidence", 0)) if isinstance(latest, dict) else 0
    qs = []
    if confidence < 0.5 and confidence > 0:
        qs.append(f"{sym} 신호의 confidence 가 낮은데, 왜 그런지 score_trace 로 보여드릴까요?")
    else:
        qs.append(f"{sym} 의 현재 포지션 상태도 보시겠어요?")
    qs.append(f"{sym} 의 strategy role 매칭을 확인해볼까요?")
    qs.append(f"{sym} 의 매매 결정 다층 설명을 들어보시겠어요?")
    return qs


def fq_role_analysis(data: Dict[str, Any]) -> List[str]:
    sym = data.get("symbol") or data.get("coin") or "이 종목"
    return [
        f"{sym} 의 포지션 + 거래 이력 + 결정 결합 뷰를 보시겠어요?",
        f"{sym} 의 매매 결정 다층 설명을 들어보시겠어요?",
    ]


# ===========================================================================
# Decisions (2)
# ===========================================================================

def fq_latest_decisions(data: Dict[str, Any]) -> List[str]:
    decisions = data.get("decisions") or []
    if not decisions:
        return ["시그널 파이프라인이 신호를 만드는지 확인해드릴까요?"]
    return [
        f"최근 결정 {len(decisions)}개가 실제 거래로 어떻게 이어졌는지 보시겠어요?",
        "이 결정들이 어떤 전략 분포에서 나왔는지 확인해볼까요?",
    ]


def fq_llm_trading_decisions(data: Dict[str, Any]) -> List[str]:
    return [
        "LLM 판단 (Track A) 과 시그널 기반 결정 (Track B) 을 비교해볼까요?",
        "LLM 판단이 실제 trade 결과로 어떻게 이어졌는지 보시겠어요?",
    ]


# ===========================================================================
# Trade History (4)
# ===========================================================================

def fq_trade_history(data: Dict[str, Any]) -> List[str]:
    trades = data.get("trades") or []
    if not trades:
        return ["현재 보유 포지션이라도 보시겠어요?"]
    qs = [f"{len(trades)}개 거래의 패턴/통계 분석을 해드릴까요?"]
    losing = [t for t in trades if isinstance(t, dict) and _safe_float(t.get("pnl", 0)) < 0]
    if losing and len(losing) / len(trades) > 0.5:
        qs.append(f"손실 비율 {len(losing)/len(trades)*100:.0f}% 의 공통 원인을 분석해드릴까요?")
    qs.append("이 거래들의 전략 분포도 보시겠어요?")
    return qs


def fq_analyze_trades(data: Dict[str, Any]) -> List[str]:
    return [
        "이 분석 결과를 학습된 전략 leaderboard 와 비교해볼까요?",
        "현재 활성 예측이 이 패턴과 일치하는지 보시겠어요?",
    ]


def fq_winning_trades(data: Dict[str, Any]) -> List[str]:
    trades = data.get("trades") or []
    if not trades:
        return ["전체 거래 이력이라도 보시겠어요?"]
    return [
        f"수익 {len(trades)} 거래의 공통 패턴을 분석해드릴까요?",
        "수익에 가장 기여한 전략을 확인해볼까요?",
    ]


def fq_losing_trades(data: Dict[str, Any]) -> List[str]:
    trades = data.get("trades") or []
    if not trades:
        return ["현재 작동중인 전략 leaderboard 를 볼까요?"]
    return [
        f"손실 {len(trades)} 거래의 시점/전략 패턴을 분석해드릴까요?",
        "손실 패턴이 deprecated feature 와 연관됐는지 보시겠어요?",
    ]


# ===========================================================================
# Layer Correlations (4)
# ===========================================================================

def fq_sector_correlations(data: Dict[str, Any]) -> List[str]:
    pairs = data.get("top_pairs") or []
    qs = []
    if pairs and isinstance(pairs[0], dict):
        a = pairs[0].get("a")
        if a:
            qs.append(f"가장 강한 pair 인 {a} 의 lead-lag 관계를 자세히 볼까요?")
    qs.append("이 섹터 구조에서 macro 인과 그래프로 layer 를 올려볼까요?")
    return qs


def fq_macro_causality_graph(data: Dict[str, Any]) -> List[str]:
    return [
        "이 인과 그래프가 시장 영향으로 어떻게 이어지는지 보시겠어요?",
        "이 인과 기반 예측의 실제 적중률을 확인해볼까요?",
    ]


def fq_symbol_peer_links(data: Dict[str, Any]) -> List[str]:
    sym = data.get("focus_symbol") or data.get("symbol")
    qs = []
    if sym:
        qs.append(f"{sym} 의 시그널과 peer 의 시그널을 비교해볼까요?")
    qs.append("이 종목이 속한 섹터의 클러스터 구조도 보시겠어요?")
    return qs


def fq_feature_governance_status_tool(data: Dict[str, Any]) -> List[str]:
    return [
        "전체 feature 의 lifecycle 상세를 볼까요?",
        "ACTIVE feature 들이 실제 정확도에 기여하는지 확인해드릴까요?",
    ]


# ===========================================================================
# Daily Brief (Phase F)
# ===========================================================================

def fq_daily_brief(data: Dict[str, Any]) -> List[str]:
    """get_daily_brief 응답 → 어느 영역으로 deep-dive 할지."""
    qs = []
    sigs = data.get("strong_signals") or []
    if sigs:
        sym = sigs[0].get("symbol") if isinstance(sigs[0], dict) else None
        if sym:
            qs.append(f"가장 강한 신호인 {sym} 을 자세히 볼까요?")
    if data.get("active_predictions_count", 0) > 0:
        qs.append("OneQAZ 가 현재 record 로 남긴 활성 예측을 보시겠어요?")
    if data.get("losing_count", 0) > 0:
        qs.append("어제 손실 거래의 패턴 분석을 해드릴까요?")
    if not qs:
        qs.append("관심있는 시장 (crypto/kr/us) 의 상세 분석을 보시겠어요?")
    return qs
