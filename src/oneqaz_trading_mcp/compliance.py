# -*- coding: utf-8 -*-
"""
MCP 외부 문구·해석 기준의 단일 소스 — 최소분 (2026-10-15)
==========================================================
두 req 의 공통 첫 커밋이다:
  - `ai_brain/main_pc_tasks/req_mcp_entity_context.md` (엔티티 컨텍스트 tool)
  - `ai_brain/main_pc_tasks/req_mcp_disclaimer_positioning.md` (면책·포지셔닝 — Phase 2 미착수)

지금은 **엔티티 tool 만** 이 모듈을 쓴다. 기존 tool·resource 의 문구(`resource_response.DISCLAIMER_TEXT`
등)는 면책 req 의 결정(D1~D16)이 승인될 때 이 모듈로 옮긴다 — 그 전에 기존 응답을 바꾸지 않는다.

여기 값들은 면책 req 제안서(`proposal_mcp_first_contact_phase1_2026_10_15.md`)의 권고안을 전제로 한다
(엔티티 req 제안서 §1 "첫접촉 제안과의 의존", 2026-10-15 사용자 승인):
  - D14 — MCP 는 P(up)/P(down) 같은 3분 확률을 내지 않는다. 확률 표현을 쓰지 않는다.
  - D1  — action(buy/sell/hold) 대신 signal_score 부호의 bias 로 말한다.
  - D16 — meta 시각 필드 이름은 data_as_of / data_age_seconds.
"""

from __future__ import annotations

import math
from typing import Any, Dict, Optional, Tuple

# 응답 상단(meta)용 짧은 면책 문구. 3분 확률이 없으므로 "probabilistic" 을 쓰지 않는다(D14).
DISCLAIMER_SHORT_EN = (
    "Public beta · Market intelligence from observed market data. Not investment advice."
)
DISCLAIMER_SHORT_KO = "공개 베타 · 관측된 시장 데이터 기반 시장 정보이며 투자 자문이 아닙니다."

# bias 경계 = 시그널 판정 원장이 UP/DOWN 을 기록하는 문턱과 같은 값
# (`trade/core/realtime_accuracy.py::record_signal_prediction` — score > 0.1 UP, < -0.1 DOWN).
# 새 숫자를 만들지 않는다: 이 경계가 바뀌면 판정 원장과 함께 바뀌어야 한다.
BIAS_THRESHOLD = 0.1


def to_bias(score: Any) -> Optional[str]:
    """signal_score → positive / neutral / negative. 값이 없거나 수가 아니면 None."""
    try:
        v = float(score)
    except (TypeError, ValueError):
        return None
    if math.isnan(v):
        return None
    if v > BIAS_THRESHOLD:
        return "positive"
    if v < -BIAS_THRESHOLD:
        return "negative"
    return "neutral"


# 숫자 옆에 붙는 해석 기준 한 줄. 정의는 코드 근거만 쓴다(첫접촉 진단 §13-6).
INTERPRETATION: Dict[str, str] = {
    "signal_score": (
        "signal_score is an internal ranking score (about -1..1): above +0.1 is logged as an UP call "
        "and below -0.1 as a DOWN call (bias). It is not a probability, and scales differ by market."
    ),
    "confidence_calibrated": (
        "confidence_calibrated is the past 30-day hit rate of signals in the same raw-confidence "
        "bucket, judged 30 min to 6.5 h after the signal. It is not P(up)."
    ),
    "relation": (
        "Each link reads '<entity> <relation> <target>' (e.g. '005930 lags HYG' = HYG's move "
        "precedes 005930's by lag_days)."
    ),
    "evidence_kind": (
        "evidence.kind: statistical = correlation measured on past returns (n = sample size); "
        "definitional = OneQAZ basket/group definition (no sample); tag = a news item tagged with "
        "the entity; hypothesis = a pre-defined or tuned influence prior. A link describes an "
        "association, not a cause."
    ),
    "validation": (
        "evidence.validation reports whether links of this kind were forward-tested. "
        "NOT_SIGNIFICANT means past association did not translate into measurable forward hit-rate "
        "skill; do not present such a link as predictive."
    ),
    "regime_label": (
        "regime_label is a 7-stage position of a composite indicator score within its own recent bars "
        "(relative position, not an absolute up/down trend)."
    ),
    "regime_avg": (
        "regime_avg is the mean 7-stage regime position (1..7, 4 = middle) of a group's members; "
        "direction_1d is the group's daily-bar direction label."
    ),
    "structure_calibration": (
        "Structure calibration compares past 1d group direction calls with the realized group return "
        "(majority-class baseline shown); skill below zero means the calls did not beat the baseline."
    ),
    "market_wide": (
        "scope=market_wide items are tagged with the entity's market, not with the entity itself; "
        "they are context, not a direct link."
    ),
    "impact_level": (
        "news impact_level is a keyword/topic rule label, not a measured market impact."
    ),
    "gaps": "Do not infer missing links; gaps lists what OneQAZ does not know or does not track.",
}

# 관계 어휘 — kind 별 허용 relation (엔티티 req 제안서 ED6, 2026-10-15 승인).
# 인과 어휘(driven_by)는 Stage A 에서 쓰지 않는다. hypothesis 의 affects/affected_by 는
# 정적 사전·튜닝 맵(엔진 가정)이라는 뜻이며 metric 과 validation 을 반드시 동반한다.
RELATIONS_BY_KIND: Dict[str, Tuple[str, ...]] = {
    "statistical": ("leads", "lags", "co_moves"),
    "definitional": ("member_of", "has_member"),
    "tag": ("mentioned_in",),
    "hypothesis": ("affects", "affected_by"),
}

# headline·claim·interpretation 금지 어휘 (첫접촉 제안 probe 2·2b 와 같은 축).
# 점수·상관 수치 옆 강도 수식어, 매매 지시, 가격 목표, 주도·추천 류.
# 정렬 기준은 "Highest-|ρ|" 처럼 **기준 이름**으로 쓴다.
FORBIDDEN_CLAIM_TERMS_EN: Tuple[str, ...] = (
    "strong", "strongest", "stronger", "weakest", "leading group", "leader",
    "buy", "sell", "entry", "target price", "opportunity", "recommend", "should",
    "probability", "p(up)", "guarantee",
)
FORBIDDEN_CLAIM_TERMS_KO: Tuple[str, ...] = (
    "강한", "강력", "가장 강", "주도", "매수", "매도", "진입", "목표가", "추천", "기회", "권장", "확률",
)
