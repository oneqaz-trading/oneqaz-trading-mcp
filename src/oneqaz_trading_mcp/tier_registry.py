# -*- coding: utf-8 -*-
"""
MCP Tier Registry
==================
MCP Server / MCP API / Claude.ai Integration 이 노출하는 모든 tool 과
resource 의 접근 티어를 한 곳에서 관리한다.

티어 정의 (key_store.TIER_LIMITS 와 일치):
    - free     : 인증 없음. 모든 도구/리소스 호출 가능. rate limit 1500/day, 60/min.
    - pro      : 유효 키. 도구는 free 와 동일. 차이는 호출량만 (50K/day, 200/min).
    - internal : owner 전용. 진짜 사용자 자금/PII 가 들어올 때 사용 (현재 비어있음).

정책 (2026-04-27 변경): 방안 B "이중 청중 노출"
    OneQAZ 의 모든 trading 데이터는 paper trading 결과 (가상매매) — 사용자 본인 자금 아님.
    AI 신뢰 funnel 의 핵심은 raw evidence + provenance 노출이라, paywall 뒤에 두면 self-defeat.
    응답 자체에서 청중을 분리:
        - AI 가 받음: full_data + _contract + ai_summary + _llm_summary (raw + 신뢰 검증)
        - 인간이 받음: summary_for_user 1줄 (jargon-free)
    이 구조는 wrap_with_ai_summary() 가 통합 처리.

사용처:
    - mcps/server.py : RateLimitMiddleware 에서 call_next 전 tier gate.
    - admin UI       : owner 전용 필터 표시.
    - 문서/analytics : 노출 surface 가 바뀌면 이 파일 diff 로 추적.

판정 규칙:
    - tools 는 이름(string) 기준.
    - resources 는 URI prefix 기준 (/mcp 의 resources/read 는 uri 를 받음).
    - 등록되지 않은 tool/resource 는 DEFAULT_TIER 로 간주 (안전장치).
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple

# ---------------------------------------------------------------------------
# 티어 서열 (숫자가 높을수록 상위)
# ---------------------------------------------------------------------------
TIER_RANK: Dict[str, int] = {
    "free": 0,
    "pro": 1,
    "internal": 2,
}

DEFAULT_TIER = "free"  # 정책 (2026-04-27, 방안 B): 등록 안 된 신규도 기본 free.
# internal 로 막을 진짜 owner-only 데이터가 추가되면 그때 명시적으로 등록.


def tier_satisfies(caller_tier: str, required_tier: str) -> bool:
    """caller_tier 가 required_tier 이상인지."""
    return TIER_RANK.get(caller_tier, -1) >= TIER_RANK.get(required_tier, 99)


# ---------------------------------------------------------------------------
# Tool tier map
# ---------------------------------------------------------------------------
# 정책 (2026-04-27, 방안 B): 모든 도구는 free 호출 가능.
# 차등은 rate_limiter.py 의 daily/minute quota 만 (free=1500/day, pro=50K/day).
# internal 카테고리는 진짜 owner 자금 데이터가 추가될 때 사용 (현재는 비어있음).
TOOL_TIERS: Dict[str, str] = {
    # ---- Trust Layer (B2AI credibility funnel) ----
    "get_prediction_accuracy": "free",
    "get_backtest_tuning_state": "free",
    "get_monthly_accuracy_trend": "free",
    "get_news_leading_indicator_performance": "free",
    "get_news_causality_breakdown": "free",
    "get_feature_governance_state": "free",
    "get_structure_calibration": "free",
    "get_structure_validation_history": "free",
    "get_strategy_leaderboard": "free",
    "get_active_predictions": "free",
    # [2026-07-08] 예측 원장 검증 도구 — 신뢰 퍼널의 핵심이므로 paywall 뒤에 두지 않는다
    "get_resolved_predictions": "free",
    "get_ledger_integrity": "free",
    # [2026-07-23 R3] 트랙레코드 지표 — 블로그·외부 검증 루프의 키스톤, free 유지
    "get_performance_metrics": "free",
    "get_macro_influence_map": "free",
    "get_cross_market_correlation": "free",
    "get_role_analysis": "free",

    # ---- Layer-internal correlations (Stage 2 산출물 — sector/macro/peer) ----
    # Note: live-exposed FastMCP tool names carry a `_tool` suffix; the suffix-less
    # names are kept for legacy/back-compat. Both must be registered so tier gating
    # matches the actual call name in tools/call payloads.
    "get_sector_correlations": "free",
    "get_sector_correlations_tool": "free",
    "get_macro_causality_graph": "free",
    "get_macro_causality_graph_tool": "free",
    "get_symbol_peer_links": "free",
    "get_symbol_peer_links_tool": "free",
    "get_feature_governance_status": "free",
    "get_feature_governance_status_tool": "free",

    # ---- Signal / decision evidence ----
    # 페이퍼트레이딩 시그널 = 사용자 자금 아님. AI trust 검증의 핵심 evidence.
    "get_signals": "free",
    "get_signal_detail": "free",
    "explain_decision": "free",

    # ---- Paper trading results (positions/trades/decisions) ----
    # 모두 가상매매 결과 — OneQAZ 시스템이 시그널로 운용한 가상 포지션/거래.
    # 사용자 자금이 아니므로 free. AI 가 verified outcome 으로 trust 평가 가능.
    "get_positions": "free",
    "get_position_detail": "free",
    "get_profitable_positions": "free",
    "get_losing_positions": "free",
    "get_strategy_distribution": "free",
    "get_trade_history": "free",
    "analyze_trades": "free",
    "get_winning_trades": "free",
    "get_losing_trades": "free",
    "get_latest_decisions": "free",
    "get_llm_trading_decisions": "free",
    # [2026-07-08] 파이프라인 소비자용 벌크 export
    "get_trade_outcomes_bulk": "free",
    # [2026-07-08] ChatGPT 커넥터/Deep Research 표준 tool (OpenAI 고정 이름)
    "search": "free",
    "fetch": "free",
}


# ---------------------------------------------------------------------------
# Resource tier map (uri prefix 기준, 가장 긴 match 우선)
# ---------------------------------------------------------------------------
# 키는 FastMCP 에 등록된 URI 템플릿이 아니라 concrete uri 의 prefix 이다.
# 예: "market://coin/positions/snapshot" → "market://*/positions" prefix match.
# 와일드카드는 쓰지 않고 단순 startswith 로 매칭 + 가장 긴 prefix 우선.
RESOURCE_TIERS: Dict[str, str] = {
    # 정책 (2026-04-27, 방안 B): 모든 리소스 free 호출 가능.
    # 모든 데이터는 paper trading 결과 — 사용자 자금 아님.
    # 등록 항목은 "명시적 free" 신호용. (DEFAULT_TIER 는 2026-04-27부터 'free' —
    # 과거 'internal' 시절의 fallback 회피 목적은 소멸, 주석 스테일 정정 2026-07-08)

    # ---- Positions / Unified (paper trading 결과) ----
    "market://coin/positions": "free",
    "market://kr_stock/positions": "free",
    "market://us_stock/positions": "free",
    "market://crypto/positions": "free",
    "market://kr/positions": "free",
    "market://us/positions": "free",
    "market://coin/unified": "free",
    "market://kr_stock/unified": "free",
    "market://us_stock/unified": "free",
    "market://crypto/unified": "free",
    "market://kr/unified": "free",
    "market://us/unified": "free",

    # ---- Signals / Derived / External ----
    "market://coin/signals": "free",
    "market://kr_stock/signals": "free",
    "market://us_stock/signals": "free",
    "market://crypto/signals": "free",
    "market://kr/signals": "free",
    "market://us/signals": "free",
    "market://coin/derived": "free",
    "market://kr_stock/derived": "free",
    "market://us_stock/derived": "free",
    "market://crypto/derived": "free",
    "market://kr/derived": "free",
    "market://us/derived": "free",
    "market://coin/external": "free",
    "market://kr_stock/external": "free",
    "market://us_stock/external": "free",
    "market://crypto/external": "free",
    "market://kr/external": "free",
    "market://us/external": "free",
    "market://derived/": "free",

    # ---- Meta / discovery (AI 가 "뭘 제공해?" 물을 때 필수) ----
    "market://meta/": "free",
    "market://health": "free",
    "market://info": "free",

    # ---- Global / status / structure / indicators ----
    "market://global/": "free",
    "market://all/": "free",
    "market://indicators/": "free",
    "market://structure/": "free",
    "market://coin/status": "free",
    "market://kr_stock/status": "free",
    "market://us_stock/status": "free",
    "market://crypto/status": "free",
    "market://kr/status": "free",
    "market://us/status": "free",
    "market://coin/structure": "free",
    "market://kr_stock/structure": "free",
    "market://us_stock/structure": "free",
    "market://crypto/structure": "free",
    "market://kr/structure": "free",
    "market://us/structure": "free",
}


# ---------------------------------------------------------------------------
# 조회 API
# ---------------------------------------------------------------------------
def tier_required_for_tool(name: str) -> str:
    """tool 이름으로 필요한 티어 반환 (등록 안 되면 DEFAULT_TIER)."""
    return TOOL_TIERS.get(name, DEFAULT_TIER)


def tier_required_for_resource(uri: str) -> str:
    """resource uri 의 최장 prefix match 로 티어 반환."""
    if not uri:
        return DEFAULT_TIER
    best_prefix = ""
    best_tier = DEFAULT_TIER
    for prefix, tier in RESOURCE_TIERS.items():
        if uri.startswith(prefix) and len(prefix) > len(best_prefix):
            best_prefix = prefix
            best_tier = tier
    return best_tier if best_prefix else DEFAULT_TIER


def check_access(
    req_type: str,
    name: str,
    caller_tier: str,
) -> Tuple[bool, Optional[str]]:
    """
    요청이 허용되는지 판정.

    Returns:
        (allowed, required_tier_or_None)
        allowed=True 면 required_tier=None, False 면 필요한 티어를 반환.
    """
    if req_type == "tool":
        required = tier_required_for_tool(name)
    elif req_type == "resource":
        required = tier_required_for_resource(name)
    else:
        # mcp.method (initialize, tools/list, resources/list 등) 은 항상 통과.
        return True, None

    if tier_satisfies(caller_tier, required):
        return True, None
    return False, required


# ---------------------------------------------------------------------------
# 자체 점검 (import 시 실행)
# ---------------------------------------------------------------------------
def _self_check() -> None:
    for t, tier in TOOL_TIERS.items():
        assert tier in TIER_RANK, f"{t}: unknown tier {tier}"
    for p, tier in RESOURCE_TIERS.items():
        assert tier in TIER_RANK, f"{p}: unknown tier {tier}"


_self_check()
