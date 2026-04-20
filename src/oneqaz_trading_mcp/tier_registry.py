# -*- coding: utf-8 -*-
"""
MCP Tier Registry
==================
MCP Server / MCP API / Claude.ai Integration 이 노출하는 모든 tool 과
resource 의 접근 티어를 한 곳에서 관리한다.

티어 정의 (key_store.TIER_LIMITS 와 일치):
    - free     : 인증 없음 (또는 무효 키). Trust Layer + aggregate 만.
    - pro      : 유효 키. authenticated aggregates + extended history.
    - internal : 내부 전용 키. 포지션/트레이드/의사결정 등 민감 데이터.

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

DEFAULT_TIER = "internal"  # whitelist 에 없으면 가장 안전한 쪽으로.


def tier_satisfies(caller_tier: str, required_tier: str) -> bool:
    """caller_tier 가 required_tier 이상인지."""
    return TIER_RANK.get(caller_tier, -1) >= TIER_RANK.get(required_tier, 99)


# ---------------------------------------------------------------------------
# Tool tier map
# ---------------------------------------------------------------------------
# Trust Layer + Market-level aggregates → free (B2AI credibility funnel)
# Symbol-level signal/decisions         → pro  (authenticated)
# Position / trade / LLM decision logs  → internal (owner only)
TOOL_TIERS: Dict[str, str] = {
    # ---- PUBLIC (Trust Layer, B2AI entry funnel) ----
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
    "get_macro_influence_map": "free",
    "get_cross_market_correlation": "free",
    "get_role_analysis": "free",

    # ---- PRO (authenticated aggregates / signals) ----
    "get_signals": "pro",
    "get_signal_detail": "pro",
    "explain_decision": "pro",

    # ---- INTERNAL (PII-like: positions, trades, LLM decisions) ----
    "get_positions": "internal",
    "get_position_detail": "internal",
    "get_profitable_positions": "internal",
    "get_losing_positions": "internal",
    "get_strategy_distribution": "internal",
    "get_trade_history": "internal",
    "analyze_trades": "internal",
    "get_winning_trades": "internal",
    "get_losing_trades": "internal",
    "get_latest_decisions": "internal",
    "get_llm_trading_decisions": "internal",
}


# ---------------------------------------------------------------------------
# Resource tier map (uri prefix 기준, 가장 긴 match 우선)
# ---------------------------------------------------------------------------
# 키는 FastMCP 에 등록된 URI 템플릿이 아니라 concrete uri 의 prefix 이다.
# 예: "market://coin/positions/snapshot" → "market://*/positions" prefix match.
# 와일드카드는 쓰지 않고 단순 startswith 로 매칭 + 가장 긴 prefix 우선.
RESOURCE_TIERS: Dict[str, str] = {
    # ---- INTERNAL (positions / unified symbol context with open positions) ----
    # market_status.py 에 등록: {market_id}/positions, /positions/snapshot
    "market://coin/positions": "internal",
    "market://kr_stock/positions": "internal",
    "market://us_stock/positions": "internal",
    "market://crypto/positions": "internal",  # alias
    "market://kr/positions": "internal",       # alias
    "market://us/positions": "internal",       # alias
    # unified/symbol 은 포지션 정보 + LLM conv 가 섞여 있어 internal.
    "market://coin/unified": "internal",
    "market://kr_stock/unified": "internal",
    "market://us_stock/unified": "internal",
    "market://crypto/unified": "internal",
    "market://kr/unified": "internal",
    "market://us/unified": "internal",

    # ---- PRO (authenticated aggregates) ----
    "market://coin/signals": "pro",
    "market://kr_stock/signals": "pro",
    "market://us_stock/signals": "pro",
    "market://crypto/signals": "pro",
    "market://kr/signals": "pro",
    "market://us/signals": "pro",
    "market://coin/derived": "pro",
    "market://kr_stock/derived": "pro",
    "market://us_stock/derived": "pro",
    "market://crypto/derived": "pro",
    "market://kr/derived": "pro",
    "market://us/derived": "pro",
    "market://coin/external": "pro",
    "market://kr_stock/external": "pro",
    "market://us_stock/external": "pro",
    "market://crypto/external": "pro",
    "market://kr/external": "pro",
    "market://us/external": "pro",
    "market://derived/": "pro",  # cross-market derived

    # ---- FREE (global + status + structure + indicators) ----
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
