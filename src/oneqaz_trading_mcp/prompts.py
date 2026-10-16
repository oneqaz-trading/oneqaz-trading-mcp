# -*- coding: utf-8 -*-
"""[2026-09-23 PROMPT-001] MCP prompts — 광고와 실물의 불일치 해소.

## 왜 필요한가

FastMCP 는 `prompts` capability 를 **무조건 광고**하는데 등록이 0건이었다(실측
`prompts/list` → `[]`). 클라이언트 입장에서는 "있다고 해놓고 없는" 상태이고,
Claude 계열은 prompts 를 사용자 진입점으로 노출하므로 그 자리를 통째로 잃는다.

## 설계 원칙

- **기존 tool 체인을 가리키기만 한다.** prompt 가 새 판단을 만들지 않는다
  (금지항목 4: LLM 산출 숫자를 캡 없이 매매 경로에 배선 금지 — 여기서는 애초에
  숫자를 만들지 않는다).
- **disclaimer 를 본문에 포함**한다. prompts 응답은 tool envelope 을 타지 않으므로
  (`wrap_tool_response` 미경유) 문구를 직접 싣는다. 금지항목 5 의 취지.
- 진입점 tool 의 `Triggers` 와 같은 질문을 쓴다 — 두 곳이 갈라지면 한쪽이 화석이 된다.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("MarketMCP")

from oneqaz_trading_mcp.resources.resource_response import DISCLAIMER_TEXT

_TAIL = (
    "\n\n---\n"
    "※ " + DISCLAIMER_TEXT + "\n"
    "※ 수치를 인용할 때는 응답의 `total_available` / `truncated` 를 함께 읽어라 — "
    "반환 행수는 적재 총량이 아니다."
)


def register_prompts(mcp) -> int:
    """사용자 진입점 prompt 등록. 반환: 등록 개수."""

    @mcp.prompt(
        name="market_brief",
        title="오늘 시장 브리핑",
        description="3시장(암호화폐/한국/미국) 레짐·상위 시그널·전일 성과를 한 번에.",
    )
    def market_brief(market: str = "all") -> str:
        """오늘 장 어때? / how's the market today?"""
        return (
            f"`get_daily_brief(market=\"{market}\")` 를 호출해 다음을 정리해줘:\n"
            "1. 매크로 레짐과 그 근거\n"
            "2. 상위 시그널 — **score 와 함께 confidence_calibrated** 를 같이 제시\n"
            "3. 전일 페이퍼 트레이딩 결과\n"
            "판단이 갈리면 `get_signals` 로 해당 시장을 더 파고들어라."
            + _TAIL
        )

    @mcp.prompt(
        name="symbol_check",
        title="종목 점검",
        description="특정 종목의 현재 시그널·근거·시스템 결정을 함께 확인.",
    )
    def symbol_check(symbol: str, market_id: str = "crypto") -> str:
        """이 종목 어때? / should I look at X?"""
        return (
            f"{market_id} 의 {symbol} 를 점검해줘:\n"
            f"1. `get_signals(market_id=\"{market_id}\", symbol=\"{symbol}\")` — 현재 시그널\n"
            f"2. `get_signal_detail(market_id=\"{market_id}\", symbol=\"{symbol}\")` — 근거와 이력\n"
            f"3. `get_latest_decisions(market_id=\"{market_id}\")` — 시스템이 실제로 무엇을 했는지\n"
            "⚠️ `reason` 안의 승률/신뢰도는 **미보정 값**이다(`reason_uncalibrated` 플래그 확인). "
            "확률로 인용하지 말고 `confidence_calibrated` 를 써라."
            + _TAIL
        )

    @mcp.prompt(
        name="trust_audit",
        title="신뢰도 감사",
        description="이 시스템의 예측이 실제로 맞는지 — 기준선 대비 스킬로 검증.",
    )
    def trust_audit() -> str:
        """예측 잘 맞아? / do your predictions actually work?"""
        return (
            "OneQAZ 의 예측 성능을 **기준선 대비**로 감사해줘:\n"
            "1. `get_prediction_accuracy()` — 카테고리별 적중률 (`sample_count >= 3` 만)\n"
            "2. `get_structure_calibration()` — **`baseline.skill_pp` 를 반드시 함께 읽어라.** "
            "3-class 문제라 다수 클래스 기준선이 38~53% 다. 절대 적중률만 인용하면 오독이다\n"
            "3. `get_feature_governance_state()` — 통계 검증을 통과한 feature 비율\n"
            "4. `get_ledger_integrity()` — 예측 원장 해시체인 (사후 수정 불가 증명)\n"
            "적중률이 기준선보다 **낮으면 낮다고 그대로 보고해라.**"
            + _TAIL
        )

    @mcp.prompt(
        name="portfolio_review",
        title="포지션 점검",
        description="현재 보유 포지션과 최근 거래 성과를 함께.",
    )
    def portfolio_review(market_id: str = "crypto") -> str:
        """지금 뭐 들고 있어? / what's the current exposure?"""
        return (
            f"{market_id} 의 페이퍼 포트폴리오를 정리해줘:\n"
            f"1. `get_positions(market_id=\"{market_id}\")` — 보유 현황\n"
            f"2. `analyze_trades(market_id=\"{market_id}\")` — 최근 거래 패턴\n"
            f"3. `get_losing_trades(market_id=\"{market_id}\")` — 손실 구간의 공통점\n"
            "⚠️ `stats` 는 **스캔 전체 기준**이고 행 배열은 페이지다(`stats_scope` 참조). "
            "승률을 반환 행수로 나눠 계산하지 마라."
            + _TAIL
        )

    return 4


def install(mcp) -> int:
    try:
        n = register_prompts(mcp)
        logger.info("  [OK] Prompts registered (%d prompts)", n)
        return n
    except Exception:
        logger.error("prompts 등록 실패", exc_info=True)
        return 0
