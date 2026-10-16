# -*- coding: utf-8 -*-
"""
mcps.schemas
============
Pydantic models that pin OneQAZ's tool envelope and a small number of high-value
`full_data` payloads. Used to populate the `outputSchema` field that FastMCP
publishes via `tools/list`, replacing the default `additionalProperties: true`
catch-all.

Design decisions:
- Envelope fields (disclaimer / request_id / timestamp / is_investment_advice /
  is_real_money / data_classification / ai_summary / summary_for_user / full_data
  / _value_signals / _next_actions / _followup_questions_for_user /
  _market_state_narrative / _llm_summary) are required keys, but downstream
  consumers may receive additional keys without breaking.
- `full_data` is `Dict[str, Any]` for tools that have not yet been pinned, and
  a structured model for the seven hot tools we care about
  (daily_brief, prediction_accuracy, strategy_leaderboard, signals, positions,
  trade_history, explain_decision).
- An `ErrorEnvelope` mirrors what `mcp_error()` produces so `outputSchema` can
  declare `oneOf: [ToolEnvelope, ErrorEnvelope]` for each tool.
- We do not import these models inside hot-path code; they are read once at
  registration time by FastMCP introspection.

When updating these models, also update:
- `docs/mcp/mcp_tool_schema.md`
- `docs/mcp/mcp_response_examples.md`
- `mcps/resources/resource_response.wrap_with_ai_summary` (if envelope shape changes)
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

try:
    from pydantic import BaseModel, ConfigDict, Field
    _HAS_PYDANTIC = True
except ImportError:  # pragma: no cover
    BaseModel = object  # type: ignore[assignment,misc]
    _HAS_PYDANTIC = False


# ---------------------------------------------------------------------------
# Shared types
# ---------------------------------------------------------------------------

if _HAS_PYDANTIC:

    class ValueSignals(BaseModel):
        """Static product/value metadata — same for every response of a tool type."""
        model_config = ConfigDict(extra="allow")

        tier_default: Literal["free", "pro", "internal"] = "free"
        tier_default_limits: Dict[str, int] = Field(
            default_factory=lambda: {"daily": 1500, "minute": 60}
        )
        data_freshness_seconds: int = 120
        # [2026-10-13] 위 값의 근거 — 실측 데이터 나이가 아니라 ai_summary 캐시 TTL 상수 (additive).
        data_freshness_basis: Literal["ai_summary_cache_ttl"] = "ai_summary_cache_ttl"
        what_pro_unlocks: str = (
            "33x daily quota (50K), 3.3x burst (200/min) — same tools, higher volume"
        )
        pricing_url: str = "https://api.oneqaz.com/pricing"
        key_signup_url: str = "https://api.oneqaz.com/keys"
        self_correcting: bool = True
        # [2026-07-20 RCA] 1.0 → 1.1: get_prediction_accuracy 셀별 persistence/skill/
        # 보정CI/v2-only 필드 추가 + edge 판정 v2 + get_signal_calibration 신설.
        # 기존 필드 제거/의미 변경 없음 (additive).
        schema_version: str = "1.1"


    class NextAction(BaseModel):
        """One AI-actionable follow-up call recommendation."""
        model_config = ConfigDict(extra="allow")

        intent: str = Field(description="Short intent label, max ~120 chars")
        tool: str = Field(description="Recommended tool name to invoke next")
        args: Dict[str, Any] = Field(default_factory=dict)
        rationale: str = Field(default="", description="Why this is recommended")
        priority: Literal["high", "normal", "low"] = "normal"


    class MarketStateNarrativeBlock(BaseModel):
        """Bilingual narrative — one of {ko, en} must carry a headline."""
        model_config = ConfigDict(extra="allow")

        headline: str
        evidence: List[str] = Field(default_factory=list)
        what_to_watch: Optional[str] = None


    class MarketStateNarrative(BaseModel):
        """Wrapper for `_market_state_narrative` — bilingual + provenance."""
        model_config = ConfigDict(extra="allow")

        ko: Optional[MarketStateNarrativeBlock] = None
        en: Optional[MarketStateNarrativeBlock] = None
        # Either "vllm" (live LLM-generated) or "templated" (deterministic fallback).
        source_kind: Optional[str] = Field(default=None, alias="_source")


    # ---------------------------------------------------------------------------
    # Envelope (success path)
    # ---------------------------------------------------------------------------

    class ToolEnvelope(BaseModel):
        """Standard wrapper around every tool response — success OR error.

        Because MCP spec requires `outputSchema` to be a single object schema
        (no top-level anyOf / oneOf), this model accepts BOTH shapes:
        - Success: ai_summary, full_data, _value_signals etc. populated.
        - Error: error=true, error_code, reason, action, retryable populated.
        Required across both shapes is only the compliance/observability tail
        (`disclaimer`, `request_id`, `timestamp`).

        Tools with pinned full_data shapes override `full_data` via subclasses.
        """
        model_config = ConfigDict(extra="allow", populate_by_name=True)

        # [2026-09-23 P0-B] 선언을 **항상 있는 것 + tool 고유분**으로 줄였다.
        # 종전에는 39종 outputSchema 에 envelope 전체가 토씨 하나 안 다르고 반복돼
        # 21,833 tok 중 17,580 이 중복이었다 (_market_state_narrative 3,720 /
        # _value_signals 2,985 / _next_actions 1,995 + 스칼라 ~8,880).
        #
        # 아래 선택적 필드들(ai_summary, _value_signals, _next_actions,
        # _market_state_narrative, 에러 경로 필드 …)은 **응답에서 사라지지 않는다** —
        # wrap_tool_response / mcp_error 가 런타임에 그대로 싣고, extra="allow" 가
        # 받는다. 바뀌는 것은 **스키마 광고분**뿐이다.
        #
        # ⚠️ 컴플라이언스 꼬리(disclaimer / request_id / timestamp /
        # is_investment_advice / data_classification)는 **일부러 남긴다.**
        # additionalProperties 는 "허용" 이지 "보장" 이 아니라서, 빼면 외부
        # 도입자가 "이 서버는 항상 disclaimer 를 주는가" 를 스키마에서 확인할 수
        # 없다. 그 선언이 이 제품(B2AI 신뢰 레이어)의 명분 자체다.
        full_data: Optional[Dict[str, Any]] = Field(default=None, description="Tool-specific payload (success path)")

        # ── Compliance / observability (always present, both paths) ──
        disclaimer: str = Field(description="Canonical compliance disclaimer (always present)")
        request_id: str = Field(description="32-hex per-response correlation id")
        timestamp: str = Field(description="RFC3339 UTC, server build time")
        is_investment_advice: Optional[Literal[False]] = Field(default=None)
        is_real_money: Optional[Literal[False]] = Field(default=None)
        data_classification: Optional[Literal["research_information_only"]] = Field(default=None)


    # ---------------------------------------------------------------------------
    # Error envelope (mirrors mcp_error)
    # ---------------------------------------------------------------------------

    class ErrorEnvelope(BaseModel):
        """Structured error response, matches `mcp_error()` output."""
        model_config = ConfigDict(extra="allow")

        error: Literal[True] = True
        error_code: str = Field(description="Stable string identifier; see mcp_error_policy.md")
        reason: str
        action: Literal["retry_after_seconds", "use_fallback", "check_availability"]
        action_value: str = ""
        fallback_tool: Optional[str] = None
        fallback_note: Optional[str] = None
        retryable: bool

        # Compliance / observability fields (mirrors success envelope tail)
        request_id: str
        timestamp: str
        disclaimer: str


    # ---------------------------------------------------------------------------
    # Pinned full_data payloads — high-value tools
    # ---------------------------------------------------------------------------

    class PredictionAccuracyMeta(BaseModel):
        model_config = ConfigDict(extra="allow")
        total_category_target_lag_cells: int
        total_samples: int
        sample_count_filter: str
        source: str
        baseline_accuracy: float
        interpretation: str


    class PredictionAccuracyData(BaseModel):
        """`full_data` for get_prediction_accuracy."""
        model_config = ConfigDict(extra="allow")
        # nested {category: {target_market: {lag_bucket: {accuracy, samples, ...}}}}
        summary: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]]
        meta: PredictionAccuracyMeta


    class StrategyLeaderboardEntry(BaseModel):
        model_config = ConfigDict(extra="allow")
        strategy_id: Optional[str] = None
        win_rate: Optional[float] = None
        profit_factor: Optional[float] = None
        trades_count: Optional[int] = None
        is_synthesized: Optional[bool] = None


    class StrategyLeaderboardMeta(BaseModel):
        model_config = ConfigDict(extra="allow")
        measured_entries: int = 0
        synthesized_entries: int = 0
        interpretation: Optional[str] = None


    class StrategyLeaderboardData(BaseModel):
        """`full_data` for get_strategy_leaderboard."""
        model_config = ConfigDict(extra="allow")
        leaderboard: List[StrategyLeaderboardEntry] = Field(default_factory=list)
        per_symbol_leaderboard: List[StrategyLeaderboardEntry] = Field(default_factory=list)
        meta: StrategyLeaderboardMeta = Field(default_factory=StrategyLeaderboardMeta)


    class TradeRow(BaseModel):
        model_config = ConfigDict(extra="allow")
        symbol: str
        action: Optional[str] = None
        profit_loss_pct: Optional[float] = None
        entry_price: Optional[float] = None
        exit_price: Optional[float] = None
        entry_timestamp: Optional[int] = None
        exit_timestamp: Optional[int] = None
        holding_duration: Optional[int] = None
        ai_score: Optional[float] = None
        signal_pattern: Optional[str] = None


    class TradeStats(BaseModel):
        model_config = ConfigDict(extra="allow")
        total_trades: int
        wins: int
        losses: int
        win_rate: float
        total_pnl: float
        avg_pnl: float


    class TradeHistoryData(BaseModel):
        """`full_data` for get_trade_history."""
        model_config = ConfigDict(extra="allow")
        market_id: str
        trades: List[TradeRow] = Field(default_factory=list)
        stats: TradeStats
        filters: Dict[str, Any] = Field(default_factory=dict)


    class PositionRow(BaseModel):
        model_config = ConfigDict(extra="allow")
        symbol: str
        entry_price: Optional[float] = None
        current_price: Optional[float] = None
        profit_loss_pct: Optional[float] = None
        quantity: Optional[float] = None
        entry_timestamp: Optional[int] = None
        holding_duration: Optional[int] = None
        ai_score: Optional[float] = None
        current_strategy: Optional[str] = None


    class PositionsStats(BaseModel):
        model_config = ConfigDict(extra="allow")
        total_positions: Optional[int] = None
        total: Optional[int] = None
        profitable: Optional[int] = None
        losing: Optional[int] = None
        avg_pnl: Optional[float] = None


    class PositionsData(BaseModel):
        """`full_data` for get_positions."""
        model_config = ConfigDict(extra="allow")
        market_id: str
        positions: List[PositionRow] = Field(default_factory=list)
        stats: Optional[PositionsStats] = None


    class SignalRow(BaseModel):
        model_config = ConfigDict(extra="allow")
        symbol: str
        market_id: Optional[str] = None
        interval: Optional[str] = None
        action: Optional[str] = None
        signal_score: Optional[float] = None
        confidence: Optional[float] = None
        price: Optional[float] = None
        timestamp: Optional[int] = None
        # [2026-09-22] warnings 는 True 인 플래그만 담기며 없으면 키 자체가 생략된다.
        warnings: Optional[Dict[str, bool]] = None
        # reason 내 승률/신뢰도가 미보정임을 표시 — 문장은 응답의
        # reason_uncalibrated_note 에 1회만 실린다.
        reason_uncalibrated: Optional[bool] = None


    class SignalsData(BaseModel):
        """`full_data` for get_signals."""
        model_config = ConfigDict(extra="allow")
        market_id: str
        signals: List[SignalRow] = Field(default_factory=list)
        filters: Dict[str, Any] = Field(default_factory=dict)
        # [2026-09-22] 응답 전 행이 공유하는 값(currency/risk_level/timestamp_str
        # 등)은 행에서 빠져 여기에 1회만 실린다. 행에 같은 키가 있으면 행이 우선.
        common: Dict[str, Any] = Field(default_factory=dict)
        # [2026-09-22 정직성] limit 절단 여부 — 외부가 결과를 '시장 전체'로
        # 오독하지 않도록 반환 건수/상한을 함께 공개한다.
        returned: Optional[int] = None
        limit: Optional[int] = None
        truncated: Optional[bool] = None
        # [2026-10-07 ANS-002] symbol 지정 시 인터벌별 최신 1행 (hours_back 무관 — 현재 판정).
        # signals/stats 는 이력 행 집계다. {interval: {action, signal_score, confidence_calibrated,
        # timestamp, timestamp_str, age_minutes}}
        latest_by_interval: Optional[Dict[str, Any]] = None


    class DailyBriefMacroRegime(BaseModel):
        model_config = ConfigDict(extra="allow")
        # [2026-10-14] 엔진 글로벌 레짐(additive). 오래됐거나(>6h) 읽을 수 없으면 None + unavailable_reason.
        regime: Optional[str] = None
        regime_score: Optional[float] = None
        regime_signed: Optional[float] = None
        as_of: Optional[str] = None
        stale: Optional[bool] = None
        unavailable_reason: Optional[str] = None
        categories: List[Any] = Field(default_factory=list)
        total: int = 0


    class DailyBriefYesterdayTrades(BaseModel):
        model_config = ConfigDict(extra="allow")
        total: int = 0
        winning: int = 0
        losing: int = 0
        by_market: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


    class DailyBriefMeta(BaseModel):
        model_config = ConfigDict(extra="allow")
        data_window: str = "last_24h"
        source: List[str] = Field(default_factory=list)
        interpretation: str = ""


    class DailyBriefData(BaseModel):
        """`full_data` for get_daily_brief."""
        model_config = ConfigDict(extra="allow")
        market: str
        narrative: str
        primary_market: Optional[str] = None
        macro_regime: DailyBriefMacroRegime = Field(default_factory=DailyBriefMacroRegime)
        strong_signals: List[SignalRow] = Field(default_factory=list)
        # [2026-10-14] 시장별 심볼당 최신 신호의 action 분포(엔진의 현재 자세) — {market: {symbols, by_action, basis}}
        market_breadth: Dict[str, Any] = Field(default_factory=dict)
        yesterday_trades: DailyBriefYesterdayTrades = Field(default_factory=DailyBriefYesterdayTrades)
        winning_count: int = 0
        losing_count: int = 0
        active_predictions_count: int = 0
        meta: DailyBriefMeta = Field(default_factory=DailyBriefMeta)


    class ExplainDecisionRecommendation(BaseModel):
        model_config = ConfigDict(extra="allow")
        verdict: Optional[str] = None
        text: Optional[str] = None
        # [2026-10-07 ANS-001] verdict 산출 입력·규칙 (signal_score, latest_decision,
        # already_holding, historical_class, news_flag, rules) — 외부 자동 검증용
        basis: Optional[Dict[str, Any]] = None


    class ExplainDecisionData(BaseModel):
        """`full_data` for explain_decision."""
        model_config = ConfigDict(extra="allow")
        symbol: str
        overall_recommendation: ExplainDecisionRecommendation = Field(
            default_factory=ExplainDecisionRecommendation
        )
        layers: Dict[str, Any] = Field(default_factory=dict)


    # ── [2026-07-08] Batch 2 pinned payloads — high-traffic tools without
    #    an outputSchema (B2AI 감사: tools/list description/schema 가 상품 진열장) ──

    class TradeAnalysisData(BaseModel):
        """`full_data` for analyze_trades (day / pattern / symbol aggregates)."""
        model_config = ConfigDict(extra="allow")
        market_id: str
        days: int = 0
        total_trades: int = 0
        # {"YYYY-MM-DD": {trades, pnl, wins}} / {pattern: {...}} / {symbol: {...}}
        daily_stats: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
        pattern_stats: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
        coin_stats: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
        top_coins: List[Any] = Field(default_factory=list)
        top_patterns: List[Any] = Field(default_factory=list)


    class DecisionRow(BaseModel):
        model_config = ConfigDict(extra="allow")
        symbol: Optional[str] = None
        decision: Optional[str] = None
        signal_score: Optional[float] = None
        timestamp: Optional[Any] = None
        timestamp_str: Optional[str] = None
        reason: Optional[str] = None
        ai_score: Optional[float] = None
        ai_reason: Optional[str] = None


    class DecisionsStats(BaseModel):
        model_config = ConfigDict(extra="allow")
        total: int = 0
        buy_count: int = 0
        sell_count: int = 0
        hold_count: int = 0


    class LatestDecisionsData(BaseModel):
        """`full_data` for get_latest_decisions (Track B decision log)."""
        model_config = ConfigDict(extra="allow")
        market_id: str
        timestamp: Optional[str] = None
        decisions: List[DecisionRow] = Field(default_factory=list)
        stats: DecisionsStats = Field(default_factory=DecisionsStats)


    class LlmTradingDecisionsData(BaseModel):
        """`full_data` for get_llm_trading_decisions (Track A judgement log).

        Rows come from SELECT * so the row shape is kept open (Dict)."""
        model_config = ConfigDict(extra="allow")
        market_id: str
        timestamp: Optional[str] = None
        decisions: List[Dict[str, Any]] = Field(default_factory=list)
        stats: DecisionsStats = Field(default_factory=DecisionsStats)


    class ActivePredictionRow(BaseModel):
        model_config = ConfigDict(extra="allow")
        source_category: Optional[str] = None
        regime_change: Optional[str] = None
        target_market: Optional[str] = None
        predicted_shift: Optional[str] = None
        lag_hours: Optional[float] = None
        confidence: Optional[float] = None
        created_at: Optional[Any] = None


    class ActivePredictionsData(BaseModel):
        """`full_data` for get_active_predictions (pending forecasts, outcome IS NULL)."""
        model_config = ConfigDict(extra="allow")
        predictions: List[ActivePredictionRow] = Field(default_factory=list)
        meta: Dict[str, Any] = Field(default_factory=dict)


    class MonthlyTrendPoint(BaseModel):
        model_config = ConfigDict(extra="allow")
        month: Optional[str] = None
        category: Optional[str] = None
        target_market: Optional[str] = None
        lag_bucket: Optional[str] = None
        accuracy: Optional[float] = None
        sample_count: Optional[int] = None


    class MonthlyAccuracyTrendData(BaseModel):
        """`full_data` for get_monthly_accuracy_trend."""
        model_config = ConfigDict(extra="allow")
        trend: List[MonthlyTrendPoint] = Field(default_factory=list)
        meta: Dict[str, Any] = Field(default_factory=dict)


    class BacktestTuningEntry(BaseModel):
        model_config = ConfigDict(extra="allow")
        category: Optional[str] = None
        target_market: Optional[str] = None
        tuned_lag_hours: Optional[float] = None
        tuned_sensitivity: Optional[float] = None
        confidence: Optional[float] = None
        sample_count: Optional[int] = None
        last_backtest: Optional[str] = None


    class BacktestTuningStateData(BaseModel):
        """`full_data` for get_backtest_tuning_state."""
        model_config = ConfigDict(extra="allow")
        tuning_entries: List[BacktestTuningEntry] = Field(default_factory=list)
        meta: Dict[str, Any] = Field(default_factory=dict)


    # ---------------------------------------------------------------------------
    # Per-tool envelope subclasses — used as return-type annotation in @mcp.tool
    # ---------------------------------------------------------------------------

    # Per-tool envelopes refine the optional `full_data` to a pinned shape
    # while keeping all other fields optional so error responses still validate.

    class PredictionAccuracyEnvelope(ToolEnvelope):
        full_data: Optional[PredictionAccuracyData] = None  # type: ignore[assignment]

    class StrategyLeaderboardEnvelope(ToolEnvelope):
        full_data: Optional[StrategyLeaderboardData] = None  # type: ignore[assignment]

    class TradeHistoryEnvelope(ToolEnvelope):
        full_data: Optional[TradeHistoryData] = None  # type: ignore[assignment]

    class PositionsEnvelope(ToolEnvelope):
        full_data: Optional[PositionsData] = None  # type: ignore[assignment]

    class SignalsEnvelope(ToolEnvelope):
        full_data: Optional[SignalsData] = None  # type: ignore[assignment]

    class DailyBriefEnvelope(ToolEnvelope):
        full_data: Optional[DailyBriefData] = None  # type: ignore[assignment]

    class ExplainDecisionEnvelope(ToolEnvelope):
        full_data: Optional[ExplainDecisionData] = None  # type: ignore[assignment]

    # ── [2026-07-08] Batch 2 envelopes ──

    class TradeAnalysisEnvelope(ToolEnvelope):
        full_data: Optional[TradeAnalysisData] = None  # type: ignore[assignment]

    class LatestDecisionsEnvelope(ToolEnvelope):
        full_data: Optional[LatestDecisionsData] = None  # type: ignore[assignment]

    class LlmTradingDecisionsEnvelope(ToolEnvelope):
        full_data: Optional[LlmTradingDecisionsData] = None  # type: ignore[assignment]

    class ActivePredictionsEnvelope(ToolEnvelope):
        full_data: Optional[ActivePredictionsData] = None  # type: ignore[assignment]

    class MonthlyAccuracyTrendEnvelope(ToolEnvelope):
        full_data: Optional[MonthlyAccuracyTrendData] = None  # type: ignore[assignment]

    class BacktestTuningStateEnvelope(ToolEnvelope):
        full_data: Optional[BacktestTuningStateData] = None  # type: ignore[assignment]


    # -----------------------------------------------------------------------
    # [2026-09-23 P0-B] 빈 outputSchema 24종의 full_data 선언
    #
    # 스캐너 지적: "Structured output declared, but exposes no named fields."
    # 반환형이 Dict[str, Any] 라 FastMCP 가 properties 없는 object 를 발행했다.
    #
    # 형태는 **살아있는 응답에서 추출**했다(손으로 쓰면 실물과 어긋나고,
    # 어긋나는 순간 스키마가 거짓 광고가 된다). 전 필드 Optional + extra=allow
    # 라 에러 응답도 그대로 통과한다 — 기존 15종과 같은 계약이다.
    # -----------------------------------------------------------------------

    class FetchData(BaseModel):
        """`full_data` for fetch — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        id: Optional[str] = None
        title: Optional[str] = None
        text: Optional[str] = None
        url: Optional[str] = None
        metadata: Dict[str, Any] = Field(default_factory=dict)


    class CrossMarketCorrelationData(BaseModel):
        """`full_data` for get_cross_market_correlation — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        correlations: List[Any] = Field(default_factory=list)
        decoupling: List[Any] = Field(default_factory=list)
        meta: Dict[str, Any] = Field(default_factory=dict)


    class FeatureGovernanceStateData(BaseModel):
        """`full_data` for get_feature_governance_state — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        features: List[Any] = Field(default_factory=list)
        status_summary: Dict[str, Any] = Field(default_factory=dict)
        returned: Optional[int] = None
        limit: Optional[int] = None
        truncated: Optional[bool] = None
        total_available: Optional[int] = None
        truncated_note: Optional[str] = None
        meta: Dict[str, Any] = Field(default_factory=dict)
        common: Dict[str, Any] = Field(default_factory=dict)
        common_note: Optional[str] = None


    class FeatureGovernanceStatusData(BaseModel):
        """`full_data` for get_feature_governance_status_tool — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        by_status: Dict[str, Any] = Field(default_factory=dict)
        recent_transitions: List[Any] = Field(default_factory=list)
        total: Optional[int] = None


    class LedgerIntegrityData(BaseModel):
        """`full_data` for get_ledger_integrity — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        recipe_version: Optional[str] = None
        recipe: Optional[str] = None
        chain_length: Optional[int] = None
        first_day: Optional[str] = None
        last_day: Optional[str] = None
        entries: List[Any] = Field(default_factory=list)
        external_anchors: Dict[str, Any] = Field(default_factory=dict)
        verification_hint: Optional[str] = None


    class LosingPositionsData(BaseModel):
        """`full_data` for get_losing_positions — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        market_id: Optional[str] = None
        positions: List[Any] = Field(default_factory=list)
        stats: Dict[str, Any] = Field(default_factory=dict)
        stats_scope: Optional[str] = None
        returned: Optional[int] = None
        limit: Optional[int] = None
        truncated: Optional[bool] = None
        total_available: Optional[int] = None
        truncated_note: Optional[str] = None
        common: Dict[str, Any] = Field(default_factory=dict)
        common_note: Optional[str] = None
        filters: Dict[str, Any] = Field(default_factory=dict)


    class MacroCausalityGraphData(BaseModel):
        """`full_data` for get_macro_causality_graph_tool — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        edges: List[Any] = Field(default_factory=list)
        total_edges: Optional[int] = None
        significant_count: Optional[int] = None


    class MacroInfluenceMapData(BaseModel):
        """`full_data` for get_macro_influence_map — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        influence_map: Dict[str, Any] = Field(default_factory=dict)
        meta: Dict[str, Any] = Field(default_factory=dict)


    class NewsCausalityBreakdownData(BaseModel):
        """`full_data` for get_news_causality_breakdown — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        breakdown: Dict[str, Any] = Field(default_factory=dict)
        meta: Dict[str, Any] = Field(default_factory=dict)


    class NewsLeadingIndicatorPerformanceData(BaseModel):
        """`full_data` for get_news_leading_indicator_performance — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        indicators: List[Any] = Field(default_factory=list)
        meta: Dict[str, Any] = Field(default_factory=dict)


    class PerformanceMetricsData(BaseModel):
        """`full_data` for get_performance_metrics — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        market: Optional[str] = None
        account_type: Optional[str] = None
        window: Dict[str, Any] = Field(default_factory=dict)
        capital_model: Dict[str, Any] = Field(default_factory=dict)
        metrics: Dict[str, Any] = Field(default_factory=dict)
        monthly_returns: List[Any] = Field(default_factory=list)
        totals: Dict[str, Any] = Field(default_factory=dict)


    class PositionDetailData(BaseModel):
        """`full_data` for get_position_detail — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        market_id: Optional[str] = None
        coin: Optional[str] = None
        position: Dict[str, Any] = Field(default_factory=dict)
        recent_trades: List[Any] = Field(default_factory=list)
        recent_decisions: List[Any] = Field(default_factory=list)


    class ProfitablePositionsData(BaseModel):
        """`full_data` for get_profitable_positions — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        market_id: Optional[str] = None
        positions: List[Any] = Field(default_factory=list)
        stats: Dict[str, Any] = Field(default_factory=dict)
        stats_scope: Optional[str] = None
        returned: Optional[int] = None
        limit: Optional[int] = None
        truncated: Optional[bool] = None
        total_available: Optional[int] = None
        truncated_note: Optional[str] = None
        common: Dict[str, Any] = Field(default_factory=dict)
        common_note: Optional[str] = None
        filters: Dict[str, Any] = Field(default_factory=dict)


    class ResolvedPredictionsData(BaseModel):
        """`full_data` for get_resolved_predictions — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        predictions: List[Any] = Field(default_factory=list)
        count: Optional[int] = None
        next_cursor: Optional[int] = None
        has_more: Optional[bool] = None
        filters: Dict[str, Any] = Field(default_factory=dict)
        meta: Dict[str, Any] = Field(default_factory=dict)


    class RoleAnalysisData(BaseModel):
        """`full_data` for get_role_analysis — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        market_id: Optional[str] = None
        symbol: Optional[str] = None
        timestamp: Optional[str] = None
        roles: Dict[str, Any] = Field(default_factory=dict)
        hierarchy: Dict[str, Any] = Field(default_factory=dict)
        combined: Dict[str, Any] = Field(default_factory=dict)


    class SectorCorrelationsData(BaseModel):
        """`full_data` for get_sector_correlations_tool — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        market_id: Optional[str] = None
        top_pairs: List[Any] = Field(default_factory=list)
        clusters: List[Any] = Field(default_factory=list)
        total_pairs: Optional[int] = None


    class SignalCalibrationData(BaseModel):
        """`full_data` for get_signal_calibration — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        snapshot_day: Optional[str] = None
        variant: Optional[str] = None
        markets: Dict[str, Any] = Field(default_factory=dict)
        meta: Dict[str, Any] = Field(default_factory=dict)


    class SignalDetailData(BaseModel):
        """`full_data` for get_signal_detail — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        market_id: Optional[str] = None
        symbol: Optional[str] = None
        interval: Optional[str] = None
        timestamp: Optional[str] = None
        latest_signal: Dict[str, Any] = Field(default_factory=dict)
        recent_history: List[Any] = Field(default_factory=list)
        pattern_feedback: Dict[str, Any] = Field(default_factory=dict)


    class StrategyDistributionData(BaseModel):
        """`full_data` for get_strategy_distribution — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        market_id: Optional[str] = None
        distribution: List[Any] = Field(default_factory=list)


    class StructureCalibrationData(BaseModel):
        """`full_data` for get_structure_calibration — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        calibration: List[Any] = Field(default_factory=list)
        returned: Optional[int] = None
        limit: Optional[int] = None
        truncated: Optional[bool] = None
        total_available: Optional[int] = None
        truncated_note: Optional[str] = None
        baseline: Dict[str, Any] = Field(default_factory=dict)
        meta: Dict[str, Any] = Field(default_factory=dict)


    class StructureValidationHistoryData(BaseModel):
        """`full_data` for get_structure_validation_history — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        history: List[Any] = Field(default_factory=list)
        summary: Dict[str, Any] = Field(default_factory=dict)
        meta: Dict[str, Any] = Field(default_factory=dict)


    class SymbolPeerLinksData(BaseModel):
        """`full_data` for get_symbol_peer_links_tool — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        market_id: Optional[str] = None
        focus_symbol: Optional[Any] = None
        as_lead: List[Any] = Field(default_factory=list)
        as_follow: List[Any] = Field(default_factory=list)
        total_links: Optional[int] = None


    class TradeOutcomesBulkData(BaseModel):
        """`full_data` for get_trade_outcomes_bulk — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        market: Optional[str] = None
        trades: List[Any] = Field(default_factory=list)
        count: Optional[int] = None
        linked_prediction_count: Optional[int] = None
        next_cursor: Optional[int] = None
        has_more: Optional[bool] = None
        window_days: Optional[int] = None
        meta: Dict[str, Any] = Field(default_factory=dict)


    class SearchData(BaseModel):
        """`full_data` for search — 실응답에서 추출(2026-09-23)."""
        model_config = ConfigDict(extra="allow")
        results: List[Any] = Field(default_factory=list)


    class CrossMarketCorrelationEnvelope(ToolEnvelope):
        full_data: Optional[CrossMarketCorrelationData] = None  # type: ignore[assignment]

    class FeatureGovernanceStateEnvelope(ToolEnvelope):
        full_data: Optional[FeatureGovernanceStateData] = None  # type: ignore[assignment]

    class FeatureGovernanceStatusEnvelope(ToolEnvelope):
        full_data: Optional[FeatureGovernanceStatusData] = None  # type: ignore[assignment]

    class LedgerIntegrityEnvelope(ToolEnvelope):
        full_data: Optional[LedgerIntegrityData] = None  # type: ignore[assignment]

    class LosingPositionsEnvelope(ToolEnvelope):
        full_data: Optional[LosingPositionsData] = None  # type: ignore[assignment]

    class MacroCausalityGraphEnvelope(ToolEnvelope):
        full_data: Optional[MacroCausalityGraphData] = None  # type: ignore[assignment]

    class MacroInfluenceMapEnvelope(ToolEnvelope):
        full_data: Optional[MacroInfluenceMapData] = None  # type: ignore[assignment]

    class NewsCausalityBreakdownEnvelope(ToolEnvelope):
        full_data: Optional[NewsCausalityBreakdownData] = None  # type: ignore[assignment]

    class NewsLeadingIndicatorPerformanceEnvelope(ToolEnvelope):
        full_data: Optional[NewsLeadingIndicatorPerformanceData] = None  # type: ignore[assignment]

    class PerformanceMetricsEnvelope(ToolEnvelope):
        full_data: Optional[PerformanceMetricsData] = None  # type: ignore[assignment]

    class PositionDetailEnvelope(ToolEnvelope):
        full_data: Optional[PositionDetailData] = None  # type: ignore[assignment]

    class ProfitablePositionsEnvelope(ToolEnvelope):
        full_data: Optional[ProfitablePositionsData] = None  # type: ignore[assignment]

    class ResolvedPredictionsEnvelope(ToolEnvelope):
        full_data: Optional[ResolvedPredictionsData] = None  # type: ignore[assignment]

    class RoleAnalysisEnvelope(ToolEnvelope):
        full_data: Optional[RoleAnalysisData] = None  # type: ignore[assignment]

    class SectorCorrelationsEnvelope(ToolEnvelope):
        full_data: Optional[SectorCorrelationsData] = None  # type: ignore[assignment]

    class SignalCalibrationEnvelope(ToolEnvelope):
        full_data: Optional[SignalCalibrationData] = None  # type: ignore[assignment]

    class SignalDetailEnvelope(ToolEnvelope):
        full_data: Optional[SignalDetailData] = None  # type: ignore[assignment]

    class StrategyDistributionEnvelope(ToolEnvelope):
        full_data: Optional[StrategyDistributionData] = None  # type: ignore[assignment]

    class StructureCalibrationEnvelope(ToolEnvelope):
        full_data: Optional[StructureCalibrationData] = None  # type: ignore[assignment]

    class StructureValidationHistoryEnvelope(ToolEnvelope):
        full_data: Optional[StructureValidationHistoryData] = None  # type: ignore[assignment]

    class SymbolPeerLinksEnvelope(ToolEnvelope):
        full_data: Optional[SymbolPeerLinksData] = None  # type: ignore[assignment]

    class TradeOutcomesBulkEnvelope(ToolEnvelope):
        full_data: Optional[TradeOutcomesBulkData] = None  # type: ignore[assignment]

    # fetch 는 ChatGPT 커넥터 표준이라 envelope 을 쓰지 않는다.
    # full_data 로 감싸면 표준 클라이언트가 결과를 못 읽는다.
    FetchResult = FetchData

    # search 는 ChatGPT 커넥터 표준이라 envelope 을 쓰지 않는다.
    # full_data 로 감싸면 표준 클라이언트가 결과를 못 읽는다.
    SearchResult = SearchData

    # -----------------------------------------------------------------------
    # [2026-10-15] 엔티티 컨텍스트 tool (get_symbol_context · get_group_context · get_market_context)
    # 표준 envelope(요약 3종 + full_data)이 아니라 역피라미드 공통 스키마 — headline 이 응답 맨 앞이다
    # (`resource_response.wrap_entity_response`, 제안서 ED11). 선언 순서 = 응답 키 순서.
    # 제안서 §2-1 의 extra="forbid" 는 쓰지 않는다 — 같은 outputSchema 를 에러 envelope(mcp_error)도
    # 통과해야 해서(이 파일의 기존 계약과 동일). 키 집합·순서는 probe E4 가 강제한다.
    # -----------------------------------------------------------------------

    class EntityHeadline(BaseModel):
        model_config = ConfigDict(extra="allow")
        claim: str
        as_of: Optional[str] = None
        basis: Optional[str] = None
        horizon: Optional[str] = None
        calibration: Optional[str] = None
        source: Optional[str] = "OneQAZ"

    class EntityEvidence(BaseModel):
        model_config = ConfigDict(extra="allow")
        kind: Literal["statistical", "definitional", "tag", "hypothesis"]
        metric: str
        value: Optional[float] = None
        n: Optional[int] = None
        as_of: Optional[str] = None
        source_table: str
        validation: Optional[Dict[str, Any]] = None

    class EntityLink(BaseModel):
        model_config = ConfigDict(extra="allow")
        target: Dict[str, Any]
        relation: Literal["leads", "lags", "co_moves", "member_of", "has_member", "mentioned_in",
                          "affects", "affected_by"] = Field(description="Reads '<entity> <relation> <target>'")
        scope: Literal["direct", "group", "market_wide"] = "direct"
        evidence: EntityEvidence
        target_observation: Optional[Dict[str, Any]] = None

    class EntityGap(BaseModel):
        model_config = ConfigDict(extra="allow")
        section: str
        reason: Literal["not_linked", "data_unavailable", "insufficient_sample", "data_quality",
                        "stale", "not_applicable"]
        detail: str

    class EntityContextEnvelope(BaseModel):
        """Entity context response — inverted pyramid; the error envelope also validates here."""
        model_config = ConfigDict(extra="allow")
        headline: Optional[List[EntityHeadline]] = None
        meta: Optional[Dict[str, Any]] = None
        entity: Optional[Dict[str, Any]] = None
        resolution: Optional[Dict[str, Any]] = None
        self: Optional[Dict[str, Any]] = None  # pydantic v2: __init__(self, /, **data) 라 필드명 self 가 가능 (alias 는 스키마에 반영되지 않았다)
        upstream: Optional[List[EntityLink]] = None
        peers: Optional[List[EntityLink]] = None
        downstream: Optional[List[EntityLink]] = None
        news: Optional[List[EntityLink]] = None
        gaps: Optional[List[EntityGap]] = None
        disclaimer: str = Field(description="Canonical compliance disclaimer (always present)")
        is_investment_advice: Optional[Literal[False]] = Field(default=None)
        data_classification: Optional[Literal["research_information_only"]] = Field(default=None)
        request_id: str = Field(description="32-hex per-response correlation id")
        timestamp: str = Field(description="RFC3339 UTC, server build time")

    __all__ = [
        "EntityContextEnvelope", "EntityLink", "EntityEvidence", "EntityGap", "EntityHeadline",
        # [2026-09-23 P0-B] 실응답 기반 full_data 모델
        "FetchData",
        "CrossMarketCorrelationData",
        "FeatureGovernanceStateData",
        "FeatureGovernanceStatusData",
        "LedgerIntegrityData",
        "LosingPositionsData",
        "MacroCausalityGraphData",
        "MacroInfluenceMapData",
        "NewsCausalityBreakdownData",
        "NewsLeadingIndicatorPerformanceData",
        "PerformanceMetricsData",
        "PositionDetailData",
        "ProfitablePositionsData",
        "ResolvedPredictionsData",
        "RoleAnalysisData",
        "SectorCorrelationsData",
        "SignalCalibrationData",
        "SignalDetailData",
        "StrategyDistributionData",
        "StructureCalibrationData",
        "StructureValidationHistoryData",
        "SymbolPeerLinksData",
        "TradeOutcomesBulkData",
        "SearchData",
        "CrossMarketCorrelationEnvelope",
        "FeatureGovernanceStateEnvelope",
        "FeatureGovernanceStatusEnvelope",
        "LedgerIntegrityEnvelope",
        "LosingPositionsEnvelope",
        "MacroCausalityGraphEnvelope",
        "MacroInfluenceMapEnvelope",
        "NewsCausalityBreakdownEnvelope",
        "NewsLeadingIndicatorPerformanceEnvelope",
        "PerformanceMetricsEnvelope",
        "PositionDetailEnvelope",
        "ProfitablePositionsEnvelope",
        "ResolvedPredictionsEnvelope",
        "RoleAnalysisEnvelope",
        "SectorCorrelationsEnvelope",
        "SignalCalibrationEnvelope",
        "SignalDetailEnvelope",
        "StrategyDistributionEnvelope",
        "StructureCalibrationEnvelope",
        "StructureValidationHistoryEnvelope",
        "SymbolPeerLinksEnvelope",
        "TradeOutcomesBulkEnvelope",
        "FetchResult",
        "SearchResult",
        # Envelopes
        "ToolEnvelope", "ErrorEnvelope",
        "PredictionAccuracyEnvelope", "StrategyLeaderboardEnvelope",
        "TradeHistoryEnvelope", "PositionsEnvelope", "SignalsEnvelope",
        "DailyBriefEnvelope", "ExplainDecisionEnvelope",
        "TradeAnalysisEnvelope", "LatestDecisionsEnvelope",
        "LlmTradingDecisionsEnvelope", "ActivePredictionsEnvelope",
        "MonthlyAccuracyTrendEnvelope", "BacktestTuningStateEnvelope",
        # Payload models (exported for docs/codegen)
        "PredictionAccuracyData", "StrategyLeaderboardData", "TradeHistoryData",
        "PositionsData", "SignalsData", "DailyBriefData", "ExplainDecisionData",
        "TradeAnalysisData", "LatestDecisionsData", "LlmTradingDecisionsData",
        "ActivePredictionsData", "MonthlyAccuracyTrendData", "BacktestTuningStateData",
        # Shared building blocks
        "ValueSignals", "NextAction", "MarketStateNarrative", "MarketStateNarrativeBlock",
    ]

else:  # pragma: no cover — pydantic missing, schemas degrade gracefully
    ToolEnvelope = dict  # type: ignore[assignment,misc]
    ErrorEnvelope = dict  # type: ignore[assignment,misc]
    PredictionAccuracyEnvelope = dict  # type: ignore[assignment,misc]
    StrategyLeaderboardEnvelope = dict  # type: ignore[assignment,misc]
    TradeHistoryEnvelope = dict  # type: ignore[assignment,misc]
    PositionsEnvelope = dict  # type: ignore[assignment,misc]
    SignalsEnvelope = dict  # type: ignore[assignment,misc]
    DailyBriefEnvelope = dict  # type: ignore[assignment,misc]
    ExplainDecisionEnvelope = dict  # type: ignore[assignment,misc]
    TradeAnalysisEnvelope = dict  # type: ignore[assignment,misc]
    LatestDecisionsEnvelope = dict  # type: ignore[assignment,misc]
    LlmTradingDecisionsEnvelope = dict  # type: ignore[assignment,misc]
    ActivePredictionsEnvelope = dict  # type: ignore[assignment,misc]
    MonthlyAccuracyTrendEnvelope = dict  # type: ignore[assignment,misc]
    BacktestTuningStateEnvelope = dict  # type: ignore[assignment,misc]
    CrossMarketCorrelationEnvelope = dict  # type: ignore[assignment,misc]
    FeatureGovernanceStateEnvelope = dict  # type: ignore[assignment,misc]
    FeatureGovernanceStatusEnvelope = dict  # type: ignore[assignment,misc]
    LedgerIntegrityEnvelope = dict  # type: ignore[assignment,misc]
    LosingPositionsEnvelope = dict  # type: ignore[assignment,misc]
    MacroCausalityGraphEnvelope = dict  # type: ignore[assignment,misc]
    MacroInfluenceMapEnvelope = dict  # type: ignore[assignment,misc]
    NewsCausalityBreakdownEnvelope = dict  # type: ignore[assignment,misc]
    NewsLeadingIndicatorPerformanceEnvelope = dict  # type: ignore[assignment,misc]
    PerformanceMetricsEnvelope = dict  # type: ignore[assignment,misc]
    PositionDetailEnvelope = dict  # type: ignore[assignment,misc]
    ProfitablePositionsEnvelope = dict  # type: ignore[assignment,misc]
    ResolvedPredictionsEnvelope = dict  # type: ignore[assignment,misc]
    RoleAnalysisEnvelope = dict  # type: ignore[assignment,misc]
    SectorCorrelationsEnvelope = dict  # type: ignore[assignment,misc]
    SignalCalibrationEnvelope = dict  # type: ignore[assignment,misc]
    SignalDetailEnvelope = dict  # type: ignore[assignment,misc]
    StrategyDistributionEnvelope = dict  # type: ignore[assignment,misc]
    StructureCalibrationEnvelope = dict  # type: ignore[assignment,misc]
    StructureValidationHistoryEnvelope = dict  # type: ignore[assignment,misc]
    SymbolPeerLinksEnvelope = dict  # type: ignore[assignment,misc]
    TradeOutcomesBulkEnvelope = dict  # type: ignore[assignment,misc]
    FetchResult = dict  # type: ignore[assignment,misc]
    SearchResult = dict  # type: ignore[assignment,misc]
    EntityContextEnvelope = dict  # type: ignore[assignment,misc]
    __all__ = []
