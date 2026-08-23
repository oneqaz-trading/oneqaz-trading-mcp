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

        # ── Success-path fields (Optional for error compatibility) ──
        ai_summary: Optional[str] = Field(default=None, description="One-line AI-oriented summary (success path)")
        summary_for_user: Optional[str] = Field(default=None, description="One-line jargon-free Korean summary (success path)")
        ai_summary_generated_at: Optional[str] = Field(default=None, description="RFC3339 UTC")
        ai_summary_ttl_seconds: Optional[int] = Field(default=None, ge=0)
        full_data: Optional[Dict[str, Any]] = Field(default=None, description="Tool-specific payload (success path)")
        llm_summary: Optional[str] = Field(default=None, alias="_llm_summary")
        value_signals: Optional[ValueSignals] = Field(default=None, alias="_value_signals")
        next_actions: Optional[List[NextAction]] = Field(default=None, alias="_next_actions")
        followup_questions_for_user: Optional[List[str]] = Field(
            default=None, alias="_followup_questions_for_user"
        )
        market_state_narrative: Optional[MarketStateNarrative] = Field(
            default=None, alias="_market_state_narrative"
        )

        # ── Error-path fields (mirrors mcp_error()) ──
        error: Optional[bool] = Field(default=None, description="Set true on error responses")
        error_code: Optional[str] = Field(default=None, description="Stable error identifier; see mcp_error_policy.md")
        reason: Optional[str] = Field(default=None, description="Human-readable cause (error path)")
        action: Optional[str] = Field(default=None, description="Recommended client action (error path)")
        action_value: Optional[str] = Field(default=None)
        fallback_tool: Optional[str] = Field(default=None, description="Suggested fallback (error path)")
        fallback_note: Optional[str] = Field(default=None)
        retryable: Optional[bool] = Field(default=None, description="Whether the client should retry (error path)")

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


    class SignalsData(BaseModel):
        """`full_data` for get_signals."""
        model_config = ConfigDict(extra="allow")
        market_id: str
        signals: List[SignalRow] = Field(default_factory=list)
        filters: Dict[str, Any] = Field(default_factory=dict)


    class DailyBriefMacroRegime(BaseModel):
        model_config = ConfigDict(extra="allow")
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
        yesterday_trades: DailyBriefYesterdayTrades = Field(default_factory=DailyBriefYesterdayTrades)
        winning_count: int = 0
        losing_count: int = 0
        active_predictions_count: int = 0
        meta: DailyBriefMeta = Field(default_factory=DailyBriefMeta)


    class ExplainDecisionRecommendation(BaseModel):
        model_config = ConfigDict(extra="allow")
        verdict: Optional[str] = None
        text: Optional[str] = None


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


    __all__ = [
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
    __all__ = []
