# Changelog

All notable changes to this project will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/).

## [0.3.0] - 2026-04-20

### Fixed
- **Critical**: package imports — previously shipped with `from mcps.*` imports that only worked inside the private monorepo. Users installing from PyPI since 0.1.0 got `ModuleNotFoundError` at runtime. All 10 files now import from `oneqaz_trading_mcp.*`.
- `resources/__init__.py` and `tools/__init__.py` were empty — populated with `register_*` exports so `server.py` can wire handlers.
- Middleware consumed the request body without restoring it — Starlette's `BaseHTTPMiddleware` internally caches body reads, so downstream FastMCP handlers continue to work. Reordered middleware so body-parse runs once before tier gate + rate limiter.

### Added
- **Trust Layer (13 tools)** — previously documented as "4 tools" in 0.2.0. The public server now exposes the full set: `get_prediction_accuracy`, `get_backtest_tuning_state`, `get_monthly_accuracy_trend`, `get_news_leading_indicator_performance`, `get_news_causality_breakdown`, `get_feature_governance_state`, `get_structure_calibration`, `get_structure_validation_history`, `get_strategy_leaderboard`, `get_active_predictions`, `get_macro_influence_map`, `get_cross_market_correlation`, `get_role_analysis`.
- **Portfolio tools (11)**: position queries, trade history, analytics, LLM decisions (all gated to `internal` tier).
- **Pro tools (3)**: `get_signals`, `get_signal_detail`, `explain_decision`.
- **Tier gate** (`tier_registry.py`) — single source of truth mapping each tool/resource to its required tier (`free`/`pro`/`internal`). Enforced in middleware before the handler runs, returning HTTP 403 with JSON-RPC `error.code = -32001`. Identical policy between this public package and the internal OneQAZ deployment.
- **`MCP_TIER_RESOLVER` env var** — self-hosters can plug in their own `module:function` to resolve an API key to a tier. Without a resolver, all callers are `free`.
- **Analytics writer** (`analytics.py`) — per-request SQLite audit log with client classification (`claude-code` / `chatgpt` / `cursor` / ...), intent tagging (`trust_eval` / `decision_support` / `data_query` / `discovery`), and 30-min session bucketing. Path configurable via `MCP_ANALYTICS_DB`.
- **Actionable error responses** — `mcp_error(MCPErrorCode.X, reason, action=MCPErrorAction.RETRY)` builder so errors carry an `action` + `fallback_tool` + `action_value` that an AI consumer can react to.
- **`wrap_with_ai_summary`** — response wrapper producing `{ai_summary, ai_summary_generated_at, ai_summary_ttl_seconds, full_data}` shape for consistent AI-agent consumption.

### Changed
- README rewritten — documents actual 27 tools + 34 resource endpoints across three tiers, replacing the stale "19 Resources + 4 Tools" claim.
- `pyproject.toml` Homepage URL corrected from `github.com/oneqaz` to `github.com/oneqaz-trading` (matches the repo org).
- `.gitattributes` added — normalizes line endings to `LF` across OS to prevent CRLF churn on Windows clones.

## [0.1.5] - 2026-04-03

### Changed
- README rewritten — positioned as "the context layer for financial AI"
- Added "Why OneQAZ" comparison table and AI developer-focused use cases
- Quick Start reordered: Live API first, local install second
- Added SECURITY.md, CHANGELOG.md, CONTRIBUTING.md, CODE_OF_CONDUCT.md
- Tool annotations added (readOnlyHint, title) for MCP spec compliance
- pyproject.toml status updated to Production/Stable

### Improved
- server.json description aligned with new positioning

## [0.1.4] - 2026-04-02

### Added
- Market Coverage section in README (exchanges and symbol universe)
- Rate limiting: 1,500 requests/day + 30 requests/min per IP
- Disclaimer section (not financial advice)
- Live API connection option in README

### Changed
- Repository URL moved to oneqaz-trading org

## [0.1.3] - 2026-04-01

### Added
- 5 MCP prompt templates for guided usage

### Fixed
- PROJECT_ROOT alias in config.py — fixes ImportError on fresh install

## [0.1.2] - 2026-03-31

### Added
- Use Cases, Sample Response, and conversation examples in README

## [0.1.1] - 2026-03-31

### Fixed
- mcp-name ownership tag for MCP Registry
- Build backend configuration

## [0.1.0] - 2026-03-31

### Added
- Initial release
- 19 Resources: global regime, market status, market structure, indicators, signals, external context, unified context, cross-market analysis
- 4 Tool types: trade history, positions, signals, trading decisions
- Multi-market support: crypto (Bithumb), US stocks (S&P 500), Korean stocks (KOSPI 200)
- SQLite-based data layer with configurable paths
- Caching with configurable TTL per resource type
- `_llm_summary` field on every response for AI agent consumption
- CLI: `oneqaz-trading-mcp init` (sample data) and `oneqaz-trading-mcp serve`
- Docker support
- Live API at api.oneqaz.com/mcp
