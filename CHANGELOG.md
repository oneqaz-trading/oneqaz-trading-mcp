# Changelog

All notable changes to this project will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Fixed

- `/privacy`: the page body is wrapped in `<!--email_off-->` so Cloudflare Email Address
  Obfuscation no longer replaces the privacy-officer and processor contact addresses with
  "[email protected]" for readers without JavaScript. No effect when the page is not served
  through Cloudflare. Ships with the next release.
- `/terms`: replaced the 2026-07-08 Terms of Use with the Terms of Service published at
  <https://oneqaz.com/terms> (byte-identical; EN/KO; site design). The live-only clauses are kept
  (derived analytics / no market-data licence, no reconstruction, no redistribution, fees & metering,
  governing law — Republic of Korea). `/privacy` gains a Terms footer link; `/keys` wording now matches
  the privacy policy (per-key usage attribution via a one-way fingerprint).

### Registry metadata

- `server.json` merged with the monorepo copy and bumped to server version `1.2.0` (the registry's current
  latest is `1.0.0` from 2026-04-07; a lower version would not become latest): adds the hosted
  `streamable-http` remote (`https://api.oneqaz.com/mcp`, optional `X-API-Key`) and `websiteUrl`, keeps the
  PyPI package entry (`0.4.4`, PostgreSQL env vars), new description. Validated against the
  2025-12-11 server schema. Not yet published.

### Documentation

- README brought in line with the live server (checked 2026-10-07):
  coverage ~1,300 symbols (KR universe is KOSPI 200 + KOSDAQ 150, not KOSPI 200 only) plus 40+
  macro instruments; Trust Layer walkthrough now reads accuracy against the persistence and
  majority-class baselines (`skill_ci_95`, `beats_majority_baseline`, `n_effective`) instead of
  raw hit rates; news lead-time tools are documented as `UNVERIFIED` (metrics withheld) rather
  than as pre-news detection; the ledger hash chain's scope is stated precisely (macro-regime
  predictions, sealed per UTC day — tamper-evident after the day closes, not a pre-outcome
  commitment); macro and market structure described as parallel overlays, not a serial chain;
  `_value_signals` URLs point at `api.oneqaz.com`; missing `market://{market_id}/positions`
  template and `X-RateLimit-Daily-Limit` header added; the Trust Layer sample is replaced by one
  tested against the live endpoint with fastmcp 2.14 and 4.0 (the old sample used a
  non-existent `mcp.Client` API and illustrative accuracy figures).

## [0.4.4] - 2026-10-07

### Changed — privacy policy completeness (Korea PIPA)

- `/privacy` now carries the items required by Korea's Personal Information Protection Act
  (Art. 30, Enforcement Decree Art. 31) and the PIPC 2025.4 drafting guideline, in English and
  Korean: legal basis, per-item retention (incl. backups), destruction procedure and method,
  provision to third parties, outsourcing and overseas transfer (Cloudflare, GitHub Pages,
  Bunny Fonts — task, country, items, timing, retention, contact, how to refuse), security
  measures, automatic collection devices (no cookies), data-subject rights and how to exercise
  them, privacy officer, remedies for infringement, and items that do not apply.
- `PRIVACY.md` §1 is now a summary table that links to the full hosted policy.
- No other code changes.

## [0.4.3] - 2026-10-07

### Changed — `/privacy` page design

- The bundled `/privacy` page now uses the oneqaz.com design system: shared theme (light/dark
  following the device setting), navigation, footer, and an **EN/KO language toggle** with a full
  Korean translation (previously English only, with a one-line Korean subtitle).
- One HTML source now serves both <https://oneqaz.com/privacy> and <https://api.oneqaz.com/privacy>
  byte-for-byte, so links and assets are absolute.
- Policy content is unchanged from 0.4.2. No other code changes.

## [0.4.2] - 2026-10-07

### Changed — privacy policy

- The policy served at `/privacy` (and published at <https://api.oneqaz.com/privacy>) is updated:
  - **Retention is now explicit.** IP addresses and IP-derived session keys are kept for up to
    12 months, then irreversibly replaced. The previous text ("retained only as long as needed, then
    aggregated or deleted") did not describe an actual deletion process.
  - **Newly disclosed fields** on the hosted service: a one-way API-key fingerprint and the resolved
    tier, HTTP status and JSON-RPC error codes, result-shape metadata (rows returned / total available
    / truncated / empty), the MCP protocol version, the `Accept` header, and request payload size.
    The `search` keyword (≤ 60 characters) is now named explicitly instead of being implied by
    "whitelisted parameters".
  - The previous "API key — used only to determine your tier" wording is corrected: usage is also
    attributed per key via the fingerprint (never the key itself).

### Added

- `PRIVACY.md` — mirror of the hosted policy plus self-hosting notes: no telemetry to OneQAZ, the two
  outbound calls the server makes, request logs go to your own PostgreSQL, and the bundled `/privacy`
  route describes OneQAZ's hosted service (replace it for your own deployment).

No other code changes: 0.4.2 is 0.4.1 plus the updated policy page.

## [0.4.1] - 2026-10-06

### Security

- Removed a hard-coded PostgreSQL password default from
  `oneqaz_trading_mcp/shared/db/config.py`. The 0.4.0 monorepo sync copied it
  verbatim. The password now comes only from the `PG_PASSWORD` environment
  variable. When it is unset, the connection fails at authentication instead of
  silently using a built-in value.
- The credential embedded in 0.4.0 was revoked on the OneQAZ side, and 0.4.0 is
  yanked on PyPI. No other code changes: 0.4.1 is 0.4.0 plus this fix.

## [0.4.0] - 2026-08-23

### Changed — the package is now a faithful production mirror

- **Source policy**: `src/oneqaz_trading_mcp/` is now synced 1:1 from the
  OneQAZ monorepo's `mcps/` module — the exact code serving
  `api.oneqaz.com/mcp` — by `scripts/sync_from_monorepo.py` (import prefixes
  rewritten, one public-only patch: the `MCP_TIER_RESOLVER` hook). The
  previous curated fork (English-translated docstrings, SQLite demo backend)
  is retired: it made every sync a manual porting project, which is how this
  repo froze between April and August 2026 while the hosted surface kept
  evolving.
- **BREAKING — PostgreSQL-only**: the local SQLite demo backend is gone.
  Self-hosting requires a OneQAZ-compatible PostgreSQL (`DB_BACKEND=postgres`
  + `PG_*` env vars; a read-only role suffices). `oneqaz-trading-mcp init`
  now prints a deprecation notice and exits. Note: no working install is
  broken by this — PyPI 0.2.0 could not run at all (`from mcps.*` import bug,
  see 0.3.0 notes), and 0.3.0 was never uploaded.
- New dependency: `psycopg[binary,pool]>=3.1`.
- Removed dead public-only modules: `init_db.py`, `cache.py`, `response.py`.

### Added — hosted-surface catch-up (tool count 32 → 39)

- **Verifiable prediction ledger (3)** — `get_ledger_integrity` (daily
  SHA-256 hash chain over all created/resolved prediction rows, canonical
  recipe published for third-party recomputation), `get_resolved_predictions`
  (row-level created→resolved→outcome lifecycle), `get_trade_outcomes_bulk`
  (cursor-paginated prediction→trade→outcome export).
- **Portfolio analytics (1)** — `get_performance_metrics` (MDD / Sharpe /
  Sortino / Calmar per market + account type, optional daily curve).
- **Signal calibration (1)** — `get_signal_calibration` (reliability diagram
  per confidence bucket + ECE).
- **ChatGPT connector standard (2)** — `search` + `fetch`.
- Plus four months of production fixes to the existing 32 tools (universe
  PG routing, latency fixes, confidence_calibrated surfacing, honest-metric
  revisions POLICY 08-11/08-12/08-18/08-20, structured actionable errors).
- Smoke-verified from a clean venv install: 39 tools / 17 static resources /
  17 templates register, and live PG calls return real data
  (`get_positions`, `get_ledger_integrity`, `get_performance_metrics`).

## [0.3.1-unreleased] - 2026-05-08 (docs only, folded into 0.4.0)

### Added — Specialist Positioning Layer (2026-05-07/08)

- **`get_daily_brief`** (1 new tool, 32 total) — single-call market overview combining macro regime + top 5 strong signals + yesterday's paper-trading P&L + active prediction count + Korean narrative. Natural first call for "what's the market doing today?". 5-minute cache.
- **`_next_actions`** (response field) — every tool response now carries up to 3 response-data-aware next-tool recommendations with `intent`, `tool`, `args` (pre-filled), `rationale`, `priority`. Driven by **response data** (e.g. weak category detection only fires when `accuracy < 0.5 + samples >= 30`), not a static dependency graph. LLM call count: 0.
- **`_followup_questions_for_user`** (response field) — Korean natural-language questions the AI can quote verbatim to the end-user. Clicking one triggers the next call. Max 3 entries.
- **`market://meta/discovery`** v2.0 — dynamic catalog via FastMCP introspection (no static `if/else` to drift), with `data_freshness` PG probe (5 source tables), `positioning` block (specialist_domains, trust_principles, what_we_do_NOT_provide, philosophy), `template_resources[*].example` field with copy-paste-ready URIs.
- **Layer correlations (4 new tools)** — Stage 2 cross-asset structure: `get_sector_correlations_tool`, `get_macro_causality_graph_tool`, `get_symbol_peer_links_tool`, `get_feature_governance_status_tool`.
- **Admin chain-depth + client-intent-matrix metrics** — server-side measurement of `_next_actions` effect (deep_chain_pct ≥ 3 = key KPI) and per-client × per-intent breakdown.

### Changed

- Tool count: **27 → 32**. Resource shape: **17 static + 17 templates** (was: "34 endpoints").
- README rewritten — documents conversation hooks, dynamic discovery, layer correlations, daily_brief.

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
