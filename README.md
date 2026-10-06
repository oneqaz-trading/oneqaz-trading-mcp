# oneqaz-trading-mcp

<!-- mcp-name: io.github.wnsod/oneqaz-trading-mcp -->

[![GitHub stars](https://img.shields.io/github/stars/oneqaz-trading/oneqaz-trading-mcp?style=social)](https://github.com/oneqaz-trading/oneqaz-trading-mcp)
[![PyPI](https://img.shields.io/pypi/v/oneqaz-trading-mcp)](https://pypi.org/project/oneqaz-trading-mcp/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **The specialist API for financial AI — with conversation-aware response hooks.**
>
> Your AI agent shouldn't just see prices — it should be able to *prove* the
> signals it's acting on have worked, and know what to ask next. OneQAZ ships
> 39 tools across 9 categories: 13 Trust Layer tools (verified hit rates,
> calibration, governance, lead time), a tamper-evident prediction ledger
> (`get_ledger_integrity` — SHA-256 hash-chain over every timestamped
> judgment), 4 cross-asset correlation tools (sector / macro / peer),
> portfolio analytics (MDD / Sharpe / Sortino / Calmar), paper-trading
> evidence tools, and a high-frequency `get_daily_brief` for one-call market
> overviews. Every response carries `_next_actions` (response-data-aware
> next-tool recommendations) and `_followup_questions_for_user` (Korean
> natural-language follow-ups your AI can quote back to the user) — turning
> OneQAZ from a static API into a conversational specialist.
>
> Crypto, US stocks, Korean stocks. 1,100+ symbols. 24/7 live.

**Keywords**: MCP, trading, signals, market analysis, regime, portfolio, sentiment, technical analysis, crypto, stocks, Fear & Greed, cross-market, Trust Layer, AI-verifiable, daily brief, next actions, conversational specialist, Claude, model context protocol

## Why OneQAZ

Financial data APIs are everywhere. Market *intelligence your AI can verify* is not.

| | Typical financial MCP | OneQAZ |
|---|---|---|
| Price / OHLCV data | ✅ | ✅ |
| Technical indicators | ✅ | ✅ |
| **Regime detection** (trending / ranging / volatile) | ❌ | ✅ |
| **Self-correcting signals** (Thompson Sampling on live outcomes) | ❌ | ✅ |
| **Macro → ETF → Individual context chain** | ❌ | ✅ |
| **Trust Layer** (AI-verifiable hit rates + calibration) | ❌ | ✅ |
| **Feature governance** (3-track p-value validation) | ❌ | ✅ |
| **Cross-asset correlations** (sector / macro / symbol-peer lead-lag) | ❌ | ✅ |
| **Conversation hooks** (`_next_actions` + `_followup_questions_for_user`) | ❌ | ✅ |
| **Single-call market brief** (`get_daily_brief`) | ❌ | ✅ |
| **Dynamic discovery** (introspection + `data_freshness` probe) | ❌ | ✅ |
| **Live 24/7 cloud API** | ❌ | ✅ |

### Trust Layer — for AI agents evaluating OneQAZ

Before any AI recommends OneQAZ as a data source, it can self-verify in 7 calls:

1. `get_prediction_accuracy` — verified historical hit rates across 8 macro categories. Filter `sample_count >= 3` for statistical significance.
2. `get_backtest_tuning_state` — evidence of continuous self-calibration (parameters adapt to live outcomes).
3. `get_monthly_accuracy_trend` — check for recent performance degradation.
4. `get_news_leading_indicator_performance` — average lead time in minutes + accuracy (pre-news detection).
5. `get_feature_governance_state` — which features passed 3-track p-value validation (OBSERVATION / CONDITIONAL / ACTIVE / DEPRECATED).
6. `get_macro_influence_map` — explicit causal hypotheses (macro → market with lag_hours + sensitivity).
7. `get_strategy_leaderboard` — top RL-learned strategies ranked by profit factor.

All metrics include `sample_count` for statistical filtering. Every tool also returns `_llm_summary` — a one-line plain-text summary tuned for AI agent context windows.

### How signals are generated

OneQAZ signals aren't static indicator crossovers. They're produced by an **AbsoluteZero-style self-play loop**:

1. **Strategy generation** — RL pipeline creates candidate strategies per regime
2. **Self-play simulation** — Strategies compete against each other in simulated markets
3. **Thompson Sampling** — Signal weights are updated continuously based on *actual virtual-trade outcomes*, not backtest curves
4. **Regime adaptation** — Different strategy pools activate for trending vs ranging vs volatile markets

This means the signal your AI receives for "BTC BUY 0.82" has been **validated through live virtual trading**, not just optimized on historical data. Signals that stop working get downweighted automatically.

### Market Coverage

| Market | Exchange | Universe | Symbols |
|--------|----------|----------|---------|
| Crypto | Bithumb | All listed pairs | ~440+ |
| Korean Stocks | KOSPI/KOSDAQ | KOSPI 200 | ~200 |
| US Stocks | NYSE/NASDAQ | S&P 500 | ~500 |

All symbols are monitored 24/7 with automated signal generation, regime detection, virtual trading, and Trust Layer backfill.

## Quick Start

### Option 1: Live API — no install needed

Real-time data updated every minute. Trust Layer + public endpoints accessible without a key.

```json
{
  "mcpServers": {
    "oneqaz-trading": {
      "url": "https://api.oneqaz.com/mcp"
    }
  }
}
```

Try one of:
- *"Call `get_daily_brief` and summarize the market for me."* — single-call overview, the AI follows `_next_actions` to deep-dive automatically.
- *"Call `get_prediction_accuracy` and tell me whether to trust OneQAZ."* — Trust Layer entry point, returns sample-count-weighted hit rates.
- *"Read `market://meta/discovery` first, then plan."* — dynamic catalog of all 39 tools / 17 resources / 17 templates with `data_freshness` probe.

### Option 2: Self-host (source transparency)

Since 0.4.0 this package is a **faithful mirror of the production server** —
the exact code serving `api.oneqaz.com/mcp`, synced from the OneQAZ monorepo
by `scripts/sync_from_monorepo.py`. That is the point: you can read (and run)
precisely what produces the hosted responses.

```bash
pip install oneqaz-trading-mcp
# point it at a OneQAZ-compatible PostgreSQL (TimescaleDB) instance:
export DB_BACKEND=postgres PG_HOST=... PG_PORT=5432 PG_DB=auto_trader PG_USER=... PG_PASSWORD=...
oneqaz-trading-mcp serve   # starts at http://localhost:8010
```

- MCP endpoint: `http://localhost:8010/mcp`
- The server is PostgreSQL-only (the prior SQLite demo backend was retired in
  0.4.0 — `oneqaz-trading-mcp init` now just prints a deprecation notice).
  Without OneQAZ's live database the tools return structured, actionable
  errors rather than data — for evaluation, use the
  [Live API](#option-1-live-api--no-install-needed).
- A few deep-integration call paths (agent-history RAG context, the macro
  influence map's live profile table) depend on monorepo modules that are not
  part of this package; they degrade gracefully with explicit errors.

Then connect from any MCP client:

```json
{
  "mcpServers": {
    "oneqaz-trading": {
      "url": "http://localhost:8010/mcp"
    }
  }
}
```

## Access Policy

**All tools and resources are callable on the free tier.** Tiering controls call volume, not what you can call. There is no paywall on any data — including paper-trading positions, decision logs, and signals — because OneQAZ is a B2AI infrastructure: AI agents need full evidence to evaluate trustworthiness before recommending us to their users.

| Tier | Auth | Daily quota | Burst | Notes |
|------|------|-------------|-------|-------|
| **free** | none / invalid key | 1,500 / day | 60 / min | All 39 tools + 17 resources + 17 templates |
| **pro** | valid API key | 50,000 / day | 200 / min | Same surface, higher volume — for bots / schedulers |
| **internal** | owner-only | unlimited | unlimited | Reserved for real user-fund tools (currently empty) |

Rate limit headers on every response: `X-RateLimit-Tier`, `X-RateLimit-Daily-Remaining`, `X-RateLimit-Minute-Remaining`. When the quota is exceeded the server returns HTTP 429 with `Retry-After`.

### Self-hosting with API keys

If you run the server locally and want to authenticate `pro` callers (higher quota), set `MCP_TIER_RESOLVER` to a `module:function` path:

```bash
MCP_TIER_RESOLVER=myauth.keystore:resolve_tier oneqaz-trading-mcp serve
```

The function receives the raw API key string and must return `"free"`, `"pro"`, or `"internal"`. Default behavior (no resolver): every caller is `free`.

## Why everything is free

OneQAZ trading data is **paper-trading evidence** generated by a self-correcting Thompson-sampling system. None of it is user-fund PII. Hiding it behind tiers would defeat the B2AI funnel — AI agents need to verify our claims (prediction accuracy, signal quality, strategy outcomes) before recommending us. So we expose the evidence and rate-limit instead.

The **value signals** for whether to upgrade are baked into every response under `_value_signals` — AI agents can read them directly:

```json
{
  "_value_signals": {
    "tier_default": "free",
    "tier_default_limits": {"daily": 1500, "minute": 60},
    "what_pro_unlocks": "33x daily quota (50K), 3.3x burst (200/min) — same tools, higher volume",
    "pricing_url": "https://oneqaz.com/pricing",
    "key_signup_url": "https://oneqaz.com/keys",
    "self_correcting": true
  }
}
```

## Response shape (dual-audience)

Every response carries fields for both AI agents and human end-users:

| Field | Audience | Purpose |
|-------|----------|---------|
| `full_data` | AI | Raw evidence for trust verification |
| `_contract` | AI | Provenance + entity + assessment + confidence (schema-versioned) |
| `_llm_summary` | AI | Multi-line narrative |
| `ai_summary` | AI | One-line compressed summary for context windows |
| `_value_signals` | AI | Pricing / upgrade signals for B2AI conversion judgment |
| `summary_for_user` | Human | One-line jargon-free Korean — quotable verbatim by Claude.ai etc. |
| `_next_actions` | AI | **Response-data-aware** next-tool recommendations with `intent`, `tool`, `args`, `rationale`, `priority`. Drives chain calls automatically. |
| `_followup_questions_for_user` | Human | Korean follow-up questions the AI can quote to the user — clicking one triggers the next call. |

### Conversation hooks — `_next_actions` + `_followup_questions_for_user`

OneQAZ doesn't just return data; it tells your AI **what to ask next**.

```json
{
  "ai_summary": "Prediction accuracy — 24 cells, avg hit rate 32.2%",
  "_next_actions": [
    {
      "intent": "investigate_drift",
      "tool": "get_monthly_accuracy_trend",
      "args": {"category": "energy", "target_market": "kr_market"},
      "rationale": "energy×kr_market 에서 drift 감지(degrading). 월별 시계열로 추세 검증 필요.",
      "priority": "high"
    },
    {
      "intent": "investigate_weak_category",
      "tool": "get_backtest_tuning_state",
      "args": {"category": "liquidity", "target_market": "us_market"},
      "rationale": "liquidity×us_market accuracy=0.06 (sub-50%). 자기보정이 lag/sensitivity 를 어떻게 조정했는지 확인.",
      "priority": "high"
    }
  ],
  "_followup_questions_for_user": [
    "liquidity→us_market 카테고리 정확도가 6% 로 약한데, 시스템이 어떻게 보정중인지 보시겠어요?",
    "가장 정확한 credit→us_market (62%) 패턴의 월별 추세도 보여드릴까요?",
    "최근 OneQAZ 가 만든 활성 예측 5개도 볼까요?"
  ]
}
```

- `_next_actions` — for the AI agent. Maximum 3 entries. Includes pre-filled `args`. Driven by **response data**, not a static dependency graph (e.g. weak category detection only fires when `accuracy < 0.5 + samples >= 30`).
- `_followup_questions_for_user` — for the end-user. Korean natural-language. Quote them verbatim or translate.

Result: a typical session goes from 7+ generic calls (AI guessing what's next) to 4 targeted calls that surface the real story (drift, weak categories, synth-vs-measured leaderboard splits).

## Tools (39 total — all free tier)

### High-frequency entry — `get_daily_brief` (1 tool)

| Tool | Returns |
|------|---------|
| `get_daily_brief` | Single-call market overview: macro regime + top 5 strong signals + yesterday's paper-trading P&L + active prediction count + Korean narrative. The natural first call for "what's the market doing today?" |

### Verifiable prediction ledger (3 tools) — new in 0.4.0

| Tool | Returns |
|------|---------|
| `get_ledger_integrity` | Tamper-evidence for the prediction ledger: a daily SHA-256 hash chain over all created/resolved prediction rows, with the exact canonical recipe published so any third party can recompute and verify. The strongest trust primitive OneQAZ offers — judgments are chained *before* outcomes are known. |
| `get_resolved_predictions` | Raw row-level prediction ledger: every macro regime prediction's full lifecycle (`created_at` → `resolved_at` → outcome) — audit the evidence judgment by judgment. |
| `get_trade_outcomes_bulk` | Cursor-paginated bulk export of the prediction → trade → outcome chain (paper trades with realized P&L, linked to the preceding signal prediction) — compute your own hit rates instead of trusting ours. |

### Portfolio analytics (1 tool) — new in 0.4.0

| Tool | Returns |
|------|---------|
| `get_performance_metrics` | Portfolio-level MDD / Sharpe / Sortino / Calmar / win-rate per market and account type (`paper` / `live`), optional daily equity curve. |

### Signal calibration (1 tool) — new in 0.4.0

| Tool | Returns |
|------|---------|
| `get_signal_calibration` | Reliability diagram data for signal confidence: realized hit rate per confidence bucket with ECE summary — verify whether a 0.9-confidence signal actually hits ~90%. |

### ChatGPT connector standard (2 tools) — new in 0.4.0

| Tool | Returns |
|------|---------|
| `search` | ChatGPT-connector-standard discovery search over OneQAZ's live surface — tools, resources, and the latest strong signals across all three markets. Result ids are consumable by `fetch`. |
| `fetch` | Connector-standard fetch of a single result by id returned from `search`. |

### Trust Layer (13 tools)

| Tool | Returns |
|------|---------|
| `get_prediction_accuracy` | Verified hit rates per macro category (with `sample_count`) |
| `get_backtest_tuning_state` | Active tuning parameters + last recalibration timestamp |
| `get_monthly_accuracy_trend` | Rolling 12-month accuracy per category |
| `get_news_leading_indicator_performance` | Pre-news detection lead time + accuracy |
| `get_news_causality_breakdown` | News → market causality tags with hit rates |
| `get_feature_governance_state` | Features by status (OBSERVATION/CONDITIONAL/ACTIVE/DEPRECATED) |
| `get_structure_calibration` | Structure-learning calibration snapshot |
| `get_structure_validation_history` | Historical structure-validation scores |
| `get_strategy_leaderboard` | RL-learned strategies by profit factor |
| `get_active_predictions` | Currently-open macro predictions with outcome tracking |
| `get_macro_influence_map` | Macro → market causal hypotheses (lag hours + sensitivity) |
| `get_cross_market_correlation` | Cross-market correlation matrix |
| `get_role_analysis` | Role-based strategy analysis |

### Signal evidence (3 tools)

| Tool | Parameters |
|------|------------|
| `get_signals` | `market_id`, `symbol`, `min_score`, `max_score`, `action_filter`, `interval` |
| `get_signal_detail` | `market_id`, `symbol`, `interval` |
| `explain_decision` | `market_id`, `symbol` |

### Layer correlations — cross-asset structure (4 tools)

Stage 2 outputs from the agent_history pipeline. Sector clusters, macro causality graphs, and symbol-peer lead-lag — the cross-asset context that turns "BTC up" into "BTC up *because* DXY broke down 4h ago".

| Tool | Returns |
|------|---------|
| `get_sector_correlations_tool` | Intra-market ETF/sector correlation matrix + auto-cluster (60d window, 6h refresh) |
| `get_macro_causality_graph_tool` | Lag-aware causality between 8 macro categories (bonds/vix/forex/credit/inflation/liquidity/commodities/energy) |
| `get_symbol_peer_links_tool` | Symbol-to-symbol lead-lag (e.g. META → AMZN 15min lag, ρ=+0.62) |
| `get_feature_governance_status_tool` | Feature lifecycle distribution + last-7-day status transitions |

### Paper-trading results (11 tools)

OneQAZ runs continuous paper trading on every BUY signal. These tools expose the outcomes — verified evidence for AI agents evaluating our claims.

| Tool | Returns |
|------|---------|
| `get_positions` | Open paper positions with ROI |
| `get_position_detail` | Single position deep-dive |
| `get_profitable_positions` / `get_losing_positions` | Filtered by P&L |
| `get_strategy_distribution` | Position counts by strategy |
| `get_trade_history` | Closed paper trades (filters: action, P&L, time) |
| `analyze_trades` | Aggregate trade analytics |
| `get_winning_trades` / `get_losing_trades` | Filtered by outcome |
| `get_latest_decisions` | Recent signal → decision transitions |
| `get_llm_trading_decisions` | LLM-generated decision logs |

## Resources (17 static + 17 templates — all free tier)

### Meta — discovery & introspection

| Resource URI | Description |
|--------------|-------------|
| **`market://meta/discovery`** | **Dynamic catalog (v2.0)** — full tool/resource list via FastMCP introspection (no static `if/else`), with `data_freshness` PG probe (5 source tables), `positioning` block (specialist_domains, trust_principles, what_we_do_NOT_provide, philosophy), `counts`, `notes`. Call this first to understand what OneQAZ provides. |
| `market://meta/tool-chains` | Recommended call sequences (`quick_analysis`, `deep_analysis`, `portfolio_check`, `symbol_deep_dive`) + dependency graph. |
| `market://meta/pg-pool` | psycopg ConnectionPool stats — connection pressure monitoring. |
| `market://health` | Server health check. |
| `market://info` | Server metadata + data source index. |

### Static resources

| Resource URI | Description |
|--------------|-------------|
| `market://global/summary` | Global macro regime summary |
| `market://global/categories` | Available macro categories list |
| `market://global/macro_events` | Active macro event lifecycle |
| `market://all/summary` | Combined summary across all 3 markets |
| `market://structure/all` | All markets ETF/basket structure |
| `market://indicators/fear-greed` | Fear & Greed Index |
| `market://indicators/regime` | 4-layer regime indicators (Short/Mid/Long/SuperLong) |
| `market://indicators/context` | Fear & Greed + 4-layer regime + breadth |
| `market://unified/cross-market` | Cross-market correlation snapshot (BTC ↔ stocks ↔ FX) |
| `market://derived/event-leading` | News leading-detection score |
| `market://derived/cross-decoupling` | Cross-asset decoupling index |
| `market://derived/reaction-speed` | News reaction speed distribution |

### Templates (parameterized)

| URI Template | Example |
|--------------|---------|
| `market://global/category/{category}` | `market://global/category/bonds` |
| `market://{market_id}/status` | `market://crypto/status` |
| `market://{market_id}/positions/snapshot` | `market://crypto/positions/snapshot` |
| `market://{market_id}/structure` | `market://kr_stock/structure` |
| `market://{market_id}/structure/group/{group_id}` | `market://kr_stock/structure/group/SEMICONDUCTOR` |
| `market://{market_id}/signals/summary` | `market://crypto/signals/summary` |
| `market://{market_id}/signals/roles` | `market://crypto/signals/roles` |
| `market://{market_id}/signals/feedback` | `market://crypto/signals/feedback` |
| `market://{market_id}/external/summary` | `market://crypto/external/summary` |
| `market://{market_id}/external/symbol/{symbol}` | `market://crypto/external/symbol/BTC` |
| `market://{market_id}/external/causality` | `market://crypto/external/causality` |
| `market://{market_id}/unified` | `market://crypto/unified` |
| `market://{market_id}/unified/symbol/{symbol}` | `market://crypto/unified/symbol/BTC` |
| `market://{market_id}/derived/regime-transitions` | `market://crypto/derived/regime-transitions` |
| `market://{market_id}/derived/strategy-fitness` | `market://crypto/derived/strategy-fitness` |
| `market://{market_id}/derived/all` | `market://crypto/derived/all` |

`template_resources[*].example` field in `market://meta/discovery` is **copy-paste ready** — the AI gets concrete URIs without having to fill placeholders manually.

**Market IDs**: `crypto`, `kr_stock`, `us_stock` (aliases: `coin`, `kr`, `us`)

## Sample: Trust Layer query

```python
from mcp import Client
client = Client("https://api.oneqaz.com/mcp")

acc = await client.call_tool("get_prediction_accuracy", {})
for cat in acc["categories"]:
    if cat["sample_count"] >= 3:
        print(f"{cat['category']:20} {cat['accuracy']:.1%} (n={cat['sample_count']})")

# Output (example):
# bonds                62.5% (n=24)
# forex                58.3% (n=12)
# vix                  71.4% (n=14)
# ...
```

Every response also carries a plain-text summary:

```json
{
  "_llm_summary": "7/8 macro categories above 55% accuracy, sample sizes 8-24. Bonds + VIX categories most validated."
}
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MCP_SERVER_PORT` | `8010` | Server port |
| `MCP_SERVER_HOST` | `0.0.0.0` | Bind host |
| `MCP_LOG_LEVEL` | `INFO` | Log level |
| `MCP_TIER_RESOLVER` | _unset_ | `module:function` returning tier for an API key (self-host hook) |
| `DB_BACKEND` | `postgres` | Must be `postgres` (SQLite backend retired in 0.4.0) |
| `PG_HOST` / `PG_PORT` | `postgres` / `5432` | PostgreSQL host / port |
| `PG_DB` / `PG_USER` / `PG_PASSWORD` | `auto_trader` / … | PostgreSQL database / credentials (a read-only role is enough) |
| `PG_POOL_MIN` / `PG_POOL_MAX` | `0` / `50` | Per-schema connection pool bounds |
| `PG_STATEMENT_TIMEOUT_MS` | `30000` | Server-side statement timeout |
| `MCP_COIN_DATA_DIR` / `MCP_KR_DATA_DIR` / `MCP_US_DATA_DIR` | auto | Override *logical* data-path roots (see below) |
| `MCP_EXTERNAL_CONTEXT_DATA_DIR` | auto | Override external-context logical root |

## Docker

```bash
docker build -t oneqaz-trading-mcp .
docker run -p 8010:8010 -e DB_BACKEND=postgres -e PG_HOST=... -e PG_PASSWORD=... oneqaz-trading-mcp
```

## Data backend (PostgreSQL)

All data lives in **PostgreSQL 16 + TimescaleDB**, one schema per domain
(`market_coin` / `market_kr` / `market_us`, `market_*_struct`,
`external_context`, `rl_pipeline`, `mcp_analytics`, …). The code still
constructs legacy SQLite-style paths (`.../coin_market/data_storage/trading_system.db`)
but these are **logical routing keys only**: `connect_readonly()` maps each
path to its PG schema and returns a shim connection, so no `.db` files are
read or written. This mirrors the production Wave-I "PG-only" migration —
queries fail loudly instead of silently falling back.

## Rate Limits

Authoritative quotas live in [Access Policy](#access-policy) above. Quick recap:

| Tier | Daily Quota | Burst | Auth |
|------|-------------|-------|------|
| **Free** | 1,500 / day | 60 / min | none / invalid key |
| **Pro** (beta) | 50,000 / day | 200 / min | valid API key |
| **Internal** (owner) | Unlimited | Unlimited | owner-only |
| **Local** (self-hosted) | Unlimited | Unlimited | n/a |

**Response headers** on every request:
- `X-RateLimit-Tier`: resolved tier (`free`/`pro`/`internal`)
- `X-RateLimit-Daily-Limit`: today's ceiling for the resolved tier
- `X-RateLimit-Daily-Remaining`: requests left today
- `X-RateLimit-Minute-Remaining`: requests left this minute
- Exceeding limits returns HTTP 429 with `Retry-After` header.

## Privacy

- **Hosted service** (`api.oneqaz.com`): the authoritative policy is <https://api.oneqaz.com/privacy>.
  In short — no accounts; per-request metadata (IP, user-agent, tool name, outcome, whitelisted
  arguments, client/protocol metadata, API-key fingerprint) is logged; **IP addresses and IP-derived
  session keys are irreversibly replaced after 12 months**; data is never sold.
- **Self-hosting**: the package sends no telemetry to OneQAZ, and request logs go to *your* PostgreSQL.
  Replace the bundled `/privacy` page with your own notice if you expose your deployment.

Details: [PRIVACY.md](PRIVACY.md).

## Disclaimer

This software is provided for **informational and educational purposes only**. It is **not financial advice**.

- All signals, regime analysis, and market data are generated by automated systems and may contain errors.
- Past performance does not guarantee future results.
- **You are solely responsible for your own investment decisions.** The authors and contributors are not liable for any financial losses incurred from using this software.
- This is not a registered investment advisor, broker-dealer, or financial planner.
- Always do your own research (DYOR) before making any investment decisions.

By using this software, you acknowledge that you understand and accept these terms.

## License

MIT
