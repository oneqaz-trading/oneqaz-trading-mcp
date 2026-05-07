# oneqaz-trading-mcp

<!-- mcp-name: io.github.wnsod/oneqaz-trading-mcp -->

[![GitHub stars](https://img.shields.io/github/stars/oneqaz-trading/oneqaz-trading-mcp?style=social)](https://github.com/oneqaz-trading/oneqaz-trading-mcp)
[![PyPI](https://img.shields.io/pypi/v/oneqaz-trading-mcp)](https://pypi.org/project/oneqaz-trading-mcp/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **The context layer for financial AI — with a self-verifying Trust Layer.**
>
> Your AI agent shouldn't just see prices — it should be able to *prove* the
> signals it's acting on have worked. OneQAZ ships 13 Trust Layer tools that
> expose verified hit rates, calibration history, governance state, and lead
> time — all sample-count-weighted so the AI can filter for statistical
> significance before recommending anything.
>
> Crypto, US stocks, Korean stocks. 1,100+ symbols. 24/7 live.

**Keywords**: MCP, trading, signals, market analysis, regime, portfolio, sentiment, technical analysis, crypto, stocks, Fear & Greed, cross-market, Trust Layer, AI-verifiable, Claude, model context protocol

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
| **Tier-gated access** (free/pro/internal surfaces) | ❌ | ✅ |
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

Ask Claude: *"Call `get_prediction_accuracy` and tell me whether to trust OneQAZ."*

### Option 2: Local (for development & testing)

```bash
pip install oneqaz-trading-mcp
oneqaz-trading-mcp init    # creates sample SQLite databases
oneqaz-trading-mcp serve   # starts at http://localhost:8010
```

- MCP endpoint: `http://localhost:8010/mcp`
- Local mode uses demo data. For live Trust Layer + signals, use the [Live API](#option-1-live-api--no-install-needed).

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
| **free** | none / invalid key | 1,500 / day | 60 / min | All 27 tools + 34 resources |
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

## Tools (27 total — all free tier)

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

## Resources (34 endpoints — all free tier)

| Resource URI | Description |
|--------------|-------------|
| `market://health` | Server health check |
| `market://global/summary` | Global macro regime summary |
| `market://global/category/{category}` | Per-category (bonds, commodities, forex, vix, credit, liquidity, inflation) |
| `market://global/categories` | Available categories list |
| `market://all/summary` | Combined summary across all markets |
| `market://indicators/fear-greed` | Fear & Greed Index |
| `market://indicators/context` | Combined market context |
| `market://structure/all` | All markets ETF/basket structure |
| `market://{market_id}/status` | Market regime + paper-trading performance |
| `market://{market_id}/structure` | Per-market structure analysis |
| `market://{market_id}/signals/summary` | 24h signal aggregation |
| `market://{market_id}/signals/feedback` | Signal pattern feedback |
| `market://{market_id}/signals/roles` | Role-based signal summary |
| `market://{market_id}/derived/*` | Derived signals (5 types) |
| `market://{market_id}/external/summary` | News / events / fundamentals |
| `market://{market_id}/external/symbol/{symbol}` | Per-symbol external context |
| `market://derived/*` | Cross-market derived signals |
| `market://{market_id}/positions/snapshot` | Current paper positions snapshot |
| `market://{market_id}/unified` | Market-level unified (positions + context) |
| `market://{market_id}/unified/symbol/{symbol}` | Per-symbol unified context chain |

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
| `MCP_TIER_RESOLVER` | _unset_ | `module:function` returning tier for an API key |
| `MCP_ANALYTICS_DB` | `<pkg>/data_storage/mcp_analytics.db` | Per-request audit log (SQLite) |
| `DATA_ROOT` | Auto-detect | Root directory for all data |
| `MCP_COIN_DATA_DIR` | `{DATA_ROOT}/market/coin_market/data_storage` | Crypto data directory |
| `MCP_KR_DATA_DIR` | `{DATA_ROOT}/market/kr_market/data_storage` | KR stock data directory |
| `MCP_US_DATA_DIR` | `{DATA_ROOT}/market/us_market/data_storage` | US stock data directory |
| `MCP_EXTERNAL_CONTEXT_DATA_DIR` | `{DATA_ROOT}/external_context/data_storage` | External context directory |
| `MCP_GLOBAL_REGIME_DATA_DIR` | `{DATA_ROOT}/market/global_regime/data_storage` | Global regime directory |

## Docker

```bash
docker build -t oneqaz-trading-mcp .
docker run -p 8010:8010 oneqaz-trading-mcp
```

## Data Directory Structure

```
{DATA_ROOT}/
├── market/
│   ├── global_regime/data_storage/
│   │   ├── global_regime_summary.json
│   │   └── {bonds,commodities,forex,vix,...}_analysis.db
│   ├── coin_market/data_storage/
│   │   ├── trading_system.db
│   │   ├── signals/{symbol}_signal.db
│   │   └── regime/market_structure_summary.json
│   ├── kr_market/data_storage/  (same structure)
│   └── us_market/data_storage/  (same structure)
└── external_context/data_storage/
    ├── coin_market/external_context.db
    ├── kr_market/external_context.db
    └── us_market/external_context.db
```

## Rate Limits

| Tier | Daily Quota | Burst |
|------|------------|-------|
| **Free** (no key) | 5,000 req/day | 60 req/min |
| **Pro** (API key, beta) | 50,000 req/day | 300 req/min |
| **Internal** (owner) | Unlimited | Unlimited |
| **Local** (self-hosted) | Unlimited | Unlimited |

**Response headers** on every request:
- `X-RateLimit-Tier`: resolved tier (`free`/`pro`/`internal`)
- `X-RateLimit-Daily-Remaining`: requests left today
- `X-RateLimit-Minute-Remaining`: requests left this minute
- Exceeding limits returns HTTP 429 with `Retry-After` header.

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
