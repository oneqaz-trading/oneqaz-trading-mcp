# oneqaz-trading-mcp

<!-- mcp-name: io.github.wnsod/oneqaz-trading-mcp -->

[![GitHub stars](https://img.shields.io/github/stars/wnsod/oneqaz-trading-mcp?style=social)](https://github.com/wnsod/oneqaz-trading-mcp)
[![PyPI](https://img.shields.io/pypi/v/oneqaz-trading-mcp)](https://pypi.org/project/oneqaz-trading-mcp/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **The context layer for financial AI.**
>
> Your AI agent shouldn't just see prices — it should understand
> what regime the market is in, which signals are actually working right now,
> and how macro flows down to individual assets.
>
> OneQAZ provides this as a single MCP endpoint.
> Crypto, US stocks, Korean stocks. 1,100+ symbols. 24/7 live.

**Keywords**: MCP, trading, signals, market analysis, regime, portfolio, sentiment, technical analysis, crypto, stocks, Fear & Greed, cross-market, Claude, model context protocol

## Why OneQAZ

Financial data APIs are everywhere. Market *intelligence* is not.

| | Typical financial MCP | OneQAZ |
|---|---|---|
| Price / OHLCV data | ✅ | ✅ |
| Technical indicators | ✅ | ✅ |
| **Regime detection** (trending / ranging / volatile) | ❌ | ✅ |
| **Self-correcting signals** (weighted by real outcomes) | ❌ | ✅ |
| **Macro → ETF → Individual context chain** | ❌ | ✅ |
| **Live 24/7 cloud API** | ❌ | ✅ |

### How signals are generated

OneQAZ signals aren't static indicator crossovers. They're produced by an **AbsoluteZero-style self-play loop**:

1. **Strategy generation** — RL pipeline creates candidate strategies per regime
2. **Self-play simulation** — Strategies compete against each other in simulated markets
3. **Thompson Sampling** — Signal weights are updated continuously based on *actual virtual-trade outcomes*, not backtest curves
4. **Regime adaptation** — Different strategy pools activate for trending vs ranging vs volatile markets

This means the signal your AI receives for "BTC BUY 0.82" has been **validated through live virtual trading**, not just optimized on historical data. Signals that stop working get downweighted automatically.

Every response includes an `_llm_summary` field optimized for AI consumption.

## What your AI gets

- **Regime detection**: Is the market trending, ranging, or volatile? Per-market and global
- **Self-correcting signals**: 1,100+ symbols scored by Thompson Sampling on actual trade outcomes
- **Macro context chain**: Global regime → bonds/forex/VIX/commodities → ETF/basket → individual symbol
- **External context**: News events, fundamentals, cross-market correlation — pre-processed for LLM consumption
- **19 Resources + 4 Tools**: Stateless HTTP, compatible with any MCP client
- **`_llm_summary` on every response**: Human-readable text summary optimized for AI agent context windows

### Market Coverage

| Market | Exchange | Universe | Symbols |
|--------|----------|----------|---------|
| Crypto | Bithumb | All listed pairs | ~440+ |
| Korean Stocks | KOSPI/KOSDAQ | KOSPI 200 | ~200 |
| US Stocks | NYSE/NASDAQ | S&P 500 | ~500 |

All symbols are monitored 24/7 with automated signal generation, regime detection, and virtual trading.

## Quick Start

### Option 1: Live API — no install needed

Real-time data, updated every minute.

```json
{
  "mcpServers": {
    "oneqaz-trading": {
      "url": "https://api.oneqaz.com/mcp"
    }
  }
}
```

Ask Claude: *"What's the current market regime?"*

### Option 2: Local (for development & testing)

Runs with demo data — great for building integrations, testing prompts, or exploring the API schema before connecting to live data.

```bash
pip install oneqaz-trading-mcp
oneqaz-trading-mcp init    # creates sample SQLite databases
oneqaz-trading-mcp serve   # starts at http://localhost:8010
```

- Swagger UI: `http://localhost:8010/docs`
- MCP endpoint: `http://localhost:8010/mcp`

> **Note**: Local mode uses static demo data. For live signals updated every minute, use the [Live API](#option-1-live-api--no-install-needed) above.

Then connect from Claude:

```json
{
  "mcpServers": {
    "oneqaz-trading": {
      "url": "http://localhost:8010/mcp"
    }
  }
}
```

## Use Cases

### 1. Give your AI agent market awareness

Connect OneQAZ and your agent understands market context without you building the pipeline:

```python
# Your agent reads regime + signals + macro in one call
context = mcp.read("market://crypto/unified")

# Or go granular
regime = mcp.read("market://crypto/status")          # what phase is the market in?
signals = mcp.call("get_signals", market_id="crypto", min_score=0.7)  # what's working now?
macro = mcp.read("market://global/summary")           # what's driving this from above?

# Feed to your agent's decision layer
prompt = f"""
  Regime: {regime}
  High-confidence signals: {signals}
  Macro context: {macro}

  Recommend portfolio action.
"""
```

### 2. Build a regime-aware trading system

Your AI reacts differently based on market state — no hardcoded rules:

```python
regime = mcp.read("market://us_stock/status")
structure = mcp.read("market://us_stock/structure")

if regime["regime"]["stage"] == "volatile":
    signals = mcp.call("get_signals", market_id="us_stock", action_filter="DEFENSIVE")
else:
    signals = mcp.call("get_signals", market_id="us_stock", min_score=0.7)
```

### 3. Cross-market macro→micro analysis

Trace how macro shifts flow into individual assets:

```python
# Macro layer
global_regime = mcp.read("market://global/summary")
bonds = mcp.read("market://global/category/bonds")

# Cross-market correlation
cross = mcp.read("market://unified/cross-market")

# Down to individual symbol with full context chain
symbol_ctx = mcp.read("market://us_stock/unified/symbol/NVDA")
```

### 4. Ask Claude directly

Already using Claude? Just connect and ask:

```
"What's the current market regime for crypto?"
"Show me the best performing positions in US stocks"
"Any macro risks I should know about?"
"Compare crypto vs US stock conditions"
```

## Sample Response

Reading `market://crypto/status` returns:

```json
{
  "market_id": "crypto",
  "regime": {
    "stage": "sideways_bullish",
    "score": 0.42,
    "confidence": 0.78
  },
  "positions": {
    "total": 5,
    "long": 4,
    "short": 1,
    "avg_roi": 3.2
  },
  "signals_24h": {
    "buy": 8,
    "sell": 3,
    "hold": 12,
    "avg_score": 0.65
  },
  "_llm_summary": "Crypto market is sideways_bullish. 5 active positions (avg ROI +3.2%). 8 BUY signals in last 24h."
}
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MCP_SERVER_PORT` | `8010` | Server port |
| `MCP_SERVER_HOST` | `0.0.0.0` | Bind host |
| `MCP_LOG_LEVEL` | `INFO` | Log level |
| `DATA_ROOT` | Auto-detect | Root directory for all data |
| `MCP_COIN_DATA_DIR` | `{DATA_ROOT}/market/coin_market/data_storage` | Crypto data directory |
| `MCP_KR_DATA_DIR` | `{DATA_ROOT}/market/kr_market/data_storage` | KR stock data directory |
| `MCP_US_DATA_DIR` | `{DATA_ROOT}/market/us_market/data_storage` | US stock data directory |
| `MCP_EXTERNAL_CONTEXT_DATA_DIR` | `{DATA_ROOT}/external_context/data_storage` | External context directory |
| `MCP_GLOBAL_REGIME_DATA_DIR` | `{DATA_ROOT}/market/global_regime/data_storage` | Global regime directory |

## Resources

| Resource URI | Description |
|-------------|-------------|
| `market://health` | Server health check |
| `market://global/summary` | Global macro regime summary |
| `market://global/category/{category}` | Per-category analysis (bonds, commodities, forex, vix, credit, liquidity, inflation) |
| `market://global/categories` | Available categories list |
| `market://structure/all` | All markets ETF/basket structure |
| `market://{market_id}/structure` | Per-market structure analysis |
| `market://{market_id}/status` | Market status (regime, positions, performance) |
| `market://{market_id}/positions/snapshot` | Current positions snapshot |
| `market://all/summary` | All markets combined summary |
| `market://indicators/fear-greed` | Fear & Greed Index |
| `market://indicators/context` | Combined market context |
| `market://{market_id}/signals/summary` | Signal summary (24h aggregation) |
| `market://{market_id}/signals/feedback` | Signal pattern feedback |
| `market://{market_id}/signals/roles` | Role-based signal summary |
| `market://{market_id}/external/summary` | External context (news, events, fundamentals) |
| `market://{market_id}/external/symbol/{symbol}` | Per-symbol external context |
| `market://{market_id}/unified/symbol/{symbol}` | Unified technical + external context |
| `market://{market_id}/unified` | Market-level unified context |
| `market://unified/cross-market` | Cross-market pattern analysis |

**Market IDs**: `crypto`, `kr_stock`, `us_stock` (aliases: `coin`, `kr`, `us`)

## Tools

| Tool | Parameters | Description |
|------|-----------|-------------|
| `get_trade_history` | market_id, limit, action_filter, min_pnl, max_pnl, hours_back | Query trade history with filters |
| `get_positions` | market_id, min_roi, max_roi, strategy, sort_by, sort_order, limit | Query open positions |
| `get_signals` | market_id, symbol, min_score, max_score, action_filter, interval | Query trading signals |
| `get_latest_decisions` | market_id, limit, decision_filter, hours_back | Query recent trading decisions |

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

| Tier | Daily Quota | Burst | Price |
|------|------------|-------|-------|
| **Free** (no key) | 5,000 req/day | 60 req/min | Free |
| **API Key** | 50,000 req/day | 300 req/min | Free (beta) |
| **Local** | Unlimited | Unlimited | Self-hosted |

**What this means at Free tier:**
- Monitor 5+ symbols all day with 1-min polling → ~7,200 req → fits within free
- Full market scan + continuous monitoring → comfortable headroom
- No API key needed to get started — just connect and go

**API key** (free during beta): Register at [oneqaz.com](https://oneqaz.com) for higher limits.

**Response headers** on every request:
- `X-RateLimit-Daily-Remaining`: requests left today
- `X-RateLimit-Minute-Remaining`: requests left this minute
- Exceeding limits returns HTTP 429 with `Retry-After` header

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
