# oneqaz-trading-mcp

<!-- mcp-name: io.github.wnsod/oneqaz-trading-mcp -->

> MCP server for **trading signals**, **market regime detection**, **position monitoring**, **Fear & Greed index**, and **cross-market pattern analysis**. Supports crypto, US stocks, and Korean stocks.

**Keywords**: MCP, trading, signals, market analysis, regime, portfolio, sentiment, technical analysis, crypto, stocks, Fear & Greed, cross-market, Claude, model context protocol

## Features

- **8 Resource categories**: Global regime, market status, market structure, indicators (Fear & Greed), signals, external context (news/events), unified context, derived signals
- **4 Tool types**: Trade history, positions, signals, and trading decisions — all with filtering and sorting
- **Multi-market**: Supports crypto, Korean stocks (KR), and US stocks
- **SQLite-based**: No external database server required
- **Stateless HTTP**: Compatible with any MCP client

## Quick Start

### Install

```bash
pip install oneqaz-trading-mcp
```

### Initialize sample data

```bash
oneqaz-trading-mcp init
```

This creates sample SQLite databases with demo trading data so you can test immediately.

### Start the server

```bash
oneqaz-trading-mcp serve
```

Server starts at `http://localhost:8010` by default.

- Swagger UI: `http://localhost:8010/docs`
- MCP endpoint: `http://localhost:8010/mcp`

### Connect from Claude

#### Live API (recommended — no install needed)

Connect directly to our 24/7 live server with real-time market data:

```json
{
  "mcpServers": {
    "oneqaz-trading": {
      "url": "https://api.oneqaz.com/mcp"
    }
  }
}
```

#### Local (self-hosted with sample data)

If you prefer to run locally with demo data:

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

### 1. Ask Claude about market conditions

Once connected, just ask Claude in natural language:

```
You: "What's the current market regime for crypto?"

Claude reads market://crypto/status and responds:
  "Crypto is currently in a sideways_bullish regime.
   Fear & Greed index is at 68 (Greed).
   3 active BUY signals with avg score 0.78."
```

```
You: "Show me the best performing positions in US stocks"

Claude calls get_positions(market_id="us_stock", sort_by="roi", sort_order="desc") and responds:
  "Top positions:
   1. NVDA — +12.3% ROI (trend_continuation strategy)
   2. AAPL — +8.1% ROI (breakout strategy)
   3. MSFT — +5.4% ROI (momentum strategy)"
```

```
You: "Any macro risks I should know about?"

Claude reads market://global/summary and responds:
  "Global regime: neutral (score: 0.12)
   - Bonds: bearish, rising yields
   - VIX: elevated, risk_level=high
   - Commodities: bullish, gold at ATH
   Recommendation: defensive positioning suggested."
```

### 2. Build an AI trading assistant

Connect the MCP server to your own AI application:

```python
# Your AI app reads market intelligence from OneQAZ
regime = mcp.read("market://crypto/status")
signals = mcp.call("get_signals", market_id="crypto", min_score=0.7)
fear_greed = mcp.read("market://indicators/fear-greed")

# Your AI decides what to do with this context
prompt = f"""
  Market regime: {regime}
  High-confidence signals: {signals}
  Sentiment: {fear_greed}

  Should the user buy, hold, or sell?
"""
```

### 3. Cross-market analysis

```
You: "Compare crypto vs US stock market conditions"

Claude reads:
  - market://crypto/status
  - market://us_stock/status
  - market://unified/cross-market

Claude: "Crypto is bullish while US stocks are neutral.
   Cross-market correlation is weakening — crypto is
   decoupling from equity risk sentiment.
   This pattern historically precedes crypto outperformance."
```

### 4. Signal monitoring

```
You: "What BUY signals fired in the last 24 hours for Korean stocks?"

Claude calls get_signals(market_id="kr_stock", action_filter="BUY"):
  "12 BUY signals in KR market:
   - Samsung (005930): score 0.85, trend_continuation
   - SK Hynix (000660): score 0.79, breakout
   - POSCO (005490): score 0.71, mean_reversion
   ..."
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

## License

MIT
