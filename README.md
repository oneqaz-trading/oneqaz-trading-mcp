# oneqaz-trading-mcp

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

Add to your Claude MCP config:

```json
{
  "mcpServers": {
    "oneqaz-trading": {
      "url": "http://localhost:8010/mcp"
    }
  }
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
