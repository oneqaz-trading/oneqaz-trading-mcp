# -*- coding: utf-8 -*-
"""Database initialization: create schemas and optionally insert sample data."""

from __future__ import annotations

import sqlite3
import json
import time
from pathlib import Path
from datetime import datetime, timezone

from oneqaz_trading_mcp.config import (
    DATA_ROOT,
    COIN_DATA_DIR,
    COIN_SIGNALS_DIR,
    COIN_TRADING_DB,
    KR_DATA_DIR,
    KR_SIGNALS_DIR,
    KR_TRADING_DB,
    US_DATA_DIR,
    US_SIGNALS_DIR,
    US_TRADING_DB,
    GLOBAL_REGIME_DIR,
    GLOBAL_REGIME_SUMMARY_JSON,
    EXTERNAL_CONTEXT_DATA_DIR,
    BONDS_ANALYSIS_DB,
    COMMODITIES_ANALYSIS_DB,
    FOREX_ANALYSIS_DB,
    VIX_ANALYSIS_DB,
)


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

TRADING_SYSTEM_SCHEMA = """
CREATE TABLE IF NOT EXISTS system_status (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS system_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    component TEXT,
    message TEXT,
    timestamp TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS virtual_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    entry_price REAL,
    current_price REAL,
    profit_loss_pct REAL DEFAULT 0,
    entry_timestamp INTEGER,
    holding_duration INTEGER DEFAULT 0,
    target_price REAL,
    ai_score REAL,
    ai_reason TEXT,
    current_strategy TEXT
);

CREATE TABLE IF NOT EXISTS virtual_trade_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    action TEXT,
    entry_price REAL,
    exit_price REAL,
    profit_loss_pct REAL DEFAULT 0,
    entry_timestamp INTEGER,
    exit_timestamp INTEGER,
    holding_duration INTEGER DEFAULT 0,
    ai_score REAL,
    ai_reason TEXT,
    strategy TEXT
);

CREATE TABLE IF NOT EXISTS virtual_trade_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    decision TEXT,
    signal_score REAL,
    timestamp INTEGER,
    reason TEXT,
    ai_score REAL,
    ai_reason TEXT
);

CREATE TABLE IF NOT EXISTS virtual_performance_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    total_trades INTEGER DEFAULT 0,
    winning_trades INTEGER DEFAULT 0,
    losing_trades INTEGER DEFAULT 0,
    win_rate REAL DEFAULT 0,
    total_profit_pct REAL DEFAULT 0,
    active_positions INTEGER DEFAULT 0,
    timestamp INTEGER
);

CREATE TABLE IF NOT EXISTS signal_feedback_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_pattern TEXT,
    success_rate REAL,
    avg_profit REAL,
    total_trades INTEGER,
    confidence REAL
);

CREATE TABLE IF NOT EXISTS strategy_feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_type TEXT,
    market_condition TEXT,
    success_rate REAL,
    avg_profit REAL,
    total_trades INTEGER
);
"""

SIGNAL_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    interval TEXT,
    timestamp INTEGER,
    action TEXT,
    signal_score REAL,
    confidence REAL,
    rsi REAL,
    macd REAL,
    mfi REAL,
    atr REAL,
    adx REAL,
    wave_phase TEXT,
    pattern_type TEXT,
    risk_level TEXT,
    volatility REAL,
    integrated_direction TEXT,
    integrated_strength REAL,
    reason TEXT,
    hierarchy_context TEXT,
    current_price REAL,
    target_price REAL
);
"""

ANALYSIS_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS analysis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    interval TEXT,
    timestamp INTEGER,
    regime_stage TEXT,
    regime_label TEXT,
    sentiment REAL,
    sentiment_label TEXT,
    integrated_direction TEXT,
    volatility_level TEXT,
    risk_level TEXT,
    regime_confidence REAL,
    regime_transition_prob REAL
);
"""

EXTERNAL_CONTEXT_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fetched_at TEXT
);

CREATE TABLE IF NOT EXISTS symbol_master (
    symbol TEXT PRIMARY KEY,
    name_ko TEXT,
    name_en TEXT,
    sector TEXT,
    industry TEXT,
    is_active INTEGER DEFAULT 1,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS fundamentals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    market_cap REAL,
    per REAL,
    pbr REAL,
    roe REAL,
    tvl REAL,
    active_addresses INTEGER,
    dominance REAL,
    sector TEXT,
    industry TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS news_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    title TEXT,
    summary TEXT,
    sentiment_score REAL,
    sentiment_label TEXT,
    sentiment_confidence REAL,
    sentiment_model TEXT,
    risk_flags TEXT,
    impact_level TEXT,
    status TEXT,
    source TEXT,
    url TEXT,
    published_at TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    sector TEXT,
    event_type TEXT,
    lifecycle_stage TEXT
);

CREATE TABLE IF NOT EXISTS scheduled_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    event_type TEXT,
    title TEXT,
    status TEXT DEFAULT 'upcoming',
    confidence REAL,
    expected_impact TEXT,
    scheduled_at TEXT,
    source TEXT,
    details TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS market_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    metric_type TEXT,
    value REAL,
    timestamp TEXT,
    details TEXT
);

CREATE TABLE IF NOT EXISTS news_reaction_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    news_id INTEGER,
    market_id TEXT,
    window_minutes INTEGER,
    reaction_score REAL,
    confidence REAL,
    lag_minutes REAL,
    sample_size INTEGER,
    direction TEXT,
    news_published_at TEXT,
    computed_at TEXT,
    details TEXT
);
"""


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_MARKETS = {
    "crypto": {
        "regime": "trending",
        "thinking_log": "BTC showing strong momentum. Watching resistance at 72000.",
        "positions": [
            ("BTC", 68500.0, 71200.0, 3.94, 7200, 73000.0, 0.82, "Strong uptrend with volume confirmation", "trend_continuation"),
            ("ETH", 3450.0, 3380.0, -2.03, 3600, 3600.0, 0.65, "Consolidation phase, watching support", "mean_reversion"),
            ("SOL", 142.0, 148.5, 4.58, 10800, 155.0, 0.78, "Breakout above key resistance", "breakout"),
        ],
        "trades": [
            ("BTC", "sell", 65000.0, 67500.0, 3.85, 86400, 43200, 0.75, "Profit target reached", "trend_continuation"),
            ("ETH", "sell", 3200.0, 3100.0, -3.13, 72000, 22000, 0.55, "Stop loss triggered", "pullback"),
            ("XRP", "sell", 0.58, 0.62, 6.90, 50000, 20000, 0.80, "Strong momentum exit", "momentum"),
        ],
        "perf": (47, 28, 19, 59.6, 12.5, 3),
        "decisions": [
            ("BTC", "buy", 0.78, 7200, "Strong trend + volume spike", 0.82, "Multi-timeframe alignment bullish"),
            ("ETH", "hold", 0.45, 3600, "Consolidation, waiting for breakout", 0.65, "Range-bound, no clear direction"),
            ("SOL", "buy", 0.85, 10800, "Breakout confirmation above 145", 0.78, "Resistance break with high volume"),
        ],
        "signals_symbols": ["btc", "eth", "sol"],
    },
    "kr_stock": {
        "regime": "ranging",
        "thinking_log": "KOSPI consolidating near 2650. Foreign investors net selling. Watching semiconductor sector.",
        "positions": [
            ("005930", 72000.0, 73500.0, 2.08, 14400, 76000.0, 0.70, "Samsung Electronics: earnings recovery expected", "trend_continuation"),
            ("000660", 185000.0, 179000.0, -3.24, 7200, 195000.0, 0.55, "SK Hynix: memory cycle bottom uncertain", "mean_reversion"),
            ("035420", 320000.0, 335000.0, 4.69, 21600, 350000.0, 0.75, "NAVER: AI growth narrative strong", "momentum"),
        ],
        "trades": [
            ("005930", "sell", 70000.0, 73000.0, 4.29, 172800, 86400, 0.72, "Target reached on earnings beat", "trend_continuation"),
            ("051910", "sell", 620000.0, 595000.0, -4.03, 86400, 43200, 0.50, "Chemical sector weakness", "defensive"),
            ("006400", "sell", 55000.0, 58500.0, 6.36, 129600, 72000, 0.80, "Samsung SDI battery demand surge", "breakout"),
        ],
        "perf": (62, 35, 27, 56.5, 8.3, 3),
        "decisions": [
            ("005930", "buy", 0.70, 14400, "Valuation attractive + foreign buying resumed", 0.70, "P/B below historical average"),
            ("000660", "hold", 0.40, 7200, "Memory cycle bottom uncertain, wait for signal", 0.55, "Downside risk limited but no catalyst"),
            ("035420", "buy", 0.80, 21600, "AI monetization story gaining traction", 0.75, "Revenue growth acceleration expected"),
        ],
        "signals_symbols": ["005930", "000660", "035420"],
    },
    "us_stock": {
        "regime": "trending",
        "thinking_log": "S&P 500 at all-time high. Mega-cap tech leading. Fed rate cut expectations supporting risk-on.",
        "positions": [
            ("AAPL", 195.0, 202.5, 3.85, 10800, 210.0, 0.80, "iPhone cycle + services growth", "trend_continuation"),
            ("NVDA", 850.0, 920.0, 8.24, 14400, 950.0, 0.88, "AI capex boom driving GPU demand", "momentum"),
            ("MSFT", 420.0, 415.0, -1.19, 7200, 440.0, 0.72, "Cloud growth slightly below expectations", "pullback"),
        ],
        "trades": [
            ("TSLA", "sell", 180.0, 210.0, 16.67, 259200, 172800, 0.85, "Robotaxi catalyst momentum", "momentum"),
            ("META", "sell", 480.0, 520.0, 8.33, 172800, 86400, 0.78, "Strong ad revenue beat", "trend_continuation"),
            ("AMZN", "sell", 185.0, 178.0, -3.78, 86400, 43200, 0.52, "AWS growth miss", "defensive"),
        ],
        "perf": (85, 52, 33, 61.2, 18.7, 3),
        "decisions": [
            ("AAPL", "buy", 0.75, 10800, "Services revenue hitting new records", 0.80, "Ecosystem moat + buyback support"),
            ("NVDA", "buy", 0.90, 14400, "AI datacenter spending accelerating", 0.88, "Dominant market position in AI chips"),
            ("MSFT", "hold", 0.55, 7200, "Cloud growth needs to reaccelerate", 0.72, "Wait for Azure guidance update"),
        ],
        "signals_symbols": ["aapl", "nvda", "msft"],
    },
}


def _insert_sample_data(conn: sqlite3.Connection, market: str = "crypto"):
    """Insert sample trading data for demo purposes."""
    now_ts = int(time.time())
    mdata = SAMPLE_MARKETS.get(market, SAMPLE_MARKETS["crypto"])

    # System status
    conn.execute("INSERT OR REPLACE INTO system_status (key, value) VALUES ('market_regime', ?)", (mdata["regime"],))
    conn.execute("INSERT OR REPLACE INTO system_status (key, value) VALUES ('thinking_log', ?)", (mdata["thinking_log"],))

    # Positions
    for sym, entry, current, pnl, dur, target, ai_s, ai_r, strat in mdata["positions"]:
        conn.execute(
            "INSERT INTO virtual_positions (symbol, entry_price, current_price, profit_loss_pct, entry_timestamp, holding_duration, target_price, ai_score, ai_reason, current_strategy) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (sym, entry, current, pnl, now_ts - dur, dur, target, ai_s, ai_r, strat),
        )

    # Trade history
    for sym, action, entry_p, exit_p, pnl, ago_entry, ago_exit, ai_s, ai_r, strat in mdata["trades"]:
        conn.execute(
            "INSERT INTO virtual_trade_history (symbol, action, entry_price, exit_price, profit_loss_pct, entry_timestamp, exit_timestamp, holding_duration, ai_score, ai_reason, strategy) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (sym, action, entry_p, exit_p, pnl, now_ts - ago_entry, now_ts - ago_exit, ago_entry - ago_exit, ai_s, ai_r, strat),
        )

    # Performance stats
    p = mdata["perf"]
    conn.execute(
        "INSERT INTO virtual_performance_stats (total_trades, winning_trades, losing_trades, win_rate, total_profit_pct, active_positions, timestamp) VALUES (?,?,?,?,?,?,?)",
        (*p, now_ts),
    )

    # Decisions
    for sym, dec, score, ago, reason, ai_s, ai_r in mdata["decisions"]:
        conn.execute(
            "INSERT INTO virtual_trade_decisions (symbol, decision, signal_score, timestamp, reason, ai_score, ai_reason) VALUES (?,?,?,?,?,?,?)",
            (sym, dec, score, now_ts - ago, reason, ai_s, ai_r),
        )

    # Signal feedback
    feedbacks = [
        ("rsi_oversold_buy", 0.62, 1.85, 23, 0.78),
        ("macd_crossover", 0.55, 1.20, 31, 0.72),
        ("volume_breakout", 0.68, 2.30, 15, 0.81),
    ]
    for f in feedbacks:
        conn.execute("INSERT INTO signal_feedback_scores (signal_pattern, success_rate, avg_profit, total_trades, confidence) VALUES (?,?,?,?,?)", f)

    conn.commit()


def _insert_sample_signals(db_path: Path, symbol: str):
    """Insert sample signal data for a symbol."""
    now_ts = int(time.time())
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(SIGNAL_DB_SCHEMA)
        signals = [
            (symbol, "15m", now_ts - 900, "BUY", 0.72, 0.68, 35.2, 0.0023, 55.1, 0.015, 28.5, "wave3", "engulfing", "medium", 0.025, "bullish", 0.65, "RSI oversold + MACD crossover", None, 71200.0, 73000.0),
            (symbol, "240m", now_ts - 800, "BUY", 0.80, 0.75, 42.1, 0.0045, 60.3, 0.022, 32.1, "wave3", "breakout", "low", 0.018, "bullish", 0.78, "Strong 4h trend continuation", None, 71200.0, 74500.0),
            (symbol, "1d", now_ts - 700, "HOLD", 0.55, 0.60, 52.0, 0.0012, 48.5, 0.030, 22.3, "wave4", "consolidation", "medium", 0.032, "neutral", 0.45, "Daily range-bound", None, 71200.0, 72000.0),
            (symbol, "combined", now_ts - 600, "BUY", 0.75, 0.72, 42.0, 0.003, 55.0, 0.020, 28.0, "wave3", "multi_confirm", "medium", 0.022, "bullish", 0.70, "Multi-timeframe bullish alignment", '{"alignment_score": 0.78, "position_key": "trend_continuation"}', 71200.0, 73500.0),
        ]
        for s in signals:
            conn.execute(
                "INSERT INTO signals (symbol, interval, timestamp, action, signal_score, confidence, rsi, macd, mfi, atr, adx, wave_phase, pattern_type, risk_level, volatility, integrated_direction, integrated_strength, reason, hierarchy_context, current_price, target_price) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                s,
            )
        conn.commit()


def _insert_sample_analysis(db_path: Path, category: str):
    """Insert sample analysis data."""
    now_ts = int(time.time())
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(ANALYSIS_DB_SCHEMA)
        symbols_map = {
            "bonds": [("TLT", "bearish"), ("IEF", "neutral"), ("SHY", "bullish")],
            "commodities": [("GC=F", "bullish"), ("CL=F", "neutral"), ("SI=F", "bearish")],
            "forex": [("DXY", "bullish"), ("EURUSD", "bearish"), ("USDJPY", "bullish")],
            "vix": [("VIX", "volatile")],
        }
        for sym, regime in symbols_map.get(category, [("SPY", "bullish")]):
            conn.execute(
                "INSERT INTO analysis (symbol, interval, timestamp, regime_stage, regime_label, sentiment, sentiment_label, integrated_direction, volatility_level, risk_level, regime_confidence, regime_transition_prob) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (sym, "1d", now_ts, regime, regime.capitalize(), 0.55, "neutral", "sideways", "medium", "moderate", 0.72, 0.15),
            )
        conn.commit()


def _create_sample_global_regime():
    """Create sample global_regime_summary.json."""
    GLOBAL_REGIME_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "overall": {"regime": "trending", "score": 0.65},
        "categories": {
            "bonds": {"regime_dominant": "bearish", "sentiment_avg": -0.2, "symbols": 3},
            "commodities": {"regime_dominant": "bullish", "sentiment_avg": 0.3, "symbols": 5},
            "forex": {"regime_dominant": "neutral", "sentiment_avg": 0.05, "symbols": 4},
            "vix": {"regime_dominant": "low_volatility", "sentiment_avg": 0.1, "symbols": 1},
            "credit": {"regime_dominant": "stable", "sentiment_avg": 0.0, "symbols": 2},
            "liquidity": {"regime_dominant": "neutral", "sentiment_avg": 0.1, "symbols": 2},
            "inflation": {"regime_dominant": "cooling", "sentiment_avg": -0.1, "symbols": 3},
        },
        "mtf_summary": {"aligned_symbols": 12, "misaligned_symbols": 3},
    }
    GLOBAL_REGIME_SUMMARY_JSON.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"  Created {GLOBAL_REGIME_SUMMARY_JSON}")


SAMPLE_EXTERNAL = {
    "coin_market": [
        ("BTC", "Bitcoin", "Cryptocurrency", "Bitcoin ETF inflows surge to record high", "Major institutional buying detected", 0.75, "positive", "high", "Reuters", "market_event"),
        ("ETH", "Ethereum", "Cryptocurrency", "Ethereum network upgrade scheduled", "Pectra upgrade expected to improve scalability", 0.45, "neutral", "medium", "CoinDesk", "technical_event"),
    ],
    "kr_market": [
        ("005930", "Samsung Electronics", "Semiconductors", "Samsung HBM4 mass production on track", "Memory cycle recovery signal strengthening", 0.65, "positive", "high", "Yonhap", "earnings_event"),
        ("000660", "SK Hynix", "Semiconductors", "SK Hynix AI memory demand exceeding forecasts", "NVIDIA partnership driving orders", 0.80, "positive", "high", "Bloomberg", "market_event"),
        ("035420", "NAVER", "Internet", "NAVER HyperCLOVA AI platform adoption growing", "Enterprise AI revenue doubling YoY", 0.55, "positive", "medium", "Korea Herald", "technical_event"),
    ],
    "us_market": [
        ("AAPL", "Apple Inc", "Technology", "Apple Vision Pro sales beat expectations", "Spatial computing ecosystem expanding", 0.60, "positive", "medium", "CNBC", "earnings_event"),
        ("NVDA", "NVIDIA Corp", "Semiconductors", "NVIDIA Blackwell GPU demand overwhelming supply", "AI training infrastructure bottleneck", 0.85, "positive", "high", "Reuters", "market_event"),
        ("MSFT", "Microsoft Corp", "Technology", "Azure revenue growth slightly misses estimates", "Cloud competition intensifying", -0.15, "negative", "medium", "Bloomberg", "earnings_event"),
    ],
}


def _create_sample_external_context():
    """Create sample external_context.db for all markets."""
    now_iso = datetime.now(timezone.utc).isoformat()
    for market_dir, rows in SAMPLE_EXTERNAL.items():
        ec_dir = EXTERNAL_CONTEXT_DATA_DIR / market_dir
        ec_dir.mkdir(parents=True, exist_ok=True)
        db_path = ec_dir / "external_context.db"
        with sqlite3.connect(str(db_path)) as conn:
            conn.executescript(EXTERNAL_CONTEXT_SCHEMA)
            conn.execute("INSERT INTO pipeline_runs (fetched_at) VALUES (?)", (now_iso,))
            for sym, name, sector, title, summary, score, label, impact, source, etype in rows:
                conn.execute("INSERT INTO symbol_master (symbol, name_en, sector, is_active) VALUES (?, ?, ?, 1)", (sym, name, sector))
                conn.execute(
                    "INSERT INTO news_events (symbol, title, summary, sentiment_score, sentiment_label, impact_level, source, event_type) VALUES (?,?,?,?,?,?,?,?)",
                    (sym, title, summary, score, label, impact, source, etype),
                )
            conn.commit()
        print(f"  Created {db_path}")


# ---------------------------------------------------------------------------
# Main init function
# ---------------------------------------------------------------------------

MARKET_CONFIGS = [
    ("crypto", COIN_DATA_DIR, COIN_TRADING_DB, COIN_SIGNALS_DIR),
    ("kr_stock", KR_DATA_DIR, KR_TRADING_DB, KR_SIGNALS_DIR),
    ("us_stock", US_DATA_DIR, US_TRADING_DB, US_SIGNALS_DIR),
]

STRUCTURE_SAMPLES = {
    "crypto": {
        "market": "crypto", "overall": {"regime": "trending", "score": 0.7},
        "groups": {
            "dominance": {"regime_dominant": "btc_dominant", "regime_avg": 0.65, "confidence": 0.8, "timing_signal": "hold"},
            "defi": {"regime_dominant": "growing", "regime_avg": 0.55, "confidence": 0.6, "timing_signal": "accumulate"},
        },
    },
    "kr_stock": {
        "market": "kr_stock", "overall": {"regime": "ranging", "score": 0.48},
        "groups": {
            "semiconductor": {"regime_dominant": "recovering", "regime_avg": 0.58, "confidence": 0.7, "timing_signal": "accumulate"},
            "financials": {"regime_dominant": "stable", "regime_avg": 0.50, "confidence": 0.65, "timing_signal": "hold"},
            "bio": {"regime_dominant": "volatile", "regime_avg": 0.42, "confidence": 0.5, "timing_signal": "cautious"},
        },
    },
    "us_stock": {
        "market": "us_stock", "overall": {"regime": "trending", "score": 0.75},
        "groups": {
            "mega_tech": {"regime_dominant": "bullish", "regime_avg": 0.80, "confidence": 0.85, "timing_signal": "trend_follow"},
            "energy": {"regime_dominant": "neutral", "regime_avg": 0.50, "confidence": 0.6, "timing_signal": "hold"},
            "financials": {"regime_dominant": "bullish", "regime_avg": 0.65, "confidence": 0.7, "timing_signal": "accumulate"},
        },
    },
}


def init_databases():
    """Initialize all database schemas with sample data for all 3 markets."""
    print("=" * 60)
    print("  oneqaz-trading-mcp: Database Initialization")
    print("=" * 60)
    print(f"  DATA_ROOT: {DATA_ROOT}")
    print()

    step = 1

    # 1. Trading system DB + Signal DBs for each market
    for market, data_dir, trading_db, signals_dir in MARKET_CONFIGS:
        mdata = SAMPLE_MARKETS[market]

        # Trading system
        data_dir.mkdir(parents=True, exist_ok=True)
        print(f"[{step}/7] Creating trading_system.db ({market})...")
        with sqlite3.connect(str(trading_db)) as conn:
            conn.executescript(TRADING_SYSTEM_SCHEMA)
            _insert_sample_data(conn, market)
        print(f"  Created {trading_db}")
        step += 1

        # Signal DBs
        signals_dir.mkdir(parents=True, exist_ok=True)
        print(f"[{step}/7] Creating signal DBs ({market})...")
        for sym in mdata["signals_symbols"]:
            db_path = signals_dir / f"{sym.lower()}_signal.db"
            _insert_sample_signals(db_path, sym.upper())
            print(f"  Created {db_path}")

        # Structure summary
        regime_dir = data_dir / "regime"
        regime_dir.mkdir(parents=True, exist_ok=True)
        struct = STRUCTURE_SAMPLES[market].copy()
        struct["updated_at"] = datetime.now(timezone.utc).isoformat()
        structure_path = regime_dir / "market_structure_summary.json"
        structure_path.write_text(json.dumps(struct, indent=2), encoding="utf-8")
        print(f"  Created {structure_path}")
        step += 1

    # Global regime
    print(f"[{step}/7] Creating global regime data...")
    _create_sample_global_regime()
    for cat, db_path in [("bonds", BONDS_ANALYSIS_DB), ("commodities", COMMODITIES_ANALYSIS_DB), ("forex", FOREX_ANALYSIS_DB), ("vix", VIX_ANALYSIS_DB)]:
        _insert_sample_analysis(db_path, cat)
        print(f"  Created {db_path}")

    # External context (all markets)
    print(f"[7/7] Creating external context data (all markets)...")
    _create_sample_external_context()

    print()
    print("=" * 60)
    print("  Initialization complete! (crypto + kr_stock + us_stock)")
    print("  Run: oneqaz-trading-mcp serve")
    print("=" * 60)


if __name__ == "__main__":
    init_databases()
