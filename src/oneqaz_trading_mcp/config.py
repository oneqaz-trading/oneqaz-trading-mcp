# -*- coding: utf-8 -*-
"""
Configuration
=============
Environment-variable-driven path and server configuration.
All data directories can be overridden via environment variables.
"""

from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Path configuration
# ---------------------------------------------------------------------------

def _detect_data_root() -> Path:
    """Detect data root from DATA_ROOT env or fallback to parent directory."""
    env_root = os.environ.get("DATA_ROOT", "").strip()
    if env_root:
        return Path(env_root).resolve()
    # Default: parent of the package (two levels up from this file)
    return Path(__file__).parent.parent.parent.parent.resolve()

DATA_ROOT = _detect_data_root()


def _data_dir(default_rel: str, env_key: str) -> Path:
    """Resolve a data directory path with optional env-var override."""
    raw = (os.environ.get(env_key) or "").strip()
    if not raw:
        return DATA_ROOT / default_rel
    override = Path(raw)
    if not override.is_absolute():
        override = (DATA_ROOT / override).resolve()
    return override


# Global regime data
GLOBAL_REGIME_DIR = _data_dir("market/global_regime/data_storage", "MCP_GLOBAL_REGIME_DATA_DIR")
GLOBAL_REGIME_SUMMARY_JSON = GLOBAL_REGIME_DIR / "global_regime_summary.json"
BONDS_ANALYSIS_DB = GLOBAL_REGIME_DIR / "bonds_analysis.db"
COMMODITIES_ANALYSIS_DB = GLOBAL_REGIME_DIR / "commodities_analysis.db"
FOREX_ANALYSIS_DB = GLOBAL_REGIME_DIR / "forex_analysis.db"
VIX_ANALYSIS_DB = GLOBAL_REGIME_DIR / "vix_analysis.db"
CREDIT_ANALYSIS_DB = GLOBAL_REGIME_DIR / "credit_analysis.db"
LIQUIDITY_ANALYSIS_DB = GLOBAL_REGIME_DIR / "liquidity_analysis.db"
INFLATION_ANALYSIS_DB = GLOBAL_REGIME_DIR / "inflation_analysis.db"

# Market structure data (ETF/basket analysis)
US_STRUCTURE_DIR = _data_dir("market/us_market/data_storage/regime", "MCP_US_STRUCTURE_DIR")
KR_STRUCTURE_DIR = _data_dir("market/kr_market/data_storage/regime", "MCP_KR_STRUCTURE_DIR")
COIN_STRUCTURE_DIR = _data_dir("market/coin_market/data_storage/regime", "MCP_COIN_STRUCTURE_DIR")
US_STRUCTURE_SUMMARY_JSON = US_STRUCTURE_DIR / "market_structure_summary.json"
KR_STRUCTURE_SUMMARY_JSON = KR_STRUCTURE_DIR / "market_structure_summary.json"
COIN_STRUCTURE_SUMMARY_JSON = COIN_STRUCTURE_DIR / "market_structure_summary.json"
US_STRUCTURE_ANALYSIS_DB = US_STRUCTURE_DIR / "group_analysis.db"
KR_STRUCTURE_ANALYSIS_DB = KR_STRUCTURE_DIR / "group_analysis.db"
COIN_STRUCTURE_ANALYSIS_DB = COIN_STRUCTURE_DIR / "group_analysis.db"

# Per-market data directories (env-var overrides supported)
COIN_DATA_DIR = _data_dir("market/coin_market/data_storage", "MCP_COIN_DATA_DIR")
KR_DATA_DIR = _data_dir("market/kr_market/data_storage", "MCP_KR_DATA_DIR")
US_DATA_DIR = _data_dir("market/us_market/data_storage", "MCP_US_DATA_DIR")
EXTERNAL_CONTEXT_DATA_DIR = _data_dir("external_context/data_storage", "MCP_EXTERNAL_CONTEXT_DATA_DIR")

# Per-market trading system DBs
COIN_TRADING_DB = COIN_DATA_DIR / "trading_system.db"
KR_TRADING_DB = KR_DATA_DIR / "trading_system.db"
US_TRADING_DB = US_DATA_DIR / "trading_system.db"

# Per-market signal directories (one DB per symbol)
COIN_SIGNALS_DIR = COIN_DATA_DIR / "signals"
KR_SIGNALS_DIR = KR_DATA_DIR / "signals"
US_SIGNALS_DIR = US_DATA_DIR / "signals"

# Market ID -> trading DB path mapping
MARKET_DB_PATHS = {
    "crypto": COIN_TRADING_DB,
    "coin": COIN_TRADING_DB,
    "kr_stock": KR_TRADING_DB,
    "kr": KR_TRADING_DB,
    "us_stock": US_TRADING_DB,
    "us": US_TRADING_DB,
}

# Market ID -> signal directory mapping
SIGNAL_DIR_PATHS = {
    "crypto": COIN_SIGNALS_DIR,
    "coin": COIN_SIGNALS_DIR,
    "kr_stock": KR_SIGNALS_DIR,
    "kr": KR_SIGNALS_DIR,
    "us_stock": US_SIGNALS_DIR,
    "us": US_SIGNALS_DIR,
}

# External context DB path mapping
EXTERNAL_DB_PATHS = {
    "crypto": EXTERNAL_CONTEXT_DATA_DIR / "coin_market" / "external_context.db",
    "coin": EXTERNAL_CONTEXT_DATA_DIR / "coin_market" / "external_context.db",
    "coin_market": EXTERNAL_CONTEXT_DATA_DIR / "coin_market" / "external_context.db",
    "kr_stock": EXTERNAL_CONTEXT_DATA_DIR / "kr_market" / "external_context.db",
    "kr": EXTERNAL_CONTEXT_DATA_DIR / "kr_market" / "external_context.db",
    "kr_market": EXTERNAL_CONTEXT_DATA_DIR / "kr_market" / "external_context.db",
    "us_stock": EXTERNAL_CONTEXT_DATA_DIR / "us_market" / "external_context.db",
    "us": EXTERNAL_CONTEXT_DATA_DIR / "us_market" / "external_context.db",
    "us_market": EXTERNAL_CONTEXT_DATA_DIR / "us_market" / "external_context.db",
    "bonds": EXTERNAL_CONTEXT_DATA_DIR / "bonds" / "external_context.db",
    "bond": EXTERNAL_CONTEXT_DATA_DIR / "bonds" / "external_context.db",
    "forex": EXTERNAL_CONTEXT_DATA_DIR / "forex" / "external_context.db",
    "commodities": EXTERNAL_CONTEXT_DATA_DIR / "commodities" / "external_context.db",
    "commodity": EXTERNAL_CONTEXT_DATA_DIR / "commodities" / "external_context.db",
    "news": EXTERNAL_CONTEXT_DATA_DIR / "news" / "external_context.db",
    "vix": EXTERNAL_CONTEXT_DATA_DIR / "vix" / "external_context.db",
    "credit": EXTERNAL_CONTEXT_DATA_DIR / "credit" / "external_context.db",
    "liquidity": EXTERNAL_CONTEXT_DATA_DIR / "liquidity" / "external_context.db",
    "inflation": EXTERNAL_CONTEXT_DATA_DIR / "inflation" / "external_context.db",
    "us_structure": EXTERNAL_CONTEXT_DATA_DIR / "us_structure" / "external_context.db",
    "kr_structure": EXTERNAL_CONTEXT_DATA_DIR / "kr_structure" / "external_context.db",
    "coin_structure": EXTERNAL_CONTEXT_DATA_DIR / "coin_structure" / "external_context.db",
}

# Category -> analysis DB mapping
ANALYSIS_DB_PATHS = {
    "bonds": BONDS_ANALYSIS_DB,
    "commodities": COMMODITIES_ANALYSIS_DB,
    "forex": FOREX_ANALYSIS_DB,
    "vix": VIX_ANALYSIS_DB,
    "credit": CREDIT_ANALYSIS_DB,
    "liquidity": LIQUIDITY_ANALYSIS_DB,
    "inflation": INFLATION_ANALYSIS_DB,
    "us_structure": US_STRUCTURE_ANALYSIS_DB,
    "kr_structure": KR_STRUCTURE_ANALYSIS_DB,
    "coin_structure": COIN_STRUCTURE_ANALYSIS_DB,
}

STRUCTURE_SUMMARY_PATHS = {
    "us_stock": US_STRUCTURE_SUMMARY_JSON,
    "us": US_STRUCTURE_SUMMARY_JSON,
    "us_structure": US_STRUCTURE_SUMMARY_JSON,
    "kr_stock": KR_STRUCTURE_SUMMARY_JSON,
    "kr": KR_STRUCTURE_SUMMARY_JSON,
    "kr_structure": KR_STRUCTURE_SUMMARY_JSON,
    "crypto": COIN_STRUCTURE_SUMMARY_JSON,
    "coin": COIN_STRUCTURE_SUMMARY_JSON,
    "coin_structure": COIN_STRUCTURE_SUMMARY_JSON,
}

# ---------------------------------------------------------------------------
# Server configuration
# ---------------------------------------------------------------------------

MCP_SERVER_PORT = int(os.environ.get("MCP_SERVER_PORT", "8010"))
MCP_SERVER_HOST = os.environ.get("MCP_SERVER_HOST", "0.0.0.0")
MCP_STATELESS = True
MCP_JSON_RESPONSE = True

# ---------------------------------------------------------------------------
# Cache TTL (seconds)
# ---------------------------------------------------------------------------

CACHE_TTL_GLOBAL_REGIME = 300   # 5 min
CACHE_TTL_MARKET_STATUS = 60    # 1 min
CACHE_TTL_FEAR_GREED = 300      # 5 min (external API)
CACHE_TTL_TRADE_HISTORY = 10    # 10 sec
CACHE_TTL_POSITIONS = 10        # 10 sec

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL = os.environ.get("MCP_LOG_LEVEL", "INFO")
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [MCP] %(message)s"

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def get_market_db_path(market_id: str) -> Path | None:
    """Return trading DB path for given market ID."""
    return MARKET_DB_PATHS.get(market_id.lower().replace("-", "_"))

def get_signals_dir(market_id: str) -> Path | None:
    """Return signal directory path for given market ID."""
    return SIGNAL_DIR_PATHS.get(market_id.lower().replace("-", "_"))

def get_signal_db_path(market_id: str, symbol: str = None) -> Path | None:
    """Return signal DB path for a market (and optionally a specific symbol)."""
    sig_dir = get_signals_dir(market_id)
    if sig_dir is None:
        return None
    if symbol:
        norm = symbol.upper().replace('-KRW', '').replace('_KRW', '').replace('KRW-', '')
        return sig_dir / f"{norm.lower()}_signal.db"
    return sig_dir

def list_signal_db_files(market_id: str) -> list:
    """List all per-symbol signal DB files for a market."""
    sig_dir = get_signals_dir(market_id)
    if sig_dir is None or not sig_dir.exists():
        return []
    return sorted(sig_dir.glob("*_signal.db"))

def get_symbol_from_signal_db(db_path: Path) -> str:
    """Extract symbol name from a signal DB file path."""
    return db_path.stem.replace('_signal', '').upper()

def get_analysis_db_path(category: str) -> Path | None:
    """Return analysis DB path for a given category."""
    return ANALYSIS_DB_PATHS.get(category.lower())

def get_structure_summary_path(market_id: str) -> Path | None:
    """Return structure summary JSON path for a given market."""
    return STRUCTURE_SUMMARY_PATHS.get(market_id.lower().replace("-", "_"))

def get_external_db_path(market_id: str) -> Path | None:
    """Return external_context DB path for a given market/category."""
    return EXTERNAL_DB_PATHS.get(market_id.lower().replace("-", "_"))

def check_paths():
    """Print path existence status (for debugging)."""
    print(f"[Config] DATA_ROOT: {DATA_ROOT}")
    print(f"[Config] GLOBAL_REGIME_SUMMARY_JSON exists: {GLOBAL_REGIME_SUMMARY_JSON.exists()}")
    for market_id, db_path in MARKET_DB_PATHS.items():
        if market_id in ("crypto", "kr_stock", "us_stock"):
            print(f"[Config] trading {market_id}: {db_path.exists()}")
    for market_id, sig_dir in SIGNAL_DIR_PATHS.items():
        if market_id in ("crypto", "kr_stock", "us_stock"):
            db_count = len(list(sig_dir.glob("*_signal.db"))) if sig_dir.exists() else 0
            print(f"[Config] signals {market_id}: {sig_dir.exists()} ({db_count} DBs)")
