# -*- coding: utf-8 -*-
"""
oneqaz-trading-mcp Server
=========================
FastMCP-based market data MCP server.

Endpoints (default port 8010):
    - GET  /mcp/resources  : List available resources
    - GET  /mcp/tools      : List available tools
    - POST /mcp/resources/read : Read a resource
    - POST /mcp/tools/call     : Call a tool
    - GET  /docs           : Swagger UI
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict

try:
    from fastmcp import FastMCP
except ImportError as e:
    print("[ERROR] fastmcp import failed:", e)
    print("  pip install fastmcp")
    sys.exit(1)

from oneqaz_trading_mcp.config import (
    MCP_SERVER_HOST,
    MCP_SERVER_PORT,
    MCP_STATELESS,
    MCP_JSON_RESPONSE,
    LOG_LEVEL,
    LOG_FORMAT,
    DATA_ROOT,
)
from oneqaz_trading_mcp.cache import SimpleCache
from oneqaz_trading_mcp.response import to_resource_text

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format=LOG_FORMAT,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("MarketMCP")

# ---------------------------------------------------------------------------
# FastMCP server instance
# ---------------------------------------------------------------------------

def _create_mcp_server() -> FastMCP:
    """Create FastMCP server with version-compatible arguments."""
    try:
        server = FastMCP(
            name="OneqazTradingMCP",
            version="0.1.0",
            instructions=(
                "Trading signal analysis and market monitoring MCP server.\n"
                "Provides global regime, market status, signals, positions, and more."
            ),
        )
    except TypeError:
        server = FastMCP(name="OneqazTradingMCP")

    try:
        import fastmcp
        fastmcp.settings.host = MCP_SERVER_HOST
        fastmcp.settings.port = MCP_SERVER_PORT
        fastmcp.settings.streamable_http_path = "/mcp"
        fastmcp.settings.stateless_http = MCP_STATELESS
        fastmcp.settings.json_response = MCP_JSON_RESPONSE
        fastmcp.settings.show_cli_banner = False
        try:
            fastmcp.settings.log_level = "CRITICAL"
        except AttributeError:
            pass
    except (AttributeError, ImportError):
        pass

    logger.info(
        "FastMCP settings: host=%s port=%s path=/mcp stateless=%s",
        MCP_SERVER_HOST, MCP_SERVER_PORT, MCP_STATELESS,
    )
    return server


mcp = _create_mcp_server()
cache = SimpleCache()

# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@mcp.resource("market://health")
def health_check() -> str:
    """Server health check. Returns: status, timestamp, version."""
    return to_resource_text({
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "0.1.0",
        "server": "OneqazTradingMCP",
        "data_root": str(DATA_ROOT),
    })


# ---------------------------------------------------------------------------
# Resource & Tool registration
# ---------------------------------------------------------------------------

def register_all_resources():
    """Register all MCP resources."""
    from oneqaz_trading_mcp.resources.global_regime import register_global_regime_resources
    from oneqaz_trading_mcp.resources.market_status import register_market_status_resources
    from oneqaz_trading_mcp.resources.market_structure import register_market_structure_resources
    from oneqaz_trading_mcp.resources.indicators import register_indicator_resources
    from oneqaz_trading_mcp.resources.signal_system import register_signal_resources
    from oneqaz_trading_mcp.resources.external_context import register_external_context_resources
    from oneqaz_trading_mcp.resources.unified_context import register_unified_context_resources
    from oneqaz_trading_mcp.resources.derived_signals import register_derived_signals_resources

    register_global_regime_resources(mcp, cache)
    register_market_status_resources(mcp, cache)
    register_market_structure_resources(mcp, cache)
    register_indicator_resources(mcp, cache)
    register_signal_resources(mcp, cache)
    register_external_context_resources(mcp, cache)
    register_unified_context_resources(mcp, cache)
    register_derived_signals_resources(mcp, cache)

    logger.info("All resources registered")


def register_all_tools():
    """Register all MCP tools."""
    from oneqaz_trading_mcp.tools.trade_history import register_trade_history_tools
    from oneqaz_trading_mcp.tools.positions import register_position_tools
    from oneqaz_trading_mcp.tools.decisions import register_decision_tools
    from oneqaz_trading_mcp.tools.signals import register_signal_tools

    register_trade_history_tools(mcp, cache)
    register_position_tools(mcp, cache)
    register_decision_tools(mcp, cache)
    register_signal_tools(mcp, cache)

    logger.info("All tools registered")


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------

def create_app():
    """Create and initialize the FastMCP app."""
    logger.info("OneqazTradingMCP Server initializing...")
    logger.info("   Data Root: %s", DATA_ROOT)
    logger.info("   Host: %s:%s", MCP_SERVER_HOST, MCP_SERVER_PORT)

    register_all_resources()
    register_all_tools()
    return mcp


def run_server():
    """Start the MCP server."""
    create_app()

    logger.info("Starting OneqazTradingMCP on %s:%s", MCP_SERVER_HOST, MCP_SERVER_PORT)
    logger.info("   Swagger UI: http://localhost:%s/docs", MCP_SERVER_PORT)

    mcp.run(
        transport="streamable-http",
        host=MCP_SERVER_HOST,
        port=MCP_SERVER_PORT,
        path="/mcp",
        json_response=MCP_JSON_RESPONSE,
        stateless_http=MCP_STATELESS,
        show_banner=False,
        log_level="CRITICAL",
    )


if __name__ == "__main__":
    run_server()
