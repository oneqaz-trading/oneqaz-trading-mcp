# -*- coding: utf-8 -*-
"""
Market Status Resource
======================
Current state for crypto / kr_stock / us_stock markets.

Data source: trading_system.db
"""

from __future__ import annotations

import sqlite3
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict

from oneqaz_trading_mcp.config import (
    CACHE_TTL_MARKET_STATUS,
    get_market_db_path,
)
from oneqaz_trading_mcp.response import (
    build_resource_explanation,
    to_resource_text,
    with_explanation_contract,
)

logger = logging.getLogger("MarketMCP")


def _load_market_status(market_id: str) -> Dict[str, Any]:
    """Load market status (positions summary, performance, regime)."""
    db_path = get_market_db_path(market_id)
    if not db_path or not db_path.exists():
        return {"error": f"Trading DB not found for market: {market_id}", "available_markets": ["crypto", "kr_stock", "us_stock"]}

    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row
            result = {"market_id": market_id, "db_path": str(db_path), "timestamp": datetime.now(timezone.utc).isoformat()}

            # System status
            try:
                system_status = {row["key"]: row["value"] for row in conn.execute("SELECT key, value FROM system_status").fetchall()}
                result["system_status"] = system_status
                result["market_regime"] = system_status.get("market_regime", "Unknown")
                result["thinking_log"] = system_status.get("thinking_log", "")
            except Exception as e:
                result["system_status"] = {"error": str(e)}
                result["market_regime"] = "Unknown"
                result["thinking_log"] = ""

            if not result.get("thinking_log"):
                try:
                    row = conn.execute("SELECT message FROM system_logs WHERE component IN ('Executor','RiskManager','Strategy') ORDER BY id DESC LIMIT 1").fetchone()
                    if row:
                        result["thinking_log"] = row["message"]
                except Exception:
                    pass

            # Positions summary
            try:
                row = conn.execute("""
                    SELECT COUNT(*) as total_positions,
                           SUM(CASE WHEN profit_loss_pct > 0 THEN 1 ELSE 0 END) as profitable,
                           SUM(CASE WHEN profit_loss_pct <= 0 THEN 1 ELSE 0 END) as losing,
                           AVG(profit_loss_pct) as avg_pnl, MAX(profit_loss_pct) as max_pnl, MIN(profit_loss_pct) as min_pnl
                    FROM virtual_positions
                """).fetchone()
                if row:
                    result["positions_summary"] = {
                        "total": row["total_positions"] or 0, "profitable": row["profitable"] or 0,
                        "losing": row["losing"] or 0, "avg_pnl": round(row["avg_pnl"] or 0, 2),
                        "max_pnl": round(row["max_pnl"] or 0, 2), "min_pnl": round(row["min_pnl"] or 0, 2),
                    }
            except Exception as e:
                result["positions_summary"] = {"error": str(e)}

            # Performance stats
            try:
                row = conn.execute("SELECT total_trades, winning_trades, losing_trades, win_rate, total_profit_pct, active_positions FROM virtual_performance_stats ORDER BY timestamp DESC LIMIT 1").fetchone()
                if row:
                    result["performance"] = {
                        "total_trades": row["total_trades"], "winning_trades": row["winning_trades"],
                        "losing_trades": row["losing_trades"], "win_rate": round(row["win_rate"] or 0, 2),
                        "total_profit_pct": round(row["total_profit_pct"] or 0, 2), "active_positions": row["active_positions"],
                    }
            except Exception as e:
                result["performance"] = {"error": str(e)}

            # Recent 24h trades
            try:
                one_day_ago = int(time.time()) - 86400
                row = conn.execute("SELECT COUNT(*) as trades_24h, SUM(CASE WHEN profit_loss_pct > 0 THEN 1 ELSE 0 END) as wins_24h, AVG(profit_loss_pct) as avg_pnl_24h FROM virtual_trade_history WHERE exit_timestamp > ?", (one_day_ago,)).fetchone()
                if row:
                    result["recent_24h"] = {"trades": row["trades_24h"] or 0, "wins": row["wins_24h"] or 0, "avg_pnl": round(row["avg_pnl_24h"] or 0, 2)}
            except Exception as e:
                result["recent_24h"] = {"error": str(e)}

            result["_llm_summary"] = _generate_market_status_text(market_id, result)
            explanation = build_resource_explanation(
                market=market_id, entity_type="market", explanation_type="mcp_market_status",
                as_of_time=result["timestamp"], headline=f"{market_id} market status snapshot",
                why_text=result["_llm_summary"],
                bullet_points=[f"market_regime={result.get('market_regime', 'Unknown')}", f"positions={result.get('positions_summary', {}).get('total', 0)}", f"win_rate={result.get('performance', {}).get('win_rate', 0)}"],
                market_context_refs={"market_regime": result.get("market_regime", "Unknown"), "positions_summary": result.get("positions_summary", {}), "performance": result.get("performance", {})},
                note="derived from market status MCP resource",
            )
            return with_explanation_contract(result, resource_type="market_status", explanation=explanation)
    except Exception as e:
        logger.error(f"Failed to load market status for {market_id}: {e}")
        return {"error": str(e), "market_id": market_id}


def _generate_market_status_text(market_id: str, data: Dict[str, Any]) -> str:
    """Generate LLM-friendly market status summary."""
    try:
        lines = [f"[{market_id.upper()} Market Status]"]
        lines.append(f"- Market regime: {data.get('market_regime', 'Unknown')}")
        pos = data.get("positions_summary", {})
        if not pos.get("error"):
            lines.append(f"- Positions: {pos.get('total', 0)} (profitable: {pos.get('profitable', 0)}, avg PnL: {pos.get('avg_pnl', 0):+.2f}%)")
        perf = data.get("performance", {})
        if not perf.get("error"):
            lines.append(f"- Performance: win_rate={perf.get('win_rate', 0):.1f}%, total_pnl={perf.get('total_profit_pct', 0):+.2f}%")
        recent = data.get("recent_24h", {})
        if not recent.get("error"):
            lines.append(f"- Last 24h: {recent.get('trades', 0)} trades, {recent.get('wins', 0)} wins")
        return "\n".join(lines)
    except Exception as e:
        return f"Summary generation failed: {e}"


def _load_positions_snapshot(market_id: str, limit: int = 10) -> Dict[str, Any]:
    """Load current positions snapshot."""
    db_path = get_market_db_path(market_id)
    if not db_path or not db_path.exists():
        return {"error": f"DB not found for market: {market_id}"}
    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT symbol, entry_price, current_price, profit_loss_pct,
                       entry_timestamp, holding_duration, target_price,
                       ai_score, ai_reason, current_strategy
                FROM virtual_positions ORDER BY profit_loss_pct DESC LIMIT ?
            """, (limit,))
            positions = []
            for row in cursor.fetchall():
                pos = dict(row)
                pos["symbol"] = pos.get("symbol") or pos.get("coin", "?")
                duration = pos.get("holding_duration", 0)
                pos["holding_time_str"] = f"{duration // 3600}h {(duration % 3600) // 60}m"
                positions.append(pos)

            summary_lines = [f"[{market_id.upper()} Positions Snapshot] Total: {len(positions)}"]
            for p in positions[:5]:
                summary_lines.append(f"- {p.get('symbol', '?')}: {p.get('profit_loss_pct', 0):+.2f}% ({p.get('current_strategy', '?')})")

            result = {"market_id": market_id, "positions": positions, "count": len(positions), "_llm_summary": "\n".join(summary_lines)}
            explanation = build_resource_explanation(
                market=market_id, entity_type="market", explanation_type="mcp_positions_snapshot",
                as_of_time=datetime.now(timezone.utc).isoformat(), headline=f"{market_id} positions snapshot",
                why_text=result["_llm_summary"], bullet_points=[f"count={len(positions)}"],
                market_context_refs={"count": len(positions)}, note="derived from positions snapshot MCP resource",
            )
            return with_explanation_contract(result, resource_type="positions_snapshot", explanation=explanation)
    except Exception as e:
        logger.error(f"Failed to load positions for {market_id}: {e}")
        return {"error": str(e), "market_id": market_id}


def register_market_status_resources(mcp, cache):
    """Register market status MCP resources."""

    @mcp.resource("market://{market_id}/status")
    def get_market_status(market_id: str) -> str:
        """
        Current market status (crypto, kr_stock, us_stock).
        Returns: Regime, positions summary, performance stats, recent 24h trades.
        """
        cache_key = f"market_status_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = _load_market_status(market_id)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/positions/snapshot")
    def get_positions_snapshot(market_id: str) -> str:
        """
        Current positions snapshot for a market.
        Returns: Position list sorted by PnL.
        """
        cache_key = f"positions_snapshot_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = _load_positions_snapshot(market_id)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://all/summary")
    def get_all_markets_summary() -> str:
        """
        Combined summary for all markets (crypto, kr_stock, us_stock).
        Returns: Per-market status overview.
        """
        cache_key = "all_markets_summary"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        result = {"timestamp": datetime.now(timezone.utc).isoformat(), "markets": {}}
        summary_lines = ["[All Markets Summary]"]
        for mid in ("crypto", "kr_stock", "us_stock"):
            status = _load_market_status(mid)
            result["markets"][mid] = status
            if not status.get("error"):
                regime = status.get("market_regime", "?")
                pos_count = status.get("positions_summary", {}).get("total", 0)
                avg_pnl = status.get("positions_summary", {}).get("avg_pnl", 0)
                summary_lines.append(f"- {mid}: {regime}, positions={pos_count}, avg_pnl={avg_pnl:+.2f}%")
            else:
                summary_lines.append(f"- {mid}: no data")
        result["_llm_summary"] = "\n".join(summary_lines)
        cache.set(cache_key, result)
        return to_resource_text(result)

    logger.info("  Market Status resources registered")
