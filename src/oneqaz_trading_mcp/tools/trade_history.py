# -*- coding: utf-8 -*-
"""
Trade History Query Tool
========================
Query trade history by market with filters

Parameters:
- market_id: market (crypto, kr_stock, us_stock)
- limit: number of results
- action: filter (buy, sell, all)
- min_pnl, max_pnl: PnL filter
"""

from __future__ import annotations

import sqlite3
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.config import (
    CACHE_TTL_TRADE_HISTORY,
    get_market_db_path,
)

logger = logging.getLogger("MarketMCP")

# ---------------------------------------------------------------------------
# Trade history query functions
# ---------------------------------------------------------------------------

def _get_trade_history(
    market_id: str,
    limit: int = 1000,
    action_filter: Optional[str] = None,
    min_pnl: Optional[float] = None,
    max_pnl: Optional[float] = None,
    hours_back: Optional[int] = None,
) -> Dict[str, Any]:
    """Query trade history"""
    db_path = get_market_db_path(market_id)
    logger.info("[MCP] get_trade_history market_id=%s db_path=%s", market_id, db_path)
    if not db_path or not db_path.exists():
        return {
            "error": f"Trading DB not found for market: {market_id}",
            "available_markets": ["crypto", "kr_stock", "us_stock"],
        }

    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row

            # SELECT only columns that exist in the table (handle schema differences across markets)
            cursor_info = conn.execute(
                "SELECT name FROM pragma_table_info('virtual_trade_history')"
            )
            existing_columns = {row[0] for row in cursor_info.fetchall()}
            wanted = [
                "symbol", "action", "profit_loss_pct", "entry_price", "exit_price",
                "entry_timestamp", "exit_timestamp", "holding_duration",
                "ai_score", "ai_reason", "signal_pattern",
                "created_at", "entry_confidence",
                "fractal_score", "mtf_score", "cross_score",
            ]
            select_cols = [c for c in wanted if c in existing_columns]
            if not select_cols:
                return {"error": "virtual_trade_history has no expected columns", "trades": []}
            cols_str = ", ".join(select_cols)
            query = f"""
                SELECT {cols_str}
                FROM virtual_trade_history
                WHERE 1=1
            """
            params: List[Any] = []

            # Action filter
            if action_filter and action_filter.lower() != "all":
                if action_filter.lower() == "buy":
                    query += " AND action LIKE 'buy%'"
                elif action_filter.lower() in ("sell", "close", "exit"):
                    query += " AND action NOT LIKE 'buy%'"

            # PnL filter
            if min_pnl is not None:
                query += " AND profit_loss_pct >= ?"
                params.append(min_pnl)
            if max_pnl is not None:
                query += " AND profit_loss_pct <= ?"
                params.append(max_pnl)

            # Time filter
            if hours_back is not None:
                import time
                cutoff = int(time.time()) - (hours_back * 3600)
                query += " AND exit_timestamp >= ?"
                params.append(cutoff)

            query += " ORDER BY exit_timestamp DESC LIMIT ?"
            params.append(limit)

            cursor = conn.execute(query, params)
            trades = []

            for row in cursor.fetchall():
                trade = dict(row)
                # Prefer symbol column (coin column removed)
                sym = trade.get("symbol") or trade.get("coin", "UNKNOWN")
                trade["symbol"] = sym

                # Format holding duration
                duration = trade.get("holding_duration", 0) or 0
                hours = duration // 3600
                mins = (duration % 3600) // 60
                trade["holding_time_str"] = f"{hours}h {mins}m"

                # Format timestamp
                if trade.get("exit_timestamp"):
                    trade["exit_time_str"] = datetime.fromtimestamp(
                        trade["exit_timestamp"]
                    ).strftime("%Y-%m-%d %H:%M")

                # Format created_at (for dashboard)
                created_at = trade.get("created_at")
                if created_at:
                    if 'T' in str(created_at):
                        trade["time"] = str(created_at).split('T')[1][:5]
                    elif ' ' in str(created_at):
                        trade["time"] = str(created_at).split(' ')[1][:5]
                    else:
                        trade["time"] = str(created_at)[:5]
                elif trade.get("exit_timestamp"):
                    trade["time"] = datetime.fromtimestamp(
                        trade["exit_timestamp"]
                    ).strftime("%H:%M")
                else:
                    trade["time"] = ""

                # entry_confidence level
                conf = trade.get("entry_confidence", 0) or 0
                if conf >= 0.8:
                    trade["confidence_level"] = "High"
                elif conf >= 0.5:
                    trade["confidence_level"] = "Medium"
                else:
                    trade["confidence_level"] = "Low"

                # Default values for None
                trade["fractal_score"] = trade.get("fractal_score") or 0.5
                trade["mtf_score"] = trade.get("mtf_score") or 0.5
                trade["cross_score"] = trade.get("cross_score") or 0.5
                trade["ai_score"] = trade.get("ai_score") or 0.0

                trades.append(trade)

            # Calculate statistics
            if trades:
                total_pnl = sum(t.get("profit_loss_pct", 0) for t in trades)
                wins = sum(1 for t in trades if t.get("profit_loss_pct", 0) > 0)
                losses = len(trades) - wins
                avg_pnl = total_pnl / len(trades)

                stats = {
                    "total_trades": len(trades),
                    "wins": wins,
                    "losses": losses,
                    "win_rate": round(wins / len(trades) * 100, 1) if trades else 0,
                    "total_pnl": round(total_pnl, 2),
                    "avg_pnl": round(avg_pnl, 2),
                }
            else:
                stats = {
                    "total_trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "win_rate": 0,
                    "total_pnl": 0,
                    "avg_pnl": 0,
                }

            # LLM summary
            summary_lines = [
                f"[{market_id.upper()} Trade History] {len(trades)} trades queried",
                f"- Win rate: {stats['win_rate']}% ({stats['wins']}W {stats['losses']}L)",
                f"- Avg PnL: {stats['avg_pnl']:+.2f}%",
            ]

            for t in trades[:5]:
                sym = t.get("symbol") or t.get("coin", "?")
                pnl = t.get("profit_loss_pct", 0)
                action = t.get("action", "?")
                summary_lines.append(f"  - {sym}: {pnl:+.2f}% ({action})")

            return {
                "market_id": market_id,
                "trades": trades,
                "stats": stats,
                "filters": {
                    "action": action_filter,
                    "min_pnl": min_pnl,
                    "max_pnl": max_pnl,
                    "hours_back": hours_back,
                    "limit": limit,
                },
                "_llm_summary": "\n".join(summary_lines),
            }

    except Exception as e:
        logger.error(f"Failed to get trade history for {market_id}: {e}")
        return {"error": str(e), "market_id": market_id}

def _get_trade_analysis(market_id: str, days: int = 7) -> Dict[str, Any]:
    """Trade analysis (daily/pattern-based statistics)"""
    db_path = get_market_db_path(market_id)

    if not db_path or not db_path.exists():
        return {"error": f"DB not found for market: {market_id}"}

    try:
        import time
        from collections import defaultdict

        cutoff = int(time.time()) - (days * 86400)

        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row

            cursor_info = conn.execute("SELECT name FROM pragma_table_info('virtual_trade_history')")
            cols = {r[0] for r in cursor_info.fetchall()}
            sym_col = "symbol" if "symbol" in cols else "coin"
            cursor = conn.execute(f"""
                SELECT {sym_col}, action, profit_loss_pct, exit_timestamp,
                       signal_pattern, ai_score
                FROM virtual_trade_history
                WHERE exit_timestamp >= ?
                ORDER BY exit_timestamp
            """, (cutoff,))

            trades = [dict(row) for row in cursor.fetchall()]

            if not trades:
                return {
                    "market_id": market_id,
                    "days": days,
                    "analysis": {},
                    "_llm_summary": f"[{market_id.upper()} Analysis] No trades in the last {days} days",
                }

            # Daily statistics
            daily_stats = defaultdict(lambda: {"trades": 0, "pnl": 0, "wins": 0})
            pattern_stats = defaultdict(lambda: {"trades": 0, "pnl": 0, "wins": 0})
            coin_stats = defaultdict(lambda: {"trades": 0, "pnl": 0, "wins": 0})

            for t in trades:
                # Daily
                ts = t.get("exit_timestamp", 0)
                day = datetime.fromtimestamp(ts).strftime("%Y-%m-%d") if ts else "unknown"
                daily_stats[day]["trades"] += 1
                daily_stats[day]["pnl"] += t.get("profit_loss_pct", 0)
                if t.get("profit_loss_pct", 0) > 0:
                    daily_stats[day]["wins"] += 1

                # By pattern
                pattern = t.get("signal_pattern", "unknown") or "unknown"
                pattern_stats[pattern]["trades"] += 1
                pattern_stats[pattern]["pnl"] += t.get("profit_loss_pct", 0)
                if t.get("profit_loss_pct", 0) > 0:
                    pattern_stats[pattern]["wins"] += 1

                # By symbol
                sym = t.get("symbol") or t.get("coin", "unknown")
                coin_stats[sym]["trades"] += 1
                coin_stats[sym]["pnl"] += t.get("profit_loss_pct", 0)
                if t.get("profit_loss_pct", 0) > 0:
                    coin_stats[sym]["wins"] += 1

            # Top 5 symbols
            top_coins = sorted(
                coin_stats.items(),
                key=lambda x: x[1]["pnl"],
                reverse=True
            )[:5]

            # Top 5 patterns
            top_patterns = sorted(
                pattern_stats.items(),
                key=lambda x: x[1]["pnl"],
                reverse=True
            )[:5]

            # LLM summary
            summary_lines = [
                f"[{market_id.upper()} Trade Analysis] Last {days} days",
                f"- Total trades: {len(trades)}",
            ]

            total_pnl = sum(t.get("profit_loss_pct", 0) for t in trades)
            total_wins = sum(1 for t in trades if t.get("profit_loss_pct", 0) > 0)
            summary_lines.append(f"- Total PnL: {total_pnl:+.2f}%, Win rate: {total_wins/len(trades)*100:.1f}%")

            summary_lines.append("- Top symbols:")
            for coin, stats in top_coins[:3]:
                summary_lines.append(f"  - {coin}: {stats['pnl']:+.2f}% ({stats['trades']} trades)")

            return {
                "market_id": market_id,
                "days": days,
                "total_trades": len(trades),
                "daily_stats": dict(daily_stats),
                "pattern_stats": dict(pattern_stats),
                "coin_stats": dict(coin_stats),
                "top_coins": top_coins,
                "top_patterns": top_patterns,
                "_llm_summary": "\n".join(summary_lines),
            }

    except Exception as e:
        logger.error(f"Failed to analyze trades for {market_id}: {e}")
        return {"error": str(e), "market_id": market_id}

# ---------------------------------------------------------------------------
# Tool registration function
# ---------------------------------------------------------------------------

def register_trade_history_tools(mcp, cache):
    """Register trade history tools"""

    @mcp.tool(
        title="Get Trade History",
        annotations={"readOnlyHint": True, "destructiveHint": False},
    )
    def get_trade_history(
        market_id: str,
        limit: int = 1000,
        action_filter: str = "all",
        min_pnl: Optional[float] = None,
        max_pnl: Optional[float] = None,
        hours_back: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Trade history query (with filtering)

        Query trade history for a specific market with filters.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock)
            limit: Number of results (default 1000, max 1000)
            action_filter: Action filter (all, buy, sell)
            min_pnl: Minimum PnL filter (e.g. -5.0)
            max_pnl: Maximum PnL filter (e.g. 10.0)
            hours_back: Within last N hours (e.g. 24)

        Returns:
            Trade history list, statistics, LLM summary
        """
        # Generate cache key
        cache_key = f"trade_history_{market_id}_{limit}_{action_filter}_{min_pnl}_{max_pnl}_{hours_back}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_TRADE_HISTORY)
        if cached:
            logger.debug(f"Cache hit: {cache_key}")
            return cached

        # Parameter validation
        limit = min(max(1, limit), 1000)

        result = _get_trade_history(
            market_id=market_id,
            limit=limit,
            action_filter=action_filter,
            min_pnl=min_pnl,
            max_pnl=max_pnl,
            hours_back=hours_back,
        )

        cache.set(cache_key, result)
        return result

    @mcp.tool(
        title="Analyze Trades",
        annotations={"readOnlyHint": True, "destructiveHint": False},
    )
    def analyze_trades(
        market_id: str,
        days: int = 7,
    ) -> Dict[str, Any]:
        """
        Trade analysis (daily/pattern/symbol statistics)

        Analyze trades over the last N days to identify patterns and performance.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock)
            days: Analysis period (default 7 days, max 30 days)

        Returns:
            Daily/pattern/symbol statistics, top symbols/patterns
        """
        cache_key = f"trade_analysis_{market_id}_{days}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_TRADE_HISTORY * 3)  # Analysis cached longer
        if cached:
            return cached

        days = min(max(1, days), 30)
        result = _get_trade_analysis(market_id, days)

        cache.set(cache_key, result)
        return result

    @mcp.tool(
        title="Get Winning Trades",
        annotations={"readOnlyHint": True, "destructiveHint": False},
    )
    def get_winning_trades(
        market_id: str,
        limit: int = 10,
    ) -> Dict[str, Any]:
        """
        Query winning trades (profitable trades only)

        Retrieves only trades with positive PnL.
        Used by LLM to learn success patterns.

        Args:
            market_id: Market ID
            limit: Number of results (default 10)

        Returns:
            Profitable trade list and statistics
        """
        return _get_trade_history(
            market_id=market_id,
            limit=limit,
            min_pnl=0.01,  # Positive only
        )

    @mcp.tool(
        title="Get Losing Trades",
        annotations={"readOnlyHint": True, "destructiveHint": False},
    )
    def get_losing_trades(
        market_id: str,
        limit: int = 10,
    ) -> Dict[str, Any]:
        """
        Query losing trades (loss trades only)

        Retrieves only trades with negative PnL.
        Used by LLM to analyze failure patterns.

        Args:
            market_id: Market ID
            limit: Number of results (default 10)

        Returns:
            Loss trade list and statistics
        """
        return _get_trade_history(
            market_id=market_id,
            limit=limit,
            max_pnl=-0.01,  # Negative only
        )

    logger.info("  [OK] Trade History tools registered")
