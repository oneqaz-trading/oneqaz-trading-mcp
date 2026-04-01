# -*- coding: utf-8 -*-
"""
Position Query Tool
===================
Query current positions by market with filters

Parameters:
- market_id: market (crypto, kr_stock, us_stock)
- min_roi, max_roi: ROI filter
- strategy: strategy filter
- sort_by: sort criteria
"""

from __future__ import annotations

import sqlite3
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Literal

from oneqaz_trading_mcp.config import (
    CACHE_TTL_POSITIONS,
    get_market_db_path,
)

logger = logging.getLogger("MarketMCP")

# ---------------------------------------------------------------------------
# Position query functions
# ---------------------------------------------------------------------------

def _get_positions(
    market_id: str,
    min_roi: Optional[float] = None,
    max_roi: Optional[float] = None,
    strategy: Optional[str] = None,
    sort_by: str = "profit_loss_pct",
    sort_order: str = "desc",
    limit: int = 1000,
) -> Dict[str, Any]:
    """Query positions (with filtering and sorting)"""
    db_path = get_market_db_path(market_id)
    logger.info("[MCP] get_positions market_id=%s db_path=%s", market_id, db_path)
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
                "SELECT name FROM pragma_table_info('virtual_positions')"
            )
            existing_columns = {row[0] for row in cursor_info.fetchall()}
            wanted = [
                "symbol", "entry_price", "current_price", "profit_loss_pct",
                "quantity", "entry_timestamp", "holding_duration",
                "target_price", "stop_loss_price", "max_profit_pct",
                "ai_score", "ai_reason", "entry_strategy", "current_strategy",
                "strategy_match", "strategy_switch_count", "evolution_level",
                "fractal_score", "mtf_score", "cross_score", "entry_confidence",
            ]
            select_cols = [c for c in wanted if c in existing_columns]
            if not select_cols:
                return {"error": "virtual_positions has no expected columns", "positions": []}
            cols_str = ", ".join(select_cols)
            query = f"""
                SELECT {cols_str}
                FROM virtual_positions
                WHERE 1=1
            """
            params: List[Any] = []

            # ROI filter
            if min_roi is not None:
                query += " AND profit_loss_pct >= ?"
                params.append(min_roi)
            if max_roi is not None:
                query += " AND profit_loss_pct <= ?"
                params.append(max_roi)

            # Strategy filter
            if strategy:
                query += " AND (current_strategy = ? OR entry_strategy = ?)"
                params.append(strategy)
                params.append(strategy)

            # Sorting
            valid_sort_cols = ["profit_loss_pct", "entry_timestamp", "holding_duration", "ai_score"]
            if sort_by not in valid_sort_cols:
                sort_by = "profit_loss_pct"
            order = "DESC" if sort_order.lower() == "desc" else "ASC"
            query += f" ORDER BY {sort_by} {order} LIMIT ?"
            params.append(limit)

            cursor = conn.execute(query, params)
            positions = []

            for row in cursor.fetchall():
                pos = dict(row)
                # Prefer symbol column (coin column removed)
                sym = pos.get("symbol") or pos.get("coin", "?")
                pos["symbol"] = sym

                # Format holding duration
                duration = pos.get("holding_duration", 0) or 0
                hours = duration // 3600
                mins = (duration % 3600) // 60
                pos["holding_time_str"] = f"{hours}h {mins}m"

                # Format entry time
                if pos.get("entry_timestamp"):
                    pos["entry_time_str"] = datetime.fromtimestamp(
                        pos["entry_timestamp"]
                    ).strftime("%m/%d %H:%M")

                # Calculate target progress
                target = pos.get("target_price", 0) or 0
                current = pos.get("current_price", 0) or 0
                entry = pos.get("entry_price", 0) or 0

                if target > entry and entry > 0:
                    progress = (current - entry) / (target - entry) * 100
                    pos["target_progress"] = round(min(progress, 100), 1)
                else:
                    pos["target_progress"] = None

                positions.append(pos)

            # Calculate statistics
            if positions:
                total_pnl = sum(p.get("profit_loss_pct", 0) for p in positions)
                profitable = sum(1 for p in positions if p.get("profit_loss_pct", 0) > 0)
                avg_pnl = total_pnl / len(positions)
                avg_ai_score = sum(p.get("ai_score", 0) or 0 for p in positions) / len(positions)

                stats = {
                    "total_positions": len(positions),
                    "profitable": profitable,
                    "losing": len(positions) - profitable,
                    "total_pnl": round(total_pnl, 2),
                    "avg_pnl": round(avg_pnl, 2),
                    "avg_ai_score": round(avg_ai_score, 2),
                }
            else:
                stats = {
                    "total_positions": 0,
                    "profitable": 0,
                    "losing": 0,
                    "total_pnl": 0,
                    "avg_pnl": 0,
                    "avg_ai_score": 0,
                }

            # LLM summary
            summary_lines = [
                f"[{market_id.upper()} Positions] {len(positions)} held",
                f"- Profitable/Losing: {stats['profitable']} / {stats['losing']}",
                f"- Avg PnL: {stats['avg_pnl']:+.2f}%",
                f"- Avg AI score: {stats['avg_ai_score']:.2f}",
            ]

            for p in positions[:5]:
                sym = p.get("symbol") or p.get("coin", "?")
                pnl = p.get("profit_loss_pct", 0)
                strategy = p.get("current_strategy", "?")
                progress = p.get("target_progress")
                progress_str = f", target {progress}%" if progress else ""
                summary_lines.append(f"  - {sym}: {pnl:+.2f}% ({strategy}{progress_str})")

            return {
                "market_id": market_id,
                "positions": positions,
                "stats": stats,
                "filters": {
                    "min_roi": min_roi,
                    "max_roi": max_roi,
                    "strategy": strategy,
                    "sort_by": sort_by,
                    "sort_order": sort_order,
                    "limit": limit,
                },
                "_llm_summary": "\n".join(summary_lines),
            }

    except Exception as e:
        logger.error(f"Failed to get positions for {market_id}: {e}")
        return {"error": str(e), "market_id": market_id}

def _get_position_detail(market_id: str, coin: str) -> Dict[str, Any]:
    """Detailed position information for a specific symbol"""
    db_path = get_market_db_path(market_id)

    if not db_path or not db_path.exists():
        return {"error": f"DB not found for market: {market_id}"}

    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row

            # Position info (virtual_positions uses symbol column)
            cursor = conn.execute("""
                SELECT * FROM virtual_positions WHERE symbol = ?
            """, (coin.upper(),))

            row = cursor.fetchone()
            if not row:
                return {
                    "error": f"Position not found for {coin} in {market_id}",
                    "market_id": market_id,
                    "coin": coin,
                }

            position = dict(row)

            # Format holding duration
            duration = position.get("holding_duration", 0) or 0
            hours = duration // 3600
            mins = (duration % 3600) // 60
            position["holding_time_str"] = f"{hours}h {mins}m"

            # Recent trade history (for this symbol)
            cursor = conn.execute("""
                SELECT action, profit_loss_pct, exit_timestamp, ai_reason
                FROM virtual_trade_history
                WHERE symbol = ?
                ORDER BY exit_timestamp DESC
                LIMIT 5
            """, (coin.upper(),))

            recent_trades = [dict(r) for r in cursor.fetchall()]

            # Recent trading decisions
            cursor = conn.execute("""
                SELECT decision, ai_reason, regime_name, timestamp
                FROM virtual_trade_decisions
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT 3
            """, (coin.upper(),))

            recent_decisions = [dict(r) for r in cursor.fetchall()]

            # LLM summary
            pnl = position.get("profit_loss_pct", 0)
            strategy = position.get("current_strategy", "?")
            ai_score = position.get("ai_score", 0)
            ai_reason = position.get("ai_reason", "")

            summary_lines = [
                f"[{coin.upper()} Position Detail]",
                f"- Current PnL: {pnl:+.2f}%",
                f"- Strategy: {strategy}",
                f"- AI score: {ai_score:.2f}",
                f"- AI reasoning: {ai_reason[:100] if ai_reason else 'N/A'}",
                f"- Holding time: {position['holding_time_str']}",
            ]

            if recent_trades:
                summary_lines.append(f"- {len(recent_trades)} recent trade(s) found")

            return {
                "market_id": market_id,
                "coin": coin.upper(),
                "position": position,
                "recent_trades": recent_trades,
                "recent_decisions": recent_decisions,
                "_llm_summary": "\n".join(summary_lines),
            }

    except Exception as e:
        logger.error(f"Failed to get position detail for {coin} in {market_id}: {e}")
        return {"error": str(e), "market_id": market_id, "coin": coin}

def _get_strategy_distribution(market_id: str) -> Dict[str, Any]:
    """Position distribution by strategy"""
    db_path = get_market_db_path(market_id)

    if not db_path or not db_path.exists():
        return {"error": f"DB not found for market: {market_id}"}

    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row

            cursor = conn.execute("""
                SELECT current_strategy,
                       COUNT(*) as count,
                       AVG(profit_loss_pct) as avg_pnl,
                       SUM(CASE WHEN profit_loss_pct > 0 THEN 1 ELSE 0 END) as wins
                FROM virtual_positions
                GROUP BY current_strategy
                ORDER BY count DESC
            """)

            distribution = []
            for row in cursor.fetchall():
                d = dict(row)
                d["avg_pnl"] = round(d["avg_pnl"] or 0, 2)
                d["win_rate"] = round(d["wins"] / d["count"] * 100, 1) if d["count"] > 0 else 0
                distribution.append(d)

            # LLM summary
            summary_lines = [f"[{market_id.upper()} Strategy Distribution]"]
            for d in distribution:
                strategy = d.get("current_strategy", "unknown")
                count = d.get("count", 0)
                avg_pnl = d.get("avg_pnl", 0)
                win_rate = d.get("win_rate", 0)
                summary_lines.append(f"- {strategy}: {count} positions, avg {avg_pnl:+.2f}%, win rate {win_rate:.1f}%")

            return {
                "market_id": market_id,
                "distribution": distribution,
                "_llm_summary": "\n".join(summary_lines),
            }

    except Exception as e:
        logger.error(f"Failed to get strategy distribution for {market_id}: {e}")
        return {"error": str(e), "market_id": market_id}

# ---------------------------------------------------------------------------
# Tool registration function
# ---------------------------------------------------------------------------

def register_position_tools(mcp, cache):
    """Register position tools"""

    @mcp.tool()
    def get_positions(
        market_id: str,
        min_roi: Optional[float] = None,
        max_roi: Optional[float] = None,
        strategy: Optional[str] = None,
        sort_by: str = "profit_loss_pct",
        sort_order: str = "desc",
        limit: int = 1000,
    ) -> Dict[str, Any]:
        """
        Position query (with filtering and sorting)

        Query currently held positions with filters.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock)
            min_roi: Minimum ROI filter (e.g. -5.0)
            max_roi: Maximum ROI filter (e.g. 10.0)
            strategy: Strategy filter (e.g. trend, scalping)
            sort_by: Sort criteria (profit_loss_pct, entry_timestamp, holding_duration, ai_score)
            sort_order: Sort order (desc, asc)
            limit: Number of results (default 1000, max 1000)

        Returns:
            Position list, statistics, LLM summary
        """
        cache_key = f"positions_{market_id}_{min_roi}_{max_roi}_{strategy}_{sort_by}_{sort_order}_{limit}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_POSITIONS)
        if cached:
            logger.debug(f"Cache hit: {cache_key}")
            return cached

        limit = min(max(1, limit), 1000)

        result = _get_positions(
            market_id=market_id,
            min_roi=min_roi,
            max_roi=max_roi,
            strategy=strategy,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
        )

        cache.set(cache_key, result)
        return result

    @mcp.tool()
    def get_position_detail(
        market_id: str,
        coin: str,
    ) -> Dict[str, Any]:
        """
        Detailed position information for a specific symbol

        Retrieves position info, recent trades, and trading decisions for the symbol.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock)
            coin: Symbol (e.g. BTC, ETH)

        Returns:
            Position detail, recent trades, trading decisions
        """
        cache_key = f"position_detail_{market_id}_{coin}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_POSITIONS)
        if cached:
            return cached

        result = _get_position_detail(market_id, coin)
        cache.set(cache_key, result)
        return result

    @mcp.tool()
    def get_profitable_positions(
        market_id: str,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """
        Query profitable positions (in profit only)

        Args:
            market_id: Market ID
            limit: Number of results

        Returns:
            Profitable position list
        """
        return _get_positions(
            market_id=market_id,
            min_roi=0.01,
            sort_by="profit_loss_pct",
            sort_order="desc",
            limit=limit,
        )

    @mcp.tool()
    def get_losing_positions(
        market_id: str,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """
        Query losing positions (in loss only)

        Args:
            market_id: Market ID
            limit: Number of results

        Returns:
            Losing position list
        """
        return _get_positions(
            market_id=market_id,
            max_roi=-0.01,
            sort_by="profit_loss_pct",
            sort_order="asc",
            limit=limit,
        )

    @mcp.tool()
    def get_strategy_distribution(market_id: str) -> Dict[str, Any]:
        """
        Position distribution by strategy

        Query strategy distribution and performance of currently held positions.

        Args:
            market_id: Market ID

        Returns:
            Position count, average PnL, and win rate per strategy
        """
        cache_key = f"strategy_distribution_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_POSITIONS)
        if cached:
            return cached

        result = _get_strategy_distribution(market_id)
        cache.set(cache_key, result)
        return result

    logger.info("  [OK] Position tools registered")
