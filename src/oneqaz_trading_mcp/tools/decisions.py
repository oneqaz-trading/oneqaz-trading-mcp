# -*- coding: utf-8 -*-
"""
Trading Decision History Tool
==============================
Query latest trading decisions and analysis rationale by market

Data source:
- virtual_trade_decisions: trading decision history
  - symbol, decision, signal_score, timestamp, reason, ai_score, ai_reason
"""

from __future__ import annotations

import sqlite3
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.config import (
    CACHE_TTL_POSITIONS,
    get_market_db_path,
)

logger = logging.getLogger("MarketMCP")

# ---------------------------------------------------------------------------
# Decision history query functions
# ---------------------------------------------------------------------------

def _get_latest_decisions(
    market_id: str,
    limit: int = 10,
    decision_filter: Optional[str] = None,
    hours_back: Optional[int] = None,
) -> Dict[str, Any]:
    """Query latest trading decisions"""
    db_path = get_market_db_path(market_id)

    if not db_path or not db_path.exists():
        return {
            "error": f"Trading DB not found for market: {market_id}",
            "available_markets": ["crypto", "kr_stock", "us_stock"],
        }

    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row

            # Check table existence
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='virtual_trade_decisions'"
            )
            if not cursor.fetchone():
                return {
                    "error": "virtual_trade_decisions table not found",
                    "market_id": market_id,
                    "decisions": [],
                }

            # Check columns
            cursor = conn.execute("PRAGMA table_info(virtual_trade_decisions)")
            columns = [col[1] for col in cursor.fetchall()]

            # Build base query (using symbol column)
            sym_col = "symbol" if "symbol" in columns else "coin"
            select_cols = [sym_col, "decision", "signal_score", "timestamp", "reason"]
            if "ai_score" in columns:
                select_cols.append("ai_score")
            if "ai_reason" in columns:
                select_cols.append("ai_reason")

            query = f"SELECT {', '.join(select_cols)} FROM virtual_trade_decisions WHERE 1=1"
            params: List[Any] = []

            # Decision filter (buy, sell, hold, etc.)
            if decision_filter:
                query += " AND decision = ?"
                params.append(decision_filter)

            # Time filter
            if hours_back is not None:
                import time
                cutoff = int(time.time()) - (hours_back * 3600)
                query += " AND timestamp >= ?"
                params.append(cutoff)

            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)

            cursor = conn.execute(query, params)
            decisions = []

            for row in cursor.fetchall():
                decision = dict(row)

                # Format timestamp
                ts = decision.get("timestamp")
                if ts:
                    try:
                        if isinstance(ts, (int, float)):
                            decision["timestamp_str"] = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            decision["timestamp_str"] = str(ts)
                    except:
                        decision["timestamp_str"] = str(ts)

                decisions.append(decision)

            # Statistics
            stats = {
                "total": len(decisions),
                "buy_count": sum(1 for d in decisions if d.get("decision") == "buy"),
                "sell_count": sum(1 for d in decisions if d.get("decision") == "sell"),
                "hold_count": sum(1 for d in decisions if d.get("decision") == "hold"),
            }

            # LLM summary
            llm_summary = _generate_decisions_summary(market_id, decisions, stats)

            return {
                "market_id": market_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "decisions": decisions,
                "stats": stats,
                "_llm_summary": llm_summary,
            }

    except Exception as e:
        logger.error(f"Failed to get decisions for {market_id}: {e}")
        return {"error": str(e), "market_id": market_id}


def _generate_decisions_summary(market_id: str, decisions: List[Dict], stats: Dict) -> str:
    """Generate LLM summary text for decisions"""
    if not decisions:
        return f"[{market_id.upper()} Trading Decisions] No recent decision history"

    lines = [f"[{market_id.upper()} Recent Trading Decisions]"]
    lines.append(f"- Total {stats['total']}: buy {stats['buy_count']}, sell {stats['sell_count']}, hold {stats['hold_count']}")

    # Summarize latest 3
    for d in decisions[:3]:
        sym = d.get("symbol") or d.get("coin", "?")
        decision = d.get("decision", "?")
        reason = d.get("ai_reason") or d.get("reason") or ""
        if len(reason) > 50:
            reason = reason[:47] + "..."
        lines.append(f"  - {sym}: {decision} - {reason}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LLM trading decision query (conversation.db -> llm_trading_decisions)
# ---------------------------------------------------------------------------

def _get_conversation_db_path() -> Optional[str]:
    project_root = str(get_market_db_path("crypto") or "").rsplit("market", 1)[0]
    if not project_root:
        import pathlib
        project_root = str(pathlib.Path(__file__).resolve().parents[2])
    path = os.path.join(project_root, "llm_factory", "store", "conversation.db")
    return path if os.path.exists(path) else None


def _get_llm_trading_decisions(
    market_id: str,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    """Query LLM trading decisions from conversation.db llm_trading_decisions table"""
    db_path = _get_conversation_db_path()
    if not db_path:
        return {"error": "conversation.db not found", "decisions": []}

    try:
        with sqlite3.connect(db_path, timeout=5) as conn:
            conn.row_factory = sqlite3.Row

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='llm_trading_decisions'"
            )
            if not cursor.fetchone():
                return {"error": "llm_trading_decisions table not found", "decisions": []}

            if symbol:
                rows = conn.execute(
                    "SELECT * FROM llm_trading_decisions WHERE market_id = ? AND symbol = ?",
                    (market_id, symbol),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM llm_trading_decisions WHERE market_id = ? ORDER BY confidence DESC",
                    (market_id,),
                ).fetchall()

            decisions = [dict(r) for r in rows]

            buy_count = sum(1 for d in decisions if d.get("action") == "buy")
            sell_count = sum(1 for d in decisions if d.get("action") == "sell")
            hold_count = sum(1 for d in decisions if d.get("action") == "hold")

            llm_summary = f"[{market_id.upper()} LLM Trading Decisions] {len(decisions)} total: buy {buy_count}, sell {sell_count}, hold {hold_count}"

            return {
                "market_id": market_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "decisions": decisions,
                "stats": {
                    "total": len(decisions),
                    "buy_count": buy_count,
                    "sell_count": sell_count,
                    "hold_count": hold_count,
                },
                "_llm_summary": llm_summary,
            }
    except Exception as e:
        logger.error(f"Failed to get LLM decisions for {market_id}: {e}")
        return {"error": str(e), "market_id": market_id, "decisions": []}


# ---------------------------------------------------------------------------
# Tool registration function
# ---------------------------------------------------------------------------

import os

def register_decision_tools(mcp, cache):
    """Register decision tools"""

    @mcp.tool(
        title="Get Latest Trading Decisions",
        annotations={"readOnlyHint": True, "destructiveHint": False},
    )
    def get_latest_decisions(
        market_id: str,
        limit: int = 10,
        decision_filter: str = None,
        hours_back: int = None,
    ) -> Dict[str, Any]:
        """
        Query latest trading decisions (signal-based Track B)

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock)
            limit: Number of results (default 10)
            decision_filter: Decision filter (buy, sell, hold)
            hours_back: Only decisions within last N hours

        Returns:
            Trading decision list and statistics
        """
        return _get_latest_decisions(
            market_id=market_id,
            limit=limit,
            decision_filter=decision_filter,
            hours_back=hours_back,
        )

    @mcp.tool(
        title="Get LLM Trading Decisions",
        annotations={"readOnlyHint": True, "destructiveHint": False},
    )
    def get_llm_trading_decisions(
        market_id: str,
        symbol: str = None,
    ) -> Dict[str, Any]:
        """
        Query LLM agent trading decisions (Track A)

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock, commodity, forex, bond)
            symbol: Specific symbol (optional, omit for entire market)

        Returns:
            LLM trading decision list (action, confidence, reason, regime, risk_level)
        """
        return _get_llm_trading_decisions(market_id=market_id, symbol=symbol)

    logger.info("  [OK] Decision Tools registered (+ LLM Trading Decisions)")
