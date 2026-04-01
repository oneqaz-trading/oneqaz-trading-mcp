# -*- coding: utf-8 -*-
"""
Signal Query Tool (per-symbol DB mode)
======================================
Query signal data from per-symbol signal DBs in the signals/ directory with dynamic filters.

Key features:
- Query latest signals for a specific symbol (direct per-symbol DB)
- Query all market signals (iterating per-symbol DBs)
- Filtering by interval/action/score
"""

from __future__ import annotations

import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.config import (
    CACHE_TTL_POSITIONS,
    get_signal_db_path,
    get_signals_dir,
    list_signal_db_files,
    get_symbol_from_signal_db,
    get_market_db_path,
)

logger = logging.getLogger("MarketMCP")

# ---------------------------------------------------------------------------
# Helper: safely query per-symbol DB
# ---------------------------------------------------------------------------

def _safe_query(db_path: Path, query: str, params=()) -> list:
    """Safely execute a query on a per-symbol DB"""
    rows = []
    try:
        with sqlite3.connect(str(db_path), timeout=5.0) as conn:
            conn.row_factory = sqlite3.Row
            for row in conn.execute(query, params).fetchall():
                rows.append(dict(row))
    except Exception as e:
        logger.debug("Signal DB query error [%s]: %s", db_path.name, e)
    return rows


def _format_signal_row(row: dict, symbol_from_file: Optional[str] = None) -> dict:
    """Convert DB row to API format. Uses symbol_from_file when DB has no symbol/coin column (cross-market consistency)."""
    # Use symbol column; fall back to file-based symbol
    symbol = row.get("symbol") or symbol_from_file or "UNKNOWN"

    signal = {
        "symbol": symbol,
        "interval": row.get("interval"),
        "signal_score": round(row.get("signal_score") or 0, 3),
        "confidence": round(row.get("confidence") or 0, 3),
        "action": row.get("action"),
        "current_price": row.get("current_price"),
        "target_price": row.get("target_price"),
        "indicators": {
            "rsi": round(row["rsi"], 1) if row.get("rsi") else None,
            "macd": round(row["macd"], 4) if row.get("macd") else None,
            "mfi": round(row["mfi"], 1) if row.get("mfi") else None,
            "atr": round(row["atr"], 4) if row.get("atr") else None,
            "adx": round(row["adx"], 1) if row.get("adx") else None,
        },
        "wave_phase": row.get("wave_phase"),
        "pattern_type": row.get("pattern_type"),
        "risk_level": row.get("risk_level"),
        "volatility": round(row["volatility"], 4) if row.get("volatility") else None,
        "direction": row.get("integrated_direction"),
        "strength": round(row["integrated_strength"], 3) if row.get("integrated_strength") else None,
        "recommended_strategy": row.get("recommended_strategy"),
        "strategy_match": round(row["strategy_match"], 3) if row.get("strategy_match") else None,
        "warnings": {
            "peak_warning": bool(row.get("peak_warning")),
            "bottom_warning": bool(row.get("bottom_warning")),
            "momentum_slowing": bool(row.get("momentum_slowing")),
            "momentum_recovering": bool(row.get("momentum_recovering")),
        },
        "reason": row.get("reason"),
        "timestamp": row.get("timestamp"),
    }

    if signal["timestamp"]:
        try:
            signal["timestamp_str"] = datetime.fromtimestamp(
                signal["timestamp"]
            ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            signal["timestamp_str"] = str(signal["timestamp"])

    return signal


# ---------------------------------------------------------------------------
# Signal query functions
# ---------------------------------------------------------------------------

def _get_latest_signals(
    market_id: str,
    coin: Optional[str] = None,
    interval: Optional[str] = None,
    action_filter: Optional[str] = None,
    min_score: Optional[float] = None,
    min_confidence: Optional[float] = None,
    limit: int = 500,
    hours_back: int = 24,
) -> Dict[str, Any]:
    """Query latest signals (per-symbol DB mode)"""

    # If a specific symbol is specified, query only that symbol's DB
    if coin:
        sym_db = get_signal_db_path(market_id, symbol=coin)
        if not sym_db or not sym_db.exists():
            return {"error": f"Signal DB not found for {coin} in {market_id}"}
        db_list = [sym_db]
    else:
        sig_dir = get_signals_dir(market_id)
        db_list = list_signal_db_files(market_id) if sig_dir else []
        if not db_list:
            return {
                "error": f"No signal DBs found for market: {market_id}",
                "available_markets": ["crypto", "kr_stock", "us_stock"],
                "signals_dir_resolved": str(sig_dir) if sig_dir else None,
                "signals": [],
                "stats": {"total": 0, "buy_count": 0, "sell_count": 0, "hold_count": 0, "avg_score": 0, "db_count": 0},
            }

    try:
        all_signals = []

        for db_file in db_list:
            symbol_from_file = get_symbol_from_signal_db(db_file)
            # Dynamic query build
            query = """
                SELECT *
                FROM signals
                WHERE timestamp > (strftime('%s', 'now') - ?)
            """
            params: List[Any] = [hours_back * 3600]

            if coin:
                # Only this symbol's DB is open, no additional row filter needed (file itself is per-symbol)
                pass

            if interval:
                query += " AND interval = ?"
                params.append(interval)

            if action_filter:
                query += " AND action = ?"
                params.append(action_filter.lower())

            if min_score is not None:
                query += " AND signal_score >= ?"
                params.append(min_score)

            if min_confidence is not None:
                query += " AND confidence >= ?"
                params.append(min_confidence)

            per_db_limit = limit if coin else 2
            query += " ORDER BY timestamp DESC, signal_score DESC LIMIT ?"
            params.append(per_db_limit)

            rows = _safe_query(db_file, query, tuple(params))
            for row in rows:
                all_signals.append(_format_signal_row(row, symbol_from_file=symbol_from_file))

        # Sort all results by score then apply limit
        all_signals.sort(key=lambda x: (x.get("timestamp") or 0, x.get("signal_score") or 0), reverse=True)
        signals = all_signals[:limit]

        # Statistics
        stats = {
            "total": len(signals),
            "buy_count": sum(1 for s in signals if s["action"] == "buy"),
            "sell_count": sum(1 for s in signals if s["action"] == "sell"),
            "hold_count": sum(1 for s in signals if s["action"] == "hold"),
            "avg_score": round(sum(s["signal_score"] for s in signals) / len(signals), 3) if signals else 0,
            "db_count": len(db_list),
        }

        llm_summary = _generate_signals_query_summary(market_id, signals, stats, coin, interval)

        return {
            "market_id": market_id,
            "filters": {
                "symbol": coin,
                "interval": interval,
                "action": action_filter,
                "min_score": min_score,
                "min_confidence": min_confidence,
                "hours_back": hours_back,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "signals": signals,
            "stats": stats,
            "_llm_summary": llm_summary,
        }

    except Exception as e:
        logger.error(f"Failed to get signals for {market_id}: {e}")
        return {"error": str(e), "market_id": market_id}


def _get_coin_signal_detail(
    market_id: str,
    coin: str,
    interval: str = "combined",
) -> Dict[str, Any]:
    """Detailed signal information for a specific symbol (read from per-symbol DB)"""
    sym_db = get_signal_db_path(market_id, symbol=coin)

    if not sym_db or not sym_db.exists():
        return {"error": f"Signal DB not found for {coin} in {market_id}"}

    try:
        with sqlite3.connect(str(sym_db)) as conn:
            conn.row_factory = sqlite3.Row

            # Latest signal
            cursor = conn.execute("""
                SELECT *
                FROM signals
                WHERE interval = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (interval,))

            row = cursor.fetchone()
            if not row:
                return {
                    "error": f"No signal found for {coin} at {interval}",
                    "symbol": coin,
                    "interval": interval,
                }

            signal = dict(row)

            # Recent signal history
            hist_cursor = conn.execute("""
                SELECT signal_score, action, timestamp
                FROM signals
                WHERE interval = ?
                ORDER BY timestamp DESC
                LIMIT 10
            """, (interval,))

            history = []
            for h in hist_cursor.fetchall():
                history.append({
                    "score": round(h["signal_score"] or 0, 3),
                    "action": h["action"],
                    "timestamp": h["timestamp"],
                })

            # Feedback data (from trading_system.db)
            feedback = {}
            try:
                trading_db = get_market_db_path(market_id)
                if trading_db and trading_db.exists():
                    with sqlite3.connect(str(trading_db)) as tconn:
                        tconn.row_factory = sqlite3.Row
                        fb_cursor = tconn.execute("""
                            SELECT signal_pattern, success_rate, avg_profit, total_trades
                            FROM signal_feedback_scores
                            WHERE symbol = ? OR symbol = ?
                            ORDER BY total_trades DESC
                            LIMIT 5
                        """, (coin.upper(), coin))
                        for fb in fb_cursor.fetchall():
                            feedback[fb["signal_pattern"]] = {
                                "success_rate": round(fb["success_rate"] or 0, 3),
                                "avg_profit": round(fb["avg_profit"] or 0, 3),
                                "total_trades": fb["total_trades"],
                            }
            except Exception:
                pass

            return {
                "market_id": market_id,
                "symbol": coin,
                "interval": interval,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "latest_signal": signal,
                "recent_history": history,
                "pattern_feedback": feedback,
            }

    except Exception as e:
        logger.error(f"Failed to get signal detail for {coin}: {e}")
        return {"error": str(e)}


def _generate_signals_query_summary(
    market_id: str,
    signals: List[Dict],
    stats: Dict,
    coin: Optional[str],
    interval: Optional[str],
) -> str:
    """Generate LLM summary for signal query results"""
    target = f"{coin or 'all'} {interval or 'all intervals'}"
    lines = [f"[{market_id.upper()} Signal Query: {target}]"]
    lines.append(f"- Total {stats['total']} ({stats.get('db_count', '?')} DBs): buy {stats['buy_count']}, sell {stats['sell_count']}, hold {stats['hold_count']}")
    lines.append(f"- Avg score: {stats['avg_score']:.2f}")

    if signals:
        lines.append("- Top signals:")
        for s in signals[:3]:
            warnings = []
            w = s.get("warnings", {})
            if w.get("peak_warning"):
                warnings.append("peak warning")
            if w.get("bottom_warning"):
                warnings.append("bottom signal")
            warn_str = f" [{', '.join(warnings)}]" if warnings else ""
            lines.append(f"  - {s['symbol']}({s['interval']}): {s['action']} score={s['signal_score']:.2f}{warn_str}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Role-based analysis (hierarchy_context based)
# ---------------------------------------------------------------------------

_ROLE_DESCRIPTIONS = {
    'timing': 'Optimize actual buy/sell timing',
    'trend': 'Confirm short-term trend continuation/reversal signals',
    'swing': 'Identify mid-term trends and wave structure',
    'regime': 'Determine market direction / long-term regime',
    'combined': 'Integrated analysis across all roles',
}


def _get_role_analysis(
    market_id: str,
    coin: str,
) -> Dict[str, Any]:
    """Role-based (timing/trend/swing/regime) signal analysis for a symbol"""
    import json as _json

    sym_db = get_signal_db_path(market_id, symbol=coin)
    if not sym_db or not sym_db.exists():
        return {"error": f"Signal DB not found for {coin} in {market_id}"}

    try:
        with sqlite3.connect(str(sym_db), timeout=5.0) as conn:
            conn.row_factory = sqlite3.Row

            # Query latest signal per interval
            rows = conn.execute("""
                SELECT * FROM signals
                WHERE timestamp = (
                    SELECT MAX(timestamp) FROM signals s2
                    WHERE s2.interval = signals.interval
                )
                ORDER BY interval
            """).fetchall()

            # Extract hierarchy_context from combined row
            hierarchy_ctx = {}
            combined_row = conn.execute("""
                SELECT hierarchy_context, signal_score, confidence, action,
                       current_price, target_price, risk_level, volatility
                FROM signals
                WHERE interval = 'combined'
                ORDER BY timestamp DESC LIMIT 1
            """).fetchone()

            if combined_row and combined_row['hierarchy_context']:
                try:
                    hierarchy_ctx = _json.loads(combined_row['hierarchy_context'])
                except Exception:
                    pass

            # Interval-to-role mapping (dynamic, based on environment variables)
            import os
            env_ivs = os.environ.get('CANDLE_INTERVALS', '15m,240m,1d')
            ivs = sorted(
                [iv.strip() for iv in env_ivs.split(',') if iv.strip()],
                key=lambda iv: int(iv[:-1]) if iv.endswith('m') else (
                    int(iv[:-1]) * 60 if iv.endswith('h') else int(iv[:-1]) * 1440
                )
            )
            role_map = {}
            if len(ivs) >= 4:
                role_map[ivs[0]] = 'timing'
                role_map[ivs[-1]] = 'regime'
                mid = len(ivs) // 2
                for iv in ivs[1:mid]:
                    role_map[iv] = 'trend'
                for iv in ivs[mid:-1]:
                    role_map[iv] = 'swing'
            elif len(ivs) == 3:
                role_map = {ivs[0]: 'timing', ivs[1]: 'trend', ivs[2]: 'regime'}
            elif len(ivs) == 2:
                role_map = {ivs[0]: 'timing', ivs[1]: 'regime'}

            # Organize signals by role
            roles = {}
            for row in [dict(r) for r in rows]:
                iv = row.get('interval', '')
                if iv == 'combined':
                    continue
                role = role_map.get(iv, 'unknown')
                roles[role] = {
                    'interval': iv,
                    'role': role,
                    'description': _ROLE_DESCRIPTIONS.get(role, ''),
                    'signal_score': round(row.get('signal_score') or 0, 3),
                    'confidence': round(row.get('confidence') or 0, 3),
                    'action': row.get('action'),
                    'direction': row.get('integrated_direction'),
                    'strength': round(row.get('integrated_strength') or 0, 3),
                    'indicators': {
                        'rsi': round(row['rsi'], 1) if row.get('rsi') else None,
                        'macd': round(row['macd'], 4) if row.get('macd') else None,
                        'adx': round(row['adx'], 1) if row.get('adx') else None,
                    },
                    'wave_phase': row.get('wave_phase'),
                    'risk_level': row.get('risk_level'),
                    'warnings': {
                        'peak_warning': bool(row.get('peak_warning')),
                        'bottom_warning': bool(row.get('bottom_warning')),
                        'momentum_slowing': bool(row.get('momentum_slowing')),
                        'momentum_recovering': bool(row.get('momentum_recovering')),
                    },
                }

            # Extract integrated meta info from hierarchy_context
            hierarchy_summary = {}
            if hierarchy_ctx:
                hierarchy_summary = {
                    'alignment_score': round(float(hierarchy_ctx.get('alignment_score', 0.5)), 3),
                    'alignment_type': hierarchy_ctx.get('alignment_type', 'unknown'),
                    'position_key': hierarchy_ctx.get('position_key', 'unknown'),
                    'regime_direction': hierarchy_ctx.get('regime_direction', 'neutral'),
                    'overall_progress': round(float(hierarchy_ctx.get('overall_progress', 0.5)), 3),
                    'overall_trend_position': hierarchy_ctx.get('overall_trend_position', 'mid'),
                    'regime_transitioning': bool(hierarchy_ctx.get('regime_transitioning', False)),
                }

            # Combined signal info
            combined_info = {}
            if combined_row:
                combined_info = {
                    'signal_score': round(combined_row['signal_score'] or 0, 3),
                    'confidence': round(combined_row['confidence'] or 0, 3),
                    'action': combined_row['action'],
                    'current_price': combined_row['current_price'],
                    'target_price': combined_row['target_price'],
                    'risk_level': combined_row['risk_level'],
                    'volatility': round(combined_row['volatility'] or 0, 4) if combined_row['volatility'] else None,
                }

            # Generate LLM summary
            llm_summary = _generate_role_analysis_summary(coin, roles, hierarchy_summary, combined_info)

            return {
                'market_id': market_id,
                'symbol': coin,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'roles': roles,
                'hierarchy': hierarchy_summary,
                'combined': combined_info,
                '_llm_summary': llm_summary,
            }

    except Exception as e:
        logger.error(f"Failed to get role analysis for {coin}: {e}")
        return {'error': str(e), 'symbol': coin}


def _generate_role_analysis_summary(
    coin: str,
    roles: Dict,
    hierarchy: Dict,
    combined: Dict,
) -> str:
    """Generate LLM summary for role-based analysis"""
    lines = [f"[{coin} Role-based Signal Analysis]"]

    role_order = ['regime', 'swing', 'trend', 'timing']
    for role_name in role_order:
        r = roles.get(role_name)
        if not r:
            continue
        desc = _ROLE_DESCRIPTIONS.get(role_name, '')
        direction = r.get('direction') or '?'
        score = r.get('signal_score', 0)
        action = r.get('action', 'hold')
        warnings = []
        w = r.get('warnings', {})
        if w.get('peak_warning'):
            warnings.append('peak warning')
        if w.get('bottom_warning'):
            warnings.append('bottom signal')
        if w.get('momentum_slowing'):
            warnings.append('momentum slowing')
        warn_str = f" [{','.join(warnings)}]" if warnings else ""
        lines.append(f"  {role_name}({r['interval']}): {action} score={score:.2f} dir={direction}{warn_str}")
        lines.append(f"    - {desc}")

    if hierarchy:
        align = hierarchy.get('alignment_score', 0.5)
        a_type = hierarchy.get('alignment_type', '?')
        pos_key = hierarchy.get('position_key', '?')
        trend_pos = hierarchy.get('overall_trend_position', '?')
        transitioning = hierarchy.get('regime_transitioning', False)
        lines.append(f"  [Integrated] alignment={align:.2f}({a_type}), direction_combo={pos_key}, trend_position={trend_pos}" +
                     (", regime transitioning!" if transitioning else ""))

    if combined:
        lines.append(f"  [Final] score={combined.get('signal_score', 0):.2f}, action={combined.get('action', '?')}, "
                     f"target={combined.get('target_price', 0)}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tool registration function
# ---------------------------------------------------------------------------

def register_signal_tools(mcp, cache):
    """Register signal tools"""

    @mcp.tool()
    def get_signals(
        market_id: str,
        coin: str = None,
        interval: str = None,
        action_filter: str = None,
        min_score: float = None,
        min_confidence: float = None,
        limit: int = 500,
        hours_back: int = 24,
    ) -> Dict[str, Any]:
        """
        Signal query (per-symbol DB mode)

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock)
            coin: Symbol (queries only that symbol's DB if specified)
            interval: Interval (15m, 30m, 240m, 1d, combined, etc.) (optional)
            action_filter: Action filter (buy, sell, hold) (optional)
            min_score: Minimum signal score (optional)
            min_confidence: Minimum confidence (optional)
            limit: Number of results (default 20)
            hours_back: Within last N hours (default 24)

        Returns:
            Signal list and statistics
        """
        return _get_latest_signals(
            market_id=market_id,
            coin=coin,
            interval=interval,
            action_filter=action_filter,
            min_score=min_score,
            min_confidence=min_confidence,
            limit=limit,
            hours_back=hours_back,
        )

    @mcp.tool()
    def get_signal_detail(
        market_id: str,
        coin: str,
        interval: str = "combined",
    ) -> Dict[str, Any]:
        """
        Detailed signal information for a specific symbol (direct per-symbol DB query)

        Args:
            market_id: Market ID
            coin: Symbol
            interval: Interval (default combined)

        Returns:
            Latest signal detail + history + feedback
        """
        return _get_coin_signal_detail(
            market_id=market_id,
            coin=coin,
            interval=interval,
        )

    @mcp.tool()
    def get_role_analysis(
        market_id: str,
        coin: str,
    ) -> Dict[str, Any]:
        """
        Role-based (timing/trend/swing/regime) signal analysis for a symbol

        Returns structured data for each role's current state, direction, warnings,
        inter-role alignment score, and trend progress based on hierarchy_context.

        Args:
            market_id: Market ID (crypto, kr_stock, us_stock)
            coin: Symbol

        Returns:
            Role-based signals + integrated hierarchy info
        """
        return _get_role_analysis(market_id=market_id, coin=coin)

    logger.info("  [OK] Signal Tools registered (per-symbol DB mode, +role_analysis)")
