# -*- coding: utf-8 -*-
"""
Signal System Resources (per-symbol DB based)
==============================================
Iterates over per-symbol signal DBs in the signals/ directory
to provide market-wide signal summary/feedback Resources.
"""

from __future__ import annotations

import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.config import (
    CACHE_TTL_MARKET_STATUS,
    get_signals_dir,
    list_signal_db_files,
    get_symbol_from_signal_db,
    get_market_db_path,
)
from oneqaz_trading_mcp.response import to_resource_text

logger = logging.getLogger("MarketMCP")

# ---------------------------------------------------------------------------
# Helper: iterate per-symbol DBs, run query, and aggregate results
# ---------------------------------------------------------------------------

_MAX_SIGNAL_DBS = 300  # Max DB iteration limit (US market ~450 -> timeout prevention)

def _query_all_signal_dbs(market_id: str, query: str, params=(), aggregate: str = "rows") -> list:
    """Run a query across all per-symbol signal DBs for a market and collect results. aggregate: rows = return all rows as list"""
    all_rows = []
    for db_file in list_signal_db_files(market_id)[:_MAX_SIGNAL_DBS]:
        try:
            with sqlite3.connect(str(db_file), timeout=2.0) as conn:
                conn.row_factory = sqlite3.Row
                for row in conn.execute(query, params).fetchall():
                    all_rows.append(dict(row))
        except Exception:
            continue
    return all_rows


# ---------------------------------------------------------------------------
# Signals Summary Resource
# ---------------------------------------------------------------------------

def _load_signals_summary(market_id: str) -> Dict[str, Any]:
    """Load signal summary info (single-loop stats + actions + top aggregation)"""
    sig_dir = get_signals_dir(market_id)
    signal_dbs = list_signal_db_files(market_id)

    if not sig_dir or not sig_dir.exists() or not signal_dbs:
        return {
            "error": f"Signal DB not found for market: {market_id}",
            "available_markets": ["crypto", "kr_stock", "us_stock"],
        }

    result = {
        "market_id": market_id,
        "signals_dir": str(sig_dir),
        "db_count": len(signal_dbs),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        total_signals = 0
        score_sum = 0.0
        conf_sum = 0.0
        all_intervals: set = set()
        action_agg: Dict[str, Dict] = {}
        top_candidates: List[Dict] = []

        # Single DB loop — collect stats + action distribution + top signals simultaneously
        for db_file in signal_dbs[:_MAX_SIGNAL_DBS]:
            try:
                with sqlite3.connect(str(db_file), timeout=2.0) as conn:
                    conn.row_factory = sqlite3.Row

                    # 24h aggregation (stats + actions)
                    for row in conn.execute("""
                        SELECT action, COUNT(*) as cnt,
                               AVG(signal_score) as avg_s, AVG(confidence) as avg_c,
                               interval
                        FROM signals
                        WHERE timestamp > (strftime('%s', 'now') - 86400)
                        GROUP BY action, interval
                    """).fetchall():
                        cnt = row["cnt"] or 0
                        if cnt == 0:
                            continue
                        total_signals += cnt
                        score_sum += (row["avg_s"] or 0) * cnt
                        conf_sum += (row["avg_c"] or 0) * cnt
                        if row["interval"]:
                            all_intervals.add(row["interval"])
                        act = (row["action"] or "unknown").lower()
                        if act not in action_agg:
                            action_agg[act] = {"count": 0, "score_sum": 0.0}
                        action_agg[act]["count"] += cnt
                        action_agg[act]["score_sum"] += (row["avg_s"] or 0) * cnt

                    # Last 1 hour combined top (max 1 per DB)
                    top_row = conn.execute("""
                        SELECT symbol, interval, signal_score, confidence, action,
                               current_price, rsi, macd, wave_phase, risk_level,
                               integrated_direction, integrated_strength, reason
                        FROM signals
                        WHERE timestamp > (strftime('%s', 'now') - 3600)
                          AND interval = 'combined'
                        ORDER BY signal_score DESC LIMIT 1
                    """).fetchone()
                    if top_row:
                        top_candidates.append(dict(top_row))
            except Exception:
                continue

        result["stats"] = {
            "total_signals_24h": total_signals,
            "unique_symbols": len(signal_dbs),
            "unique_intervals": len(all_intervals),
            "avg_signal_score": round(score_sum / total_signals, 3) if total_signals > 0 else 0,
            "avg_confidence": round(conf_sum / total_signals, 3) if total_signals > 0 else 0,
        }

        result["action_distribution"] = {
            act: {
                "count": v["count"],
                "avg_score": round(v["score_sum"] / v["count"], 3) if v["count"] else 0,
            } for act, v in action_agg.items()
        }

        top_candidates.sort(key=lambda x: x.get("signal_score") or 0, reverse=True)
        top_signals = []
        for row in top_candidates[:10]:
            top_signals.append({
                "symbol": row.get("symbol"),
                "interval": row.get("interval"),
                "signal_score": round(row.get("signal_score") or 0, 3),
                "confidence": round(row.get("confidence") or 0, 3),
                "action": row.get("action"),
                "current_price": row.get("current_price"),
                "rsi": round(row["rsi"], 1) if row.get("rsi") else None,
                "macd": round(row["macd"], 4) if row.get("macd") else None,
                "wave_phase": row.get("wave_phase"),
                "risk_level": row.get("risk_level"),
                "direction": row.get("integrated_direction"),
                "strength": round(row["integrated_strength"], 3) if row.get("integrated_strength") else None,
                "reason": row.get("reason"),
            })
        result["top_signals"] = top_signals
        result["_llm_summary"] = _generate_signals_summary(market_id, result)

    except Exception as e:
        logger.error(f"Failed to load signals summary for {market_id}: {e}")
        result["error"] = str(e)

    return result


def _generate_signals_summary(market_id: str, data: Dict) -> str:
    """Generate signal summary text for LLM consumption"""
    lines = [f"[{market_id.upper()} Signals Summary]"]

    stats = data.get("stats", {})
    if not stats.get("error"):
        lines.append(f"- 24h signals: {stats.get('total_signals_24h', 0)} entries, {stats.get('unique_symbols', 0)} symbols")
        lines.append(f"- avg score: {stats.get('avg_signal_score', 0):.2f}, avg confidence: {stats.get('avg_confidence', 0):.2f}")

    action_dist = data.get("action_distribution", {})
    if not isinstance(action_dist, dict) or not action_dist.get("error"):
        buy_count = action_dist.get("buy", {}).get("count", 0)
        sell_count = action_dist.get("sell", {}).get("count", 0)
        hold_count = action_dist.get("hold", {}).get("count", 0)
        lines.append(f"- Action distribution: buy {buy_count}, sell {sell_count}, hold {hold_count}")

    top_signals = data.get("top_signals", [])
    if top_signals and isinstance(top_signals, list):
        lines.append("- Top signals (last 1 hour):")
        for s in top_signals[:3]:
            lines.append(f"  {s['symbol']}({s['interval']}): {s['action']} score={s['signal_score']:.2f}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Signal Feedback Resource (queried from trading_system.db)
# ---------------------------------------------------------------------------

def _load_signal_feedback(market_id: str) -> Dict[str, Any]:
    """Load signal feedback (per-pattern success rates) from trading_system.db"""
    trading_db = get_market_db_path(market_id)

    if not trading_db or not trading_db.exists():
        return {"error": f"Trading DB not found for market: {market_id}"}

    try:
        with sqlite3.connect(str(trading_db)) as conn:
            conn.row_factory = sqlite3.Row
            result = {
                "market_id": market_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            try:
                cursor = conn.execute("""
                    SELECT signal_pattern, success_rate, avg_profit, total_trades, confidence
                    FROM signal_feedback_scores
                    WHERE total_trades >= 10
                    ORDER BY success_rate DESC
                    LIMIT 20
                """)
                patterns = []
                for row in cursor.fetchall():
                    patterns.append({
                        "pattern": row["signal_pattern"],
                        "success_rate": round(row["success_rate"] or 0, 3),
                        "avg_profit": round(row["avg_profit"] or 0, 3),
                        "total_trades": row["total_trades"],
                        "confidence": round(row["confidence"] or 0, 3),
                    })
                result["top_patterns"] = patterns
            except Exception as e:
                result["top_patterns"] = {"error": str(e)}

            try:
                cursor = conn.execute("""
                    SELECT strategy_type, market_condition, success_rate, avg_profit, total_trades
                    FROM strategy_feedback
                    WHERE total_trades >= 5
                    ORDER BY success_rate DESC
                    LIMIT 15
                """)
                strategies = []
                for row in cursor.fetchall():
                    strategies.append({
                        "strategy": row["strategy_type"],
                        "market_condition": row["market_condition"],
                        "success_rate": round(row["success_rate"] or 0, 3),
                        "avg_profit": round(row["avg_profit"] or 0, 3),
                        "total_trades": row["total_trades"],
                    })
                result["top_strategies"] = strategies
            except Exception as e:
                result["top_strategies"] = {"error": str(e)}

            return result

    except Exception as e:
        logger.error(f"Failed to load signal feedback for {market_id}: {e}")
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Role-based Signal Summary (market-wide)
# ---------------------------------------------------------------------------

_ROLE_ORDER = ['regime', 'swing', 'trend', 'timing']
_ROLE_DESCRIPTIONS = {
    'timing': 'Entry/exit timing optimization',
    'trend': 'Short-term trend continuation/reversal confirmation',
    'swing': 'Mid-term trend and wave structure analysis',
    'regime': 'Market direction / long-term regime classification',
}


def _load_signals_role_summary(market_id: str) -> Dict[str, Any]:
    """Market-wide role-based signal summary (per-symbol DB iteration, hierarchy_context aggregation)"""
    import json as _json, os

    signal_dbs = list_signal_db_files(market_id)
    if not signal_dbs:
        return {"error": f"No signal DBs found for market: {market_id}"}

    # Interval -> role mapping
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

    # Role-based aggregation
    role_agg = {r: {'buy': 0, 'sell': 0, 'hold': 0, 'score_sum': 0.0, 'count': 0}
                for r in _ROLE_ORDER}
    alignment_sum = 0.0
    alignment_count = 0
    position_keys = {}

    for db_file in signal_dbs[:_MAX_SIGNAL_DBS]:
        try:
            with sqlite3.connect(str(db_file), timeout=2.0) as conn:
                conn.row_factory = sqlite3.Row

                # Latest signal per interval
                for row in conn.execute("""
                    SELECT interval, signal_score, action
                    FROM signals
                    WHERE timestamp > (strftime('%s', 'now') - 3600)
                      AND interval != 'combined'
                    ORDER BY timestamp DESC
                """).fetchall():
                    iv = row['interval']
                    role = role_map.get(iv)
                    if not role or role not in role_agg:
                        continue
                    agg = role_agg[role]
                    act = (row['action'] or 'hold').lower()
                    if act in agg:
                        agg[act] += 1
                    agg['score_sum'] += float(row['signal_score'] or 0)
                    agg['count'] += 1

                # Read hierarchy_context from combined
                hrow = conn.execute("""
                    SELECT hierarchy_context FROM signals
                    WHERE interval = 'combined'
                    ORDER BY timestamp DESC LIMIT 1
                """).fetchone()
                if hrow and hrow['hierarchy_context']:
                    try:
                        hctx = _json.loads(hrow['hierarchy_context'])
                        al = float(hctx.get('alignment_score', 0.5))
                        alignment_sum += al
                        alignment_count += 1
                        pk = hctx.get('position_key', '')
                        if pk:
                            position_keys[pk] = position_keys.get(pk, 0) + 1
                    except Exception:
                        pass
        except Exception:
            continue

    # Format role summary
    roles = {}
    for role in _ROLE_ORDER:
        agg = role_agg[role]
        cnt = agg['count']
        iv = next((k for k, v in role_map.items() if v == role), '?')
        roles[role] = {
            'interval': iv,
            'description': _ROLE_DESCRIPTIONS.get(role, ''),
            'signal_count': cnt,
            'avg_score': round(agg['score_sum'] / cnt, 3) if cnt > 0 else 0,
            'action_distribution': {
                'buy': agg['buy'],
                'sell': agg['sell'],
                'hold': agg['hold'],
            },
        }

    # Combined meta
    top_combos = sorted(position_keys.items(), key=lambda x: x[1], reverse=True)[:5]

    # LLM summary
    lines = [f"[{market_id.upper()} Role-based signal summary]"]
    for role in _ROLE_ORDER:
        r = roles[role]
        lines.append(f"  {role}({r['interval']}): {r['signal_count']} entries, "
                     f"avg={r['avg_score']:.2f}, buy={r['action_distribution']['buy']}, "
                     f"sell={r['action_distribution']['sell']}")
    if alignment_count > 0:
        avg_align = alignment_sum / alignment_count
        lines.append(f"  [Combined] avg alignment={avg_align:.2f} ({alignment_count} symbols)")
    if top_combos:
        lines.append(f"  [Combination] top: {', '.join(f'{k}({v})' for k, v in top_combos[:3])}")

    return {
        'market_id': market_id,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'roles': roles,
        'hierarchy_meta': {
            'avg_alignment': round(alignment_sum / alignment_count, 3) if alignment_count > 0 else 0.5,
            'symbols_with_hierarchy': alignment_count,
            'top_position_keys': dict(top_combos),
        },
        '_llm_summary': "\n".join(lines),
    }


# ---------------------------------------------------------------------------
# Resource Registration
# ---------------------------------------------------------------------------

def register_signal_resources(mcp, cache):
    """Register signal system Resources"""

    @mcp.resource("market://{market_id}/signals/summary")
    def signals_summary(market_id: str) -> Dict[str, Any]:
        """Signal summary info (per-symbol DB aggregation). Returns: stats, action_distribution, top_signals"""
        cache_key = f"signals_summary_{market_id}"
        cached = cache.get(cache_key, ttl=300)
        if cached:
            return to_resource_text(cached)
        data = _load_signals_summary(market_id)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/signals/feedback")
    def signals_feedback(market_id: str) -> Dict[str, Any]:
        """Signal feedback (per-pattern/strategy success rates). Returns: top_patterns, top_strategies"""
        cache_key = f"signals_feedback_{market_id}"
        cached = cache.get(cache_key, ttl=300)
        if cached:
            return to_resource_text(cached)
        data = _load_signal_feedback(market_id)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/signals/roles")
    def signals_roles_summary(market_id: str) -> Dict[str, Any]:
        """Role-based (timing/trend/swing/regime) signal summary. Returns: roles, hierarchy_meta"""
        cache_key = f"signals_roles_{market_id}"
        cached = cache.get(cache_key, ttl=300)
        if cached:
            return to_resource_text(cached)
        data = _load_signals_role_summary(market_id)
        cache.set(cache_key, data)
        return to_resource_text(data)

    logger.info("  Signal System Resources registered (per-symbol DB mode, +roles)")
