# -*- coding: utf-8 -*-
"""
Signal System Resources (종목별 DB 대응)
========================================
signals/ 디렉터리 내 종목별 시그널 DB를 순회하여
시장 전체 시그널 요약/피드백 Resource를 제공합니다.
"""

from __future__ import annotations

import asyncio
import sqlite3
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcps.config import (
    CACHE_TTL_MARKET_STATUS,
    get_signals_dir,
    list_signal_db_files,
    get_symbol_from_signal_db,
    get_market_db_path,
)
from mcps.resources.resource_response import to_resource_text, mcp_error, MCPErrorCode, MCPErrorAction, wrap_with_ai_summary

logger = logging.getLogger("MarketMCP")

# ---------------------------------------------------------------------------
# 헬퍼: 종목별 DB를 순회하며 쿼리 실행 후 결과 합산
# ---------------------------------------------------------------------------

_MAX_SIGNAL_DBS = 150  # DB 순회 최대 한도
_DB_POOL_SIZE = 20     # 병렬 DB 조회 스레드 수
_DB_TIMEOUT = 2.0      # 개별 DB 연결 타임아웃 (초)

def _query_single_db(db_file: Path, query: str, params=()) -> list:
    """단일 DB 쿼리 (스레드풀 워커용)."""
    try:
        with sqlite3.connect(str(db_file), timeout=_DB_TIMEOUT) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(query, params).fetchall()]
    except Exception:
        return []


def _query_all_signal_dbs(market_id: str, query: str, params=(), aggregate: str = "rows") -> list:
    """시장의 모든 종목별 시그널 DB에서 쿼리 병렬 실행 후 결과 수집."""
    db_files = list_signal_db_files(market_id)[:_MAX_SIGNAL_DBS]
    all_rows = []
    with ThreadPoolExecutor(max_workers=_DB_POOL_SIZE) as pool:
        futures = {pool.submit(_query_single_db, f, query, params): f for f in db_files}
        for fut in as_completed(futures):
            all_rows.extend(fut.result())
    return all_rows


# ---------------------------------------------------------------------------
# 시그널 요약 Resource
# ---------------------------------------------------------------------------

def _query_single_db_summary(db_file: Path) -> dict:
    """단일 DB에서 시그널 요약 데이터 수집 (병렬 워커용)."""
    try:
        with sqlite3.connect(str(db_file), timeout=_DB_TIMEOUT) as conn:
            conn.row_factory = sqlite3.Row
            agg_rows = conn.execute("""
                SELECT action, COUNT(*) as cnt,
                       AVG(signal_score) as avg_s, AVG(confidence) as avg_c,
                       interval
                FROM signals
                WHERE timestamp > (strftime('%s', 'now') - 86400)
                GROUP BY action, interval
            """).fetchall()
            top_row = conn.execute("""
                SELECT symbol, interval, signal_score, confidence, action,
                       current_price, rsi, macd, wave_phase, risk_level,
                       integrated_direction, integrated_strength, reason
                FROM signals
                WHERE interval = 'combined'
                ORDER BY rowid DESC LIMIT 1
            """).fetchone()
            return {
                "agg": [dict(r) for r in agg_rows],
                "top": dict(top_row) if top_row else None,
            }
    except Exception:
        return {"agg": [], "top": None}


def _load_signals_summary(market_id: str) -> Dict[str, Any]:
    """시그널 요약 정보 로드 (병렬 DB 조회)"""
    sig_dir = get_signals_dir(market_id)
    signal_dbs = list_signal_db_files(market_id)

    if not sig_dir or not sig_dir.exists() or not signal_dbs:
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Signal DB not found for market: {market_id}",
            action=MCPErrorAction.CHECK,
            fallback_note="available_markets: crypto, kr_stock, us_stock",
        )

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

        # 병렬 DB 조회 — 150개를 20 스레드로 병렬 처리 (~15초)
        db_subset = signal_dbs[:_MAX_SIGNAL_DBS]
        with ThreadPoolExecutor(max_workers=_DB_POOL_SIZE) as pool:
            futures = {pool.submit(_query_single_db_summary, f): f for f in db_subset}
            for fut in as_completed(futures):
                db_result = fut.result()
                for row in db_result["agg"]:
                    cnt = row.get("cnt") or 0
                    if cnt == 0:
                        continue
                    total_signals += cnt
                    score_sum += (row.get("avg_s") or 0) * cnt
                    conf_sum += (row.get("avg_c") or 0) * cnt
                    if row.get("interval"):
                        all_intervals.add(row["interval"])
                    act = (row.get("action") or "unknown").lower()
                    if act not in action_agg:
                        action_agg[act] = {"count": 0, "score_sum": 0.0}
                    action_agg[act]["count"] += cnt
                    action_agg[act]["score_sum"] += (row.get("avg_s") or 0) * cnt
                if db_result["top"]:
                    top_candidates.append(db_result["top"])
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
        result.update(mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            str(e),
            action=MCPErrorAction.RETRY,
            action_value="30",
        ))

    return result


def _generate_signals_summary(market_id: str, data: Dict) -> str:
    """LLM용 시그널 요약 텍스트"""
    lines = [f"[{market_id.upper()} 시그널 요약]"]

    stats = data.get("stats", {})
    if not stats.get("error"):
        lines.append(f"- 24시간 시그널: {stats.get('total_signals_24h', 0)}건, {stats.get('unique_symbols', 0)}종목")
        lines.append(f"- 평균 점수: {stats.get('avg_signal_score', 0):.2f}, 평균 신뢰도: {stats.get('avg_confidence', 0):.2f}")

    action_dist = data.get("action_distribution", {})
    if not isinstance(action_dist, dict) or not action_dist.get("error"):
        buy_count = action_dist.get("buy", {}).get("count", 0)
        sell_count = action_dist.get("sell", {}).get("count", 0)
        hold_count = action_dist.get("hold", {}).get("count", 0)
        lines.append(f"- 액션 분포: 매수 {buy_count}, 매도 {sell_count}, 홀드 {hold_count}")

    top_signals = data.get("top_signals", [])
    if top_signals and isinstance(top_signals, list):
        lines.append("- 상위 시그널 (최근 1시간):")
        for s in top_signals[:3]:
            lines.append(f"  {s['symbol']}({s['interval']}): {s['action']} score={s['signal_score']:.2f}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 시그널 피드백 Resource (trading_system.db에서 조회)
# ---------------------------------------------------------------------------

def _load_signal_feedback(market_id: str) -> Dict[str, Any]:
    """시그널 피드백 (패턴별 성공률) 로드 - trading_system.db 사용"""
    trading_db = get_market_db_path(market_id)

    if not trading_db or not trading_db.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Trading DB not found for market: {market_id}",
            action=MCPErrorAction.CHECK,
            fallback_tool=f"market://{market_id}/signals/summary",
            fallback_note="시그널 요약에서 시장 존재 확인",
        )

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
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            str(e),
            action=MCPErrorAction.RETRY,
            action_value="30",
        )


# ---------------------------------------------------------------------------
# 역할별 시그널 요약 (시장 전체)
# ---------------------------------------------------------------------------

_ROLE_ORDER = ['regime', 'swing', 'trend', 'timing']
_ROLE_DESCRIPTIONS = {
    'timing': '실제 매수·매도 타이밍 최적화',
    'trend': '단기 추세의 지속/반전 신호 확인',
    'swing': '중기 추세와 파동 구조 파악',
    'regime': '시장 방향성 / 장기 레짐 구분',
}


def _load_signals_role_summary(market_id: str) -> Dict[str, Any]:
    """시장 전체 역할별 시그널 요약 (종목별 DB 순회, hierarchy_context 집계)"""
    import json as _json, os

    signal_dbs = list_signal_db_files(market_id)
    if not signal_dbs:
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"No signal DBs found for market: {market_id}",
            action=MCPErrorAction.CHECK,
            fallback_note="available_markets: crypto, kr_stock, us_stock",
        )

    # 인터벌 → 역할 매핑
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

    # 역할별 집계
    role_agg = {r: {'buy': 0, 'sell': 0, 'hold': 0, 'score_sum': 0.0, 'count': 0}
                for r in _ROLE_ORDER}
    alignment_sum = 0.0
    alignment_count = 0
    position_keys = {}

    def _query_role_single(db_file):
        try:
            with sqlite3.connect(str(db_file), timeout=_DB_TIMEOUT) as conn:
                conn.row_factory = sqlite3.Row
                sig_rows = conn.execute("""
                    SELECT interval, signal_score, action
                    FROM signals WHERE interval != 'combined'
                    ORDER BY rowid DESC LIMIT 20
                """).fetchall()
                hrow = conn.execute("""
                    SELECT hierarchy_context FROM signals
                    WHERE interval = 'combined'
                    ORDER BY rowid DESC LIMIT 1
                """).fetchone()
                return {
                    "sigs": [dict(r) for r in sig_rows],
                    "hctx": hrow['hierarchy_context'] if hrow and hrow['hierarchy_context'] else None,
                }
        except Exception:
            return {"sigs": [], "hctx": None}

    with ThreadPoolExecutor(max_workers=_DB_POOL_SIZE) as pool:
        futures = {pool.submit(_query_role_single, f): f for f in signal_dbs[:_MAX_SIGNAL_DBS]}
        for fut in as_completed(futures):
            db_result = fut.result()
            for row in db_result["sigs"]:
                iv = row.get('interval')
                role = role_map.get(iv)
                if not role or role not in role_agg:
                    continue
                agg = role_agg[role]
                act = (row.get('action') or 'hold').lower()
                if act in agg:
                    agg[act] += 1
                agg['score_sum'] += float(row.get('signal_score') or 0)
                agg['count'] += 1
            if db_result["hctx"]:
                try:
                    hctx = _json.loads(db_result["hctx"])
                    al = float(hctx.get('alignment_score', 0.5))
                    alignment_sum += al
                    alignment_count += 1
                    pk = hctx.get('position_key', '')
                    if pk:
                        position_keys[pk] = position_keys.get(pk, 0) + 1
                except Exception:
                    pass

    # 역할별 요약 포맷
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

    # 통합 메타
    top_combos = sorted(position_keys.items(), key=lambda x: x[1], reverse=True)[:5]

    # LLM 요약
    lines = [f"[{market_id.upper()} 역할별 시그널 요약]"]
    for role in _ROLE_ORDER:
        r = roles[role]
        lines.append(f"  {role}({r['interval']}): {r['signal_count']}건, "
                     f"avg={r['avg_score']:.2f}, buy={r['action_distribution']['buy']}, "
                     f"sell={r['action_distribution']['sell']}")
    if alignment_count > 0:
        avg_align = alignment_sum / alignment_count
        lines.append(f"  [통합] 평균 alignment={avg_align:.2f} ({alignment_count}종목)")
    if top_combos:
        lines.append(f"  [조합] 상위: {', '.join(f'{k}({v})' for k, v in top_combos[:3])}")

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
# AI Summary
# ---------------------------------------------------------------------------

def _ai_summary_signals(data: dict) -> str:
    stats = data.get("stats", {})
    total = stats.get("total_signals_24h", 0)
    symbols = stats.get("unique_symbols", 0)
    avg_score = stats.get("avg_signal_score", 0)
    dist = data.get("action_distribution", {})
    buy = dist.get("BUY", {}).get("count", 0)
    sell = dist.get("SELL", {}).get("count", 0)
    hold = dist.get("HOLD", {}).get("count", 0)
    top_sigs = data.get("top_signals", [])
    top_part = ""
    if top_sigs:
        s = top_sigs[0]
        top_part = f" 상위: {s.get('symbol', '?')}({s.get('action', '?')}, {s.get('signal_score', 0):.1f})"
    market_id = data.get("market_id", "")
    return f"{market_id} 시그널: 24h {total}건, {symbols}종목. 매수/매도/홀드={buy}/{sell}/{hold}. avg점수={avg_score:.1f}.{top_part}"


# ---------------------------------------------------------------------------
# Resource 등록 함수
# ---------------------------------------------------------------------------

_RESOURCE_TIMEOUT = 30  # 개별 리소스 최대 처리 시간 (초)


async def _safe_load(func, *args) -> Any:
    """asyncio.to_thread + timeout 보호. 타임아웃 시 에러 dict 반환."""
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(func, *args),
            timeout=_RESOURCE_TIMEOUT,
        )
    except asyncio.TimeoutError:
        logger.warning("[Resource] %s timeout (%ds)", func.__name__, _RESOURCE_TIMEOUT)
        err = mcp_error(
            MCPErrorCode.TIMEOUT,
            f"Resource timeout ({_RESOURCE_TIMEOUT}s)",
            action=MCPErrorAction.RETRY,
            action_value="30",
        )
        err["_timeout"] = True
        return err
    except Exception as e:
        logger.error("[Resource] %s error: %s", func.__name__, e)
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            str(e),
            action=MCPErrorAction.RETRY,
            action_value="30",
        )


def register_signal_resources(mcp, cache):
    """시그널 시스템 Resource 등록"""

    @mcp.resource("market://{market_id}/signals/summary")
    async def signals_summary(market_id: str) -> Dict[str, Any]:
        """[역할] 시장 전체 시그널 집계(24h 시그널 수, 액션 분포, 상위 시그널). [호출 시점] 시장 시그널 동향 파악 시. 개별 종목 전에 먼저 호출. [선행 조건] 없음 (시그널 최상위). [후속 추천] get_signals(market_id, coin), market://{market_id}/signals/roles. [주의] 종목별 DB 병렬 순회(최대 150개). TTL=300초. 응답 15초+. [출력 스키마] ai_summary 래핑. full_data: market_id(str), db_count(int), stats{total_signals_24h,unique_symbols,avg_signal_score,avg_confidence}, action_distribution{action→{count,avg_score}}, top_signals[{symbol,interval,signal_score,confidence,action,rsi,macd,wave_phase,direction,strength,reason}], _llm_summary(str)."""
        cache_key = f"signals_summary_{market_id}"
        cached = cache.get(cache_key, ttl=300)
        if cached:
            if not cached.get("error"):
                cached = wrap_with_ai_summary(cached, "signal_system", _ai_summary_signals)
            return to_resource_text(cached)
        data = await _safe_load(_load_signals_summary, market_id)
        if not data.get("_timeout"):
            cache.set(cache_key, data)
        if not data.get("error"):
            data = wrap_with_ai_summary(data, "signal_system", _ai_summary_signals)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/signals/feedback")
    async def signals_feedback(market_id: str) -> Dict[str, Any]:
        """[역할] 시그널 패턴/전략별 성공률 데이터. [호출 시점] 시그널 패턴 성공률 분석 시. [선행 조건] signals/summary 권장. [후속 추천] analyze_trades. [주의] 최소 10건 이상 거래 패턴만. TTL=300초. [출력 스키마] market_id(str), top_patterns[{pattern,success_rate,avg_profit,total_trades,confidence}], top_strategies[{strategy,market_condition,success_rate,avg_profit,total_trades}]."""
        cache_key = f"signals_feedback_{market_id}"
        cached = cache.get(cache_key, ttl=300)
        if cached:
            return to_resource_text(cached)
        data = await _safe_load(_load_signal_feedback, market_id)
        if not data.get("_timeout"):
            cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/signals/roles")
    async def signals_roles_summary(market_id: str) -> Dict[str, Any]:
        """[역할] 역할별(timing/trend/swing/regime) 시그널 집계와 계층 정렬도. [호출 시점] 시간프레임간 시그널 일치/불일치 분석 시. [선행 조건] signals/summary 권장. [후속 추천] get_role_analysis(market_id, coin). [주의] hierarchy_context 기반. TTL=300초. [출력 스키마] market_id(str), roles{role→{interval,description,signal_count,avg_score,action_distribution}}, hierarchy_meta{avg_alignment,symbols_with_hierarchy}, _llm_summary(str)."""
        cache_key = f"signals_roles_{market_id}"
        cached = cache.get(cache_key, ttl=300)
        if cached:
            return to_resource_text(cached)
        data = await _safe_load(_load_signals_role_summary, market_id)
        if not data.get("_timeout"):
            cache.set(cache_key, data)
        return to_resource_text(data)

    logger.info("  Signal System Resources registered (per-symbol DB mode, +roles)")
