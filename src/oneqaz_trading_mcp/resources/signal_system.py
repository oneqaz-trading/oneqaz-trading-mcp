# -*- coding: utf-8 -*-
"""
Signal System Resources (종목별 DB 대응)
========================================
signals/ 디렉터리 내 종목별 시그널 DB를 순회하여
시장 전체 시그널 요약/피드백 Resource를 제공합니다.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.config import (
    CACHE_TTL_MARKET_STATUS,
    get_signals_dir,
    list_signal_db_files,  # legacy 진단 함수(_load_signals_*_legacy) 전용 잔존
    get_market_db_path,
    connect_readonly,
)
from oneqaz_trading_mcp.resources.resource_response import to_resource_text, mcp_error, MCPErrorCode, MCPErrorAction, wrap_with_ai_summary

logger = logging.getLogger("MarketMCP")

# ---------------------------------------------------------------------------
# 헬퍼: 종목별 DB를 순회하며 쿼리 실행 후 결과 합산
# ---------------------------------------------------------------------------

_MAX_SIGNAL_DBS = 150  # DB 순회 최대 한도
_DB_POOL_SIZE = 20     # 병렬 DB 조회 스레드 수
_DB_TIMEOUT = 2.0      # 개별 DB 연결 타임아웃 (초)


# 이전에는 로컬 helper를 썼지만 mcps.config.connect_readonly 공용 버전 사용.
# 호환성 위해 같은 이름 alias만 유지.
_connect_readonly = connect_readonly


def _query_single_db(db_file: Path, query: str, params=()) -> list:
    """단일 DB 쿼리 (스레드풀 워커용) — read-only."""
    try:
        with _connect_readonly(db_file) as conn:
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
    """단일 DB에서 시그널 요약 데이터 수집 (병렬 워커용) — read-only."""
    try:
        with _connect_readonly(db_file) as conn:
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
                ORDER BY timestamp DESC LIMIT 1
            """).fetchone()
            return {
                "agg": [dict(r) for r in agg_rows],
                "top": dict(top_row) if top_row else None,
            }
    except Exception:
        return {"agg": [], "top": None}


_SUMMARY_STALENESS = 600  # 요약 JSON이 이보다 오래되면 legacy fallback (초)

# market_id → PG 스키마 매핑. crypto/coin 별칭 흡수.
_MARKET_PG_SCHEMA = {
    "crypto": "market_coin",
    "coin": "market_coin",
    "kr_stock": "market_kr",
    "kr": "market_kr",
    "us_stock": "market_us",
    "us": "market_us",
}

# interval 파티션 프루닝을 위한 화이트리스트. signals_other(DEFAULT)는 제외.
# → 5파티션 Parallel Seq Scan 9.8s → 4파티션 프루닝 후 ~180ms.
_SIGNAL_PRUNE_INTERVALS = ("15m", "30m", "240m", "1d")

# 최근 몇 시간 윈도를 기본 집계 범위로 사용. 24h 는 수천만 row 누적으로 BRIN 에도 느리다.
_SUMMARY_WINDOW_SEC = 3600


def _load_signals_summary(market_id: str) -> Dict[str, Any]:
    """시그널 요약 로드 — PG 단일 쿼리 + interval 파티션 프루닝.

    [Wave H 이관 잔재] 원래 list_signal_db_files() 로 수백 SQLite 샤드를
    ThreadPool 로 스캔하다 30s 타임아웃. PG 로 이관 후 SQLite 는 stale.
    여기서는 market_{coin,kr,us}.signals 파티션 테이블에서 직접 집계.
    """
    schema = _MARKET_PG_SCHEMA.get(market_id.lower().replace("-", "_"))
    if not schema:
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Unknown market_id for signals summary: {market_id}",
            action=MCPErrorAction.CHECK,
            fallback_note="available_markets: crypto, kr_stock, us_stock",
        )

    return _load_signals_summary_pg(market_id, schema)


def _load_signals_summary_pg(market_id: str, schema: str) -> Dict[str, Any]:
    """PG market_{coin,kr,us}.signals 에서 최근 윈도 집계."""
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection

    result: Dict[str, Any] = {
        "market_id": market_id,
        "signals_dir": f"pg:{schema}.signals",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        conn = open_schema_connection(schema, readonly=True)
    except Exception as exc:
        logger.warning("PG connect failed for %s: %s", schema, exc)
        result.update(mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"PG schema {schema} unavailable: {exc}",
            action=MCPErrorAction.RETRY,
            action_value="15",
        ))
        return result

    try:
        cutoff = int(time.time()) - _SUMMARY_WINDOW_SEC
        # shim 은 qmark(?) → %s 변환. 여기서는 qmark 유지.
        placeholders = ",".join(["?"] * len(_SIGNAL_PRUNE_INTERVALS))

        # 1) action × interval 집계
        agg_cur = conn.execute(
            f"""
            SELECT action,
                   interval,
                   COUNT(*)                     AS cnt,
                   AVG(COALESCE(signal_score,0)) AS avg_s,
                   AVG(COALESCE(confidence,0))   AS avg_c
            FROM {schema}.signals
            WHERE interval IN ({placeholders})
              AND timestamp > ?
            GROUP BY action, interval
            """,
            (*_SIGNAL_PRUNE_INTERVALS, cutoff),
        )
        agg_rows = agg_cur.fetchall()

        total_signals = 0
        score_sum = 0.0
        conf_sum = 0.0
        all_intervals: set = set()
        action_agg: Dict[str, Dict[str, float]] = {}
        symbol_set: set = set()

        for row in agg_rows:
            act = (row[0] or "unknown").lower()
            interval = row[1]
            cnt = int(row[2] or 0)
            avg_s = float(row[3] or 0.0)
            avg_c = float(row[4] or 0.0)
            if cnt == 0:
                continue
            total_signals += cnt
            score_sum += avg_s * cnt
            conf_sum += avg_c * cnt
            if interval:
                all_intervals.add(interval)
            if act not in action_agg:
                action_agg[act] = {"count": 0, "score_sum": 0.0}
            action_agg[act]["count"] += cnt
            action_agg[act]["score_sum"] += avg_s * cnt

        # 2) 종목 수 (DISTINCT) — 별도 쿼리가 GROUP BY 보다 훨씬 싸다.
        sym_cur = conn.execute(
            f"""
            SELECT COUNT(DISTINCT symbol)
            FROM {schema}.signals
            WHERE interval IN ({placeholders})
              AND timestamp > ?
            """,
            (*_SIGNAL_PRUNE_INTERVALS, cutoff),
        )
        sym_row = sym_cur.fetchone()
        unique_symbols = int(sym_row[0] or 0) if sym_row else 0

        # 3) 상위 시그널 — [2026-06-15] 기존 LIMIT 10 + 발화 [:3] 은 매 사이클 같은
        # 최고득점 종목만 노출(수집 수천 종목 → 발화 3종목 반복의 직접 원인). 후보를
        # 넓게(40) 가져와 회전 풀로 삼는다. top_signals 페이로드는 여전히 ~12개로 잘라
        # 프롬프트 비대/캐시 무효화 최소화. 회전은 _rotate_signals 가 처리.
        top_cur = conn.execute(
            f"""
            SELECT symbol, interval, signal_score, confidence, action,
                   current_price, rsi, macd, wave_phase, risk_level,
                   integrated_direction, integrated_strength, reason
            FROM {schema}.signals
            WHERE interval IN ({placeholders})
              AND timestamp > ?
            ORDER BY signal_score DESC NULLS LAST
            LIMIT 40
            """,
            (*_SIGNAL_PRUNE_INTERVALS, cutoff),
        )
        top_rows = top_cur.fetchall()

        top_signals = []
        for r in top_rows:
            top_signals.append({
                "symbol": r[0],
                "interval": r[1],
                "signal_score": round(float(r[2] or 0), 3),
                "confidence": round(float(r[3] or 0), 3),
                "action": r[4],
                "current_price": r[5],
                "rsi": round(float(r[6]), 1) if r[6] is not None else None,
                "macd": round(float(r[7]), 4) if r[7] is not None else None,
                "wave_phase": r[8],
                "risk_level": r[9],
                "direction": r[10],
                "strength": round(float(r[11]), 3) if r[11] is not None else None,
                "reason": r[12],
            })

        result["db_count"] = 1  # 단일 PG 스키마
        result["stats"] = {
            "total_signals_1h": total_signals,
            "unique_symbols": unique_symbols,
            "unique_intervals": len(all_intervals),
            "avg_signal_score": round(score_sum / total_signals, 3) if total_signals else 0,
            "avg_confidence": round(conf_sum / total_signals, 3) if total_signals else 0,
            "window_sec": _SUMMARY_WINDOW_SEC,
        }
        result["action_distribution"] = {
            act: {
                "count": v["count"],
                "avg_score": round(v["score_sum"] / v["count"], 3) if v["count"] else 0,
            } for act, v in action_agg.items()
        }
        # [2026-06-15] 40 후보 → anchor 4 고정 + 8 회전 = 12 페이로드. 회전을 페이로드
        # 시점에 적용해 summary·market_agent·캐시가 모두 회전된 종목을 보게 한다(단일
        # 소스). 발화 요약은 이 12 안에서 다시 anchor2+rotate3 로 5종목 노출.
        result["top_signals"] = _rotate_signals(top_signals, anchor=4, rotate=8)
        result["_llm_summary"] = _generate_signals_summary(market_id, result)

    except Exception as exc:
        logger.error("PG signals summary failed for %s: %s", market_id, exc)
        result.update(mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            str(exc),
            action=MCPErrorAction.RETRY,
            action_value="30",
        ))
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return result


def _format_summary_from_cache(
    cached: Dict[str, Any], market_id: str, sig_dir: Path, signal_dbs: list
) -> Dict[str, Any]:
    """사전 집계 JSON → MCP 응답 포맷 변환."""
    result = {
        "market_id": market_id,
        "signals_dir": str(sig_dir),
        "db_count": cached.get("db_count", len(signal_dbs)),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stats": cached.get("stats", {}),
        "action_distribution": cached.get("action_distribution", {}),
        "top_signals": cached.get("top_signals", []),
    }
    result["_llm_summary"] = _generate_signals_summary(market_id, result)
    return result


def _load_signals_summary_legacy(
    market_id: str, sig_dir: Path = None, signal_dbs: list = None
) -> Dict[str, Any]:
    """시그널 요약 정보 로드 (병렬 DB 조회 — legacy fallback)"""
    if sig_dir is None:
        sig_dir = get_signals_dir(market_id)
    if signal_dbs is None:
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


# [2026-06-15] 시그널 회전 — 매 사이클 같은 최고득점 종목만 발화되던 것을 완화.
# 상위 anchor 개는 항상(시청자가 최강 시그널을 봐야 함), 그 아래는 time 오프셋으로
# 회전 노출. MCP 캐시 TTL=300s 라 win=300 이면 캐시 갱신마다 다른 종목이 돈다.
# 결정적(time 기반) — 워커 무관, Math.random 미사용. 후보 부족 시 그냥 있는 만큼.
def _rotate_signals(signals: list, *, anchor: int = 2, rotate: int = 3,
                    win: int = 300) -> list:
    """상위 anchor 고정 + 나머지에서 rotate 개를 시간 오프셋으로 회전 추출.
    같은 심볼은 최고 점수 1개만(다른 interval 중복 노출 방지 — BONK×2 같은 것)."""
    if not signals:
        return []
    # 심볼 dedup — signals 는 score DESC 정렬이라 첫 등장이 최고점
    _seen: set = set()
    deduped = []
    for s in signals:
        sym = s.get("symbol")
        if sym in _seen:
            continue
        _seen.add(sym)
        deduped.append(s)
    signals = deduped
    head = signals[:anchor]
    tail = signals[anchor:]
    if not tail or rotate <= 0:
        return head
    n = len(tail)
    off = (int(time.time()) // win) % n
    picked = [tail[(off + i) % n] for i in range(min(rotate, n))]
    return head + picked


def _generate_signals_summary(market_id: str, data: Dict) -> str:
    """LLM용 시그널 요약 텍스트"""
    lines = [f"[{market_id.upper()} 시그널 요약]"]

    stats = data.get("stats", {})
    if not stats.get("error"):
        # PG 집계는 1h, legacy fallback 은 24h. 둘 다 지원.
        window_label = "1시간" if stats.get("window_sec") == 3600 else "24시간"
        total = stats.get("total_signals_1h", stats.get("total_signals_24h", 0))
        lines.append(f"- {window_label} 시그널: {total}건, {stats.get('unique_symbols', 0)}종목")
        lines.append(f"- 평균 점수: {stats.get('avg_signal_score', 0):.2f}, 평균 신뢰도: {stats.get('avg_confidence', 0):.2f}")

    action_dist = data.get("action_distribution", {})
    if not isinstance(action_dist, dict) or not action_dist.get("error"):
        buy_count = action_dist.get("buy", {}).get("count", 0)
        sell_count = action_dist.get("sell", {}).get("count", 0)
        hold_count = action_dist.get("hold", {}).get("count", 0)
        lines.append(f"- 액션 분포: 매수 {buy_count}, 매도 {sell_count}, 홀드 {hold_count}")

    top_signals = data.get("top_signals", [])
    if top_signals and isinstance(top_signals, list):
        # [2026-06-15] 상위 2 고정 + 3 회전 = 5종목. 매 사이클 다른 종목이 섞여
        # '같은 시그널 반복' 완화. 방향/리스크도 노출해 발화 소재를 넓힌다.
        picked = _rotate_signals(top_signals, anchor=2, rotate=3)
        lines.append("- 상위 시그널 (최근 1시간, 일부 회전):")
        for s in picked:
            try:
                seg = f"  {s['symbol']}({s.get('interval','')}): {s.get('action','')} score={float(s.get('signal_score') or 0):.2f}"
            except (TypeError, ValueError):
                continue
            d = s.get("direction")
            if d and d not in ("nan", "neutral", "none"):
                seg += f", {d}"
            rsi = s.get("rsi")
            if isinstance(rsi, (int, float)):
                seg += f", RSI {rsi:.0f}"
            risk = s.get("risk_level")
            if risk and risk not in ("nan", "none"):
                seg += f", 리스크 {risk}"
            lines.append(seg)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 시그널 피드백 Resource (trading_system.db에서 조회)
# ---------------------------------------------------------------------------

def _load_signal_feedback(market_id: str) -> Dict[str, Any]:
    """시그널 피드백 (패턴별 성공률) 로드 - trading_system.db 사용"""
    trading_db = get_market_db_path(market_id)

    # [2026-08-10] .exists() 게이트 제거 — trading_db 는 PG 라우팅용 논리 키
    # (connect_readonly 가 PG 스키마로 라우팅, 실파일 안 엶 — trade_history.py 동일 수리)
    if not trading_db:
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Trading DB not found for market: {market_id}",
            action=MCPErrorAction.CHECK,
            fallback_tool=f"market://{market_id}/signals/summary",
            fallback_note="시그널 요약에서 시장 존재 확인",
        )

    try:
        # trading_db도 read-only — 동시 writer(virtual_trade_executor)와 격리
        with _connect_readonly(trading_db) as conn:
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
                logger.warning("top_patterns load failed: %s", e)
                result["top_patterns"] = {"unavailable": True, "reason": "데이터 수집 중"}

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
                logger.warning("top_strategies load failed: %s", e)
                result["top_strategies"] = {"unavailable": True, "reason": "데이터 수집 중"}

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
    """역할별 시그널 요약 — 사전 집계 JSON 우선, 없으면 PG LATERAL 집계.

    [2026-08-10] list_signal_db_files() 게이트/유니버스 제거 — 파일 glob 은
    2026-05 SQLite 동결 백업이라 이후 상장 심볼이 비가시였다. 심볼 유니버스는
    PG 경로가 직접 열거한다.
    """
    sig_dir = get_signals_dir(market_id)

    # Fast path: 사전 집계 JSON
    if sig_dir:
        roles_path = sig_dir / "_signal_roles_summary.json"
        try:
            with open(roles_path, "r") as f:
                cached = json.load(f)
            if time.time() - cached.get("updated_at", 0) < _SUMMARY_STALENESS:
                return _format_roles_from_cache(cached, market_id)
        except (FileNotFoundError, json.JSONDecodeError, KeyError, OSError):
            pass

    # PG 집계 경로 (2026-07-22) — 종전 legacy 150-DB 스캔(심볼당 쿼리 2개 = 300쿼리)을
    # LATERAL 2쿼리로 대체. _load_signals_summary_pg 선례처럼 PG 실패 시 mcp_error
    # (legacy 폴백 없음 — silent 강등 금지).
    schema = _MARKET_PG_SCHEMA.get(market_id.lower().replace("-", "_"))
    if not schema:
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Unknown market_id for roles summary: {market_id}",
            action=MCPErrorAction.CHECK,
            fallback_note="available_markets: crypto, kr_stock, us_stock",
        )
    return _load_signals_role_summary_pg(market_id, schema)


def _format_roles_from_cache(cached: Dict[str, Any], market_id: str) -> Dict[str, Any]:
    """사전 집계 roles JSON → MCP 응답 포맷 변환."""
    role_map = cached.get("role_map", {})
    role_agg = cached.get("role_agg", {})
    hmeta = cached.get("hierarchy_meta", {})

    roles = {}
    for role in _ROLE_ORDER:
        agg = role_agg.get(role, {})
        cnt = agg.get('count', 0)
        iv = next((k for k, v in role_map.items() if v == role), '?')
        roles[role] = {
            'interval': iv,
            'description': _ROLE_DESCRIPTIONS.get(role, ''),
            'signal_count': cnt,
            'avg_score': round(agg.get('score_sum', 0) / cnt, 3) if cnt > 0 else 0,
            'action_distribution': {
                'buy': agg.get('buy', 0),
                'sell': agg.get('sell', 0),
                'hold': agg.get('hold', 0),
            },
        }

    alignment_sum = hmeta.get('alignment_sum', 0)
    alignment_count = hmeta.get('alignment_count', 0)
    top_combos = sorted(hmeta.get('position_keys', {}).items(), key=lambda x: x[1], reverse=True)[:5]

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


def _build_role_map(market_id: str) -> Dict[str, str]:
    """시장별 인터벌 셋 → 역할(timing/trend/swing/regime) 매핑.

    [2026-07-22] 종전엔 전역 env CANDLE_INTERVALS(코인 기준 기본값)로 만들어
    kr/us(실제 5m/15m/30m/1d)에서 trend(240m)가 항상 0건이고 5m/30m 행이
    미매핑으로 버려졌다. data_collection.core.intervals.MARKET_INTERVALS
    중앙 상수를 시장별로 조회해 생성한다 — 분할 결과는 PG
    rl_pipeline.role_interval_map 시드와 동일 (coin: 15m=timing/30m=trend/
    240m=swing/1d=regime, kr/us: 5m=timing/15m=trend/30m=swing/1d=regime.
    시드의 kr/us 1w:regime 은 라벨링용 여분 — 라이브 signals 에 1w 행 없음).
    env 오버라이드는 프로세스 전역이라 3시장 동시 서빙과 양립 불가 — 미지원.
    """
    from oneqaz_trading_mcp.data_collection.core.intervals import MARKET_INTERVALS

    schema = _MARKET_PG_SCHEMA.get(market_id.lower().replace("-", "_"))
    if not schema:
        raise KeyError(f"unknown market_id for role map: {market_id!r}")
    ivs = sorted(
        MARKET_INTERVALS[schema.removeprefix("market_")],
        key=lambda iv: int(iv[:-1]) if iv.endswith('m') else (
            int(iv[:-1]) * 60 if iv.endswith('h') else int(iv[:-1]) * 1440
        )
    )
    role_map: Dict[str, str] = {}
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
    return role_map


def _format_role_summary(
    market_id: str,
    role_map: Dict[str, str],
    role_agg: Dict[str, Dict[str, Any]],
    alignment_sum: float,
    alignment_count: int,
    position_keys: Dict[str, int],
) -> Dict[str, Any]:
    """역할별 집계 → MCP 응답 포맷 (PG 경로/legacy 공용)."""
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

    top_combos = sorted(position_keys.items(), key=lambda x: x[1], reverse=True)[:5]

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


# 시장별 활성 interval 화이트리스트 캐시 (market_id → (fetched_at, [interval,...])).
# 중첩 LATERAL 이 interval 을 고정해야 coin(LIST 파티션 프루닝)/kr·us(sym+iv+ts 인덱스
# prefix)가 인덱스를 탄다. `interval <> 'combined'` 직접 필터는 coin signals_other
# (combined 전용 파티션)를 심볼당 전체 순회해 150심볼에 200s+ (2026-07-22 실측).
# 인터벌 셋은 배포 상수 수준이라 1h 캐시. 프로브 자체는 시장당 0.5~1.8s.
_ROLE_IV_CACHE: Dict[str, tuple] = {}
_ROLE_IV_TTL = 3600
_ROLE_IV_WINDOW_SEC = 86400

# 심볼 유니버스 열거 시 인터벌 브랜치당 최신순 스캔 상한 (tools/signals.py 의
# _SCAN_CAP_PER_INTERVAL 과 동일 근거 — 활성 500심볼 × 6사이클).
_SYMBOL_ENUM_CAP = 3000


def _get_active_intervals(conn, schema: str, market_id: str) -> List[str]:
    """최근에 실존하는 non-combined interval 목록 (캐시 1h).

    [2026-08-10] 종전 naive `SELECT DISTINCT interval ... timestamp > cutoff` 는
    240m/1d 파티션의 BRIN lossy bitmap 이 백만 행 recheck 를 유발, 데이터 증가로
    30s statement_timeout 초과(실측 — 콜드 시 roles 리소스 자체가 깨짐).
    인터벌 후보 셋은 배포 상수(MARKET_INTERVALS — 시그널 생성측과 동일 소스)라
    후보별 EXISTS 프로브((interval, timestamp DESC) btree, O(log))로 대체.
    24h 윈도가 기본. kr/us 주말·연휴처럼 24h 가 비면 14d 로 재시도(사다리 유지).
    """
    cached = _ROLE_IV_CACHE.get(market_id)
    if cached and time.time() - cached[0] < _ROLE_IV_TTL:
        return cached[1]
    from oneqaz_trading_mcp.data_collection.core.intervals import MARKET_INTERVALS

    candidates = [
        iv for iv in MARKET_INTERVALS.get(schema.removeprefix("market_"), ())
        if iv != "combined"
    ]
    intervals: List[str] = []
    for window in (_ROLE_IV_WINDOW_SEC, _ROLE_IV_WINDOW_SEC * 14):
        cutoff = int(time.time()) - window
        found: List[str] = []
        for iv in candidates:
            cur = conn.execute(
                f"SELECT 1 FROM {schema}.signals WHERE interval = ? AND timestamp > ? LIMIT 1",
                (iv, cutoff),
            )
            if cur.fetchone():
                found.append(iv)
        intervals = found
        if intervals:
            break
    if intervals:
        _ROLE_IV_CACHE[market_id] = (time.time(), intervals)
    return intervals


def _list_active_signal_symbols(
    conn, schema: str, intervals: List[str],
    window_sec: int = 86400, per_branch: int = _SYMBOL_ENUM_CAP,
) -> List[str]:
    """최근 윈도 내 발화 심볼 열거 — 인터벌별 최신순 캡 스캔의 DISTINCT.

    [2026-08-10] 종전 list_signal_db_files() 파일명 열거는 2026-05 SQLite 동결
    백업이라 신규 상장 심볼(coin 실측 27개: BSB 등)이 비가시. naive
    `SELECT DISTINCT symbol ... timestamp > cutoff` 는 240m/1d 파티션 BRIN
    lossy bitmap 이 백만 행 recheck 를 유발해 20s+ 타임아웃(실측) —
    (interval, timestamp DESC) btree 조기종료 브랜치 UNION 으로 대체.
    """
    if not intervals:
        return []
    branch = (
        f"(SELECT symbol FROM {schema}.signals"
        " WHERE interval = ? AND timestamp > ?"
        f" ORDER BY timestamp DESC LIMIT {int(per_branch)})"
    )
    union_sql = " UNION ALL ".join([branch] * len(intervals))
    cutoff = int(time.time()) - window_sec
    params: List[Any] = []
    for iv in intervals:
        params.extend([iv, cutoff])
    cur = conn.execute(f"SELECT DISTINCT symbol FROM ({union_sql}) u", tuple(params))
    return sorted(r[0] for r in cur.fetchall() if r[0])


def _load_signals_role_summary_pg(market_id: str, schema: str) -> Dict[str, Any]:
    """역할별 시그널 요약 — PG LATERAL 집계 (쿼리 3~4개).

    legacy(심볼 DB 150개 × 2쿼리 순회)와 동일 의미론:
    - 심볼 유니버스: PG 최근 발화 심볼 열거 (_list_active_signal_symbols,
      _MAX_SIGNAL_DBS 캡 동일 — 2026-08-10 동결 파일명 열거 대체)
    - 심볼당 최신 20행(combined 제외)의 interval×action 분포 → 역할 버킷
      (전역 top-20 ⊆ 인터벌별 top-20 의 합집합 — 중첩 LATERAL 로 동일 결과)
    - 심볼당 최신 combined 행의 hierarchy_context → alignment/조합 키 집계
    """
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection

    role_map = _build_role_map(market_id)
    role_agg = {r: {'buy': 0, 'sell': 0, 'hold': 0, 'score_sum': 0.0, 'count': 0}
                for r in _ROLE_ORDER}
    alignment_sum = 0.0
    alignment_count = 0
    position_keys: Dict[str, int] = {}

    try:
        conn = open_schema_connection(schema, readonly=True)
    except Exception as exc:
        logger.warning("PG connect failed for %s: %s", schema, exc)
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"PG schema {schema} unavailable: {exc}",
            action=MCPErrorAction.RETRY,
            action_value="15",
        )

    try:
        intervals = _get_active_intervals(conn, schema, market_id)

        # 심볼 유니버스: 활성 인터벌 + combined 브랜치에서 최근 발화 심볼 열거.
        symbols = _list_active_signal_symbols(
            conn, schema, list(intervals) + ["combined"],
        )[:_MAX_SIGNAL_DBS]
        if not symbols:
            return mcp_error(
                MCPErrorCode.DB_NOT_FOUND,
                f"No recent signals found for market: {market_id}",
                action=MCPErrorAction.CHECK,
                fallback_note="available_markets: crypto, kr_stock, us_stock",
            )

        # 1) 심볼별 최신 20행(combined 제외) → interval×action 집계.
        #    interval 고정 중첩 LATERAL: coin 은 LIST 파티션 프루닝,
        #    kr/us 는 (symbol, interval, timestamp DESC) 인덱스 prefix.
        sig_cur = conn.execute(
            f"""
            SELECT t.interval,
                   LOWER(COALESCE(NULLIF(t.action, ''), 'hold')) AS act,
                   COUNT(*)                                      AS cnt,
                   SUM(COALESCE(t.signal_score, 0))              AS score_sum
            FROM unnest(?::text[]) AS s(symbol)
            CROSS JOIN LATERAL (
                SELECT u.interval, u.action, u.signal_score
                FROM unnest(?::text[]) AS iv(interval)
                CROSS JOIN LATERAL (
                    SELECT sig.interval, sig.action, sig.signal_score,
                           sig.timestamp
                    FROM {schema}.signals sig
                    WHERE sig.symbol = s.symbol
                      AND sig.interval = iv.interval
                    ORDER BY sig.timestamp DESC
                    LIMIT 20
                ) u
                ORDER BY u.timestamp DESC
                LIMIT 20
            ) t
            GROUP BY 1, 2
            """,
            (symbols, intervals),
        )
        for row in sig_cur.fetchall():
            role = role_map.get(row[0])
            if not role or role not in role_agg:
                continue
            agg = role_agg[role]
            act = row[1]
            cnt = int(row[2] or 0)
            if act in agg:
                agg[act] += cnt
            agg['score_sum'] += float(row[3] or 0.0)
            agg['count'] += cnt

        # 2) 심볼별 최신 combined 행의 hierarchy_context.
        hctx_cur = conn.execute(
            f"""
            SELECT t.hierarchy_context
            FROM unnest(?::text[]) AS s(symbol)
            CROSS JOIN LATERAL (
                SELECT sig.hierarchy_context
                FROM {schema}.signals sig
                WHERE sig.symbol = s.symbol
                  AND sig.interval = 'combined'
                ORDER BY sig.timestamp DESC
                LIMIT 1
            ) t
            WHERE t.hierarchy_context IS NOT NULL
            """,
            (symbols,),
        )
        for row in hctx_cur.fetchall():
            hctx = row[0]
            if not hctx:
                continue
            try:
                # PG JSONB 는 psycopg 가 dict 로 자동 파싱해 돌려준다.
                if isinstance(hctx, (str, bytes)):
                    hctx = json.loads(hctx)
                al = float(hctx.get('alignment_score', 0.5))
                alignment_sum += al
                alignment_count += 1
                pk = hctx.get('position_key', '')
                if pk:
                    position_keys[pk] = position_keys.get(pk, 0) + 1
            except Exception:
                continue
    except Exception as exc:
        logger.error("PG roles summary failed for %s: %s", market_id, exc)
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            str(exc),
            action=MCPErrorAction.RETRY,
            action_value="30",
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return _format_role_summary(
        market_id, role_map, role_agg, alignment_sum, alignment_count, position_keys
    )


def _load_signals_role_summary_legacy(market_id: str, signal_dbs: list = None) -> Dict[str, Any]:
    """역할별 시그널 요약 (병렬 DB 순회 — legacy. 2026-07-22부터 리소스 경로는
    _load_signals_role_summary_pg 사용, 본 함수는 진단/대조용으로 잔존)"""
    import json as _json

    if signal_dbs is None:
        signal_dbs = list_signal_db_files(market_id)
    if not signal_dbs:
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"No signal DBs found for market: {market_id}",
            action=MCPErrorAction.CHECK,
            fallback_note="available_markets: crypto, kr_stock, us_stock",
        )

    role_map = _build_role_map(market_id)
    role_agg = {r: {'buy': 0, 'sell': 0, 'hold': 0, 'score_sum': 0.0, 'count': 0}
                for r in _ROLE_ORDER}
    alignment_sum = 0.0
    alignment_count = 0
    position_keys = {}

    def _query_role_single(db_file):
        try:
            with _connect_readonly(db_file) as conn:
                sig_rows = conn.execute("""
                    SELECT interval, signal_score, action
                    FROM signals WHERE interval != 'combined'
                    ORDER BY timestamp DESC LIMIT 20
                """).fetchall()
                hrow = conn.execute("""
                    SELECT hierarchy_context FROM signals
                    WHERE interval = 'combined'
                    ORDER BY timestamp DESC LIMIT 1
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
                    # PG JSONB 는 psycopg 가 dict 로 자동 파싱해 돌려준다 (SQLite 는 str).
                    hctx = db_result["hctx"]
                    if isinstance(hctx, (str, bytes)):
                        hctx = _json.loads(hctx)
                    al = float(hctx.get('alignment_score', 0.5))
                    alignment_sum += al
                    alignment_count += 1
                    pk = hctx.get('position_key', '')
                    if pk:
                        position_keys[pk] = position_keys.get(pk, 0) + 1
                except Exception:
                    pass

    return _format_role_summary(
        market_id, role_map, role_agg, alignment_sum, alignment_count, position_keys
    )


# ---------------------------------------------------------------------------
# AI Summary
# ---------------------------------------------------------------------------

# [2026-06-13] 윈도우 라벨 정합 — PG 경로는 total_signals_1h(window_sec=3600)로
# 저장하는데 요약은 total_signals_24h 만 읽어 항상 0 + "24시간 없음" 오표기였다
# (멘쿤 휴장 '전무' 발화의 데이터 측 원인). 두 키 모두 폴백 + window_sec 로 라벨 산출.
def _signal_total_and_label(stats: dict) -> tuple[int, str]:
    total = stats.get("total_signals_1h", stats.get("total_signals_24h", 0)) or 0
    win = stats.get("window_sec", 3600)
    label = "최근 1시간" if win and win <= 3600 else "최근 24시간"
    return total, label


def _ai_summary_signals(data: dict) -> str:
    stats = data.get("stats", {})
    total, win_label = _signal_total_and_label(stats)
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
    return f"{market_id} 시그널: {win_label} {total}건, {symbols}종목. 매수/매도/홀드={buy}/{sell}/{hold}. avg점수={avg_score:.1f}.{top_part}"


def _user_summary_signals(data: dict) -> str:
    """인간 사용자 1줄 — jargon-free 한국어."""
    market_id = data.get("market_id", "")
    market_label = {"crypto": "암호화폐", "kr_stock": "한국 주식", "us_stock": "미국 주식"}.get(market_id, market_id)
    stats = data.get("stats", {})
    total, win_label = _signal_total_and_label(stats)
    symbols = stats.get("unique_symbols", 0)
    dist = data.get("action_distribution", {})
    buy = dist.get("BUY", {}).get("count", 0)
    sell = dist.get("SELL", {}).get("count", 0)
    if total == 0:
        return f"{market_label} 시장의 {win_label} 신규 시그널이 없습니다."
    return f"{market_label} 시장 {win_label}: {symbols}개 종목에서 매수 신호 {buy}건, 매도 신호 {sell}건이 감지됐습니다."


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
                cached = wrap_with_ai_summary(cached, "signal_system", _ai_summary_signals, _user_summary_signals)
            return to_resource_text(cached)
        data = await _safe_load(_load_signals_summary, market_id)
        if not data.get("_timeout"):
            cache.set(cache_key, data)
        if not data.get("error"):
            data = wrap_with_ai_summary(data, "signal_system", _ai_summary_signals, _user_summary_signals)
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
