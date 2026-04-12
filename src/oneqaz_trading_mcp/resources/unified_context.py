# -*- coding: utf-8 -*-
"""
통합 컨텍스트 Resource (Unified Context)
========================================
내부(market 기술적 분석) + 외부(external_context 매크로/뉴스)를 합치는 핵심 레이어.

3가지 레벨:
  Level 1 - 심볼 단위:  BTC 기술적 지표 + BTC 관련 뉴스/이벤트
  Level 2 - 시장 단위:  FOMC 같은 글로벌 이벤트가 crypto 전체에 미치는 영향
  Level 3 - 교차 시장:  금↑+BTC↓ = risk-off, 국채금리↑+주식↓ = 금리민감

데이터 소스:
  - market/{market_id}/data_storage/ → 기술적 분석 (내부)
  - external_context/data_storage/   → 뉴스/이벤트/매크로 (외부)
  - market/global_regime/            → 글로벌 레짐 (교차 시장 기초)
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import logging
import threading
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

from mcps.config import (
    PROJECT_ROOT,
    CACHE_TTL_MARKET_STATUS,
    get_market_db_path,
    get_external_db_path,
    get_analysis_db_path,
    get_signals_dir,
    get_structure_summary_path,
    GLOBAL_REGIME_SUMMARY_JSON,
)
from mcps.resources.resource_response import (
    build_resource_explanation,
    to_resource_text,
    with_explanation_contract,
    mcp_error,
    MCPErrorCode,
    MCPErrorAction,
    wrap_with_ai_summary,
)

logger = logging.getLogger("MarketMCP")

CACHE_TTL_UNIFIED = 120

# 시장 ID → global_regime 카테고리 매핑 (교차 분석용)
MARKET_TO_REGIME_CATEGORY = {
    "crypto": None,
    "kr_stock": None,
    "us_stock": None,
    "commodity": "commodities",
    "commodities": "commodities",
    "forex": "forex",
    "bond": "bonds",
    "bonds": "bonds",
    "vix": "vix",
    "credit": "credit",
    "liquidity": "liquidity",
    "inflation": "inflation",
}

ALL_MARKETS = ["crypto", "kr_stock", "us_stock"]
ALL_CATEGORIES = ["commodities", "forex", "bonds", "vix", "credit", "liquidity", "inflation"]
ALL_STRUCTURE_MARKETS = ["us_stock", "kr_stock", "crypto"]

# ─── 교차 시장 상관관계 패턴 정의 ─────────────────────────────────────────
CROSS_MARKET_PATTERNS = [
    {
        "id": "gold_btc_inverse",
        "name": "금↑ BTC↓ (위험회피)",
        "pair": ("GC=F", "BTC"),
        "source_market": ("commodities", "crypto"),
        "expected": "inverse",
        "interpretation": "risk_off",
        "description": "금이 상승하고 BTC가 하락하면 위험회피(risk-off) 심리가 강함",
    },
    {
        "id": "gold_btc_aligned",
        "name": "금↑ BTC↑ (인플레이션 헤지)",
        "pair": ("GC=F", "BTC"),
        "source_market": ("commodities", "crypto"),
        "expected": "aligned",
        "interpretation": "inflation_hedge",
        "description": "금과 BTC가 동시 상승하면 인플레이션 헤지 수요",
    },
    {
        "id": "dxy_em_inverse",
        "name": "달러↑ 신흥시장↓",
        "pair": ("DX-Y.NYB", None),
        "source_market": ("forex", "kr_stock"),
        "expected": "inverse",
        "interpretation": "dollar_strength",
        "description": "달러 강세는 신흥시장에 부정적",
    },
    {
        "id": "bond_yield_stock_inverse",
        "name": "국채금리↑ 주식↓ (금리민감)",
        "pair": ("^TNX", None),
        "source_market": ("bonds", "us_stock"),
        "expected": "inverse",
        "interpretation": "rate_sensitivity",
        "description": "국채금리 상승은 주식시장에 하방 압력",
    },
    {
        "id": "oil_inflation",
        "name": "유가↑ (인플레이션 압력)",
        "pair": ("CL=F", None),
        "source_market": ("commodities", None),
        "expected": "signal",
        "interpretation": "inflation_pressure",
        "description": "유가 상승은 인플레이션 압력으로 전체 금융시장에 영향",
    },
    {
        "id": "vix_risk",
        "name": "VIX↑ (공포 확산)",
        "pair": ("^VIX", None),
        "source_market": ("vix", None),
        "expected": "signal",
        "interpretation": "fear_spike",
        "description": "VIX 급등은 전 시장 위험회피 신호",
    },
    {
        "id": "vxn_tech_fear",
        "name": "VXN↑ (기술주 공포)",
        "pair": ("^VXN", None),
        "source_market": ("vix", None),
        "expected": "signal",
        "interpretation": "tech_fear",
        "description": "나스닥 변동성 급등은 기술주·코인 동반 하락 신호",
    },
]

# 글로벌 이벤트 키워드 → 영향받는 시장
GLOBAL_EVENT_IMPACT = {
    "FOMC": ["crypto", "kr_stock", "us_stock", "bonds", "forex", "liquidity", "credit"],
    "금리": ["crypto", "kr_stock", "us_stock", "bonds", "forex", "liquidity", "credit"],
    "CPI": ["crypto", "kr_stock", "us_stock", "bonds", "commodities", "inflation"],
    "PCE": ["crypto", "us_stock", "bonds", "inflation"],
    "고용": ["us_stock", "bonds", "forex"],
    "인플레이션": ["crypto", "kr_stock", "us_stock", "bonds", "commodities", "inflation"],
    "GDP": ["kr_stock", "us_stock", "bonds"],
    "OPEC": ["commodities", "kr_stock", "us_stock", "inflation"],
    "연준": ["crypto", "kr_stock", "us_stock", "bonds", "forex", "liquidity"],
    "BOJ": ["forex", "kr_stock", "bonds", "liquidity"],
    "ECB": ["forex", "us_stock", "bonds", "liquidity"],
    "관세": ["kr_stock", "us_stock", "commodities", "inflation"],
    "전쟁": ["crypto", "kr_stock", "us_stock", "commodities", "forex", "bonds", "vix", "credit"],
    "제재": ["crypto", "commodities", "forex", "credit"],
    "변동성": ["crypto", "kr_stock", "us_stock", "vix", "credit"],
    "공포": ["crypto", "kr_stock", "us_stock", "vix", "credit"],
    "유동성": ["crypto", "kr_stock", "us_stock", "bonds", "liquidity"],
    "신용": ["bonds", "credit", "vix"],
}


# ─── DB 유틸 ──────────────────────────────────────────────────────────────

_conn_cache: Dict[str, sqlite3.Connection] = {}
_conn_cache_lock = threading.Lock()


def _safe_connect(db_path: Path) -> Optional[sqlite3.Connection]:
    """DB 커넥션 캐시 — 동일 경로에 대해 재사용 (메모리/성능 개선)."""
    if not db_path or not db_path.exists():
        return None
    key = str(db_path)
    with _conn_cache_lock:
        conn = _conn_cache.get(key)
        if conn is not None:
            try:
                conn.execute("SELECT 1")
                return conn
            except Exception:
                _conn_cache.pop(key, None)
        try:
            conn = sqlite3.connect(key, timeout=5, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            _conn_cache[key] = conn
            return conn
        except Exception:
            return None


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def _fetch_rows(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> List[Dict]:
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    except Exception:
        return []


# ─── Level 1: 심볼 단위 내부+외부 병합 ───────────────────────────────────

def _load_internal_symbol_snapshot(market_id: str, symbol: str) -> Dict[str, Any]:
    """내부 기술적 데이터: 시그널 DB에서 최신 분석"""
    from mcps.config import get_signal_db_path
    db_path = get_signal_db_path(market_id, symbol)
    if not db_path or not db_path.exists():
        return {}
    conn = _safe_connect(db_path)
    if not conn:
        return {}
    try:
        rows = _fetch_rows(conn, """
            SELECT * FROM signals ORDER BY timestamp DESC LIMIT 1
        """)
        return rows[0] if rows else {}
    finally:
        pass  # conn은 캐시에서 재사용 (close 불필요)


def _load_external_symbol_snapshot(market_id: str, symbol: str) -> Dict[str, Any]:
    """외부 컨텍스트 데이터: 해당 심볼의 뉴스/이벤트/펀더멘탈"""
    db_path = get_external_db_path(market_id)
    conn = _safe_connect(db_path)
    if not conn:
        return {}
    try:
        result = {}
        sym = symbol.upper()
        if _table_exists(conn, "news_events"):
            result["news"] = _fetch_rows(conn, """
                SELECT title, sentiment_score, sentiment_label, sentiment_confidence,
                       sentiment_model, risk_flags, impact_level, published_at
                FROM news_events
                WHERE UPPER(symbol) = ? OR symbol = 'GLOBAL'
                ORDER BY COALESCE(published_at, created_at) DESC LIMIT 5
            """, (sym,))
        if _table_exists(conn, "scheduled_events"):
            result["events"] = _fetch_rows(conn, """
                SELECT title, expected_impact, scheduled_at, status
                FROM scheduled_events
                WHERE (UPPER(symbol) = ? OR symbol = 'GLOBAL') AND status = 'upcoming'
                ORDER BY scheduled_at ASC LIMIT 3
            """, (sym,))
        if _table_exists(conn, "fundamentals"):
            rows = _fetch_rows(conn, """
                SELECT market_cap, per, pbr, roe, sector, industry
                FROM fundamentals WHERE UPPER(symbol) = ? LIMIT 1
            """, (sym,))
            result["fundamentals"] = rows[0] if rows else {}
        return result
    finally:
        pass  # conn은 캐시에서 재사용 (close 불필요)


def _merge_symbol_context(market_id: str, symbol: str) -> Dict[str, Any]:
    """심볼 단위 내부+외부 병합"""
    internal = _load_internal_symbol_snapshot(market_id, symbol)
    external = _load_external_symbol_snapshot(market_id, symbol)

    volatility_spike = False
    if internal:
        vol = str(internal.get("volatility_level") or internal.get("risk_level") or "").lower()
        volatility_spike = vol in ("high", "very_high", "critical")

    external_trigger = False
    top_news = external.get("news", [])
    for n in top_news:
        score = n.get("sentiment_score")
        if score is not None and abs(float(score)) >= 0.5:
            external_trigger = True
            break

    correlation_type = "none"
    if volatility_spike and external_trigger:
        correlation_type = "both_active"
    elif volatility_spike:
        correlation_type = "internal_trigger"
    elif external_trigger:
        correlation_type = "external_trigger"

    return {
        "symbol": symbol,
        "internal": internal,
        "external": external,
        "volatility_spike": volatility_spike,
        "external_trigger": external_trigger,
        "correlation_type": correlation_type,
    }


# ─── Level 2: 시장 단위 (글로벌 이벤트 영향) ─────────────────────────────

def _load_global_events_affecting(market_id: str) -> List[Dict[str, Any]]:
    """모든 외부 컨텍스트 DB에서 market_id에 영향을 주는 글로벌 이벤트 수집"""
    affecting = []
    news_db = get_external_db_path("news")
    conn = _safe_connect(news_db)
    if conn:
        try:
            if _table_exists(conn, "news_events"):
                rows = _fetch_rows(conn, """
                    SELECT title, summary, sentiment_score, impact_level, published_at
                    FROM news_events
                    ORDER BY COALESCE(published_at, created_at) DESC LIMIT 20
                """)
                for row in rows:
                    title = (row.get("title") or "").lower()
                    for keyword, markets in GLOBAL_EVENT_IMPACT.items():
                        if keyword.lower() in title and market_id in markets:
                            affecting.append({
                                **row,
                                "trigger_keyword": keyword,
                                "affected_markets": markets,
                            })
                            break
        finally:
            pass  # conn은 캐시에서 재사용 (close 불필요)

    for cat in ALL_CATEGORIES:
        cat_db = get_external_db_path(cat)
        cat_conn = _safe_connect(cat_db)
        if not cat_conn:
            continue
        try:
            if _table_exists(cat_conn, "scheduled_events"):
                rows = _fetch_rows(cat_conn, """
                    SELECT title, expected_impact, scheduled_at, status
                    FROM scheduled_events WHERE status = 'upcoming'
                    ORDER BY scheduled_at ASC LIMIT 5
                """)
                for row in rows:
                    title = (row.get("title") or "").lower()
                    for keyword, markets in GLOBAL_EVENT_IMPACT.items():
                        if keyword.lower() in title and market_id in markets:
                            affecting.append({
                                **row,
                                "type": "scheduled",
                                "category": cat,
                                "trigger_keyword": keyword,
                                "affected_markets": markets,
                            })
                            break
        finally:
            pass  # conn 캐시 재사용

    return affecting


def _load_market_internal_summary(market_id: str) -> Dict[str, Any]:
    """시장 전체 내부 요약 (trading_system.db)"""
    db_path = get_market_db_path(market_id)
    conn = _safe_connect(db_path)
    if not conn:
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"No trading DB for {market_id}",
            action=MCPErrorAction.CHECK,
            action_value=f"market://{market_id}/status",
            market_id=market_id,
        )
    try:
        result = {}
        try:
            rows = _fetch_rows(conn, "SELECT key, value FROM system_status")
            status = {r["key"]: r["value"] for r in rows}
            result["regime"] = status.get("market_regime", "Unknown")
            result["scanning"] = status.get("scanning_coins", "")
        except Exception:
            result["regime"] = "Unknown"

        try:
            rows = _fetch_rows(conn, """
                SELECT COUNT(*) as cnt,
                       AVG(profit_loss_pct) as avg_pnl,
                       SUM(CASE WHEN profit_loss_pct > 0 THEN 1 ELSE 0 END) as wins
                FROM virtual_positions
            """)
            if rows:
                result["positions_count"] = rows[0].get("cnt", 0)
                result["avg_pnl"] = round(rows[0].get("avg_pnl") or 0, 2)
                result["winning_count"] = rows[0].get("wins", 0)
        except Exception:
            pass
        return result
    finally:
        pass  # conn은 캐시에서 재사용 (close 불필요)


def _load_market_external_summary(market_id: str) -> Dict[str, Any]:
    """시장 전체 외부 요약"""
    db_path = get_external_db_path(market_id)
    conn = _safe_connect(db_path)
    if not conn:
        return {}
    try:
        result = {}
        if _table_exists(conn, "news_events"):
            rows = _fetch_rows(conn, """
                SELECT title, sentiment_score, sentiment_label, sentiment_confidence,
                       sentiment_model, risk_flags, impact_level, published_at, symbol
                FROM news_events
                ORDER BY COALESCE(published_at, created_at) DESC LIMIT 10
            """)
            result["recent_news"] = rows
            high_impact = [r for r in rows if r.get("impact_level") == "high"]
            result["high_impact_news_count"] = len(high_impact)
            risk_news = [r for r in rows if r.get("risk_flags") and r["risk_flags"] != "[]"]
            result["risk_flagged_news_count"] = len(risk_news)

        if _table_exists(conn, "scheduled_events"):
            rows = _fetch_rows(conn, """
                SELECT title, expected_impact, scheduled_at, symbol
                FROM scheduled_events WHERE status = 'upcoming'
                ORDER BY scheduled_at ASC LIMIT 5
            """)
            result["upcoming_events"] = rows

        if _table_exists(conn, "market_metrics"):
            rows = _fetch_rows(conn, """
                SELECT symbol, metric_type, value
                FROM market_metrics
                ORDER BY timestamp DESC LIMIT 20
            """)
            result["metrics"] = rows

        return result
    finally:
        pass  # conn은 캐시에서 재사용 (close 불필요)


def _build_unified_market_context(market_id: str) -> Dict[str, Any]:
    """시장 단위 통합 컨텍스트 (Level 1+2)"""
    internal = _load_market_internal_summary(market_id)
    external = _load_market_external_summary(market_id)
    global_events = _load_global_events_affecting(market_id)

    alert_level = "normal"
    if len(global_events) >= 3:
        alert_level = "high"
    elif len(global_events) >= 1:
        alert_level = "elevated"

    high_impact = external.get("high_impact_news_count", 0)
    if high_impact >= 2:
        alert_level = "high"

    return {
        "market_id": market_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "internal": internal,
        "external": external,
        "global_events_affecting": global_events,
        "alert_level": alert_level,
        "_llm_summary": _build_unified_llm_summary(market_id, internal, external, global_events),
    }


def _build_unified_llm_summary(
    market_id: str,
    internal: Dict,
    external: Dict,
    global_events: List[Dict],
) -> str:
    lines = [f"[{market_id.upper()} 통합 컨텍스트]"]

    regime = internal.get("regime", "Unknown")
    pos_count = internal.get("positions_count", 0)
    avg_pnl = internal.get("avg_pnl", 0)
    lines.append(f"내부: 레짐={regime}, 포지션={pos_count}개, 평균수익={avg_pnl:+.2f}%")

    news_count = len(external.get("recent_news", []))
    hi_count = external.get("high_impact_news_count", 0)
    event_count = len(external.get("upcoming_events", []))
    lines.append(f"외부: 뉴스={news_count}건(고영향={hi_count}), 예정이벤트={event_count}건")

    if global_events:
        lines.append(f"글로벌 이벤트 영향: {len(global_events)}건")
        for ev in global_events[:3]:
            title = ev.get("title", "")[:60]
            kw = ev.get("trigger_keyword", "")
            lines.append(f"  - [{kw}] {title}")

    risk_count = external.get("risk_flagged_news_count", 0)
    if risk_count:
        lines.append(f"⚠ 리스크 감지 뉴스: {risk_count}건")

    top_news = external.get("recent_news", [])[:3]
    if top_news:
        lines.append("최근 주요 뉴스:")
        for n in top_news:
            title = (n.get("title") or "")[:60]
            score = n.get("sentiment_score")
            label = n.get("sentiment_label", "")
            model = n.get("sentiment_model", "")
            conf = n.get("sentiment_confidence")
            model_tag = f"[{model}]" if model and model != "keyword_fallback" else ""
            score_str = ""
            if isinstance(score, (int, float)):
                conf_str = f", 신뢰도={conf:.0%}" if isinstance(conf, (int, float)) and conf > 0 else ""
                score_str = f" ({label}={score:+.2f}{conf_str}) {model_tag}"
            risk_raw = n.get("risk_flags", "")
            risk_str = ""
            if risk_raw and risk_raw not in ("[]", "null", ""):
                risk_str = f" ⚠{risk_raw}"
            lines.append(f"  - {title}{score_str}{risk_str}")

    upcoming = external.get("upcoming_events", [])[:2]
    if upcoming:
        lines.append("예정 이벤트:")
        for e in upcoming:
            title = (e.get("title") or "")[:50]
            sched = (e.get("scheduled_at") or "")[:10]
            lines.append(f"  - {title} ({sched})")

    return "\n".join(lines)


# ─── Level 3: 교차 시장 상관관계 ─────────────────────────────────────────

def _load_regime_directions() -> Dict[str, Dict[str, Any]]:
    """글로벌 레짐에서 카테고리/심볼별 방향성 로드"""
    directions: Dict[str, Dict[str, Any]] = {}

    for category in ALL_CATEGORIES:
        db_path = get_analysis_db_path(category)
        conn = _safe_connect(db_path)
        if not conn:
            continue
        try:
            rows = _fetch_rows(conn, """
                SELECT symbol, regime_label, sentiment_label, integrated_direction,
                       regime_stage, volatility_level
                FROM analysis
                ORDER BY timestamp DESC LIMIT 50
            """)
            seen = set()
            for row in rows:
                sym = row.get("symbol", "")
                if sym in seen:
                    continue
                seen.add(sym)
                direction = _classify_direction(row)
                directions[sym] = {
                    "category": category,
                    "direction": direction,
                    "regime": row.get("regime_label") or row.get("regime_stage", ""),
                    "sentiment": row.get("sentiment_label", ""),
                    "volatility": row.get("volatility_level", ""),
                    "raw_direction": row.get("integrated_direction", ""),
                }
        finally:
            pass  # conn은 캐시에서 재사용 (close 불필요)

    for market_id in ALL_MARKETS:
        db_path = get_market_db_path(market_id)
        conn = _safe_connect(db_path)
        if not conn:
            continue
        try:
            status_rows = _fetch_rows(conn, "SELECT key, value FROM system_status")
            status = {r["key"]: r["value"] for r in status_rows}
            regime = (status.get("market_regime") or "").lower()
            d = "neutral"
            if any(k in regime for k in ("bull", "up", "risk_on", "risk-on")):
                d = "up"
            elif any(k in regime for k in ("bear", "down", "risk_off", "risk-off")):
                d = "down"
            directions[f"_market_{market_id}"] = {
                "category": market_id,
                "direction": d,
                "regime": status.get("market_regime", "Unknown"),
            }
        finally:
            pass  # conn은 캐시에서 재사용 (close 불필요)

    return directions


def _classify_direction(row: Dict) -> str:
    d = (row.get("integrated_direction") or "").lower()
    if any(k in d for k in ("bull", "up", "long", "상승")):
        return "up"
    if any(k in d for k in ("bear", "down", "short", "하락")):
        return "down"
    regime = (row.get("regime_label") or row.get("regime_stage") or "").lower()
    if any(k in regime for k in ("bull", "risk_on", "risk-on")):
        return "up"
    if any(k in regime for k in ("bear", "risk_off", "risk-off")):
        return "down"
    return "neutral"


def _detect_cross_market_correlations(directions: Dict[str, Dict]) -> List[Dict[str, Any]]:
    """교차 시장 상관관계 패턴 감지"""
    detected = []

    for pattern in CROSS_MARKET_PATTERNS:
        sym_a, sym_b = pattern["pair"]
        src_a, src_b = pattern["source_market"]

        dir_a = None
        if sym_a and sym_a in directions:
            dir_a = directions[sym_a]
        elif sym_a:
            for key, val in directions.items():
                if sym_a.upper() in key.upper() and val.get("category") == src_a:
                    dir_a = val
                    break

        dir_b = None
        if sym_b and sym_b in directions:
            dir_b = directions[sym_b]
        elif sym_b:
            for key, val in directions.items():
                if sym_b.upper() in key.upper():
                    dir_b = val
                    break
        if dir_b is None and src_b:
            market_key = f"_market_{src_b}"
            if market_key in directions:
                dir_b = directions[market_key]

        if pattern["expected"] == "signal":
            if dir_a and dir_a["direction"] != "neutral":
                strength = dir_a.get("volatility", "")
                score = 0.7 if strength in ("high", "very_high") else 0.4
                detected.append({
                    "pattern_id": pattern["id"],
                    "pattern_name": pattern["name"],
                    "interpretation": pattern["interpretation"],
                    "description": pattern["description"],
                    "direction_a": dir_a["direction"],
                    "direction_b": None,
                    "score": score,
                    "active": True,
                })
            continue

        if not dir_a or not dir_b:
            continue

        a_dir = dir_a["direction"]
        b_dir = dir_b["direction"]

        if a_dir == "neutral" and b_dir == "neutral":
            continue

        is_inverse = (a_dir == "up" and b_dir == "down") or (a_dir == "down" and b_dir == "up")
        is_aligned = (a_dir == b_dir) and a_dir != "neutral"

        active = False
        if pattern["expected"] == "inverse" and is_inverse:
            active = True
        elif pattern["expected"] == "aligned" and is_aligned:
            active = True

        score = 0.8 if active else 0.3

        detected.append({
            "pattern_id": pattern["id"],
            "pattern_name": pattern["name"],
            "interpretation": pattern["interpretation"],
            "description": pattern["description"],
            "direction_a": a_dir,
            "direction_b": b_dir,
            "score": score,
            "active": active,
        })

    return detected


def _build_cross_market_context() -> Dict[str, Any]:
    """교차 시장 전체 분석 (Level 3)"""
    directions = _load_regime_directions()
    correlations = _detect_cross_market_correlations(directions)

    active = [c for c in correlations if c.get("active")]
    dominant_interpretation = None
    if active:
        interp_counts: Dict[str, float] = {}
        for c in active:
            interp = c["interpretation"]
            interp_counts[interp] = interp_counts.get(interp, 0) + c["score"]
        dominant_interpretation = max(interp_counts, key=interp_counts.get)

    regime_summary = _load_global_regime_brief()

    structure_brief = _load_all_structure_briefs()

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "directions": {
            k: {"direction": v["direction"], "regime": v.get("regime", ""), "category": v.get("category", "")}
            for k, v in directions.items()
        },
        "correlations": correlations,
        "active_patterns": active,
        "active_count": len(active),
        "dominant_interpretation": dominant_interpretation,
        "global_regime": regime_summary,
        "market_structure": structure_brief,
        "_llm_summary": _build_cross_market_llm_summary(directions, active, dominant_interpretation, regime_summary),
    }
    return result


def _load_all_structure_briefs() -> Dict[str, Any]:
    """모든 시장의 구조 요약 brief 로드"""
    result: Dict[str, Any] = {}
    for market_id in ALL_STRUCTURE_MARKETS:
        path = get_structure_summary_path(market_id)
        if not path or not path.exists():
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            overall = data.get("overall", {})
            groups_brief = {}
            for gid, ginfo in data.get("groups", {}).items():
                if isinstance(ginfo, dict):
                    groups_brief[gid] = {
                        "regime_dominant": ginfo.get("regime_dominant"),
                        "regime_avg": ginfo.get("regime_avg"),
                        "timing_signal": ginfo.get("timing_signal"),
                        "confidence": ginfo.get("confidence"),
                    }
            result[market_id] = {
                "overall_regime": overall.get("regime", "unknown"),
                "overall_score": overall.get("score", 0),
                "groups": groups_brief,
                "updated_at": data.get("updated_at"),
            }
        except Exception:
            continue
    return result


def _load_global_regime_brief() -> Dict[str, Any]:
    try:
        if GLOBAL_REGIME_SUMMARY_JSON.exists():
            with open(GLOBAL_REGIME_SUMMARY_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {
                "overall_regime": data.get("overall", {}).get("regime", "Unknown"),
                "overall_score": data.get("overall", {}).get("score", 0),
                "updated_at": data.get("updated_at"),
            }
    except Exception:
        pass
    return {"overall_regime": "Unknown", "overall_score": 0}


def _build_cross_market_llm_summary(
    directions: Dict,
    active_patterns: List[Dict],
    dominant: Optional[str],
    regime: Dict,
) -> str:
    lines = ["[교차 시장 상관관계 분석]"]
    lines.append(f"글로벌 레짐: {regime.get('overall_regime', '?')} (점수: {regime.get('overall_score', 0):.2f})")

    market_dirs = {k: v for k, v in directions.items() if k.startswith("_market_")}
    if market_dirs:
        lines.append("시장별 방향:")
        for k, v in market_dirs.items():
            name = k.replace("_market_", "")
            lines.append(f"  - {name}: {v['direction']} ({v.get('regime', '')})")

    if active_patterns:
        lines.append(f"\n활성 패턴 ({len(active_patterns)}개):")
        for p in active_patterns:
            lines.append(f"  ⚡ {p['pattern_name']}: {p['description']}")
        if dominant:
            INTERP_KO = {
                "risk_off": "위험회피 심리 우세",
                "risk_on": "위험선호 심리 우세",
                "inflation_hedge": "인플레이션 헤지 수요",
                "dollar_strength": "달러 강세 영향",
                "rate_sensitivity": "금리 민감 구간",
                "inflation_pressure": "인플레이션 압력",
                "fear_spike": "공포 급등",
            }
            lines.append(f"\n지배적 해석: {INTERP_KO.get(dominant, dominant)}")
    else:
        lines.append("\n활성 교차 패턴 없음 - 시장간 뚜렷한 상관 신호 미감지")

    return "\n".join(lines)


# ─── 통합 엔드포인트: 전체 합치기 ─────────────────────────────────────────

def _load_feature_gate_summary(market_id: str) -> Dict[str, Any]:
    """agent_history에서 Feature Gate 현황 로드"""
    try:
        from agent_history.core.db_manager import AgentHistoryDB
        db = AgentHistoryDB(market_id)
        try:
            summary = db.get_feature_gate_summary()
            active = db.get_active_features()
            return {
                "summary": summary,
                "active_features": active[:10],
            }
        finally:
            db.close()
    except Exception:
        return {}


def _load_agent_history_rag(market_id: str, regime: str = "") -> str:
    """agent_history RAG 컨텍스트 로드"""
    try:
        from agent_history.core.rag_provider import get_history_context
        return get_history_context(market_id, current_regime=regime or None)
    except Exception:
        return ""


def _load_inference_watchlist(market_id: str) -> List[Dict[str, Any]]:
    """이벤트 전파 추론 Watchlist 로드"""
    market_map = {"crypto": "coin_market", "kr_stock": "kr_market", "us_stock": "us_market"}
    search_markets = ["news"]
    mapped = market_map.get(market_id)
    if mapped:
        search_markets.append(mapped)

    items = []
    for mid in search_markets:
        db_path = get_external_db_path(mid)
        conn = _safe_connect(db_path)
        if not conn:
            continue
        try:
            if not _table_exists(conn, "inference_candidates"):
                continue
            rows = _fetch_rows(conn, """
                SELECT candidate_symbol, candidate_market, source_title,
                       relation_type, relation_detail, adjusted_confidence,
                       coverage_penalty, created_at
                FROM inference_candidates
                WHERE status = 'watching'
                ORDER BY adjusted_confidence DESC, created_at DESC
                LIMIT 5
            """)
            items.extend(rows)
        finally:
            pass  # conn은 캐시에서 재사용 (close 불필요)
    return items[:5]


def _load_regime_flow_summary(market_id: str) -> str:
    """레짐 흐름 분석 결과를 LLM 요약 텍스트로 로드"""
    market_map = {"crypto": "coin_market", "kr_stock": "kr_market", "us_stock": "us_market",
                  "commodity": "commodities", "forex": "forex", "bond": "bonds", "vix": "vix"}
    mapped = market_map.get(market_id, market_id)
    db_path = get_external_db_path(mapped)
    conn = _safe_connect(db_path)
    if not conn:
        return ""

    parts = []
    try:
        # 시장 전체 레짐 흐름
        if _table_exists(conn, "market_regime_flow"):
            mf = _fetch_rows(conn, """
                SELECT dominant_regime, avg_regime_stage, avg_volume_ratio,
                       bullish_pct, bearish_pct, neutral_pct,
                       volume_trend, regime_trend, snapshot_at
                FROM market_regime_flow
                WHERE market_id = ?
                ORDER BY snapshot_at DESC LIMIT 1
            """, (mapped,))
            if mf:
                m = mf[0]
                parts.append(
                    f"[시장 레짐 흐름] 레짐={m.get('dominant_regime','?')} "
                    f"(stage={m.get('avg_regime_stage','?'):.1f}), "
                    f"volume_ratio={m.get('avg_volume_ratio',1):.2f}, "
                    f"bullish={m.get('bullish_pct',0):.0%}/bearish={m.get('bearish_pct',0):.0%}/neutral={m.get('neutral_pct',0):.0%}, "
                    f"거래량추세={m.get('volume_trend','?')}, 레짐추세={m.get('regime_trend','?')}"
                )

        # 섹터별 레짐 흐름 (상위 5개)
        if _table_exists(conn, "sector_regime_flow"):
            sf = _fetch_rows(conn, """
                SELECT sector, dominant_regime, avg_volume_ratio,
                       volume_trend, regime_trend, symbol_count
                FROM sector_regime_flow
                WHERE market_id = ?
                ORDER BY snapshot_at DESC, avg_volume_ratio DESC
                LIMIT 8
            """, (mapped,))
            if sf:
                sect_lines = ["[섹터별 레짐]"]
                for s in sf:
                    sect_lines.append(
                        f"  {s.get('sector','?')}({s.get('symbol_count',0)}종목): "
                        f"{s.get('dominant_regime','?')}, vol={s.get('avg_volume_ratio',1):.2f}"
                        f"({s.get('volume_trend','?')}), 추세={s.get('regime_trend','?')}"
                    )
                parts.append("\n".join(sect_lines))

        # 레짐 전이 예측 (상위 3개)
        if _table_exists(conn, "regime_transitions"):
            current_regime = ""
            if _table_exists(conn, "market_regime_flow"):
                cr = conn.execute(
                    "SELECT dominant_regime FROM market_regime_flow WHERE market_id = ? ORDER BY snapshot_at DESC LIMIT 1",
                    (mapped,),
                ).fetchone()
                if cr:
                    current_regime = cr[0]

            if current_regime:
                trans = conn.execute(
                    """
                    SELECT to_regime, volume_condition, count
                    FROM regime_transitions
                    WHERE market_id = ? AND scope = 'market' AND from_regime = ?
                    ORDER BY count DESC LIMIT 5
                    """,
                    (mapped, current_regime),
                ).fetchall()
                if trans:
                    total = sum(t[2] for t in trans)
                    pred_lines = [f"[레짐 전환 예측] 현재={current_regime}"]
                    for t in trans[:3]:
                        prob = t[2] / total if total > 0 else 0
                        pred_lines.append(
                            f"  → {t[0]} (확률={prob:.0%}, 거래량조건={t[1]}, 샘플={t[2]})"
                        )
                    parts.append("\n".join(pred_lines))

        # 크로스마켓 시그널
        news_db = _safe_connect(get_external_db_path("news"))
        if news_db:
            try:
                if _table_exists(news_db, "cross_market_correlation"):
                    cross = news_db.execute(
                        """
                        SELECT source_market, source_regime_change, lag_hours,
                               correlation_score, count
                        FROM cross_market_correlation
                        WHERE target_market = ? AND correlation_score >= 0.4 AND count >= 2
                        ORDER BY correlation_score DESC LIMIT 5
                        """,
                        (mapped,),
                    ).fetchall()
                    if cross:
                        cross_lines = ["[크로스마켓 레짐 상관]"]
                        for c in cross:
                            cross_lines.append(
                                f"  {c[0]}({c[1]}) → 본 시장 영향: "
                                f"평균 {c[2]:.0f}시간 후, 상관={c[3]:.2f}, 관측={c[4]}회"
                            )
                        parts.append("\n".join(cross_lines))
            finally:
                news_db.close()

    except Exception:
        pass
    finally:
        pass  # conn은 캐시에서 재사용 (close 불필요)

    return "\n".join(parts)


def _build_full_unified_context(market_id: str) -> Dict[str, Any]:
    """특정 시장의 전체 통합 컨텍스트 (Level 1+2+3 + Feature Gate + Agent History + Inference)"""
    market_ctx = _build_unified_market_context(market_id)
    cross_market = _build_cross_market_context()

    current_regime = market_ctx.get("internal", {}).get("regime", "")

    feature_gate = _load_feature_gate_summary(market_id)
    history_rag = _load_agent_history_rag(market_id, current_regime)
    inference_watchlist = _load_inference_watchlist(market_id)
    regime_flow = _load_regime_flow_summary(market_id)

    full_summary_parts = []
    if market_ctx.get("_llm_summary"):
        full_summary_parts.append(market_ctx["_llm_summary"])
    if cross_market.get("_llm_summary"):
        full_summary_parts.append(cross_market["_llm_summary"])
    if history_rag:
        full_summary_parts.append(history_rag)

    if regime_flow:
        full_summary_parts.append(regime_flow)

    if inference_watchlist:
        wl_lines = ["[이벤트 전파 추론 Watchlist]"]
        for item in inference_watchlist:
            sym = item.get("candidate_symbol", "?")
            src = (item.get("source_title") or "")[:50]
            rel = item.get("relation_type", "related")
            conf = item.get("adjusted_confidence", 0)
            penalty = item.get("coverage_penalty", 0)
            detail = (item.get("relation_detail") or "")[:40]
            conf_val = float(conf) if conf else 0.0
            penalty_val = float(penalty) if penalty else 0.0
            wl_lines.append(
                f"  🔍 {sym} ({rel}): '{src}' "
                f"[신뢰도={conf_val:.2f}, penalty={penalty_val:.2f}] {detail}"
            )
        full_summary_parts.append("\n".join(wl_lines))

    # LLM summary 크기 제한 (메모리 보호)
    llm_summary = "\n\n---\n\n".join(full_summary_parts)
    if len(llm_summary) > 8000:
        llm_summary = llm_summary[:8000] + "\n...(truncated for memory efficiency)"

    data = {
        "market_id": market_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level_1_2": market_ctx,
        "level_3": cross_market,
        "feature_gate": feature_gate,
        "inference_watchlist": inference_watchlist,
        "regime_flow": regime_flow,
        "alert_level": market_ctx.get("alert_level", "normal"),
        "active_cross_patterns": cross_market.get("active_count", 0),
        "dominant_interpretation": cross_market.get("dominant_interpretation"),
        "_llm_summary": llm_summary,
    }
    data["component_ttls"] = {
        "internal_status": {"source": f"market://{market_id}/status", "ttl_seconds": 60},
        "external_summary": {"source": f"market://{market_id}/external/summary", "ttl_seconds": 60},
        "global_regime": {"source": "market://global/summary", "ttl_seconds": 300},
        "cross_market": {"source": "market://unified/cross-market", "ttl_seconds": 120},
        "signals": {"source": f"market://{market_id}/signals/summary", "ttl_seconds": 300},
    }
    data["_usage_guidance"] = (
        "이 통합 컨텍스트는 여러 소스를 한번에 반환하지만 각 컴포넌트 갱신 주기가 다릅니다. "
        "정밀 분석이 필요하면 개별 Resource를 직접 호출하세요: "
        f"market://{market_id}/status (TTL=60초), "
        f"market://{market_id}/external/summary (TTL=60초), "
        "market://global/summary (TTL=300초)"
    )
    explanation = build_resource_explanation(
        market=market_id,
        entity_type="market",
        explanation_type="mcp_unified_context",
        as_of_time=data["timestamp"],
        headline=f"{market_id} unified context snapshot",
        why_text=data.get("_llm_summary") or f"{market_id} unified context available",
        bullet_points=[
            f"alert_level={data.get('alert_level', 'normal')}",
            f"active_cross_patterns={data.get('active_cross_patterns', 0)}",
            f"dominant_interpretation={data.get('dominant_interpretation') or 'none'}",
        ],
        market_context_refs={
            "alert_level": data.get("alert_level", "normal"),
            "active_cross_patterns": data.get("active_cross_patterns", 0),
            "dominant_interpretation": data.get("dominant_interpretation"),
        },
        note="derived from unified MCP resource",
    )
    return with_explanation_contract(
        data,
        resource_type="unified_context",
        explanation=explanation,
    )


# ─── Resource 등록 ────────────────────────────────────────────────────────

def _ai_summary_unified(data: dict) -> str:
    l12 = data.get("level_1_2", {})
    internal = l12.get("market_internal_summary", {})
    regime = internal.get("market_regime", "unknown")
    pos_count = internal.get("active_positions", 0)
    avg_pnl = internal.get("avg_pnl", 0)
    alert = data.get("alert_level", "normal")
    patterns = data.get("active_cross_patterns", 0)
    interp = data.get("dominant_interpretation", "")
    market_id = data.get("market_id", "")
    return f"{market_id} 통합: 레짐={regime}, 포지션 {pos_count}개(avg {avg_pnl:.1f}%), 알림={alert}, 교차패턴 {patterns}개. {interp}"


def register_unified_context_resources(mcp, cache):
    """통합 컨텍스트 Resource 등록"""

    @mcp.resource("market://{market_id}/unified")
    async def get_unified_context(market_id: str) -> Dict[str, Any]:
        """[역할] 시장 단위 통합 컨텍스트(내부 기술분석 + 외부 뉴스/이벤트 + 교차시장). [호출 시점] 하나의 호출로 시장 전체 맥락 파악 시. 개별 Resource 3-4개 대체. [선행 조건] 없음. 정밀 분석 필요 시 개별 resource 직접 호출이 캐시 효율적. [후속 추천] market://{market_id}/unified/symbol/{symbol}, get_signals. [주의] TTL=120초. 내부 여러 DB 조회로 응답 느릴 수 있음. component_ttls 참고.
        [출력 스키마] ai_summary 래핑. full_data: market_id(str), internal{regime,positions_count,avg_pnl}, external{recent_news[],high_impact_news_count,upcoming_events[]}, alert_level(str:normal|elevated|high), feature_gate{...}, _contract{...}, _llm_summary(str)."""
        cache_key = f"unified_context_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_UNIFIED)
        if cached:
            return to_resource_text(cached)
        data = await asyncio.to_thread(_build_full_unified_context, market_id)
        if not data.get("error"):
            data = wrap_with_ai_summary(data, "unified_context", _ai_summary_unified)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://unified/cross-market")
    async def get_cross_market_correlations() -> Dict[str, Any]:
        """[역할] 교차시장 상관관계(금-BTC, 국채-주식, 달러-신흥시장 등 패턴 감지). [호출 시점] 시장간 상관관계/디커플링 분석 시. [선행 조건] market://global/summary 권장. [후속 추천] market://derived/cross-decoupling, market://all/summary. [주의] TTL=120초. 사전 정의된 패턴 기반.
        [출력 스키마] _contract 포함. directions{symbol→{direction,regime,category}}, correlations[{pattern_name,interpretation,score,active}], active_count(int), dominant_interpretation(str|null), global_regime{overall_regime,overall_score}, market_structure{market_id→{overall_regime,groups{...}}}, _llm_summary(str)."""
        cache_key = "cross_market_correlations"
        cached = cache.get(cache_key, ttl=CACHE_TTL_UNIFIED)
        if cached:
            return to_resource_text(cached)
        raw = await asyncio.to_thread(_build_cross_market_context)
        explanation = build_resource_explanation(
            market="global",
            entity_type="market",
            explanation_type="mcp_cross_market_context",
            as_of_time=raw.get("timestamp"),
            headline="cross market correlation snapshot",
            why_text=raw.get("_llm_summary") or "cross market context available",
            bullet_points=[
                f"active_count={raw.get('active_count', 0)}",
                f"dominant_interpretation={raw.get('dominant_interpretation') or 'none'}",
            ],
            market_context_refs={
                "active_count": raw.get("active_count", 0),
                "dominant_interpretation": raw.get("dominant_interpretation"),
            },
            note="derived from cross-market MCP resource",
        )
        data = with_explanation_contract(
            raw,
            resource_type="cross_market_context",
            explanation=explanation,
        )
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/unified/symbol/{symbol}")
    async def get_unified_symbol_context(market_id: str, symbol: str) -> Dict[str, Any]:
        """[역할] 종목 단위 통합 컨텍스트(기술분석 + 외부 뉴스/이벤트). [호출 시점] 특정 종목의 내부+외부 맥락을 한번에 파악 시. [선행 조건] market://{market_id}/unified 후 drill-down 권장. [후속 추천] get_role_analysis, get_position_detail. [주의] 종목별 외부 데이터 없을 수 있음.
        [출력 스키마] _contract 포함. symbol(str), internal{signal_score,confidence,action,rsi,macd}, external{news[],events[],fundamentals{}}, volatility_spike(bool), external_trigger(bool), correlation_type(str:none|internal_trigger|external_trigger|both_active), _llm_summary(str)."""
        cache_key = f"unified_symbol_{market_id}_{symbol}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_UNIFIED)
        if cached:
            return to_resource_text(cached)
        raw = await asyncio.to_thread(_merge_symbol_context, market_id, symbol)
        explanation = build_resource_explanation(
            market=market_id,
            symbol=symbol,
            entity_type="symbol",
            explanation_type="mcp_unified_symbol_context",
            as_of_time=datetime.now(timezone.utc).isoformat(),
            headline=f"{symbol} unified symbol context",
            why_text=raw.get("_llm_summary") or f"{symbol} symbol context available",
            bullet_points=[
                f"correlation_type={raw.get('correlation_type', 'none')}",
                f"external_trigger={raw.get('external_trigger', False)}",
                f"volatility_spike={raw.get('volatility_spike', False)}",
            ],
            market_context_refs={
                "correlation_type": raw.get("correlation_type", "none"),
                "external_trigger": raw.get("external_trigger", False),
                "volatility_spike": raw.get("volatility_spike", False),
            },
            note="derived from unified symbol MCP resource",
        )
        data = with_explanation_contract(
            raw,
            resource_type="unified_symbol_context",
            explanation=explanation,
        )
        cache.set(cache_key, data)
        return to_resource_text(data)

    logger.info("  🔗 Unified Context resources registered (Level 1/2/3)")
