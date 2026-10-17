# -*- coding: utf-8 -*-
"""
엔티티 컨텍스트 원천 읽기 — PG 읽기 전용 + 구조 요약 JSON + TTL 캐시
=====================================================================
원천별 함수 1개. 판정(필터·gap 분류)은 하지 않고 **사실(행·개수·시각)** 만 돌려준다 —
연결 객체로 바꾸는 것과 gap 이유를 정하는 것은 `links` / `context` 의 몫이다.

실패 정책: DB·파일 오류는 예외로 올린다(조용한 0 금지 — CLAUDE.md 조용한 사망 패턴 ②).
호출부(`context`)가 섹션 단위로 잡아 error 로그를 남기고 그 섹션을 gap 으로 정직하게 표기한다.

원천 선택 근거 = ai_brain/main_pc_tasks/diag_mcp_entity_context_phase0_2026_10_15.md §1·§2
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger("MarketMCP")

MARKETS: Tuple[str, ...] = ("crypto", "kr_stock", "us_stock")
SIGNAL_SCHEMA = {"crypto": "market_coin", "kr_stock": "market_kr", "us_stock": "market_us"}
STRUCT_SCHEMA = {"crypto": "market_coin_struct", "kr_stock": "market_kr_struct", "us_stock": "market_us_struct"}
# agent_history 학습기(peer·cross_layer·sector)의 시장 키
AH_MARKET = {"crypto": "coin", "kr_stock": "kr_stock", "us_stock": "us_stock"}
# external_context.symbol_master · news_events.target_markets 의 시장 키
MASTER_SIGNAL = {"crypto": "coin_market", "kr_stock": "kr_market", "us_stock": "us_market"}
MASTER_STRUCT = {"crypto": "coin_structure", "kr_stock": "kr_structure", "us_stock": "us_structure"}
_INTERVAL_KEY = {"crypto": "coin", "kr_stock": "kr", "us_stock": "us"}

# 거시 카테고리 구성 종목 행(symbol_master.market_id = 카테고리)
MACRO_CATEGORIES: Tuple[str, ...] = (
    "bonds", "commodities", "credit", "forex", "inflation", "liquidity", "vix",
)

# symbol_master 신선 행 기준 — mcps/symbol_coverage._MASTER_FRESH_DAYS 와 같은 값
# (오등록 잔재: coin 의 KR 6자리 199행 03-24 · us 소문자 607행 04-16 을 이것으로 거른다)
MASTER_FRESH_DAYS = 3

# 원천 갱신 주기에 맞춘 캐시 수명(초)
TTL_STRUCTURE = 60.0
TTL_SIGNALS = 60.0
TTL_LINKS = 600.0       # peer · cross_layer · sector · skill — 학습기 일 1회 이하 갱신
TTL_NEWS = 120.0
TTL_CALIBRATION = 600.0


class _TTLCache:
    def __init__(self) -> None:
        self._d: Dict[Any, Tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get_or_set(self, key: Any, ttl: float, fn: Callable[[], Any]) -> Any:
        now = time.time()
        with self._lock:
            hit = self._d.get(key)
            if hit and hit[0] > now:
                return hit[1]
        val = fn()  # 실패는 캐시하지 않는다 (에러를 저장해 재반환하던 기존 캐시의 결함 — 첫접촉 진단 §2-3)
        with self._lock:
            self._d[key] = (now + ttl, val)
            if len(self._d) > 4096:  # 상한 — 오래된 것부터
                for k in sorted(self._d, key=lambda k: self._d[k][0])[:1024]:
                    self._d.pop(k, None)
        return val


_cache = _TTLCache()


def _rows(schema: str, sql: str, params: Iterable[Any] = ()) -> List[Dict[str, Any]]:
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    conn = open_schema_connection(schema, readonly=True)
    try:
        return [dict(r) for r in conn.execute(sql, tuple(params)).fetchall()]
    finally:
        try:
            conn.close()
        except Exception:  # 닫기 실패는 결과에 영향 없음 (명시적 사유: 커넥션 정리 경로)
            pass


def num(v: Any) -> Optional[float]:
    """Decimal/float/int → float. None·NaN·해석 불가 → None."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f


def is_nan_value(v: Any) -> bool:
    try:
        return math.isnan(float(v))
    except (TypeError, ValueError):
        return False


def timing_interval(market: str) -> str:
    """시장의 timing 인터벌 = 시장 인터벌 셋의 가장 짧은 것 (coin 15m · kr/us 5m).

    역할 매핑 정본(`resolve_market_role_map`)은 rl_pipeline 을 import 해 응답 경로에 무겁다.
    `get_market_intervals` 는 짧은 순으로 정렬돼 있고 첫 원소가 timing 역할이다(2026-10-15 실측 일치).
    """
    from oneqaz_trading_mcp.data_collection.core.intervals import get_market_intervals
    return get_market_intervals(_INTERVAL_KEY[market])[0]


# ─── 구조 요약 JSON (그룹 상태·멤버십) ─────────────────────────────────

def _load_structure(market: str) -> Optional[Dict[str, Any]]:
    from oneqaz_trading_mcp.config import get_structure_summary_path
    path = get_structure_summary_path(market)
    if path is None or not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _member_key(market: str, sym: str) -> str:
    """구조 멤버 표기 → 시그널 유니버스 표기. coin 구조 멤버는 yfinance 표기(BTC-USD)다."""
    s = str(sym or "").strip().upper()
    if market == "crypto" and s.endswith("-USD"):
        return s[: -len("-USD")]
    return s


def structure_index(market: str) -> Optional[Dict[str, Any]]:
    """{updated_at, updated_ts, overall, extra, groups{gid: raw}, members{gid: [(raw_sym, key, name)]},
        member_groups{key: [gid]}}. 파일 없음 → None."""
    def _build() -> Optional[Dict[str, Any]]:
        raw = _load_structure(market)
        if not raw:
            return None
        from oneqaz_trading_mcp.entity.session import epoch_from_any, iso_from_any
        groups = raw.get("groups") or {}
        members: Dict[str, List[Tuple[str, str, Optional[str]]]] = {}
        member_groups: Dict[str, List[str]] = {}
        for gid, g in groups.items():
            names = g.get("symbol_names_ko") or {}
            lst = []
            for sym, nm in names.items():
                key = _member_key(market, sym)
                real = nm if nm and str(nm).upper() != str(sym).upper() else None
                lst.append((str(sym), key, real))
                member_groups.setdefault(key, []).append(gid)
            members[gid] = lst
        extra = {k: raw.get(k) for k in ("index_spread", "breadth", "concentration", "dominance", "alt_strength")
                 if raw.get(k) is not None}
        return {
            "updated_at": iso_from_any(raw.get("updated_at")),
            "updated_ts": epoch_from_any(raw.get("updated_at")),
            "overall": raw.get("overall") or {},
            "extra": extra,
            "groups": groups,
            "members": members,
            "member_groups": member_groups,
        }
    return _cache.get_or_set(("structure", market), TTL_STRUCTURE, _build)


def global_regime_overall() -> Optional[Dict[str, Any]]:
    def _build():
        from oneqaz_trading_mcp.config import GLOBAL_REGIME_SUMMARY_JSON
        if not GLOBAL_REGIME_SUMMARY_JSON.exists():
            return None
        with open(GLOBAL_REGIME_SUMMARY_JSON, "r", encoding="utf-8") as f:
            d = json.load(f)
        return {"overall": d.get("overall") or {}, "updated_at": d.get("updated_at")}
    return _cache.get_or_set(("global_overall",), TTL_STRUCTURE, _build)


# ─── 시그널 ─────────────────────────────────────────────────────────────

def latest_signal(market: str, symbol: str, interval: str) -> Optional[Dict[str, Any]]:
    """(symbol, interval) 최신 1행. (symbol, interval, timestamp DESC) 인덱스 조기 종료."""
    schema = SIGNAL_SCHEMA[market]

    def _q():
        rows = _rows(
            schema,
            f"SELECT signal_score, confidence, regime_label, timestamp FROM {schema}.signals "
            "WHERE symbol = ? AND interval = ? ORDER BY timestamp DESC LIMIT 1",
            (symbol, interval),
        )
        return rows[0] if rows else None
    return _cache.get_or_set(("sig", market, symbol, interval), TTL_SIGNALS, _q)


def latest_combined_many(market: str, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """여러 심볼의 combined 최신 1행 — LATERAL 로 심볼마다 인덱스 조기 종료.

    DISTINCT ON (symbol) 한 방은 심볼 전 행 정렬이라 coin 에서 수만 행을 읽는다(signals.py:718 주석) — 쓰지 않는다.
    """
    syms = sorted({s for s in symbols if s})
    if not syms:
        return {}
    schema = SIGNAL_SCHEMA[market]

    def _q():
        rows = _rows(
            schema,
            "SELECT s.sym AS symbol, x.signal_score, x.regime_label, x.timestamp "
            "FROM unnest(?::text[]) AS s(sym) CROSS JOIN LATERAL ("
            f"  SELECT signal_score, regime_label, timestamp FROM {schema}.signals "
            "   WHERE symbol = s.sym AND interval = 'combined' ORDER BY timestamp DESC LIMIT 1"
            ") x",
            (syms,),
        )
        return {r["symbol"]: r for r in rows}
    return _cache.get_or_set(("sigmany", market, tuple(syms)), TTL_SIGNALS, _q)


# ─── 이름 ───────────────────────────────────────────────────────────────

def _real_name(sym: str, v: Any) -> Optional[str]:
    s = str(v or "").strip()
    if not s or s.upper() == str(sym).upper() or s == "종목":
        return None
    return s


def names_for(master_ids: List[str], symbols: List[str]) -> Dict[Tuple[str, str], Dict[str, Optional[str]]]:
    """{(master_id, SYMBOL): {name_ko, name_en}} — 신선 행만(오등록 잔재 제외)."""
    syms = sorted({str(s).upper() for s in symbols if s})
    mids = sorted(set(master_ids))
    if not syms or not mids:
        return {}

    def _q():
        rows = _rows(
            "external_context",
            "SELECT market_id, symbol, name_ko, name_en FROM external_context.symbol_master "
            "WHERE market_id = ANY(?) AND upper(symbol) = ANY(?) "
            "  AND updated_at > now() - make_interval(days => ?)",
            (mids, syms, MASTER_FRESH_DAYS),
        )
        out: Dict[Tuple[str, str], Dict[str, Optional[str]]] = {}
        for r in rows:
            sym = str(r["symbol"]).upper()
            out[(r["market_id"], sym)] = {
                "name_ko": _real_name(sym, r["name_ko"]),
                "name_en": _real_name(sym, r["name_en"]),
            }
        return out
    return _cache.get_or_set(("names", tuple(mids), tuple(syms)), TTL_LINKS, _q)


# ─── 종목↔종목 lead-lag (symbol_peer_links) ─────────────────────────────

def peer_links(market: str, symbol: str) -> Dict[str, Any]:
    """{batch_at, rows(최신 배치), stale_rows(그 이전), market_has_any}.

    writer 는 매 실행 상위 30종목을 다시 계산하고 |r|≥0.30 쌍만 upsert 한다(삭제 없음).
    최신 배치에 없는 행 = 이번 실행에서 기준 미달 → 제안서 ED7 에 따라 서빙하지 않는다.
    """
    ah = AH_MARKET[market]

    def _q():
        head = _rows("agent_history",
                     "SELECT max(computed_at) AS batch_at FROM agent_history.symbol_peer_links WHERE market = ?",
                     (ah,))
        batch_at = head[0]["batch_at"] if head else None
        rows = _rows(
            "agent_history",
            "SELECT symbol_lead, symbol_follow, lag_minutes, correlation, sample_size, computed_at "
            "FROM agent_history.symbol_peer_links WHERE market = ? AND (symbol_lead = ? OR symbol_follow = ?)",
            (ah, symbol, symbol),
        )
        from oneqaz_trading_mcp.entity.session import epoch_from_any
        b = epoch_from_any(batch_at)
        cur, old = [], []
        for r in rows:
            t = epoch_from_any(r["computed_at"])
            (cur if (b is not None and t is not None and t >= b - 3600) else old).append(r)
        return {"batch_at": batch_at, "rows": cur, "stale_rows": old, "market_has_any": batch_at is not None}
    return _cache.get_or_set(("peer", market, symbol), TTL_LINKS, _q)


# ─── 상위계층(거시·ETF·바스켓) → 종목 lead-lag (cross_layer_links) ───────────

_CL_COLS = ("follow_symbol, lead_layer, lead_category, lead_symbol, lag_days, correlation, "
            "sample_size, p_value, stable, lookback_days, computed_at")


def cross_layer_for_follow(market: str, symbol: str) -> List[Dict[str, Any]]:
    ah = AH_MARKET[market]
    return _cache.get_or_set(("cl_f", market, symbol), TTL_LINKS, lambda: _rows(
        "agent_history",
        f"SELECT {_CL_COLS} FROM agent_history.cross_layer_links WHERE follow_market = ? AND follow_symbol = ?",
        (ah, symbol),
    ))


def cross_layer_for_basket(market: str, group_id: str) -> List[Dict[str, Any]]:
    ah = AH_MARKET[market]
    return _cache.get_or_set(("cl_b", market, group_id), TTL_LINKS, lambda: _rows(
        "agent_history",
        f"SELECT {_CL_COLS} FROM agent_history.cross_layer_links "
        "WHERE follow_market = ? AND lead_layer = 'basket' AND lead_symbol = ?",
        (ah, f"basket:{group_id}"),
    ))


def cross_layer_coverage(market: str) -> Dict[str, Any]:
    """학습기가 이 시장에서 follow 로 계산한 종목 수·시각 (gap 설명용)."""
    ah = AH_MARKET[market]

    def _q():
        r = _rows("agent_history",
                  "SELECT count(DISTINCT follow_symbol) AS follows, max(computed_at) AS computed_at "
                  "FROM agent_history.cross_layer_links WHERE follow_market = ?", (ah,))
        return r[0] if r else {"follows": 0, "computed_at": None}
    return _cache.get_or_set(("cl_cov", market), TTL_LINKS, _q)


def cross_layer_skill(market: str) -> Optional[Dict[str, Any]]:
    """발화→판정 원장 기반 시장 단위 전향 skill (`cross_layer_market_skill` 뷰)."""
    ah = AH_MARKET[market]

    def _q():
        r = _rows("agent_history",
                  "SELECT n_resolved, n_fire_days, skill, t_cluster, verdict_hint, last_fire_date "
                  "FROM agent_history.cross_layer_market_skill WHERE follow_market = ?", (ah,))
        return r[0] if r else None
    return _cache.get_or_set(("cl_skill", market), TTL_LINKS, _q)


# ─── 그룹↔그룹 상관 (sector_correlation_matrix 최신 스냅샷) ──────────────

def sector_pairs(market: str, group_id: str) -> Dict[str, Any]:
    """{snapshot_at, rows(최신 스냅샷·이 그룹 포함, NaN 포함 원행), in_snapshot}.

    최신 스냅샷만 본다 — 같은 표에 06-26 에 멈춘 종목(ETF) 단위 화석 행(n=8~9)과 이전 스냅샷 잔존 행이
    섞여 있다(진단 §1-1). 스냅샷 경계 = 시장 max(computed_at) − 1h.
    """
    ah = AH_MARKET[market]

    def _q():
        head = _rows("agent_history",
                     "SELECT max(computed_at) AS snap FROM agent_history.sector_correlation_matrix WHERE market = ?",
                     (ah,))
        snap = head[0]["snap"] if head else None
        if snap is None:
            return {"snapshot_at": None, "rows": [], "in_snapshot": False}
        rows = _rows(
            "agent_history",
            "SELECT symbol_a, symbol_b, correlation, sample_size, lookback_days, interval, computed_at "
            "FROM agent_history.sector_correlation_matrix "
            "WHERE market = ? AND computed_at >= ? - interval '1 hour' AND (symbol_a = ? OR symbol_b = ?)",
            (ah, snap, group_id, group_id),
        )
        return {"snapshot_at": snap, "rows": rows, "in_snapshot": bool(rows)}
    return _cache.get_or_set(("sector", market, group_id), TTL_LINKS, _q)


# ─── 뉴스 ───────────────────────────────────────────────────────────────

_NEWS_COLS = "id, symbol, title, title_en, published_at, source, sentiment_label, impact_level, event_type"


def symbol_news(market: str, symbol: str, days: int, limit: int) -> Dict[str, Any]:
    """{total, rows} — 그 종목으로 태깅된 뉴스. 시장 범위(target_markets)로 티커 충돌을 막는다."""
    mid = MASTER_SIGNAL[market]

    def _q():
        where = ("symbol = ? AND ? = ANY(target_markets) "
                 "AND published_at > now() - make_interval(days => ?)")
        params = (symbol, mid, days)
        total = _rows("external_context",
                      f"SELECT count(*) AS n FROM external_context.news_events WHERE {where}", params)
        rows = _rows("external_context",
                     f"SELECT {_NEWS_COLS} FROM external_context.news_events WHERE {where} "
                     "ORDER BY published_at DESC LIMIT ?", (*params, limit))
        return {"total": int(total[0]["n"]) if total else 0, "rows": rows}
    return _cache.get_or_set(("snews", market, symbol, days, limit), TTL_NEWS, _q)


def market_news(market: str, hours: int, limit: int) -> Dict[str, Any]:
    mid = MASTER_SIGNAL[market]

    def _q():
        where = "? = ANY(target_markets) AND published_at > now() - make_interval(hours => ?)"
        params = (mid, hours)
        total = _rows("external_context",
                      f"SELECT count(*) AS n FROM external_context.news_events WHERE {where}", params)
        rows = _rows("external_context",
                     f"SELECT {_NEWS_COLS} FROM external_context.news_events WHERE {where} "
                     "ORDER BY published_at DESC LIMIT ?", (*params, limit))
        return {"total": int(total[0]["n"]) if total else 0, "rows": rows}
    return _cache.get_or_set(("mnews", market, hours, limit), TTL_NEWS, _q)


def news_tagging_coverage(market: str) -> int:
    """최근 30일 이 시장 시그널 유니버스 종목 중 뉴스 태깅이 1건 이상인 종목 수.

    0 이면 "이 시장 종목은 태깅 파이프라인 대상이 아니다" (gap = data_unavailable, not_linked 아님).
    2026-10-15 실측: kr 308/349 · us 0/503 · coin 0/467.
    """
    mid = MASTER_SIGNAL[market]

    def _q():
        r = _rows(
            "external_context",
            "SELECT count(DISTINCT n.symbol) AS k FROM external_context.news_events n "
            "JOIN external_context.symbol_master m ON m.symbol = n.symbol AND m.market_id = ? "
            " AND m.updated_at > now() - make_interval(days => ?) "
            "WHERE ? = ANY(n.target_markets) AND n.published_at > now() - interval '30 days'",
            (mid, MASTER_FRESH_DAYS, mid),
        )
        return int(r[0]["k"]) if r else 0
    return _cache.get_or_set(("ncov", market), TTL_LINKS, _q)


# ─── 보정 (headline.calibration) ────────────────────────────────────────

def signal_calibration_cell(market: str, interval: str) -> Optional[Dict[str, Any]]:
    """`get_signal_calibration` 과 같은 계산(v1 = raw confidence 버킷 — confidence_calibrated 의 기반)."""
    def _q():
        from oneqaz_trading_mcp.tools.signal_calibration import _get_signal_calibration
        d = _get_signal_calibration(market_id=market)
        if not isinstance(d, dict) or d.get("error"):
            return None
        m = (d.get("markets") or {}).get(market) or {}
        cell = (m.get("intervals") or {}).get(interval)
        if not cell:
            return None
        return {
            "ece": cell.get("ece"),
            "n": cell.get("n_total"),
            "status": (cell.get("quality") or {}).get("status"),
            "snapshot_day": m.get("snapshot_day"),
        }
    return _cache.get_or_set(("calib", market, interval), TTL_CALIBRATION, _q)


def structure_skill(market: str, interval: str = "1d", days: int = 30) -> Dict[str, Any]:
    """구조 예측 판정(시장 × 인터벌, 최근 days): n · 적중률 · 최빈 클래스 기준선 · skill."""
    schema = STRUCT_SCHEMA[market]

    def _q():
        rows = _rows(
            schema,
            f"SELECT actual_direction, count(*) AS n, sum(is_correct) AS hits FROM {schema}.structure_predictions "
            "WHERE interval = ? AND validated_at IS NOT NULL "
            "  AND validated_at > extract(epoch FROM now() - make_interval(days => ?)) "
            "GROUP BY actual_direction",
            (interval, days),
        )
        n = sum(int(r["n"] or 0) for r in rows)
        hits = sum(int(r["hits"] or 0) for r in rows)
        if n == 0:
            return {"n": 0, "hit_rate": None, "baseline": None, "skill": None}
        base = max(int(r["n"] or 0) for r in rows) / n
        hr = hits / n
        return {"n": n, "hit_rate": round(hr, 3), "baseline": round(base, 3), "skill": round(hr - base, 3)}
    return _cache.get_or_set(("sskill", market, interval, days), TTL_CALIBRATION, _q)


# ─── 거시 → 시장 가설 (influence map) ───────────────────────────────────

def macro_hypotheses(market: str) -> Dict[str, Any]:
    """{cells: {category: cell}, as_of(튜닝 시각), status}. 셀 = get_macro_influence_map 과 같은 계산."""
    def _q():
        # 프로파일 모듈이 이 배포에 없으면(공개 패키지) ImportError 를 그대로 올린다. 아래 경로는 그 실패를
        # 에러 dict 로 감싸 RuntimeError(일시 장애)로 보이게 하므로, 상시 부재를 여기서 먼저 가른다.
        import market.global_regime.profiles  # noqa: F401
        from oneqaz_trading_mcp.tools.trust_layer import _get_macro_influence_map
        d = _get_macro_influence_map(market)
        if not isinstance(d, dict) or d.get("error"):
            raise RuntimeError(f"macro influence map unavailable: {d.get('reason') if isinstance(d, dict) else d}")
        target = MASTER_SIGNAL[market]
        cells = {cat: v.get(target) for cat, v in (d.get("influence_map") or {}).items() if v.get(target)}
        tun = _rows("market_global",
                    "SELECT category, max(last_backtest) AS t FROM market_global.backtest_tuning "
                    "WHERE target_market = ? GROUP BY category", (target,))
        return {
            "cells": cells,
            "tuned_at": {r["category"]: r["t"] for r in tun},
            "status": (d.get("meta") or {}).get("effective_status"),
        }
    return _cache.get_or_set(("macro_h", market), TTL_LINKS, _q)


def macro_category_members() -> Dict[str, Dict[str, Optional[str]]]:
    """{SYMBOL: {category, name_ko}} — symbol_master 의 거시 카테고리 구성 종목 (HYG → credit 등)."""
    def _q():
        rows = _rows(
            "external_context",
            "SELECT market_id, symbol, name_ko FROM external_context.symbol_master WHERE market_id = ANY(?) "
            "AND updated_at > now() - make_interval(days => ?)",
            (list(MACRO_CATEGORIES), MASTER_FRESH_DAYS),
        )
        return {str(r["symbol"]).upper(): {"category": r["market_id"], "name_ko": _real_name(r["symbol"], r["name_ko"])}
                for r in rows}
    return _cache.get_or_set(("macro_members",), TTL_LINKS, _q)
