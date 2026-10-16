# -*- coding: utf-8 -*-
"""[2026-07-20 RCA C1] 표면 전용 confidence 캘리브레이션 매핑.

signal_calibration_daily 최신 스냅샷(버킷별 실현 적중률)으로 시장(×interval)별
단조(PAV) 매핑을 만들어 `confidence_calibrated` 를 **병행 노출**한다.

원칙 (docs/rca_20260720/confidence_investigation.md C1):
- 기존 confidence 필드 불변 (additive) — 매매 경로 무접촉, MCP 표면 전용.
- 실측이 없으면 조용히 None (fail-open) — 추정치 날조 금지.
- interval 매핑은 판정 표본 200+ 일 때만, 아니면 시장 전체 집계로 폴백
  ('combined' 등 스냅샷에 없는 interval 포함).
- [2026-10-05] 판정 인공물 셀(`is_perfect_hit_artifact`)은 매핑에서 빼고 None —
  시장 집계로 폴백하지 않는다 (그 interval 의 판정 자체가 무효이므로).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger("MarketMCP")

_BUCKET_BOUNDS: List[Tuple[str, float, float]] = [
    ("[0.0,0.5)", 0.0, 0.5),
    ("[0.5,0.6)", 0.5, 0.6),
    ("[0.6,0.7)", 0.6, 0.7),
    ("[0.7,0.8)", 0.7, 0.8),
    ("[0.8,0.9)", 0.8, 0.9),
    ("[0.9,1.0]", 0.9, 1.01),
]

_MIN_INTERVAL_SAMPLES = 200   # interval 전용 매핑 최소 판정 표본
_CACHE_TTL_SEC = 600

_lock = threading.Lock()
_cache: Dict[str, object] = {"ts": 0.0, "maps": None}

# [2026-10-05] 판정 인공물 판별 — get_signal_calibration 의 셀 품질 가드와 공유하는 단일 정의.
ARTIFACT_MIN_N = 30


def is_perfect_hit_artifact(buckets: Iterable[Dict[str, Any]]) -> bool:
    """버킷 하나라도 n ≥ ARTIFACT_MIN_N 인데 전부 적중(hits == n)이면 판정 인공물로 본다.

    방향 판정에서 수십 건 연속 100% 적중은 실현될 수 없는 값이다. 실측(2026-10-05):
    kr/us 1d 는 30분 판정 창에서 1d 가격이 움직이지 않아 NEUTRAL 만 전부 적중으로 남았다.
    0% 는 인공물로 보지 않는다 — 고확신 버킷이 체계적으로 틀리는 것은 캘리브레이션이
    보여줘야 할 핵심 증거다 (반대편 스틸맨 C1③).
    """
    for b in buckets:
        n = int(b.get("n") or 0)
        hits = int(b.get("hits") or 0)
        if n >= ARTIFACT_MIN_N and hits == n:
            return True
    return False


def _bucket_index(confidence: float) -> Optional[int]:
    for i, (_name, lo, hi) in enumerate(_BUCKET_BOUNDS):
        if lo <= confidence < hi:
            return i
    return None


def pav_values(pairs: List[Tuple[Optional[float], float]]) -> List[Optional[float]]:
    """가중 Pool-Adjacent-Violators (비감소 제약).

    pairs: 버킷 순서대로 (실측 적중률 or None, 가중치=n). None 버킷은 이웃
    블록에 편입되지 않고 결과도 None (해당 버킷 표본 0 → 매핑 불가).
    """
    blocks: List[List[float]] = []   # [sum_wy, sum_w, start_idx, end_idx]
    idx_present: List[int] = []
    for i, (y, w) in enumerate(pairs):
        if y is None or w <= 0:
            continue
        idx_present.append(i)
        blocks.append([y * w, w, i, i])
        while len(blocks) >= 2 and (
            blocks[-2][0] / blocks[-2][1] > blocks[-1][0] / blocks[-1][1]
        ):
            b = blocks.pop()
            blocks[-1][0] += b[0]
            blocks[-1][1] += b[1]
            blocks[-1][3] = b[3]

    out: List[Optional[float]] = [None] * len(pairs)
    for sum_wy, sum_w, start, end in blocks:
        v = sum_wy / sum_w
        for i in range(start, end + 1):
            if i in idx_present:
                out[i] = round(v, 4)
    return out


def _load_maps() -> Dict[Tuple[str, Optional[str]], Optional[List[Optional[float]]]]:
    """최신 스냅샷 → {(market, interval|None): PAV 단조 버킷 값 | None(판정 인공물 셀)}."""
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection

    conn = open_schema_connection("mcp_analytics", readonly=True)
    try:
        # [2026-07-21] variant='v1' 고정 — 이 매핑의 입력은 raw confidence 다.
        # (v2 는 이미 outcome 기반이라 재매핑 대상이 아님)
        # [2026-10-05] 시장별 최신 day — 종전 전역 MAX(day) 는 score_* variant 나 다른
        # 시장이 먼저 적재된 날 늦게 도는 시장(crypto 집계 재시도 중)의 매핑을 비웠다.
        latest = {
            r["market_id"]: r["d"]
            for r in conn.execute(
                "SELECT market_id, MAX(day) AS d FROM signal_calibration_daily "
                "WHERE variant = 'v1' GROUP BY market_id"
            ).fetchall()
            if r["d"]
        }
        if not latest:
            return {}
        params: List[Any] = []
        for mid, d in latest.items():
            params.extend([mid, d])
        rows = [dict(r) for r in conn.execute(
            "SELECT market_id, \"interval\", bucket, n, hits "
            "FROM signal_calibration_daily WHERE variant = 'v1' AND ("
            + " OR ".join("(market_id = ? AND day = ?)" for _ in latest)
            + ")",
            params,
        ).fetchall()]
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # [2026-10-05] 판정 인공물 셀은 interval 매핑에서도, 시장 전체 집계에서도 뺀다.
    #   종전엔 kr/us 1d 가 1.0 으로 매핑돼 get_signals 의 confidence_calibrated 로 나갔고,
    #   prompts.py 는 그 값을 확률로 인용하라고 안내했다 (반대편 스틸맨 B18).
    by_cell: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        by_cell.setdefault((r["market_id"], r["interval"]), []).append(r)
    artifact_cells = {cell for cell, cell_rows in by_cell.items()
                      if is_perfect_hit_artifact(cell_rows)}
    if artifact_cells:
        rows = [r for r in rows if (r["market_id"], r["interval"]) not in artifact_cells]

    # (market, interval) + (market, None=전체) 집계
    agg: Dict[Tuple[str, Optional[str]], Dict[str, List[int]]] = {}
    for r in rows:
        for key in ((r["market_id"], r["interval"]), (r["market_id"], None)):
            b = agg.setdefault(key, {}).setdefault(r["bucket"], [0, 0])
            b[0] += int(r["n"])
            b[1] += int(r["hits"])

    maps: Dict[Tuple[str, Optional[str]], Optional[List[Optional[float]]]] = {}
    for key, buckets in agg.items():
        total = sum(v[0] for v in buckets.values())
        if key[1] is not None and total < _MIN_INTERVAL_SAMPLES:
            continue
        pairs: List[Tuple[Optional[float], float]] = []
        for name, _lo, _hi in _BUCKET_BOUNDS:
            if name in buckets and buckets[name][0] > 0:
                n, hits = buckets[name]
                pairs.append((hits / n, float(n)))
            else:
                pairs.append((None, 0.0))
        maps[key] = pav_values(pairs)
    # 인공물 셀 표식 — calibrated_confidence 가 시장 집계로 폴백하지 않게 한다.
    for cell in artifact_cells:
        maps[cell] = None
    return maps


def _get_maps() -> Dict[Tuple[str, Optional[str]], Optional[List[Optional[float]]]]:
    now = time.time()
    with _lock:
        if _cache["maps"] is not None and now - float(_cache["ts"]) < _CACHE_TTL_SEC:
            return _cache["maps"]  # type: ignore[return-value]
    try:
        maps = _load_maps()
    except Exception as e:
        logger.warning("confidence calibration map load failed (fail-open): %s", e)
        maps = {}
    with _lock:
        _cache["maps"] = maps
        _cache["ts"] = now
    return maps


def calibrated_confidence(
    market_id: Optional[str],
    interval: Optional[str],
    confidence: Optional[float],
) -> Optional[float]:
    """confidence → 실측 기반 캘리브레이션 값. 매핑 불가 시 None (fail-open)."""
    if market_id is None or confidence is None:
        return None
    try:
        c = float(confidence)
    except (TypeError, ValueError):
        return None
    idx = _bucket_index(c)
    if idx is None:
        return None
    maps = _get_maps()
    key = (market_id, interval)
    if key in maps and maps[key] is None:
        return None  # 판정 인공물 셀 — 측정 매핑 없음 (시장 집계 폴백 금지)
    vals = maps.get(key) or maps.get((market_id, None))
    if not vals:
        return None
    return vals[idx]


CALIBRATION_NOTE = (
    "confidence_calibrated = realized hit-rate mapping of the raw confidence "
    "(bucket-level, monotone/PAV, from the daily signal_calibration snapshot; "
    "market-level fallback when the interval lacks 200+ judged samples). "
    "Raw confidence is a ranking heuristic, NOT a probability (see "
    "get_signal_calibration); use confidence_calibrated as the probability-like "
    "quantity. null = no measured mapping available (never fabricated), including "
    "intervals whose judgments are withheld as an artifact (see "
    "get_signal_calibration intervals[*].quality)."
)


# ---------------------------------------------------------------------------
# [2026-10-14] 점수(|signal_score|) 구간별 실측 24h 적중률 — get_daily_brief 신호 예시의 근거 표
# ---------------------------------------------------------------------------
# 원천 = mcp_analytics.signal_calibration_daily variant='score_raw_h24' (mcps/ledger_integrity.py
# snapshot_score_calibration: combined · predicted_direction='UP'(매수) · score>=0 · (symbol, 30분 창) 최신 1행 ·
# 최근 30일 · is_correct_24h). 버킷 라벨 = 'x2[0.1,0.2)' … 'x2[0.6,inf)'. 매수 신호만 측정된다.
_SCORE_CACHE: Dict[str, object] = {"ts": 0.0, "data": None}


def _load_score_evidence() -> Dict[str, Dict[str, Any]]:
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    conn = open_schema_connection("mcp_analytics", readonly=True)
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT s.market_id, s.day, s.bucket, s.n, s.hits FROM signal_calibration_daily s "
            "JOIN (SELECT market_id, MAX(day) AS d FROM signal_calibration_daily "
            "      WHERE variant = 'score_raw_h24' GROUP BY market_id) l "
            "  ON l.market_id = s.market_id AND l.d = s.day "
            "WHERE s.variant = 'score_raw_h24'"
        ).fetchall()]
    finally:
        try:
            conn.close()
        except Exception:
            pass
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        m = out.setdefault(r["market_id"], {"day": str(r["day"]), "buckets": {}, "n": 0, "hits": 0})
        n, hits = int(r["n"] or 0), int(r["hits"] or 0)
        m["buckets"][r["bucket"]] = (n, hits)
        m["n"] += n
        m["hits"] += hits
    return out


def _get_score_evidence() -> Dict[str, Dict[str, Any]]:
    now = time.time()
    with _lock:
        if _SCORE_CACHE["data"] is not None and now - float(_SCORE_CACHE["ts"]) < _CACHE_TTL_SEC:
            return _SCORE_CACHE["data"]  # type: ignore[return-value]
    try:
        data = _load_score_evidence()
    except Exception as e:  # fail-open — 근거 표가 없으면 숫자를 싣지 않는다(지어내지 않음)
        logger.warning("score bucket evidence load failed: %s", e)
        data = {}
    with _lock:
        _SCORE_CACHE["ts"], _SCORE_CACHE["data"] = now, data
    return data


def score_bucket_label(score: float) -> Optional[str]:
    """스냅샷과 같은 버킷 라벨(ledger_integrity._X2_BUCKET). 음수(매도)는 측정 밖 → None."""
    if score is None or score < 0:
        return None
    if score >= 0.6:
        return "x2[0.6,inf)"
    lo = int(score * 10) / 10.0
    return f"x2[{lo:.1f},{lo + 0.1:.1f})"


def score_bucket_hit_rate(market_id: Optional[str], action: Optional[str], score: Optional[float]) -> Optional[Dict[str, Any]]:
    """신호 1건의 점수 구간 실측 24h 적중률 {bucket, hit_rate, n, market_base_rate, day}. 측정 불가 = None."""
    if market_id is None or score is None or action != "buy":
        return None
    label = score_bucket_label(float(score))
    ev = _get_score_evidence().get(market_id)
    if not label or not ev or label not in ev["buckets"]:
        return None
    n, hits = ev["buckets"][label]
    if n <= 0:
        return None
    return {"bucket": label, "hit_rate": round(hits / n, 3), "n": n,
            "market_base_rate": round(ev["hits"] / ev["n"], 3) if ev["n"] else None, "day": ev["day"]}


def score_edge_evidence(market_id: str) -> Optional[Dict[str, Any]]:
    """시장의 점수 구간별 실측 24h 적중률 표 — 점수가 적중을 가르는지(우위) 사람이·AI 가 직접 보게 한다."""
    ev = _get_score_evidence().get(market_id)
    if not ev or not ev["n"]:
        return None
    buckets = [{"bucket": b, "n": n, "hit_rate": round(h / n, 3)}
               for b, (n, h) in sorted(ev["buckets"].items()) if n > 0]
    return {"day": ev["day"], "base_rate_24h": round(ev["hits"] / ev["n"], 3), "n": ev["n"], "buckets": buckets}
