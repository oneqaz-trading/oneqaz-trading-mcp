# -*- coding: utf-8 -*-
"""[2026-07-20 RCA C1] 표면 전용 confidence 캘리브레이션 매핑.

signal_calibration_daily 최신 스냅샷(버킷별 실현 적중률)으로 시장(×interval)별
단조(PAV) 매핑을 만들어 `confidence_calibrated` 를 **병행 노출**한다.

원칙 (docs/rca_20260720/confidence_investigation.md C1):
- 기존 confidence 필드 불변 (additive) — 매매 경로 무접촉, MCP 표면 전용.
- 실측이 없으면 조용히 None (fail-open) — 추정치 날조 금지.
- interval 매핑은 판정 표본 200+ 일 때만, 아니면 시장 전체 집계로 폴백
  ('combined' 등 스냅샷에 없는 interval 포함).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Dict, List, Optional, Tuple

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


def _load_maps() -> Dict[Tuple[str, Optional[str]], List[Optional[float]]]:
    """최신 스냅샷 → {(market, interval|None): PAV 단조 버킷 값}."""
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection

    conn = open_schema_connection("mcp_analytics", readonly=True)
    try:
        last = conn.execute(
            "SELECT MAX(day) AS d FROM signal_calibration_daily"
        ).fetchone()
        day = last["d"] if last else None
        if not day:
            return {}
        # [2026-07-21] variant='v1' 고정 — 이 매핑의 입력은 raw confidence 다.
        # (v2 는 이미 outcome 기반이라 재매핑 대상이 아님)
        rows = [dict(r) for r in conn.execute(
            "SELECT market_id, \"interval\", bucket, n, hits "
            "FROM signal_calibration_daily WHERE day = ? AND variant = 'v1'",
            (day,),
        ).fetchall()]
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # (market, interval) + (market, None=전체) 집계
    agg: Dict[Tuple[str, Optional[str]], Dict[str, List[int]]] = {}
    for r in rows:
        for key in ((r["market_id"], r["interval"]), (r["market_id"], None)):
            b = agg.setdefault(key, {}).setdefault(r["bucket"], [0, 0])
            b[0] += int(r["n"])
            b[1] += int(r["hits"])

    maps: Dict[Tuple[str, Optional[str]], List[Optional[float]]] = {}
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
    return maps


def _get_maps() -> Dict[Tuple[str, Optional[str]], List[Optional[float]]]:
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
    vals = maps.get((market_id, interval)) or maps.get((market_id, None))
    if not vals:
        return None
    return vals[idx]


CALIBRATION_NOTE = (
    "confidence_calibrated = realized hit-rate mapping of the raw confidence "
    "(bucket-level, monotone/PAV, from the daily signal_calibration snapshot; "
    "market-level fallback when the interval lacks 200+ judged samples). "
    "Raw confidence is a ranking heuristic, NOT a probability (see "
    "get_signal_calibration); use confidence_calibrated as the probability-like "
    "quantity. null = no measured mapping available (never fabricated)."
)
