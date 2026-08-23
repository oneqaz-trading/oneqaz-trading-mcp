# -*- coding: utf-8 -*-
"""get_signal_calibration — Level-1 시그널 confidence 캘리브레이션 (RCA 2026-07-20 T5).

signal_calibration_daily 스냅샷(mcps/ledger_integrity.py 일별 집계)을 읽어
confidence 버킷별 실현 적중률 + ECE 를 서빙한다. 에이전트가 "confidence 0.9 는
실제로 0.9 인가"를 스스로 검증할 수 있게 하는 reliability diagram 데이터.

실현 적중 정의(기존 시그널 feedback 파이프라인 재사용, trade/core/realtime_accuracy.py):
- 시그널 생성 시점 가격 → verify_at 도달 후 가격의 방향 일치 (UP/DOWN)
- |변동| < 0.3% 는 판단 불가(-1)로 제외, NEUTRAL 예측은 |변동| < 1% 면 적중
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.resources.resource_response import (
    MCPErrorCode,
    mcp_error,
    wrap_tool_response,
)

logger = logging.getLogger("MarketMCP")

_BUCKET_MID = {
    "[0.0,0.5)": 0.25,
    "[0.5,0.6)": 0.55,
    "[0.6,0.7)": 0.65,
    "[0.7,0.8)": 0.75,
    "[0.8,0.9)": 0.85,
    "[0.9,1.0]": 0.95,
}
_BUCKET_ORDER = list(_BUCKET_MID.keys())

_VALID_MARKETS = ("crypto", "kr_stock", "us_stock")


def _ece(buckets: List[Dict[str, Any]]) -> Optional[float]:
    """Expected Calibration Error — 버킷 중앙값 근사 (meta 에 명시)."""
    total = sum(b["n"] for b in buckets)
    if total <= 0:
        return None
    err = 0.0
    for b in buckets:
        mid = _BUCKET_MID.get(b["bucket"])
        if mid is None or b["n"] == 0:
            continue
        err += (b["n"] / total) * abs(b["observed_accuracy"] - mid)
    return round(err, 4)


def _get_signal_calibration(
    market_id: Optional[str] = None,
    interval: Optional[str] = None,
    variant: str = "v1",
) -> Dict[str, Any]:
    if market_id is not None and market_id not in _VALID_MARKETS:
        return mcp_error(
            MCPErrorCode.MISSING_REQUIRED_FIELD,
            f"unknown market_id '{market_id}'",
            available_markets=list(_VALID_MARKETS),
        )
    if variant not in ("v1", "v2"):
        return mcp_error(
            MCPErrorCode.MISSING_REQUIRED_FIELD,
            f"unknown variant '{variant}' (v1 = raw confidence, v2 = outcome-based shadow)",
        )

    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    try:
        conn = open_schema_connection("mcp_analytics", readonly=True)
    except Exception as exc:
        logger.exception("get_signal_calibration: PG open failed")
        return mcp_error(MCPErrorCode.DB_NOT_FOUND, f"mcp_analytics unavailable: {exc}")

    try:
        last = conn.execute(
            "SELECT MAX(day) AS d FROM signal_calibration_daily"
        ).fetchone()
        day = last["d"] if last else None
        if not day:
            return mcp_error(
                MCPErrorCode.NO_DATA,
                "no calibration snapshot yet (daily thread populates signal_calibration_daily)",
            )

        query = (
            "SELECT market_id, \"interval\", bucket, n, hits, ts_min, ts_max "
            "FROM signal_calibration_daily WHERE day = ? AND variant = ?"
        )
        params: List[Any] = [day, variant]
        if market_id:
            query += " AND market_id = ?"
            params.append(market_id)
        if interval:
            query += " AND \"interval\" = ?"
            params.append(interval)
        rows = [dict(r) for r in conn.execute(query, params).fetchall()]
    except Exception as exc:
        logger.exception("get_signal_calibration failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"query failed: {exc}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # (market, interval) 별 + market 전체(all) 집계
    markets: Dict[str, Any] = {}
    for mid in sorted({r["market_id"] for r in rows}):
        m_rows = [r for r in rows if r["market_id"] == mid]
        intervals: Dict[str, Any] = {}
        for itv in sorted({r["interval"] for r in m_rows}):
            i_rows = [r for r in m_rows if r["interval"] == itv]
            intervals[itv] = _bucket_block(i_rows)
        markets[mid] = {
            "intervals": intervals,
            "all_intervals": _bucket_block(m_rows),
            "observation_window_utc": _window_of(m_rows),
        }

    return {
        "snapshot_day": day,
        "variant": variant,
        "markets": markets,
        "meta": {
            "variant_note": (
                "variant=v1: raw heuristic confidence (uninformative — measured flat "
                "~50% for crypto/kr). variant=v2: outcome-based confidence_v2 "
                "(RCA C2, 2026-07-21). PROMOTED 2026-08-20 (POLICY 08-20-r1): v2 met "
                "the phase gate (ECE beating v1 on 7+ consecutive daily snapshots in "
                "all 3 markets; paired-cohort full-ECE reduction 88~93%) and now "
                "serves the wait-gate decision path with v1 fail-open fallback. "
                "Both variants remain recorded for post-promotion regression watch."
            ),
            "source": "mcp_analytics.signal_calibration_daily (daily snapshot)",
            "realized_hit_definition": (
                "Reuses the existing signal feedback pipeline judgment "
                "(trade/core/realtime_accuracy.py): predicted UP/DOWN vs realized price "
                "direction between signal time and verify_at; |change| < 0.3% is excluded "
                "as undecidable; NEUTRAL predictions count as hits iff |change| < 1%. "
                "Only judged rows (is_correct in {0,1}) are counted."
            ),
            "confidence_source": (
                "signals.confidence — the same value served by get_signals (Level 1). "
                "Joined to the judgment ledger on (symbol, interval, timestamp); rows "
                "whose signal record has aged out of the signals table are not joinable, "
                "so coverage is partial (observation window ≈ signals retention, ~2 weeks)."
            ),
            "ece_method": (
                "ECE = sum_b (n_b/N) * |observed_accuracy_b - bucket_midpoint_b| — "
                "bucket-midpoint approximation."
            ),
            "sample_caveat": (
                "n is nominal: same-symbol/same-cycle signals are correlated trials, so "
                "treat significance conservatively (see get_prediction_accuracy "
                "n_effective discussion for the same issue at the macro layer)."
            ),
            "interpretation": (
                "A calibrated confidence c should realize ≈ c hit rate. If the [0.9,1.0] "
                "bucket realizes far below 0.9, treat signal confidence as a relative "
                "ranking score, not a probability."
            ),
        },
    }


def _bucket_block(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    agg: Dict[str, Dict[str, int]] = {}
    for r in rows:
        b = agg.setdefault(r["bucket"], {"n": 0, "hits": 0})
        b["n"] += int(r["n"])
        b["hits"] += int(r["hits"])
    buckets = []
    for name in _BUCKET_ORDER:
        if name not in agg:
            continue
        n = agg[name]["n"]
        hits = agg[name]["hits"]
        buckets.append({
            "bucket": name,
            "n": n,
            "hits": hits,
            "observed_accuracy": round(hits / n, 4) if n else None,
        })
    return {
        "buckets": buckets,
        "n_total": sum(b["n"] for b in buckets),
        "ece": _ece([b for b in buckets if b["observed_accuracy"] is not None]),
    }


def _window_of(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    ts_mins = [r["ts_min"] for r in rows if r.get("ts_min")]
    ts_maxs = [r["ts_max"] for r in rows if r.get("ts_max")]
    if not ts_mins or not ts_maxs:
        return None
    from datetime import datetime, timezone
    return {
        "from": datetime.fromtimestamp(min(ts_mins), tz=timezone.utc).isoformat(),
        "to": datetime.fromtimestamp(max(ts_maxs), tz=timezone.utc).isoformat(),
    }


def _ai_summary_calibration(data: Dict[str, Any]) -> str:
    parts = []
    for mid, m in (data.get("markets") or {}).items():
        blk = m.get("all_intervals") or {}
        top = next((b for b in reversed(blk.get("buckets") or []) if b["bucket"] == "[0.9,1.0]"), None)
        ece = blk.get("ece")
        seg = f"{mid}: ECE={ece}"
        if top:
            seg += f", conf 0.9+ realized {top['observed_accuracy']} (n={top['n']})"
        parts.append(seg)
    return (
        "Confidence reliability table (bucket-level realized hit rates). "
        + "; ".join(parts)
        + ". If realized << stated confidence, treat confidence as ranking score, not probability."
    )


def _user_summary_calibration(data: Dict[str, Any]) -> str:
    n = sum((m.get("all_intervals") or {}).get("n_total", 0)
            for m in (data.get("markets") or {}).values())
    return f"시그널 확신도 버킷별 실현 적중률 표를 반환했습니다 (판정 표본 {n:,}건)."


def register_signal_calibration_tools(mcp, cache):
    """get_signal_calibration 등록 (RCA T5)."""

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_signal_calibration(
        market_id: str = None,
        interval: str = None,
        variant: str = "v1",
    ) -> Dict[str, Any]:
        """
        Purpose: Reliability diagram data for Level-1 signal confidence — realized hit
            rate per confidence bucket ([0.5,0.6) ... [0.9,1.0]) with ECE summary.
            Lets an agent verify whether a 0.9-confidence signal actually hits ~90%.
        Triggers (casual questions too): "is your confidence calibrated?",
            "confidence 0.9 믿어도 돼?", "시그널 확신도 실제 적중률 보여줘",
            "how reliable are signal confidences?".
        When to call: before trusting get_signals confidence values as probabilities.
        Prerequisites: none.
        Next steps: get_prediction_accuracy (macro-layer skill), get_signals.
        Caveats: snapshot is daily; observation window ≈ signals table retention
            (~2 weeks); n is nominal (correlated trials — see meta.sample_caveat).

        Args:
            market_id: Optional filter (crypto | kr_stock | us_stock)
            interval: Optional candle interval filter (e.g. 15m, 30m, 240m, 1d)
            variant: "v1" (raw heuristic confidence, default) or "v2"
                (outcome-based shadow confidence — RCA C2, accumulating since 2026-07-21)

        Disclaimer: Information only, not investment advice.
        """
        import asyncio
        def _build():
            result = _get_signal_calibration(market_id=market_id, interval=interval,
                                             variant=variant)
            # [2026-08-11-r1] era 주석 — 시그널 라벨/잣대 정책 변경(action 정직화 등)이
            # 있으면 분포가 버전 경계에서 단절된다. 외부 AI 소비자가 커브 이동을
            # 정책 변경으로 설명할 수 있도록 현재 policy_version 을 노출한다.
            try:
                from oneqaz_trading_mcp.shared.policy_version import get_policy_version
                if isinstance(result, dict) and isinstance(result.get('meta'), dict):
                    result['meta']['policy_version'] = get_policy_version()
                elif isinstance(result, dict):
                    result['policy_version'] = get_policy_version()
            except Exception:
                pass
            return wrap_tool_response(
                result, "trust_layer",
                _ai_summary_calibration, _user_summary_calibration,
            )
        return await asyncio.to_thread(_build)
