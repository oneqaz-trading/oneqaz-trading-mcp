# -*- coding: utf-8 -*-
"""get_signal_calibration — Level-1 시그널 confidence 캘리브레이션 (RCA 2026-07-20 T5).

signal_calibration_daily 스냅샷(mcps/ledger_integrity.py 일별 집계)을 읽어
confidence 버킷별 실현 적중률 + ECE 를 서빙한다. 에이전트가 "confidence 0.9 는
실제로 0.9 인가"를 스스로 검증할 수 있게 하는 reliability diagram 데이터.

실현 적중 정의(기존 시그널 feedback 파이프라인 재사용, trade/core/realtime_accuracy.py):
- 시그널 생성 시점 가격 → verify_at 도달 후 가격의 방향 일치 (UP/DOWN)
- 인터벌별 단일 밴드(`_judge_prediction`): NEUTRAL 은 |변동| < 밴드면 적중, UP/DOWN 은
  |변동| < 밴드면 판단 불가(-1). (종전 서술 "0.3% / 1%" 는 롤백 전용 레거시 규칙)
- ⚠️ verify_at 은 **모든 인터벌에서 시그널 + 30분**이다 (1d · 240m 포함, 2026-10-05 실측).

[2026-10-05 B2] 셀 품질 가드 — `_cell_quality` 참조.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any, Dict, List, Optional

from pydantic import Field

from oneqaz_trading_mcp.confidence_calibration import ARTIFACT_MIN_N, is_perfect_hit_artifact
from oneqaz_trading_mcp.resources.resource_response import (
    MCPErrorCode,
    mcp_error,
    wrap_tool_response,
)

logger = logging.getLogger("MarketMCP")

# [2026-09-23 P0-B] pinned outputSchema — FastMCP 가 반환 annotation 을
# introspect 해 outputSchema 를 만든다. pydantic 부재 시 Dict 로 격하.
try:
    from oneqaz_trading_mcp.schemas import SignalCalibrationEnvelope
except ImportError:  # pragma: no cover
    SignalCalibrationEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]



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

# [2026-10-05 B2] 셀 품질 가드 — (market, interval) 셀의 판정 표본이 confidence 를
# 확률로 읽을 근거가 되는지 응답에 표시한다.
# 계기: kr_stock · us_stock 1d 셀이 [0.9,1.0] 버킷 적중률 1.000 (n 1,320 / 945) 으로
# 외부에 광고되고 있었다. 실측 원인 — 판정 지평이 모든 인터벌에서 30분이고,
# kr/us 1d 가격은 30분 동안 움직이지 않아(변화 0.000) UP/DOWN 은 전부 판정 불가(-1)로
# 빠지고 NEUTRAL 만 전부 적중으로 남는다 (같은 신호의 24h 판정은 kr NEUTRAL 약 32%).
# 판정 원장 · confidence_v2 (엔진)는 여기서 고치지 않는다 — 응답에서만 가린다
# (mcps/verification.py 와 같은 원칙: DB 원본 불변, raw 수치를 남기면 소비 AI 가 인용한다).
# 가림 여부는 데이터가 정한다 → 인공물 행이 창 밖으로 나가면 자동 해제된다.
# [2026-10-05 스틸맨 정정] 가드 축소: 0% 버킷은 가리지 않고(체계적 오판은 보여줘야 할 증거),
# non_monotone 은 허용오차를 넘는 하락만, all_intervals 는 원래 의미(전 인터벌)를 유지한다.
# 인공물 판별은 confidence_calibrated 매핑과 같은 정의를 쓴다(단일 정의 — 상단 import).
_DEGENERATE_SHARE = 0.95  # 한 버킷이 표본의 95% 이상 → confidence 에 순위 정보가 없다
_MONO_MIN_N = 100         # non_monotone 비교에 쓰는 버킷 최소 표본
_MONO_TOL = 0.05          # 이보다 큰 하락만 non_monotone (n 은 명목값이라 작은 차이는 잡음)

# 가드가 발화했을 때만 덧붙이는 실측 원인 설명 (발화 조건은 아니다).
_KNOWN_ARTIFACT_CAUSES = {
    ("kr_stock", "1d"): (
        "30-minute judgment of a 1d signal: the kr_stock 1d price did not move within "
        "30 minutes, so UP/DOWN rows were undecidable and only NEUTRAL rows survived as "
        "hits. These rows date from before 2026-09-02 (1d 30-minute judgments stopped "
        "after that); the cell ages out of the observation window around 2026-10-12. "
        "The 24h judgment of the same NEUTRAL signals realizes about 32%."
    ),
    ("us_stock", "1d"): (
        "30-minute judgment of a 1d signal: the us_stock 1d price did not move within "
        "30 minutes, so UP/DOWN rows were undecidable and only NEUTRAL rows survived as "
        "hits. These rows date from before 2026-09-01 (1d 30-minute judgments stopped "
        "after that); the cell ages out of the observation window around 2026-10-11."
    ),
}


def _cell_quality(market_id: str, itv: str, block: Dict[str, Any]) -> Dict[str, Any]:
    """셀 품질 — withheld > degenerate > non_monotone > ok 중 처음 해당하는 상태."""
    buckets = block.get("buckets") or []
    n_total = block.get("n_total") or 0
    if n_total <= 0 or not buckets:
        return {"status": "no_data"}

    if is_perfect_hit_artifact(buckets):
        return {
            "status": "withheld",
            "reason": "judgment_artifact_suspected",
            "buckets_flagged": [b["bucket"] for b in buckets
                                if b["n"] >= ARTIFACT_MIN_N and b["hits"] == b["n"]],
            "note": _KNOWN_ARTIFACT_CAUSES.get((market_id, itv)) or (
                f"A bucket with n>={ARTIFACT_MIN_N} realized exactly 100%; treated as a "
                "judgment artifact, not as calibration evidence."
            ),
        }

    top_share = max(b["n"] for b in buckets) / n_total
    if top_share >= _DEGENERATE_SHARE:
        return {
            "status": "degenerate",
            "top_bucket_share": round(top_share, 4),
            "note": (
                "Confidence is concentrated in one bucket, so it carries no ranking "
                "information in this cell; ECE here reflects bucket width, not calibration."
            ),
        }

    # 버킷은 _BUCKET_ORDER (오름차순) 로 정렬돼 있다. 어떤 높은 버킷이 더 낮은 버킷보다
    # _MONO_TOL 넘게 낮을 때만 표시한다 — 종전 무검정 비교는 30셀 중 27셀에 붙어 상시 빨강이 됐다.
    rated = [b["observed_accuracy"] for b in buckets
             if b["n"] >= _MONO_MIN_N and b["observed_accuracy"] is not None]
    worst_drop = max((rated[i] - rated[j] for i in range(len(rated))
                      for j in range(i + 1, len(rated))), default=0.0)
    if worst_drop > _MONO_TOL:
        return {
            "status": "non_monotone",
            "max_drop": round(worst_drop, 4),
            "note": (
                f"A higher confidence bucket realizes more than {_MONO_TOL:.0%} below a lower "
                f"one (buckets with n>={_MONO_MIN_N}); confidence is not a reliable ranking here."
            ),
        }
    return {"status": "ok"}


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
        # [2026-10-05] 시장 × variant 별 최신 day — 종전 전역 MAX(day) 는 다른 시장 ·
        # score_* variant 가 먼저 적재된 날 늦게 도는 시장(crypto 집계 타임아웃 재시도 중)을
        # 응답에서 **통째로 지웠다** (10-02 · 10-05 crypto 소실 실측).
        day_q = ("SELECT market_id, MAX(day) AS d FROM signal_calibration_daily "
                 "WHERE variant = ?")
        day_params: List[Any] = [variant]
        if market_id:
            day_q += " AND market_id = ?"
            day_params.append(market_id)
        day_q += " GROUP BY market_id"
        latest = {r["market_id"]: r["d"] for r in conn.execute(day_q, day_params).fetchall()
                  if r["d"]}
        if not latest:
            return mcp_error(
                MCPErrorCode.NO_DATA,
                "no calibration snapshot yet (daily thread populates signal_calibration_daily)",
            )
        day = max(latest.values())

        query = (
            "SELECT market_id, \"interval\", bucket, n, hits, ts_min, ts_max "
            "FROM signal_calibration_daily WHERE variant = ? AND ("
            + " OR ".join("(market_id = ? AND day = ?)" for _ in latest)
            + ")"
        )
        params: List[Any] = [variant]
        for mid, d in latest.items():
            params.extend([mid, d])
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
        withheld: List[str] = []
        for itv in sorted({r["interval"] for r in m_rows}):
            i_rows = [r for r in m_rows if r["interval"] == itv]
            blk = _bucket_block(i_rows)
            quality = _cell_quality(mid, itv, blk)
            if quality["status"] == "withheld":
                # [2026-10-05 B2] 버킷 수치를 남기면 소비 AI 가 그대로 인용한다 — 제거.
                blk = {"buckets": [], "n_total": blk["n_total"], "ece": None}
                withheld.append(itv)
            blk["quality"] = quality
            intervals[itv] = blk
        # [2026-10-05 스틸맨 정정] all_intervals 는 원래 의미(전 인터벌)를 유지한다 — 기존 필드
        # 의미를 바꾸는 것은 contract_change 다. 인공물을 뺀 집계는 새 키로 낸다.
        clean_rows = [r for r in m_rows if r["interval"] not in withheld]
        newest = max(latest.values())
        markets[mid] = {
            "snapshot_day": latest.get(mid),
            # 이 시장의 스냅샷이 가장 최신 day 보다 늦다 (일일 집계 실패·재시도 중).
            "stale": latest.get(mid) != newest,
            "intervals": intervals,
            "all_intervals": _bucket_block(m_rows),
            "all_intervals_excluding_withheld": _bucket_block(clean_rows),
            "withheld_intervals": withheld,
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
            # [2026-10-05 스틸맨 정정] 종전 문구(0.3% / 1%)는 롤백 전용 레거시 규칙이었다.
            "realized_hit_definition": (
                "Reuses the signal feedback pipeline judgment "
                "(trade/core/realtime_accuracy.py::_judge_prediction): one band per interval "
                "(at the 30-minute horizon the static per-interval table applies, e.g. 1d = 1.0%). "
                "NEUTRAL predictions are hits iff |change| < band; UP/DOWN predictions are "
                "undecidable when |change| < band, otherwise a hit iff the direction matches. "
                "Only judged rows (is_correct in {0,1}) are counted."
            ),
            "judgment_horizon": (
                "verify_at is signal time + 30 minutes for EVERY interval (15m ... 1d and "
                "combined). A 1d-interval cell therefore measures the 30-minute move after "
                "a 1d signal, not the next day's outcome."
            ),
            "confidence_source": (
                "variant v1 = signals.confidence (the value served by get_signals); "
                "variant v2 = signals.confidence_v2. Joined to the judgment ledger on "
                "(symbol, interval, timestamp). Observation window = "
                "min(CALIB_LOOKBACK_SEC, ledger retention), default 30 days. Snapshots taken "
                "from 2026-09-21 until this fix was deployed used up to ~40 days because of a "
                "hidden 10-day margin; read the actual span in markets[*].observation_window_utc."
            ),
            "cell_quality_rule": (
                "intervals[*].quality.status — withheld: a bucket with n>=30 realized "
                "exactly 100% (judgment artifact suspected; that interval's buckets are "
                "removed, see withheld_intervals); degenerate: one bucket holds >=95% of the "
                "sample (no ranking information); non_monotone: a higher bucket realizes "
                "more than 5 percentage points below a lower one (buckets with n>=100); "
                "ok; no_data."
            ),
            "aggregation_note": (
                "all_intervals keeps its original meaning (every interval, including "
                "withheld ones); all_intervals_excluding_withheld drops withheld intervals "
                "and is the one to cite. Top-level snapshot_day is the most recent day "
                "across markets; markets[*].snapshot_day is each market's own day and "
                "markets[*].stale=true when it lags the most recent one."
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
        blk = m.get("all_intervals_excluding_withheld") or {}
        top = next((b for b in reversed(blk.get("buckets") or []) if b["bucket"] == "[0.9,1.0]"), None)
        ece = blk.get("ece")
        seg = f"{mid}: ECE={ece}"
        if top:
            seg += f", conf 0.9+ realized {top['observed_accuracy']} (n={top['n']})"
        flagged = [
            f"{itv}={(ib.get('quality') or {}).get('status')}"
            for itv, ib in (m.get("intervals") or {}).items()
            if (ib.get("quality") or {}).get("status") in ("withheld", "degenerate", "non_monotone")
        ]
        if flagged:
            seg += f" [cell flags: {', '.join(flagged)}]"
        parts.append(seg)
    return (
        "Confidence reliability table (bucket-level realized hit rates, 30-minute "
        "judgment for every interval). "
        + "; ".join(parts)
        + ". Cells flagged withheld/degenerate/non_monotone are not calibration evidence. "
        "If realized << stated confidence, treat confidence as ranking score, not probability."
    )


def _user_summary_calibration(data: Dict[str, Any]) -> str:
    markets = (data.get("markets") or {}).values()
    markets = list(markets)
    n = sum((m.get("all_intervals_excluding_withheld") or {}).get("n_total", 0) for m in markets)
    n_withheld = sum(len(m.get("withheld_intervals") or []) for m in markets)
    msg = f"시그널 확신도 버킷별 실현 적중률 표를 반환했습니다 (판정 표본 {n:,}건)."
    if n_withheld:
        msg += f" 판정 결함이 의심되는 셀 {n_withheld}개는 수치를 가렸습니다."
    return msg


def register_signal_calibration_tools(mcp, cache):
    """get_signal_calibration 등록 (RCA T5)."""

    @mcp.tool(title='Signal Calibration', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_signal_calibration(
        market_id: Annotated[Optional[str], Field(description='Optional filter (crypto | kr_stock | us_stock)')] = None,
        interval: Annotated[Optional[str], Field(description='Optional candle interval filter (e.g. 15m, 30m, 240m, 1d)')] = None,
        variant: Annotated[str, Field(description='"v1" (raw heuristic confidence, default) or "v2" (outcome-based shadow confidence — RCA C2, accumulating since 2026-07-21)')] = 'v1',
    ) -> SignalCalibrationEnvelope:
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
        Caveats: snapshot is daily (per-market day; markets[*].stale flags a lagging
            market); read the observation window from markets[*].observation_window_utc;
            every interval is judged 30 minutes after the signal (meta.judgment_horizon);
            n is nominal (correlated trials — see meta.sample_caveat); read
            intervals[*].quality.status before citing a cell (withheld / degenerate /
            non_monotone) and cite all_intervals_excluding_withheld, not all_intervals.

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
