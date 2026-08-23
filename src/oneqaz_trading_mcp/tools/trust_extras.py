# -*- coding: utf-8 -*-
"""get_prediction_accuracy 허브 심화 블록 3종 (2026-08-18, schema 1.2 additive).

배경: 30d 실사용 실측 — 외부 세션의 90%(1,477/1,639)가 get_prediction_accuracy 를
호출하고 그 다음 단계로 넘어가는 건 8.5%뿐. 신뢰 평가의 1번 관문에 답을 깊게 하는
것이 (새 도구 신설보다) 최고 레버리지라는 판정 (hub-and-spoke 전략).

블록 3종 — 전부 additive, fail-open(실패 시 unavailable 블록, 본 응답 불침해):
  signal_gauge_v2  시그널 레벨 정직 잣대 (수수료 차감 24h net — 거시예측 축의 반쪽 보완)
  known_biases     자기 결함 공개 (측정된 편향 좌표 + 차단/수리 이력 — 신뢰 차별화)
  ledger_health    채점 파이프 생존 뱃지 (침묵 고장이 상품 가치를 죽이는 것의 방어)

TTL 캐시로 DB 부하/지연 보호 (도구 호출 월 ~8k). 쿼리는 전부 readonly.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Optional

logger = logging.getLogger("MarketMCP")

_MARKETS = {
    "coin": "market_coin",
    "kr": "market_kr",
    "us": "market_us",
}

# 왕복 수수료 가정(%) — admin theory_v2 잣대와 동일 상수/env (learning_health.py 정합)
_FEE_RT_PCT = {
    "coin": float(os.getenv("THEORY_FEE_RT_COIN_PCT", "0.50")),
    "kr": float(os.getenv("THEORY_FEE_RT_KR_PCT", "0.20")),
    "us": float(os.getenv("THEORY_FEE_RT_US_PCT", "0.20")),
}

_CACHE: Dict[str, Dict[str, Any]] = {}
_TTL_SEC = int(os.getenv("TRUST_EXTRAS_TTL_SEC", "900"))


def _cached(key: str, builder) -> Dict[str, Any]:
    now = time.time()
    ent = _CACHE.get(key)
    if ent and now - ent["ts"] < _TTL_SEC:
        return ent["data"]
    try:
        data = builder()
    except Exception as exc:  # fail-open — 본 응답을 침해하지 않는다
        logger.warning("trust_extras %s build failed: %s", key, exc)
        data = {"available": False, "error": str(exc)[:120]}
    _CACHE[key] = {"ts": now, "data": data}
    return data


def _open(schema: str):
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    return open_schema_connection(schema, readonly=True)


# ---------------------------------------------------------------------------
# ① signal_gauge_v2 — 시그널 레벨 정직 잣대 (admin /learning/theory-gap v2 미러)
# ---------------------------------------------------------------------------
def _build_signal_gauge_v2() -> Dict[str, Any]:
    out: Dict[str, Any] = {"available": True, "window_days": 7, "markets": {}}
    for mkt, schema in _MARKETS.items():
        fee = _FEE_RT_PCT[mkt]
        conn = _open(schema)
        try:
            row = conn.execute(
                "SELECT COUNT(*) FILTER (WHERE is_correct IN (0,1)) AS judged, "
                "       COUNT(*) FILTER (WHERE is_correct = 1) AS hits, "
                "       AVG(actual_change_pct) FILTER (WHERE is_correct IN (0,1)) AS avg_chg, "
                "       COUNT(*) FILTER (WHERE is_correct_24h IN (0,1)) AS judged_24h, "
                "       AVG(actual_change_pct_24h) FILTER (WHERE is_correct_24h IN (0,1)) AS avg_chg_24h "
                "FROM signal_predictions "
                "WHERE verified = 1 AND predicted_direction = 'UP' AND \"interval\" = 'combined' "
                "  AND timestamp > EXTRACT(EPOCH FROM NOW())::bigint - 7*86400"
            ).fetchone()
        finally:
            try:
                conn.close()
            except Exception:
                pass
        judged = int(row["judged"] or 0)
        hits = int(row["hits"] or 0)
        avg_chg = float(row["avg_chg"]) if row["avg_chg"] is not None else None
        avg24 = float(row["avg_chg_24h"]) if row["avg_chg_24h"] is not None else None
        out["markets"][mkt] = {
            "buy_judged": judged,
            "buy_hit_rate": round(hits / judged, 4) if judged else None,
            "avg_change_pct": round(avg_chg, 4) if avg_chg is not None else None,
            "judged_24h": int(row["judged_24h"] or 0),
            "avg_change_pct_24h": round(avg24, 4) if avg24 is not None else None,
            "net_expected_24h_pct": round(avg24 - fee, 4) if avg24 is not None else None,
            "fee_rt_pct_assumed": fee,
        }
    out["interpretation"] = (
        "Signal-level honest gauge (single 'combined' judgment stream — no duplicate "
        "counting; the pre-2026-08-18 signal gauge mixed 5 streams and overcounted). "
        "net_expected_24h_pct = mean realized 24h change of BUY signals minus an assumed "
        "round-trip fee. Negative net = no exploitable edge at that horizon after costs, "
        "regardless of hit rate. This is the signal-side complement to the macro-forecast "
        "cells above; both are measured on live, timestamped ledgers."
    )
    return out


# ---------------------------------------------------------------------------
# ② known_biases — 자기 결함 공개 (측정 좌표 + 수리 이력. 수동 큐레이션, 날짜 명시)
# ---------------------------------------------------------------------------
_KNOWN_BIASES = {
    "available": True,
    "disclosure_policy": (
        "Self-measured defects with coordinates, discovered by our own adversarial "
        "audits. Published deliberately: a data source that hides its biases cannot be "
        "trusted on its successes. Each entry carries the measurement date and, where "
        "applicable, the mitigation and its policy tag (cohort-splittable in trade data)."
    ),
    "entries": [
        {
            "id": "x2_extreme_tail_collapse",
            "found": "2026-08-18",
            "status": "mitigated (POLICY 2026-08-18-r1)",
            "what": (
                "Signal scores in the extreme tail (x2 >= 0.6) have INVERTED quality: "
                "24h hit rate collapses to ~23% (coin) / ~35% (us) vs 47-66% in the "
                "healthy mid-range. Momentum blow-off tops were being ranked as the "
                "best candidates."
            ),
            "mitigation": (
                "Buy-side derate gate since 2026-08-18 (tail entries blocked unless "
                "pattern evidence n>=30 & win>=0.55). Signal-level measurement continues "
                "unfiltered on the calibration scoreboard, so the defect and its "
                "mitigation remain independently verifiable."
            ),
        },
        {
            "id": "legacy_signal_gauge_overcounting",
            "found": "2026-08-18",
            "status": "corrected (signal_gauge_v2)",
            "what": (
                "Signal accuracy figures published before 2026-08-18 mixed 5 judgment "
                "streams (combined + per-interval), counting one signal up to 5x, and a "
                "2026-07-06 headline figure (60.6% coin) was measured on ~4% judgment "
                "coverage. Treat pre-cutover signal-gauge numbers as a different cohort."
            ),
            "mitigation": "signal_gauge_v2 uses the single combined stream with magnitude and fee-net fields.",
        },
        {
            "id": "coin_edge_regime_dependent",
            "found": "2026-08-18",
            "status": "disclosed (no mitigation claimed)",
            "what": (
                "As of mid-Aug 2026, coin BUY signals show ~zero gross edge at the "
                "judgment horizon and negative 24h drift net of fees; kr/us show "
                "positive 24h net (+0.3-0.4% gross ~0.28-0.37% net at assumed fees). "
                "Edges are regime-dependent and can disappear; we publish this rather "
                "than argue with it."
            ),
            "mitigation": None,
        },
        {
            "id": "paper_fills_are_gross",
            "found": "2026-08-18 (documented)",
            "status": "disclosed",
            "what": (
                "Paper-trading P&L in trade-history tools is gross: candle-based fills, "
                "no fees or slippage. Live execution would be worse by roughly the "
                "round-trip fee plus slippage. Use net_expected fields for cost-aware "
                "reads."
            ),
            "mitigation": None,
        },
        {
            "id": "macro_persistence_null",
            "found": "2026-07-20",
            "status": "corrected (schema 1.1 skill fields)",
            "what": (
                "Raw macro-forecast accuracy mostly measures regime persistence, not "
                "alpha (97-99% cells sat within 0.05pp of the persistence null). "
                "Judge cells by skill_ci_95, already served in this response."
            ),
            "mitigation": "edge/anti cells are persistence-skill judged since 2026-07-20.",
        },
    ],
}


# ---------------------------------------------------------------------------
# ③ ledger_health — 채점 파이프 생존 뱃지
# ---------------------------------------------------------------------------
def _build_ledger_health() -> Dict[str, Any]:
    out: Dict[str, Any] = {"available": True, "markets": {}, "overall": "ok"}
    degraded = []
    for mkt, schema in _MARKETS.items():
        entry: Dict[str, Any] = {}
        conn = _open(schema)
        try:
            row = conn.execute(
                "SELECT ROUND((EXTRACT(EPOCH FROM NOW()) - MAX(timestamp)) / 3600.0) AS h30, "
                "       ROUND((EXTRACT(EPOCH FROM NOW()) - MAX(timestamp) "
                "              FILTER (WHERE is_correct_24h IN (0,1))) / 3600.0) AS h24 "
                "FROM signal_predictions WHERE verified = 1 AND is_correct IN (0,1)"
            ).fetchone()
            entry["hours_since_last_30m_judgment"] = float(row["h30"]) if row["h30"] is not None else None
            entry["hours_since_last_24h_judgment"] = float(row["h24"]) if row["h24"] is not None else None
            # 판정 기준 = "자격 있는 미판정 백로그" (휴장/연휴 캘린더 갭에 오탐 없음 —
            # hours-since 기준은 광복절 3연휴에서 kr 을 오탐했다, 2026-08-18 스모크 실측).
            # eligible = 시그널이 24h+2h 를 지나 판정 자격이 생겼는데 NULL 인 행.
            bk = conn.execute(
                "SELECT COUNT(*) AS n, "
                "       ROUND((EXTRACT(EPOCH FROM NOW()) - MIN(timestamp)) / 3600.0) AS oldest_h "
                "FROM signal_predictions "
                "WHERE is_correct_24h IS NULL "
                "  AND timestamp <= EXTRACT(EPOCH FROM NOW())::bigint - 26*3600 "
                "  AND timestamp >= EXTRACT(EPOCH FROM NOW())::bigint - 21*86400"
            ).fetchone()
            entry["judgment_backlog_eligible"] = int(bk["n"] or 0)
            entry["backlog_oldest_hours"] = float(bk["oldest_h"]) if bk["oldest_h"] is not None else None
            dial = conn.execute(
                "SELECT (CASE WHEN SUM(n_learned_adj)>0 THEN 1 ELSE 0 END"
                " + CASE WHEN SUM(n_adaptive)>0 THEN 1 ELSE 0 END"
                " + CASE WHEN SUM(n_verified_t)>0 THEN 1 ELSE 0 END) AS alive "
                "FROM dial_activation_daily WHERE day > CURRENT_DATE - 7"
            ).fetchone()
            entry["core_dials_alive_7d"] = int(dial["alive"] or 0)
        finally:
            try:
                conn.close()
            except Exception:
                pass
        # degraded = 자격 발생 후 72h 넘게 방치된 미판정 행 존재 (2026-08 실사고인
        # 'us 4일 침묵 정지'는 이 기준으로 즉시 검출, 연휴 무판정은 백로그 0이라 무오탐)
        _oldest = entry.get("backlog_oldest_hours")
        entry["status"] = ("degraded"
                           if (_oldest is not None and _oldest - 26 > 72
                               and entry["judgment_backlog_eligible"] > 0)
                           else "ok")
        if entry["status"] != "ok":
            degraded.append(mkt)
        out["markets"][mkt] = entry
    if degraded:
        out["overall"] = "degraded"
        out["degraded_markets"] = degraded
    out["interpretation"] = (
        "Liveness of the grading pipes behind every number this server publishes. "
        "A trust ledger whose judgment pipeline silently dies serves stale truth. "
        "Status rule (machine-checkable from the served fields): degraded when "
        "judgment_backlog_eligible > 0 AND the oldest eligible-but-unjudged signal "
        "has waited more than 72h past its eligibility (backlog_oldest_hours - 26 > 72). "
        "Eligibility = signal older than 26h (24h horizon + 2h candle buffer). "
        "Backlog-based on purpose: hours-since-last-judgment false-alarms over "
        "market holidays (a 3-day Korean holiday weekend produced a 103h gap with "
        "zero actual backlog). hours_since_* fields are informational only. "
        "Dial liveness (core_dials_alive_7d of 3) exposes silently-dead scoring "
        "components. Added 2026-08-18 after our own audit caught a 4-day silent "
        "judgment stall in the us pipeline."
    )
    return out


# ---------------------------------------------------------------------------
# 공개 API
# ---------------------------------------------------------------------------
def get_trust_extras() -> Dict[str, Any]:
    """세 블록을 additive dict 로 반환 — _get_prediction_accuracy 가 병합."""
    return {
        "signal_gauge_v2": _cached("signal_gauge_v2", _build_signal_gauge_v2),
        "known_biases": _KNOWN_BIASES,
        "ledger_health": _cached("ledger_health", _build_ledger_health),
    }
