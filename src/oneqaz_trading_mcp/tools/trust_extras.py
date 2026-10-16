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
# 실패 결과 캐시는 짧게 — 아래 _cached 주석 참조 (2026-09-21).
_TTL_FAIL_SEC = int(os.getenv("TRUST_EXTRAS_TTL_FAIL_SEC", "60"))


def _cached(key: str, builder) -> Dict[str, Any]:
    now = time.time()
    ent = _CACHE.get(key)
    # [2026-09-21] 성공과 실패의 TTL 을 분리. 종전엔 둘 다 900초라 **1회 실패가
    # 15분 배지 소등**으로 증폭됐다 (타임아웃 1건 -> 그 뒤 15분간 모든 호출자가
    # `available: false` 를 받는다). 실패는 재시도가 싸므로 짧게 잡는다.
    if ent and now - ent["ts"] < ent.get("ttl", _TTL_SEC):
        return ent["data"]
    try:
        data = builder()
        _ttl = _TTL_SEC
    except Exception as exc:  # fail-open — 본 응답을 침해하지 않는다
        logger.warning("trust_extras %s build failed: %s", key, exc)
        data = {"available": False, "error": str(exc)[:120]}
        _ttl = _TTL_FAIL_SEC
    _CACHE[key] = {"ts": now, "data": data, "ttl": _ttl}
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
        {
            "id": "macro_persistence_null_relabel",
            "found": "2026-10-07",
            "status": "corrected (accuracy_ext live-ledger null; effective after engine batch restart — see get_prediction_accuracy meta.persistence_method)",
            "what": (
                "On 2026-10-01 the full candle history was relabeled (legacy -> PKL regime "
                "labels). The persistence null was reconstructed from the relabeled history while "
                "outcomes keep the labels recorded at the time, so the two matched only 1-7% of "
                "the time; persistence collapsed to 0.12-0.35 and 8 cells were shown as 'edge'. "
                "With a consistent null (the validator's own live record at creation) only 1 "
                "cell clears persistence and 19 forecast cells are significantly worse than it. "
                "The same relabel silently re-activated 3 coin cells that had been deprecated."
            ),
            "mitigation": (
                "persistence null now comes from the live-recorded outcome stream (label-version "
                "consistent); deprecation is judged on the post-relabel cohort only."
            ),
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
            # [2026-09-21] 두 MAX 를 **별도 문으로 분리**. (보존 30일 -> 365일 감사 후속)
            #   종전 단일 문은 두 번째 MAX 에 집계 `FILTER` 가 붙어 있어 PG 의
            #   MAX -> 인덱스 역방향 스캔(InitPlan+Limit 1) 최적화가 통째로 무력화됐다.
            #   분리 실험으로 인과 확정: 첫 MAX 단독 = `Index Scan Backward` cost 0.54,
            #   FILTER 를 더하는 순간 `Parallel Seq Scan` **cost 242,866**.
            #   실측(market_coin): 941ms(최대 3,110ms) / 호출당 블록 175,466 = 원장 전수.
            #   시간 하한이 없어 **테이블 크기에 정비례**하므로 12배면 11~17초
            #   (콜드 37~67초)로 풀 statement_timeout 30s 에 닿는다. 그리고 실패하면
            #   `_cached` 가 `{"available": false}` 를 **900초** 캐시해 1회 실패가
            #   **15분 배지 소등**으로 증폭된다. 외부 노출 경로다(30일 8,199콜 / 926 IP).
            #
            #   분리 후 각 문은 InitPlan + Limit 1 이라 스캔 깊이가 "마지막 판정까지의
            #   거리" 에만 의존하고 **테이블 크기와 완전 무관**해진다.
            #   실측 cost 242,865.95 -> **0.57**, 2,091ms -> 203ms, 출력값 동일.
            #   ⚠️ ROUND/EXTRACT 산술은 **SQL 에 그대로 둔다** — Python `round()` 로 옮기면
            #      PG(사사오입)와 Python(은행가 반올림)의 경계값 처리가 달라진다.
            #   ⚠️ h24 쪽에 `is_correct IN (0,1)` 를 반드시 함께 남겨야 값이 같다.
            #   두 문이 한 스냅샷이 아니지만 "몇 시간 전" 배지라 무의미한 차이다.
            row30 = conn.execute(
                "SELECT ROUND((EXTRACT(EPOCH FROM NOW()) - MAX(timestamp)) / 3600.0) AS h30 "
                "FROM signal_predictions WHERE verified = 1 AND is_correct IN (0,1)"
            ).fetchone()
            row24 = conn.execute(
                "SELECT ROUND((EXTRACT(EPOCH FROM NOW()) - MAX(timestamp)) / 3600.0) AS h24 "
                "FROM signal_predictions WHERE verified = 1 AND is_correct IN (0,1) "
                "  AND is_correct_24h IN (0,1)"
            ).fetchone()
            entry["hours_since_last_30m_judgment"] = (
                float(row30["h30"]) if row30 is not None and row30["h30"] is not None else None)
            entry["hours_since_last_24h_judgment"] = (
                float(row24["h24"]) if row24 is not None and row24["h24"] is not None else None)
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
