# -*- coding: utf-8 -*-
"""
Daily Brief Tool
================
Phase F (2026-05-07): 거대 AI 가 매일 사용자한테 시장 요약 줄 때 자동 호출하는
high-frequency tool.

설계:
    - 단일 호출로 매크로 + 시그널 + 활성 예측 + 어제 거래 결과 결합.
    - 기존 _get_* helper 들을 모아서 사용 (신규 데이터 X).
    - 데이터 경로는 PG ~8 query. 내레이션(_market_state_narrative)은 vLLM 산출물이나
      [2026-07-08] 비차단(stale-while-revalidate) — 응답 경로에서 LLM 을 기다리지 않는다.
      (종전 "LLM 호출 0" 서술은 스테일였음. p95 79.7s 의 주범은 내레이션이 아니라
      strong_signals 의 24h 전행 스캔 쿼리 — 시장당 최대 30s(statement_timeout) ×
      3시장 순차 = 최악 90s. 2026-07-08 실측 분해 후 쿼리 재구성으로 수리)
    - market="all" 이면 3시장 전체. market="crypto"/"kr_stock"/"us_stock" 이면 특정 시장.
    - 응답에 _next_actions + _followup_questions 박혀서 자연스럽게 deep-dive 유도.

호출 예시:
    > "오늘 시장 어때?"
    → ChatGPT 가 get_daily_brief("all") 한 번 호출
    → regime + 강한 시그널 top 3 + 활성 예측 top 5 + 어제 paper trading 결과
    → followup_questions 제시 → 사용자 클릭 → 추가 호출

KPI:
    이 tool 이 가장 자주 호출될 것. 도입 후 daily_brief / total_call 비율 추적.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Annotated, Any, Dict, List, Optional, TYPE_CHECKING

from pydantic import Field

if TYPE_CHECKING:
    from oneqaz_trading_mcp.schemas import DailyBriefEnvelope  # forward-only; runtime import below

# Runtime import: FastMCP introspects the function annotation to build outputSchema,
# so the symbol must resolve at decoration time. The wrapped function still returns
# a plain dict — pydantic models are used for schema only, not for validation.
try:
    from oneqaz_trading_mcp.schemas import DailyBriefEnvelope  # noqa: F401  (used as annotation)
except ImportError:  # pragma: no cover
    DailyBriefEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]

from oneqaz_trading_mcp.resources.resource_response import normalize_choice, wrap_tool_response
from oneqaz_trading_mcp.tools.followup_questions import fq_daily_brief

logger = logging.getLogger("MarketMCP")


# ---------------------------------------------------------------------------
# next_actions for daily_brief
# ---------------------------------------------------------------------------

def _for_daily_brief(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """daily_brief 응답 → 사용자가 관심있을 영역으로 deep-dive 추천."""
    actions: List[Dict[str, Any]] = []
    # 강한 시그널 발견 → 상세
    signals = data.get("strong_signals") or []
    if signals:
        top = signals[0] if isinstance(signals[0], dict) else None
        if top:
            sym = top.get("symbol") or top.get("coin")
            market = top.get("market_id") or top.get("market") or "crypto"
            if sym:
                actions.append({
                    "intent": "deep_dive_top_signal",
                    "tool": "explain_decision",
                    "args": {"market_id": market, "symbol": sym},
                    "rationale": f"점수 1위 예시 신호 {sym} 의 다층 설명 (강한 신호라는 뜻이 아님 — 판정은 explain_decision 의 basis 참조).",
                    "priority": "high",
                })
    # 손실 비율 높으면 분석 추천
    if data.get("losing_count", 0) > data.get("winning_count", 0):
        market = data.get("primary_market", "crypto")
        actions.append({
            "intent": "analyze_yesterday_losses",
            "tool": "analyze_trades",
            "args": {"market_id": market, "days": 1},
            "rationale": "어제 손실 비율이 수익 비율보다 높음. 패턴 분석 권장.",
            "priority": "high",
        })
    # 활성 예측 있으면 자세히
    if data.get("active_predictions_count", 0) > 0:
        actions.append({
            "intent": "see_active_predictions",
            "tool": "get_active_predictions",
            "args": {"limit": 5},
            "rationale": "OneQAZ 가 record 로 남긴 활성 예측 상세.",
        })
    return actions


# ---------------------------------------------------------------------------
# 데이터 수집 (PG 직접 query — 가벼운 cross-schema)
# ---------------------------------------------------------------------------

def _safe_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _market_to_schema(market: str) -> str:
    """market id → market_{m} schema."""
    m = market.lower()
    if m in ("crypto", "coin"):
        return "market_coin"
    if m in ("kr", "kr_stock"):
        return "market_kr"
    if m in ("us", "us_stock"):
        return "market_us"
    return "market_coin"


# [2026-10-05 스틸맨 C3] 이보다 오래된 신호는 '지금' 신호가 아니다 (장이 닫혔거나 파이프라인 정지).
# 근거: 시그널 사이클 coin 15분 · kr/us 5분, coin 기록 완료 p95 ≈ T+16분 (agent-native 진단 0-11).
_LIVE_SIGNAL_MAX_AGE_MIN = 120


def _strong_signals_for_market(
    market: str,
    top_n: int = 3,
    window_out: Optional[Dict[str, Any]] = None,
    breadth_out: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """최신 시그널 사이클의 combined 시그널 중 |signal_score| 상위 N 예시 (방향 정합 행만).

    [2026-10-14] 순서 기준 raw confidence → |signal_score|. raw confidence 는 시장 대부분이 0.70~0.74 에
    몰리고 0.9+ 이상치는 점수가 거의 0이었다(실측 coin O 0.914/score 0.027 · us MTD 0.921/0.041) — 그 기준이면
    약한 신호가 1위로 올라왔다. 보정 매핑도 버킷 간 평평(coin 0.40~0.415)해 신뢰도는 적중을 가르지 못한다.
    |score| 는 엔진 자신의 방향 확신이지만 **실측 24h 적중 우위가 대체로 없다**(score_raw_h24 대부분 구간이 시장 평균 근처, us 역단조 · coin 최상위 희소 구간 0.4~0.5 만 0.67 n=513) —
    그래서 신호마다 그 점수 구간의 실측 적중률(`score_bucket_hit_rate_24h`)을 함께 싣는다. '강한 신호' 가 아니다.
    breadth_out 을 넘기면 심볼당 최신 행의 action 분포(엔진의 현재 자세)를 채운다.

    [2026-10-05 B3] 창을 **최신 2개 사이클**로 정의한다. 종전 "24시간 내" 표기는
    `ORDER BY timestamp DESC LIMIT 3000` (07-08 지연 수리) 때문에 실제 창이 coin 약 1.5h ·
    us 약 25분이었고, 24h 하한 탓에 휴장 중인 kr/us 는 빈 배열이었다.
    직전 사이클을 함께 읽는 이유: 최신 사이클은 기록 중일 수 있다 (coin 기록 완료 p95 T+964s).
    심볼당 최신 행 1개만 쓴다 (현재 상태). window_out 을 넘기면 실제 창을 채운다.
    """
    try:
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
        schema = _market_to_schema(market)
        conn = open_schema_connection(schema, readonly=True)
        try:
            # signals 테이블 (timestamp epoch sec)
            # [2026-07-06] action-score 방향 정합 필터 — confidence 만으로 뽑으면
            # "buy 인데 score 음수" 같은 모순 조합이 외부 노출됨 (라이브 실측:
            # kr 067160 buy/score -0.1156 이 strong_signals top 서빙).
            # [2026-07-08] 지연 근본수리 — 종전 쿼리는 24h 창 전 행(kr 2만+행,
            # shared hit 612MB)을 heap fetch 후 confidence 정렬해 시장당 20~30s
            # (statement_timeout 30s 에 kr 이 잘려 무음 누락, 3시장 순차 최악 90s
            # = daily_brief p95 79.7s 의 주범). timestamp DESC 인덱스로 최신
            # combined 3,000행만 걷은 뒤 앱단 정렬로 재구성. combined 한정은
            # 서빙 의미도 개선 (per-interval 행의 top 점유·동일 심볼 중복 제거).
            import time
            from datetime import datetime, timezone
            # 하한은 인덱스 범위 제한용 — 연휴 휴장(최대 수일)에도 마지막 세션을 찾는다.
            lower = int(time.time()) - 7 * 86400
            ts_rows = conn.execute(
                "SELECT DISTINCT timestamp FROM signals "
                "WHERE interval = 'combined' AND timestamp > ? "
                "ORDER BY timestamp DESC LIMIT 2",
                (lower,),
            ).fetchall()
            cycles = [int(r["timestamp"] if isinstance(r, dict) else r[0]) for r in ts_rows]
            if window_out is not None:
                window_out.update({
                    "definition": "latest 2 signal cycles of interval=combined (latest row per symbol)",
                    "cycles_utc": [
                        datetime.fromtimestamp(c, tz=timezone.utc).isoformat() for c in cycles
                    ],
                })
            if not cycles:
                return []
            # [2026-08-18 위생] 심볼 dedupe — DISTINCT ON (symbol) 으로 심볼당 1행.
            # [2026-10-05 B3] 심볼당 **최신** 행 (현재 상태) — 그 행이 hold 면 제외된다.
            # [2026-10-14] 심볼당 최신 행을 전부 받아(시장당 수백 행 — DISTINCT ON 스캔량은 종전과 같다)
            #   앱단에서 action 분포를 세고, 방향 정합 행을 |score| 내림차순(동률 = confidence)으로 고른다.
            all_rows = conn.execute(
                "SELECT DISTINCT ON (symbol) symbol, interval, action, signal_score, confidence, current_price, timestamp"
                "  FROM signals WHERE interval = 'combined' AND timestamp >= ? AND timestamp <= ?"
                "  ORDER BY symbol, timestamp DESC",
                (min(cycles), max(cycles)),
            ).fetchall()
            all_rows = [dict(r) if not isinstance(r, dict) else r for r in all_rows]
            if breadth_out is not None:
                counts: Dict[str, int] = {}
                for r in all_rows:
                    a = str(r.get("action") or "unknown").lower()
                    counts[a] = counts.get(a, 0) + 1
                breadth_out.update({
                    "symbols": len(all_rows),
                    "by_action": dict(sorted(counts.items(), key=lambda kv: -kv[1])),
                    "basis": "latest combined row per symbol in the latest 2 signal cycles (the engine's current stance)",
                })
            consistent = [
                r for r in all_rows
                if r.get("confidence") is not None and r.get("signal_score") is not None
                and ((r.get("action") == "buy" and float(r["signal_score"]) > 0)
                     or (r.get("action") == "sell" and float(r["signal_score"]) < 0))
            ]
            consistent.sort(key=lambda r: (abs(float(r["signal_score"])), float(r["confidence"])), reverse=True)
            rows = consistent[:top_n]
            from oneqaz_trading_mcp.confidence_calibration import score_bucket_hit_rate
            out = []
            for r in rows:
                _conf = _safe_float(r["confidence"] if isinstance(r, dict) else r[4])
                _itv = r["interval"] if isinstance(r, dict) else r[1]
                # [2026-07-20 RCA C1] 실측 캘리브레이션 병행 (fail-open)
                try:
                    from oneqaz_trading_mcp.confidence_calibration import calibrated_confidence
                    _cal = calibrated_confidence(market, _itv, _conf)
                except Exception:
                    _cal = None
                entry = {
                    "market_id": market,
                    "symbol": r["symbol"] if isinstance(r, dict) else r[0],
                    "interval": _itv,
                    "action": r["action"] if isinstance(r, dict) else r[2],
                    "signal_score": _safe_float(r["signal_score"] if isinstance(r, dict) else r[3]),
                    "confidence": _conf,
                    "confidence_calibrated": _cal,
                    "price": _safe_float(r["current_price"] if isinstance(r, dict) else r[5]),
                    # [2026-08-18 위생] 크로스마켓 배열의 통화 오독 방지 (패널 적발:
                    # FAST 51.265 USD 와 HBAR 92.7 KRW 가 라벨 없이 혼재)
                    "currency": "USD" if market == "us_stock" else "KRW",
                    "timestamp": int(r["timestamp"] if isinstance(r, dict) else r[6]),
                }
                # [2026-10-05 스틸맨 C3] 신호의 나이 — 휴장 시장의 지난 세션 신호를 구분한다.
                entry["signal_age_minutes"] = int((time.time() - entry["timestamp"]) // 60)
                # [2026-10-14] 그 점수 구간의 실측 24h 적중률(매수만 측정 — 매도·측정 없음 = None, 지어내지 않음)
                try:
                    entry["score_bucket_hit_rate_24h"] = score_bucket_hit_rate(market, entry["action"], entry["signal_score"])
                except Exception:
                    logger.exception("score bucket hit rate failed (%s)", market)
                    entry["score_bucket_hit_rate_24h"] = None
                # [2026-08-18 위생] 보정 불가 시 raw confidence 단독 노출 방지 라벨
                if _cal is None:
                    entry["confidence_note"] = "uncalibrated (raw only — 확률로 해석 금지)"
                out.append(entry)
            return out
        finally:
            conn.close()
    except Exception as e:
        logger.warning("daily_brief strong_signals(%s) failed: %s", market, e)
        return []


def _yesterday_trade_summary(market: str) -> Dict[str, Any]:
    """어제 paper trade 결과 — count + winning/losing 분리."""
    try:
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
        schema = _market_to_schema(market)
        conn = open_schema_connection(schema, readonly=True)
        try:
            # virtual_trade_history 사용 (action 별)
            import time
            since = int(time.time()) - 24 * 3600
            # [2026-07-06] exit_timestamp 는 이미 epoch sec(bigint) — 과거의
            # EXTRACT(EPOCH FROM exit_timestamp) 는 PG 에서 매 호출
            # `extract(unknown, bigint) does not exist` 로 throw 했고 except 가
            # 삼켜 전 시장 0 건("24h 거래 없음")을 외부 서빙했다 (실제 24h
            # coin 2,339 + kr 160 건 라이브 실증). 3시장 ts=epoch sec 정책.
            row = conn.execute(
                "SELECT COUNT(*) AS total, "
                "SUM(CASE WHEN profit_loss_pct > 0 THEN 1 ELSE 0 END) AS winning, "
                "SUM(CASE WHEN profit_loss_pct < 0 THEN 1 ELSE 0 END) AS losing, "
                "AVG(profit_loss_pct) AS avg_pnl "
                "FROM virtual_trade_history "
                "WHERE exit_timestamp > ?",
                (since,),
            ).fetchone()
            if row is None:
                return {"total": 0, "winning": 0, "losing": 0, "avg_pnl": 0.0}
            total = row["total"] if isinstance(row, dict) else row[0]
            winning = row["winning"] if isinstance(row, dict) else row[1]
            losing = row["losing"] if isinstance(row, dict) else row[2]
            avg_pnl = row["avg_pnl"] if isinstance(row, dict) else row[3]
            return {
                "total": int(total or 0),
                "winning": int(winning or 0),
                "losing": int(losing or 0),
                "avg_pnl": _safe_float(avg_pnl),
            }
        finally:
            conn.close()
    except Exception as e:
        logger.warning("daily_brief yesterday_trades(%s) failed: %s", market, e)
        return {"total": 0, "winning": 0, "losing": 0, "avg_pnl": 0.0}


def _active_predictions_count() -> int:
    try:
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
        conn = open_schema_connection("market_global", readonly=True)
        try:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM macro_regime_predictions WHERE outcome IS NULL "
                # [2026-10-07] 섀도우(강등 셀 비공개 예측) 제외
                "AND lifecycle_state_at_creation IS DISTINCT FROM 'DEPRECATED'"
            ).fetchone()
            if row is None:
                return 0
            return int(row["c"] if isinstance(row, dict) else row[0])
        finally:
            conn.close()
    except Exception as e:
        logger.warning("daily_brief active_predictions failed: %s", e)
        return 0


_REGIME_MAX_AGE_SEC = 6 * 3600  # 엔진 가드와 같은 값 — trade/core/agent_history_bridge._SUMMARY_MAX_AGE_SEC


def _engine_global_regime(now_ts: Optional[float] = None) -> Dict[str, Any]:
    """엔진 글로벌 레짐(market/global_regime 분석기 산출 global_regime_summary.json 의 overall).

    [2026-10-14] 매매 엔진이 매크로 점수에 **가중 0.35**로 쓰는 바로 그 값이다
    (trade/core/agent_history_bridge._read_global_regime_signed). 신선도 가드(6h)와 부호 변환식을
    엔진과 같게 둔다 — 엔진 쪽을 바꾸면 여기도 같이 바꿀 것. 오래됐거나 읽을 수 없으면 값을 비우고
    사유를 적는다(엔진도 그때는 이 성분을 뺀다). 숫자를 지어내지 않는다.
    """
    import json
    import time
    from datetime import datetime, timezone

    from oneqaz_trading_mcp.config import GLOBAL_REGIME_SUMMARY_JSON

    out: Dict[str, Any] = {"regime": None, "regime_score": None, "regime_signed": None,
                           "as_of": None, "stale": None, "unavailable_reason": None}
    try:
        with open(GLOBAL_REGIME_SUMMARY_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        out["unavailable_reason"] = "global regime summary not found"
        return out
    except (OSError, ValueError) as e:
        logger.warning("daily_brief engine regime unreadable: %s", e)
        out["unavailable_reason"] = "global regime summary unreadable"
        return out
    overall = (data.get("overall") or {}) if isinstance(data, dict) else {}
    out["as_of"] = data.get("updated_at") if isinstance(data, dict) else None
    try:
        updated = datetime.fromisoformat(str(out["as_of"]))
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        age = (now_ts if now_ts is not None else time.time()) - updated.timestamp()
    except (TypeError, ValueError):
        age = None
    if age is None or age > _REGIME_MAX_AGE_SEC:
        out["stale"] = True
        out["unavailable_reason"] = ("global regime summary older than 6h — the engine also drops it"
                                     if age is not None else "global regime summary has no valid updated_at")
        return out
    out["stale"] = False
    regime = str(overall.get("regime") or "").strip().lower() or None
    try:
        score = float(overall.get("score"))
    except (TypeError, ValueError):
        out["unavailable_reason"] = "global regime score missing"
        return out
    magnitude = min(1.0, max(0.0, (score - 0.5) * 2.0))
    out["regime"] = regime
    out["regime_score"] = round(score, 3)
    out["regime_signed"] = round(magnitude if regime == "risk_on" else -magnitude if regime == "risk_off" else 0.0, 3)
    return out


def _macro_regime_snapshot() -> Dict[str, Any]:
    """엔진 글로벌 레짐 + 매크로 카테고리별 최근 7일 캔들 표본 수(데이터 커버리지).

    [2026-10-05 B3 → 스틸맨 C3] 종전 docstring · tool 설명은 "최신 regime" 이었으나 실제로는
    카테고리별 캔들 행 수만 냈다. 한때 external_context.market_regime_flow 의 1d 라벨을
    붙였으나 이질 종목 혼합 집계(bonds 금리·가격, forex DXY·통화쌍)라 의미가 통하지 않았고
    as_of 도 기저 캔들이 아니라 집계 시각이었다 → 철회. 필드 이름은 계약 유지 목적으로 둔다.
    [2026-10-14] 이름과 내용의 불일치(서버 안내·tool 설명은 "regime" 을 약속)를 엔진 글로벌 레짐
    (`regime`·`regime_score`·`regime_signed`·`as_of`·`stale`)을 **추가**해 해소한다. 카테고리 항목은
    여전히 커버리지 — 카테고리 라벨은 C3 판단대로 싣지 않는다(drill-down = market://global/category/{category}).
    """
    engine = _engine_global_regime()
    try:
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
        conn = open_schema_connection("market_global", readonly=True)
        try:
            # [2026-07-03] 동결 analysis(04-16 이후 writer 0) → 라이브 candles + 7일 창.
            rows = conn.execute(
                "SELECT category, COUNT(*) AS c FROM candles "
                "WHERE timestamp >= EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days') "
                "GROUP BY category ORDER BY c DESC LIMIT 8"
            ).fetchall()
            categories = []
            for r in rows:
                cat = r["category"] if isinstance(r, dict) else r[0]
                cnt = r["c"] if isinstance(r, dict) else r[1]
                categories.append({"category": cat, "sample_count": int(cnt)})
        finally:
            conn.close()
    except Exception as e:
        logger.warning("daily_brief macro_regime failed: %s", e)
        categories = []
    return {
        **engine,
        "categories": categories,
        "total": len(categories),
        "basis": (
            "regime = the engine's global macro regime (risk_on / neutral / risk_off) from the global regime "
            "analyzer — the same value that feeds the trading engine's macro score. regime_score is a 0.5-1 "
            "strength, NOT a probability; regime_signed maps it to -1..+1 (risk_off negative) exactly as the "
            "engine does. It averages stage labels across heterogeneous macro categories, so treat it as a "
            "coarse backdrop. Values are null when the summary is older than 6h (stale=true) or unavailable. "
            "categories[*].sample_count = market_global.candles rows per macro category in the last 7 days "
            "(data coverage, not a regime label); per-symbol detail: market://global/category/{category}."
        ),
    }


# ---------------------------------------------------------------------------
# [2026-07-20 RCA T6] market_status — 휴장/장애 구분 불가 해소
# ---------------------------------------------------------------------------
# 외부 감사에서 "us_stock 24h paper trade 0건"이 휴장(정상)인지 파이프라인
# 다운(장애)인지 응답만으로 구분 불가했다 (감사자는 요일 착오로 장애를 의심).
# 세션 캘린더(주말/장중) + 캔들 수신 신선도로 open|closed|pipeline_error 판정.
# 휴일 캘린더는 없으므로 pipeline_error 에는 holiday 가능성 note 를 병기한다.

_STATUS_SCHEMA = {"crypto": "market_coin", "kr_stock": "market_kr", "us_stock": "market_us"}
_STATUS_FRESH_SEC = 5400  # 장중 캔들 무수신 90분 → pipeline_error


def _us_session_utc(now_utc) -> tuple:
    """미국 정규장 UTC 시각 (DST: 3월 둘째 일요일 ~ 11월 첫째 일요일)."""
    import calendar
    y = now_utc.year

    def _nth_sunday(month: int, nth: int) -> int:
        cal = calendar.monthcalendar(y, month)
        sundays = [w[calendar.SUNDAY] for w in cal if w[calendar.SUNDAY]]
        return sundays[nth - 1]

    dst_start = (3, _nth_sunday(3, 2))
    dst_end = (11, _nth_sunday(11, 1))
    md = (now_utc.month, now_utc.day)
    is_dst = dst_start <= md < dst_end
    return (13.5, 20.0) if is_dst else (14.5, 21.0)  # 09:30–16:00 ET


def _last_candle_epoch(market_id: str) -> Optional[int]:
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    import time as _t
    schema = _STATUS_SCHEMA.get(market_id)
    if not schema:
        return None
    try:
        conn = open_schema_connection(schema, readonly=True)
    except Exception:
        return None
    try:
        row = conn.execute(
            "SELECT timestamp FROM candles WHERE timestamp > ? "
            "ORDER BY timestamp DESC LIMIT 1",
            (int(_t.time()) - 7 * 86400,),
        ).fetchone()
        if not row or not row["timestamp"]:
            return None
        # [2026-10-14 H9(A), Mac rev.3] 반환값 = 최신 봉 행을 **처음 받은 시각**(candles.inserted_at, DEFAULT now()).
        #   종전엔 봉 라벨 max(timestamp) 를 그대로 썼는데 kr/us 는 봉 끝 라벨(10-02 규약)이라 진행 중인 30m 봉이
        #   최대 30분 미래 값으로 이겼다(실측 10-14 10:50 KST: kr 라벨 11:00 vs 수신 10:49). inserted_at 은 미래일 수 없다.
        #   진행봉 갱신 때는 바뀌지 않으므로 실제 마지막 수신보다 가장 짧은 봉 1개(kr/us 5m, coin 15m)만큼 늦을 수 있다.
        #   ⚠️ 두 번 조회한다 — 한 문장 서브쿼리면 계획 단계가 전 청크를 열어 계획 1.6s + 실행 1.2s(실측). 하한은 정수 리터럴.
        floor = int(row["timestamp"]) - 3600
        recv = conn.execute(
            f"SELECT EXTRACT(EPOCH FROM MAX(inserted_at))::bigint AS r FROM candles WHERE timestamp >= {floor}"
        ).fetchone()
        return int(recv["r"]) if recv and recv["r"] else None
    except Exception:
        logger.exception("market_status: candle freshness query failed (%s)", market_id)
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _market_status(market_id: str) -> Dict[str, Any]:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    now_ep = now.timestamp()
    last_ep = _last_candle_epoch(market_id)
    last_iso = (
        datetime.fromtimestamp(last_ep, tz=timezone.utc).isoformat() if last_ep else None
    )
    fresh = last_ep is not None and (now_ep - last_ep) <= _STATUS_FRESH_SEC

    if market_id == "crypto":
        status = "open" if fresh else "pipeline_error"
    else:
        hour_utc = now.hour + now.minute / 60.0
        weekday = now.weekday() < 5
        if market_id == "kr_stock":
            in_session = weekday and (0.0 <= hour_utc <= 6.5)      # KST 09:00–15:30
        else:
            lo, hi = _us_session_utc(now)
            in_session = weekday and (lo <= hour_utc <= hi)
        if not in_session:
            status = "closed"
        else:
            status = "open" if fresh else "pipeline_error"

    out: Dict[str, Any] = {
        "status": status,
        "last_data_received_at": last_iso,
        "basis": (
            "session calendar (weekday/hours, no holiday calendar) + candle feed freshness (90min). "
            "last_data_received_at = when the newest candle row was first received (never in the future); "
            "it can lag the actual last update by up to one shortest bar (kr/us 5m, coin 15m)."
        ),
    }
    if status == "pipeline_error":
        out["note"] = (
            "no fresh data during expected session hours — could be a market holiday "
            "(holiday calendar not modeled) or an ingestion outage"
        )
    return out


# ---------------------------------------------------------------------------
# 메인 함수
# ---------------------------------------------------------------------------

def _get_daily_brief(market: str = "all") -> Dict[str, Any]:
    """모든 데이터 결합."""
    # [2026-09-22] market='bogus' 가 target_markets=['bogus'] 로 그대로 흘러
    # 각 하위 조회가 빈 결과를 내고, 전체 응답은 "데이터 없음" 처럼 보였다.
    # 오타와 실제 공백을 구분할 수 없는 상태였다.
    market, _err = normalize_choice("market_or_all", "market", market)
    if _err:
        return _err
    market = market or "all"
    if market == "all":
        target_markets = ["crypto", "kr_stock", "us_stock"]
        primary = "crypto"
    else:
        target_markets = [market]
        primary = market

    # 매크로 regime
    macro = _macro_regime_snapshot()

    # 강한 시그널 (각 시장 최신 2사이클, 합쳐서 top 5)
    # [2026-10-05 스틸맨 C3] 휴장 시장의 지난 세션 신호를 "오늘의 강신호" 에 섞지 않는다 —
    # 대체휴일에 3일 전 kr 신호가 'all' 1위로 "오늘 가장 강한 신호" 라 서빙됐다.
    strong_signals: List[Dict[str, Any]] = []
    last_session_signals: List[Dict[str, Any]] = []
    signal_windows: Dict[str, Any] = {}
    market_breadth: Dict[str, Any] = {}
    signal_edge: Dict[str, Any] = {}
    for m in target_markets:
        win: Dict[str, Any] = {}
        br: Dict[str, Any] = {}
        sig = _strong_signals_for_market(m, top_n=2 if market == "all" else 5, window_out=win, breadth_out=br)
        signal_windows[m] = win
        if br:
            market_breadth[m] = br
        try:
            from oneqaz_trading_mcp.confidence_calibration import score_edge_evidence
            signal_edge[m] = score_edge_evidence(m)
        except Exception:
            logger.exception("daily_brief score edge evidence failed (%s)", m)
            signal_edge[m] = None
        for s in sig:
            if s.get("signal_age_minutes", 0) > _LIVE_SIGNAL_MAX_AGE_MIN:
                last_session_signals.append(s)
            else:
                strong_signals.append(s)
    # [2026-10-14] 시장을 섞어 다시 정렬하지 않는다 — 종전엔 시장별 상위 2개를 raw confidence 로 재정렬해 top 5 로
    #   잘랐는데, raw confidence 는 시장마다 척도가 달라(응답 자체가 "시장 간 비교는 calibrated 로" 라고 적는다)
    #   coin 이상치가 앞을 차지했다. 시장 순서(target_markets)대로, 시장 안은 |score| 순으로 둔다.

    # 어제 거래 결과 (시장별 + 합산)
    trades_by_market: Dict[str, Any] = {}
    total_winning = 0
    total_losing = 0
    total_trades = 0
    for m in target_markets:
        t = _yesterday_trade_summary(m)
        # [2026-07-20 RCA T6] 휴장/장애 구분 필드
        t["market_status"] = _market_status(m)
        trades_by_market[m] = t
        total_winning += t["winning"]
        total_losing += t["losing"]
        total_trades += t["total"]

    # 활성 예측 수
    active_n = _active_predictions_count()

    # 종합 narrative
    if total_trades == 0:
        narrative = f"24시간 동안 {market} 거래 데이터 없음. 활성 예측 {active_n}건 record 중."
    else:
        wr = (total_winning / total_trades * 100) if total_trades > 0 else 0
        # [2026-10-07 TRUST-001] '강한 신호' 표기 정정 — 강도 임계가 없다.
        # [2026-10-14] 순서 기준 |score| + 실측 우위 없음을 문장에 직접 적는다.
        narrative = (
            f"지난 24시간 {market} 시장: {total_trades}건 paper trade "
            f"(수익 {total_winning}/손실 {total_losing}, 승률 {wr:.0f}%). "
            f"점수 상위 예시 신호 {len(strong_signals)}건(시장별 |점수| 순 — 강한 신호라는 뜻이 아님. 신호마다 그 점수 "
            f"구간의 실측 24h 적중률 score_bucket_hit_rate_24h 를 시장 평균과 비교할 것). "
            f"활성 예측 {active_n}건 record 중."
        )
    stance = []
    for m, br in market_breadth.items():
        ba = br.get("by_action") or {}
        stance.append(f"{m} 매수 {ba.get('buy', 0)}·매도 {ba.get('sell', 0)}·관망 {ba.get('hold', 0)}")
    if stance:
        narrative += " 엔진 자세(심볼당 최신 신호): " + " / ".join(stance) + "."
    if last_session_signals:
        narrative += (
            f" 장이 열려 있지 않은 시장의 지난 세션 신호 {len(last_session_signals)}건은 "
            f"last_session_signals 로 분리."
        )
    # [2026-10-14] 엔진 글로벌 레짐 — 값이 있을 때만(오래됐으면 말하지 않는다).
    if macro.get("regime") and macro.get("regime_signed") is not None:
        narrative += f" 엔진 글로벌 레짐 {macro['regime']}(−1~+1 환산 {macro['regime_signed']:+.2f})."

    return {
        "market": market,
        "narrative": narrative,
        "primary_market": primary,
        "macro_regime": macro,
        "strong_signals": strong_signals,
        "last_session_signals": last_session_signals,
        "market_breadth": market_breadth,
        "yesterday_trades": {
            "total": total_trades,
            "winning": total_winning,
            "losing": total_losing,
            "by_market": trades_by_market,
        },
        "winning_count": total_winning,
        "losing_count": total_losing,
        "active_predictions_count": active_n,
        "meta": {
            # [2026-10-05 B3] 종전 "last_24h" 는 강신호 창(실제 최근 수 사이클)과 맞지 않았다.
            # 스키마상 문자열이라 형식은 유지하고, 정확한 창은 windows 에 싣는다.
            "data_window": "strong_signals=latest_2_signal_cycles; yesterday_trades=last_24h",
            # [2026-10-07 TRUST-001] 필드명은 계약이라 유지하되 선별 기준을 명시한다.
            "strong_signals_criteria": (
                "EXAMPLES, not strong signals: per market, the top-N direction-consistent combined signals "
                "(buy with score>0, sell with score<0) in the latest 2 cycles, ranked by |signal_score| "
                "(the engine's own directional conviction); markets are listed in order and never re-ranked "
                "against each other (score and confidence scales differ by market). There is no strength "
                "threshold. In most score buckets the measured 24h hit rate is close to the market average "
                "(meta.signal_edge_evidence); sparse top buckets can differ — check n. A higher score is not by "
                "itself a more reliable signal. Each "
                "signal carries score_bucket_hit_rate_24h {bucket, hit_rate, n, market_base_rate} "
                "(buy signals only; null = not measured). Raw confidence is a heuristic, not a probability. "
                "Do not call these 'strong'."
            ),
            # [2026-10-14] 점수가 적중을 가르는지 직접 보게 하는 근거 표(score_raw_h24 일별 스냅샷, 30일, 매수만)
            "signal_edge_evidence": signal_edge,
            # [2026-10-14] 버킷 단순 적중률은 같은 날 시장 효과가 섞인다 — 날짜 클러스터 검정 결과를 함께 적는다.
            "signal_edge_evidence_note": (
                "Bucket hit rates are raw: signals on the same day share one market move, and high-score "
                "signals cluster on a few days, so bucket gaps partly reflect market days. In a 2026-10-14 test "
                "(same-day mean removed, day-clustered bootstrap, 30 days) the high-score (0.4+) minus low-score "
                "(0.1-0.2) difference was not significant in any market (95% CIs include 0) — neither an edge "
                "nor an inversion. Small top buckets (low n) are noise."
            ),
            "windows": {
                "strong_signals": signal_windows,
                "strong_signals_live_max_age_minutes": _LIVE_SIGNAL_MAX_AGE_MIN,
                "yesterday_trades": "last 24h by exit_timestamp",
                "macro_regime": ("regime = latest engine global regime (stale if older than 6h); "
                                 "categories[*].sample_count = last 7 days (data coverage, not a regime label)"),
            },
            # [2026-10-05 B3] 종전 목록의 macro_regime_forecast · macro_regime_analysis 는
            # 이 tool 이 읽지 않는 화석이었다. 실제로 읽는 원천만 적는다.
            "source": [
                "market_{m}.signals (interval=combined)",
                "market_{m}.virtual_trade_history",
                "market_global.candles",
                "market_global.macro_regime_predictions",
                "market/global_regime/data_storage/global_regime_summary.json (engine global regime)",
            ],
            "interpretation": (
                "Single-call market overview. Combines the engine's macro regime (+ macro data coverage) + strong signals + "
                "yesterday's paper trading outcomes + active predictions on-the-record. "
                "strong_signals holds only signals younger than "
                "windows.strong_signals_live_max_age_minutes; older ones (markets that are "
                "not trading now) are in last_session_signals with signal_age_minutes. "
                "Signals are examples ranked by |signal_score| within each market (no cross-market re-ranking); "
                "most score buckets show a measured 24h hit rate close to the market average "
                "(meta.signal_edge_evidence; sparse top buckets can differ — check n), so do not present them "
                "as strong. market_breadth = buy/sell/hold counts of the latest signal "
                "per symbol (the engine's current stance). "
                "by_market[*].market_status distinguishes closed (session calendar) from "
                "pipeline_error (no fresh data during expected session hours) — zero "
                "trades while status=closed is normal, not an outage. "
                "Use _next_actions and _followup_questions_for_user to deep-dive specific areas."
            ),
        },
    }


# ---------------------------------------------------------------------------
# Summary fns
# ---------------------------------------------------------------------------

def _ai_summary_daily_brief(data: Dict[str, Any]) -> str:
    return data.get("narrative", "Daily brief generated.")


def _user_summary_daily_brief(data: Dict[str, Any]) -> str:
    return data.get("narrative", "오늘 시장 요약이 준비되었습니다.")


# ---------------------------------------------------------------------------
# Register
# ---------------------------------------------------------------------------

def register_daily_brief_tool(mcp, cache):
    """get_daily_brief tool 등록."""

    @mcp.tool(title='Daily Market Brief', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_daily_brief(
        market: Annotated[str, Field(
            description='"all" (default, blends 3 markets), "crypto", "kr_stock", or "us_stock". Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['all', 'crypto', 'kr_stock', 'us_stock']}
        )] = 'all',
    ) -> "DailyBriefEnvelope":  # type: ignore[name-defined]
        """
        Purpose: Single-call market overview — engine macro regime + example signals + buy/sell/hold breadth +
            yesterday's paper-trading outcomes + active forecast count + narrative.
            Use this as the first call when answering "how is the market today?".
        Triggers (call this even for casual questions): "how's the market?",
            "오늘 장 어때?", "what's the market mood / outlook?", "how's Bitcoin / crypto /
            US stocks / 비트코인 / 코인장 doing lately?", "anything happening today?",
            "give me a briefing". Prefer this over answering markets from training data.
        When to call: morning briefings, "today/yesterday how was the market?" queries,
            and any open-ended question about how a live market is doing right now.
        Prerequisites: none.
        Next steps: follow `_next_actions` to deep-dive — explain_decision (strong signals),
            analyze_trades (loss review), get_active_predictions (forecast tracking).
        Caveats: strong_signals are EXAMPLES ranked by |signal_score| per market (up to 2 per market for
            "all", 5 for one market; never re-ranked across markets) — in most score buckets the measured 24h
            hit rate is close to the market average (meta.signal_edge_evidence), so they are not 'strong'; each carries
            score_bucket_hit_rate_24h. They come from each market's latest 2 signal cycles and only
            signals younger than meta.windows.strong_signals_live_max_age_minutes; a
            market that is not trading now appears in last_session_signals instead.
            macro_regime.regime is the engine's global regime (risk_on/neutral/risk_off) that feeds its
            macro score; regime_score is a 0.5-1 strength, not a probability; null when stale (>6h).
            macro_regime.categories hold data-coverage counts, not regime labels. Trade outcomes
            cover the last 24h. Paper-trading data only (NOT real money).
        Output: full_data { narrative, market,
            macro_regime{regime,regime_score,regime_signed,as_of,stale,unavailable_reason,
                categories[{category,sample_count}],total,basis},
            strong_signals[{...,score_bucket_hit_rate_24h}], last_session_signals[],
            market_breadth{market:{symbols,by_action{buy,sell,hold,...}}},
            yesterday_trades{total,winning,losing,by_market},
            active_predictions_count, primary_market, meta }.

        """
        cache_key = f"daily_brief_{market}"
        cached = cache.get(cache_key, ttl=300)  # 5분 캐시
        if cached:
            return cached

        # [2026-07-08] wrap(내레이션 포함)까지 스레드로 — 종전엔 wrap 이 이벤트루프
        # 위에서 돌아 vLLM 동기 대기 시 MCP 서버 전체가 최대 35s 정지했다.
        # (내레이션 자체도 stale-while-revalidate 비차단으로 전환됨 — 이중 방어)
        def _build():
            result = _get_daily_brief(market)
            return wrap_tool_response(
                result, "daily_brief",
                _ai_summary_daily_brief, _user_summary_daily_brief,
                next_actions_fn=_for_daily_brief,
                followup_questions_fn=fq_daily_brief,
                narrative_context="daily_brief",
            )

        wrapped = await asyncio.to_thread(_build)
        cache.set(cache_key, wrapped)
        return wrapped

    logger.info("  [OK] Daily Brief Tool registered")
