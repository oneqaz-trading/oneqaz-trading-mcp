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
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from oneqaz_trading_mcp.schemas import DailyBriefEnvelope  # forward-only; runtime import below

# Runtime import: FastMCP introspects the function annotation to build outputSchema,
# so the symbol must resolve at decoration time. The wrapped function still returns
# a plain dict — pydantic models are used for schema only, not for validation.
try:
    from oneqaz_trading_mcp.schemas import DailyBriefEnvelope  # noqa: F401  (used as annotation)
except ImportError:  # pragma: no cover
    DailyBriefEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]

from oneqaz_trading_mcp.resources.resource_response import wrap_tool_response
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
                    "rationale": f"오늘 가장 강한 신호 {sym} 의 다층 설명.",
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


def _strong_signals_for_market(market: str, top_n: int = 3) -> List[Dict[str, Any]]:
    """24시간 내 confidence 가장 높은 combined 시그널 top N."""
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
            since = int(time.time()) - 24 * 3600
            # [2026-08-18 위생] 심볼 dedupe — 같은 심볼의 근접 사이클 중복이 top5
            # 슬롯을 점유하던 결함 수리 (신뢰 평가 패널 실측: 5건 중 2쌍 중복).
            # DISTINCT ON (symbol) 으로 심볼당 최고 confidence 1행만.
            rows = conn.execute(
                f"SELECT symbol, interval, action, signal_score, confidence, current_price, timestamp FROM ("
                f"  SELECT DISTINCT ON (symbol) symbol, interval, action, signal_score, confidence, current_price, timestamp FROM ("
                f"    SELECT symbol, interval, action, signal_score, confidence, current_price, timestamp"
                f"    FROM signals WHERE timestamp > ? AND interval = 'combined'"
                f"    ORDER BY timestamp DESC LIMIT 3000"
                f"  ) w WHERE confidence IS NOT NULL "
                f"  AND ((action = 'buy' AND signal_score > 0) "
                f"       OR (action = 'sell' AND signal_score < 0)) "
                f"  ORDER BY symbol, confidence DESC, timestamp DESC"
                f") t ORDER BY confidence DESC LIMIT ?",
                (since, top_n),
            ).fetchall()
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
                "SELECT COUNT(*) AS c FROM macro_regime_predictions WHERE outcome IS NULL"
            ).fetchone()
            if row is None:
                return 0
            return int(row["c"] if isinstance(row, dict) else row[0])
        finally:
            conn.close()
    except Exception as e:
        logger.warning("daily_brief active_predictions failed: %s", e)
        return 0


def _macro_regime_snapshot() -> Dict[str, Any]:
    """8개 매크로 카테고리의 최신 regime."""
    try:
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
        conn = open_schema_connection("market_global", readonly=True)
        try:
            # [2026-07-03] 동결 analysis(04-16 이후 writer 0) → 라이브 candles
            # + 7일 창 — 외부 AI 클라이언트에 '최신 레짐 샘플수'로 제시되는 값이므로
            # 실제 최근 데이터만 집계한다.
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
            return {"categories": categories, "total": len(categories)}
        finally:
            conn.close()
    except Exception as e:
        logger.warning("daily_brief macro_regime failed: %s", e)
        return {"categories": [], "total": 0}


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
        return int(row["timestamp"]) if row and row["timestamp"] else None
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
        "basis": "session calendar (weekday/hours, no holiday calendar) + candle feed freshness (90min)",
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
    if market == "all":
        target_markets = ["crypto", "kr_stock", "us_stock"]
        primary = "crypto"
    else:
        target_markets = [market]
        primary = market

    # 매크로 regime
    macro = _macro_regime_snapshot()

    # 강한 시그널 (각 시장 top 1, 합치면 최대 3)
    strong_signals: List[Dict[str, Any]] = []
    for m in target_markets:
        sig = _strong_signals_for_market(m, top_n=2 if market == "all" else 5)
        strong_signals.extend(sig)
    # confidence 내림차순 정렬 → top 5 (overall)
    strong_signals.sort(key=lambda x: x.get("confidence", 0), reverse=True)
    strong_signals = strong_signals[:5]

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
        narrative = (
            f"지난 24시간 {market} 시장: {total_trades}건 paper trade "
            f"(수익 {total_winning}/손실 {total_losing}, 승률 {wr:.0f}%). "
            f"강한 신호 {len(strong_signals)}건. 활성 예측 {active_n}건 record 중."
        )

    return {
        "market": market,
        "narrative": narrative,
        "primary_market": primary,
        "macro_regime": macro,
        "strong_signals": strong_signals,
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
            "data_window": "last_24h",
            "source": [
                "live_signals", "paper_trade_history",
                "macro_regime_forecast", "macro_regime_analysis",
            ],
            "interpretation": (
                "Single-call market overview. Combines macro regime + strong signals + "
                "yesterday's paper trading outcomes + active predictions on-the-record. "
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

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_daily_brief(market: str = "all") -> "DailyBriefEnvelope":  # type: ignore[name-defined]
        """
        Purpose: Single-call market overview — macro regime + top 5 strong signals +
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
        Caveats: 24-hour window. Paper-trading data only (NOT real money).
        Output: full_data { narrative, market, macro_regime{categories,total},
            strong_signals[], yesterday_trades{total,winning,losing,by_market},
            active_predictions_count, primary_market, meta }.

        Args:
            market: "all" (default, blends 3 markets), "crypto", "kr_stock", or "us_stock"

        Disclaimer: Information only, not investment advice.
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
