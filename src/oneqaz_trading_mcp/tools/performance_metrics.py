# -*- coding: utf-8 -*-
"""
포트폴리오 성과 지표 계산 Tool (계산 경로 A안 — 본체 MCP 단일 계산·서빙)
=======================================================================
[2026-07-23] 블로그 신뢰 업그레이드 R3 (HANDOFF_2026_07_22_trust_upgrade_backend).

블로그(Mac)·외부 클라이언트가 트랙레코드 지표(MDD/샤프/소르티노/Calmar/월별수익률)를
이 도구 하나로만 조회한다 → 계산 경로 단일화가 구조적으로 충족됨.

데이터 소스: admin.v1_daily_equity (합성 고정 북 S=400 모델의 단일 정의처).
이 도구는 daily_return_pct 를 소비만 하고 자본 모델을 재정의하지 않는다.

원칙:
- account_type 은 필수 인자 — paper/live 혼합 집계를 API 레벨에서 차단 (GIPS).
- live 는 실전 기록 축적 전까지 명시적 에러 (paper 커브와 절대 잇지 않음).
- 공개 앵커 = 2026-06-16 (블로그 공개 발행 시작일). window_start 기본값.
"""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.config import CACHE_TTL_TRADE_HISTORY
from oneqaz_trading_mcp.resources.resource_response import mcp_error, MCPErrorCode, wrap_tool_response

logger = logging.getLogger("MarketMCP")

# 공개 트랙레코드 앵커 (v1_daily_equity 의 equity_index_public 앵커와 동일)
PUBLIC_ANCHOR = date(2026, 6, 16)
# 합성 자본 모델 상수 — 정의 자체는 admin.v1_daily_equity 뷰가 단일 소스.
# 여기서는 응답 메타데이터(방법론 공시)용으로만 사용.
BOOK_SLOTS = 400

_MARKET_ALIAS = {
    "coin": "coin", "crypto": "coin",
    "kr": "kr", "kr_stock": "kr",
    "us": "us", "us_stock": "us",
    "all": "all",
}
# 연환산 계수: coin 은 24/7(365 거래일), kr/us 는 252 거래일 관행.
# 'all' 은 3시장 고정 1/3 배분 합성이라 coin 이 매일 움직임 → 365 사용.
_ANNUALIZE = {"coin": 365, "kr": 252, "us": 252, "all": 365}

_KST = timezone(timedelta(hours=9))


def _today_kst() -> date:
    return datetime.now(_KST).date()


def _parse_date(value: Optional[str], default: date, field: str) -> date:
    if value is None or value == "":
        return default
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"{field} 는 YYYY-MM-DD 형식이어야 합니다: {value!r}") from exc


def _fetch_daily_rows(markets: List[str], start: date, end: date) -> List[Dict[str, Any]]:
    """admin.v1_daily_equity 에서 일별 행 조회 (paper 전용 뷰)."""
    from oneqaz_trading_mcp.shared.db.pg_pool import get_pool

    pool = get_pool("admin")
    sql = """
        SELECT market, trade_date, closed_count, win_count, daily_return_pct
        FROM admin.v1_daily_equity
        WHERE account_type = 'paper'
          AND market = ANY(%s)
          AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date, market
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (markets, start, end))
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def _combine_all_markets(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """market='all': 3시장 고정 1/3 배분 합성.

    그날 청산이 없는 시장의 일수익률은 0 (북은 항상 존재, 거래가 없었을 뿐).
    """
    by_date: Dict[date, Dict[str, Any]] = {}
    for r in rows:
        d = r["trade_date"]
        agg = by_date.setdefault(d, {"trade_date": d, "closed_count": 0, "win_count": 0, "ret_sum": 0.0})
        agg["closed_count"] += int(r["closed_count"])
        agg["win_count"] += int(r["win_count"])
        agg["ret_sum"] += float(r["daily_return_pct"])
    out = []
    for d in sorted(by_date):
        agg = by_date[d]
        out.append({
            "market": "all",
            "trade_date": d,
            "closed_count": agg["closed_count"],
            "win_count": agg["win_count"],
            "daily_return_pct": agg["ret_sum"] / 3.0,
        })
    return out


def _compute_metrics(returns_pct: List[float], annualize: int) -> Dict[str, Any]:
    """일수익률(%) 시계열 → 지표. 표본 부족 시 해당 지표 None (N/A — 억지 산출 금지)."""
    n = len(returns_pct)
    fracs = [r / 100.0 for r in returns_pct]

    # 에쿼티 (윈도 시작 100 기준 일복리)
    equity = [100.0]
    for f in fracs:
        equity.append(equity[-1] * (1.0 + f))
    equity = equity[1:]  # 거래일과 1:1

    cum_return_pct = (equity[-1] / 100.0 - 1.0) * 100.0 if equity else 0.0

    # MDD (peak-to-trough, 시작점 100 포함)
    peak = 100.0
    mdd = 0.0
    for e in equity:
        peak = max(peak, e)
        mdd = min(mdd, (e / peak - 1.0) * 100.0)

    mean = sum(fracs) / n if n else 0.0

    sharpe = None
    sortino = None
    if n >= 2:
        var = sum((f - mean) ** 2 for f in fracs) / (n - 1)
        std = math.sqrt(var)
        if std > 0:
            sharpe = round(mean / std * math.sqrt(annualize), 3)
        # Sortino: target 0, 전체 관측 기준 downside deviation
        dd = math.sqrt(sum(min(f, 0.0) ** 2 for f in fracs) / n)
        if dd > 0:
            sortino = round(mean / dd * math.sqrt(annualize), 3)

    annualized_return_pct = None
    calmar = None
    if n >= 1 and equity[-1] > 0:
        annualized_return_pct = round(((equity[-1] / 100.0) ** (annualize / n) - 1.0) * 100.0, 3)
        if mdd < 0:
            calmar = round(annualized_return_pct / abs(mdd), 3)

    return {
        "trading_days": n,
        "cumulative_return_pct": round(cum_return_pct, 3),
        "annualized_return_pct": annualized_return_pct,
        "mdd_pct": round(mdd, 3),
        "sharpe": sharpe,
        "sortino": sortino,
        "calmar": calmar,
        "annualization_factor": annualize,
        "equity_end": round(equity[-1], 4) if equity else None,
    }


def _monthly_returns(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_month: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        key = r["trade_date"].strftime("%Y-%m")
        m = by_month.setdefault(key, {"month": key, "factor": 1.0, "closed_count": 0, "win_count": 0, "trading_days": 0})
        m["factor"] *= (1.0 + float(r["daily_return_pct"]) / 100.0)
        m["closed_count"] += int(r["closed_count"])
        m["win_count"] += int(r["win_count"])
        m["trading_days"] += 1
    out = []
    for key in sorted(by_month):
        m = by_month[key]
        cc = m["closed_count"]
        out.append({
            "month": key,
            "return_pct": round((m["factor"] - 1.0) * 100.0, 3),
            "closed_count": cc,
            "win_rate_pct": round(m["win_count"] / cc * 100.0, 1) if cc else None,
            "trading_days": m["trading_days"],
        })
    return out


def _get_performance_metrics(
    market: str,
    account_type: str,
    window_start: Optional[str] = None,
    window_end: Optional[str] = None,
    include_daily_curve: bool = False,
) -> Dict[str, Any]:
    # --- account_type 필수 검증 (혼합 집계 API 레벨 차단) ---
    at = (account_type or "").strip().lower()
    if at == "live":
        return mcp_error(
            MCPErrorCode.NO_DATA,
            "live(실전) 성과 기록이 아직 없습니다 (활성 실전 계좌 0). "
            "live 커브는 실측 계좌 잔고 기반으로 별도 구축 예정이며, "
            "paper 커브와 절대 연결하지 않습니다 (커브 미연결 원칙).",
            account_type="live",
        )
    if at != "paper":
        return mcp_error(
            MCPErrorCode.NO_DATA,
            f"account_type 은 'paper' 또는 'live' 여야 합니다: {account_type!r}. "
            "paper/live 혼합 집계는 지원하지 않습니다 (필수 인자).",
        )

    mkt = _MARKET_ALIAS.get((market or "").strip().lower())
    if not mkt:
        return mcp_error(
            MCPErrorCode.NO_DATA,
            f"market 은 coin|kr|us|all (또는 crypto/kr_stock/us_stock) 이어야 합니다: {market!r}",
        )

    try:
        start = _parse_date(window_start, PUBLIC_ANCHOR, "window_start")
        end = _parse_date(window_end, _today_kst(), "window_end")
    except ValueError as exc:
        return mcp_error(MCPErrorCode.NO_DATA, str(exc))
    if start > end:
        return mcp_error(MCPErrorCode.NO_DATA, f"window_start({start}) > window_end({end})")

    fetch_markets = ["coin", "kr", "us"] if mkt == "all" else [mkt]
    rows = _fetch_daily_rows(fetch_markets, start, end)
    if mkt == "all":
        rows = _combine_all_markets(rows)

    model_meta = {
        "type": "synthetic_fixed_book",
        "book_slots": BOOK_SLOTS,
        "position_notional": "capital/400 × sizing_multiplier(0.25~2.0)",
        "daily_return_formula": "Σ(profit_loss_pct × sizing_multiplier) / 400",
        "public_anchor": PUBLIC_ANCHOR.isoformat(),
        "note": (
            "페이퍼 엔진에는 자본/현금 실계열이 없어(무자본 사이징 배수 체결) 합성 고정 북 모델을 사용. "
            "근거: 2026-06-16 이후 동시 노출(Σ배수) 최대 371 < 400 — 노출이 자본 100%를 넘지 않음. "
            "미사용 슬롯은 현금 0% (cash drag 포함). 정의 단일 소스 = admin.v1_daily_equity."
        ),
    }
    base = {
        "market": mkt,
        "account_type": "paper",
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "capital_model": model_meta,
    }

    if not rows:
        return {
            **base,
            "metrics": None,
            "monthly_returns": [],
            "totals": {"closed_count": 0, "win_rate_pct": None},
            "_llm_summary": f"[{mkt}/paper] {start}~{end} 창에 청산 트레이드 없음",
        }

    returns = [float(r["daily_return_pct"]) for r in rows]
    metrics = _compute_metrics(returns, _ANNUALIZE[mkt])
    monthly = _monthly_returns(rows)

    closed_total = sum(int(r["closed_count"]) for r in rows)
    win_total = sum(int(r["win_count"]) for r in rows)
    totals = {
        "closed_count": closed_total,
        "win_count": win_total,
        "win_rate_pct": round(win_total / closed_total * 100.0, 1) if closed_total else None,
    }

    result: Dict[str, Any] = {
        **base,
        "metrics": metrics,
        "monthly_returns": monthly,
        "totals": totals,
    }

    if include_daily_curve:
        equity = 100.0
        curve = []
        for r in rows:
            equity *= (1.0 + float(r["daily_return_pct"]) / 100.0)
            curve.append({
                "date": r["trade_date"].isoformat(),
                "daily_return_pct": round(float(r["daily_return_pct"]), 6),
                "equity": round(equity, 4),
                "closed_count": int(r["closed_count"]),
            })
        result["daily_curve"] = curve

    result["_llm_summary"] = (
        f"[{mkt}/paper 성과] {start}~{end}: 누적 {metrics['cumulative_return_pct']:+.2f}%, "
        f"MDD {metrics['mdd_pct']:.2f}%, 샤프 {metrics['sharpe']}, "
        f"청산 {closed_total}건 승률 {totals['win_rate_pct']}%"
    )
    return result


def _ai_summary(data: dict) -> str:
    m = data.get("metrics") or {}
    w = data.get("window", {})
    return (
        f"{data.get('market')}/{data.get('account_type')} {w.get('start')}~{w.get('end')} — "
        f"cum {m.get('cumulative_return_pct')}%, MDD {m.get('mdd_pct')}%, "
        f"sharpe {m.get('sharpe')}, sortino {m.get('sortino')}, calmar {m.get('calmar')}"
    )


def _user_summary(data: dict) -> str:
    m = data.get("metrics")
    w = data.get("window", {})
    if not m:
        return f"{data.get('market')} 시장 {w.get('start')}~{w.get('end')} 구간에 청산 트레이드가 없습니다."
    return (
        f"{data.get('market')} 시장 모의매매 {w.get('start')}~{w.get('end')} 성과: "
        f"누적 {m.get('cumulative_return_pct'):+.2f}%, 최대낙폭 {m.get('mdd_pct'):.2f}%입니다. "
        f"(합성 고정 북 모델 — 방법론 참조)"
    )


# ---------------------------------------------------------------------------
# Tool 등록
# ---------------------------------------------------------------------------

def register_performance_metrics_tools(mcp, cache):
    """성과 지표 Tool 등록"""

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_performance_metrics(
        market: str,
        account_type: str,
        window_start: Optional[str] = None,
        window_end: Optional[str] = None,
        include_daily_curve: bool = False,
    ) -> Dict[str, Any]:
        """
        Purpose: Portfolio-level performance metrics (MDD / Sharpe / Sortino / Calmar /
            monthly returns / equity curve) over a FIXED window — the single canonical
            computation path shared by the OneQAZ blog and external clients.
        Triggers (casual questions too): "what's the max drawdown?", "MDD 얼마야?",
            "샤프 비율 보여줘", "monthly returns table?", "트랙레코드 지표", "에쿼티 커브 데이터".
        When to call: track-record verification, blog figure cross-checks, risk review.
        Prerequisites: none.
        Next steps: get_trade_history for the underlying trades, analyze_trades for breakdowns.
        Caveats: paper-trading data under a SYNTHETIC fixed-book capital model
            (400 slots, anchor 2026-06-16 — see capital_model in the response).
            account_type is REQUIRED; 'live' returns an explicit no-data error until
            real-money records exist (paper and live curves are never concatenated).
            Fixed window → same inputs always reproduce the same numbers (as-of verifiable).

        Args:
            market: coin | kr | us | all (aliases crypto/kr_stock/us_stock accepted).
                'all' = fixed 1/3 allocation across the three books.
            account_type: REQUIRED. 'paper' (simulated) or 'live' (real — not yet available).
            window_start: ISO date (YYYY-MM-DD). Default 2026-06-16 (public track-record anchor).
            window_end: ISO date. Default today (KST).
            include_daily_curve: include per-day equity curve rows (default false).

        Disclaimer: Information only, not investment advice. Simulated performance.
        """
        cache_key = f"perf_metrics_{market}_{account_type}_{window_start}_{window_end}_{include_daily_curve}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_TRADE_HISTORY * 3)
        if cached:
            return cached

        result = await asyncio.to_thread(
            _get_performance_metrics,
            market=market,
            account_type=account_type,
            window_start=window_start,
            window_end=window_end,
            include_daily_curve=include_daily_curve,
        )

        wrapped = wrap_tool_response(result, "performance_metrics", _ai_summary, _user_summary)
        cache.set(cache_key, wrapped)
        return wrapped

    logger.info("  [OK] Performance Metrics tools registered")
