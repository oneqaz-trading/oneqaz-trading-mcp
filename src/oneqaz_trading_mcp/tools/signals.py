# -*- coding: utf-8 -*-
"""
시그널 조회 Tool
================
PG market_{coin,kr,us}.signals 에서 동적 조건으로 시그널 데이터를 조회합니다.

주요 기능:
- 특정 종목의 최신 시그널 조회 (per-symbol 논리 키 → PG symbol 필터 라우팅)
- 전체 시장 시그널 조회 (PG 단일 통합 쿼리 — 2026-08-10 glob 열거 대체)
- 인터벌/액션별 점수 기반 필터링
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional

from pydantic import Field

from oneqaz_trading_mcp.config import (
    CACHE_TTL_POSITIONS,
    get_signal_db_path,
    get_symbol_from_signal_db,
    get_market_db_path,
    connect_readonly,
)
from oneqaz_trading_mcp.resources.resource_response import (
    mcp_error, MCPErrorCode, MCPErrorAction, wrap_tool_response,
    hoist_constant_fields, clamp_limit, truncation_fields, normalize_choice,
)

# Pinned outputSchema for get_signals (FastMCP introspects return annotation).
try:
    from oneqaz_trading_mcp.schemas import RoleAnalysisEnvelope, SignalDetailEnvelope, SignalsEnvelope
except ImportError:  # pragma: no cover
    SignalsEnvelope = Dict[str, Any]  # type: ignore[assignment,misc]

logger = logging.getLogger("MarketMCP")

_MARKET_LABEL_KO = {"crypto": "암호화폐", "kr_stock": "한국 주식", "us_stock": "미국 주식"}


def _clean_direction(v) -> str | None:
    """[2026-06-15] 방향 라벨 정화 — 소스 integrated_direction 에 stringify 된 numpy
    NaN("nan")이 섞여 카드에 "nan" 이 그대로 노출되던 것 차단. nan/none/빈값 → neutral."""
    if v is None:
        return None
    s = str(v).strip()
    if s.lower() in ("nan", "none", "null", ""):
        return "neutral"
    return s


def _market_label_signals(market_id: str) -> str:
    return _MARKET_LABEL_KO.get(market_id, market_id)


def _ai_summary_signals(data: dict) -> str:
    market_id = data.get("market_id", "")
    stats = data.get("stats", {}) or {}
    sym = str((data.get("filters") or {}).get("symbol") or "").upper()
    latest = data.get("latest_by_interval") or {}
    if sym and latest:
        head = latest.get("combined") or next(iter(latest.values()))
        return (
            f"{sym} ({market_id}) latest as of {_kst_hm(head.get('timestamp'))}: "
            f"{'; '.join(_latest_parts(latest, False))} | history rows "
            f"(last {(data.get('filters') or {}).get('hours_back', 24)}h, all intervals): "
            f"{stats.get('total', 0)} — buy {stats.get('buy_count', 0)} / sell {stats.get('sell_count', 0)} / "
            f"hold {stats.get('hold_count', 0)} (history counts, not the current call)"
        )
    return (
        f"{market_id} 시그널 {stats.get('total', 0)}건 — "
        f"buy {stats.get('buy_count', 0)} / sell {stats.get('sell_count', 0)} / hold {stats.get('hold_count', 0)}, "
        f"avg score {stats.get('avg_score', 0):.2f}"
    )


def _user_summary_signals(data: dict) -> str:
    market_id = data.get("market_id", "")
    label = _market_label_signals(market_id)
    stats = data.get("stats", {}) or {}
    total = stats.get("total", 0)
    sym = str((data.get("filters") or {}).get("symbol") or "").upper()
    latest = data.get("latest_by_interval") or {}
    if sym and latest:
        return _symbol_latest_summary_ko(sym, latest, stats, data.get("filters") or {})
    if total == 0:
        return f"{label} 시장에 조건에 맞는 시그널이 없습니다."
    buy = stats.get("buy_count", 0)
    sell = stats.get("sell_count", 0)
    # [2026-10-07 ANS-002] 이 수치는 최근 N시간 이력 행 집계다 — '지금 매수 N건' 으로 읽히지 않게 표기.
    return (
        f"{label} 시장 최근 {(data.get('filters') or {}).get('hours_back', 24)}시간 시그널 이력 "
        f"{total}행 중 매수 {buy}·매도 {sell} (종목·인터벌별 행을 센 이력 집계, 현재 판정 아님)."
    )


# [2026-10-07 ANS-002] symbol 지정 조회의 요약은 '최신 판정' 과 '이력 집계' 를 분리한다.
# 종전 요약은 BTC 를 조회해도 종목명 없이 "50개의 시그널, 매수 5건" 이었는데, 50행은
# 5 interval × 10 스냅샷 이력이고 매수 5건은 2시간 전 combined 행이었다(13:00 기준 전 인터벌 hold).
# "사도 돼?" 질문이 가장 먼저 닿는 경로라 오독 비용이 가장 크다.
_ACTION_KO = {"buy": "매수", "sell": "매도", "hold": "관망"}


def _kst_hm(ts) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone(timedelta(hours=9))).strftime("%m-%d %H:%M KST")
    except Exception:
        return "?"


def _latest_parts(latest: dict, ko: bool) -> List[str]:
    parts = []
    for iv, v in latest.items():
        act = v.get("action") or "?"
        label = _ACTION_KO.get(act, act) if ko else act
        if iv == "combined":
            parts.append(f"combined {label}({(v.get('signal_score') or 0):+.2f})")
        else:
            parts.append(f"{iv} {label}")
    return parts


def _symbol_latest_summary_ko(sym: str, latest: dict, stats: dict, filters: dict) -> str:
    head = latest.get("combined") or next(iter(latest.values()))
    hours = filters.get("hours_back", 24)
    total = stats.get("total", 0)
    hist = (
        f"최근 {hours}시간 이력 {total}행 중 매수 {stats.get('buy_count', 0)}·매도 {stats.get('sell_count', 0)}"
        if total else f"최근 {hours}시간 신호 이력 없음"
    )
    return (
        f"{sym} 최신({_kst_hm(head.get('timestamp'))}): {' · '.join(_latest_parts(latest, True))}"
        f" / {hist} (이력 집계는 현재 판정이 아님)."
    )


def _ai_summary_signal_detail(data: dict) -> str:
    sym = data.get("symbol", "?")
    sig = data.get("latest_signal", {}) or {}
    score = sig.get("signal_score") or 0
    return f"{sym} 최신 시그널 — action={sig.get('action', '?')}, score={score:.2f}, interval={data.get('interval', '?')}"


def _user_summary_signal_detail(data: dict) -> str:
    sym = data.get("symbol", "?")
    sig = data.get("latest_signal", {}) or {}
    action = sig.get("action", "?")
    label_action = {"buy": "매수", "sell": "매도", "hold": "관망"}.get(action, action)
    return f"{sym} 종목의 최신 시그널은 '{label_action}'입니다."


def _ai_summary_role_analysis(data: dict) -> str:
    sym = data.get("symbol", "?")
    h = data.get("hierarchy", {}) or {}
    align = h.get("alignment_score", 0) or 0
    return f"{sym} 역할별 분석 — alignment={align:.2f} ({h.get('alignment_type', '?')}), 방향조합={h.get('position_key', '?')}"


def _user_summary_role_analysis(data: dict) -> str:
    sym = data.get("symbol", "?")
    h = data.get("hierarchy", {}) or {}
    a_type = h.get("alignment_type", "")
    if not a_type:
        return f"{sym} 종목의 멀티타임프레임 분석 결과가 아직 충분치 않습니다."
    return f"{sym} 종목은 단기·중기·장기 시그널 정합도가 '{a_type}' 상태입니다."


# ---------------------------------------------------------------------------
# 헬퍼: 종목별 DB에서 안전하게 쿼리
# ---------------------------------------------------------------------------

def _safe_query(db_path: Path, query: str, params=()) -> list:
    """종목별 DB에서 안전하게 쿼리 실행"""
    rows = []
    try:
        with connect_readonly(db_path, timeout=5.0) as conn:
            for row in conn.execute(query, params).fetchall():
                rows.append(dict(row))
    except Exception as e:
        logger.debug("Signal DB query error [%s]: %s", db_path.name, e)
    return rows


# [2026-09-22 경량화] reason 캐비앗 정본 — 종전에는 이 문장이 행마다 복제돼
# 실측 crypto/500행에서 88.5KB(고유값 1)를 차지했다. 행에는 불리언
# reason_uncalibrated 만 두고 문장은 응답 최상위에 1회만 싣는다.
_REASON_UNCALIBRATED_NOTE = (
    "reason 내 승률/신뢰도는 전략 내부 미보정 값(소표본 가능)입니다 — "
    "확률로 해석하지 마세요. 보정된 확률은 confidence_calibrated 를 사용."
)

# [2026-09-22 경량화] 응답 내 전 행이 같은 값을 갖는 필드는 common 으로 1회만
# 올린다. 실측 crypto/500행: currency 9.5KB + risk_level 12.0KB +
# timestamp_str 22.0KB (셋 다 고유값 1). 값이 갈리는 응답에서는 행에 그대로
# 남으므로 정보 손실이 없다 — '상수라서 뺀다'가 아니라 '이 응답에서 상수일 때만'.
_HOISTABLE_FIELDS = ("currency", "risk_level", "timestamp_str")

# limit 상한 — 외부 클라이언트가 limit=100000 으로 전 구간을 긁는 것을 막는다.
_SIGNALS_LIMIT_MAX = 500
_SIGNALS_LIMIT_DEFAULT = 50


# [2026-09-22 STEP A] 구현은 mcps/resources/resource_response.py 로 승격됐다
# (hoist_constant_fields / clamp_limit). 상수만 이 파일에 남긴다 — 어느 필드를
# 올릴지는 tool 마다 다르고, 특히 차원키를 올리면 안 되는 tool 이 있기 때문이다.


def _calibration_note() -> str:
    try:
        from oneqaz_trading_mcp.confidence_calibration import CALIBRATION_NOTE
        return CALIBRATION_NOTE
    except Exception:
        return "confidence_calibrated unavailable"


def _format_signal_row(row: dict, symbol_from_file: Optional[str] = None,
                       market_id: Optional[str] = None) -> dict:
    """DB 행을 API 형식으로 변환. symbol_from_file이 있으면 DB에 symbol/coin이 없을 때 사용 (시장별 DB 일관성)."""
    # symbol 컬럼 사용; 없으면 파일명 기반 심볼 사용
    symbol = row.get("symbol") or symbol_from_file or "UNKNOWN"

    # [2026-07-20 RCA C1] 실측 기반 캘리브레이션 병행 노출 (매핑 불가 시 null)
    try:
        from oneqaz_trading_mcp.confidence_calibration import calibrated_confidence
        _cal = calibrated_confidence(market_id, row.get("interval"), row.get("confidence"))
    except Exception:
        _cal = None

    signal = {
        "symbol": symbol,
        "interval": row.get("interval"),
        "signal_score": round(row.get("signal_score") or 0, 3),
        "confidence": round(row.get("confidence") or 0, 3),
        "confidence_calibrated": _cal,
        "action": row.get("action"),
        "current_price": row.get("current_price"),
        "target_price": row.get("target_price"),
        "indicators": {
            "rsi": round(row["rsi"], 1) if row.get("rsi") else None,
            "macd": round(row["macd"], 4) if row.get("macd") else None,
            "mfi": round(row["mfi"], 1) if row.get("mfi") else None,
            "atr": round(row["atr"], 4) if row.get("atr") else None,
            "adx": round(row["adx"], 1) if row.get("adx") else None,
        },
        "wave_phase": row.get("wave_phase"),
        "pattern_type": row.get("pattern_type"),
        "risk_level": row.get("risk_level"),
        "volatility": round(row["volatility"], 4) if row.get("volatility") else None,
        "direction": _clean_direction(row.get("integrated_direction")),
        "strength": round(row["integrated_strength"], 3) if row.get("integrated_strength") else None,
        "recommended_strategy": row.get("recommended_strategy"),
        "strategy_match": round(row["strategy_match"], 3) if row.get("strategy_match") else None,
        "reason": row.get("reason"),
        # [2026-08-18 위생] 통화 단위 명시 — daily_brief 등 크로스마켓 배열에서
        # USD/KRW 혼재 오독 방지 (신뢰 평가 패널 적발)
        "currency": "USD" if (market_id or "").lower().startswith("us") else "KRW",
        "timestamp": row.get("timestamp"),
    }

    # [2026-09-22 경량화] warnings 희소화 — True 인 플래그만 담고, 전부 False 면
    # 키 자체를 생략한다 (실측 crypto/500행에서 이 필드만 59.4KB, 384/500 이
    # all-False). 소비자는 w.get("peak_warning") 형태라 희소 dict 에서도 동작이
    # 동일하다 — 아래 _generate_signals_query_summary 의 고점경고/저점신호 마커 포함.
    _warn = {
        k: True
        for k in ("peak_warning", "bottom_warning",
                  "momentum_slowing", "momentum_recovering")
        if row.get(k)
    }
    if _warn:
        signal["warnings"] = _warn

    # [2026-08-18 위생] reason 원시 문자열의 미보정 승률/신뢰도 방어 —
    # '승률:100.0%'(전략 내부 소표본 값)를 순진한 LLM 클라이언트가 확률로 인용하는
    # 최대 위험 지목 (신뢰 평가 패널 2/4 페르소나). 숫자를 숨기지 않되(공개 원칙)
    # 인라인 '(미보정)' 마킹 + 캐비앗 필드로 무력화한다.
    _r = signal.get("reason")
    if _r and ("승률" in _r or "신뢰도" in _r):
        try:
            signal["reason"] = re.sub(r"(승률|신뢰도)\s*:", r"\1(미보정):", _r)
            # [2026-09-22 경량화] 캐비앗 문장은 응답 최상위 1회
            # (_REASON_UNCALIBRATED_NOTE). 행에는 플래그만 — 경고 의미론 불변.
            signal["reason_uncalibrated"] = True
        except Exception:
            pass

    if signal["timestamp"]:
        try:
            # [2026-08-18 위생] TZ 명시 (KST) — 무표기 로컬 시간은 외부 클라이언트가
            # 신선도를 최대 9시간 오판 (신뢰 평가 패널 적발)
            signal["timestamp_str"] = datetime.fromtimestamp(
                signal["timestamp"], tz=timezone(timedelta(hours=9))
            ).strftime("%Y-%m-%d %H:%M:%S KST")
        except Exception:
            signal["timestamp_str"] = str(signal["timestamp"])

    return signal


# ---------------------------------------------------------------------------
# 시그널 조회 함수
# ---------------------------------------------------------------------------

# _format_signal_row 가 소비하는 컬럼만 명시 (SELECT * 금지 — score_trace 등
# 광폭 JSON 컬럼의 전송/디토스트 방지). 3시장 스키마 전수 존재 확인(2026-08-10).
_SIGNAL_ROW_COLS = (
    "symbol, interval, signal_score, confidence, action, current_price, "
    "target_price, rsi, macd, mfi, atr, adx, wave_phase, pattern_type, "
    "risk_level, volatility, integrated_direction, integrated_strength, "
    "recommended_strategy, strategy_match, peak_warning, bottom_warning, "
    "momentum_slowing, momentum_recovering, reason, timestamp"
)

# 인터벌 브랜치당 최신순 스캔 상한. (interval, timestamp DESC) btree 조기종료로
# 테이블 크기 무관 상수 IO (coin 43ms / kr 736ms / us 4ms — 2026-08-10 실측).
# naive `timestamp > cutoff` 24h 스캔은 240m/1d 파티션 BRIN lossy bitmap 이
# 백만 행 recheck 를 유발해 20s+ 타임아웃 — 사용 금지.
# 3000 ≈ 활성 500심볼 × 6사이클: 윈도 내 존재하지만 최근 수 사이클 동안 발화
# 없는 심볼만 캡 밖으로 밀릴 수 있다 (응답은 어차피 최신순 상위 limit 개).
_SCAN_CAP_PER_INTERVAL = 3000
_PER_SYMBOL_ROWS = 2  # 시장 전체 조회 시 심볼당 최신 행 수 (구 per_db_limit=2 동일)


def _query_market_signals_pg(
    market_id: str,
    interval: Optional[str],
    action_filter: Optional[str],
    min_score: Optional[float],
    min_confidence: Optional[float],
    limit: int,
    hours_back: int,
):
    """시장 전체 최신 시그널 — PG market_{m}.signals 단일 통합 쿼리.

    [2026-08-10] 종전 list_signal_db_files() glob 은 2026-05 SQLite 동결 백업의
    파일명으로 심볼을 열거해, 이후 상장된 활성 심볼(coin 실측 27개: BSB, AEON,
    ARX 등)이 시장 전체 조회에서 비가시였다. PG 에서 직접 조회해 심볼 유니버스를
    실데이터와 일치시킨다 (07-22 "MCP roles 300쿼리→2" 선례의 확장 —
    구 방식은 심볼당 커넥션+쿼리 1회씩 수백 회).

    반환: 시그널 row dict 리스트. 시장 ID 불명 시 mcp_error dict.
    """
    import time as _time
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    from oneqaz_trading_mcp.resources.signal_system import _MARKET_PG_SCHEMA, _get_active_intervals

    schema = _MARKET_PG_SCHEMA.get(market_id.lower().replace("-", "_"))
    if not schema:
        return mcp_error(MCPErrorCode.DB_NOT_FOUND, f"No signal DBs found for market: {market_id}", action=MCPErrorAction.CHECK, action_value="market://info", available_markets=["crypto", "kr_stock", "us_stock"])

    conn = open_schema_connection(schema, readonly=True)
    try:
        if interval:
            branch_ivs = [interval]
        else:
            # 활성 인터벌(1h 캐시) + combined. 구 per-symbol DB 순회도 combined
            # 행을 포함했으므로 동일 의미론 유지.
            branch_ivs = list(_get_active_intervals(conn, schema, market_id))
            if "combined" not in branch_ivs:
                branch_ivs.append("combined")
        cutoff = int(_time.time()) - hours_back * 3600

        extra_sql = ""
        extra_params: List[Any] = []
        if action_filter:
            extra_sql += " AND action = ?"
            extra_params.append(action_filter.lower())
        if min_score is not None:
            extra_sql += " AND signal_score >= ?"
            extra_params.append(min_score)
        if min_confidence is not None:
            extra_sql += " AND confidence >= ?"
            extra_params.append(min_confidence)

        branch = (
            f"(SELECT {_SIGNAL_ROW_COLS} FROM {schema}.signals"
            f" WHERE interval = ? AND timestamp > ?{extra_sql}"
            f" ORDER BY timestamp DESC LIMIT {_SCAN_CAP_PER_INTERVAL})"
        )
        union_sql = " UNION ALL ".join([branch] * len(branch_ivs))
        params: List[Any] = []
        for iv in branch_ivs:
            params.extend([iv, cutoff, *extra_params])

        sql = (
            "SELECT * FROM ("
            " SELECT u.*, ROW_NUMBER() OVER ("
            "   PARTITION BY u.symbol"
            "   ORDER BY u.timestamp DESC, u.signal_score DESC NULLS LAST"
            " ) AS _rn"
            f" FROM ({union_sql}) u"
            ") w WHERE _rn <= ?"
            " ORDER BY timestamp DESC, signal_score DESC NULLS LAST LIMIT ?"
        )
        params.extend([_PER_SYMBOL_ROWS, limit])
        rows = [dict(r) for r in conn.execute(sql, tuple(params)).fetchall()]
    finally:
        try:
            conn.close()
        except Exception:
            pass

    for r in rows:
        r.pop("_rn", None)
    return rows


def _get_latest_signals(
    market_id: str,
    coin: Optional[str] = None,
    interval: Optional[str] = None,
    action_filter: Optional[str] = None,
    min_score: Optional[float] = None,
    min_confidence: Optional[float] = None,
    limit: int = _SIGNALS_LIMIT_DEFAULT,
    hours_back: int = 24,
) -> Dict[str, Any]:
    """최신 시그널 조회 (특정 종목: per-symbol 라우팅 / 시장 전체: PG 단일 쿼리)"""

    # [2026-09-22] 기본 500 → 50. 실측 crypto/500행 응답이 553KB 로, 단일 응답이
    # 소형 클라이언트 컨텍스트를 통째로 먹었다. 상한은 여전히 500 이며 잘린 사실은
    # truncated/ truncated_note 로 명시 노출한다 (아래).
    limit = clamp_limit(limit, _SIGNALS_LIMIT_DEFAULT, _SIGNALS_LIMIT_MAX)

    # [2026-09-22] 열거형 파라미터 검증 — 종전에는 interval='99z' / action_filter='yolo'
    # 가 WHERE 절에 그대로 들어가 **조용히 0건**을 냈다. 0건은 "그런 시그널이 없다"
    # 라는 유효한 답과 구분되지 않아 호출자가 오타를 영영 모른다.
    interval, _err = normalize_choice("interval", "interval", interval)
    if _err:
        return _err
    action_filter, _err = normalize_choice("action", "action_filter", action_filter)
    if _err:
        return _err
    # [2026-10-07 VALID-001] 시장 정규화 + 종목 커버리지 확인. 종전에는 없는 종목·종목명·
    # 시장 착오가 전부 "조건에 맞는 시그널이 없습니다"(0건)로 끝나 정상 답과 구분되지 않았다.
    market_id, _err = normalize_choice("market", "market_id", market_id)
    if _err:
        return _err
    latest_by_interval: Dict[str, Any] = {}
    if coin:
        from oneqaz_trading_mcp.symbol_coverage import check_symbol
        _nf = check_symbol(market_id, coin, tool="get_signals")
        if _nf:
            return _nf

    try:
        all_signals = []

        if coin:
            # 특정 종목: per-symbol 논리 키 → PG symbol 필터 라우팅 (기존 경로 유지)
            sym_db = get_signal_db_path(market_id, symbol=coin)
            # [2026-08-10] .exists() 게이트 제거 — sym_db 는 PG 라우팅용 논리 키.
            # SQLite 동결(2026-05) 후 상장된 심볼은 파일이 없어 PG 에 시그널이 있는데도
            # SYMBOL_NOT_FOUND 를 내던 활성 결함 (coin 활성 27심볼 실측, BSB 등).
            if not sym_db:
                return mcp_error(MCPErrorCode.SYMBOL_NOT_FOUND, f"Signal DB not found for {coin} in {market_id}", action=MCPErrorAction.FALLBACK, action_value=f"get_signals(market_id='{market_id}')", fallback_tool=f"market://{market_id}/signals/summary")

            symbol_from_file = get_symbol_from_signal_db(sym_db)
            query = """
                SELECT *
                FROM signals
                WHERE timestamp > (strftime('%s', 'now') - ?)
            """
            params: List[Any] = [hours_back * 3600]

            if interval:
                query += " AND interval = ?"
                params.append(interval)

            if action_filter:
                query += " AND action = ?"
                params.append(action_filter.lower())

            if min_score is not None:
                query += " AND signal_score >= ?"
                params.append(min_score)

            if min_confidence is not None:
                query += " AND confidence >= ?"
                params.append(min_confidence)

            query += " ORDER BY timestamp DESC, signal_score DESC LIMIT ?"
            params.append(limit)

            rows = _safe_query(sym_db, query, tuple(params))
            for row in rows:
                all_signals.append(_format_signal_row(row, symbol_from_file=symbol_from_file,
                                                      market_id=market_id))
            symbol_count = 1
            latest_by_interval = _latest_by_interval(sym_db, market_id)
        else:
            # 시장 전체: PG 통합 쿼리 (2026-08-10 — 동결 glob 열거 대체)
            rows = _query_market_signals_pg(
                market_id, interval, action_filter, min_score, min_confidence,
                limit, hours_back,
            )
            if isinstance(rows, dict):  # mcp_error
                return rows
            for row in rows:
                all_signals.append(_format_signal_row(row, market_id=market_id))
            symbol_count = len({s["symbol"] for s in all_signals})

        # 전체 결과를 점수순으로 정렬 후 limit 적용
        all_signals.sort(key=lambda x: (x.get("timestamp") or 0, x.get("signal_score") or 0), reverse=True)
        signals = all_signals[:limit]

        # 통계 (db_count: 구 "스캔한 심볼 DB 파일 수" → 응답 내 고유 심볼 수.
        # 키 이름은 하위호환 유지)
        stats = {
            "total": len(signals),
            "buy_count": sum(1 for s in signals if s["action"] == "buy"),
            "sell_count": sum(1 for s in signals if s["action"] == "sell"),
            "hold_count": sum(1 for s in signals if s["action"] == "hold"),
            "avg_score": round(sum(s["signal_score"] for s in signals) / len(signals), 3) if signals else 0,
            "db_count": symbol_count,
        }
        
        # [2026-09-22 정직성] 잘림 노출 — 종전 응답은 stats.total 에 '반환된 건수'만
        # 실어, limit 에 걸려 잘린 것과 원래 그 수뿐인 것을 외부에서 구분할 수
        # 없었다. LLM 클라이언트는 이를 '시장 전체'로 읽는다.
        # [2026-09-22 STEP B] total_available 도입. 단 signals 의 pre-slice 총량
        # (len(all_signals))은 SQL 스캔 상한(_SCAN_CAP_PER_INTERVAL) 과 LIMIT 에
        # 걸린 뒤의 수라 **정확한 총량이 아니다** ⇒ 하한으로 표시한다.
        # governance/calibration 처럼 전량 조회 후 자르는 tool 만 정확한 총량을 낸다.
        trunc = truncation_fields(
            returned=len(signals),
            total=len(all_signals),
            limit=limit,
            maximum=_SIGNALS_LIMIT_MAX,
            hint="interval/action_filter/min_score 로 범위를 좁히세요.",
            total_is_lower_bound=True,
        )
        truncated = trunc["truncated"]

        llm_summary = _generate_signals_query_summary(market_id, signals, stats, coin, interval,
                                                      latest_by_interval)
        if truncated:
            llm_summary += (
                "\n- ⚠️ 상위 {}건만 반환(잘림) — 조건에 맞는 실제 건수는 더 많을 수 있음".format(limit)
            )

        # [2026-09-22 경량화] 전 행이 같은 값인 필드를 common 으로 승격 (행에서 제거)
        common = hoist_constant_fields(signals, _HOISTABLE_FIELDS)

        resp: Dict[str, Any] = {
            "market_id": market_id,
            "filters": {
                "symbol": coin,
                "interval": interval,
                "action": action_filter,
                "min_score": min_score,
                "min_confidence": min_confidence,
                "hours_back": hours_back,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "signals": signals,
            "stats": stats,
            **trunc,
            "confidence_calibrated_note": _calibration_note(),
            "_llm_summary": llm_summary,
        }
        if latest_by_interval:
            # [2026-10-07 ANS-002] additive — 인터벌별 최신 1행 (hours_back 과 무관한 현재 판정).
            resp["latest_by_interval"] = latest_by_interval
            resp["latest_by_interval_note"] = (
                "latest_by_interval = the newest signal per interval for this symbol (the current "
                "call, independent of hours_back). `signals` / `stats` are the history rows within "
                "hours_back across all intervals — counts there are history, not current calls."
            )
        if common:
            resp["common"] = common
            resp["common_note"] = (
                "common 의 값은 signals 의 모든 행에 적용됩니다(행에서는 생략). "
                "행에 같은 키가 있으면 행 값이 우선합니다."
            )
        if any(s.get("reason_uncalibrated") for s in signals):
            resp["reason_uncalibrated_note"] = _REASON_UNCALIBRATED_NOTE
        return resp
        
    except Exception as e:
        logger.error(f"Failed to get signals for {market_id}: {e}")
        return mcp_error(MCPErrorCode.NO_DATA, str(e), market_id=market_id)


def _get_coin_signal_detail(
    market_id: str,
    coin: str,
    interval: str = "combined",
) -> Dict[str, Any]:
    """특정 종목의 상세 시그널 정보 (종목별 DB에서 읽기)"""
    # [2026-10-07 VALID-001] 시장 정규화 + 종목 커버리지 확인 (signals 공용 검사)
    market_id, _err = normalize_choice("market", "market_id", market_id)
    if _err:
        return _err
    interval, _err = normalize_choice("interval", "interval", interval or "combined")
    if _err:
        return _err
    from oneqaz_trading_mcp.symbol_coverage import check_symbol
    _nf = check_symbol(market_id, coin, tool="get_signal_detail")
    if _nf:
        return _nf
    sym_db = get_signal_db_path(market_id, symbol=coin)

    # [2026-08-10] .exists() 게이트 제거 — 위 _get_latest_signals 와 동일한 이유
    if not sym_db:
        return mcp_error(MCPErrorCode.SYMBOL_NOT_FOUND, f"Signal DB not found for {coin} in {market_id}", action=MCPErrorAction.FALLBACK, action_value=f"get_signals(market_id='{market_id}')", fallback_tool=f"market://{market_id}/signals/summary")

    try:
        with connect_readonly(sym_db) as conn:

            # 최신 시그널
            cursor = conn.execute("""
                SELECT *
                FROM signals
                WHERE interval = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (interval,))

            row = cursor.fetchone()
            if not row:
                # [2026-10-07 VALID-001] 종전 SIGNAL_STALE(retryable, 60초 후 재시도)은 틀린
                # 분류였다 — 종목은 있으나 이 interval 의 행이 아예 없는 것은 기다려도 생기지
                # 않는다(예: kr/us 에는 240m 이 없다). 없는 종목은 위 check_symbol 이 먼저 거른다.
                from oneqaz_trading_mcp.data_collection.core.intervals import MARKET_INTERVALS
                from oneqaz_trading_mcp.resources.signal_system import _MARKET_PG_SCHEMA
                _ivs = list(MARKET_INTERVALS.get(_MARKET_PG_SCHEMA.get(market_id, "").removeprefix("market_"), ()))
                return mcp_error(
                    MCPErrorCode.NO_DATA,
                    f"{coin} exists in {market_id} but has no {interval} signal rows. "
                    f"Intervals produced for this market: {', '.join(['combined'] + _ivs)}.",
                    action=MCPErrorAction.FALLBACK,
                    action_value=f"get_signal_detail(market_id='{market_id}', symbol='{coin}', interval='combined')",
                    fallback_tool=f"get_signals(market_id='{market_id}', symbol='{coin}')",
                    market_id=market_id, symbol=coin, interval=interval,
                    available_intervals=["combined"] + _ivs,
                )

            signal = dict(row)
            
            # 최근 시그널 이력
            hist_cursor = conn.execute("""
                SELECT signal_score, action, timestamp
                FROM signals
                WHERE interval = ?
                ORDER BY timestamp DESC
                LIMIT 10
            """, (interval,))
            
            history = []
            for h in hist_cursor.fetchall():
                history.append({
                    "score": round(h["signal_score"] or 0, 3),
                    "action": h["action"],
                    "timestamp": h["timestamp"],
                })
            
            # 피드백 데이터 (trading_system.db에서)
            feedback = {}
            try:
                trading_db = get_market_db_path(market_id)
                # [2026-08-10] .exists() 제거 — PG 라우팅 논리 키. 실패는 아래 except 흡수
                if trading_db:
                    with connect_readonly(trading_db) as tconn:
                        fb_cursor = tconn.execute("""
                            SELECT signal_pattern, success_rate, avg_profit, total_trades
                            FROM signal_feedback_scores
                            WHERE symbol = ? OR symbol = ?
                            ORDER BY total_trades DESC
                            LIMIT 5
                        """, (coin.upper(), coin))
                        for fb in fb_cursor.fetchall():
                            feedback[fb["signal_pattern"]] = {
                                "success_rate": round(fb["success_rate"] or 0, 3),
                                "avg_profit": round(fb["avg_profit"] or 0, 3),
                                "total_trades": fb["total_trades"],
                            }
            except Exception:
                pass
            
            detail: Dict[str, Any] = {
                "market_id": market_id,
                "symbol": coin,
                "interval": interval,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "latest_signal": signal,
                "recent_history": history,
                "pattern_feedback": feedback,
            }
            # [2026-10-07 VALID-001] 신선도는 에러가 아니라 필드로 — 휴장 시장의 마지막 세션
            # 신호도 정상 데이터다. 소비자가 '지금 신호' 인지 스스로 판단하게 나이를 싣는다.
            try:
                _ts = int(signal.get("timestamp") or 0)
                if _ts:
                    detail["signal_age_minutes"] = int((datetime.now(timezone.utc).timestamp() - _ts) // 60)
                    detail["signal_timestamp_str"] = _kst_hm(_ts)
            except (TypeError, ValueError):
                pass
            # [2026-09-22] 행의 reason_uncalibrated 플래그에 대응하는 캐비앗 정본.
            # 문장을 행에서 뺀 이상 응답 어딘가에는 반드시 실려야 한다.
            if (isinstance(signal, dict) and signal.get("reason_uncalibrated")) or any(
                isinstance(h, dict) and h.get("reason_uncalibrated") for h in (history or [])
            ):
                detail["reason_uncalibrated_note"] = _REASON_UNCALIBRATED_NOTE
            return detail
            
    except Exception as e:
        logger.error(f"Failed to get signal detail for {coin}: {e}")
        return mcp_error(MCPErrorCode.NO_DATA, str(e), symbol=coin, market_id=market_id)


def _latest_by_interval(sym_db: Path, market_id: str) -> Dict[str, Any]:
    """[2026-10-07 ANS-002] 심볼의 인터벌별 최신 1행 (combined 먼저).

    인터벌별 `WHERE interval=? ORDER BY timestamp DESC LIMIT 1` — (symbol, interval, timestamp)
    PK 인덱스 조기 종료라 인터벌당 수 ms. DISTINCT ON (interval) 한 방은 심볼 전 행 정렬이라
    coin 에서 수만 행을 읽는다(explain_decision 27s 와 같은 함정) — 쓰지 않는다.
    실패는 경고 후 {} — 요약이 이력 표기로 돌아갈 뿐 응답은 깨지지 않는다.
    """
    from oneqaz_trading_mcp.data_collection.core.intervals import MARKET_INTERVALS
    from oneqaz_trading_mcp.resources.signal_system import _MARKET_PG_SCHEMA

    schema = _MARKET_PG_SCHEMA.get(market_id, "")
    ivs = ["combined"] + [
        iv for iv in MARKET_INTERVALS.get(schema.removeprefix("market_"), ()) if iv != "combined"
    ]
    out: Dict[str, Any] = {}
    now = datetime.now(timezone.utc).timestamp()
    try:
        with connect_readonly(sym_db, timeout=5.0) as conn:
            for iv in ivs:
                row = conn.execute(
                    "SELECT action, signal_score, confidence, timestamp FROM signals "
                    "WHERE interval = ? ORDER BY timestamp DESC LIMIT 1",
                    (iv,),
                ).fetchone()
                if not row:
                    continue
                ts = row["timestamp"]
                try:
                    from oneqaz_trading_mcp.confidence_calibration import calibrated_confidence
                    _cal = calibrated_confidence(market_id, iv, row["confidence"])
                except Exception:
                    _cal = None
                out[iv] = {
                    "action": row["action"],
                    "signal_score": round(row["signal_score"] or 0, 3),
                    "confidence_calibrated": _cal,
                    "timestamp": ts,
                    "timestamp_str": _kst_hm(ts),
                    "age_minutes": int((now - ts) // 60) if ts else None,
                }
    except Exception as e:
        logger.warning("latest_by_interval failed [%s/%s]: %s", market_id, sym_db.name, e)
        return {}
    return out


def _generate_signals_query_summary(
    market_id: str,
    signals: List[Dict],
    stats: Dict,
    coin: Optional[str],
    interval: Optional[str],
    latest_by_interval: Optional[Dict[str, Any]] = None,
) -> str:
    """LLM용 시그널 쿼리 결과 요약"""
    target = f"{coin or '전체'} {interval or '전체 인터벌'}"
    lines = [f"[{market_id.upper()} 시그널 조회: {target}]"]
    if latest_by_interval:
        head = latest_by_interval.get("combined") or next(iter(latest_by_interval.values()))
        lines.append(
            f"- 최신 판정({_kst_hm(head.get('timestamp'))}): "
            + " · ".join(_latest_parts(latest_by_interval, True))
        )
        lines.append("- 아래 건수는 이력 행 집계 (현재 판정 아님):")
    lines.append(f"- 총 {stats['total']}건({stats.get('db_count', '?')}개 종목): 매수 {stats['buy_count']}, 매도 {stats['sell_count']}, 홀드 {stats['hold_count']}")
    lines.append(f"- 평균 점수: {stats['avg_score']:.2f}")
    
    if signals:
        lines.append("- 상위 시그널:")
        for s in signals[:3]:
            warnings = []
            w = s.get("warnings", {})
            if w.get("peak_warning"):
                warnings.append("고점경고")
            if w.get("bottom_warning"):
                warnings.append("저점신호")
            warn_str = f" [{', '.join(warnings)}]" if warnings else ""
            lines.append(f"  - {s['symbol']}({s['interval']}): {s['action']} score={s['signal_score']:.2f}{warn_str}")
    
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 역할별 분석 (hierarchy_context 기반)
# ---------------------------------------------------------------------------

_ROLE_DESCRIPTIONS = {
    'timing': '실제 매수·매도 타이밍 최적화',
    'trend': '단기 추세의 지속/반전 신호 확인',
    'swing': '중기 추세와 파동 구조 파악',
    'regime': '시장 방향성 / 장기 레짐 구분',
    'combined': '모든 역할 통합 분석',
}


def _get_role_analysis(
    market_id: str,
    coin: str,
) -> Dict[str, Any]:
    """종목의 역할별(timing/trend/swing/regime) 시그널 분석"""
    import json as _json

    # [2026-10-07 VALID-001] 시장 정규화 + 종목 커버리지 확인 (signals 공용 검사)
    market_id, _err = normalize_choice("market", "market_id", market_id)
    if _err:
        return _err
    from oneqaz_trading_mcp.symbol_coverage import check_symbol
    _nf = check_symbol(market_id, coin, tool="get_role_analysis")
    if _nf:
        return _nf

    sym_db = get_signal_db_path(market_id, symbol=coin)
    # [2026-08-10] .exists() 게이트 제거 — 위 _get_latest_signals 와 동일한 이유
    if not sym_db:
        return mcp_error(MCPErrorCode.SYMBOL_NOT_FOUND, f"Signal DB not found for {coin} in {market_id}", action=MCPErrorAction.FALLBACK, action_value=f"get_signals(market_id='{market_id}')", fallback_tool=f"market://{market_id}/signals/summary")

    try:
        with connect_readonly(sym_db, timeout=5.0) as conn:

            # 인터벌별 최신 시그널 조회 (per-interval LIMIT 1 — O(N²) correlated subquery 회피)
            intervals = [
                r['interval'] for r in conn.execute(
                    "SELECT DISTINCT interval FROM signals"
                ).fetchall()
            ]
            rows = []
            for iv in intervals:
                r = conn.execute(
                    "SELECT * FROM signals WHERE interval = ? "
                    "ORDER BY timestamp DESC LIMIT 1",
                    (iv,),
                ).fetchone()
                if r is not None:
                    rows.append(r)
            rows.sort(key=lambda row: row['interval'])

            # combined 행에서 hierarchy_context 추출
            # KR/US 및 일부 crypto signal DB 는 hierarchy_context 컬럼이 없음 —
            # 컬럼 존재 여부 확인 후 조건부 SELECT (graceful degrade).
            try:
                cols = {r[1] for r in conn.execute("PRAGMA table_info(signals)").fetchall()}
            except Exception:
                cols = set()
            hierarchy_col = "hierarchy_context" if "hierarchy_context" in cols else "NULL AS hierarchy_context"
            hierarchy_ctx = {}
            combined_row = conn.execute(
                f"SELECT {hierarchy_col}, signal_score, confidence, action, "
                "       current_price, target_price, risk_level, volatility "
                "FROM signals "
                "WHERE interval = 'combined' "
                "ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()

            if combined_row and combined_row['hierarchy_context']:
                try:
                    hierarchy_ctx = _json.loads(combined_row['hierarchy_context'])
                except Exception:
                    pass

            # 인터벌 → 역할 매핑 — 시장 인식형 공용 헬퍼.
            # (종전 env CANDLE_INTERVALS 기반은 kr/us 의 5m/30m 을 'unknown' 처리)
            from oneqaz_trading_mcp.resources.signal_system import _build_role_map
            role_map = _build_role_map(market_id)

            # 역할별 시그널 정리
            roles = {}
            for row in [dict(r) for r in rows]:
                iv = row.get('interval', '')
                if iv == 'combined':
                    continue
                role = role_map.get(iv, 'unknown')
                roles[role] = {
                    'interval': iv,
                    'role': role,
                    'description': _ROLE_DESCRIPTIONS.get(role, ''),
                    'signal_score': round(row.get('signal_score') or 0, 3),
                    'confidence': round(row.get('confidence') or 0, 3),
                    'action': row.get('action'),
                    'direction': _clean_direction(row.get('integrated_direction')),
                    'strength': round(row.get('integrated_strength') or 0, 3),
                    'indicators': {
                        'rsi': round(row['rsi'], 1) if row.get('rsi') else None,
                        'macd': round(row['macd'], 4) if row.get('macd') else None,
                        'adx': round(row['adx'], 1) if row.get('adx') else None,
                    },
                    'wave_phase': row.get('wave_phase'),
                    'risk_level': row.get('risk_level'),
                    'warnings': {
                        'peak_warning': bool(row.get('peak_warning')),
                        'bottom_warning': bool(row.get('bottom_warning')),
                        'momentum_slowing': bool(row.get('momentum_slowing')),
                        'momentum_recovering': bool(row.get('momentum_recovering')),
                    },
                }

            # hierarchy_context에서 통합 메타 정보 추출
            hierarchy_summary = {}
            if hierarchy_ctx:
                hierarchy_summary = {
                    'alignment_score': round(float(hierarchy_ctx.get('alignment_score', 0.5)), 3),
                    'alignment_type': hierarchy_ctx.get('alignment_type', 'unknown'),
                    'position_key': hierarchy_ctx.get('position_key', 'unknown'),
                    'regime_direction': hierarchy_ctx.get('regime_direction', 'neutral'),
                    'overall_progress': round(float(hierarchy_ctx.get('overall_progress', 0.5)), 3),
                    'overall_trend_position': hierarchy_ctx.get('overall_trend_position', 'mid'),
                    'regime_transitioning': bool(hierarchy_ctx.get('regime_transitioning', False)),
                }

            # combined 시그널 정보
            combined_info = {}
            if combined_row:
                combined_info = {
                    'signal_score': round(combined_row['signal_score'] or 0, 3),
                    'confidence': round(combined_row['confidence'] or 0, 3),
                    'action': combined_row['action'],
                    'current_price': combined_row['current_price'],
                    'target_price': combined_row['target_price'],
                    'risk_level': combined_row['risk_level'],
                    'volatility': round(combined_row['volatility'] or 0, 4) if combined_row['volatility'] else None,
                }

            # LLM 요약 생성
            llm_summary = _generate_role_analysis_summary(coin, roles, hierarchy_summary, combined_info)

            return {
                'market_id': market_id,
                'symbol': coin,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'roles': roles,
                'hierarchy': hierarchy_summary,
                'combined': combined_info,
                '_llm_summary': llm_summary,
            }

    except Exception as e:
        logger.error(f"Failed to get role analysis for {coin}: {e}")
        return mcp_error(MCPErrorCode.NO_DATA, str(e), symbol=coin, market_id=market_id)


def _generate_role_analysis_summary(
    coin: str,
    roles: Dict,
    hierarchy: Dict,
    combined: Dict,
) -> str:
    """LLM용 역할별 분석 요약"""
    lines = [f"[{coin} 역할별 시그널 분석]"]

    role_order = ['regime', 'swing', 'trend', 'timing']
    for role_name in role_order:
        r = roles.get(role_name)
        if not r:
            continue
        desc = _ROLE_DESCRIPTIONS.get(role_name, '')
        direction = r.get('direction') or '?'
        score = r.get('signal_score', 0)
        action = r.get('action', 'hold')
        warnings = []
        w = r.get('warnings', {})
        if w.get('peak_warning'):
            warnings.append('고점경고')
        if w.get('bottom_warning'):
            warnings.append('저점신호')
        if w.get('momentum_slowing'):
            warnings.append('모멘텀둔화')
        warn_str = f" [{','.join(warnings)}]" if warnings else ""
        lines.append(f"  {role_name}({r['interval']}): {action} score={score:.2f} dir={direction}{warn_str}")
        lines.append(f"    └ {desc}")

    if hierarchy:
        align = hierarchy.get('alignment_score', 0.5)
        a_type = hierarchy.get('alignment_type', '?')
        pos_key = hierarchy.get('position_key', '?')
        trend_pos = hierarchy.get('overall_trend_position', '?')
        transitioning = hierarchy.get('regime_transitioning', False)
        lines.append(f"  [통합] alignment={align:.2f}({a_type}), 방향조합={pos_key}, 추세위치={trend_pos}" +
                     (", 레짐전환중⚠️" if transitioning else ""))

    if combined:
        lines.append(f"  [최종] score={combined.get('signal_score', 0):.2f}, action={combined.get('action', '?')}, "
                     f"target={combined.get('target_price', 0)}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tool 등록 함수
# ---------------------------------------------------------------------------

def register_signal_tools(mcp, cache):
    """시그널 관련 Tool 등록"""
    # Phase 5+B (2026-05-07): 응답 데이터 기반 _next_actions 추천.
    from oneqaz_trading_mcp.tools.next_actions import (
        for_signals, for_signal_detail, for_role_analysis,
    )
    # Phase D-E (2026-05-07): user-facing followup questions.
    from oneqaz_trading_mcp.tools.followup_questions import (
        fq_signals, fq_signal_detail, fq_role_analysis,
    )

    @mcp.tool(title='Trading Signals', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_signals(
        market_id: Annotated[str, Field(
            description='Market ID (crypto, kr_stock, us_stock; aliases coin/kr/us accepted)', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )],
        symbol: Annotated[Optional[str], Field(
            description='Asset identifier to query (preferred; optional — targets a specific symbol DB)'
        )] = None,
        coin: Annotated[Optional[str], Field(
            description='Legacy alias of symbol (kept for backward compatibility)'
        )] = None,
        interval: Annotated[Optional[str], Field(
            description='Timeframe filter (15m, 30m, 240m, 1d, combined)', json_schema_extra={'enum': ['5m', '15m', '30m', '240m', '1d', 'combined']}
        )] = None,
        action_filter: Annotated[Optional[str], Field(
            description='Action filter (buy, sell, hold)', json_schema_extra={'enum': ['buy', 'sell', 'hold']}
        )] = None,
        min_score: Annotated[Optional[float], Field(
            description='Minimum signal score threshold'
        )] = None,
        min_confidence: Annotated[Optional[float], Field(
            description='Minimum confidence threshold'
        )] = None,
        limit: Annotated[int, Field(
            description='Max results (default 50, max 500; values outside the range are clamped)'
        )] = _SIGNALS_LIMIT_DEFAULT,
        hours_back: Annotated[int, Field(
            description='Only signals within last N hours (default 24)'
        )] = 24,
    ) -> SignalsEnvelope:
        """
        Purpose: Query research signals with dynamic filters (symbol / interval / action / score / confidence).
        Triggers (casual questions too): "should I buy / sell X?", "살까 말까?", "good entry?",
            "what's the signal for BTC / AAPL / 삼성전자?", "is X bullish or bearish?",
            "any buy signals right now?". Returns a research signal + score (NOT an order or advice —
            always surface the disclaimer). Pair with get_latest_decisions to show what the system did.
        When to call: drilling into a specific signal slice; symbol-by-symbol scanning;
            any "should I trade X?" question about a live symbol.
        Prerequisites: market://{market_id}/signals/summary recommended for global view.
        Next steps: get_signal_detail, get_role_analysis.
        Caveats: When `symbol`/`coin` is omitted, the whole market is scanned in one
            consolidated query (2 newest rows per symbol, newest-first scan cap per interval).
            Results are capped: check `truncated` / `truncated_note` before reading the set
            as "the whole market". Fields that are identical across every returned row are
            hoisted into `common` and omitted from the rows (see `common_note`); a row-level
            key, when present, wins over `common`. `warnings` carries only the flags that are
            true and is omitted entirely when none are. `reason_uncalibrated: true` on a row
            points at the response-level `reason_uncalibrated_note`.

        """
        _symbol = symbol or coin
        result = await asyncio.to_thread(
            _get_latest_signals,
            market_id=market_id,
            coin=_symbol,
            interval=interval,
            action_filter=action_filter,
            min_score=min_score,
            min_confidence=min_confidence,
            limit=limit,
            hours_back=hours_back,
        )
        # [2026-08-11-r1] era 주석 — action 라벨 정책(정직화 스테이지) 변경 시 buy/hold
        # 분포가 버전 경계에서 단절되므로, 외부 소비자가 분포 이동을 설명할 수 있도록
        # policy_version 을 응답에 노출한다.
        try:
            from oneqaz_trading_mcp.shared.policy_version import get_policy_version
            if isinstance(result, dict):
                result['policy_version'] = get_policy_version()
        except Exception:
            pass
        return wrap_tool_response(
            result, "signals",
            _ai_summary_signals, _user_summary_signals,
            next_actions_fn=for_signals,
            followup_questions_fn=fq_signals,
        )

    @mcp.tool(title='Signal Detail', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_signal_detail(
        market_id: Annotated[str, Field(
            description='Market ID (crypto, kr_stock, us_stock). Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )],
        coin: Annotated[Optional[str], Field(
            description='Legacy alias of symbol (kept for backward compatibility)'
        )] = None,
        interval: Annotated[str, Field(
            description='Timeframe (default: combined)', json_schema_extra={'enum': ['5m', '15m', '30m', '240m', '1d', 'combined']}
        )] = 'combined',
        symbol: Annotated[Optional[str], Field(
            description='Asset identifier (preferred; e.g., BTC, AAPL)'
        )] = None,
    ) -> SignalDetailEnvelope:
        """
        Purpose: Per-symbol signal deep-dive — latest signal + history + feedback.
        Triggers (casual questions too): "why is BTC a buy?", "그 시그널 근거가 뭐야?",
            "signal history for AAPL?", "이 종목 시그널 자세히 보여줘",
            "how has this signal performed before?".
        When to call: drilling into a single ticker's signal context.
        Prerequisites: confirm existence via get_signals first.
        Next steps: get_role_analysis, get_position_detail.
        Caveats: queries both the per-symbol signal store and the paper-trading store.


        """
        _symbol = symbol or coin
        if not _symbol:
            return mcp_error(
                MCPErrorCode.MISSING_REQUIRED_FIELD,
                "Provide 'symbol' (preferred) or legacy alias 'coin'",
                action=MCPErrorAction.FALLBACK,
                action_value=f"get_signals(market_id='{market_id}')",
                fallback_tool="get_signals",
            )
        result = await asyncio.to_thread(
            _get_coin_signal_detail,
            market_id=market_id,
            coin=_symbol,
            interval=interval,
        )
        return wrap_tool_response(
            result, "signals",
            _ai_summary_signal_detail, _user_summary_signal_detail,
            next_actions_fn=for_signal_detail,
            followup_questions_fn=fq_signal_detail,
        )

    @mcp.tool(title='Interval Role Analysis', annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def get_role_analysis(
        market_id: Annotated[str, Field(
            description='Market ID (crypto, kr_stock, us_stock). Aliases coin/kr/us and any letter case are accepted.', json_schema_extra={'enum': ['crypto', 'kr_stock', 'us_stock']}
        )],
        coin: Annotated[Optional[str], Field(
            description='Legacy alias of symbol (kept for backward compatibility)'
        )] = None,
        symbol: Annotated[Optional[str], Field(
            description='Asset identifier (preferred; e.g., BTC, AAPL)'
        )] = None,
    ) -> RoleAnalysisEnvelope:
        """
        Purpose: Role-aware signal alignment per symbol (timing / trend / swing / regime) plus hierarchy alignment.
        Triggers (casual questions too): "is BTC bullish across timeframes?", "단기랑 장기가 같은 방향이야?",
            "multi-timeframe view for AAPL?", "시간대별 신호가 일치해?", "short-term vs long-term signal?".
        When to call: multi-timeframe analysis, cross-role agreement checks.
        Prerequisites: get_signal_detail recommended.
        Next steps: market://{market_id}/unified/symbol/{symbol}, get_position_detail.
        Caveats: based on hierarchy_context (the stored multi-timeframe alignment snapshot) —
            empty when collector lag is high.


        """
        _symbol = symbol or coin
        if not _symbol:
            return mcp_error(
                MCPErrorCode.MISSING_REQUIRED_FIELD,
                "Provide 'symbol' (preferred) or legacy alias 'coin'",
                action=MCPErrorAction.FALLBACK,
                action_value=f"get_signals(market_id='{market_id}')",
                fallback_tool="get_signals",
            )
        result = await asyncio.to_thread(_get_role_analysis, market_id=market_id, coin=_symbol)
        return wrap_tool_response(
            result, "signals",
            _ai_summary_role_analysis, _user_summary_role_analysis,
            next_actions_fn=for_role_analysis,
            followup_questions_fn=fq_role_analysis,
        )

    logger.info("  [OK] Signal Tools registered (per-symbol DB mode, +role_analysis)")
