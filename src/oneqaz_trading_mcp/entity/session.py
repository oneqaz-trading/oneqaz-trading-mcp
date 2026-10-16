# -*- coding: utf-8 -*-
"""
세션 상태·기준(basis) 판정 — 엔티티 응답의 meta.session_state / headline.basis
================================================================================
정책 정본은 `data_collection/market_hours.py`(KRX 09:00~15:30 KST · NYSE 09:30~16:00 ET, 주말·공휴일 휴장).
그 함수를 직접 부르지 않는 이유: `check_krx_market_hours()` 는 평일 09:30 이후 공휴일 판정을 위해
pykrx 로 **외부 네트워크**를 부른다(`is_kr_holiday_today`). 응답 경로가 외부 API 를 기다리면 안 되므로
같은 시간표 + `holidays` 달력(market_hours 가 먼저 보는 것과 같은 달력)만 쓴다. 임시 휴장은 못 잡지만,
basis 는 달력이 아니라 **데이터 시각**으로 판정하므로 임시 휴장에도 "전 세션 기준" 이 정직하게 나온다.
"""

from __future__ import annotations

import logging
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Optional

logger = logging.getLogger("MarketMCP")

_KST = timezone(timedelta(hours=9))

# market_hours.py 의 기본 시간표와 같은 값 (env 재정의 KRX_MARKET_START 등은 수집기 검증용이라 따르지 않는다)
_KR_OPEN, _KR_CLOSE = dtime(9, 0), dtime(15, 30)
_US_OPEN, _US_CLOSE = dtime(9, 30), dtime(16, 0)

# 장중 데이터로 볼 최대 나이(초). timing 인터벌 kr·us 5m · coin 15m 의 사이클이 늦어도 이 안에 든다
# (Agent-Native 계약 §5 stale_budget = 2 × timing: kr·us 10분 · coin 30분).
_INTRADAY_MAX_AGE = {"crypto": 1800, "kr_stock": 600, "us_stock": 600}


def _us_eastern():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/New_York")
    except Exception:  # tzdata 부재 — pytz 로
        import pytz
        return pytz.timezone("US/Eastern")


def _is_holiday(market: str, d) -> bool:
    try:
        import holidays as _h
    except ImportError:
        logger.warning("entity.session: holidays 패키지 없음 — 평일·시간만으로 판정")
        return False
    try:
        if market == "kr_stock":
            return d in _h.KR(years=d.year) or (d.month, d.day) in ((5, 1), (12, 31))
        if market == "us_stock":
            return d in _h.financial_holidays("NYSE", years=d.year)
    except Exception as e:  # 달력 조회 실패는 시간표 판정으로 계속 (경고는 남긴다)
        logger.warning("entity.session: %s 휴장 달력 조회 실패: %s", market, e)
    return False


def session_state(market: str, now: Optional[datetime] = None) -> str:
    """'always_open' (crypto) | 'open' | 'closed'."""
    if market == "crypto":
        return "always_open"
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if market == "kr_stock":
        local = now_utc.astimezone(_KST)
        lo, hi = _KR_OPEN, _KR_CLOSE
    elif market == "us_stock":
        local = now_utc.astimezone(_us_eastern())
        lo, hi = _US_OPEN, _US_CLOSE
    else:
        return "closed"
    if local.weekday() >= 5 or _is_holiday(market, local.date()):
        return "closed"
    return "open" if lo <= local.time() <= hi else "closed"


def data_basis(market: str, data_ts: Optional[float], session: str,
               now_ts: Optional[float] = None) -> str:
    """데이터가 무엇을 기준으로 한 값인가.

    - intraday     : 세션 중이고 데이터가 최근 사이클(_INTRADAY_MAX_AGE 안)
    - last_session : 세션이 닫혀 있다 — 마지막 정규장(또는 마지막 사이클) 기준
    - delayed      : 세션 중인데 데이터가 사이클보다 오래됐다 (수집·시그널 지연)
    - unknown      : 데이터 시각 없음
    """
    if data_ts is None:
        return "unknown"
    now_ts = now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp()
    age = now_ts - float(data_ts)
    if session == "closed":
        return "last_session"
    return "intraday" if age <= _INTRADAY_MAX_AGE.get(market, 1800) else "delayed"


def structure_basis(market: str, updated_ts: Optional[float], session: str,
                    now: Optional[datetime] = None) -> str:
    """구조(그룹) 산출물의 기준 — 일봉 기반이라 데이터 나이가 아니라 '어느 세션의 봉인가' 로 판정한다.

    - last_session : 세션이 닫혀 있거나, 세션 중이라도 오늘 개장 전에 계산된 값
    - intraday     : 오늘 세션 중 계산 — 진행 중인 일봉을 포함한다 (crypto 는 항상)
    """
    if updated_ts is None:
        return "unknown"
    if market == "crypto":
        return "intraday"
    if session == "closed":
        return "last_session"
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if market == "kr_stock":
        local = now_utc.astimezone(_KST)
        opened = local.replace(hour=_KR_OPEN.hour, minute=_KR_OPEN.minute, second=0, microsecond=0)
    else:
        local = now_utc.astimezone(_us_eastern())
        opened = local.replace(hour=_US_OPEN.hour, minute=_US_OPEN.minute, second=0, microsecond=0)
    return "intraday" if float(updated_ts) >= opened.timestamp() else "last_session"


def iso_utc(ts: Optional[float]) -> Optional[str]:
    """epoch 초 → ISO8601 UTC('Z'). None 은 None."""
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def iso_from_any(v) -> Optional[str]:
    """datetime / epoch / ISO 문자열 → ISO8601 UTC. 해석 불가면 None.

    ⚠️ PG timestamptz 는 psycopg 가 datetime 으로 준다 — `.replace("Z", ...)` 문자열 처리 금지
    (CLAUDE.md 조용한 사망 패턴 ①).
    """
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(v, (int, float)):
        return iso_utc(v)
    if isinstance(v, str):
        s = v.strip()
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None
        return iso_from_any(dt)
    return None


def epoch_from_any(v) -> Optional[float]:
    iso = iso_from_any(v)
    if iso is None:
        return None
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


def kst_label(ts: Optional[float]) -> str:
    """headline 용 'MM-DD HH:MM KST'."""
    if ts is None:
        return "unknown time"
    try:
        return datetime.fromtimestamp(float(ts), tz=_KST).strftime("%m-%d %H:%M KST")
    except (TypeError, ValueError, OverflowError, OSError):
        return "unknown time"
