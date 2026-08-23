"""캔들 인터벌 상수 + 역할 파서 — 수집기/분석기/러너 공용.

SQLite → PG 이관 이전부터 여러 모듈이 동일한 상수(INTERVAL_SECONDS, DEFAULT_DAYS)
와 환경 변수 파싱(INTERVAL_ROLES) 을 중복 정의하고 있었다. 이관 후 리팩토링에서
한 곳으로 모음.

사용처:
- `data_collection.global_collector`: INTERVAL_SECONDS, DEFAULT_DAYS
- `data_collection.structure_collector`: 위와 동일 (global 에서 re-export 중)
- `market.global_regime.analyzer`: INTERVAL_WEIGHTS + parse_interval_roles
- `market.market_structure.runner`: parse_interval_roles
- 4 collector(krx/us/coin candles_calculate/global): MARKET_INTERVALS 단일 source

[Phase 0 / 2026-04-28] MARKET_INTERVALS / get_market_intervals 추가:
3시장 매매 엔진(coin/kr/us)이 사용하는 interval set 을 단일 dict 로 중앙화.
이전엔 4 collector 에 hardcoded ('15m,30m,240m,1d' / '5m,15m,30m,1d') 분산.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List

# ---------------------------------------------------------------------------
# 캔들 인터벌 → 초 단위 (UPSERT gap 탐지 및 증분 안전장치 계산용)
#
# [2026-05-21] coin/kr/us collector 가 각자 하드코딩하던 INTERVAL_SECONDS 를
# 여기로 통합. 분봉(5m~240m) 포함 — 3 collector 의 합집합. 값 불일치 위험 제거.
# ---------------------------------------------------------------------------
INTERVAL_SECONDS: Dict[str, int] = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "60m": 3600,
    "1h": 3600,
    "240m": 14400,
    "4h": 14400,
    "1d": 86400,
    "1w": 604800,
    "1M": 2592000,
}

# ---------------------------------------------------------------------------
# 증분 수집시 기본 조회 기간 (일). 인터벌별 주기를 감안한 보수적 기본값.
# env 의 DAYS_BACK_* 로 오버라이드 가능 — 이 상수는 최후 fallback.
# ---------------------------------------------------------------------------
DEFAULT_DAYS: Dict[str, int] = {
    "1d": 730,
    "1w": 1040,
    "1M": 3650,
}

# ---------------------------------------------------------------------------
# 멀티타임프레임 가중치 — 분석기 집계 기본값.
# 1d 노이즈 많음(0.5), 1w 기준(1.0), 1M 추세 반영(1.5).
# ---------------------------------------------------------------------------
INTERVAL_WEIGHTS: Dict[str, float] = {
    "1d": 0.5,
    "1w": 1.0,
    "1M": 1.5,
}
DEFAULT_INTERVAL_WEIGHT: float = 1.0


def parse_interval_roles(
    env_value: str = "",
    default: str = "1d:timing,1w:swing,1M:regime",
) -> Dict[str, str]:
    """'1d:timing,1w:swing,1M:regime' 형식 → dict.

    env_value 가 빈 문자열/공백이면 `default` 를 사용.
    """
    raw = env_value.strip() if env_value else default
    if not raw:
        raw = default
    roles: Dict[str, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" in pair:
            k, v = pair.split(":", 1)
            roles[k.strip()] = v.strip()
    return roles


# ============================================================================
# MARKET_INTERVALS — 3시장 매매 엔진의 interval set (Phase 0 / 2026-04-28)
# ============================================================================
# 시장별 interval 차이가 PG role_interval_map 시드와 정확히 일치해야 한다.
# 이 dict 는 4 collector(krx/us/coin/global) 와 trade/strategy_signal_generator
# 의 fallback 경로에서 단일 source 로 import.
#
# 정합성 보증:
#   - role_aware_v2_seed.sql:16-30 의 (market_type, interval) 쌍과 1:1 매칭
#   - data_collection/core/interval_policy.py:75-106 의 INTERVAL_POLICY 키와 일치
#   - 누락 시 검증 테스트 (validate_market_intervals) 가 차단

MARKET_INTERVALS: Dict[str, List[str]] = {
    # 24/7 시장 (Bithumb): 240m 까지 길게 → swing 학습용
    "coin":   ["15m", "30m", "240m", "1d"],
    # 세션 기반 (yfinance+KIS): 5m 추가 → 갭 직후 타이밍
    "kr":     ["5m", "15m", "30m", "1d"],
    # 세션 기반 (yfinance): kr 와 동일 set
    "us":     ["5m", "15m", "30m", "1d"],
    # 매크로/구조: 일/주/월
    "global":      ["1d", "1w", "1M"],
    # market_structure (ETF/basket) — schema.py / candles_pg_writer 와 동일 키 사용.
    # candles_calculate 가 MARKET_ID='coin_struct' 등을 그대로 넘기므로 명시 등록 필요.
    "coin_struct": ["1d", "1w", "1M"],
    "kr_struct":   ["1d", "1w", "1M"],
    "us_struct":   ["1d", "1w", "1M"],
    # legacy alias — 호출 코드가 일반화된 'struct' 만 들고 있을 때 대비. 신규 코드는 *_struct 사용.
    "struct":      ["1d", "1w", "1M"],
}


def get_market_intervals(market_id: str) -> List[str]:
    """시장별 interval 목록 반환. env CANDLE_INTERVALS 가 있으면 우선.

    Args:
        market_id: 'coin' | 'kr' | 'us' | 'global' | 'struct'

    Returns:
        ['15m', '30m', ...] 형태 list. env override 시 그 값 사용.

    Raises:
        KeyError: 등록되지 않은 market_id.
    """
    if market_id not in MARKET_INTERVALS:
        raise KeyError(
            f"unknown market_id={market_id!r}. registered: {sorted(MARKET_INTERVALS)}"
        )
    env_raw = os.environ.get("CANDLE_INTERVALS", "").strip()
    if env_raw:
        return [iv.strip() for iv in env_raw.split(",") if iv.strip()]
    return list(MARKET_INTERVALS[market_id])


def get_market_type_for_pg(market_id: str) -> str:
    """data_collection 의 market_id 를 rl_pipeline.role_interval_map 의 market_type 으로 변환.

    Args:
        market_id: 'coin' | 'kr' | 'us'

    Returns:
        'COIN' | 'KR_STOCK' | 'US_STOCK'

    Raises:
        ValueError: 매핑 불가능한 market_id.
    """
    mapping = {"coin": "COIN", "kr": "KR_STOCK", "us": "US_STOCK"}
    if market_id not in mapping:
        raise ValueError(
            f"market_id={market_id!r} 는 PG market_type 매핑 없음. "
            f"매매 엔진 시장(coin/kr/us)만 지원."
        )
    return mapping[market_id]


# ============================================================================
# 인터벌 → 역할 강제 lookup (PG role_interval_map 단일 source of truth)
# ============================================================================
# [정책 — 2026-05-10] 모든 파이프라인(캔들/시그널/전략/페이퍼) 이 이 헬퍼만 사용.
# - 휴리스틱 (분 단위 정렬해서 자동 분할) 금지.
# - PG 매핑이 없으면 RuntimeError — 조용한 fallback 금지.
# - 결과는 프로세스 lifetime 캐시 (rl_pipeline.role_indicator_matrix 가 관리).

def resolve_role_strict(market: str, interval: str) -> str:
    """시장×인터벌 → 역할 (timing|trend|swing|regime) 을 PG seed 기반으로 반환.

    Args:
        market: 'coin' | 'kr' | 'us' (또는 'COIN'/'KR_STOCK'/'US_STOCK' 직접)
        interval: '1d' | '240m' | '30m' | '15m' | '5m'

    Returns:
        역할 문자열.

    Raises:
        ValueError: 등록되지 않은 시장/인터벌 (시드와 어긋남 = 운영 중 silent
            mismatch 방지). caller 가 try/except 로 휴리스틱 폴백을 짜면 의미가
            없으므로, 호출자도 예외를 그대로 전파해야 함.
    """
    # market 정규화 — 'coin' / 'kr' / 'us' → 'COIN' / 'KR_STOCK' / 'US_STOCK'
    m = (market or "").strip()
    upper = m.upper()
    if upper in ("COIN", "KR_STOCK", "US_STOCK"):
        market_type = upper
    else:
        try:
            market_type = get_market_type_for_pg(m.lower())
        except ValueError as e:
            raise ValueError(
                f"resolve_role_strict: unknown market={market!r}. "
                f"매매 엔진 시장(coin/kr/us)만 지원."
            ) from e

    try:
        from rl_pipeline.core.role_indicator_matrix import get_role_for_interval
    except Exception as e:
        raise RuntimeError(
            f"resolve_role_strict: role_indicator_matrix import 실패 — {e}. "
            "rl_pipeline 설치 또는 PG 연결 확인."
        ) from e

    role = get_role_for_interval(market_type, interval)
    if role is None:
        raise ValueError(
            f"resolve_role_strict: ({market_type}, {interval}) 매핑 없음. "
            "rl_pipeline.role_interval_map 시드 확인 필요 "
            "(migrations/role_aware_v2_seed.sql)."
        )
    return role


def resolve_market_role_map(market: str) -> Dict[str, str]:
    """특정 시장의 전체 interval→role 매핑 반환 (PG seed 기반).

    Returns:
        {"1d": "regime", "240m": "swing", "30m": "trend", "15m": "timing"} (COIN 예시)

    Raises:
        ValueError: 등록되지 않은 시장 또는 시드가 비었을 때.
    """
    m = (market or "").strip()
    upper = m.upper()
    if upper in ("COIN", "KR_STOCK", "US_STOCK"):
        market_type = upper
    else:
        market_type = get_market_type_for_pg(m.lower())

    try:
        from rl_pipeline.core.role_indicator_matrix import get_all_interval_roles_for_market
    except Exception as e:
        raise RuntimeError(
            f"resolve_market_role_map: role_indicator_matrix import 실패 — {e}"
        ) from e

    mapping = get_all_interval_roles_for_market(market_type) or {}
    if not mapping:
        raise ValueError(
            f"resolve_market_role_map: market_type={market_type} 에 대한 매핑 없음 "
            "(role_interval_map 시드 누락)."
        )
    return mapping


def validate_market_intervals() -> Dict[str, List[str]]:
    """MARKET_INTERVALS 와 PG role_interval_map / INTERVAL_POLICY 정합성 검증.

    Returns:
        {market_id: [error_msg, ...]} — 빈 dict 면 모두 정합.
        에러 키:
          - 'pg_missing': PG 에 매핑이 없는 (market, interval) 쌍
          - 'policy_missing': INTERVAL_POLICY 에 없는 (market, interval) 쌍
          - 'pg_extra': PG 에는 있는데 MARKET_INTERVALS 에 없는 interval
    """
    errors: Dict[str, List[str]] = {}

    # PG seed 로드
    try:
        from rl_pipeline.core.role_indicator_matrix import get_role_for_interval
    except Exception as e:
        return {"_import_error": [str(e)]}

    for market_id in ("coin", "kr", "us"):
        market_type = get_market_type_for_pg(market_id)
        market_errors: List[str] = []
        for iv in MARKET_INTERVALS[market_id]:
            role = get_role_for_interval(market_type, iv)
            if role is None:
                market_errors.append(
                    f"pg_missing: ({market_type}, {iv}) → role 매핑 없음"
                )
        # INTERVAL_POLICY 정합
        try:
            from data_collection.core.interval_policy import get_policy
            for iv in MARKET_INTERVALS[market_id]:
                try:
                    get_policy(market_id, iv)
                except KeyError as e:
                    market_errors.append(f"policy_missing: {e}")
        except ImportError:
            pass

        if market_errors:
            errors[market_id] = market_errors

    return errors


# ============================================================================
# 시장 시간대 "오늘 자정" epoch — 휴장 갭 클램프 (2026-05-21)
# ============================================================================
# coin/kr/us collector 가 분봉 catch-up 폭주를 막으려고 "오늘 (시장 TZ) 00:00"
# 의 UTC epoch 를 계산해 start_ts 를 클램프한다. us 는 zoneinfo→pytz→UTC 3중
# 폴백 블록을 두 곳에 복붙하고 있었다. 한 군데로 모음.


def today_kst_midnight_utc_ts() -> int:
    """KST '오늘 00:00' 을 UTC epoch seconds 로 반환.

    KST 는 DST 가 없는 고정 오프셋(UTC+9) 이라 zoneinfo 없이 정확히 계산
    가능 — 폴백 불필요. krx_collector 원본 동작과 동일.
    """
    kst = timezone(timedelta(hours=9))
    midnight = datetime.now(kst).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(midnight.astimezone(timezone.utc).timestamp())


def today_et_midnight_utc_ts() -> int:
    """US/Eastern '오늘 00:00' 을 UTC epoch seconds 로 반환.

    ET 는 DST 가 있어 IANA TZ DB 가 필요하다. zoneinfo → pytz → UTC(자정)
    순서로 폴백 — us_collector 원본의 3중 폴백을 그대로 옮긴 것. zoneinfo·pytz
    둘 다 실패하면 UTC 자정으로 떨어진다(더 이른 시각 = 클램프가 덜 공격적,
    보수적 안전 측).
    """
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("US/Eastern")
        midnight = datetime.now(et).replace(hour=0, minute=0, second=0, microsecond=0)
        return int(midnight.astimezone(timezone.utc).timestamp())
    except Exception:
        pass
    try:
        import pytz
        et = pytz.timezone("US/Eastern")
        midnight = datetime.now(et).replace(hour=0, minute=0, second=0, microsecond=0)
        return int(midnight.astimezone(timezone.utc).timestamp())
    except Exception:
        pass
    utc_midnight = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0)
    return int(utc_midnight.timestamp())
