# -*- coding: utf-8 -*-
"""
Rate Limiter
============
Tier-aware rate limiting per identity (API key or IP).

Tiers (2026-04-27 정책 방안 B):
- free:     1,500/day, 60/min  (no API key)
            AI 가 한 질문에 5-9 도구 동시 호출하는 패턴 수용 위해 burst 60.
- pro:      50,000/day, 200/min (valid API key)
            봇/스케줄러용 고빈도 호출.
- internal: unlimited (owner-only, 진짜 사용자 자금 도구가 추가될 때 사용).

도구/리소스 자체는 모두 free 호출 가능. 차등은 호출량만.

[2026-10-16] 두 버킷 (req_mcp_onboarding OD3 · OD4):
- **data**     = tools/call · resources/read · prompts/get — 위 플랜 한도(일·분)를 쓴다.
- **protocol** = initialize · notifications/* · ping · */list · 파싱 불가 본문 등 — 일 한도에서 빠지고
                 분당 PROTOCOL_PER_MINUTE 만 본다. 근거: codex-mcp-client 가 10-07 재연결 중 프로토콜 요청
                 61회/90초로 분당 60 에 걸려 tool 을 한 번도 못 불렀다(30일 외부 분당 최대 57).
- 초과 응답에 결제·가입 유도 문구를 넣지 않는다(공개 베타 — 구 "Upgrade to Pro" 삭제).
"""

from __future__ import annotations

import time
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Tuple

# ---------------------------------------------------------------------------
# Tier configuration
# ---------------------------------------------------------------------------

TIER_LIMITS = {
    "free":     {"daily": 1_500,   "minute": 60},
    "pro":      {"daily": 50_000,  "minute": 200},
    "internal": {"daily": 999_999, "minute": 999},
}

CLEANUP_INTERVAL = 3600  # cleanup stale entries every hour

# [2026-10-16] 프로토콜 버킷 — 분당만, 식별자별. 앱 데이터 분당 한도(free 60)의 5배 = 재연결 폭주만 막는다.
PROTOCOL_PER_MINUTE = 300

# 데이터 버킷에 드는 JSON-RPC 메서드. 미들웨어의 req_type 이 tool/resource 이거나 이름이 아래인 요청.
_DATA_MCP_METHODS = frozenset({"prompts/get"})


def bucket_for(req_type: str, req_name: str) -> str:
    """요청 분류 → 'data' | 'protocol'. tools/call·resources/read·prompts/get 만 data."""
    if req_type in ("tool", "resource") or req_name in _DATA_MCP_METHODS:
        return "data"
    return "protocol"


# 사람·에이전트용 안내 문구(429 본문 data.message) — req 6.2 원문 그대로.
LIMIT_NOTICE = "OneQAZ public beta rate limit reached. Retry after the indicated time."


def _limit_message(scope: str, retry_after: int) -> str:
    what = "daily, resets 00:00 UTC" if scope == "daily" else "per-minute"
    return f"OneQAZ public beta rate limit reached ({what}). Retry after {retry_after} s."

# ---------------------------------------------------------------------------
# [2026-10-07 S2/P1] 집행 정본 = PG api.plans (60초 캐시). 위 TIER_LIMITS 는 **시드·콜드스타트 값**이다.
#   - 종전엔 한도가 이 파일과 key_store.TIER_LIMITS(낡은 Free 30/분) 두 곳에 있었다 — 그 자체가 위험.
#   - 조회 실패: **마지막으로 읽은 값 유지** + ERROR 로그(5분 스로틀). 한 번도 못 읽었으면 위 시드
#     (= 2026-10-07 api.plans seed 와 같은 값) — 무제한 개방도, 침묵도 하지 않는다.
#   - 미지 플랜명은 종전과 같이 free 한도.
# ---------------------------------------------------------------------------
import logging as _logging

_logger = _logging.getLogger("MarketMCP")
_PLANS_TTL_OK = 60.0
_PLANS_TTL_FAIL = 10.0
_PLANS_LOG_INTERVAL = 300.0
_plans_cache: Dict[str, Dict[str, int]] = {}
_plans_state = {"ts": 0.0, "ttl": 0.0, "last_err_log": 0.0, "source": "seed", "refreshing": False}
_plans_lock = threading.Lock()


def _load_plans() -> Dict[str, Dict[str, int]]:
    """api.plans → {plan_id: {"daily", "minute"}} (active 만). 실패는 예외로 올린다."""
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    conn = open_schema_connection("api")
    try:
        rows = conn.execute(
            "SELECT plan_id, daily_limit, minute_limit FROM api.plans WHERE active"
        ).fetchall()
    finally:
        conn.close()
    plans = {r["plan_id"]: {"daily": int(r["daily_limit"]), "minute": int(r["minute_limit"])} for r in rows}
    if "free" not in plans:
        raise ValueError("api.plans 에 active 'free' 플랜이 없다")
    return plans


def _refresh_plans() -> None:
    """[2026-10-11] 한도표 갱신 — 백그라운드 스레드에서만 돈다(요청 경로에서 DB 를 기다리지 않게).

    종전에는 plan_limits() 가 요청 처리 중에 락을 잡은 채 `api.plans` 를 동기로 읽었다. PG 가 막히면
    TTL(실패 10s)마다 한 요청이 풀 getconn 8s 를 기다렸고, 그동안 다른 요청은 락에서 줄을 섰다 —
    MCP 단일 프로세스 이벤트 루프가 사실상 정지(Mac 요청서 #6 과 같은 부류).
    """
    try:
        fresh, err = _load_plans(), None
    except Exception as e:  # 조회 실패 — 마지막 값 유지, 크게 남긴다(명시적 사유)
        fresh, err = None, e
    now = time.time()
    with _plans_lock:
        try:
            if err is None:
                _plans_cache.clear()
                _plans_cache.update(fresh)
                _plans_state.update(ts=now, ttl=_PLANS_TTL_OK, source="db")
            else:
                _plans_state.update(ts=now, ttl=_PLANS_TTL_FAIL)
                if now - _plans_state["last_err_log"] >= _PLANS_LOG_INTERVAL:
                    _plans_state["last_err_log"] = now
                    _logger.error(
                        "[rate_limit] api.plans 조회 실패 — %s 한도 유지: %s",
                        "마지막 캐시" if _plans_cache else "코드 시드(TIER_LIMITS)", err,
                    )
        finally:
            _plans_state["refreshing"] = False


def plan_limits() -> Dict[str, Dict[str, int]]:
    """현재 플랜 한도표 (캐시). 실패해도 예외를 내지 않고 마지막 값을 돌려준다.

    [2026-10-11] 요청 경로는 **절대 DB 를 기다리지 않는다** — TTL 이 지났으면 갱신 스레드를 하나만
    띄우고 지금 가진 값(없으면 코드 시드 TIER_LIMITS)으로 즉시 진행한다. 기동 직후 시드 구간을 없애려면
    서버 기동 때 prime_plan_limits() 를 한 번 부른다(요청 경로 아님).
    """
    now = time.time()
    with _plans_lock:
        if now - _plans_state["ts"] >= _plans_state["ttl"] and not _plans_state["refreshing"]:
            _plans_state["refreshing"] = True
            threading.Thread(target=_refresh_plans, name="plans-refresh", daemon=True).start()
        return _plans_cache or TIER_LIMITS


def prime_plan_limits(timeout_sec: float = 5.0) -> bool:
    """서버 기동 시 1회 — 한도표를 미리 읽어 첫 요청들이 코드 시드로 처리되지 않게 한다.

    DB 에서 읽었으면 True. 실패·시간 초과면 False(시드로 계속 — 갱신은 이후 백그라운드가 재시도).
    """
    plan_limits()
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        with _plans_lock:
            if not _plans_state["refreshing"] and _plans_state["ts"] > 0:
                return _plans_state["source"] == "db"
        time.sleep(0.05)
    return False


def _limits_for(plan: str) -> Dict[str, int]:
    table = plan_limits()
    return table.get(plan) or table.get("free") or TIER_LIMITS["free"]

# ---------------------------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------------------------

@dataclass
class _Record:
    """Track usage for a single identity (IP or API key)."""
    daily_count: int = 0
    daily_reset: float = 0.0
    minute_counts: list = field(default_factory=list)


class RateLimiter:
    """Thread-safe, in-memory, tier-aware rate limiter."""

    def __init__(self):
        self._records: Dict[str, _Record] = defaultdict(_Record)
        self._proto: Dict[str, list] = defaultdict(list)   # 프로토콜 버킷 — 분당 타임스탬프만
        self._lock = threading.Lock()
        self._last_cleanup = time.time()

    # Expose default limits for backward compatibility
    @property
    def daily_limit(self) -> int:
        return _limits_for("free")["daily"]

    @property
    def minute_limit(self) -> int:
        return _limits_for("free")["minute"]

    def check(self, identity: str, tier: str = "free") -> Tuple[bool, Dict]:
        """
        Check if request is allowed for a given identity and tier.

        Args:
            identity: IP address or API key (tracking unit)
            tier: 'free', 'pro', or 'internal'

        Returns:
            (allowed, info_dict)
        """
        limits = _limits_for(tier)
        daily_limit = limits["daily"]
        minute_limit = limits["minute"]
        now = time.time()

        with self._lock:
            self._maybe_cleanup(now)
            rec = self._records[identity]

            # Reset daily counter
            if rec.daily_reset == 0.0 or now >= rec.daily_reset:
                rec.daily_count = 0
                rec.daily_reset = now + 86400 - (now % 86400)

            # Clean minute window
            cutoff = now - 60
            rec.minute_counts = [t for t in rec.minute_counts if t > cutoff]

            base_info = {
                "tier": tier,
                "daily_limit": daily_limit,
                "minute_limit": minute_limit,
                "bucket": "data",
            }

            # Check minute limit (burst protection)
            if len(rec.minute_counts) >= minute_limit:
                retry_after = int(rec.minute_counts[0] + 60 - now) + 1
                return False, {
                    **base_info,
                    "error": "rate_limit_exceeded",
                    "message": _limit_message("minute", retry_after),
                    "remaining_daily": max(0, daily_limit - rec.daily_count),
                    "remaining_minute": 0,
                    "retry_after": retry_after,
                    "limit_type": "minute",
                    "scope": "minute",
                    "limit": {"window": "1m", "max_calls": minute_limit, "used": len(rec.minute_counts)},
                }

            # Check daily limit
            if rec.daily_count >= daily_limit:
                retry_after = int(rec.daily_reset - now) + 1
                return False, {
                    **base_info,
                    "error": "daily_quota_exceeded",
                    "message": _limit_message("daily", retry_after),
                    "remaining_daily": 0,
                    "remaining_minute": 0,
                    "retry_after": retry_after,
                    "limit_type": "daily",
                    "scope": "daily",
                    "limit": {"window": "1d", "max_calls": daily_limit, "used": rec.daily_count},
                }

            # Allow request
            rec.daily_count += 1
            rec.minute_counts.append(now)

            return True, {
                **base_info,
                "remaining_daily": max(0, daily_limit - rec.daily_count),
                "remaining_minute": max(0, minute_limit - len(rec.minute_counts)),
            }

    def check_protocol(self, identity: str, tier: str = "free") -> Tuple[bool, Dict]:
        """프로토콜 버킷 검사(일 한도 불소모). 응답 헤더용 데이터 버킷 잔여는 **소모 없이** 같이 돌려준다."""
        limits = _limits_for(tier)
        now = time.time()
        with self._lock:
            self._maybe_cleanup(now)
            stamps = [t for t in self._proto[identity] if t > now - 60]
            self._proto[identity] = stamps
            rec = self._records.get(identity)
            if rec is not None and rec.daily_reset and now < rec.daily_reset:
                daily_used = rec.daily_count
            else:
                daily_used = 0
            minute_used = len([t for t in rec.minute_counts if t > now - 60]) if rec is not None else 0
            info = {
                "tier": tier,
                "daily_limit": limits["daily"],
                "minute_limit": limits["minute"],
                "bucket": "protocol",
                "remaining_daily": max(0, limits["daily"] - daily_used),
                "remaining_minute": max(0, limits["minute"] - minute_used),
            }
            if len(stamps) >= PROTOCOL_PER_MINUTE:
                retry_after = int(stamps[0] + 60 - now) + 1
                return False, {
                    **info,
                    "error": "rate_limit_exceeded",
                    "message": _limit_message("minute", retry_after),
                    "retry_after": retry_after,
                    "limit_type": "minute",
                    "scope": "minute",
                    "limit": {"window": "1m", "max_calls": PROTOCOL_PER_MINUTE, "used": len(stamps)},
                }
            stamps.append(now)
            return True, info

    def get_usage(self, identity: str, tier: str = "free") -> Dict:
        """Get current usage stats for an identity."""
        limits = _limits_for(tier)
        now = time.time()
        with self._lock:
            rec = self._records.get(identity)
            if not rec:
                return {
                    "tier": tier,
                    "daily_used": 0,
                    "daily_limit": limits["daily"],
                    "minute_used": 0,
                    "minute_limit": limits["minute"],
                }
            cutoff = now - 60
            minute_count = len([t for t in rec.minute_counts if t > cutoff])
            return {
                "tier": tier,
                "daily_used": rec.daily_count,
                "daily_limit": limits["daily"],
                "daily_remaining": max(0, limits["daily"] - rec.daily_count),
                "minute_used": minute_count,
                "minute_limit": limits["minute"],
                "minute_remaining": max(0, limits["minute"] - minute_count),
            }

    def get_all_stats(self) -> Dict:
        """Get aggregate stats for admin dashboard."""
        now = time.time()
        with self._lock:
            active_ips = 0
            total_daily = 0
            for identity, rec in self._records.items():
                if rec.daily_count > 0 and now < rec.daily_reset:
                    active_ips += 1
                    total_daily += rec.daily_count
            return {
                "active_identities": active_ips,
                "total_requests_today": total_daily,
                "tier_limits": plan_limits(),
                "tier_limits_source": _plans_state["source"],
            }

    def _maybe_cleanup(self, now: float):
        """Remove stale records periodically."""
        if now - self._last_cleanup < CLEANUP_INTERVAL:
            return
        self._last_cleanup = now
        stale = [k for k, rec in self._records.items() if now >= rec.daily_reset + 86400]
        for k in stale:
            del self._records[k]
        for k in [k for k, ts in self._proto.items() if not ts or ts[-1] <= now - 60]:
            del self._proto[k]


def rate_limit_error_body(rpc_id, info: Dict) -> Dict:
    """429 본문 (req_mcp_onboarding 6.2 / OD4) — JSON-RPC 오류 봉투 안 `error.data` 에 구조화 필드.

    - `id` 는 요청 id 를 반사한다(파싱 불가·알림이면 null).
    - 구 필드(tier·daily_limit·minute_limit·remaining_*·retry_after·limit_type)는 한 마이너 동안 병기.
      구 `error` 값(rate_limit_exceeded / daily_quota_exceeded)은 `legacy_error` 로 옮겼다 —
      `error` 는 req 6.2 대로 'rate_limited' 고정.
    - 결제·가입 유도 문구 없음. 면책은 compliance 짧은 문구(단일 소스).
    """
    from oneqaz_trading_mcp.compliance import DISCLAIMER_SHORT_EN

    retry_after = int(info.get("retry_after", 60))
    return {
        "jsonrpc": "2.0",
        "id": rpc_id,
        "error": {
            "code": -32000,
            "message": info.get("message") or _limit_message(info.get("scope", "minute"), retry_after),
            "data": {
                "error": "rate_limited",
                "scope": info.get("scope") or info.get("limit_type"),
                "bucket": info.get("bucket", "data"),
                "retry_after_sec": retry_after,
                "limit": info.get("limit"),
                "message": LIMIT_NOTICE,
                "disclaimer": DISCLAIMER_SHORT_EN,
                "is_investment_advice": False,
                "data_classification": "research_information_only",
                # ── 구 필드 (한 마이너 동안 병기) ──
                "legacy_error": info.get("error"),
                "tier": info.get("tier"),
                "daily_limit": info.get("daily_limit"),
                "minute_limit": info.get("minute_limit"),
                "remaining_daily": info.get("remaining_daily"),
                "remaining_minute": info.get("remaining_minute"),
                "retry_after": retry_after,
                "limit_type": info.get("limit_type"),
            },
        },
    }


# Singleton instance
rate_limiter = RateLimiter()
