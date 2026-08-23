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
        self._lock = threading.Lock()
        self._last_cleanup = time.time()

    # Expose default limits for backward compatibility
    @property
    def daily_limit(self) -> int:
        return TIER_LIMITS["free"]["daily"]

    @property
    def minute_limit(self) -> int:
        return TIER_LIMITS["free"]["minute"]

    def check(self, identity: str, tier: str = "free") -> Tuple[bool, Dict]:
        """
        Check if request is allowed for a given identity and tier.

        Args:
            identity: IP address or API key (tracking unit)
            tier: 'free', 'pro', or 'internal'

        Returns:
            (allowed, info_dict)
        """
        limits = TIER_LIMITS.get(tier, TIER_LIMITS["free"])
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
            }

            # Check minute limit (burst protection)
            if len(rec.minute_counts) >= minute_limit:
                retry_after = int(rec.minute_counts[0] + 60 - now) + 1
                return False, {
                    **base_info,
                    "error": "rate_limit_exceeded",
                    "message": f"Too many requests. Limit: {minute_limit}/minute. Retry after {retry_after}s.",
                    "remaining_daily": max(0, daily_limit - rec.daily_count),
                    "remaining_minute": 0,
                    "retry_after": retry_after,
                    "limit_type": "minute",
                }

            # Check daily limit
            if rec.daily_count >= daily_limit:
                retry_after = int(rec.daily_reset - now) + 1
                hours_left = retry_after // 3600
                upgrade_msg = " Upgrade to Pro for higher limits." if tier == "free" else ""
                return False, {
                    **base_info,
                    "error": "daily_quota_exceeded",
                    "message": (
                        f"Daily quota exhausted ({daily_limit} requests/day). "
                        f"Resets in ~{hours_left}h.{upgrade_msg}"
                    ),
                    "remaining_daily": 0,
                    "remaining_minute": 0,
                    "retry_after": retry_after,
                    "limit_type": "daily",
                }

            # Allow request
            rec.daily_count += 1
            rec.minute_counts.append(now)

            return True, {
                **base_info,
                "remaining_daily": max(0, daily_limit - rec.daily_count),
                "remaining_minute": max(0, minute_limit - len(rec.minute_counts)),
            }

    def get_usage(self, identity: str, tier: str = "free") -> Dict:
        """Get current usage stats for an identity."""
        limits = TIER_LIMITS.get(tier, TIER_LIMITS["free"])
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
                "tier_limits": TIER_LIMITS,
            }

    def _maybe_cleanup(self, now: float):
        """Remove stale records periodically."""
        if now - self._last_cleanup < CLEANUP_INTERVAL:
            return
        self._last_cleanup = now
        stale = [k for k, rec in self._records.items() if now >= rec.daily_reset + 86400]
        for k in stale:
            del self._records[k]


# Singleton instance
rate_limiter = RateLimiter()
