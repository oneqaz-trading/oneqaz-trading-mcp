# -*- coding: utf-8 -*-
"""
Rate Limiter
============
Daily quota-based rate limiting per IP.

Free tier limits:
- 1,000 requests/day per IP
- 60 requests/minute per IP (burst protection)

Designed so users can choose how to spend their daily quota:
- Monitor 2-3 symbols all day, OR
- Scan many symbols in a few bursts

Future paid tiers can increase or remove limits.
"""

from __future__ import annotations

import time
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Tuple

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DAILY_LIMIT = 1_500         # requests per IP per day (~150K total / 100 users)
MINUTE_LIMIT = 30           # requests per IP per minute (burst protection)
CLEANUP_INTERVAL = 3600     # cleanup stale entries every hour

# ---------------------------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------------------------

@dataclass
class _IPRecord:
    """Track usage for a single IP."""
    daily_count: int = 0
    daily_reset: float = 0.0      # timestamp when daily count resets
    minute_counts: list = field(default_factory=list)  # list of timestamps


class RateLimiter:
    """Thread-safe, in-memory rate limiter."""

    def __init__(
        self,
        daily_limit: int = DAILY_LIMIT,
        minute_limit: int = MINUTE_LIMIT,
    ):
        self.daily_limit = daily_limit
        self.minute_limit = minute_limit
        self._records: Dict[str, _IPRecord] = defaultdict(_IPRecord)
        self._lock = threading.Lock()
        self._last_cleanup = time.time()

    def check(self, ip: str) -> Tuple[bool, Dict]:
        """
        Check if request is allowed.

        Returns:
            (allowed, info_dict)
            info_dict contains: remaining_daily, remaining_minute, retry_after
        """
        now = time.time()

        with self._lock:
            self._maybe_cleanup(now)
            rec = self._records[ip]

            # Reset daily counter at midnight (every 24h from first request)
            if rec.daily_reset == 0.0 or now >= rec.daily_reset:
                rec.daily_count = 0
                # Next reset: start of next UTC day
                rec.daily_reset = now + 86400 - (now % 86400)

            # Clean minute window
            cutoff = now - 60
            rec.minute_counts = [t for t in rec.minute_counts if t > cutoff]

            # Check minute limit (burst protection)
            if len(rec.minute_counts) >= self.minute_limit:
                retry_after = int(rec.minute_counts[0] + 60 - now) + 1
                return False, {
                    "error": "rate_limit_exceeded",
                    "message": f"Too many requests. Limit: {self.minute_limit}/minute. Retry after {retry_after}s.",
                    "remaining_daily": max(0, self.daily_limit - rec.daily_count),
                    "remaining_minute": 0,
                    "retry_after": retry_after,
                    "limit_type": "minute",
                }

            # Check daily limit
            if rec.daily_count >= self.daily_limit:
                retry_after = int(rec.daily_reset - now) + 1
                hours_left = retry_after // 3600
                return False, {
                    "error": "daily_quota_exceeded",
                    "message": (
                        f"Daily quota exhausted ({self.daily_limit} requests/day). "
                        f"Resets in ~{hours_left}h. "
                        f"Upgrade to Pro for higher limits."
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
                "remaining_daily": max(0, self.daily_limit - rec.daily_count),
                "remaining_minute": max(0, self.minute_limit - len(rec.minute_counts)),
            }

    def get_usage(self, ip: str) -> Dict:
        """Get current usage stats for an IP."""
        now = time.time()
        with self._lock:
            rec = self._records.get(ip)
            if not rec:
                return {
                    "daily_used": 0,
                    "daily_limit": self.daily_limit,
                    "minute_used": 0,
                    "minute_limit": self.minute_limit,
                }
            cutoff = now - 60
            minute_count = len([t for t in rec.minute_counts if t > cutoff])
            return {
                "daily_used": rec.daily_count,
                "daily_limit": self.daily_limit,
                "daily_remaining": max(0, self.daily_limit - rec.daily_count),
                "minute_used": minute_count,
                "minute_limit": self.minute_limit,
                "minute_remaining": max(0, self.minute_limit - minute_count),
            }

    def get_all_stats(self) -> Dict:
        """Get aggregate stats for admin dashboard."""
        now = time.time()
        with self._lock:
            active_ips = 0
            total_daily = 0
            for ip, rec in self._records.items():
                if rec.daily_count > 0 and now < rec.daily_reset:
                    active_ips += 1
                    total_daily += rec.daily_count
            return {
                "active_ips": active_ips,
                "total_requests_today": total_daily,
                "daily_limit_per_ip": self.daily_limit,
                "minute_limit_per_ip": self.minute_limit,
            }

    def _maybe_cleanup(self, now: float):
        """Remove stale IP records periodically."""
        if now - self._last_cleanup < CLEANUP_INTERVAL:
            return
        self._last_cleanup = now
        stale = [ip for ip, rec in self._records.items() if now >= rec.daily_reset + 86400]
        for ip in stale:
            del self._records[ip]


# Singleton instance
rate_limiter = RateLimiter()
