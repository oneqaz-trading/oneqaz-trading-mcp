# -*- coding: utf-8 -*-
"""Simple in-memory TTL cache."""

from __future__ import annotations

import time
from typing import Any, Dict, Optional


class SimpleCache:
    """TTL-based in-memory cache."""

    def __init__(self):
        self._cache: Dict[str, tuple[Any, float]] = {}

    def get(self, key: str, ttl: int = 30) -> Optional[Any]:
        """Get a cached value. Returns None if expired or missing."""
        if key not in self._cache:
            return None
        value, timestamp = self._cache[key]
        if time.time() - timestamp > ttl:
            del self._cache[key]
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        """Store a value in the cache."""
        self._cache[key] = (value, time.time())

    def clear(self) -> None:
        """Clear the entire cache."""
        self._cache.clear()
