# -*- coding: utf-8 -*-
"""API-key resolution for self-hosted servers (public package only).

The hosted server checks keys against OneQAZ's private key store
(``api.marketplace.key_store``). This package has no such store, so
``scripts/sync_from_monorepo.py`` rewrites ``from api.marketplace.key_store import …``
to this module. It is never overwritten by a sync.

Behavior:

* ``MCP_TIER_RESOLVER`` unset (default): there is no key store, so a request that
  carries a key is served as **free** — the same as a request without one. The
  server treats :class:`KeyStoreUnavailable` exactly that way and never answers 401.
* ``MCP_TIER_RESOLVER="module:function"``: ``function(api_key)`` decides.
  Return ``"free"``, ``"pro"`` or ``"internal"`` to serve that tier, ``None`` (or an
  empty value) to serve free, or ``"invalid"`` to reject the key with HTTP 401
  like the hosted server does. If the resolver cannot be imported or raises, the
  error is logged and the request is served as free.
"""

from __future__ import annotations

import importlib
import logging
import os
import threading
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Tuple

logger = logging.getLogger("MarketMCP")

_TIERS = ("free", "pro", "internal")
_REJECT = "invalid"

_lock = threading.Lock()
_resolvers: Dict[str, Callable[[str], Optional[str]]] = {}


class KeyStoreUnavailable(RuntimeError):
    """No key store to consult — the caller serves the request as free."""


@dataclass(frozen=True)
class KeyInfo:
    """The subset of the hosted ``KeyInfo`` that the server reads."""

    tier: str
    plan_id: Optional[str] = None

    @property
    def effective_plan(self) -> str:
        return self.plan_id or self.tier


def _load_resolver(path: str) -> Callable[[str], Optional[str]]:
    with _lock:
        fn = _resolvers.get(path)
        if fn is not None:
            return fn
        mod_name, sep, fn_name = path.rpartition(":")
        if not sep or not mod_name or not fn_name:
            raise ValueError(f"MCP_TIER_RESOLVER must look like 'module:function', got {path!r}")
        fn = getattr(importlib.import_module(mod_name), fn_name)
        _resolvers[path] = fn
        return fn


def lookup_key(api_key: str) -> Tuple[str, Optional[KeyInfo]]:
    """``(status, info)`` like the hosted key store: status is ``valid`` or ``unknown``.

    Raises :class:`KeyStoreUnavailable` when no resolver is configured or the
    resolver fails, so the server serves the request as free.
    """
    path = os.environ.get("MCP_TIER_RESOLVER", "").strip()
    if not path:
        raise KeyStoreUnavailable("no key store configured (set MCP_TIER_RESOLVER to validate keys)")
    try:
        tier = _load_resolver(path)(api_key)
    except Exception as exc:  # 사용자 해석기 결함 — 크게 남기고 free 로 계속 (요청은 막지 않는다)
        logger.error("MCP_TIER_RESOLVER %s failed: %s", path, exc, exc_info=True)
        raise KeyStoreUnavailable(str(exc)) from exc
    if not tier:
        return "valid", KeyInfo(tier="free")
    tier = str(tier).strip().lower()
    if tier == _REJECT:
        return "unknown", None
    if tier not in _TIERS:
        logger.warning("MCP_TIER_RESOLVER returned unknown tier %r — serving free", tier)
        return "valid", KeyInfo(tier="free")
    return "valid", KeyInfo(tier=tier)
