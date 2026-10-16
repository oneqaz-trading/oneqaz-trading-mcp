# -*- coding: utf-8 -*-
"""계정 상태 tool — OAuth P3a 시험 대상 (2026-10-08).

`get_account_status` 는 **데이터 가치 0**인 보호 tool 이다(설계 §6 P3a): 연결된 계정(또는 API 키)의 플랜·한도만 돌려준다.
무자격 호출은 tool 까지 오지 않는다 — RateLimitMiddleware 가 401 + PRM 포인터로 막고, Claude 는 그 401 을 보고
"계정 연결" 카드를 띄운다(lazy auth). MCP_OAUTH_MODE=on 일 때만 등록된다(그 외엔 tools/list 불변 39개).
"""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import urlsplit

logger = logging.getLogger("MarketMCP")


def _principal_from_request() -> dict:
    from fastmcp.server.dependencies import get_http_request

    from oneqaz_trading_mcp.auth_principal import classify_credential, legacy_api_key, resolve_oauth_token
    from oneqaz_trading_mcp.rate_limiter import rate_limiter

    headers = get_http_request().headers
    cred = classify_credential(headers)
    if cred.kind == "oauth":
        st, p = resolve_oauth_token(cred.secret)
        if st == "valid" and p is not None:
            usage = rate_limiter.get_usage(f"acct:{p.account_id}", tier=p.plan)
            host = urlsplit(p.oauth_client_id).hostname if p.oauth_client_id.startswith("https://") else None
            return {"principal": "oauth", "account_id": p.account_id, "plan": p.plan,
                    "connected_app": {"client_id": p.oauth_client_id, "host": host},
                    "granted_scopes": list(p.scopes), "access_token_expires_at": int(p.expires_at), "usage": usage}
    key = legacy_api_key(headers)
    if key:
        from oneqaz_trading_mcp.self_host_auth import lookup_key

        st, info = lookup_key(key)
        if st == "valid" and info is not None:
            from oneqaz_trading_mcp.analytics import api_key_client_id

            plan = info.effective_plan
            if info.tier != "free":
                usage = rate_limiter.get_usage(key, tier=plan)
            else:  # free 키는 한도를 IP 로 센다 — 여기선 IP 별 사용량을 모르므로 한도만(0 사용으로 오인 금지)
                u = rate_limiter.get_usage("", tier=plan)
                usage = {"tier": plan, "daily_limit": u["daily_limit"], "minute_limit": u["minute_limit"],
                         "note": "usage is counted per IP address on this plan"}
            return {"principal": "api_key", "key_fingerprint": api_key_client_id(key), "plan": plan, "usage": usage}
    return {"principal": "anonymous"}


def _ai(d: dict) -> str:
    if d.get("principal") == "oauth":
        u = d.get("usage") or {}
        return (f"Connected OneQAZ account {d['account_id']} on plan '{d['plan']}' via "
                f"{(d.get('connected_app') or {}).get('host') or 'a registered app'}; "
                f"daily limit {u.get('daily_limit')}, per-minute limit {u.get('minute_limit')}.")
    if d.get("principal") == "api_key":
        return f"Authenticated with an API key on plan '{d['plan']}'."
    return "No OneQAZ account is connected."


def _user(d: dict) -> str:
    if d.get("principal") == "oauth":
        return f"OneQAZ 계정이 연결돼 있습니다 — 플랜 {d['plan']}."
    if d.get("principal") == "api_key":
        return f"API 키로 인증됐습니다 — 플랜 {d['plan']}."
    return "연결된 OneQAZ 계정이 없습니다."


def register_account_tools(mcp, cache) -> None:
    from oneqaz_trading_mcp.resources.resource_response import MCPErrorAction, MCPErrorCode, mcp_error, wrap_tool_response

    @mcp.tool(title="Account Status", annotations={"readOnlyHint": True, "openWorldHint": False, "idempotentHint": True})
    async def get_account_status() -> dict:
        """
        Purpose: Show which OneQAZ account (or API key) this connection uses, its plan and its rate limits.
            Requires a connected OneQAZ account — calling it without one prompts your AI app to connect
            (free, no API key needed; Pro users can use their key). All other tools work without an account.
        Triggers: "is my OneQAZ account connected?", "what plan am I on?", "내 OneQAZ 계정 상태", "남은 호출 한도".
        When to call: only when the user asks about their own account or limits.
        Output: full_data { principal (oauth|api_key), plan, usage {daily_limit, daily_used, minute_limit, ...},
            account_id & connected_app (oauth only) }.
        """
        data = await asyncio.to_thread(_principal_from_request)
        if data.get("principal") == "anonymous":
            return mcp_error(MCPErrorCode.AUTH_FAILED, "No OneQAZ account is connected to this session.",
                             action=MCPErrorAction.CHECK, action_value="connect your OneQAZ account in your AI app")
        return wrap_tool_response(data, "account_status", _ai, _user)

    logger.info("  [OK] Account tools registered (get_account_status — OAuth P3a)")
