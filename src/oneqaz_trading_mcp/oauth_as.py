# -*- coding: utf-8 -*-
"""OAuth 인가 서버(AS) — OAuth P2 (2026-10-07, 비공개 — MCP_OAUTH_AS=on 일 때만 라우트 등록).

설계 정본: ai_brain/mac_mini_tasks/HANDOFF_2026_10_07_oauth_design.md (G0 = 권고안 채택)
  D1 자체 최소 AS — **SDK 핸들러 재사용**(mcp.server.auth: authorize·token — PKCE S256 검증·redirect 일치·
     코드 만료는 SDK), 이 모듈은 provider(저장소 연동) + CIMD + 동의 화면 + iss + 공개 전용 DCR + revoke(SDK 결함 우회) 만 쓴다.
  D2 CIMD 우선(mcps/oauth_cimd.py) + DCR 하위호환(공개 클라이언트만·https/loopback redirect 만·IP 한도·플래그로 OFF).
  D4 PRM(.well-known/oauth-protected-resource)은 **여기서 공개하지 않는다** — P3a(헤더 포인터) 이후.
  ⛔ FastMCP `auth=` 는 켜지 않는다(서버 전체 인증이 돼 익명 free 가 무너진다). 라우트는 custom route 로만.

연결 수단 = ① **무료로 연결**(키 없는 무료 계정 — 2026-10-14 R1, `OAUTH_FREE_CONNECT`) ② 기존 API 키(대조에만 쓰고 저장·기록하지 않는다).
  R1 이유: claude.ai 등은 401 을 기다리지 않고 **연결 단계에서** AS 메타데이터를 찾아 OAuth 를 시작한다(2026-10-14 실측).
  로그인 수단이 키뿐이면 키 없는 사용자는 연결 자체가 막힌다 → 어느 플랫폼이 어떤 경로로 OAuth 에 들어와도 키 없이 끝나게 한다.
쿠키 없음 — 대기 요청은 서버(api.oauth_pending)에 보관.
토큰: access `oqzat_`+256bit(1h) · refresh `oqzrt_`+256bit(30d, 매 사용 회전, 재사용 시 계열 전체 폐기). 저장은 sha256 만.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import logging
import os
import secrets
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, Optional
from urllib.parse import urlsplit

from oneqaz_trading_mcp.oauth_cimd import CimdError, is_cimd_client_id, resolve_cimd
from oneqaz_trading_mcp.oauth_store import (
    ACCESS_TTL_SEC, ClientRecord, PgOAuthStore, StoreUnavailable, new_id,
)

logger = logging.getLogger("MarketMCP")

SCOPE_READ = "oneqaz:read"
SCOPE_OFFLINE = "offline_access"
VALID_SCOPES = [SCOPE_READ, SCOPE_OFFLINE]
DEFAULT_SCOPES = [SCOPE_READ]
MAX_KEY_ATTEMPTS = 5
_LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "[::1]", "::1")


def as_enabled() -> bool:
    return os.getenv("MCP_OAUTH_AS", "off").strip().lower() == "on"


def issuer() -> str:
    return os.getenv("OAUTH_ISSUER", "https://api.oneqaz.com").rstrip("/")


def resource_url() -> str:
    return os.getenv("OAUTH_RESOURCE", "https://api.oneqaz.com/mcp").rstrip("/")


def dcr_enabled() -> bool:
    return os.getenv("OAUTH_DCR_ENABLED", "1") == "1"


def free_connect_enabled() -> bool:
    """동의 화면 "무료로 연결"(키 없는 무료 계정). 킬스위치 = OAUTH_FREE_CONNECT=0 (끄면 10-08 키 전용 화면)."""
    return os.getenv("OAUTH_FREE_CONNECT", "1") == "1"


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, str(default))))
    except ValueError:
        logger.error("%s=%r 정수 아님 — 기본값 %d 사용", name, os.getenv(name), default)
        return default


def sha256_hex(v: str) -> str:
    return hashlib.sha256(v.encode("utf-8")).hexdigest()


def _same_resource(a: Optional[str], b: str) -> bool:
    """RFC 8707 resource 비교 — 끝 슬래시·호스트 대소문자만 관대하게."""
    if not a:
        return False
    ua, ub = urlsplit(a.rstrip("/")), urlsplit(b.rstrip("/"))
    return (ua.scheme, (ua.hostname or "").lower(), ua.port, ua.path) == (ub.scheme, (ub.hostname or "").lower(), ub.port, ub.path)


def redirect_allowed(requested: str, registered: List[str]) -> bool:
    """정확 일치, 단 루프백 http 는 포트 무시(RFC 8252 §7.3 — 네이티브 앱은 실행 때마다 임의 포트)."""
    if requested in registered:
        return True
    r = urlsplit(requested)
    if r.scheme != "http" or (r.hostname or "") not in _LOOPBACK_HOSTS:
        return False
    for reg in registered:
        g = urlsplit(reg)
        if g.scheme == "http" and (g.hostname or "") == (r.hostname or "") and g.path == r.path and g.query == r.query:
            return True
    return False


def _is_loopback_uri(u: str) -> bool:
    s = urlsplit(u)
    return s.scheme == "http" and (s.hostname or "") in _LOOPBACK_HOSTS


def _client_ip(request) -> str:
    return (request.headers.get("cf-connecting-ip")
            or request.headers.get("x-forwarded-for", "").split(",")[0].strip()
            or (request.client.host if request.client else "local"))


class _Window:
    """IP 별 슬라이딩 창 한도(프로세스 메모리). 무차별 대입·등록 스팸 억제용."""

    def __init__(self, limit: int, window_sec: int):
        self.limit, self.window = limit, window_sec
        self._hits: Dict[str, Deque[float]] = defaultdict(deque)

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        q = self._hits[key]
        while q and now - q[0] > self.window:
            q.popleft()
        if len(q) >= self.limit:
            return False
        q.append(now)
        return True


# ─────────────────────────────────────────────────────────────────────────────
# Provider — SDK OAuthAuthorizationServerProvider 프로토콜 구현
# ─────────────────────────────────────────────────────────────────────────────


def _client_model():
    from mcp.shared.auth import InvalidRedirectUriError, OAuthClientInformationFull

    class OneqazClient(OAuthClientInformationFull):
        def validate_redirect_uri(self, redirect_uri):  # type: ignore[override]
            if redirect_uri is not None:
                if redirect_allowed(str(redirect_uri), [str(u) for u in (self.redirect_uris or [])]):
                    return redirect_uri
                raise InvalidRedirectUriError(f"Redirect URI '{redirect_uri}' not registered for client")
            return super().validate_redirect_uri(None)

    return OneqazClient


class OneqazProvider:
    """저장소(PgOAuthStore/MemoryOAuthStore) 위의 provider. DB 호출은 to_thread 로 이벤트 루프 밖에서."""

    def __init__(self, store=None, *, cimd_resolver=resolve_cimd):
        self.store = store if store is not None else PgOAuthStore()
        self._cimd = cimd_resolver
        self._Client = _client_model()

    async def _db(self, fn, *a):
        return await asyncio.to_thread(fn, *a)

    def _to_client(self, rec: ClientRecord):
        return self._Client(
            client_id=rec.oauth_client_id, redirect_uris=rec.redirect_uris, token_endpoint_auth_method="none",
            grant_types=["authorization_code", "refresh_token"], response_types=["code"],
            scope=" ".join(VALID_SCOPES), client_name=rec.client_name,
        )

    async def get_client_record(self, client_id: str) -> Optional[ClientRecord]:
        rec = await self._db(self.store.get_client, client_id)
        if is_cimd_client_id(client_id):
            if rec is not None and rec.cache_until and rec.cache_until > time.time():
                return rec
            try:
                fresh = await self._db(self._cimd, client_id)
            except CimdError as e:
                logger.warning("[OAUTH] CIMD 거부 client=%s: %s", client_id[:120], e)
                return None
            await self._db(self.store.upsert_client, fresh)
            return fresh
        return rec

    async def get_client(self, client_id: str):
        rec = await self.get_client_record(client_id)
        return self._to_client(rec) if rec else None

    async def register_client(self, client_info) -> None:  # SDK DCR 핸들러는 쓰지 않는다(공개 전용 자체 핸들러)
        raise NotImplementedError

    async def authorize(self, client, params) -> str:
        from mcp.server.auth.provider import AuthorizeError

        res = params.resource or resource_url()
        if not _same_resource(res, resource_url()):
            raise AuthorizeError("invalid_target", "unknown resource for this authorization server")
        scopes = params.scopes or list(DEFAULT_SCOPES)
        if SCOPE_READ not in scopes:
            scopes = [SCOPE_READ] + [s for s in scopes if s != SCOPE_READ]
        req = secrets.token_urlsafe(24)
        await self._db(self.store.save_pending, sha256_hex(req), client.client_id, {
            "redirect_uri": str(params.redirect_uri),
            "redirect_uri_explicit": bool(params.redirect_uri_provided_explicitly),
            "state": params.state, "scopes": scopes, "code_challenge": params.code_challenge,
            "resource": resource_url(),
        })
        return f"{issuer()}/oauth/consent?req={req}"

    async def load_authorization_code(self, client, authorization_code: str):
        from mcp.server.auth.provider import AuthorizationCode

        rec = await self._db(self.store.load_code, sha256_hex(authorization_code))
        if rec is None:
            return None
        if rec.used:
            # RFC 6749 §4.1.2: 코드 재사용 = 탈취 의심 → 그 코드로 발급된 토큰 전부 폐기(SHOULD)
            logger.warning("[OAUTH] 인가 코드 재사용 감지 — grant 폐기 grant=%s client=%s", rec.grant_id, rec.oauth_client_id[:80])
            await self._db(self.store.revoke_grant, rec.grant_id)
            return None
        if rec.grant_revoked:
            return None
        return AuthorizationCode(
            code=authorization_code, scopes=rec.scopes, expires_at=rec.expires_at, client_id=rec.oauth_client_id,
            code_challenge=rec.code_challenge, redirect_uri=rec.redirect_uri,
            redirect_uri_provided_explicitly=rec.redirect_uri_explicit, resource=rec.resource, subject=rec.account_id,
        )

    async def _issue(self, grant_id: str, family_id: str, scopes: List[str]):
        from mcp.shared.auth import OAuthToken

        access = "oqzat_" + secrets.token_urlsafe(32)
        refresh = "oqzrt_" + secrets.token_urlsafe(32)
        await self._db(self.store.issue_tokens, grant_id, family_id, sha256_hex(access), sha256_hex(refresh))
        return OAuthToken(access_token=access, token_type="Bearer", expires_in=ACCESS_TTL_SEC,
                          refresh_token=refresh, scope=" ".join(scopes))

    async def exchange_authorization_code(self, client, authorization_code):
        from mcp.server.auth.provider import TokenError

        h = sha256_hex(authorization_code.code)
        if not await self._db(self.store.consume_code, h):  # 동시 교환 경합의 패자
            rec = await self._db(self.store.load_code, h)
            if rec is not None:
                await self._db(self.store.revoke_grant, rec.grant_id)
            raise TokenError("invalid_grant", "authorization code already used")
        rec = await self._db(self.store.load_code, h)
        await self._db(self.store.touch_client, rec.oauth_client_id)
        logger.info("[OAUTH] 토큰 발급(code) account=%s client=%s", rec.account_id, rec.oauth_client_id[:80])
        return await self._issue(rec.grant_id, new_id("fam_"), rec.scopes)

    async def load_refresh_token(self, client, refresh_token: str):
        from mcp.server.auth.provider import RefreshToken

        if not refresh_token.startswith("oqzrt_"):
            return None
        rec = await self._db(self.store.load_token, sha256_hex(refresh_token), "refresh")
        if rec is None:
            return None
        if rec.revoked:
            # 회전된(=폐기된) refresh 를 다시 냈다 = 탈취 의심 → 계열 전체 폐기 (public client 회전 MUST 의 짝)
            n = await self._db(self.store.revoke_family, rec.family_id)
            logger.warning("[OAUTH] refresh 재사용 감지 — 계열 폐기 %d건 account=%s", n, rec.account_id)
            return None
        if rec.grant_revoked or rec.account_status != "active":
            return None
        return RefreshToken(token=refresh_token, client_id=rec.oauth_client_id, scopes=rec.scopes,
                            expires_at=int(rec.expires_at), resource=rec.resource, subject=rec.account_id)

    async def exchange_refresh_token(self, client, refresh_token, scopes: List[str]):
        from mcp.server.auth.provider import TokenError

        h = sha256_hex(refresh_token.token)
        rec = await self._db(self.store.load_token, h, "refresh")
        if rec is None or not await self._db(self.store.revoke_token_if_active, h):
            if rec is not None:
                await self._db(self.store.revoke_family, rec.family_id)
            raise TokenError("invalid_grant", "refresh token is no longer valid")
        return await self._issue(rec.grant_id, rec.family_id, rec.scopes)

    async def load_access_token(self, token: str):
        from mcp.server.auth.provider import AccessToken

        if not token.startswith("oqzat_"):
            return None
        rec = await self._db(self.store.load_token, sha256_hex(token), "access")
        if rec is None or rec.revoked or rec.grant_revoked or rec.account_status != "active" or rec.expires_at < time.time():
            return None
        return AccessToken(token=token, client_id=rec.oauth_client_id, scopes=rec.scopes,
                           expires_at=int(rec.expires_at), resource=rec.resource, subject=rec.account_id)

    async def revoke_token(self, token) -> None:
        from mcp.server.auth.provider import RefreshToken

        h = sha256_hex(token.token)
        if isinstance(token, RefreshToken):
            # RFC 7009 §2.1: refresh 폐기 시 같은 grant 의 access 도 폐기 SHOULD → 계열 단위
            rec = await self._db(self.store.load_token, h, "refresh")
            if rec is not None:
                await self._db(self.store.revoke_family, rec.family_id)
            return
        await self._db(self.store.revoke_token_if_active, h)

    async def exchange_identity_assertion(self, client, params):  # SEP-990 — 미지원
        from mcp.server.auth.provider import TokenError
        raise TokenError("unsupported_grant_type", "identity assertion grant is not supported")


# ─────────────────────────────────────────────────────────────────────────────
# 동의 화면 (쿠키 없음 · 프레임 금지 · no-store)
# ─────────────────────────────────────────────────────────────────────────────

_PAGE_HEADERS = {
    "Cache-Control": "no-store",
    "Pragma": "no-cache",
    "X-Frame-Options": "DENY",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    # form-action 은 두지 않는다 — 제출 뒤 클라이언트 redirect_uri 로의 302 를 일부 브라우저가 form-action 으로 막는다.
    "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'",
}

_SCOPE_LABEL = {
    SCOPE_READ: "OneQAZ 연구 데이터 읽기 · Read OneQAZ research data",
    SCOPE_OFFLINE: "다시 로그인하지 않고 연결 유지 · Stay connected (refresh token)",
}


def _page(title: str, body: str, status: int = 200):
    from starlette.responses import HTMLResponse

    doc = f"""<!doctype html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>{html.escape(title)}</title>
<style>body{{font-family:system-ui,-apple-system,sans-serif;max-width:520px;margin:40px auto;padding:0 16px;color:#1a1a1a;background:#fff}}
h1{{font-size:20px}}.box{{border:1px solid #ddd;border-radius:8px;padding:16px;margin:16px 0}}
.warn{{background:#fff7e6;border-color:#f0c36d}}.err{{background:#fdecea;border-color:#e57373}}
label{{display:block;margin:12px 0 4px;font-weight:600}}input[type=password]{{width:100%;padding:8px;box-sizing:border-box}}
button{{padding:10px 16px;margin:12px 8px 0 0;border-radius:6px;border:1px solid #888;background:#f5f5f5;cursor:pointer}}
button.primary{{background:#1a1a1a;color:#fff;border-color:#1a1a1a}}small{{color:#555}}
@media (prefers-color-scheme:dark){{body{{background:#121212;color:#eee}}.box{{border-color:#444}}.warn{{background:#3a2e12}}.err{{background:#3a1515}}
button{{background:#2a2a2a;color:#eee}}button.primary{{background:#eee;color:#111}}small{{color:#aaa}}}}</style></head>
<body>{body}</body></html>"""
    return HTMLResponse(doc, status_code=status, headers=_PAGE_HEADERS)


def _consent_body(req: str, rec: ClientRecord, pending: Dict[str, Any], error: Optional[str] = None) -> str:
    """동의 화면. 무료 연결이 켜져 있으면 "무료로 연결"이 기본 버튼이고 API 키 입력은 접어 둔다(Pro 전용).

    폼을 둘로 나눈다 — 한 폼에 두면 키 칸에서 Enter 를 칠 때 첫 제출 버튼("무료로 연결")이 눌려 키가 무시된다.
    """
    if not free_connect_enabled():
        return _consent_body_key_only(req, rec, pending, error)
    redirect = pending.get("redirect_uri", "")
    rhost = urlsplit(redirect).hostname or redirect
    if rec.registration == "cimd":
        app = f"<b>{html.escape(urlsplit(rec.oauth_client_id).hostname or '')}</b> <small>(메타데이터 문서 호스트로 확인됨)</small>"
    else:
        app = (f"<b>{html.escape(rec.client_name or rec.oauth_client_id)}</b> "
               "<small>(앱이 스스로 밝힌 이름 — 확인되지 않음 · self-declared, unverified)</small>")
    scopes = "".join(f"<li>{html.escape(_SCOPE_LABEL.get(s, s))}</li>" for s in pending.get("scopes", []))
    warn = ""
    if rec.redirect_uris and all(_is_loopback_uri(u) for u in rec.redirect_uris):
        warn = ('<div class="box warn">이 앱은 <b>이 컴퓨터의 로컬 프로그램</b>으로 돌아갑니다. 직접 실행한 프로그램이 맞는지 확인하세요.'
                '<br><small>This app returns to a program on your own computer — continue only if you started it.</small></div>')
    err = f'<div class="box err">{html.escape(error)}</div>' if error else ""
    q = html.escape(req)
    key_open = " open" if error else ""  # 키 오류 메시지는 키 칸과 함께 보여야 한다
    return f"""<h1>OneQAZ 연결 · Connect OneQAZ</h1>
<div class="box">앱 · App: {app}<br>돌아갈 주소 · Returns to: <b>{html.escape(rhost)}</b>
<p>요청 권한 · Requested access:</p><ul>{scopes}</ul></div>{warn}{err}
<form method="post" action="/oauth/consent">
<input type="hidden" name="req" value="{q}">
<p>API 키가 없어도 됩니다 — 무료 플랜으로 바로 연결됩니다. 이메일 등 개인정보는 받지 않습니다.<br>
<small>No API key needed — you connect on the free plan. We ask for no email or other personal details.
<a href="https://api.oneqaz.com/pricing">한도 · Limits</a></small></p>
<button class="primary" type="submit" name="action" value="approve_free">무료로 연결 · Connect free</button>
<button type="submit" name="action" value="deny">거부 · Deny</button>
</form>
<details{key_open}><summary>Pro API 키가 있나요? · Have a Pro API key?</summary>
<form method="post" action="/oauth/consent">
<input type="hidden" name="req" value="{q}">
<label for="k">OneQAZ API 키 · API key</label>
<input id="k" type="password" name="api_key" autocomplete="off" spellcheck="false" placeholder="oqz_…">
<small>입력한 키는 대조에만 쓰고 저장·기록하지 않습니다 · Your key is only checked, never stored or logged.</small><br>
<button type="submit" name="action" value="approve">키로 연결 · Connect with key</button>
</form>
</details>
<p><small><a href="https://oneqaz.com/privacy">처리방침 · Privacy</a> · <a href="https://oneqaz.com/terms">약관 · Terms</a><br>
연구 정보 제공 목적이며 투자 권유가 아닙니다 · Research information only, not investment advice.</small></p>"""


def _consent_body_key_only(req: str, rec: ClientRecord, pending: Dict[str, Any], error: Optional[str] = None) -> str:
    """OAUTH_FREE_CONNECT=0 일 때의 10-08 화면(API 키 로그인 전용) — 킬스위치 복귀 형태."""
    redirect = pending.get("redirect_uri", "")
    rhost = urlsplit(redirect).hostname or redirect
    if rec.registration == "cimd":
        app = f"<b>{html.escape(urlsplit(rec.oauth_client_id).hostname or '')}</b> <small>(메타데이터 문서 호스트로 확인됨)</small>"
    else:
        app = (f"<b>{html.escape(rec.client_name or rec.oauth_client_id)}</b> "
               "<small>(앱이 스스로 밝힌 이름 — 확인되지 않음 · self-declared, unverified)</small>")
    scopes = "".join(f"<li>{html.escape(_SCOPE_LABEL.get(s, s))}</li>" for s in pending.get("scopes", []))
    warn = ""
    if rec.redirect_uris and all(_is_loopback_uri(u) for u in rec.redirect_uris):
        warn = ('<div class="box warn">이 앱은 <b>이 컴퓨터의 로컬 프로그램</b>으로 돌아갑니다. 직접 실행한 프로그램이 맞는지 확인하세요.'
                '<br><small>This app returns to a program on your own computer — continue only if you started it.</small></div>')
    err = f'<div class="box err">{html.escape(error)}</div>' if error else ""
    return f"""<h1>OneQAZ 계정 연결 · Connect your OneQAZ account</h1>
<div class="box">앱 · App: {app}<br>돌아갈 주소 · Returns to: <b>{html.escape(rhost)}</b>
<p>요청 권한 · Requested access:</p><ul>{scopes}</ul></div>{warn}{err}
<form method="post" action="/oauth/consent">
<input type="hidden" name="req" value="{html.escape(req)}">
<label for="k">OneQAZ API 키 · API key</label>
<input id="k" type="password" name="api_key" autocomplete="off" spellcheck="false" placeholder="oqz_…">
<small>입력한 키는 대조에만 쓰고 저장·기록하지 않습니다 · Your key is only checked, never stored or logged.
<a href="https://oneqaz.com/privacy">처리방침 · Privacy</a> · <a href="https://oneqaz.com/terms">약관 · Terms</a></small><br>
<button class="primary" type="submit" name="action" value="approve">연결 허용 · Allow</button>
<button type="submit" name="action" value="deny">거부 · Deny</button>
</form>
<p><small>연구 정보 제공 목적이며 투자 권유가 아닙니다 · Research information only, not investment advice.</small></p>"""


# ─────────────────────────────────────────────────────────────────────────────
# 라우트
# ─────────────────────────────────────────────────────────────────────────────


def authorization_server_metadata() -> Dict[str, Any]:
    iss = issuer()
    md: Dict[str, Any] = {
        "issuer": iss,
        "authorization_endpoint": f"{iss}/authorize",
        "token_endpoint": f"{iss}/token",
        "revocation_endpoint": f"{iss}/revoke",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none"],
        "revocation_endpoint_auth_methods_supported": ["none"],
        "scopes_supported": VALID_SCOPES,
        "client_id_metadata_document_supported": True,
        "authorization_response_iss_parameter_supported": True,
        "service_documentation": "https://oneqaz.com/",
        "op_policy_uri": "https://oneqaz.com/privacy",
        "op_tos_uri": "https://oneqaz.com/terms",
    }
    if dcr_enabled():
        md["registration_endpoint"] = f"{iss}/register"
    return md


def _prm_url() -> str:
    from oneqaz_trading_mcp.auth_principal import oauth_prm_url
    return oauth_prm_url()


def protected_resource_metadata() -> Dict[str, Any]:
    return {
        "resource": resource_url(),
        "authorization_servers": [issuer()],
        "scopes_supported": [SCOPE_READ],
        "bearer_methods_supported": ["header"],
        "resource_name": "OneQAZ Trading Intelligence MCP",
        "resource_documentation": "https://oneqaz.com/",
        "resource_policy_uri": "https://oneqaz.com/privacy",
        "resource_tos_uri": "https://oneqaz.com/terms",
    }


def build_routes(provider: Optional[OneqazProvider] = None) -> List[Any]:
    """Starlette Route 목록. server.py(MCP_OAUTH_AS=on)와 테스트가 같은 함수를 쓴다."""
    from mcp.server.auth.handlers.authorize import AuthorizationHandler
    from mcp.server.auth.handlers.token import TokenHandler
    from mcp.server.auth.middleware.client_auth import ClientAuthenticator
    from mcp.server.auth.provider import construct_redirect_uri
    from mcp.server.transport_security import RequestBodyLimitMiddleware
    from mcp.shared.auth import OAuthClientMetadata
    from pydantic import ValidationError
    from starlette.requests import Request
    from starlette.responses import JSONResponse, RedirectResponse, Response
    from starlette.routing import Route, request_response

    provider = provider or OneqazProvider()
    store = provider.store
    authn = ClientAuthenticator(provider)
    authorize_handler = AuthorizationHandler(provider)
    token_handler = TokenHandler(provider, authn)
    consent_limit = _Window(30, 600)
    register_limit = _Window(10, 3600)
    # [2026-10-14 R1] 키 없는 무료 계정 생성 상한(프로세스 메모리 — 재시작 시 초기화). 공유 IP(사무실·통신사 NAT) 고려해 IP 당 10.
    free_ip_limit = _Window(_env_int("OAUTH_FREE_ACCOUNTS_PER_IP_DAY", 10), 86400)
    free_global_limit = _Window(_env_int("OAUTH_FREE_ACCOUNTS_DAY_MAX", 500), 86400)
    body_limit = 64 * 1024

    def limited(handler):
        async def guarded(request: Request) -> Response:
            # 저장소·권한 오류는 ERROR(트레이스백)로 남기고 503 으로 **명시** 실패한다 — 500 스택 노출·침묵 둘 다 아님.
            try:
                return await handler(request)
            except Exception:
                logger.error("[OAUTH] %s 처리 중 내부 오류", request.url.path, exc_info=True)
                if request.url.path == "/oauth/consent":
                    return _page("일시 장애", "<h1>잠시 후 다시 시도하세요</h1><p>인가 서버 내부 오류입니다.</p>", 503)
                return JSONResponse({"error": "temporarily_unavailable",
                                     "error_description": "authorization server error"}, status_code=503)
        return RequestBodyLimitMiddleware(request_response(guarded), body_limit)

    async def metadata(request: Request) -> Response:
        return JSONResponse(authorization_server_metadata(), headers={"Cache-Control": "public, max-age=300"})

    async def protected_resource(request: Request) -> Response:
        # [P3a] PRM(RFC 9728) — well-known 이 아니라 401 헤더 포인터의 URL 로만 알린다(설계 D4 1단계).
        #   resource 는 사용자가 입력하는 커넥터 URL 과 정확히 같아야 한다(끝 슬래시 없음). offline_access 는 넣지 않는다.
        return JSONResponse(protected_resource_metadata(), headers={"Cache-Control": "public, max-age=300"})

    async def authorize(request: Request) -> Response:
        resp = await authorize_handler.handle(request)
        # RFC 9207: 클라이언트로 돌아가는 모든 인가 응답(오류 포함)에 iss. SDK 오류 redirect 에는 없으므로 보탠다.
        loc = resp.headers.get("location") if resp.status_code in (302, 303) else None
        if loc and not loc.startswith(f"{issuer()}/oauth/consent") and "iss=" not in (urlsplit(loc).query or ""):
            return RedirectResponse(construct_redirect_uri(loc, iss=issuer()), status_code=resp.status_code)
        return resp

    async def consent_get(request: Request) -> Response:
        req = request.query_params.get("req", "")
        pending = await provider._db(store.get_pending, sha256_hex(req)) if req else None
        if pending is None:
            return _page("요청 만료", "<h1>요청이 만료됐거나 이미 처리됐습니다</h1><p>앱에서 연결을 다시 시작하세요. "
                         "<small>This request expired or was already used — start again from the app.</small></p>", 400)
        rec = await provider.get_client_record(pending.oauth_client_id)
        if rec is None:
            return _page("앱 확인 불가", "<h1>앱 정보를 확인할 수 없습니다</h1>", 400)
        return _page("OneQAZ 계정 연결", _consent_body(req, rec, pending.params))

    async def consent_post(request: Request) -> Response:
        from oneqaz_trading_mcp.self_host_auth import KeyStoreUnavailable, lookup_key

        ip = _client_ip(request)
        if not consent_limit.allow(ip):
            return _page("잠시 후 다시", "<h1>시도가 너무 많습니다</h1><p>잠시 후 다시 시도하세요.</p>", 429)
        form = await request.form()
        req = str(form.get("req") or "")
        action = str(form.get("action") or "")
        api_key = str(form.get("api_key") or "").strip()
        rh = sha256_hex(req) if req else ""
        pending = await provider._db(store.get_pending, rh) if req else None
        if pending is None:
            return _page("요청 만료", "<h1>요청이 만료됐거나 이미 처리됐습니다</h1><p>앱에서 연결을 다시 시작하세요.</p>", 400)
        p = pending.params

        def back(**kw):
            return RedirectResponse(construct_redirect_uri(p["redirect_uri"], state=p.get("state"), iss=issuer(), **kw),
                                    status_code=302)

        if action == "deny":
            if await provider._db(store.consume_pending, rh):
                return back(error="access_denied", error_description="the user denied the request")
            return _page("요청 만료", "<h1>요청이 이미 처리됐습니다</h1>", 400)
        if action not in ("approve", "approve_free") or (action == "approve_free" and not free_connect_enabled()):
            return _page("잘못된 요청", "<h1>잘못된 요청</h1>", 400)

        rec = await provider.get_client_record(pending.oauth_client_id)
        if rec is None:
            return _page("앱 확인 불가", "<h1>앱 정보를 확인할 수 없습니다</h1>", 400)

        if action == "approve_free":
            # [2026-10-14 R1] 키 없는 무료 계정. 생성 상한(IP·전역 하루) — 계정 단위 무료 한도의 증폭(계정 남발)을 막는다.
            if not free_ip_limit.allow(ip):
                logger.warning("[OAUTH] 무료 연결 IP 상한 초과 (ip=%s)", ip)
                return _page("잠시 후 다시", "<h1>오늘 이 네트워크에서 만든 무료 연결이 너무 많습니다</h1>"
                             "<p>내일 다시 시도하세요. <small>Too many free connections from this network today.</small></p>", 429)
            if not free_global_limit.allow("*"):
                logger.error("[OAUTH] 무료 연결 전역 일일 상한 도달 — 남발 의심, 운영자 확인 필요")
                return _page("잠시 후 다시", "<h1>지금은 새 무료 연결을 받을 수 없습니다</h1>"
                             "<p>잠시 후 다시 시도하세요. <small>Free connections are paused — try again later.</small></p>", 429)
            if not await provider._db(store.consume_pending, rh):
                return _page("요청 만료", "<h1>요청이 이미 처리됐습니다</h1>", 400)
            account_id, _plan, _st = await provider._db(store.create_free_account)
            grant_id = await provider._db(store.create_grant, account_id, pending.oauth_client_id, list(p["scopes"]), p["resource"])
            code = secrets.token_urlsafe(32)
            await provider._db(store.save_code, sha256_hex(code), grant_id, p["redirect_uri"],
                               bool(p["redirect_uri_explicit"]), p["code_challenge"])
            logger.info("[OAUTH] 동의 완료(무료·키 없음) account=%s client=%s scopes=%s", account_id,
                        pending.oauth_client_id[:80], " ".join(p["scopes"]))
            return back(code=code)

        if not api_key:
            # 빈 키 제출은 시도 횟수에 넣지 않는다 — 오타가 아니라 "키가 없다" 는 뜻이므로 길을 알려준다.
            msg = ("API 키를 입력하세요. 키가 없으면 위의 '무료로 연결'을 누르세요 · Enter your API key, or choose 'Connect free' above."
                   if free_connect_enabled() else "API 키를 입력하세요 · Enter your API key.")
            return _page("OneQAZ 연결", _consent_body(req, rec, p, error=msg), 400)
        try:
            status, info = await asyncio.to_thread(lookup_key, api_key) if api_key else ("unknown", None)
        except KeyStoreUnavailable:
            return _page("일시 장애", "<h1>잠시 후 다시 시도하세요</h1><p>키 저장소에 일시적으로 연결할 수 없습니다.</p>", 503)
        if status != "valid" or info is None:
            n = await provider._db(store.bump_pending_attempts, rh, MAX_KEY_ATTEMPTS)
            logger.warning("[OAUTH] 동의 화면 키 거부 (%s, 시도 %d/%d, ip=%s)", status, n, MAX_KEY_ATTEMPTS, ip)
            if n >= MAX_KEY_ATTEMPTS:
                return _page("시도 초과", "<h1>시도 횟수를 넘었습니다</h1><p>앱에서 연결을 다시 시작하세요.</p>", 400)
            msg = {"expired": "만료된 키입니다 · This key has expired.",
                   "inactive": "비활성화된 키입니다 · This key has been deactivated."}.get(
                status, "확인되지 않는 키입니다 · This key was not recognized.")
            return _page("OneQAZ 계정 연결", _consent_body(req, rec, p, error=msg), 400)
        if not await provider._db(store.consume_pending, rh):
            return _page("요청 만료", "<h1>요청이 이미 처리됐습니다</h1>", 400)
        account_id, _plan, acc_status = await provider._db(store.account_for_key, api_key, info.effective_plan)
        if acc_status != "active":
            return back(error="access_denied", error_description="account is not active")
        grant_id = await provider._db(store.create_grant, account_id, pending.oauth_client_id, list(p["scopes"]), p["resource"])
        code = secrets.token_urlsafe(32)
        await provider._db(store.save_code, sha256_hex(code), grant_id, p["redirect_uri"],
                           bool(p["redirect_uri_explicit"]), p["code_challenge"])
        logger.info("[OAUTH] 동의 완료 account=%s client=%s scopes=%s", account_id, pending.oauth_client_id[:80],
                    " ".join(p["scopes"]))
        return back(code=code)

    async def consent(request: Request) -> Response:
        return await (consent_post(request) if request.method == "POST" else consent_get(request))

    async def revoke(request: Request) -> Response:
        """RFC 7009 — 자체 구현. ⚠️ SDK RevocationHandler(mcp 2.3)는 RevocationRequest.client_secret 을 기본값 없이
        선언해 **공개 클라이언트(우리 전부)의 폐기 요청을 400 으로 거절**한다(2026-10-07 실측). 토큰을 못 찾아도 200(RFC)."""
        from mcp.server.auth.middleware.client_auth import AuthenticationError

        try:
            client = await authn.authenticate_request(request)
        except AuthenticationError as e:
            return JSONResponse({"error": "invalid_client", "error_description": e.message}, status_code=401)
        form = await request.form()
        token = str(form.get("token") or "")
        if not token:
            return JSONResponse({"error": "invalid_request", "error_description": "token is required"}, status_code=400)
        hint = str(form.get("token_type_hint") or "")
        loaders = [provider.load_access_token, lambda t: provider.load_refresh_token(client, t)]
        if hint == "refresh_token":
            loaders.reverse()
        for load in loaders:
            found = await load(token)
            if found is not None:
                if found.client_id == client.client_id:
                    await provider.revoke_token(found)
                break
        return Response(status_code=200, headers={"Cache-Control": "no-store"})

    async def register(request: Request) -> Response:
        """RFC 7591 DCR — 공개 클라이언트만(secret 미발급)·redirect 는 https 또는 http 루프백만·IP 당 시간 10건."""
        def bad(desc: str, status: int = 400):
            return JSONResponse({"error": "invalid_client_metadata", "error_description": desc}, status_code=status)

        if not register_limit.allow(_client_ip(request)):
            return JSONResponse({"error": "slow_down", "error_description": "too many registrations"}, status_code=429)
        try:
            md = OAuthClientMetadata.model_validate_json(await request.body())
        except ValidationError:
            return bad("client metadata is invalid")
        if md.token_endpoint_auth_method not in (None, "none"):
            return bad("only public clients (token_endpoint_auth_method=none) are supported")
        uris = [str(u) for u in (md.redirect_uris or [])]
        for u in uris:
            s = urlsplit(u)
            if not (s.scheme == "https" or _is_loopback_uri(u)) or s.fragment:
                return bad("redirect_uris must be https or http loopback without fragment")
        if "authorization_code" not in md.grant_types or "code" not in md.response_types:
            return bad("authorization_code grant with response_type code is required")
        req_scopes = (md.scope or SCOPE_READ).split()
        if not set(req_scopes) <= set(VALID_SCOPES):
            return bad("requested scope is not supported")
        cid = new_id("dcr_")
        rec = ClientRecord(oauth_client_id=cid, registration="dcr", redirect_uris=uris,
                           client_name=(md.client_name[:120] if md.client_name else None), platform_key="unknown")
        await provider._db(store.upsert_client, rec)
        logger.info("[OAUTH] DCR 등록 client=%s name=%s", cid, (md.client_name or "")[:60])
        return JSONResponse({
            "client_id": cid, "client_id_issued_at": int(time.time()), "token_endpoint_auth_method": "none",
            "redirect_uris": uris, "grant_types": ["authorization_code", "refresh_token"], "response_types": ["code"],
            "client_name": rec.client_name, "scope": " ".join(req_scopes),
        }, status_code=201, headers={"Cache-Control": "no-store"})

    routes = [
        Route("/.well-known/oauth-authorization-server", endpoint=metadata, methods=["GET"]),
        Route(urlsplit(_prm_url()).path, endpoint=protected_resource, methods=["GET"]),
        Route("/authorize", endpoint=limited(authorize), methods=["GET", "POST"]),
        Route("/oauth/consent", endpoint=limited(consent), methods=["GET", "POST"]),
        Route("/token", endpoint=limited(token_handler.handle), methods=["POST"]),
        Route("/revoke", endpoint=limited(revoke), methods=["POST"]),
    ]
    if dcr_enabled():
        routes.append(Route("/register", endpoint=limited(register), methods=["POST"]))
    return routes


def register_oauth_as(mcp) -> bool:
    """MCP_OAUTH_AS=on 일 때만 AS 라우트를 MCP 서버에 붙인다. 기본 off = 등록 0(전부 404, 종전과 동일)."""
    if not as_enabled():
        return False
    for r in build_routes():
        mcp._additional_http_routes.append(r)
    logger.warning("[OAUTH] 인가 서버 라우트 활성 (issuer=%s, DCR=%s) — PRM = 401 헤더 포인터(%s)만, well-known 미공개(설계 D4)",
                   issuer(), dcr_enabled(), _prm_url())
    return True
