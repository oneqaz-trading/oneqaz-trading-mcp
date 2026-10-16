# -*- coding: utf-8 -*-
"""CIMD(Client ID Metadata Document) 해석 — OAuth P2 (2026-10-07).

MCP 2026-07-28 권고 등록 방식: client_id 가 HTTPS URL 이고, 그 URL 의 JSON 문서가 클라이언트 메타데이터다.
서버가 **외부 URL 을 직접 가져오므로 SSRF 표면**이다 — 설계 D2 가드레일을 전부 건다
(본체 사설망에 PG·admin 이 있다 — 주소는 공개 미러에 싣지 않는다):
  · HTTPS only · 기본 포트(443)만 · userinfo 금지 · 경로 필수
  · 호스트 allowlist(`OAUTH_CIMD_HOSTS`, 기본 claude.ai,chatgpt.com — 스펙 Trust Policies 허용)
  · DNS 해석 결과가 **전부 공인 주소**일 때만(사설·루프백·링크로컬·예약·멀티캐스트 차단)
  · 리다이렉트 비추적 · 3초 타임아웃 · 본문 5KB 상한 · 오류는 캐시하지 않음
  · 문서의 client_id 가 URL 과 **정확히 일치**
  · 공개 클라이언트만(token_endpoint_auth_method 미지정/none, 또는 token_endpoint_auth_methods_supported 에 none 포함)
    — none 을 지원하지 않는 private_key_jwt 전용 클라이언트는 거부(JWT 서명 검증 미구현)
⚠️ DNS 재바인딩(검사 후 연결 사이 IP 변경)은 allowlist 호스트가 Anthropic·OpenAI 소유라 위협 모델 밖으로 둔다.
   allowlist 를 넓힐 때는 이 전제가 깨진다 — IP 고정 연결을 먼저 구현할 것.
"""

from __future__ import annotations

import ipaddress
import json
import logging
import os
import socket
import time
from typing import Callable, List, Optional, Tuple
from urllib.parse import urlsplit

from oneqaz_trading_mcp.oauth_store import ClientRecord

logger = logging.getLogger("MarketMCP")

MAX_DOC_BYTES = 5 * 1024
FETCH_TIMEOUT_SEC = 3.0
CACHE_TTL_SEC = 3600

_PLATFORM_BY_HOST = {"claude.ai": "claude", "anthropic.com": "claude", "chatgpt.com": "chatgpt", "openai.com": "chatgpt"}


class CimdError(Exception):
    """CIMD 해석 실패 — 호출부는 invalid_client 로 바꾼다. 메시지는 사용자 안내용(내부 주소·스택 미포함)."""


def allowed_hosts() -> List[str]:
    raw = os.getenv("OAUTH_CIMD_HOSTS", "claude.ai,chatgpt.com")
    return [h.strip().lower() for h in raw.split(",") if h.strip()]


def is_cimd_client_id(client_id: str) -> bool:
    return client_id.lower().startswith("https://")


def platform_for_host(host: str) -> str:
    host = host.lower()
    for k, v in _PLATFORM_BY_HOST.items():
        if host == k or host.endswith("." + k):
            return v
    return "unknown"


def _check_url(client_id: str) -> str:
    u = urlsplit(client_id)
    if u.scheme != "https":
        raise CimdError("client_id URL must use https")
    if u.username or u.password:
        raise CimdError("client_id URL must not contain credentials")
    if u.port not in (None, 443):
        raise CimdError("client_id URL must use the default https port")
    if not u.path or u.path == "/":
        raise CimdError("client_id URL must have a path")
    if u.fragment:
        raise CimdError("client_id URL must not have a fragment")
    host = (u.hostname or "").lower()
    if host not in allowed_hosts():
        raise CimdError(f"client host '{host}' is not on this server's allowlist")
    return host


def _all_public(host: str, resolver: Callable[[str], List[str]]) -> List[str]:
    ips = resolver(host)
    if not ips:
        raise CimdError("client host did not resolve")
    for ip in ips:
        a = ipaddress.ip_address(ip)
        if not a.is_global or a.is_private or a.is_loopback or a.is_link_local or a.is_reserved or a.is_multicast:
            raise CimdError("client host resolves to a non-public address")
    return ips


def _default_resolver(host: str) -> List[str]:
    return sorted({ai[4][0] for ai in socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)})


def _default_fetch(url: str) -> Tuple[int, bytes, Optional[str]]:
    """(status, body[:MAX+1], content-type). 리다이렉트 비추적·타임아웃·상한 읽기."""
    import httpx

    with httpx.Client(follow_redirects=False, timeout=FETCH_TIMEOUT_SEC,
                      headers={"Accept": "application/json", "User-Agent": "OneQAZ-MCP-AS/1"}) as c:
        with c.stream("GET", url) as r:
            buf = b""
            for chunk in r.iter_bytes():
                buf += chunk
                if len(buf) > MAX_DOC_BYTES:
                    break
            return r.status_code, buf, r.headers.get("content-type")


def resolve_cimd(client_id: str, *, resolver: Callable[[str], List[str]] = _default_resolver,
                 fetch: Callable[[str], Tuple[int, bytes, Optional[str]]] = _default_fetch) -> ClientRecord:
    host = _check_url(client_id)
    _all_public(host, resolver)
    try:
        status, body, _ctype = fetch(client_id)
    except Exception as e:  # 네트워크 오류 — 원문(내부 정보) 대신 종류만
        raise CimdError(f"client metadata document could not be fetched ({type(e).__name__})") from e
    if status != 200:
        raise CimdError(f"client metadata document returned HTTP {status}")
    if len(body) > MAX_DOC_BYTES:
        raise CimdError("client metadata document exceeds 5KB")
    try:
        doc = json.loads(body.decode("utf-8"))
    except Exception as e:
        raise CimdError("client metadata document is not valid JSON") from e
    if not isinstance(doc, dict):
        raise CimdError("client metadata document must be a JSON object")
    if doc.get("client_id") != client_id:
        raise CimdError("client metadata document client_id does not match its URL")
    uris = doc.get("redirect_uris")
    if not isinstance(uris, list) or not uris or not all(isinstance(x, str) and x for x in uris):
        raise CimdError("client metadata document must list redirect_uris")
    auth = doc.get("token_endpoint_auth_method")
    supported = doc.get("token_endpoint_auth_methods_supported")
    # 이 AS 는 공개 클라이언트(none + PKCE)만 받는다. 기본값이 다른 방식이어도 문서가 none 지원을 밝히면 none 으로 쓴다
    # — 2026-10-07 실측: ChatGPT CIMD = private_key_jwt 기본 + supported ['none','private_key_jwt'].
    #   private_key_jwt(클라이언트 JWT 서명 검증)는 미구현 — none 미지원 클라이언트만 거부된다.
    if auth not in (None, "none") and not (isinstance(supported, list) and "none" in supported):
        raise CimdError(f"token_endpoint_auth_method '{auth}' is not supported (public clients only)")
    name = doc.get("client_name")
    return ClientRecord(
        oauth_client_id=client_id, registration="cimd", redirect_uris=list(uris),
        client_name=(str(name)[:120] if name else None), platform_key=platform_for_host(host),
        auth_method="none", metadata=doc, cache_until=time.time() + CACHE_TTL_SEC,
    )
