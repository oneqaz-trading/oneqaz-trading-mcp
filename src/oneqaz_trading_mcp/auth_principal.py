# -*- coding: utf-8 -*-
"""MCP 자격증명 판별·401 빌더 — OAuth P1 RS 골격 (2026-10-07, **응답 동작 불변**).

설계 정본: ai_brain/mac_mini_tasks/HANDOFF_2026_10_07_oauth_design.md §4-3·§6 P1.

왜 P1 이 먼저인가: OAuth 를 켜는 순간 클라이언트는 `Authorization: Bearer <OAuth 토큰>` 을 보낸다.
현행 미들웨어는 Bearer 값을 **무조건 API 키로 조회**하므로 미상 키 401 → 클라이언트 재인가 → 또 401 의
루프가 된다. 그래서 자격증명을 **접두사로 먼저 가르는** 판별기를 응답 동작을 바꾸지 않은 채 먼저 깐다.

모드 (`MCP_OAUTH_MODE`):
  - ``off``    (기본) — 현행과 **응답·원장 모두 동일**. 판별기는 호출되지 않는다.
  - ``shadow`` — 응답은 off 와 동일. 판별 결과가 현행 추출과 갈리면 스로틀 WARNING 을 남기고,
                 401 원장 행의 주체 칸만 설계 §4-4 규칙(Bearer 비-API키 → ``oauth``, 키 지문 NULL)으로 적는다.
  - ``on``     — [2026-10-08 P3a] OAuth 토큰(`Bearer oqzat_…`)을 저장소로 검증해 계정 주체로 수락하고, 보호 도구
                 (`OAUTH_PROTECTED_TOOLS`, 기본 get_account_status)의 무자격 호출에 PRM 포인터 401 을 준다.
                 API 키 경로·익명 무료 도구는 off 와 같다. 무효 Bearer 는 401 invalid_token + PRM 포인터(설계 D3 #4).

⛔ 이 모듈은 자격증명 원문을 로그·원장에 남기지 않는다(``Credential.__repr__`` 가 가린다).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Mapping, Optional

logger = logging.getLogger("MarketMCP")

OAUTH_ACCESS_PREFIX = "oqzat_"  # 설계 §4-2: access 토큰 `oqzat_` + 32B base64url
API_KEY_PREFIX = "oqz_"         # api/marketplace/key_store.py::create_key — `oqz_` + uuid4 hex

# 현행 401 의 WWW-Authenticate. P3a(헤더 포인터) 전까지 `resource_metadata` 를 붙이지 않는다 —
# 동작하는 인가 서버 없이 PRM 위치를 알리면 클라이언트가 막다른 OAuth 흐름에 들어간다(설계 D4).
WWW_AUTHENTICATE_INVALID_TOKEN = 'Bearer realm="OneQAZ MCP", error="invalid_token"'

# [2026-10-08 P3a] on 허용 — P2(저장소·인가 서버)·G2(쓰기 권한)·G3(처리방침 게시) 완료 후.
_VALID_MODES = ("off", "shadow", "on")


def resolve_oauth_mode(raw: Optional[str] = None) -> str:
    """`MCP_OAUTH_MODE` 해석. 미인식 값은 ERROR 후 off (응답 동작이 바뀌는 쪽으로 넘어가지 않는다)."""
    value = (os.getenv("MCP_OAUTH_MODE", "off") if raw is None else raw).strip().lower() or "off"
    if value in _VALID_MODES:
        return value
    logger.error("MCP_OAUTH_MODE=%r 미인식 (허용: off|shadow|on) — off 로 동작한다", value)
    return "off"


OAUTH_MODE = resolve_oauth_mode()


@dataclass(frozen=True)
class Credential:
    """요청이 제시한 자격증명의 **종류**. 원문(secret)은 대조에만 쓰고 출력하지 않는다."""

    kind: str  # "anonymous" | "api_key" | "oauth" | "unknown_bearer"
    secret: str = field(default="", repr=False)
    source: Optional[str] = None  # "x-api-key" | "authorization" | None
    conflict: bool = False  # OAuth 토큰과 API 키가 함께 왔다 (설계: OAuth 우선)

    def __repr__(self) -> str:  # 원문 노출 방지 — 로그에 객체를 통째로 찍어도 안전하게
        return f"Credential(kind={self.kind!r}, source={self.source!r}, conflict={self.conflict}, len={len(self.secret)})"

    @property
    def ledger_principal_override(self) -> Optional[str]:
        """shadow 401 원장 행의 주체 칸. None = 현행 파생(키 지문 유무로 api_key/anonymous).

        설계 §4-4: Bearer 로 온 비-API키 값(무효 토큰 = 행렬 #4)은 'oauth' 로 귀속하고 키 지문을 비운다.
        단 원장은 **실제로 일어난 일**을 적는다 — 충돌(X-API-Key 동반)이면 현행은 X-API-Key 를 조회해
        거부했으므로 api_key 가 사실이고, `Basic …` 처럼 Bearer 가 아닌 값은 헤더 원문이 키로 조회돼
        거부됐으므로 역시 현행 파생이 사실이다. 이 차이는 원장이 아니라 shadow 로그(사유 코드)가 센다.
        """
        if self.kind in ("oauth", "unknown_bearer") and not self.conflict:
            return "oauth"
        return None


def legacy_api_key(headers: Mapping[str, str]) -> str:
    """현행(AUTH-001, 2026-09-23~) 추출 규칙 **그대로**. off 모드 바이트 동일성의 기준이다.

    X-API-Key 가 우선이고, 없으면 Authorization 에서 **대소문자 구분** `Bearer ` 접두를 떼어낸다
    (`bearer x`·`Basic x` 는 헤더 원문 전체가 키로 조회돼 401 이 된다 — 현행 동작).
    """
    return headers.get("x-api-key") or headers.get("authorization", "").removeprefix("Bearer ").strip()


def classify_credential(headers: Mapping[str, str]) -> Credential:
    """설계 §4-3 판별 순서 — 접두사로 먼저 가른다.

    1. `Authorization: Bearer oqzat_…` → oauth (X-API-Key 가 함께 오면 conflict, OAuth 우선)
    2. `X-API-Key: …`                  → api_key
    3. `Authorization: Bearer oqz_…`   → api_key (현행 호환)
    4. 그 외 Bearer 값                  → unknown_bearer (P2 이후 401 invalid_token)
    5. 없음                             → anonymous
    스킴은 RFC 7235 대로 대소문자를 가리지 않는다(현행 추출과 갈리는 지점 — shadow 가 이 차이를 센다).
    """
    xkey = (headers.get("x-api-key") or "").strip()
    scheme, _, rest = (headers.get("authorization") or "").strip().partition(" ")
    bearer = rest.strip() if scheme.lower() == "bearer" else ""

    if bearer.startswith(OAUTH_ACCESS_PREFIX):
        return Credential("oauth", bearer, "authorization", conflict=bool(xkey))
    if xkey:
        return Credential("api_key", xkey, "x-api-key")
    if bearer.startswith(API_KEY_PREFIX):
        return Credential("api_key", bearer, "authorization")
    if bearer:
        return Credential("unknown_bearer", bearer, "authorization")
    return Credential("anonymous")


def shadow_divergence(headers: Mapping[str, str], cred: Credential) -> Optional[str]:
    """on 모드였다면 현행과 다르게 처리됐을 사유. 같으면 None. (원문 미포함)"""
    legacy = legacy_api_key(headers)
    if cred.conflict:
        return "credential_conflict"  # 현행 = X-API-Key 우선 / 설계 = OAuth 우선
    if cred.kind == "oauth":
        return "oauth_token"  # 현행 = API 키로 조회 → 401
    if cred.kind == "unknown_bearer":
        return "non_api_key_bearer"  # 현행 = API 키로 조회 → 401 (설계도 401, 사유만 invalid_token)
    if cred.kind == "anonymous" and legacy:
        return "non_bearer_authorization"  # 예: Basic — 현행 = 401 / 설계 = 익명
    if cred.kind == "api_key" and cred.secret != legacy:
        return "scheme_case"  # 예: `bearer oqz_…` — 현행 = 헤더 원문 조회 → 401 / 설계 = 키 조회
    return None


_shadow_lock = threading.Lock()
_shadow_last: dict = {}
_SHADOW_LOG_INTERVAL_SEC = 600


def note_shadow_divergence(reason: str, ip: str) -> None:
    """사유별 10분 1회 WARNING (요청마다 찍으면 공격성 트래픽에 로그가 잠긴다)."""
    now = time.monotonic()
    with _shadow_lock:
        last, count = _shadow_last.get(reason, (0.0, 0))
        count += 1
        if now - last < _SHADOW_LOG_INTERVAL_SEC:
            _shadow_last[reason] = (last, count)
            return
        _shadow_last[reason] = (now, 0)
    logger.warning("[OAUTH-P1 shadow] 판별 차이 reason=%s (직전 창 누적 %d건, 최근 ip=%s) — 응답은 현행 유지", reason, count, ip)


# ─────────────────────────────────────────────────────────────────────────────
# P3a — 리소스 서버(on 모드): OAuth 토큰 수락 · PRM 포인터 401 · 보호 도구
#   설계 D3 행렬: #3 보호 대상 무자격 → 401 + resource_metadata(+scope, error 없음 — RFC 6750 §3.1)
#               #4 무효·만료 OAuth 토큰 → 401 error="invalid_token" + resource_metadata
#               #10 토큰 저장소 장애 → 보호 대상 503 / 그 외 익명 free 통과 + ERROR
#   D4 P3a: PRM 은 well-known 이 아니라 401 헤더 포인터의 URL 로만 알린다.
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class OAuthPrincipal:
    account_id: str
    oauth_client_id: str
    plan: str
    scopes: tuple
    expires_at: float


_oauth_store = None  # 테스트가 MemoryOAuthStore 로 바꿔 끼운다
_token_cache: dict = {}
_TOKEN_CACHE_TTL = 30.0  # 폐기 반영 지연 상한(설계 §4-2 "30초 이하")
_cache_lock = threading.Lock()
_last_store_error = 0.0


def _store():
    global _oauth_store
    if _oauth_store is None:
        from oneqaz_trading_mcp.oauth_store import PgOAuthStore
        _oauth_store = PgOAuthStore()
    return _oauth_store


def oauth_prm_url() -> str:
    iss = os.getenv("OAUTH_ISSUER", "https://api.oneqaz.com").rstrip("/")
    return os.getenv("OAUTH_PRM_URL", f"{iss}/oauth/protected-resource")


def protected_tools() -> frozenset:
    """계정 연결이 필요한 tool. P3a 시험 대상은 데이터 가치 0 인 get_account_status 하나(설계 §6 P3a)."""
    raw = os.getenv("OAUTH_PROTECTED_TOOLS", "get_account_status")
    return frozenset(t.strip() for t in raw.split(",") if t.strip())


def resolve_oauth_token(token: str) -> tuple:
    """(status, OAuthPrincipal|None). status ∈ valid / invalid / unavailable. 30초 캐시(유효·무효 모두)."""
    import hashlib

    global _last_store_error
    h = hashlib.sha256(token.encode("utf-8")).hexdigest()
    now = time.monotonic()
    with _cache_lock:
        hit = _token_cache.get(h)
        if hit and now - hit[0] < _TOKEN_CACHE_TTL:
            st, p = hit[1], hit[2]
            if st == "valid" and p is not None and p.expires_at < time.time():
                st, p = "invalid", None
            return st, p
    from oneqaz_trading_mcp.oauth_store import StoreUnavailable

    try:
        rec = _store().load_token(h, "access")
    except StoreUnavailable:
        if time.monotonic() - _last_store_error > 300:
            _last_store_error = time.monotonic()
            logger.error("[OAUTH] 토큰 저장소 조회 실패 — 보호 대상 503 / 그 외 익명 통과 (5분 스로틀)", exc_info=True)
        return "unavailable", None
    if (rec is None or rec.revoked or rec.grant_revoked or rec.account_status != "active"
            or rec.expires_at < time.time()):
        result = ("invalid", None)
    else:
        result = ("valid", OAuthPrincipal(rec.account_id, rec.oauth_client_id, rec.account_plan,
                                          tuple(rec.scopes), rec.expires_at))
    with _cache_lock:
        if len(_token_cache) > 5000:
            _token_cache.clear()
        _token_cache[h] = (now, result[0], result[1])
    return result


def build_oauth_401(error: Optional[str] = None):
    """보호 대상 무자격(error=None) 또는 무효 토큰(error='invalid_token') 401 — PRM 포인터 + disclaimer envelope."""
    from starlette.responses import JSONResponse

    from oneqaz_trading_mcp.resources.resource_response import DISCLAIMER_TEXT

    parts = [f'resource_metadata="{oauth_prm_url()}"', 'scope="oneqaz:read"']
    if error:
        parts.append(f'error="{error}"')
    reason = ("The access token is invalid, expired or revoked. Reconnect your OneQAZ account in your AI app."
              if error else
              "This tool needs a connected OneQAZ account. Connect OneQAZ in your AI app "
              "(free — no API key needed; Pro users can use their key) and try again. "
              "Other tools work without an account.")
    return JSONResponse(
        status_code=401,
        headers={"WWW-Authenticate": "Bearer " + ", ".join(parts)},
        content={
            "error": True,
            "error_code": "invalid_token" if error else "account_required",
            "reason": reason,
            "action": "connect_account",
            "retryable": True,
            "disclaimer": DISCLAIMER_TEXT,
            "is_investment_advice": False,
            "data_classification": "research_information_only",
        },
    )


def auth_failed_reason(key_status: str) -> str:
    # 사유 구분: 키는 128bit 난수(oqz_+uuid4)라 "존재했던 키" 를 알려줘도
    # 열거 공격 이득이 없고, 도입자에게는 오타와 만료가 전혀 다른 조치다.
    return {
        "expired": "The supplied API key has expired. Request a renewed key, or omit the key "
                   "entirely to use the free tier.",
        "inactive": "The supplied API key has been deactivated. Contact the operator, or omit "
                    "the key entirely to use the free tier.",
    }.get(key_status, (
        "The supplied API key was not recognized. "
        "Omit the key entirely to use the free tier — "
        "an unrecognized key is rejected rather than silently downgraded."
    ))


def build_auth_failed_401(key_status: str, www_authenticate: str = WWW_AUTHENTICATE_INVALID_TOKEN):
    """현행 API 키 거부 401 — 본문·헤더 바이트 동일(tests/validation/test_oauth_p1_principal.py 골든).

    `www_authenticate` 는 P3a 에서 `resource_metadata` 포인터를 붙일 자리다. P1 호출부는 기본값만 쓴다.
    """
    from starlette.responses import JSONResponse

    from oneqaz_trading_mcp.resources.resource_response import DISCLAIMER_TEXT

    return JSONResponse(
        status_code=401,
        headers={"WWW-Authenticate": www_authenticate},
        content={
            "error": True,
            "error_code": "auth_failed",
            "reason": auth_failed_reason(key_status),
            "action": "check_availability",
            "retryable": False,
            "disclaimer": DISCLAIMER_TEXT,
            "is_investment_advice": False,
            "data_classification": "research_information_only",
        },
    )
