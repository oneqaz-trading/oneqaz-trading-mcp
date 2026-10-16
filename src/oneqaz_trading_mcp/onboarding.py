# -*- coding: utf-8 -*-
"""MCP 진입점 정규화 + 유입 표시(ref) — req_mcp_onboarding Part A (2026-10-16).

근거: `ai_brain/main_pc_tasks/proposal_mcp_onboarding_phase1_2026_10_16.md` OD2 · OD6
(진단 = `diag_mcp_onboarding_phase0_2026_10_16.md` F1 · F5 · §7).

## 1. `/mcp/` 끝 슬래시 (OD2)

FastMCP 는 MCP 를 `Route("/mcp")` 정확 일치로 등록한다. `/mcp/` 로 오면 Starlette 라우터가
`redirect_slashes` 로 307 을 내는데, 터널 뒤라 앱이 원 scheme 을 모르므로 **`Location: http://…`** 가 된다
(2026-10-16 외부 실측). 끝 슬래시를 붙이는 클라이언트는 조용히 평문으로 강등됐다.
→ 라우팅 전에 경로를 `/mcp` 로 바꿔 **리다이렉트 자체를 없앤다.** 다른 경로(`/mcp/server-card` 등)는 건드리지 않는다.

## 2. 유입 표시 `ref` (OD6)

연결 URL 의 쿼리 `?ref=<id>` 를 **고정 목록**으로만 기록한다 — 목록 밖 값은 `other`, 쿼리가 없으면 `direct`.
임의 문자열을 원장에 남기지 않기 위해서다(처리방침 §02 고지 범위). 경로 별칭(`/mcp/r/<id>`)을 쓰지 않는 이유:
OAuth 클라이언트는 연결 URL 을 `resource` 로 보내고 우리 인가 서버(`oauth_as._same_resource`)는 **경로**를
비교하므로 claude.ai 연결이 `invalid_target` 으로 깨진다. 쿼리는 비교에서 빠진다.
⚠️ VS Code 는 쿼리의 `=` 를 `%3D` 로 인코딩하는 버그가 있다(microsoft/vscode#289129) — 한 번 디코드한 원문도 본다.
⛔ 목록을 넓히면 oneqaz.com/privacy 고지와 같은 커밋에서 바꿀 것.
"""

from __future__ import annotations

import re
from typing import Union
from urllib.parse import parse_qsl, unquote

REF_DIRECT = "direct"
REF_OTHER = "other"

# 등록처·안내 페이지 id. 값의 의미는 ai_brain/main_pc_tasks/proposal_mcp_onboarding_phase1_2026_10_16.md §2-3.
KNOWN_REFS = frozenset({
    "quickstart",   # oneqaz.com/quickstart (예정) — 퀵스타트 연결 안내
    "home",         # oneqaz.com 홈 #connect
    "llms",         # llms.txt
    "official",     # 공식 MCP Registry
    "pypi",         # PyPI 패키지 설명
    "github",       # GitHub 공개 레포 README
    "mcpso",        # mcp.so
    "glama",        # Glama
    "smithery",     # Smithery
    "pulsemcp",     # PulseMCP
    "lobehub",      # LobeHub
    "g2",           # G2 AI Marketplace
    "awesome",      # awesome-mcp-servers 류 GitHub 목록
})

_REF_RE = re.compile(r"^[a-z0-9]{1,16}$")


def _first_ref(qs: str) -> Union[str, None]:
    for k, v in parse_qsl(qs, keep_blank_values=True):
        if k.strip().lower() == "ref":
            return v
    return None


def resolve_ref(query_string: Union[bytes, str, None]) -> str:
    """요청 쿼리스트링 → `direct` / KNOWN_REFS 의 id / `other`."""
    if not query_string:
        return REF_DIRECT
    if isinstance(query_string, bytes):
        query_string = query_string.decode("latin-1")
    raw = _first_ref(query_string)
    if raw is None and "%3d" in query_string.lower():
        raw = _first_ref(unquote(query_string))   # VS Code `ref%3Dglama`
    if raw is None:
        return REF_DIRECT
    v = raw.strip().lower()
    if _REF_RE.match(v) and v in KNOWN_REFS:
        return v
    return REF_OTHER


class McpPathNormalizeMiddleware:
    """`/mcp/` → `/mcp` (리다이렉트 없이). 순수 ASGI — 본문·응답을 건드리지 않는다.

    `_middleware_list` 의 맨 앞(사용자 미들웨어 중 가장 바깥)에 둔다. 라우팅은 가장 안쪽(Router)이므로
    여기서 바꾼 경로로 매칭된다.
    """

    _TRAILING = "/mcp/"

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http" and scope.get("path") == self._TRAILING:
            scope = dict(scope)
            scope["path"] = "/mcp"
            scope["raw_path"] = b"/mcp"
        await self.app(scope, receive, send)
