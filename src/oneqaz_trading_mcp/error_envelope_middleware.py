# -*- coding: utf-8 -*-
"""[2026-09-23 ERR-001] tool 호출 에러를 envelope 계약으로 통일하는 미들웨어.

## 왜 필요한가

FastMCP 는 tool 함수를 부르기 **전에** inputSchema 로 인자를 검증한다. 이 단계에서
난 에러는 우리 `mcp_error()` 를 거치지 않으므로 envelope 이 통째로 빠진다.

실측(2026-09-23, `get_signals` 를 인자 없이 호출):
```
isError=True · structuredContent=None
disclaimer 없음 · request_id 없음 · error_code 없음
본문: "1 validation error for call[get_signals] market_id Missing required argument
       [type=missing_argument, input_value={}, input_type=dict] For further information ..."
```
- **컴플라이언스**: disclaimer 가 빠진 응답이 외부로 나간다. 금지항목 5 는 응답 쪽
  규정이고 에러 응답도 응답이다 (2026-08-18 에 `mcp_error()` 는 같은 이유로 수리됐는데,
  **검증 단계는 그 수리의 사각지대**였다).
- **외부 계약**: 도입자가 분기하는 `error_code` 가 없어 오호출을 프로그램으로 구분할 수 없다.
- **정보 노출**: pydantic 내부 표현(`input_value`, `type=missing_argument`, 문서 URL)이
  그대로 나간다.

## 왜 ToolResult 가 아니라 ToolError 인가

`ToolResult(content, structured_content)` 로 돌려주면 `isError` 가 **False** 가 된다 —
실패한 호출을 성공으로 위장하는 것이고, 이 레포에서 제일 비싼 버그 유형(유효해 보이는
값을 돌려주는 것)을 스스로 만드는 꼴이다. `ToolError` 를 던지면 `isError=true` 가
유지되면서 본문만 우리 envelope 으로 바뀐다.
"""
from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger("MarketMCP")

try:
    from fastmcp.server.middleware import Middleware
    from fastmcp.exceptions import ToolError
    _AVAILABLE = True
except ImportError:  # pragma: no cover
    Middleware = object  # type: ignore[assignment,misc]
    ToolError = Exception  # type: ignore[assignment,misc]
    _AVAILABLE = False

from oneqaz_trading_mcp.resources.resource_response import (
    MCPErrorAction,
    MCPErrorCode,
    mcp_error,
)

# pydantic 에러 type → 우리 안정 식별자
_PYDANTIC_TYPE_MAP = {
    "missing_argument": MCPErrorCode.MISSING_REQUIRED_FIELD,
    "missing": MCPErrorCode.MISSING_REQUIRED_FIELD,
    "unexpected_keyword_argument": MCPErrorCode.INVALID_PARAMETER,
    "extra_forbidden": MCPErrorCode.INVALID_PARAMETER,
}


def _clean_validation_error(exc: Exception) -> tuple[Any, str, list]:
    """pydantic ValidationError 를 외부용 문구로 정제.

    내부 표현(input_value / 문서 URL / call[...] 래퍼)은 싣지 않는다.
    """
    # [2026-10-07] FastMCP 4.x 는 인자 검증 실패를 fastmcp.exceptions.ValidationError 로 감싸고 원인
    # pydantic ValidationError 를 __cause__ 에 둔다(.errors() 없음). 원인 사슬을 따라 오류 목록을 찾는다.
    # 끝내 못 찾으면 str(exc) 를 내보내지 않는다 — pydantic 내부 표현(input_value·URL)이 외부로 새던 경로다.
    errors = None
    cur, hops = exc, 0
    while cur is not None and hops < 4:
        fn = getattr(cur, "errors", None)
        if callable(fn):
            try:
                errors = list(fn())
                break
            except Exception:
                pass
        cur = cur.__cause__ or cur.__context__
        hops += 1
    if errors is None:
        return MCPErrorCode.INVALID_PARAMETER, "invalid arguments for this tool (see tools/list inputSchema)", []

    code = MCPErrorCode.INVALID_PARAMETER
    parts = []
    params = []
    for e in errors:
        loc = ".".join(str(x) for x in (e.get("loc") or ()) if x != "call")
        etype = str(e.get("type") or "")
        mapped = _PYDANTIC_TYPE_MAP.get(etype)
        if mapped is not None:
            code = mapped
        msg = str(e.get("msg") or "invalid value")
        parts.append((loc + ": " + msg) if loc else msg)
        if loc:
            params.append(loc)
    return code, "; ".join(parts)[:400], params


class EnvelopeErrorMiddleware(Middleware):  # type: ignore[misc]
    """tool 호출 경로의 모든 예외를 envelope 계약으로 변환한다."""

    async def on_call_tool(self, context, call_next):  # type: ignore[no-untyped-def]
        try:
            return await call_next(context)
        except ToolError:
            # tool 이 의도적으로 던진 것 — 이미 계약을 통과했거나 자체 문구가 있다
            raise
        except Exception as exc:
            tool_name = getattr(getattr(context, "message", None), "name", None) or "unknown"
            if type(exc).__name__ == "ValidationError":
                code, reason, params = _clean_validation_error(exc)
                # 검증 실패는 클라이언트 입력 문제 — 운영 오류가 아니므로 warning
                logger.warning("[ERR-001] %s 인자 검증 실패: %s", tool_name, reason)
                extra = {"tool": tool_name}
                if params:
                    extra["parameters"] = params
                envelope = mcp_error(
                    code, reason,
                    action=MCPErrorAction.CHECK,
                    action_value="tools/list",
                    **extra,
                )
            else:
                # 코드 결함 — 스택을 로그에 남기되 외부에는 내부 표현을 싣지 않는다
                logger.error("[ERR-001] %s 실행 중 예외", tool_name, exc_info=True)
                envelope = mcp_error(
                    MCPErrorCode.INTERNAL_ERROR,
                    f"{type(exc).__name__} while executing {tool_name}",
                    tool=tool_name,
                )
            raise ToolError(json.dumps(envelope, ensure_ascii=False, default=str)) from None


def install(server) -> bool:
    """서버에 미들웨어를 건다. 실패해도 서버 기동은 막지 않는다."""
    if not _AVAILABLE:
        logger.warning("[ERR-001] fastmcp middleware 미지원 — envelope 통일 건너뜀")
        return False
    try:
        server.add_middleware(EnvelopeErrorMiddleware())
        logger.info("  [OK] ERR-001 envelope 에러 미들웨어 등록")
        return True
    except Exception:
        logger.error("[ERR-001] 미들웨어 등록 실패", exc_info=True)
        return False
