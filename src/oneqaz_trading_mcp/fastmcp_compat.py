# -*- coding: utf-8 -*-
"""FastMCP 2.x ↔ 4.x 호환 헬퍼 (2026-10-07, MCP 2026-07-28 지원 트랙 사전 작업).

FastMCP 4.x 는 서버 카탈로그 API `get_tools()/get_resources()/get_resource_templates()`(dict 반환)를
`list_tools()/list_resources()/list_resource_templates()`(list 반환)로 바꿨다. 이 차이를 호출부가 모르게
**한 곳에서** 흡수한다. 종전 호출부는 예외를 `{}` 로 삼켜, 4.x 에서는 `market://meta/discovery` 카탈로그가
0건·fetch 가 "tool not found" 가 되는 **조용한 사망**이 예상됐다(업그레이드 영향 조사 10-07).

반환은 항상 2.x 형태(dict)다 — tools: {name: tool}, resources: {uri: resource}, templates: {uri_template: t}.
"""

from __future__ import annotations

import inspect
from typing import Any, Dict


async def _maybe_await(v: Any) -> Any:
    return await v if inspect.isawaitable(v) else v


async def _catalog(server: Any, getter: str, lister: str, key_attrs: tuple) -> Dict[str, Any]:
    if hasattr(server, getter):  # FastMCP 2.x — dict
        return dict(await _maybe_await(getattr(server, getter)()))
    if hasattr(server, lister):  # FastMCP 4.x — list
        items = await _maybe_await(getattr(server, lister)())
        out: Dict[str, Any] = {}
        for it in items or []:
            key = next((getattr(it, a) for a in key_attrs if getattr(it, a, None) is not None), None)
            if key is not None:
                out[str(key)] = it
        return out
    raise AttributeError(f"FastMCP 카탈로그 API 없음: {getter} / {lister}")


async def catalog_tools(server: Any) -> Dict[str, Any]:
    return await _catalog(server, "get_tools", "list_tools", ("name", "key"))


async def catalog_resources(server: Any) -> Dict[str, Any]:
    return await _catalog(server, "get_resources", "list_resources", ("uri", "key"))


async def catalog_resource_templates(server: Any) -> Dict[str, Any]:
    return await _catalog(server, "get_resource_templates", "list_resource_templates", ("uri_template", "key"))


def disable_banner(settings: Any) -> None:
    """배너 끄기 — 2.x `show_cli_banner` / 4.x `show_server_banner`.

    4.x Settings 는 validate_assignment=True 라 **없는 필드에 대입하면 ValidationError** 로 모듈 import 가
    죽는다(기동 실패). 실재하는 필드에만 대입한다.
    """
    fields = getattr(type(settings), "model_fields", {}) or {}
    for name in ("show_cli_banner", "show_server_banner"):
        if name in fields:
            setattr(settings, name, False)
