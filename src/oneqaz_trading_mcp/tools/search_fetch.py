# -*- coding: utf-8 -*-
"""
Search / Fetch Tools — ChatGPT 커넥터 표준 (2026-07-08)
========================================================
OpenAI ChatGPT 커넥터·Deep Research 는 정확히 `search` / `fetch` 라는 이름의
tool 2종을 요구하며 응답 top-level shape 이 고정이다
(https://platform.openai.com/docs/mcp → developers.openai.com/api/docs/mcp,
2026-07-08 WebFetch 재확인):

    search(query)  → {"results": [{"id", "title", "url"}]}
    fetch(id)      → {"id", "title", "text", "url", "metadata"(optional)}

두 tool 모두 structuredContent + JSON 직렬화 text content 를 병행 요구 —
FastMCP 는 dict 반환 시 둘 다 자동 생성하므로 여기서는 plain dict 만 반환한다.

이 표면이 없으면 ChatGPT 경로 자체가 막힌다 (30일 호출 0 실측).
부수 효과: Gemini 는 MCP resource 를 지원하지 않아 30+개 resource 가 불가시인데,
fetch 의 resource resolver 가 핵심 resource 데이터를 tool 표면으로 직접 서빙해
이 갭도 함께 메운다 (FastMCP 내부 read API 를 쓰지 않고 각 resource 의
underlying 데이터 빌더 함수를 직접 호출).

id 네임스페이스:
    tool:{name}                  → tool docstring 전문 + tools/call 호출 안내
    resource:{uri}               → 핵심 resource 의 실데이터 (resolver 커버 시)
    signal:{market}:{symbol}     → 해당 심볼 최신 combined 시그널

envelope 정책 (중요):
    wrap_tool_response 는 쓰지 않는다 — OpenAI 의 search/fetch 고정 top-level
    shape 과 full_data 중첩 구조가 양립 불가하기 때문. 대신 disclaimer /
    is_investment_advice / data_classification 을 top-level 에 병기하는
    "flat envelope" 로 CLAUDE.md 규칙 5(외부 노출 disclaimer envelope)의
    의도를 충족한다.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.resources.resource_response import DISCLAIMER_TEXT, to_resource_text

logger = logging.getLogger("MarketMCP")

_BASE_URL = "https://api.oneqaz.com/mcp"
_CORPUS_TTL_SEC = 600  # 코퍼스 인메모리 캐시 10분
_FETCH_CACHE_TTL = 120  # resource fetch 결과 캐시 (server cache 사용 가능 시)
_RESOLVE_TIMEOUT = 30  # 개별 resolver 최대 처리 시간 (초) — resource 핸들러와 동일

_SIGNAL_MARKETS = ("crypto", "kr_stock", "us_stock")
_SIGNAL_TOP_N = 20

# 등록 시점에 박히는 참조 — introspect_catalog 는 FastMCP 인스턴스가 필요하므로
# 코퍼스의 카탈로그 파트는 등록 후 첫 search 호출 때 lazy 빌드된다.
# (컨테이너 내부 직접 호출 테스트에서는 None 허용 — signals 파트만 빌드)
_MCP_INSTANCE: Any = None
_CACHE_REF: Any = None

# 코퍼스 인메모리 캐시 (모듈 수준 — server cache 와 독립, 테스트 경로에서도 동작)
_corpus_cache: Dict[str, Any] = {"built_at": 0.0, "entries": None}

# market id 정규화 (resolver / signal 경로 공용)
_MARKET_ALIASES = {
    "crypto": "crypto", "coin": "crypto", "coin_market": "crypto",
    "kr": "kr_stock", "kr_stock": "kr_stock", "kr_market": "kr_stock",
    "us": "us_stock", "us_stock": "us_stock", "us_market": "us_stock",
}

# 자주 쓰는 자연어 → 심볼 별칭 (검색 recall 보조, 최소한만)
_QUERY_ALIASES = {
    "bitcoin": "btc", "비트코인": "btc",
    "ethereum": "eth", "이더리움": "eth",
}


# ---------------------------------------------------------------------------
# Flat envelope — 규칙 5 의도 충족 (사유는 모듈 docstring 참조)
# ---------------------------------------------------------------------------

def _flat_envelope(payload: Dict[str, Any]) -> Dict[str, Any]:
    """OpenAI 고정 shape 위에 disclaimer 필드들을 top-level 병기."""
    out = dict(payload)
    out["disclaimer"] = DISCLAIMER_TEXT
    out["is_investment_advice"] = False
    out["data_classification"] = "research_information_only"
    return out


# ---------------------------------------------------------------------------
# 코퍼스 빌드 (catalog + strong signals)
# ---------------------------------------------------------------------------

async def _build_corpus(mcp_server: Any) -> List[Dict[str, Any]]:
    """검색 코퍼스 빌드.

    entry 구조: {id, title, url, name(스코어링용 주 텍스트), purpose, symbol(옵션)}
    """
    entries: List[Dict[str, Any]] = []

    # 1) tool / resource 카탈로그 (introspect_catalog 재사용 — 정적 목록 박지 않음)
    if mcp_server is not None:
        try:
            from oneqaz_trading_mcp.discovery_helpers import introspect_catalog
            catalog = await introspect_catalog(mcp_server)
            for cat_tools in (catalog.get("tools_by_category") or {}).values():
                for t in cat_tools:
                    name = t.get("name") or ""
                    if not name or name in ("search", "fetch"):
                        continue  # 자기 자신은 코퍼스에서 제외
                    entries.append({
                        "id": f"tool:{name}",
                        "title": f"{name} (OneQAZ tool)",
                        "url": f"{_BASE_URL}#tool:{name}",
                        "name": name,
                        "purpose": t.get("purpose") or "",
                    })
            for res in catalog.get("static_resources") or []:
                uri = res.get("uri") or ""
                if not uri:
                    continue
                entries.append({
                    "id": f"resource:{uri}",
                    "title": f"{uri} (OneQAZ resource)",
                    "url": f"{_BASE_URL}#resource:{uri}",
                    "name": uri,
                    "purpose": res.get("purpose") or "",
                })
            for tpl in catalog.get("template_resources") or []:
                # 템플릿은 fetch 가능한 concrete example uri 를 id 로 노출
                # (render_example 기본값: market_id=crypto, symbol=BTC 등)
                example = tpl.get("example") or ""
                uri_tpl = tpl.get("uri") or ""
                if not example or "<value>" in example:
                    continue
                entries.append({
                    "id": f"resource:{example}",
                    "title": f"{uri_tpl} (OneQAZ resource template, example={example})",
                    "url": f"{_BASE_URL}#resource:{example}",
                    "name": f"{uri_tpl} {example}",
                    "purpose": tpl.get("purpose") or "",
                })
        except Exception as e:
            # 카탈로그 실패해도 signals 코퍼스는 서빙 (graceful degrade)
            logger.warning("search corpus: catalog introspection failed: %s", e)

    # 2) 시장 3개의 combined 최신 강한 시그널 top-20 (_strong_signals_for_market 재사용)
    try:
        from oneqaz_trading_mcp.tools.daily_brief import _strong_signals_for_market
        for market in _SIGNAL_MARKETS:
            try:
                sigs = await asyncio.to_thread(_strong_signals_for_market, market, _SIGNAL_TOP_N)
            except Exception as e:
                logger.warning("search corpus: signals(%s) failed: %s", market, e)
                continue
            # 같은 심볼의 combined 행이 24h 창에 여러 개일 수 있음 — confidence
            # 내림차순 정렬이므로 첫 행(최고 확신)만 유지 (id 중복 방지, 실측 수리)
            seen_syms: set = set()
            for s in sigs:
                sym = s.get("symbol")
                if not sym or sym in seen_syms:
                    continue
                seen_syms.add(sym)
                action = (s.get("action") or "").upper()
                entries.append({
                    "id": f"signal:{market}:{sym}",
                    "title": f"{sym} {action} signal ({market})",
                    "url": f"{_BASE_URL}#signal:{market}:{sym}",
                    "name": sym,
                    "purpose": (
                        f"{market} latest combined {action} signal — "
                        f"score {s.get('signal_score')}, confidence {s.get('confidence')}"
                    ),
                    "symbol": sym,
                })
    except Exception as e:
        logger.warning("search corpus: signals block failed: %s", e)

    return entries


async def _get_corpus(force: bool = False) -> List[Dict[str, Any]]:
    """TTL 10분 인메모리 캐시 경유 코퍼스 조회."""
    now = time.time()
    if (
        not force
        and _corpus_cache["entries"] is not None
        and now - _corpus_cache["built_at"] < _CORPUS_TTL_SEC
    ):
        return _corpus_cache["entries"]
    entries = await _build_corpus(_MCP_INSTANCE)
    _corpus_cache["entries"] = entries
    _corpus_cache["built_at"] = now
    return entries


# ---------------------------------------------------------------------------
# search 스코어링
# ---------------------------------------------------------------------------

def _tokenize(query: str) -> List[str]:
    toks = [t for t in re.split(r"[^a-z0-9가-힣_\-]+", (query or "").lower()) if t]
    return [_QUERY_ALIASES.get(t, t) for t in toks]


def _search_impl(query: str, entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """소문자 토큰 부분일치 스코어링 — name 3점, purpose 1점, 심볼 정확일치 5점."""
    tokens = _tokenize(query)
    if not tokens:
        return []
    scored: List[tuple] = []
    for e in entries:
        name = (e.get("name") or "").lower()
        purpose = (e.get("purpose") or "").lower()
        symbol = (e.get("symbol") or "").lower()
        score = 0
        for tok in tokens:
            if symbol and tok == symbol:
                score += 5
            if tok in name:
                score += 3
            elif tok in purpose:
                score += 1
        if score > 0:
            scored.append((score, e))
    scored.sort(key=lambda x: (-x[0], x[1]["id"]))
    return [
        {"id": e["id"], "title": e["title"], "url": e["url"]}
        for _score, e in scored[:10]
    ]


# ---------------------------------------------------------------------------
# fetch — resource resolver (Gemini 갭 메우기 핵심)
# ---------------------------------------------------------------------------
# 각 resource 의 등록 핸들러 내부가 호출하는 데이터 빌더 함수를 직접 import.
# FastMCP 내부 read API 사용 금지 (정책). 커버 목록은 확실히 매핑된 것만.

async def _resolve_resource(uri: str) -> Optional[Dict[str, Any]]:
    """커버된 resource uri → 실데이터 dict. 미커버는 None."""
    if uri == "market://global/summary":
        from oneqaz_trading_mcp.resources.global_regime import _load_global_regime_summary
        return await asyncio.to_thread(_load_global_regime_summary)

    if uri == "market://global/macro_events":
        from oneqaz_trading_mcp.resources.external_context import _load_macro_events
        return await asyncio.to_thread(_load_macro_events)

    if uri == "market://indicators/fear-greed":
        from oneqaz_trading_mcp.resources.indicators import _load_fear_greed_index
        return await asyncio.to_thread(_load_fear_greed_index)

    if uri == "market://indicators/regime":
        from oneqaz_trading_mcp.resources.indicators import _load_market_regime_analysis
        return await asyncio.to_thread(_load_market_regime_analysis)

    if uri == "market://indicators/context":
        from oneqaz_trading_mcp.resources.indicators import _load_market_context
        return await asyncio.to_thread(_load_market_context)

    if uri == "market://structure/all":
        from oneqaz_trading_mcp.resources.market_structure import read_all_market_structures
        txt = await asyncio.to_thread(read_all_market_structures)
        return json.loads(txt)

    if uri == "market://unified/cross-market":
        from oneqaz_trading_mcp.resources.unified_context import _build_cross_market_context
        return await asyncio.to_thread(_build_cross_market_context)

    m = re.match(r"^market://global/category/([\w\-]+)$", uri)
    if m:
        from oneqaz_trading_mcp.resources.global_regime import _load_category_analysis
        return await asyncio.to_thread(_load_category_analysis, m.group(1))

    m = re.match(r"^market://([\w\-]+)/structure$", uri)
    if m:
        market = _MARKET_ALIASES.get(m.group(1).lower())
        if market:
            from oneqaz_trading_mcp.resources.market_structure import _load_structure_summary
            return await asyncio.to_thread(_load_structure_summary, market)

    m = re.match(r"^market://([\w\-]+)/unified$", uri)
    if m:
        market = _MARKET_ALIASES.get(m.group(1).lower())
        if market:
            from oneqaz_trading_mcp.resources.unified_context import _abuild_full_unified_context
            return await _abuild_full_unified_context(market)

    m = re.match(r"^market://([\w\-]+)/external/summary$", uri)
    if m:
        market = _MARKET_ALIASES.get(m.group(1).lower())
        if market:
            from oneqaz_trading_mcp.resources.external_context import _load_external_summary
            return await asyncio.to_thread(_load_external_summary, market)

    return None


async def _fetch_resource_doc(uri: str) -> Dict[str, Any]:
    doc_id = f"resource:{uri}"
    url = f"{_BASE_URL}#resource:{uri}"

    # server cache 가 있으면 짧게 캐시 (동일 uri 반복 fetch 보호)
    cache_key = f"searchfetch_res_{uri}"
    if _CACHE_REF is not None:
        cached = _CACHE_REF.get(cache_key, ttl=_FETCH_CACHE_TTL)
        if cached:
            return cached

    data: Optional[Dict[str, Any]] = None
    try:
        data = await asyncio.wait_for(_resolve_resource(uri), timeout=_RESOLVE_TIMEOUT)
    except asyncio.TimeoutError:
        return _flat_envelope({
            "id": doc_id,
            "title": f"{uri} (timeout)",
            "text": json.dumps({
                "error": True, "error_code": "timeout",
                "reason": f"resource resolve timed out ({_RESOLVE_TIMEOUT}s) — retry later",
            }, ensure_ascii=False),
            "url": url,
            "metadata": {"kind": "resource", "uri": uri, "error": "timeout"},
        })
    except Exception as e:
        logger.warning("fetch resource resolve failed for %s: %s", uri, e)
        return _flat_envelope({
            "id": doc_id,
            "title": f"{uri} (error)",
            "text": json.dumps({
                "error": True, "error_code": "internal_error",
                "reason": f"resource resolve failed: {str(e)[:200]}",
            }, ensure_ascii=False),
            "url": url,
            "metadata": {"kind": "resource", "uri": uri, "error": "internal_error"},
        })

    if data is not None:
        result = _flat_envelope({
            "id": doc_id,
            "title": f"{uri} (OneQAZ resource)",
            "text": to_resource_text(data),
            "url": url,
            "metadata": {
                "kind": "resource",
                "uri": uri,
                "source": "oneqaz_live_builder",
            },
        })
        if _CACHE_REF is not None:
            _CACHE_REF.set(cache_key, result, ttl=_FETCH_CACHE_TTL)
        return result

    # 미커버 resource — 카탈로그 purpose 라도 서빙 (graceful degrade, 추측 데이터 금지)
    purpose = ""
    try:
        for e in await _get_corpus():
            if e["id"] == doc_id:
                purpose = e.get("purpose") or ""
                break
    except Exception:
        pass
    return _flat_envelope({
        "id": doc_id,
        "title": f"{uri} (OneQAZ resource — description only)",
        "text": json.dumps({
            "uri": uri,
            "purpose": purpose or "(no description available)",
            "note": (
                "This resource is not covered by the fetch resolver. "
                "MCP-capable clients can read it directly via the standard "
                "resources/read method on " + _BASE_URL + "."
            ),
        }, ensure_ascii=False),
        "url": url,
        "metadata": {"kind": "resource", "uri": uri, "resolver_coverage": False},
    })


# ---------------------------------------------------------------------------
# fetch — tool docstring
# ---------------------------------------------------------------------------

async def _fetch_tool_doc(name: str) -> Dict[str, Any]:
    doc_id = f"tool:{name}"
    url = f"{_BASE_URL}#tool:{name}"
    if _MCP_INSTANCE is None:
        return _flat_envelope({
            "id": doc_id,
            "title": f"{name} (catalog unavailable)",
            "text": json.dumps({
                "error": True, "error_code": "no_data",
                "reason": "tool catalog unavailable in this context",
            }, ensure_ascii=False),
            "url": url,
            "metadata": {"kind": "tool", "error": "catalog_unavailable"},
        })
    try:
        tools = await _MCP_INSTANCE.get_tools()
    except Exception as e:
        tools = {}
        logger.warning("fetch tool doc: get_tools failed: %s", e)
    tool = tools.get(name)
    if tool is None:
        return _flat_envelope({
            "id": doc_id,
            "title": f"{name} (not found)",
            "text": json.dumps({
                "error": True, "error_code": "no_data",
                "reason": f"no tool named '{name}' — use the search tool to discover valid ids",
            }, ensure_ascii=False),
            "url": url,
            "metadata": {"kind": "tool", "error": "not_found"},
        })
    desc = getattr(tool, "description", None) or "(no description)"
    text = (
        f"OneQAZ MCP tool: {name}\n\n"
        f"{desc}\n\n"
        f"How to call: invoke via the MCP JSON-RPC method tools/call on {_BASE_URL} "
        f"with {{\"name\": \"{name}\", \"arguments\": {{...}}}}. "
        "Every response carries a disclaimer — research information only, not investment advice."
    )
    return _flat_envelope({
        "id": doc_id,
        "title": f"{name} (OneQAZ tool)",
        "text": text,
        "url": url,
        "metadata": {"kind": "tool", "name": name},
    })


# ---------------------------------------------------------------------------
# fetch — 최신 combined 시그널
# ---------------------------------------------------------------------------

def _latest_combined_signal(market: str, symbol: str) -> Optional[Dict[str, Any]]:
    """해당 심볼의 최신 combined 시그널 1건 (PG)."""
    from oneqaz_trading_mcp.tools.daily_brief import _market_to_schema, _safe_float
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    schema = _market_to_schema(market)
    conn = open_schema_connection(schema, readonly=True)
    try:
        row = conn.execute(
            "SELECT symbol, interval, action, signal_score, confidence, "
            "current_price, timestamp FROM signals "
            "WHERE symbol = ? AND interval = 'combined' "
            "ORDER BY timestamp DESC LIMIT 1",
            (symbol.upper(),),
        ).fetchone()
        if row is None:
            return None
        return {
            "market_id": market,
            "symbol": row["symbol"] if isinstance(row, dict) else row[0],
            "interval": row["interval"] if isinstance(row, dict) else row[1],
            "action": row["action"] if isinstance(row, dict) else row[2],
            "signal_score": _safe_float(row["signal_score"] if isinstance(row, dict) else row[3]),
            "confidence": _safe_float(row["confidence"] if isinstance(row, dict) else row[4]),
            "price": _safe_float(row["current_price"] if isinstance(row, dict) else row[5]),
            "timestamp": int(row["timestamp"] if isinstance(row, dict) else row[6]),
        }
    finally:
        conn.close()


async def _fetch_signal_doc(rest: str) -> Dict[str, Any]:
    doc_id = f"signal:{rest}"
    url = f"{_BASE_URL}#signal:{rest}"
    parts = rest.split(":", 1)
    market_raw = parts[0] if parts else ""
    symbol = parts[1] if len(parts) > 1 else ""
    market = _MARKET_ALIASES.get(market_raw.lower())
    if not market or not symbol:
        return _flat_envelope({
            "id": doc_id,
            "title": "invalid signal id",
            "text": json.dumps({
                "error": True, "error_code": "invalid_symbol",
                "reason": "signal id must be 'signal:{market}:{symbol}' with market in "
                          "crypto / kr_stock / us_stock",
            }, ensure_ascii=False),
            "url": url,
            "metadata": {"kind": "signal", "error": "invalid_id"},
        })
    try:
        sig = await asyncio.to_thread(_latest_combined_signal, market, symbol)
    except Exception as e:
        logger.warning("fetch signal(%s:%s) failed: %s", market, symbol, e)
        return _flat_envelope({
            "id": doc_id,
            "title": f"{symbol} signal ({market}) — error",
            "text": json.dumps({
                "error": True, "error_code": "internal_error",
                "reason": f"signal query failed: {str(e)[:200]}",
            }, ensure_ascii=False),
            "url": url,
            "metadata": {"kind": "signal", "error": "internal_error"},
        })
    if sig is None:
        return _flat_envelope({
            "id": doc_id,
            "title": f"{symbol} signal ({market}) — no data",
            "text": json.dumps({
                "error": True, "error_code": "no_data",
                "reason": f"no combined signal found for {symbol} in {market}",
            }, ensure_ascii=False),
            "url": url,
            "metadata": {"kind": "signal", "market": market, "symbol": symbol, "error": "no_data"},
        })
    action = (sig.get("action") or "").upper()
    return _flat_envelope({
        "id": doc_id,
        "title": f"{sig.get('symbol')} {action} signal ({market})",
        "text": to_resource_text(sig),
        "url": url,
        "metadata": {
            "kind": "signal",
            "market": market,
            "symbol": sig.get("symbol"),
            "note": "paper-trading research signal, not a buy/sell recommendation",
        },
    })


# ---------------------------------------------------------------------------
# fetch — 네임스페이스 라우팅
# ---------------------------------------------------------------------------

async def _fetch_impl(doc_id: str) -> Dict[str, Any]:
    did = (doc_id or "").strip()
    if did.startswith("tool:"):
        return await _fetch_tool_doc(did[len("tool:"):])
    if did.startswith("resource:"):
        return await _fetch_resource_doc(did[len("resource:"):])
    if did.startswith("signal:"):
        return await _fetch_signal_doc(did[len("signal:"):])
    # 미지 id — fetch 고정 shape 을 유지한 채 명확한 에러
    return _flat_envelope({
        "id": did,
        "title": "unknown id",
        "text": json.dumps({
            "error": True, "error_code": "no_data",
            "reason": (
                "id must start with 'tool:', 'resource:' or 'signal:' — "
                "obtain valid ids from the search tool first"
            ),
        }, ensure_ascii=False),
        "url": _BASE_URL,
        "metadata": {"error": "unknown_id_namespace"},
    })


# ---------------------------------------------------------------------------
# Register
# ---------------------------------------------------------------------------

def register_search_fetch_tools(mcp, cache):
    """ChatGPT 커넥터 표준 search / fetch tool 등록."""
    global _MCP_INSTANCE, _CACHE_REF
    _MCP_INSTANCE = mcp
    _CACHE_REF = cache

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def search(query: str) -> Dict[str, Any]:
        """
        Purpose: ChatGPT-connector-standard discovery search over OneQAZ's live surface —
            tools, resources, and the latest strong combined signals across crypto /
            kr_stock / us_stock. Returns result ids consumable by the `fetch` tool.
        Triggers: ChatGPT connectors and Deep Research call this automatically for any
            user query routed to OneQAZ ("bitcoin signal", "prediction accuracy",
            "korean stocks today", ...). Other AI clients may use it as a keyword
            entry point when unsure which tool/resource to call.
        When to call: first step of connector-style discovery. MCP-native clients can
            instead browse tools/list + resources/list directly.
        Prerequisites: none.
        Next steps: pass any result id to `fetch` for the full document.
        Caveats: corpus is rebuilt at most every 10 minutes (tool/resource catalog +
            top-20 strong signals per market). Empty results list means no match.
        Output: {results: [{id, title, url}], disclaimer, is_investment_advice,
            data_classification} — flat envelope, OpenAI fixed shape.

        Args:
            query: free-text search string (English/Korean, symbols like BTC/AAPL)

        Disclaimer: Information only, not investment advice.
        """
        entries = await _get_corpus()
        results = _search_impl(query, entries)
        return _flat_envelope({"results": results})

    @mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True, "idempotentHint": True})
    async def fetch(id: str) -> Dict[str, Any]:
        """
        Purpose: ChatGPT-connector-standard document fetch by id from `search` results.
            Namespaces: `tool:{name}` returns the tool's full documentation and how to
            call it; `resource:{uri}` returns the resource's live data (core resources
            resolved server-side — also the bridge for clients without MCP resource
            support, e.g. Gemini); `signal:{market}:{symbol}` returns the symbol's
            latest combined research signal.
        Triggers: ChatGPT connectors / Deep Research call this after `search`. Clients
            without MCP resource support can call it directly with a known resource id,
            e.g. fetch("resource:market://global/summary").
        When to call: whenever the full content behind a search result id is needed.
        Prerequisites: a valid id — from `search` results or a known namespace id.
        Next steps: for tool docs, call the named tool via tools/call; for signals,
            get_signal_detail / explain_decision for deeper evidence.
        Caveats: uncovered resource uris return description-only text (no fabricated
            data). `text` is a JSON document for resource/signal ids.
        Output: {id, title, text, url, metadata, disclaimer, is_investment_advice,
            data_classification} — flat envelope, OpenAI fixed shape.

        Args:
            id: document id — "tool:{name}", "resource:{uri}", or
                "signal:{market}:{symbol}" (market: crypto / kr_stock / us_stock)

        Disclaimer: Information only, not investment advice.
        """
        return await _fetch_impl(id)

    logger.info("  [OK] Search/Fetch tools registered (ChatGPT connector standard, 2 tools)")
