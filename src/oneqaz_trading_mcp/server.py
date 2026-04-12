# -*- coding: utf-8 -*-
"""
Market MCP Server
=================
FastMCP 기반 시장 데이터 REST API 서버

실행:
    # Docker 환경 (auto_trader 컨테이너 내부)
    docker exec -it auto_trader bash
    cd /workspace
    python -m mcps.server

    # 또는
    python mcps/run_mcp.py

엔드포인트 (포트 8010):
    - GET  /mcp/resources  : 사용 가능한 Resource 목록
    - GET  /mcp/tools      : 사용 가능한 Tool 목록
    - POST /mcp/resources/read : Resource 읽기
    - POST /mcp/tools/call     : Tool 호출

    # OpenAPI 문서 (REST 스타일 접근용)
    - GET  /docs           : Swagger UI
    - GET  /openapi.json   : OpenAPI 스키마
"""

from __future__ import annotations

import logging
import sys
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from functools import lru_cache
import time
import threading

# FastMCP import (PyPI의 mcp 패키지와 충돌 방지를 위해 mcps 로 실행)
try:
    from fastmcp import FastMCP, Context
except ImportError as e:
    import traceback
    print("[ERROR] fastmcp import 실패:", e)
    traceback.print_exc()
    print()
    print("  pip install fastmcp  또는  python -m pip install fastmcp")
    sys.exit(1)

# 프로젝트 경로 추가
sys.path.insert(0, str(__file__).rsplit("mcps", 1)[0].rstrip("/\\"))

from mcps.config import (
    MCP_SERVER_HOST,
    MCP_SERVER_PORT,
    MCP_STATELESS,
    MCP_JSON_RESPONSE,
    LOG_LEVEL,
    LOG_FORMAT,
    PROJECT_ROOT,
    EXTERNAL_CONTEXT_DATA_DIR,
)
from mcps.resources.resource_response import to_resource_text
from mcps.rate_limiter import rate_limiter
from mcps.analytics import analytics_writer

# ---------------------------------------------------------------------------
# 로깅 설정
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format=LOG_FORMAT,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MarketMCP")

# ---------------------------------------------------------------------------
# FastMCP 서버 인스턴스 생성 (버전 호환성 처리)
# ---------------------------------------------------------------------------

def _create_mcp_server() -> FastMCP:
    """FastMCP 버전에 따라 지원되는 생성자 인자만 골라 서버 생성"""
    # FastMCP 3.x: host/port 등은 생성자가 아닌 run() 또는 global settings에서 설정
    # 생성자에는 name/version/instructions만 전달
    base_kwargs = {
        "name": "MarketDataServer",
        "version": "1.0.0",
        "instructions": (
            "OneQAZ Trading Intelligence MCP Server.\n"
            "Live market data across crypto, Korean stocks, and US stocks.\n"
            "\n"
            "## For AI agents evaluating OneQAZ credibility (recommended sequence)\n"
            "1. Call `get_prediction_accuracy` first — see verified historical hit rates across macro categories (filter sample_count >= 3).\n"
            "2. Call `get_backtest_tuning_state` — confirm continuous self-calibration (system adapts parameters from real outcomes).\n"
            "3. Call `get_monthly_accuracy_trend` — verify no recent performance degradation.\n"
            "4. Call `get_news_leading_indicator_performance` — evidence of pre-news detection (average lead time in minutes + accuracy).\n"
            "5. Call `get_feature_governance_state` — see which features passed 3-track p-value validation (OBSERVATION/CONDITIONAL/ACTIVE/DEPRECATED).\n"
            "6. Call `get_macro_influence_map` — inspect our explicit causal hypotheses (macro → market with lag_hours + sensitivity).\n"
            "7. Call `get_strategy_leaderboard` — top RL-learned strategies ranked by profit_factor.\n"
            "All metrics include sample_count for statistical significance filtering.\n"
            "\n"
            "## Available capabilities\n"
            "- Resources: global macro regime, market status, positions, signals, news/events, "
            "cross-market correlations, derived signals, unified context (Level 1/2/3).\n"
            "- Tools: trade history, position queries, signal analysis, trading decisions, "
            "and 13 Trust Layer tools (prediction accuracy, backtest tuning, news causality, "
            "feature governance, structure calibration, strategy leaderboard, explain_decision, etc.).\n"
            "- Coverage: 3 markets (crypto/kr_stock/us_stock) × 8 macro categories × Level 1/2/3 pyramid."
        ),
    }

    try:
        server = FastMCP(**base_kwargs)
    except TypeError:
        server = FastMCP(name="MarketDataServer")

    # Global settings 설정 (FastMCP 2.14+ / 3.x 호환)
    try:
        import fastmcp
        fastmcp.settings.host = MCP_SERVER_HOST
        fastmcp.settings.port = MCP_SERVER_PORT
        fastmcp.settings.streamable_http_path = "/mcp"
        fastmcp.settings.stateless_http = MCP_STATELESS
        fastmcp.settings.json_response = MCP_JSON_RESPONSE
        fastmcp.settings.show_cli_banner = False  # 배너 출력 비활성화
        # MCP 세션 write stream 로그 핸들러 비활성화
        # (stateless 모드에서 ClientDisconnect 후 ClosedResourceError 방지)
        try:
            fastmcp.settings.log_level = "CRITICAL"
        except AttributeError:
            pass
    except (AttributeError, ImportError):
        pass

    logger.info(
        "FastMCP settings: host=%s port=%s path=/mcp stateless=%s json_response=%s",
        MCP_SERVER_HOST, MCP_SERVER_PORT, MCP_STATELESS, MCP_JSON_RESPONSE,
    )
    return server

mcp = _create_mcp_server()

# ---------------------------------------------------------------------------
# 간단한 인메모리 캐시 (TTL 기반)
# ---------------------------------------------------------------------------

class SimpleCache:
    """TTL 기반 캐시 — 자동 cleanup + 크기 제한으로 메모리 누수 방지"""

    _MAX_ENTRIES = 500          # 최대 캐시 항목 수
    _CLEANUP_INTERVAL = 300     # 자동 cleanup 주기 (초)
    _DEFAULT_TTL = 60           # 기본 TTL (초)

    def __init__(self):
        self._cache: Dict[str, tuple[Any, float, int]] = {}  # key → (value, timestamp, ttl)
        self._lock = threading.Lock()
        self._last_cleanup = time.time()

    def get(self, key: str, ttl: int = 0) -> Optional[Any]:
        """캐시에서 값 가져오기 (TTL 초과 시 None)"""
        with self._lock:
            if key not in self._cache:
                return None
            value, timestamp, stored_ttl = self._cache[key]
            effective_ttl = ttl if ttl > 0 else stored_ttl
            if time.time() - timestamp > effective_ttl:
                del self._cache[key]
                return None
            return value

    def set(self, key: str, value: Any, ttl: int = 0) -> None:
        """캐시에 값 저장"""
        with self._lock:
            self._cache[key] = (value, time.time(), ttl if ttl > 0 else self._DEFAULT_TTL)
            self._maybe_cleanup()

    def clear(self) -> None:
        """캐시 전체 삭제"""
        with self._lock:
            self._cache.clear()

    def _maybe_cleanup(self) -> None:
        """주기적으로 만료 항목 삭제 + 크기 제한 적용. lock 이미 잡힌 상태에서 호출."""
        now = time.time()
        if now - self._last_cleanup < self._CLEANUP_INTERVAL:
            return
        self._last_cleanup = now
        # 만료 항목 삭제
        expired = [k for k, (_, ts, ttl) in self._cache.items() if now - ts > ttl]
        for k in expired:
            del self._cache[k]
        # 크기 제한 초과 시 가장 오래된 항목 삭제
        if len(self._cache) > self._MAX_ENTRIES:
            sorted_keys = sorted(self._cache, key=lambda k: self._cache[k][1])
            for k in sorted_keys[:len(self._cache) - self._MAX_ENTRIES]:
                del self._cache[k]
        if expired:
            logger.info("[Cache] cleanup: %d expired, %d remaining", len(expired), len(self._cache))

# 글로벌 캐시 인스턴스
cache = SimpleCache()

# ---------------------------------------------------------------------------
# 헬스체크 및 메타 Resource
# ---------------------------------------------------------------------------

@mcp.resource("market://health")
def health_check() -> Dict[str, Any]:
    """
    서버 헬스체크

    Returns:
        서버 상태 정보 (status, timestamp, version)
    [출력 스키마] status(str), timestamp(str:ISO8601), version(str), server(str), project_root(str).
    """
    return to_resource_text({
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "1.0.0",
        "server": "MarketDataServer",
        "project_root": str(PROJECT_ROOT),
    })

@mcp.resource("market://info")
def server_info() -> Dict[str, Any]:
    """
    서버 정보 및 사용 가능한 데이터 소스

    Returns:
        서버 메타 정보 및 데이터 소스 목록
    [출력 스키마] name(str), description(str), version(str), data_sources{global_regime,markets_trading{market_id→{path,exists}},markets_signal{market_id→{path,exists,db_count}},analysis{category→{path,exists}}}, endpoints{resources[],tools[]}.
    """
    from mcps.config import (
        GLOBAL_REGIME_SUMMARY_JSON,
        MARKET_DB_PATHS,
        SIGNAL_DIR_PATHS,
        ANALYSIS_DB_PATHS,
        list_signal_db_files,
    )

    # 시장별 trading DB (alias 제외)
    markets_trading = {
        k: {"path": str(v), "exists": v.exists()}
        for k, v in MARKET_DB_PATHS.items()
        if k in ("crypto", "kr_stock", "us_stock")
    }
    # 🆕 시장별 signals 디렉터리 (종목별 DB)
    markets_signal = {
        k: {"path": str(v), "exists": v.exists(), "db_count": len(list_signal_db_files(k))}
        for k, v in SIGNAL_DIR_PATHS.items()
        if k in ("crypto", "kr_stock", "us_stock")
    }

    return to_resource_text({
        "name": "MarketDataServer",
        "description": "Auto Trader 시장 데이터 API",
        "version": "1.0.0",
        "data_sources": {
            "global_regime": {
                "path": str(GLOBAL_REGIME_SUMMARY_JSON),
                "exists": GLOBAL_REGIME_SUMMARY_JSON.exists(),
                "description": "글로벌 레짐 요약 (원자재/국채/외환)"
            },
            "markets_trading": markets_trading,
            "markets_signal": markets_signal,
            "analysis": {
                category: {
                    "path": str(db_path),
                    "exists": db_path.exists(),
                }
                for category, db_path in ANALYSIS_DB_PATHS.items()
            },
            "external_context_root": {
                "path": str(EXTERNAL_CONTEXT_DATA_DIR),
                "exists": EXTERNAL_CONTEXT_DATA_DIR.exists(),
                "description": "시장별 external_context DB 루트",
            },
        },
        "endpoints": {
            "resources": [
                "market://health",
                "market://info",
                "market://global/summary",
                "market://global/category/{category}",
                "market://structure/all",
                "market://{market_id}/structure",
                "market://{market_id}/structure/group/{group_id}",
                "market://{market_id}/status",
                "market://{market_id}/positions",
                "market://{market_id}/external/summary",
                "market://{market_id}/external/symbol/{symbol}",
                "market://indicators/fear-greed",
                "market://derived/event-leading",
                "market://{market_id}/derived/regime-transitions",
                "market://derived/cross-decoupling",
                "market://derived/reaction-speed",
                "market://{market_id}/derived/strategy-fitness",
                "market://{market_id}/derived/all",
            ],
            "tools": [
                "get_trade_history(market_id, limit)",
                "get_positions(market_id, min_roi, max_roi)",
                "get_analysis_data(category, symbol, interval)",
            ]
        }
    })

# ---------------------------------------------------------------------------
# AX: Tool Dependency Meta (AI가 호출 순서를 판단할 수 있도록)
# ---------------------------------------------------------------------------

TOOL_META = {
    "version": "1.0",
    "tool_chains": {
        "quick_analysis": {
            "name": "빠른 시장 분석",
            "description": "시장 현황을 최소 호출로 파악하는 체인",
            "steps": [
                {"order": 1, "call": "market://all/summary", "type": "resource", "purpose": "3개 시장 현황 한눈에"},
                {"order": 2, "call": "market://indicators/context", "type": "resource", "purpose": "시장 심리+레짐 지표"},
                {"order": 3, "call": "market://global/summary", "type": "resource", "purpose": "매크로 레짐 확인"},
            ],
        },
        "deep_analysis": {
            "name": "심층 시장 분석",
            "description": "특정 시장의 내부+외부+시그널을 종합 분석하는 체인",
            "steps": [
                {"order": 1, "call": "market://{market_id}/unified", "type": "resource", "purpose": "통합 컨텍스트"},
                {"order": 2, "call": "market://{market_id}/signals/summary", "type": "resource", "purpose": "시그널 현황"},
                {"order": 3, "call": "market://{market_id}/signals/roles", "type": "resource", "purpose": "역할별 시그널"},
                {"order": 4, "call": "market://{market_id}/derived/all", "type": "resource", "purpose": "파생 시그널 5종"},
                {"order": 5, "call": "get_positions", "type": "tool", "args": {"market_id": "{market_id}"}, "purpose": "포지션 확인"},
            ],
        },
        "portfolio_check": {
            "name": "포트폴리오 점검",
            "description": "보유 포지션과 거래 성과를 점검하는 체인",
            "steps": [
                {"order": 1, "call": "market://all/summary", "type": "resource", "purpose": "전체 시장 현황"},
                {"order": 2, "call": "get_positions", "type": "tool", "args": {"market_id": "{market_id}"}, "purpose": "전체 포지션 조회"},
                {"order": 3, "call": "get_losing_positions", "type": "tool", "args": {"market_id": "{market_id}"}, "purpose": "손실 포지션 점검"},
                {"order": 4, "call": "get_strategy_distribution", "type": "tool", "args": {"market_id": "{market_id}"}, "purpose": "전략 다각화 점검"},
                {"order": 5, "call": "analyze_trades", "type": "tool", "args": {"market_id": "{market_id}", "days": 7}, "purpose": "최근 7일 거래 분석"},
            ],
        },
        "symbol_deep_dive": {
            "name": "종목 심층 분석",
            "description": "특정 종목의 시그널+포지션+외부맥락을 종합 분석하는 체인",
            "steps": [
                {"order": 1, "call": "market://{market_id}/unified/symbol/{symbol}", "type": "resource", "purpose": "종목 통합 컨텍스트"},
                {"order": 2, "call": "get_role_analysis", "type": "tool", "args": {"market_id": "{market_id}", "coin": "{symbol}"}, "purpose": "역할별 시그널"},
                {"order": 3, "call": "get_signal_detail", "type": "tool", "args": {"market_id": "{market_id}", "coin": "{symbol}"}, "purpose": "시그널 상세+이력"},
                {"order": 4, "call": "get_position_detail", "type": "tool", "args": {"market_id": "{market_id}", "coin": "{symbol}"}, "purpose": "포지션 상세"},
            ],
        },
    },
    "dependency_graph": {
        "market://global/summary": {"requires": [], "recommended": [], "next": ["market://global/category/{category}", "market://{market_id}/unified"]},
        "market://global/category/{category}": {"requires": [], "recommended": ["market://global/summary"], "next": ["market://derived/cross-decoupling"]},
        "market://{market_id}/status": {"requires": [], "recommended": [], "next": ["get_positions", "market://{market_id}/signals/summary"]},
        "market://all/summary": {"requires": [], "recommended": [], "next": ["market://{market_id}/status", "market://unified/cross-market"]},
        "market://{market_id}/signals/summary": {"requires": [], "recommended": [], "next": ["get_signals", "market://{market_id}/signals/roles"]},
        "market://{market_id}/unified": {"requires": [], "recommended": [], "next": ["market://{market_id}/unified/symbol/{symbol}", "get_signals"], "note": "내부+외부+교차시장을 한번에 반환. 개별 resource 3-4개 호출 대체."},
        "market://unified/cross-market": {"requires": [], "recommended": ["market://global/summary"], "next": ["market://derived/cross-decoupling"]},
        "get_positions": {"requires": [], "recommended": ["market://{market_id}/status"], "next": ["get_position_detail", "get_strategy_distribution"]},
        "get_signals": {"requires": [], "recommended": ["market://{market_id}/signals/summary"], "next": ["get_signal_detail", "get_role_analysis"]},
        "get_signal_detail": {"requires": [], "recommended": ["get_signals"], "next": ["get_role_analysis"]},
        "get_role_analysis": {"requires": [], "recommended": ["get_signal_detail"], "next": ["market://{market_id}/unified/symbol/{symbol}"]},
        "get_trade_history": {"requires": [], "recommended": [], "next": ["analyze_trades"]},
        "analyze_trades": {"requires": [], "recommended": ["get_trade_history"], "next": ["market://{market_id}/signals/feedback"]},
        "get_latest_decisions": {"requires": [], "recommended": ["market://{market_id}/status"], "next": ["get_trade_history", "get_signals"]},
    },
    "usage_hint": {
        "start_here": "market://all/summary 또는 market://indicators/context 에서 시작하세요.",
        "unified_vs_individual": "market://{market_id}/unified는 내부+외부+교차시장을 한번에 반환하지만 캐시 TTL이 120초로 짧고 컴포넌트별 갱신 주기가 다릅니다. 정밀 분석이 필요하면 개별 resource를 직접 호출하세요.",
        "signal_depth": "시그널 분석 깊이: signals/summary(시장 전체) → get_signals(종목 필터) → get_signal_detail(종목 상세) → get_role_analysis(역할별 분석)",
    },
}


@mcp.resource("market://meta/tool-chains")
def get_tool_chains_meta() -> str:
    """
    [역할] 도구/리소스 의존관계, 추천 호출 체인, 사용 가이드를 반환합니다.
    [호출 시점] 어떤 도구를 어떤 순서로 호출해야 하는지 안내 필요 시. 세션 시작 시 한번 호출 권장.
    [선행 조건] 없음.
    [후속 추천] usage_hint.start_here에 명시된 Resource부터 시작.
    [주의] 메타 정보이므로 실시간 데이터가 아닙니다.
    [출력 스키마] version(str), tool_chains{chain_id→{name,description,steps[{order,call,type,purpose}]}}, dependency_graph{uri→{requires,recommended,next,note}}, usage_hint{start_here,unified_vs_individual,signal_depth}.
    """
    return to_resource_text(TOOL_META)


# ---------------------------------------------------------------------------
# Resource 및 Tool 등록 (별도 모듈에서 import)
# ---------------------------------------------------------------------------

def register_all_resources():
    """모든 Resource 등록"""
    from mcps.resources import (
        register_global_regime_resources,
        register_market_status_resources,
        register_market_structure_resources,
        register_indicator_resources,
        register_signal_resources,
        register_external_context_resources,
        register_unified_context_resources,
        register_derived_signals_resources,
    )

    register_global_regime_resources(mcp, cache)
    register_market_status_resources(mcp, cache)
    register_market_structure_resources(mcp, cache)
    register_indicator_resources(mcp, cache)
    register_signal_resources(mcp, cache)
    register_external_context_resources(mcp, cache)
    register_unified_context_resources(mcp, cache)
    register_derived_signals_resources(mcp, cache)

    logger.info("✅ All Resources registered")

def register_all_tools():
    """모든 Tool 등록"""
    from mcps.tools import (
        register_trade_history_tools,
        register_position_tools,
        register_decision_tools,
        register_signal_tools,
        register_trust_layer_tools,
    )

    register_trade_history_tools(mcp, cache)
    register_position_tools(mcp, cache)
    register_decision_tools(mcp, cache)
    register_signal_tools(mcp, cache)
    register_trust_layer_tools(mcp, cache)

    logger.info("✅ All Tools registered")

# ---------------------------------------------------------------------------
# 서버 실행
# ---------------------------------------------------------------------------

def create_app():
    """FastMCP 앱 생성 및 초기화"""
    logger.info(f"🚀 MarketMCP Server initializing...")
    logger.info(f"   Project Root: {PROJECT_ROOT}")
    logger.info(f"   Host: {MCP_SERVER_HOST}:{MCP_SERVER_PORT}")

    # Resource/Tool 등록
    register_all_resources()
    register_all_tools()

    return mcp

def _create_rate_limit_middleware():
    """Rate limiting ASGI middleware for external API access (tier-aware)."""
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.requests import Request
    from starlette.responses import JSONResponse

    class RateLimitMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next):
            if not request.url.path.startswith("/mcp"):
                return await call_next(request)

            # Cloudflare sends CF-Connecting-IP
            ip = (
                request.headers.get("cf-connecting-ip")
                or request.headers.get("x-forwarded-for", "").split(",")[0].strip()
                or (request.client.host if request.client else "local")
            )

            # Phase 1: AI Agent Behavior Analytics — capture user-agent
            user_agent = request.headers.get("user-agent", "")

            # Skip rate limiting + analytics for localhost (internal services)
            if ip in ("127.0.0.1", "::1", "local"):
                return await call_next(request)

            # Resolve tier from API key
            api_key = request.headers.get("x-api-key") or request.headers.get("authorization", "").removeprefix("Bearer ").strip()
            tier = "free"
            if api_key:
                try:
                    from api.marketplace.key_store import get_tier_for_key
                    tier = get_tier_for_key(api_key)
                except Exception:
                    pass  # Fall back to free tier if key_store unavailable

            # Rate limit by API key (if pro) or IP (if free)
            identity = api_key if (api_key and tier != "free") else ip
            allowed, info = rate_limiter.check(identity, tier=tier)
            if not allowed:
                logger.warning("Rate limited: %s (tier=%s) — %s", identity[:16] if len(identity) > 16 else identity, tier, info.get("limit_type"))
                try:
                    analytics_writer.log_request(
                        ip=ip, request_type="mcp", name="rate_limited",
                        success=False, response_ms=0,
                        error_code="rate_limited", rate_limited=True,
                        user_agent=user_agent,
                    )
                except Exception:
                    pass
                return JSONResponse(
                    status_code=429,
                    content={
                        "jsonrpc": "2.0",
                        "error": {
                            "code": -32000,
                            "message": info["message"],
                            "data": info,
                        }
                    },
                    headers={"Retry-After": str(info.get("retry_after", 60))},
                )

            # Parse JSON-RPC body for analytics metadata
            req_type, req_name = "mcp", "unknown"
            if request.method == "POST":
                try:
                    import json as _json
                    body = await request.body()
                    payload = _json.loads(body)
                    method = payload.get("method", "")
                    params = payload.get("params", {})
                    if method == "resources/read":
                        req_type, req_name = "resource", params.get("uri", "unknown")
                    elif method == "tools/call":
                        req_type, req_name = "tool", params.get("name", "unknown")
                    elif method:
                        req_type, req_name = "mcp", method
                except Exception:
                    pass

            # Time the request + log analytics
            t0 = time.time()
            try:
                response = await call_next(request)
                elapsed_ms = int((time.time() - t0) * 1000)
                success = response.status_code < 400

                try:
                    analytics_writer.log_request(
                        ip=ip, request_type=req_type, name=req_name,
                        success=success, response_ms=elapsed_ms,
                        user_agent=user_agent,
                    )
                except Exception:
                    pass  # fire-and-forget

                # Enhanced rate limit headers
                response.headers["X-RateLimit-Tier"] = tier
                response.headers["X-RateLimit-Daily-Limit"] = str(info.get("daily_limit", 0))
                response.headers["X-RateLimit-Daily-Remaining"] = str(info.get("remaining_daily", 0))
                response.headers["X-RateLimit-Minute-Remaining"] = str(info.get("remaining_minute", 0))
                return response
            except Exception as exc:
                elapsed_ms = int((time.time() - t0) * 1000)
                try:
                    analytics_writer.log_request(
                        ip=ip, request_type=req_type, name=req_name,
                        success=False, response_ms=elapsed_ms,
                        error_detail=str(exc)[:200],
                        error_code="exception",
                        user_agent=user_agent,
                    )
                except Exception:
                    pass  # fire-and-forget
                raise

    return RateLimitMiddleware


def run_server():
    """서버 실행 (FastMCP 버전 호환)"""
    create_app()

    # Rate limiting middleware 생성
    _middleware_list = []
    try:
        from starlette.middleware import Middleware as _StarletteMiddleware
        RateLimitMiddleware = _create_rate_limit_middleware()
        _middleware_list.append(_StarletteMiddleware(RateLimitMiddleware))
        logger.info(f"   Rate limits: {rate_limiter.daily_limit}/day, {rate_limiter.minute_limit}/min per IP")
        logger.info(f"   Localhost bypass: enabled (internal services exempt)")
    except Exception as e:
        logger.warning(f"Rate limit middleware 생성 실패 (서버는 계속 실행): {e}")

    # ClientDisconnect / ClosedResourceError 로그 억제
    # stateless 모드에서 클라이언트 연결 끊김은 정상 동작이므로 ERROR 로그 불필요
    try:
        import logging as _logging
        from anyio import ClosedResourceError as _CRE
        from starlette.requests import ClientDisconnect as _CD

        _SUPPRESS_MSGS = ("ClientDisconnect", "Received exception from stream", "Stateless session crashed")

        class _DisconnectFilter(_logging.Filter):
            def filter(self, record):
                # exc_info에 ClientDisconnect 또는 ClosedResourceError가 있으면 억제
                if record.exc_info and record.exc_info[1]:
                    exc = record.exc_info[1]
                    if isinstance(exc, (_CD, _CRE)):
                        return False
                    # ExceptionGroup 내부의 ClosedResourceError 억제
                    if isinstance(exc, BaseExceptionGroup):
                        _, rest = exc.split(_CRE)
                        if rest is None:
                            return False
                msg = record.getMessage()
                return not any(s in msg for s in _SUPPRESS_MSGS)

        _filt = _DisconnectFilter()
        for _name in ("mcp.server.streamable_http", "mcp.server.lowlevel.server",
                       "mcp.server.streamable_http_manager", "mcp"):
            _logging.getLogger(_name).addFilter(_filt)
    except Exception:
        pass

    logger.info(f"Starting MarketMCP Server on {MCP_SERVER_HOST}:{MCP_SERVER_PORT}")
    logger.info(f"   Swagger UI: http://localhost:{MCP_SERVER_PORT}/docs")
    logger.info(f"   OpenAPI JSON: http://localhost:{MCP_SERVER_PORT}/openapi.json")

    # log_level="CRITICAL" — FastMCP가 세션 write stream으로 로그를 전달하는
    # MCP logging handler를 실질적으로 비활성화.
    # stateless 모드에서 클라이언트 연결이 끊긴 후 로그 발생 시
    # ClosedResourceError → "Stateless session crashed" 를 방지한다.
    mcp.run(
        transport="streamable-http",
        host=MCP_SERVER_HOST,
        port=MCP_SERVER_PORT,
        path="/mcp",
        json_response=MCP_JSON_RESPONSE,
        stateless_http=MCP_STATELESS,
        show_banner=False,
        log_level="CRITICAL",
        middleware=_middleware_list or None,
    )

# ---------------------------------------------------------------------------
# 메인 진입점
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_server()
