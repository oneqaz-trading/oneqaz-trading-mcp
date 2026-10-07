# -*- coding: utf-8 -*-
"""
MCP Server Health Route
=======================
api.oneqaz.com/health, /status, /metrics 의 실제 응답자.

배포 컨텍스트:
    - 로컬 PC (Docker auto_trader_public): PG 직결 → 풍부한 응답
    - AWS EC2 (mcp-server): SQLite readonly only → degraded 모드도 정상 작동
        AWS 에는 api/ 디렉토리가 동기화되지 않으므로 self-contained 로 작성.

cloudflared 2025.8.1 의 local-config path 정규식 회귀로 path 분기 라우팅이
edge 에 푸시되지 않는 문제를 우회하기 위해 MCP server (port 8010) 가
직접 /health, /status, /metrics 를 노출한다.

응답 스키마:
    {
        "status": "live" | "degraded",
        "timestamp": ISO8601,
        "uptime_hint": str,
        "regime": { overall, score, categories, updated_at },
        "signals": { last_24h_count, last_signal_at, active_markets },
        "predictions": { active_count },
        "tools_count": int,
        "version": str,
        "degraded": bool,
        "probe_errors": [str, ...]   # degraded 일 때만
    }

캐시: 5분 TTL. 실패해도 200 + degraded:true + 마지막 캐시값.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("MarketMCP")


# ---------------------------------------------------------------------------
# 캐시
# ---------------------------------------------------------------------------

_CACHE_TTL_S = 300  # 5분
_lock = threading.Lock()
# [2026-08-15] 재빌드 single-flight — 종전엔 TTL 만료 순간 동시 호출자(CF LB 다중
# edge + landing /metrics 60s 폴링)가 각자 _build_payload() 를 돌려 느린 프로브가
# 직렬로 쌓였다 (03:59→04:00 61초 루프 동결 실측). 한 명만 빌드, 나머지는
# 락 해제 후 방금 갱신된 캐시를 재확인해 즉시 반환한다.
_rebuild_lock = threading.Lock()
_cache: Dict[str, Any] = {"ts": 0.0, "payload": None}
_LAST_GOOD: Dict[str, Any] = {"ts": 0.0, "payload": None}
_BOOT_TS = time.time()


# ---------------------------------------------------------------------------
# Probe — self-contained, shared.db.compat 만 사용 (PG/SQLite 자동 분기)
# ---------------------------------------------------------------------------

def _global_regime_summary_path() -> Optional[Path]:
    """global_regime_summary.json 의 경로를 환경별로 찾는다."""
    candidates = []
    # mcps.config 가 있으면 거기서 가져온다 (로컬)
    try:
        from oneqaz_trading_mcp.config import GLOBAL_REGIME_SUMMARY_JSON  # type: ignore
        candidates.append(Path(str(GLOBAL_REGIME_SUMMARY_JSON)))
    except Exception:
        pass
    # 로컬(Docker) fallback 후보 — AWS 배포는 2026-07-06 제거 완료
    candidates.append(
        Path("/workspace/market/global_regime/data_storage/global_regime_summary.json")
    )
    for p in candidates:
        try:
            if p.exists():
                return p
        except Exception:
            continue
    return None


def _probe_regime() -> Dict[str, Any]:
    """global_regime_summary.json → overall + categories."""
    p = _global_regime_summary_path()
    if p is None:
        return {"overall": "unknown", "categories": {}, "updated_at": None}
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        overall = data.get("overall", {}) or {}
        categories = data.get("categories", {}) or {}
        cat_simple = {
            k: (v.get("regime_dominant") if isinstance(v, dict) else None)
            for k, v in categories.items()
        }
        return {
            "overall": overall.get("regime", "unknown"),
            "score": overall.get("score"),
            "categories": cat_simple,
            "updated_at": data.get("updated_at"),
        }
    except Exception as e:
        logger.warning("MCP health probe regime failed: %s", e)
        raise


_MARKET_SCHEMAS = {
    "crypto": "market_coin",
    "kr_stock": "market_kr",
    "us_stock": "market_us",
}


def _probe_signals() -> Dict[str, Any]:
    """3시장 signals 테이블 — 최근 24h count + last_signal_at.

    PG (로컬) / SQLite (AWS) 자동 분기 — shared.db.compat.open_schema_connection.

    [2026-05-08] 최적화: signals 가 ~6GB partitioned + BRIN 인덱스라 24h 범위
    `COUNT(*) + MAX(timestamp)` 를 하나의 풀 쿼리로 돌리면 lossy bitmap recheck 가
    175s 걸려 PG worker 가 그동안 점유. 두 단계로 분리:
      1. `EXISTS(... WHERE timestamp > since LIMIT 1)` — active 판정 (인덱스 1 hit, ms)
      2. `COUNT(*) WHERE timestamp > since` — 24h count (느려도 statement_timeout 으로 보호)
    각 시장은 독립 try/except — 한 시장 timeout 이 다른 시장 차단 안 함.
    """
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    since = int(time.time()) - 24 * 3600

    total_count = 0
    last_ts: Optional[int] = None
    active_markets: List[str] = []

    for market_id, schema in _MARKET_SCHEMAS.items():
        try:
            conn = open_schema_connection(schema, readonly=True)
            try:
                # 1) active 판정 + last_ts (LIMIT 1 빠른 인덱스 lookup)
                row = conn.execute(
                    "SELECT MAX(timestamp) AS last_ts FROM signals WHERE timestamp > ?",
                    (since,),
                ).fetchone()
                lt = None
                if row is not None:
                    lt = row["last_ts"] if isinstance(row, dict) else row[0]

                if lt is None:
                    # 24h 내 신호 없음 → active 아님, count 안 봐도 됨
                    continue

                # 2) active 시장 — count 도 시도. 느릴 수 있으나 한 시장당 독립.
                #    실패해도 active 상태 + last_ts 는 유지 (count 0 으로 표시).
                cnt = 0
                try:
                    crow = conn.execute(
                        "SELECT COUNT(*) AS c FROM signals WHERE timestamp > ?",
                        (since,),
                    ).fetchone()
                    if crow is not None:
                        c = crow["c"] if isinstance(crow, dict) else crow[0]
                        cnt = int(c or 0)
                except Exception as ce:
                    logger.warning(
                        "MCP health probe count(%s) failed (active 유지): %s",
                        market_id, ce,
                    )

                active_markets.append(market_id)
                total_count += cnt
                lt_int = int(lt)
                if last_ts is None or lt_int > last_ts:
                    last_ts = lt_int
            finally:
                conn.close()
        except Exception as e:
            # 시장 1개 실패는 전체 실패가 아님 — 다른 시장 계속
            logger.warning("MCP health probe signals(%s) failed: %s", market_id, e)
            continue

    last_iso = (
        datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat()
        if last_ts else None
    )
    return {
        "last_24h_count": total_count,
        "last_signal_at": last_iso,
        "last_signal_ts": last_ts,
        "active_markets": active_markets,
    }


def _probe_predictions() -> Dict[str, Any]:
    """market_global.macro_regime_predictions — outcome 미정 active 개수."""
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    try:
        conn = open_schema_connection("market_global", readonly=True)
        try:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM macro_regime_predictions WHERE outcome IS NULL"
            ).fetchone()
            c = (
                row["c"] if isinstance(row, dict)
                else row[0] if row is not None else 0
            )
            return {"active_count": int(c or 0)}
        finally:
            conn.close()
    except Exception as e:
        logger.warning("MCP health probe predictions failed: %s", e)
        raise


def _probe_tools_count() -> int:
    """등록된 MCP tool 수 — 정적 fallback (server discovery 와 동기 유지).

    [2026-07-08] 32 → 37: prediction ledger 2종(get_resolved_predictions,
    get_ledger_integrity) + get_trade_outcomes_bulk + search/fetch.
    """
    return 37


# ---------------------------------------------------------------------------
# Build / Cache
# ---------------------------------------------------------------------------

def _uptime_hint() -> str:
    secs = max(0, int(time.time() - _BOOT_TS))
    if secs < 3600:
        return f"{secs // 60}m"
    if secs < 86400:
        return f"{secs // 3600}h{(secs % 3600) // 60}m"
    return f"{secs // 86400}d{(secs % 86400) // 3600}h"


def _build_payload() -> Dict[str, Any]:
    errors: List[str] = []
    regime: Dict[str, Any] = {"overall": "unknown", "categories": {}, "updated_at": None}
    signals: Dict[str, Any] = {
        "last_24h_count": 0, "last_signal_at": None, "active_markets": []
    }
    predictions: Dict[str, Any] = {"active_count": 0}

    try:
        regime = _probe_regime()
    except Exception as e:
        errors.append(f"regime:{type(e).__name__}")
    try:
        signals = _probe_signals()
    except Exception as e:
        errors.append(f"signals:{type(e).__name__}")
    try:
        predictions = _probe_predictions()
    except Exception as e:
        errors.append(f"predictions:{type(e).__name__}")

    # [2026-06-08] serving_ok — 데이터 신선도 기반 "진짜 서빙 가능" 판정.
    # Cloudflare LB health monitor 가 이 값으로 home/aws 를 가른다.
    # AWS 디스크 포화 시 probe 가 예외 없이 0 row 를 반환(per-market try/except)
    # → 기존 degraded=bool(errors) 는 False 인데 실제론 빈 응답을 서빙하던
    # 문제를 막는다. signals 24h 또는 active predictions 중 하나라도 살아있으면 OK.
    sig_count = int((signals or {}).get("last_24h_count", 0) or 0)
    sig_recent = bool((signals or {}).get("last_signal_at"))
    pred_active = int((predictions or {}).get("active_count", 0) or 0)
    serving_ok = (sig_count > 0) or sig_recent or (pred_active > 0)

    # degraded = probe 에러가 있었거나(부분실패) 데이터가 비었거나(서빙불가).
    degraded = bool(errors) or (not serving_ok)

    payload = {
        "status": "live" if serving_ok and not errors else "degraded",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uptime_hint": _uptime_hint(),
        "regime": regime,
        "signals": signals,
        "predictions": predictions,
        "tools_count": _probe_tools_count(),
        "version": "2.0",
        "degraded": degraded,
        # serving_ok=False → /health 가 HTTP 503 반환(LB 가 unhealthy 로 마킹).
        # errors 만 있고 데이터는 살아있으면(부분실패) 503 아님 — flapping 방지.
        "serving_ok": serving_ok,
    }
    if errors:
        payload["probe_errors"] = errors
    return payload


def _get_health_payload() -> Dict[str, Any]:
    """5분 캐시 + degraded fallback. 절대 raise 안 함.

    [2026-08-15] blocking 호출자 전용 — async 핸들러는 반드시 asyncio.to_thread 로
    감싸서 부른다 (프로브의 동기 PG 쿼리가 이벤트 루프를 얼리던 실사고).
    """
    now = time.time()
    with _lock:
        cached = _cache.get("payload")
        cached_ts = _cache.get("ts", 0.0)
        if cached is not None and (now - cached_ts) < _CACHE_TTL_S:
            return cached

    with _rebuild_lock:
        # double-check: 락 대기 동안 다른 스레드가 이미 갱신했으면 그걸 반환
        now = time.time()
        with _lock:
            cached = _cache.get("payload")
            cached_ts = _cache.get("ts", 0.0)
            if cached is not None and (now - cached_ts) < _CACHE_TTL_S:
                return cached
        return _rebuild_payload_locked(now)


def _rebuild_payload_locked(now: float) -> Dict[str, Any]:
    """_rebuild_lock 보유 상태에서만 호출 — 실제 프로브 실행 + 캐시 갱신."""
    try:
        fresh = _build_payload()
    except Exception as e:
        logger.exception("MCP health _build_payload outer failure: %s", e)
        with _lock:
            last_good = _LAST_GOOD.get("payload")
            if last_good is not None:
                stale = dict(last_good)
                stale["status"] = "degraded"
                stale["degraded"] = True
                # build 전체 실패라 신선도 판정 불가 → serving_ok=False (503).
                stale["serving_ok"] = False
                stale["probe_errors"] = stale.get("probe_errors", []) + [f"build:{type(e).__name__}"]
                stale["served_from_cache"] = True
                return stale
        return {
            "status": "degraded",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uptime_hint": _uptime_hint(),
            "regime": {"overall": "unknown", "categories": {}, "updated_at": None},
            "signals": {"last_24h_count": 0, "last_signal_at": None, "active_markets": []},
            "predictions": {"active_count": 0},
            "tools_count": 32,
            "version": "2.0",
            "degraded": True,
            "serving_ok": False,
            "probe_errors": [f"build:{type(e).__name__}"],
        }

    with _lock:
        _cache["ts"] = now
        _cache["payload"] = fresh
        if not fresh.get("degraded"):
            _LAST_GOOD["ts"] = now
            _LAST_GOOD["payload"] = fresh
        else:
            if _LAST_GOOD.get("payload") is None:
                _LAST_GOOD["payload"] = fresh
                _LAST_GOOD["ts"] = now
    return fresh


def _get_metrics_payload() -> Dict[str, Any]:
    """경량 metrics — landing 위젯 friendly flat shape."""
    p = _get_health_payload()
    cats = (p.get("regime") or {}).get("categories", {}) or {}
    return {
        "status": p.get("status"),
        "timestamp": p.get("timestamp"),
        "regime_overall": (p.get("regime") or {}).get("overall"),
        "regime_categories": cats,
        "signals_last_24h": (p.get("signals") or {}).get("last_24h_count"),
        "last_signal_at": (p.get("signals") or {}).get("last_signal_at"),
        "active_markets": (p.get("signals") or {}).get("active_markets"),
        "active_predictions": (p.get("predictions") or {}).get("active_count"),
        "tools_count": p.get("tools_count"),
        "version": p.get("version"),
        "degraded": p.get("degraded"),
        "serving_ok": p.get("serving_ok"),
    }


# ---------------------------------------------------------------------------
# Route Registration (FastMCP custom_route)
# ---------------------------------------------------------------------------

_CORS_HEADERS = {
    # 응답 본문이 모두 public (no auth, no PII) 이므로 * 허용 안전.
    # landing(oneqaz.com) → api.oneqaz.com fetch 가 cross-origin 이라 필수.
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    # 브라우저 캐시 차단 — 60초마다 polling 이지만 stale 응답 보지 않게.
    "Cache-Control": "no-store, max-age=0",
}


def register_health_routes(mcp) -> None:
    """FastMCP 인스턴스에 /health, /status, /metrics 등록."""
    from starlette.requests import Request
    from starlette.responses import JSONResponse, Response

    def _json(payload: Dict[str, Any], status_code: int = 200) -> JSONResponse:
        return JSONResponse(payload, status_code=status_code, headers=_CORS_HEADERS)

    @mcp.custom_route("/health", methods=["GET", "OPTIONS"])
    async def _mcp_health(request: Request) -> Response:
        if request.method == "OPTIONS":
            return Response(status_code=204, headers=_CORS_HEADERS)
        try:
            # [2026-08-15] to_thread 필수 — 프로브의 동기 PG 쿼리(statement_timeout
            # 10s)가 이벤트 루프 위에서 돌면 MCP 전체가 first-byte 스톨 (61초 동결
            # 실측, /global/summary 10~20.5s 지연의 진범).
            payload = await asyncio.to_thread(_get_health_payload)
            # [2026-06-08] serving_ok=False → HTTP 503 → Cloudflare LB monitor 가
            # 이 origin 을 unhealthy 로 마킹(Expected 200). 디스크 포화·데이터
            # 공백처럼 200 이지만 실제론 못 서빙하는 좀비 origin 으로의 failover 차단.
            # serving_ok 키가 없으면(구 캐시/예외 fallback) 보수적으로 200 유지.
            code = 503 if payload.get("serving_ok") is False else 200
            return _json(payload, status_code=code)
        except Exception as e:
            logger.exception("MCP /health unrecoverable: %s", e)
            return _json(
                {
                    "status": "degraded",
                    "version": "2.0",
                    "tools_count": 32,
                    "degraded": True,
                    "serving_ok": False,
                    "probe_errors": [f"runtime:{type(e).__name__}"],
                },
                status_code=503,
            )

    @mcp.custom_route("/status", methods=["GET", "OPTIONS"])
    async def _mcp_status(request: Request) -> Response:
        return await _mcp_health(request)

    @mcp.custom_route("/metrics", methods=["GET", "OPTIONS"])
    async def _mcp_metrics(request: Request) -> Response:
        if request.method == "OPTIONS":
            return Response(status_code=204, headers=_CORS_HEADERS)
        try:
            return _json(await asyncio.to_thread(_get_metrics_payload))
        except Exception as e:
            logger.exception("MCP /metrics unrecoverable: %s", e)
            return _json(
                {"degraded": True, "probe_errors": [f"runtime:{type(e).__name__}"]}
            )

    # [2026-06-29] Privacy policy — MCP/connector 디렉토리 정책상 (Claude/OpenAI)
    # remote connector 는 privacy policy URL 이 필수(누락 시 즉시 거절). landing
    # 의 oneqaz.com/privacy 와 동일 내용을 connector 와 같은 도메인(api.oneqaz.com)
    # 에서도 노출해 일관성 확보. self-contained HTML — AWS 에 landing 파일 없음.
    @mcp.custom_route("/privacy", methods=["GET", "OPTIONS"])
    async def _mcp_privacy(request: Request) -> Response:
        if request.method == "OPTIONS":
            return Response(status_code=204, headers=_CORS_HEADERS)
        html_headers = {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=3600",
            "Content-Type": "text/html; charset=utf-8",
        }
        return Response(_PRIVACY_HTML, status_code=200, headers=html_headers)

    # [2026-07-08] 예측 원장 해시 체인 공개 — 외부 관찰자가 아카이브해두면
    # 이후의 조용한 원장 수정이 재계산 불일치로 드러난다 (tamper-evidence).
    # MCP tool(get_ledger_integrity)과 동일 데이터의 HTTP 표면 — 크롤러/브라우저용.
    @mcp.custom_route("/ledger", methods=["GET", "OPTIONS"])
    async def _mcp_ledger(request: Request) -> Response:
        if request.method == "OPTIONS":
            return Response(status_code=204, headers=_CORS_HEADERS)
        try:
            from oneqaz_trading_mcp.ledger_integrity import get_chain
            try:
                days = min(400, max(1, int(request.query_params.get("days", "60"))))
            except (TypeError, ValueError):
                days = 60
            payload = await asyncio.to_thread(get_chain, days)
            payload["disclaimer"] = (
                "Integrity metadata for OneQAZ research forecasts. Information only, "
                "not investment advice."
            )
            payload["data_classification"] = "research_information_only"
            payload["is_investment_advice"] = False
            return _json(payload)
        except Exception as e:
            logger.exception("MCP /ledger failed: %s", e)
            return _json({"available": False, "error": type(e).__name__}, status_code=503)

    # [2026-07-08] SLA 이력 자체 발행 — 크롤러 10곳+ 이 이미 외부에서 liveness 를
    # 채점 중이므로 per-tool 지연/에러 이력을 우리가 먼저 공개한다 (생존성 신호).
    @mcp.custom_route("/sla", methods=["GET", "OPTIONS"])
    async def _mcp_sla(request: Request) -> Response:
        if request.method == "OPTIONS":
            return Response(status_code=204, headers=_CORS_HEADERS)
        try:
            from oneqaz_trading_mcp.ledger_integrity import get_sla_history
            try:
                days = min(120, max(1, int(request.query_params.get("days", "30"))))
            except (TypeError, ValueError):
                days = 30
            payload = await asyncio.to_thread(get_sla_history, days)
            payload["disclaimer"] = (
                "Operational metadata for the OneQAZ research service. Information only, "
                "not investment advice."
            )
            payload["data_classification"] = "research_information_only"
            return _json(payload)
        except Exception as e:
            logger.exception("MCP /sla failed: %s", e)
            return _json({"available": False, "error": type(e).__name__}, status_code=503)

    # [2026-07-08] pricing / keys — 모든 MCP 응답의 _value_signals 와 403/429 안내가
    # 이 URL 을 광고하는데 종전엔 oneqaz.com/pricing·/keys 가 404 (업그레이드 퍼널
    # 데드엔드 + 감사 AI 링크 무결성 감점). self-contained HTML 로 즉시 해소.
    @mcp.custom_route("/pricing", methods=["GET", "OPTIONS"])
    async def _mcp_pricing(request: Request) -> Response:
        if request.method == "OPTIONS":
            return Response(status_code=204, headers=_CORS_HEADERS)
        html_headers = {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=3600",
            "Content-Type": "text/html; charset=utf-8",
        }
        return Response(_PRICING_HTML, status_code=200, headers=html_headers)

    @mcp.custom_route("/keys", methods=["GET", "OPTIONS"])
    async def _mcp_keys(request: Request) -> Response:
        if request.method == "OPTIONS":
            return Response(status_code=204, headers=_CORS_HEADERS)
        html_headers = {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=3600",
            "Content-Type": "text/html; charset=utf-8",
        }
        return Response(_KEYS_HTML, status_code=200, headers=html_headers)

    # [2026-07-08] Terms of Use — 종전엔 응답 데이터 이용조건 문서가 0건이라
    # 거대 AI 운영사가 인제스트 판단 시 볼 라이선스 조항이 없었다 (라이선스 감사
    # 지적). docs/legal/upstream_data_license_review_2026_07_08.md 의 조항 초안 기반.
    @mcp.custom_route("/terms", methods=["GET", "OPTIONS"])
    async def _mcp_terms(request: Request) -> Response:
        if request.method == "OPTIONS":
            return Response(status_code=204, headers=_CORS_HEADERS)
        html_headers = {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=3600",
            "Content-Type": "text/html; charset=utf-8",
        }
        return Response(_TERMS_HTML, status_code=200, headers=html_headers)

    logger.info("[OK] Health routes registered (/health, /status, /metrics, /privacy, /ledger, /sla, /pricing, /keys, /terms)")


# ---------------------------------------------------------------------------
# Privacy Policy HTML — landing/privacy.html(oneqaz.com/privacy)과 **바이트 동일**한 단일 원본.
# [2026-10-07] 사이트 테마·EN/KO 토글을 공유하도록 재작성. 링크·로고는 절대주소라 어느 도메인에서도 동작.
# ⛔ 한쪽만 고치지 말 것 — landing 파일을 고친 뒤 이 상수에 그대로 붙인다(백슬래시·삼중따옴표 금지).
# 수집 항목은 mcps/analytics.py log_request() 실측 기준 · 보존 약속(§3)의 이행 = scripts/mcp_requests_ip_retention.py
# ---------------------------------------------------------------------------

_PRIVACY_HTML = """<!DOCTYPE html>
<html lang="en" data-lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Privacy Policy — OneQAZ</title>
<meta name="description" content="OneQAZ privacy policy (MCP server, API, website) — items, purposes and legal basis, retention, destruction, outsourcing and overseas transfer, rights, privacy officer. 개인정보 처리방침.">
<meta name="robots" content="index,follow">
<link rel="canonical" href="https://oneqaz.com/privacy">
<link rel="icon" href="https://oneqaz.com/images/logo.png" type="image/png">
<link rel="preconnect" href="https://fonts.bunny.net">
<link href="https://fonts.bunny.net/css?family=outfit:300,400,500,600,700|noto-sans-kr:300,400,500,700" rel="stylesheet">
<!--
  ONE SOURCE, TWO URLS — this exact file is served at https://oneqaz.com/privacy (landing repo)
  and https://api.oneqaz.com/privacy (monorepo mcps/health_route.py::_PRIVACY_HTML, mirrored into
  the public package). Keep them byte-identical: links and assets are absolute for that reason.
  No backslashes and no triple quotes anywhere — the MCP copy lives inside a Python string.
-->
<style>
*,*::before,*::after{margin:0;padding:0;box-sizing:border-box}
:root,[data-theme="dark"]{
  --bg:#1c1c24;--panel-bg:#252530;--card-bg:#2a2a36;
  --text:#e8e4df;--text-muted:#8a8680;
  --amber:#d4a853;--green:#5cb85c;--red:#d9534f;--blue:#6b9fc9;--gold:#c49b6f;
  --border:#3a3a4a;--hover:rgba(255,255,255,0.02);
  --font:'Outfit','Noto Sans KR',sans-serif;
  --scrollbar-thumb:rgba(255,255,255,0.1);
  --nav-bg:rgba(28,28,36,0.9);
}
[data-theme="light"]{
  --bg:#e8e4de;--panel-bg:#f2efe9;--card-bg:#ece9e3;
  --text:#3a3632;--text-muted:#7a756e;
  --amber:#9e7520;--green:#2d7a3a;--red:#b03225;--blue:#4272a0;--gold:#8c6a38;
  --border:#cdc7bc;--hover:rgba(0,0,0,0.03);
  --scrollbar-thumb:rgba(0,0,0,0.15);
  --nav-bg:rgba(242,239,233,0.9);
}
html{scroll-behavior:smooth;font-size:16px}
body{background:var(--bg);color:var(--text);font-family:var(--font);line-height:1.6;overflow-x:hidden}
::selection{background:var(--amber);color:var(--bg)}
body::after{content:'';position:fixed;inset:0;pointer-events:none;z-index:9999;background:repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(0,0,0,0.06) 2px,rgba(0,0,0,0.06) 4px);transition:opacity .4s}
[data-theme="light"] body::after{opacity:0.3}
body::before{
  content:'';position:fixed;inset:-10%;pointer-events:none;z-index:9998;
  background:radial-gradient(ellipse at center,transparent 55%,rgba(0,0,0,0.18) 100%);
  clip-path:polygon(50% 0%,93.3% 25%,93.3% 75%,50% 100%,6.7% 75%,6.7% 25%);
  transition:opacity .4s;
}
[data-theme="light"] body::before{opacity:0.08}
::-webkit-scrollbar{width:4px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--scrollbar-thumb);border-radius:2px}

/* Language: both languages ship in the HTML (crawlable); the toggle hides one. */
[data-lang="en"] [data-l="ko"],[data-lang="ko"] [data-l="en"]{display:none !important}

.version-tag{position:fixed;top:12px;left:16px;z-index:100;display:block}
.version-logo{width:28px;height:28px;filter:drop-shadow(0 0 6px rgba(212,168,83,0.3));opacity:.8;transition:opacity .2s}
.version-logo:hover{opacity:1}
.progress-bar{position:fixed;top:0;left:0;width:100%;height:2px;z-index:1000;background:var(--panel-bg)}
.progress-bar .fill{height:100%;width:0;background:linear-gradient(90deg,var(--amber),var(--gold));transition:width .3s}

nav{position:fixed;top:0;right:0;z-index:100;padding:16px 24px;display:flex;gap:20px;align-items:center;font-size:12px;transition:background-color .3s}
/* Document page: once scrolled, give the nav the same frosted bar the home page uses on mobile so body text never runs under it. */
nav.scrolled{left:0;justify-content:flex-end;background:var(--nav-bg);backdrop-filter:blur(8px)}
nav a{color:var(--text-muted);text-decoration:none;letter-spacing:1.5px;transition:color .2s;text-transform:uppercase;font-weight:500}
nav a:hover{color:var(--amber)}
nav a.nav-cta{border:1px solid var(--amber);color:var(--amber);padding:5px 12px;border-radius:6px;font-weight:600;transition:background .2s,box-shadow .2s,color .2s}
nav a.nav-cta:hover{background:rgba(212,168,83,0.1);box-shadow:0 0 12px rgba(212,168,83,0.15);color:var(--amber)}
.lang-toggle{display:flex;border:1px solid var(--border);border-radius:6px;overflow:hidden;margin-left:4px}
.lang-toggle button{background:transparent;border:none;color:var(--text-muted);padding:4px 10px;font-size:11px;font-weight:600;letter-spacing:1px;cursor:pointer;font-family:var(--font);transition:all .2s}
.lang-toggle button.active{background:var(--amber);color:var(--bg)}

.container{max-width:1080px;width:100%;margin:0 auto;padding:0 40px}
.doc{max-width:820px;padding:128px 0 72px}
.section-label{font-size:11px;color:var(--amber);letter-spacing:3px;text-transform:uppercase;margin-bottom:8px;font-weight:600;opacity:.8}
.section-title{font-size:clamp(28px,4vw,40px);font-weight:700;margin-bottom:14px;color:var(--text);line-height:1.25}
.section-desc{font-size:14px;color:var(--text-muted);max-width:720px;line-height:1.9;margin-bottom:10px;font-weight:300}
.doc-meta{font-size:12px;color:var(--gold);letter-spacing:1px;font-weight:500;margin-bottom:28px}
.toc{display:flex;flex-wrap:wrap;gap:8px;margin:0 0 40px}
.toc a{font-size:11px;letter-spacing:1px;text-transform:uppercase;color:var(--text-muted);border:1px solid var(--border);border-radius:6px;padding:4px 10px;text-decoration:none;font-weight:500;transition:color .2s,border-color .2s}
.toc a:hover{color:var(--amber);border-color:var(--amber)}

.lead{font-size:15px;line-height:1.9;font-weight:300;color:var(--text);margin-bottom:20px}
.note{background:var(--panel-bg);border:1px solid var(--border);border-left:3px solid var(--amber);border-radius:10px;padding:16px 20px;font-size:14px;line-height:1.85;font-weight:300;margin:0 0 44px}
.note strong{color:var(--amber)}

.policy section{scroll-margin-top:84px;margin-bottom:44px}
.policy h2{display:flex;align-items:baseline;gap:12px;font-size:20px;font-weight:600;color:var(--text);margin-bottom:16px;padding-bottom:10px;border-bottom:1px solid var(--border)}
.policy h2 .num{font-size:11px;color:var(--amber);letter-spacing:2px;font-weight:600;flex-shrink:0}
.policy p{font-size:15px;line-height:1.9;color:var(--text);font-weight:300;margin-bottom:14px}
.policy ul{list-style:none;margin:0 0 14px}
.policy ul li{position:relative;padding-left:20px;font-size:15px;line-height:1.85;font-weight:300;margin-bottom:10px}
.policy ul li::before{content:'';position:absolute;left:3px;top:.72em;width:6px;height:6px;border-radius:50%;background:var(--amber);opacity:.8}
.policy strong{font-weight:600}
.policy a,.doc-foot a{color:var(--amber);text-decoration:none}
.policy a:hover,.doc-foot a:hover{text-decoration:underline}
.fields{border:1px solid var(--border);border-radius:12px;background:var(--panel-bg);overflow:hidden;margin:4px 0 16px}
.field{display:grid;grid-template-columns:210px 1fr;gap:18px;padding:14px 20px}
.field+.field{border-top:1px solid var(--border)}
.field dt{font-size:13px;font-weight:600;color:var(--amber);letter-spacing:.3px;line-height:1.7}
.field dd{font-size:14px;line-height:1.8;color:var(--text);font-weight:300}
code{background:var(--card-bg);border:1px solid var(--border);border-radius:4px;padding:1px 6px;font-size:.85em;color:var(--gold);word-break:break-word}
.doc-foot{font-size:12px;color:var(--text-muted);line-height:1.8;border-top:1px solid var(--border);padding-top:20px;margin-top:8px}

footer{padding:60px 0 40px;border-top:1px solid var(--border);text-align:center}
.footer-links{display:flex;justify-content:center;gap:28px;margin-bottom:24px;flex-wrap:wrap}
.footer-links a{color:var(--text-muted);text-decoration:none;font-size:12px;letter-spacing:1px;transition:color .2s;font-weight:500}
.footer-links a:hover,.footer-links a.current{color:var(--amber)}
.footer-copy{font-size:11px;color:#4a4a5a;margin-bottom:16px}

[data-theme="light"] .version-logo{filter:brightness(0.3) drop-shadow(0 0 4px rgba(158,117,32,0.2))}
[data-theme="light"] .fields,[data-theme="light"] .note{box-shadow:0 1px 4px rgba(0,0,0,0.05)}
body,nav,.fields,.note{transition:background-color .4s,color .4s,border-color .4s}

@media(max-width:768px){
  nav{padding:8px 10px;gap:8px;justify-content:flex-end;background:var(--nav-bg);backdrop-filter:blur(8px);width:100%;left:0;right:0}
  nav a{font-size:9px;letter-spacing:.5px}
  nav a.nav-cta{padding:4px 8px}
  .version-tag{top:10px;left:10px}
  .version-logo{width:22px;height:22px}
  .lang-toggle button{padding:3px 8px;font-size:10px}
  .container{padding:0 16px}
  .doc{padding:88px 0 48px}
  .field{grid-template-columns:1fr;gap:4px;padding:12px 14px}
  .policy p,.policy ul li{font-size:14px}
  .footer-links{gap:16px}
}
</style>
<script>
(function () {
  var root = document.documentElement;
  try {
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) root.setAttribute('data-theme', 'light');
  } catch (e) {}
  var m = location.search.match(/[?&]lang=(en|ko)/);
  var saved = null;
  try { saved = localStorage.getItem('oqz-lang'); } catch (e) {}
  var lang = (m && m[1]) || (saved === 'ko' || saved === 'en' ? saved : 'en');
  root.setAttribute('data-lang', lang);
  root.lang = lang;
})();
</script>
</head>
<body>

<div class="progress-bar"><div class="fill" id="progressFill"></div></div>
<a class="version-tag" href="https://oneqaz.com/" aria-label="OneQAZ home"><img src="https://oneqaz.com/images/logo.png" alt="OneQAZ" class="version-logo"></a>

<nav>
  <a href="https://oneqaz.com/">OneQAZ</a>
  <a href="https://blog.oneqaz.com/en/" target="_blank" rel="noopener" class="nav-cta" data-blog-path=""><span data-l="en">Daily Reads ↗</span><span data-l="ko">데일리 리포트 ↗</span></a>
  <div class="lang-toggle">
    <button id="langEn" type="button" onclick="setLang('en')">EN</button>
    <button id="langKo" type="button" onclick="setLang('ko')">KO</button>
  </div>
</nav>

<!-- Cloudflare Email Address Obfuscation would turn the legally required contact addresses into
     "[email protected]" for readers without JavaScript (crawlers, AI readers). email_off keeps them
     as plain text on this page only. -->
<!--email_off-->
<main class="container">
<div class="doc">

<!-- ═══════════════════════════ EN ═══════════════════════════ -->
<article data-l="en" lang="en">
  <div class="section-label">Legal</div>
  <h1 class="section-title">Privacy Policy</h1>
  <p class="section-desc">OneQAZ Trading Intelligence — MCP Server, API &amp; Website</p>
  <div class="doc-meta">Last updated 2026-10-07 · Effective 2026-10-07</div>

  <div class="toc">
    <a href="#en-purpose">Purpose</a><a href="#en-items">Items</a><a href="#en-retention">Retention</a>
    <a href="#en-destruction">Destruction</a><a href="#en-thirdparty">Third parties</a><a href="#en-transfer">Outsourcing &amp; transfer</a>
    <a href="#en-security">Security</a><a href="#en-cookies">Cookies</a><a href="#en-rights">Your rights</a>
    <a href="#en-officer">Privacy officer</a><a href="#en-remedies">Remedies</a><a href="#en-ai">AI clients</a>
    <a href="#en-na">Not applicable</a><a href="#en-changes">Changes</a>
  </div>

  <p class="lead">OneQAZ ("we") provides a research-information service that exposes live market intelligence (crypto,
  Korean stocks, US stocks) to AI clients over the Model Context Protocol (MCP) and an HTTP API. This policy explains how
  we process personal information when you (or an AI client acting on your behalf) use <code>api.oneqaz.com</code>, and
  when you visit <code>oneqaz.com</code>. It is written to meet Korea's Personal Information Protection Act (PIPA) and to
  inform users elsewhere, including in the EU/EEA and the UK. Our own servers are located in the Republic of Korea.</p>

  <div class="note"><strong>Not investment advice.</strong> OneQAZ outputs are paper-trading research signals, not
  recommendations to buy or sell. We do not execute trades, hold funds, or accept financial transactions.</div>

  <div class="policy">
    <section id="en-purpose">
      <h2><span class="num">01</span>Purposes and legal basis</h2>
      <ul>
        <li>Operate and secure the service (rate limiting, abuse prevention, debugging).</li>
        <li>Diagnose client and protocol compatibility problems.</li>
        <li>Measure aggregate usage (which tools are called, by which client type) to improve the product.</li>
        <li>Enforce access tiers, attribute usage to API keys, and issue and manage the API keys you request.</li>
      </ul>
      <p>We provide the service without accounts or consent screens. Request logs are processed on the basis of our
      legitimate interest in operating and securing the service, limited to what is reasonably necessary (PIPA
      Art. 15(1)(6); GDPR Art. 6(1)(f)). The contact email of an API-key holder is processed to issue and manage the key
      you asked for (PIPA Art. 15(1)(4); GDPR Art. 6(1)(b)).</p>
      <p>We do <strong>not</strong> sell your data, build advertising profiles, use request logs to train AI models, or make
      automated decisions about you based on them.</p>
    </section>

    <section id="en-items">
      <h2><span class="num">02</span>Personal information we process</h2>
      <p>We do <strong>not</strong> require user accounts, and we do not collect names or payment details. The following is
      generated automatically for each request to <code>api.oneqaz.com</code>:</p>
      <dl class="fields">
        <div class="field"><dt>IP address</dt><dd>Rate limiting, abuse prevention, and regional routing. For requests routed
        through Cloudflare this is the client address Cloudflare reports.</dd></div>
        <div class="field"><dt>User-agent</dt><dd>To identify the client type (Claude, ChatGPT, Gemini, etc.).</dd></div>
        <div class="field"><dt>Tool / resource name</dt><dd>The requested tool or resource and the request type
        (e.g. <code>get_daily_brief</code>).</dd></div>
        <div class="field"><dt>Outcome metadata</dt><dd>Success/failure, HTTP status, JSON-RPC error code, a short error
        description (at most 200 characters; input values echoed back by validation errors are redacted), latency (ms), and
        the shape of the result (rows returned, total available, whether it was truncated or empty).</dd></div>
        <div class="field"><dt>Derived session key</dt><dd>A hash of IP + user-agent + a 30-minute time bucket. No cookie or
        other persistent identifier is set on your client.</dd></div>
        <div class="field"><dt>API key (if provided)</dt><dd>Used to determine your rate-limit tier and access level. With each
        request we record the resolved tier and a one-way fingerprint of the key (the first 16 hex characters of its SHA-256
        hash). The key itself is never written to request logs.</dd></div>
        <div class="field"><dt>Whitelisted request parameters</dt><dd>A limited summary of tool arguments (e.g.
        <code>symbol</code>, <code>market</code>, <code>interval</code>, <code>category</code>, date ranges, result limits,
        identifiers) and, for the <code>search</code> tool, the search keyword truncated to 60 characters. Calls made without
        arguments are recorded as such. Argument content outside this whitelist is <strong>not</strong> stored.</dd></div>
        <div class="field"><dt>MCP client &amp; protocol metadata</dt><dd>The client name/version sent at
        <code>initialize</code> (<code>clientInfo</code>), the MCP protocol version, the <code>Accept</code> header (first 200
        characters), the protocol session id header if your client sends one, the call order within a session, and
        request/response payload sizes.</dd></div>
        <div class="field"><dt>Traffic classification</dt><dd>A derived label (crawler / operator-self-test / external) used to
        keep aggregate usage statistics honest.</dd></div>
        <div class="field"><dt>Contact email (API-key holders only)</dt><dd>Provided by you when you ask for an API key.</dd></div>
      </dl>
      <p>Of these, the IP address, the derived session key, the API-key fingerprint and the contact email are personal
      information or can identify you in combination; the other fields describe the request, not you. We do not store the
      contents of responses we send you. Visiting <code>oneqaz.com</code> does not send us any personal information (see
      sections 06 and 08).</p>
    </section>

    <section id="en-retention">
      <h2><span class="num">03</span>Retention period</h2>
      <ul>
        <li><strong>IP address and derived session key</strong> — up to <strong>12 months</strong> from the request.</li>
        <li><strong>Other request fields</strong> — kept as usage history. Once the IP-derived fields are replaced they no
        longer identify you, except that requests made with an API key remain attributable to that key while its contact
        email exists.</li>
        <li><strong>Contact email of an API-key holder</strong> — until the key is revoked or you ask us to delete it.</li>
        <li><strong>Database backups</strong> — rotated and deleted within about four weeks.</li>
      </ul>
      <p>If another law requires us to keep data for longer, we keep only the items it requires, for the period it requires,
      separately from other data.</p>
    </section>

    <section id="en-destruction">
      <h2><span class="num">04</span>Destruction procedure and method</h2>
      <ul>
        <li><strong>Procedure</strong> — when the retention period ends, IP addresses and session keys are irreversibly
        replaced by a processing job using a one-way transformation whose random key is discarded after each run, so the
        records can no longer be linked to an IP address. Contact emails are deleted without delay when the key is revoked or
        on your request.</li>
        <li><strong>Method</strong> — electronic records are deleted or overwritten so that they cannot be restored or
        reproduced. We keep no paper records.</li>
      </ul>
    </section>

    <section id="en-thirdparty">
      <h2><span class="num">05</span>Provision to third parties</h2>
      <p>We do not provide personal information to third parties. The only exception is where the law requires it — for
      example, a lawful request from an investigative authority with a warrant (PIPA Arts. 17–18).</p>
    </section>

    <section id="en-transfer">
      <h2><span class="num">06</span>Outsourcing and overseas transfer</h2>
      <p>We entrust the following providers with processing that is necessary to deliver the service over the internet,
      which involves transfer outside Korea. We disclose the details here under PIPA Art. 28-8(1)(3).</p>
      <dl class="fields">
        <div class="field"><dt>Cloudflare, Inc.</dt><dd><strong>Task</strong> CDN, DDoS protection and secure tunnel (TLS
        termination) for <code>api.oneqaz.com</code> · <strong>Country</strong> United States, and the countries of Cloudflare's
        global data-center network · <strong>Items</strong> IP address, request headers (incl. user-agent) and request contents
        in transit · <strong>When &amp; how</strong> over the network, each time you call the API · <strong>Retention</strong>
        per Cloudflare's processing terms · <strong>Contact</strong> privacyquestions@cloudflare.com, 101 Townsend St.,
        San Francisco, CA 94107, USA</dd></div>
        <div class="field"><dt>GitHub, Inc.</dt><dd><strong>Task</strong> hosting the <code>oneqaz.com</code> website
        (GitHub Pages) · <strong>Country</strong> United States · <strong>Items</strong> IP address and browser information of
        website visitors · <strong>When &amp; how</strong> over the network, each time you visit the website ·
        <strong>Retention</strong> per GitHub's policies · <strong>Contact</strong> privacy@github.com, 88 Colin P. Kelly Jr.
        St., San Francisco, CA 94107, USA</dd></div>
        <div class="field"><dt>BunnyWay d.o.o. (Bunny Fonts)</dt><dd><strong>Task</strong> delivering the web fonts used on
        our pages · <strong>Country</strong> Slovenia (EU), and the countries of its CDN · <strong>Items</strong> IP address
        and browser information · <strong>When &amp; how</strong> over the network, when your browser loads the fonts ·
        <strong>Retention</strong> per Bunny's policies · <strong>Contact</strong> support@bunny.net, Dunajska cesta 165,
        1000 Ljubljana, Slovenia</dd></div>
      </dl>
      <p><strong>How to refuse.</strong> These transfers are needed to deliver the API and the website over the internet, so
      to refuse them you need to stop using the API or visiting the website; neither can be provided without them. If your
      browser blocks web fonts, the pages still work with system fonts.</p>
      <p>When you reach OneQAZ through an AI client, the AI platform you chose handles your request under its own policy;
      that is not a transfer made by OneQAZ.</p>
    </section>

    <section id="en-security">
      <h2><span class="num">07</span>Security measures</h2>
      <ul>
        <li><strong>Administrative</strong> — access to personal information is limited to the operator.</li>
        <li><strong>Technical</strong> — encrypted transport (HTTPS/TLS); the database is not exposed to the internet and is
        reachable only from our internal network; separate database roles per service; API keys are never written to request
        logs (one-way fingerprint only); only a whitelisted summary of arguments is stored; IP addresses are irreversibly
        replaced after 12 months.</li>
        <li><strong>Physical</strong> — servers and storage are kept at a location managed directly by the operator.</li>
      </ul>
    </section>

    <section id="en-cookies">
      <h2><span class="num">08</span>Automatic collection devices (cookies)</h2>
      <ul>
        <li>We do not use cookies, analytics, advertising or tracking scripts, and we do not collect behavioural information
        for targeted advertising.</li>
        <li>The website remembers your language choice (<code>oqz-lang</code>) in your browser's local storage only; it is
        never sent to us. You can remove it by clearing this site's data in your browser.</li>
      </ul>
    </section>

    <section id="en-rights">
      <h2><span class="num">09</span>Your rights and how to exercise them</h2>
      <ul>
        <li>You may request access to, correction or deletion of, or suspension of processing of your personal information
        (PIPA Arts. 35–37). Where the GDPR applies, you may also object to processing and request restriction or
        portability.</li>
        <li>Send requests to <a href="mailto:contact@oneqaz.com">contact@oneqaz.com</a>. A legal representative or an
        authorised agent may act for you with a power of attorney.</li>
        <li>Because request logs are not tied to accounts, please include the IP address and approximate time of your
        requests, or the API key concerned (you may send its SHA-256 fingerprint instead of the key itself), so that we can
        find the records.</li>
        <li>We respond within 10 days. Where the law allows us to limit a request (for example, where another law requires
        retention or it would harm someone else's rights), we will tell you the reason.</li>
        <li>If you are in the EU/EEA or the UK, you may also lodge a complaint with your local data-protection authority.</li>
      </ul>
    </section>

    <section id="en-officer">
      <h2><span class="num">10</span>Privacy officer</h2>
      <dl class="fields">
        <div class="field"><dt>Department</dt><dd>OneQAZ Privacy Office — responsible for personal information protection,
        access requests and complaints</dd></div>
        <div class="field"><dt>Contact</dt><dd><a href="mailto:contact@oneqaz.com">contact@oneqaz.com</a></dd></div>
      </dl>
    </section>

    <section id="en-remedies">
      <h2><span class="num">11</span>Remedies for infringement</h2>
      <p>You can seek dispute mediation or advice from the following Korean bodies, which are independent of OneQAZ:</p>
      <ul>
        <li>Personal Information Dispute Mediation Committee — 1833-6972 · <a href="https://www.kopico.go.kr" target="_blank" rel="noopener">www.kopico.go.kr</a></li>
        <li>Personal Information Infringement Report Center (KISA) — 118 · <a href="https://privacy.kisa.or.kr" target="_blank" rel="noopener">privacy.kisa.or.kr</a></li>
        <li>Supreme Prosecutors' Office — 1301 · <a href="https://www.spo.go.kr" target="_blank" rel="noopener">www.spo.go.kr</a></li>
        <li>Korean National Police Agency — 182 · <a href="https://ecrm.police.go.kr" target="_blank" rel="noopener">ecrm.police.go.kr</a></li>
      </ul>
    </section>

    <section id="en-ai">
      <h2><span class="num">12</span>AI client disclosure</h2>
      <p>When an AI client (such as Claude, ChatGPT, or Gemini) calls OneQAZ on your behalf, your request reaches us through
      that client's platform. Every response carries <code>disclaimer</code>, <code>is_investment_advice=false</code>, and
      <code>data_classification=research_information_only</code>.</p>
    </section>

    <section id="en-na">
      <h2><span class="num">13</span>Not applicable</h2>
      <ul>
        <li>We do not process sensitive information or unique identifiers such as resident registration numbers.</li>
        <li>The service is not directed at children under 14, and we do not knowingly collect their information.</li>
        <li>We do not process pseudonymised information under PIPA's special provisions, and we make no automated decisions
        that affect your rights.</li>
      </ul>
    </section>

    <section id="en-changes">
      <h2><span class="num">14</span>Changes</h2>
      <p>This policy takes effect on 2026-10-07. We announce changes by updating the date above; previous versions are
      available in the <a href="https://github.com/wnsod/oneqaz/commits/main/privacy.html" target="_blank" rel="noopener">public
      change history</a>.</p>
    </section>
  </div>

  <p class="doc-foot">This policy is published identically at <a href="https://oneqaz.com/privacy">oneqaz.com/privacy</a>
  and <a href="https://api.oneqaz.com/privacy">api.oneqaz.com/privacy</a>.</p>
</article>

<!-- ═══════════════════════════ KO ═══════════════════════════ -->
<article data-l="ko" lang="ko">
  <div class="section-label">법적 고지</div>
  <h1 class="section-title">개인정보 처리방침</h1>
  <p class="section-desc">OneQAZ 트레이딩 인텔리전스 — MCP 서버, API 및 웹사이트</p>
  <div class="doc-meta">최종 수정 2026-10-07 · 시행 2026-10-07</div>

  <div class="toc">
    <a href="#ko-purpose">처리 목적</a><a href="#ko-items">처리 항목</a><a href="#ko-retention">보유 기간</a>
    <a href="#ko-destruction">파기</a><a href="#ko-thirdparty">제3자 제공</a><a href="#ko-transfer">위탁·국외 이전</a>
    <a href="#ko-security">안전성 확보</a><a href="#ko-cookies">쿠키</a><a href="#ko-rights">이용자 권리</a>
    <a href="#ko-officer">보호책임자</a><a href="#ko-remedies">권익침해 구제</a><a href="#ko-ai">AI 클라이언트</a>
    <a href="#ko-na">해당 없음</a><a href="#ko-changes">변경</a>
  </div>

  <p class="lead">OneQAZ(이하 "OneQAZ")는 Model Context Protocol(MCP)과 HTTP API를 통해 AI 클라이언트에게 라이브 시장 정보
  (암호화폐·국내주식·미국주식)를 제공하는 리서치 정보 서비스입니다. 본 방침은 이용자(또는 이용자를 대신하는 AI 클라이언트)가
  <code>api.oneqaz.com</code>을 이용하거나 <code>oneqaz.com</code>을 방문할 때 개인정보를 어떻게 처리하는지 설명하며,
  「개인정보 보호법」에 따라 작성되었습니다. OneQAZ의 자체 서버는 대한민국에 있습니다.</p>

  <div class="note"><strong>투자 권유가 아닙니다.</strong> OneQAZ의 결과물은 페이퍼 트레이딩 기반 리서치 신호이며, 매수·매도를
  권유하지 않습니다. OneQAZ는 매매를 실행하거나 자금을 보관하거나 금융 거래를 받지 않습니다.</div>

  <div class="policy">
    <section id="ko-purpose">
      <h2><span class="num">01</span>개인정보의 처리 목적 및 근거</h2>
      <ul>
        <li>서비스 운영·보안(요청 수 제한, 남용 방지, 디버깅).</li>
        <li>클라이언트·프로토콜 호환성 문제 진단.</li>
        <li>제품 개선을 위한 집계 사용량 측정(어떤 tool 이 어떤 클라이언트에서 호출되는지).</li>
        <li>접근 등급 적용, API 키별 사용량 집계, 이용자가 요청한 API 키의 발급·관리.</li>
      </ul>
      <p>OneQAZ는 회원가입이나 동의 절차 없이 서비스를 제공합니다. 요청 로그는 서비스 운영·보안이라는 정당한 이익을 달성하기 위해
      합리적으로 필요한 범위에서 「개인정보 보호법」 제15조 제1항 제6호에 근거하여 처리합니다. API 키 보유자의 연락 이메일은 이용자가
      요청한 키를 발급·관리하기 위해 같은 조 제1항 제4호에 근거하여 처리합니다.</p>
      <p>개인정보를 판매하거나 광고 프로필을 만들지 <strong>않으며</strong>, 요청 로그를 AI 모델 학습이나 이용자에 대한 자동화된 결정에
      사용하지 않습니다.</p>
    </section>

    <section id="ko-items">
      <h2><span class="num">02</span>처리하는 개인정보 항목</h2>
      <p>회원 계정을 요구하지 <strong>않으며</strong>, 이름·결제 정보를 수집하지 않습니다. <code>api.oneqaz.com</code>에 대한 요청마다
      다음 정보가 자동으로 생성·기록됩니다.</p>
      <dl class="fields">
        <div class="field"><dt>IP 주소</dt><dd>요청 수 제한, 남용 방지, 지역 라우팅. Cloudflare를 거친 요청은 Cloudflare가 전달한 클라이언트
        주소입니다.</dd></div>
        <div class="field"><dt>User-agent</dt><dd>클라이언트 종류(Claude, ChatGPT, Gemini 등) 식별.</dd></div>
        <div class="field"><dt>tool / resource 이름</dt><dd>요청한 tool 또는 resource 와 요청 유형(예: <code>get_daily_brief</code>).</dd></div>
        <div class="field"><dt>결과 메타데이터</dt><dd>성공·실패, HTTP 상태, JSON-RPC 오류 코드, 짧은 오류 설명(최대 200자 — 검증 오류가 되돌려
        보낸 입력값은 가림), 응답 시간(ms), 결과 형태(반환 행 수, 전체 건수, 잘림·빈 결과 여부).</dd></div>
        <div class="field"><dt>파생 세션 키</dt><dd>IP + user-agent + 30분 구간의 해시. 이용자 기기에 쿠키나 기타 영구 식별자를 남기지 않습니다.</dd></div>
        <div class="field"><dt>API 키(제공한 경우)</dt><dd>요청 한도 등급과 접근 수준 판정에 사용합니다. 요청마다 판정된 등급과 키의 단방향
        지문(SHA-256 해시 앞 16자리)을 기록합니다. 키 자체는 요청 로그에 기록하지 않습니다.</dd></div>
        <div class="field"><dt>허용 목록 요청 인자</dt><dd>tool 인자의 제한된 요약(<code>symbol</code>, <code>market</code>,
        <code>interval</code>, <code>category</code>, 기간, 결과 개수, 식별자 등)과 <code>search</code> 도구의 검색어(최대 60자).
        인자 없이 호출한 경우 그 사실만 기록합니다. 허용 목록 밖의 인자 내용은 저장하지 <strong>않습니다</strong>.</dd></div>
        <div class="field"><dt>MCP 클라이언트·프로토콜 메타데이터</dt><dd><code>initialize</code> 때 보낸 클라이언트 이름·버전
        (<code>clientInfo</code>), MCP 프로토콜 버전, <code>Accept</code> 헤더(앞 200자), 클라이언트가 보낸 경우 프로토콜 세션 ID
        헤더, 세션 내 호출 순서, 요청·응답 크기.</dd></div>
        <div class="field"><dt>트래픽 분류</dt><dd>집계 통계를 정직하게 유지하기 위한 파생 라벨(크롤러 / 운영자 자체 점검 / 외부).</dd></div>
        <div class="field"><dt>연락 이메일(API 키 보유자만)</dt><dd>API 키를 신청할 때 이용자가 제공합니다.</dd></div>
      </dl>
      <p>이 중 IP 주소, 파생 세션 키, API 키 지문, 연락 이메일은 개인정보이거나 다른 정보와 결합하여 이용자를 식별할 수 있는 정보이며,
      나머지는 이용자가 아니라 요청을 설명하는 정보입니다. 이용자에게 보낸 응답 본문은 저장하지 않습니다. <code>oneqaz.com</code>
      방문만으로 OneQAZ에 전달되는 개인정보는 없습니다(06·08 참조).</p>
    </section>

    <section id="ko-retention">
      <h2><span class="num">03</span>개인정보의 처리 및 보유 기간</h2>
      <ul>
        <li><strong>IP 주소와 파생 세션 키</strong> — 요청일로부터 <strong>최대 12개월</strong>.</li>
        <li><strong>그 밖의 요청 기록</strong> — 사용 이력으로 보관합니다. IP 파생 항목이 치환된 뒤에는 이용자를 식별하지 않으며, 다만 API 키로
        한 요청은 그 키의 연락 이메일이 남아 있는 동안 해당 키에 귀속됩니다.</li>
        <li><strong>API 키 보유자의 연락 이메일</strong> — 키를 폐기하거나 이용자가 삭제를 요청할 때까지.</li>
        <li><strong>데이터베이스 백업</strong> — 순환 방식으로 약 4주 이내에 삭제됩니다.</li>
      </ul>
      <p>다른 법령이 더 오래 보존하도록 정한 경우에는 그 법령이 정한 항목만, 정한 기간 동안 다른 정보와 분리하여 보관합니다.</p>
    </section>

    <section id="ko-destruction">
      <h2><span class="num">04</span>개인정보의 파기 절차 및 방법</h2>
      <ul>
        <li><strong>파기 절차</strong> — 보유 기간이 지난 IP 주소와 세션 키는 실행마다 폐기되는 임의 키를 쓴 단방향 변환 작업으로 비가역
        치환하여, 기록을 IP 주소와 연결할 수 없게 합니다. 연락 이메일은 키를 폐기하거나 이용자가 요청하면 지체 없이 삭제합니다.</li>
        <li><strong>파기 방법</strong> — 전자적 파일 형태의 정보는 복구·재생할 수 없도록 삭제하거나 덮어씁니다. 종이 문서는 보관하지 않습니다.</li>
      </ul>
    </section>

    <section id="ko-thirdparty">
      <h2><span class="num">05</span>개인정보의 제3자 제공</h2>
      <p>OneQAZ는 개인정보를 제3자에게 제공하지 않습니다. 다만 수사기관이 영장 등 법령에 따른 절차로 요청하는 경우처럼 법률에 특별한 규정이
      있는 경우는 예외로 합니다(「개인정보 보호법」 제17조·제18조).</p>
    </section>

    <section id="ko-transfer">
      <h2><span class="num">06</span>개인정보 처리의 위탁 및 국외 이전</h2>
      <p>OneQAZ는 인터넷으로 서비스를 제공하는 데 필요한 처리를 아래 업체에 위탁하며, 이 과정에서 개인정보가 국외로 이전됩니다.
      「개인정보 보호법」 제28조의8 제1항 제3호에 따라 그 내용을 여기에 공개합니다.</p>
      <dl class="fields">
        <div class="field"><dt>Cloudflare, Inc.</dt><dd><strong>위탁 업무</strong> <code>api.oneqaz.com</code>의 CDN, DDoS 방어,
        보안 터널(TLS 종단) · <strong>이전 국가</strong> 미국 및 Cloudflare 글로벌 데이터센터 소재 국가 · <strong>이전 항목</strong> IP 주소,
        요청 헤더(user-agent 포함), 전송 중인 요청 내용 · <strong>이전 시기·방법</strong> API 호출 시마다 네트워크로 전송 ·
        <strong>보유 기간</strong> Cloudflare 처리 약관에 따름 · <strong>연락처</strong> privacyquestions@cloudflare.com, 101 Townsend
        St., San Francisco, CA 94107, USA</dd></div>
        <div class="field"><dt>GitHub, Inc.</dt><dd><strong>위탁 업무</strong> <code>oneqaz.com</code> 웹사이트 호스팅(GitHub Pages) ·
        <strong>이전 국가</strong> 미국 · <strong>이전 항목</strong> 웹사이트 방문자의 IP 주소와 브라우저 정보 · <strong>이전 시기·방법</strong>
        웹사이트 방문 시마다 네트워크로 전송 · <strong>보유 기간</strong> GitHub 정책에 따름 · <strong>연락처</strong> privacy@github.com,
        88 Colin P. Kelly Jr. St., San Francisco, CA 94107, USA</dd></div>
        <div class="field"><dt>BunnyWay d.o.o. (Bunny Fonts)</dt><dd><strong>위탁 업무</strong> 페이지에 쓰는 웹 글꼴 제공 ·
        <strong>이전 국가</strong> 슬로베니아(EU) 및 CDN 소재 국가 · <strong>이전 항목</strong> IP 주소와 브라우저 정보 ·
        <strong>이전 시기·방법</strong> 브라우저가 글꼴을 불러올 때 네트워크로 전송 · <strong>보유 기간</strong> Bunny 정책에 따름 ·
        <strong>연락처</strong> support@bunny.net, Dunajska cesta 165, 1000 Ljubljana, Slovenia</dd></div>
      </dl>
      <p><strong>거부 방법과 효과.</strong> 위 이전은 API와 웹사이트를 인터넷으로 제공하는 데 필요하므로, 거부하려면 API 이용이나 웹사이트
      방문을 중단해야 하며 이 경우 서비스를 이용할 수 없습니다. 브라우저에서 웹 글꼴을 차단해도 페이지는 기본 글꼴로 정상 이용할 수 있습니다.</p>
      <p>AI 클라이언트를 통해 OneQAZ를 이용하는 경우, 이용자가 선택한 AI 플랫폼이 자체 방침에 따라 요청을 처리하며 이는 OneQAZ가 하는 국외
      이전이 아닙니다.</p>
    </section>

    <section id="ko-security">
      <h2><span class="num">07</span>개인정보의 안전성 확보 조치</h2>
      <ul>
        <li><strong>관리적 조치</strong> — 개인정보에 대한 접근 권한을 운영자로 제한합니다.</li>
        <li><strong>기술적 조치</strong> — 전송 구간 암호화(HTTPS/TLS), 데이터베이스는 인터넷에 노출하지 않고 내부망에서만 접근,
        서비스별 데이터베이스 계정 분리, API 키는 요청 로그에 원문 대신 단방향 지문만 기록, 요청 인자는 허용 목록 요약만 저장,
        12개월이 지난 IP 주소는 비가역 치환.</li>
        <li><strong>물리적 조치</strong> — 서버와 저장장치는 운영자가 직접 관리하는 장소에 둡니다.</li>
      </ul>
    </section>

    <section id="ko-cookies">
      <h2><span class="num">08</span>개인정보 자동 수집 장치의 설치·운영 및 거부</h2>
      <ul>
        <li>쿠키, 방문 분석, 광고·추적 스크립트를 사용하지 않으며, 맞춤형 광고를 위한 행태정보를 수집하지 않습니다.</li>
        <li>웹사이트는 언어 선택(<code>oqz-lang</code>)을 이용자 브라우저의 로컬 저장소에만 기억하며 OneQAZ로 전송하지 않습니다. 브라우저에서
        이 사이트의 데이터를 지우면 삭제됩니다.</li>
      </ul>
    </section>

    <section id="ko-rights">
      <h2><span class="num">09</span>정보주체의 권리·의무 및 행사 방법</h2>
      <ul>
        <li>이용자는 언제든지 개인정보의 열람, 정정·삭제, 처리정지를 요구할 수 있습니다(「개인정보 보호법」 제35조~제37조).</li>
        <li>요구는 <a href="mailto:contact@oneqaz.com">contact@oneqaz.com</a>으로 보내 주십시오. 법정대리인이나 위임을 받은 대리인을 통해서도
        할 수 있으며, 이 경우 위임장을 제출해야 합니다.</li>
        <li>요청 로그는 계정과 연결되어 있지 않으므로, 기록을 찾을 수 있도록 요청에 사용한 IP 주소와 대략적인 이용 시간, 또는 해당 API 키
        (키 대신 SHA-256 지문을 보내도 됩니다)를 함께 알려 주십시오.</li>
        <li>요구를 받은 날부터 10일 이내에 처리 결과를 알려 드립니다. 다른 법령에 따라 보존해야 하거나 다른 사람의 권리를 침해할 우려가 있는 등
        법령상 제한 사유가 있는 경우에는 그 사유를 알려 드립니다.</li>
      </ul>
    </section>

    <section id="ko-officer">
      <h2><span class="num">10</span>개인정보 보호책임자</h2>
      <dl class="fields">
        <div class="field"><dt>담당 부서</dt><dd>OneQAZ 개인정보 보호 담당 — 개인정보 보호 업무, 열람 청구 접수·처리, 고충 처리</dd></div>
        <div class="field"><dt>연락처</dt><dd><a href="mailto:contact@oneqaz.com">contact@oneqaz.com</a></dd></div>
      </dl>
    </section>

    <section id="ko-remedies">
      <h2><span class="num">11</span>권익침해 구제 방법</h2>
      <p>개인정보 침해로 인한 구제를 받기 위해 아래 기관에 분쟁 조정이나 상담을 신청할 수 있습니다. 이 기관들은 OneQAZ와 별개의 기관입니다.</p>
      <ul>
        <li>개인정보분쟁조정위원회 — 1833-6972 · <a href="https://www.kopico.go.kr" target="_blank" rel="noopener">www.kopico.go.kr</a></li>
        <li>개인정보침해신고센터(한국인터넷진흥원) — (국번 없이) 118 · <a href="https://privacy.kisa.or.kr" target="_blank" rel="noopener">privacy.kisa.or.kr</a></li>
        <li>대검찰청 — (국번 없이) 1301 · <a href="https://www.spo.go.kr" target="_blank" rel="noopener">www.spo.go.kr</a></li>
        <li>경찰청 — (국번 없이) 182 · <a href="https://ecrm.police.go.kr" target="_blank" rel="noopener">ecrm.police.go.kr</a></li>
      </ul>
    </section>

    <section id="ko-ai">
      <h2><span class="num">12</span>AI 클라이언트 고지</h2>
      <p>AI 클라이언트(Claude, ChatGPT, Gemini 등)가 이용자를 대신해 OneQAZ를 호출하면, 요청은 해당 클라이언트의 플랫폼을 거쳐 도달합니다.
      모든 응답에는 <code>disclaimer</code>, <code>is_investment_advice=false</code>,
      <code>data_classification=research_information_only</code> 가 포함됩니다.</p>
    </section>

    <section id="ko-na">
      <h2><span class="num">13</span>해당하지 않는 사항</h2>
      <ul>
        <li>민감정보와 주민등록번호 등 고유식별정보를 처리하지 않습니다.</li>
        <li>만 14세 미만 아동을 대상으로 하는 서비스가 아니며, 아동의 개인정보를 알면서 수집하지 않습니다.</li>
        <li>「개인정보 보호법」의 가명정보 처리 특례를 이용하지 않으며, 이용자의 권리에 영향을 미치는 자동화된 결정을 하지 않습니다.</li>
      </ul>
    </section>

    <section id="ko-changes">
      <h2><span class="num">14</span>개인정보 처리방침의 변경</h2>
      <p>본 방침은 2026년 10월 7일부터 시행합니다. 변경 사항은 상단의 날짜를 갱신하여 알리며, 이전 처리방침은
      <a href="https://github.com/wnsod/oneqaz/commits/main/privacy.html" target="_blank" rel="noopener">공개 변경 이력</a>에서 확인할 수 있습니다.</p>
    </section>
  </div>

  <p class="doc-foot">본 방침은 <a href="https://oneqaz.com/privacy">oneqaz.com/privacy</a> 와
  <a href="https://api.oneqaz.com/privacy">api.oneqaz.com/privacy</a> 에 동일하게 게시됩니다.</p>
</article>

</div>
</main>

<footer>
  <div class="container">
    <div class="footer-links">
      <a href="https://github.com/wnsod/oneqaz-trading-mcp" target="_blank" rel="noopener">GitHub</a>
      <a href="https://pypi.org/project/oneqaz-trading-mcp/" target="_blank" rel="noopener">PyPI</a>
      <a href="https://blog.oneqaz.com/en/rss.xml" target="_blank" rel="noopener" data-blog-path="rss.xml">RSS</a>
      <a href="https://oneqaz.com/privacy" class="current">Privacy</a>
      <a href="https://oneqaz.com/terms">Terms</a>
      <a href="mailto:contact@oneqaz.com">Contact</a>
    </div>
    <div class="footer-copy">&copy; 2026 OneQAZ. All rights reserved.</div>
  </div>
</footer>
<!--/email_off-->

<script>
var BLOG_BASE = 'https://blog.oneqaz.com/';
var TITLES = { en: 'Privacy Policy — OneQAZ', ko: '개인정보 처리방침 — OneQAZ' };

function setLang(lang) {
  var root = document.documentElement;
  root.setAttribute('data-lang', lang);
  root.lang = lang;
  try { localStorage.setItem('oqz-lang', lang); } catch (e) {}
  document.getElementById('langEn').classList.toggle('active', lang === 'en');
  document.getElementById('langKo').classList.toggle('active', lang === 'ko');
  document.title = TITLES[lang];
  // Blog links follow the site language (EN → blog /en/ mirror) — same rule as the home page.
  var links = document.querySelectorAll('a[data-blog-path]');
  for (var i = 0; i < links.length; i++) {
    links[i].href = BLOG_BASE + (lang === 'en' ? 'en/' : '') + links[i].getAttribute('data-blog-path');
  }
}
setLang(document.documentElement.getAttribute('data-lang') === 'ko' ? 'ko' : 'en');

try {
  window.matchMedia('(prefers-color-scheme: light)').addEventListener('change', function (e) {
    document.documentElement.setAttribute('data-theme', e.matches ? 'light' : 'dark');
  });
} catch (e) {}

var NAV = document.querySelector('nav');
function onScroll() {
  var h = document.documentElement;
  var max = h.scrollHeight - h.clientHeight;
  document.getElementById('progressFill').style.width = (max > 0 ? h.scrollTop / max * 100 : 0) + '%';
  NAV.classList.toggle('scrolled', h.scrollTop > 24);
}
window.addEventListener('scroll', onScroll);
onScroll();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# [2026-07-08] Pricing / Keys HTML — _value_signals·403·429 가 광고하는 URL 의 실체.
# self-contained (외부 자원 0). 실제 과금(Stripe)은 의도적 보류 상태
# (docs/handoff/monetization_activation_2026_06_17.md SLA 졸업 게이트) — 그때까지
# pro 키는 이메일 신청 경로로 안내한다.
# ---------------------------------------------------------------------------

_PAGE_STYLE = """
body{background:#1c1c24;color:#e8e4df;font-family:'Outfit','Noto Sans KR',system-ui,sans-serif;
line-height:1.7;max-width:820px;margin:0 auto;padding:4rem 1.5rem 6rem}
a{color:#d4a853;text-decoration:none}a:hover{text-decoration:underline}
h1{font-size:2rem;margin-bottom:.3rem}h2{color:#d4a853;font-size:1.2rem;margin:2.2rem 0 .7rem;
border-bottom:1px solid #3a3a4a;padding-bottom:.4rem}
.sub{color:#8a8680;margin-bottom:2rem}
table{border-collapse:collapse;width:100%;margin:1rem 0}
th,td{border:1px solid #3a3a4a;padding:.6rem .8rem;text-align:left;font-size:.95rem}
th{background:#252530;color:#d4a853}
code{background:#2a2a36;border:1px solid #3a3a4a;border-radius:4px;padding:.1rem .4rem;
font-size:.85em;color:#c49b6f}pre{background:#2a2a36;border:1px solid #3a3a4a;border-radius:6px;
padding:1rem;overflow-x:auto;font-size:.85em}
.note{background:#252530;border-left:3px solid #d4a853;padding:1rem 1.2rem;border-radius:4px;margin:1.2rem 0;font-size:.95rem}
ul{margin:0 0 1rem 1.3rem}li{margin-bottom:.5rem}
"""

_PRICING_HTML = f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Pricing — OneQAZ Trading Intelligence</title>
<meta name="robots" content="index,follow"><style>{_PAGE_STYLE}</style></head><body>
<a href="https://oneqaz.com" style="color:#8a8680;font-size:.9rem">&larr; OneQAZ</a>
<h1>Pricing</h1>
<div class="sub">OneQAZ Trading Intelligence &mdash; MCP Server &amp; API (api.oneqaz.com)</div>
<div class="note"><strong>Not investment advice.</strong> All tools serve paper-trading research
information (<code>data_classification=research_information_only</code>).</div>
<table>
<tr><th></th><th>Free (default)</th><th>Pro</th></tr>
<tr><td>API key</td><td>Not required</td><td>Required (<a href="/keys">get a key</a>)</td></tr>
<tr><td>Daily calls</td><td>1,500 / day per IP</td><td>50,000 / day per key</td></tr>
<tr><td>Burst</td><td>60 / min</td><td>200 / min</td></tr>
<tr><td>Tools &amp; resources</td><td colspan="2">Identical — every tool is available on both tiers.
Raw evidence and provenance are never paywalled.</td></tr>
<tr><td>Price</td><td>$0</td><td>Metered billing launches after our public SLA gate is met.
Early access: <a href="mailto:contact@oneqaz.com">contact@oneqaz.com</a></td></tr>
</table>
<h2>Why the same tools on both tiers?</h2>
<p>OneQAZ sells <em>volume and reliability</em>, not gated data. An AI agent must be able to verify
our track record (<code>get_prediction_accuracy</code>, <code>get_ledger_integrity</code>,
<code>get_resolved_predictions</code>) before anyone pays — a paywalled trust layer defeats itself.</p>
<h2>Quota signals</h2>
<p>Every response includes <code>X-RateLimit-*</code> headers and a <code>_value_signals</code>
block. HTTP 429 includes <code>Retry-After</code>.</p>
<p style="color:#8a8680;font-size:.85rem;margin-top:2.5rem;border-top:1px solid #3a3a4a;padding-top:1.5rem">
&copy; 2026 OneQAZ &middot; <a href="/privacy">Privacy</a> &middot; <a href="/terms">Terms</a> &middot; <a href="https://oneqaz.com">oneqaz.com</a></p>
</body></html>"""

_KEYS_HTML = f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>API Keys — OneQAZ Trading Intelligence</title>
<meta name="robots" content="index,follow"><style>{_PAGE_STYLE}</style></head><body>
<a href="https://oneqaz.com" style="color:#8a8680;font-size:.9rem">&larr; OneQAZ</a>
<h1>API Keys</h1>
<div class="sub">Optional &mdash; only needed for Pro-tier quota. Free tier works with no key.</div>
<h2>Using a key</h2>
<p>Send it on every request as a header (either form works):</p>
<pre>X-API-Key: &lt;your-key&gt;
Authorization: Bearer &lt;your-key&gt;</pre>
<p>Remote MCP endpoint: <code>https://api.oneqaz.com/mcp</code> (streamable-http).</p>
<h2>Getting a Pro key</h2>
<p>Self-serve signup opens with metered billing (see <a href="/pricing">pricing</a>). Until then,
request early access by email &mdash; include your use case and expected daily call volume:</p>
<p><a href="mailto:contact@oneqaz.com?subject=OneQAZ%20Pro%20API%20key">contact@oneqaz.com</a></p>
<h2>Key handling</h2>
<ul>
<li>Keys determine your rate-limit tier; usage is attributed per key through a one-way fingerprint, never the key itself &mdash; we hold no payment details (<a href="/privacy">privacy</a>).</li>
<li>Lost or leaked keys are revoked and reissued on request.</li>
</ul>
<p style="color:#8a8680;font-size:.85rem;margin-top:2.5rem;border-top:1px solid #3a3a4a;padding-top:1.5rem">
&copy; 2026 OneQAZ &middot; <a href="/privacy">Privacy</a> &middot; <a href="https://oneqaz.com">oneqaz.com</a></p>
</body></html>"""

# [2026-10-07] Terms — landing/terms.html(oneqaz.com/terms)과 **바이트 동일**한 단일 원본(07-08 Terms of Use 대체,
# 라이브 판 전용 조항·준거법 병합). ⛔ 한쪽만 고치지 말 것 — landing 을 고친 뒤 그대로 붙인다(백슬래시·삼중따옴표 금지).
_TERMS_HTML = """<!DOCTYPE html>
<html lang="en" data-lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Terms of Service — OneQAZ</title>
<meta name="description" content="OneQAZ terms of service (MCP server, API) — research information, not investment advice; AI client obligations, acceptable use, API keys and rate limits, availability, ledger anchors. 이용약관.">
<meta name="robots" content="index,follow">
<link rel="canonical" href="https://oneqaz.com/terms">
<link rel="icon" href="https://oneqaz.com/images/logo.png" type="image/png">
<link rel="preconnect" href="https://fonts.bunny.net">
<link href="https://fonts.bunny.net/css?family=outfit:300,400,500,600,700|noto-sans-kr:300,400,500,700" rel="stylesheet">
<!--
  ONE SOURCE, TWO URLS — this exact file is served at https://oneqaz.com/terms (landing repo)
  and https://api.oneqaz.com/terms (monorepo, same convention as _PRIVACY_HTML). Keep them byte-identical: links and assets are absolute for that reason.
  No backslashes and no triple quotes anywhere — the MCP copy lives inside a Python string.
-->
<style>
*,*::before,*::after{margin:0;padding:0;box-sizing:border-box}
:root,[data-theme="dark"]{
  --bg:#1c1c24;--panel-bg:#252530;--card-bg:#2a2a36;
  --text:#e8e4df;--text-muted:#8a8680;
  --amber:#d4a853;--green:#5cb85c;--red:#d9534f;--blue:#6b9fc9;--gold:#c49b6f;
  --border:#3a3a4a;--hover:rgba(255,255,255,0.02);
  --font:'Outfit','Noto Sans KR',sans-serif;
  --scrollbar-thumb:rgba(255,255,255,0.1);
  --nav-bg:rgba(28,28,36,0.9);
}
[data-theme="light"]{
  --bg:#e8e4de;--panel-bg:#f2efe9;--card-bg:#ece9e3;
  --text:#3a3632;--text-muted:#7a756e;
  --amber:#9e7520;--green:#2d7a3a;--red:#b03225;--blue:#4272a0;--gold:#8c6a38;
  --border:#cdc7bc;--hover:rgba(0,0,0,0.03);
  --scrollbar-thumb:rgba(0,0,0,0.15);
  --nav-bg:rgba(242,239,233,0.9);
}
html{scroll-behavior:smooth;font-size:16px}
body{background:var(--bg);color:var(--text);font-family:var(--font);line-height:1.6;overflow-x:hidden}
::selection{background:var(--amber);color:var(--bg)}
body::after{content:'';position:fixed;inset:0;pointer-events:none;z-index:9999;background:repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(0,0,0,0.06) 2px,rgba(0,0,0,0.06) 4px);transition:opacity .4s}
[data-theme="light"] body::after{opacity:0.3}
body::before{
  content:'';position:fixed;inset:-10%;pointer-events:none;z-index:9998;
  background:radial-gradient(ellipse at center,transparent 55%,rgba(0,0,0,0.18) 100%);
  clip-path:polygon(50% 0%,93.3% 25%,93.3% 75%,50% 100%,6.7% 75%,6.7% 25%);
  transition:opacity .4s;
}
[data-theme="light"] body::before{opacity:0.08}
::-webkit-scrollbar{width:4px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--scrollbar-thumb);border-radius:2px}

/* Language: both languages ship in the HTML (crawlable); the toggle hides one. */
[data-lang="en"] [data-l="ko"],[data-lang="ko"] [data-l="en"]{display:none !important}

.version-tag{position:fixed;top:12px;left:16px;z-index:100;display:block}
.version-logo{width:28px;height:28px;filter:drop-shadow(0 0 6px rgba(212,168,83,0.3));opacity:.8;transition:opacity .2s}
.version-logo:hover{opacity:1}
.progress-bar{position:fixed;top:0;left:0;width:100%;height:2px;z-index:1000;background:var(--panel-bg)}
.progress-bar .fill{height:100%;width:0;background:linear-gradient(90deg,var(--amber),var(--gold));transition:width .3s}

nav{position:fixed;top:0;right:0;z-index:100;padding:16px 24px;display:flex;gap:20px;align-items:center;font-size:12px;transition:background-color .3s}
/* Document page: once scrolled, give the nav the same frosted bar the home page uses on mobile so body text never runs under it. */
nav.scrolled{left:0;justify-content:flex-end;background:var(--nav-bg);backdrop-filter:blur(8px)}
nav a{color:var(--text-muted);text-decoration:none;letter-spacing:1.5px;transition:color .2s;text-transform:uppercase;font-weight:500}
nav a:hover{color:var(--amber)}
nav a.nav-cta{border:1px solid var(--amber);color:var(--amber);padding:5px 12px;border-radius:6px;font-weight:600;transition:background .2s,box-shadow .2s,color .2s}
nav a.nav-cta:hover{background:rgba(212,168,83,0.1);box-shadow:0 0 12px rgba(212,168,83,0.15);color:var(--amber)}
.lang-toggle{display:flex;border:1px solid var(--border);border-radius:6px;overflow:hidden;margin-left:4px}
.lang-toggle button{background:transparent;border:none;color:var(--text-muted);padding:4px 10px;font-size:11px;font-weight:600;letter-spacing:1px;cursor:pointer;font-family:var(--font);transition:all .2s}
.lang-toggle button.active{background:var(--amber);color:var(--bg)}

.container{max-width:1080px;width:100%;margin:0 auto;padding:0 40px}
.doc{max-width:820px;padding:128px 0 72px}
.section-label{font-size:11px;color:var(--amber);letter-spacing:3px;text-transform:uppercase;margin-bottom:8px;font-weight:600;opacity:.8}
.section-title{font-size:clamp(28px,4vw,40px);font-weight:700;margin-bottom:14px;color:var(--text);line-height:1.25}
.section-desc{font-size:14px;color:var(--text-muted);max-width:720px;line-height:1.9;margin-bottom:10px;font-weight:300}
.doc-meta{font-size:12px;color:var(--gold);letter-spacing:1px;font-weight:500;margin-bottom:28px}
.toc{display:flex;flex-wrap:wrap;gap:8px;margin:0 0 40px}
.toc a{font-size:11px;letter-spacing:1px;text-transform:uppercase;color:var(--text-muted);border:1px solid var(--border);border-radius:6px;padding:4px 10px;text-decoration:none;font-weight:500;transition:color .2s,border-color .2s}
.toc a:hover{color:var(--amber);border-color:var(--amber)}

.lead{font-size:15px;line-height:1.9;font-weight:300;color:var(--text);margin-bottom:20px}
.note{background:var(--panel-bg);border:1px solid var(--border);border-left:3px solid var(--amber);border-radius:10px;padding:16px 20px;font-size:14px;line-height:1.85;font-weight:300;margin:0 0 44px}
.note strong{color:var(--amber)}

.policy section{scroll-margin-top:84px;margin-bottom:44px}
.policy h2{display:flex;align-items:baseline;gap:12px;font-size:20px;font-weight:600;color:var(--text);margin-bottom:16px;padding-bottom:10px;border-bottom:1px solid var(--border)}
.policy h2 .num{font-size:11px;color:var(--amber);letter-spacing:2px;font-weight:600;flex-shrink:0}
.policy p{font-size:15px;line-height:1.9;color:var(--text);font-weight:300;margin-bottom:14px}
.policy ul{list-style:none;margin:0 0 14px}
.policy ul li{position:relative;padding-left:20px;font-size:15px;line-height:1.85;font-weight:300;margin-bottom:10px}
.policy ul li::before{content:'';position:absolute;left:3px;top:.72em;width:6px;height:6px;border-radius:50%;background:var(--amber);opacity:.8}
.policy strong{font-weight:600}
.policy a,.doc-foot a{color:var(--amber);text-decoration:none}
.policy a:hover,.doc-foot a:hover{text-decoration:underline}
.fields{border:1px solid var(--border);border-radius:12px;background:var(--panel-bg);overflow:hidden;margin:4px 0 16px}
.field{display:grid;grid-template-columns:210px 1fr;gap:18px;padding:14px 20px}
.field+.field{border-top:1px solid var(--border)}
.field dt{font-size:13px;font-weight:600;color:var(--amber);letter-spacing:.3px;line-height:1.7}
.field dd{font-size:14px;line-height:1.8;color:var(--text);font-weight:300}
code{background:var(--card-bg);border:1px solid var(--border);border-radius:4px;padding:1px 6px;font-size:.85em;color:var(--gold);word-break:break-word}
.doc-foot{font-size:12px;color:var(--text-muted);line-height:1.8;border-top:1px solid var(--border);padding-top:20px;margin-top:8px}

footer{padding:60px 0 40px;border-top:1px solid var(--border);text-align:center}
.footer-links{display:flex;justify-content:center;gap:28px;margin-bottom:24px;flex-wrap:wrap}
.footer-links a{color:var(--text-muted);text-decoration:none;font-size:12px;letter-spacing:1px;transition:color .2s;font-weight:500}
.footer-links a:hover,.footer-links a.current{color:var(--amber)}
.footer-copy{font-size:11px;color:#4a4a5a;margin-bottom:16px}

[data-theme="light"] .version-logo{filter:brightness(0.3) drop-shadow(0 0 4px rgba(158,117,32,0.2))}
[data-theme="light"] .fields,[data-theme="light"] .note{box-shadow:0 1px 4px rgba(0,0,0,0.05)}
body,nav,.fields,.note{transition:background-color .4s,color .4s,border-color .4s}

@media(max-width:768px){
  nav{padding:8px 10px;gap:8px;justify-content:flex-end;background:var(--nav-bg);backdrop-filter:blur(8px);width:100%;left:0;right:0}
  nav a{font-size:9px;letter-spacing:.5px}
  nav a.nav-cta{padding:4px 8px}
  .version-tag{top:10px;left:10px}
  .version-logo{width:22px;height:22px}
  .lang-toggle button{padding:3px 8px;font-size:10px}
  .container{padding:0 16px}
  .doc{padding:88px 0 48px}
  .field{grid-template-columns:1fr;gap:4px;padding:12px 14px}
  .policy p,.policy ul li{font-size:14px}
  .footer-links{gap:16px}
}
</style>
<script>
(function () {
  var root = document.documentElement;
  try {
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) root.setAttribute('data-theme', 'light');
  } catch (e) {}
  var m = location.search.match(/[?&]lang=(en|ko)/);
  var saved = null;
  try { saved = localStorage.getItem('oqz-lang'); } catch (e) {}
  var lang = (m && m[1]) || (saved === 'ko' || saved === 'en' ? saved : 'en');
  root.setAttribute('data-lang', lang);
  root.lang = lang;
})();
</script>
</head>
<body>

<div class="progress-bar"><div class="fill" id="progressFill"></div></div>
<a class="version-tag" href="https://oneqaz.com/" aria-label="OneQAZ home"><img src="https://oneqaz.com/images/logo.png" alt="OneQAZ" class="version-logo"></a>

<nav>
  <a href="https://oneqaz.com/">OneQAZ</a>
  <a href="https://blog.oneqaz.com/en/" target="_blank" rel="noopener" class="nav-cta" data-blog-path=""><span data-l="en">Daily Reads ↗</span><span data-l="ko">데일리 리포트 ↗</span></a>
  <div class="lang-toggle">
    <button id="langEn" type="button" onclick="setLang('en')">EN</button>
    <button id="langKo" type="button" onclick="setLang('ko')">KO</button>
  </div>
</nav>

<!-- Cloudflare Email Address Obfuscation would turn contact addresses into "[email protected]" for readers
     without JavaScript (crawlers, AI readers). email_off keeps them as plain text on this page only. -->
<!--email_off-->
<main class="container">
<div class="doc">

<!-- ═══════════════════════════ EN ═══════════════════════════ -->
<article data-l="en" lang="en">
  <div class="section-label">Legal</div>
  <h1 class="section-title">Terms of Service</h1>
  <p class="section-desc">OneQAZ Trading Intelligence — MCP Server &amp; API</p>
  <div class="doc-meta">Last updated 2026-10-07 · Effective 2026-10-07</div>

  <div class="toc">
    <a href="#en-service">Service</a><a href="#en-advice">Not advice</a><a href="#en-clients">AI clients</a>
    <a href="#en-use">Acceptable use</a><a href="#en-access">Keys &amp; limits</a><a href="#en-data">Data</a>
    <a href="#en-availability">Availability</a><a href="#en-ledger">Ledger anchors</a><a href="#en-ip">IP</a>
    <a href="#en-suspension">Suspension</a><a href="#en-changes">Changes</a><a href="#en-law">Governing law</a>
    <a href="#en-contact">Contact</a>
  </div>

  <p class="lead">By connecting to <code>api.oneqaz.com</code> — directly, or through an AI client acting on your behalf —
  you agree to these Terms. If you operate an AI client or integration that connects to OneQAZ, you accept them on behalf
  of that client.</p>

  <div class="note"><strong>Not investment advice.</strong> OneQAZ outputs are paper-trading research signals, not
  recommendations to buy or sell. We do not execute trades, hold funds, or accept financial transactions.</div>

  <div class="policy">
    <section id="en-service">
      <h2><span class="num">01</span>What this service is</h2>
      <p>OneQAZ exposes live market intelligence (crypto, Korean stocks, US stocks) and global macro context to AI clients
      over the Model Context Protocol (MCP) and an HTTP API. It is a <strong>research-information service</strong>.</p>
      <p>The service can be used without an account. Access is attributed to the connecting client (AI platform, developer
      or organization) or, when one is used, to an API key — not to individual end users.</p>
    </section>

    <section id="en-advice">
      <h2><span class="num">02</span>Not investment advice</h2>
      <p>This is the most important term in this document.</p>
      <ul>
        <li>OneQAZ outputs are <strong>research signals and observed market states</strong>, not recommendations to buy, sell or hold any asset.</li>
        <li>Performance figures come from <strong>paper trading</strong> (simulated, no real money). Past simulated performance does not predict future real-money results.</li>
        <li>OneQAZ is <strong>not</strong> a licensed investment adviser, broker or financial institution and provides no personalized investment advice. Using the service creates no advisory or fiduciary relationship.</li>
        <li>Responses carry <code>disclaimer</code>, <code>is_investment_advice=false</code> and <code>data_classification=research_information_only</code>.</li>
        <li><strong>You are solely responsible for any trading or financial decision you make.</strong></li>
      </ul>
    </section>

    <section id="en-clients">
      <h2><span class="num">03</span>Obligations of AI clients</h2>
      <p>If you operate an AI client or product that relays OneQAZ data to people, you must:</p>
      <ul>
        <li>Surface the <code>disclaimer</code> when quoting OneQAZ data to a person.</li>
        <li>Not present OneQAZ output as personalized investment advice or as a buy/sell recommendation.</li>
        <li>Not strip the paper-trading context from performance figures.</li>
        <li>Not attribute to OneQAZ a recommendation it did not make.</li>
        <li>Keep the <code>disclaimer</code>, <code>data_classification</code> and source (<code>_source</code>) fields intact when relaying data.</li>
      </ul>
    </section>

    <section id="en-use">
      <h2><span class="num">04</span>Acceptable use</h2>
      <p>You may query the service for research, analysis and product integration. You may <strong>not</strong>:</p>
      <ul>
        <li>Circumvent rate limits, or use multiple identities or keys to obtain additional quota.</li>
        <li>Redistribute or resell responses, in whole or in substantial part, as a data product or feed, or scrape the dataset in bulk.</li>
        <li>Reconstruct underlying market data (such as prices, volumes or order books) from responses.</li>
        <li>Attempt to access non-public endpoints, other clients' data or the underlying infrastructure.</li>
        <li>Use the service in a way that degrades availability for other clients, including high-frequency automated polling that never completes a request.</li>
      </ul>
      <p>You are responsible for complying with the laws that apply to your use and with the terms of any upstream data source.</p>
    </section>

    <section id="en-access">
      <h2><span class="num">05</span>Access, API keys and rate limits</h2>
      <dl class="fields">
        <div class="field"><dt>Free — no key</dt><dd>1,500 requests per day · 60 per minute</dd></div>
        <div class="field"><dt>Pro — API key</dt><dd>50,000 requests per day · 200 per minute</dd></div>
      </dl>
      <ul>
        <li>Requests without a key use the Free tier. A request with an <strong>invalid, inactive or expired key is rejected</strong> with HTTP 401 and a <code>WWW-Authenticate</code> header; it is not silently downgraded.</li>
        <li>Exceeding a limit returns HTTP 429 with a <code>Retry-After</code> header. Current limits are reported in <code>X-RateLimit-*</code> response headers.</li>
        <li>API keys are issued to the requester, must be kept confidential and must not be shared or resold. We may revoke a key that is misused or exposed.</li>
        <li>The Free tier is free of charge. Terms for paid (Pro) access or metered billing will be published before they take effect; unpaid Pro access may be suspended.</li>
        <li>Tiers and limits may change; material changes are announced before they take effect.</li>
      </ul>
    </section>

    <section id="en-data">
      <h2><span class="num">06</span>Data we record</h2>
      <p>We record the minimum needed to operate, secure and improve the service — request metadata, a whitelisted summary
      of structured tool parameters, the <code>search</code> keyword truncated to 60 characters, and a one-way fingerprint of
      an API key (never the key itself). What we record, why, and for how long is set out in the
      <a href="https://oneqaz.com/privacy">Privacy Policy</a>, which forms part of these Terms.</p>
    </section>

    <section id="en-availability">
      <h2><span class="num">07</span>Availability and accuracy</h2>
      <p>The service is provided <strong>"as is"</strong>, without warranty of any kind.</p>
      <ul>
        <li>No guaranteed uptime, latency, or continuity of any specific tool or field.</li>
        <li>Market data may be delayed, incomplete, revised or unavailable.</li>
        <li>Tools, schemas and response shapes may change; breaking changes are announced where practicable.</li>
        <li>To the extent permitted by law, OneQAZ is not liable for losses arising from use of, or inability to use, the service.</li>
      </ul>
    </section>

    <section id="en-ledger">
      <h2><span class="num">08</span>Prediction ledger and public anchors</h2>
      <p>Predictions are recorded before they are scored, and the prediction ledger is hash-chained daily. The chain hash is
      periodically published at <a href="https://blog.oneqaz.com/anchors/" target="_blank" rel="noopener">blog.oneqaz.com/anchors</a>
      with an OpenTimestamps proof, so anyone can check that published records were not altered afterwards. These anchors prove
      <strong>immutability after publication</strong> — not the accuracy or future performance of any signal.</p>
    </section>

    <section id="en-ip">
      <h2><span class="num">09</span>Intellectual property</h2>
      <p>OneQAZ retains all rights in the service, its analytics, schemas and documentation. These Terms grant a limited,
      revocable, non-exclusive right to query the service and use the returned data within your product, subject to
      section 04. Responses are derived analytics, not a market-data feed: no licence to underlying third-party market data is
      granted. Raw market data originates from third-party sources and remains subject to their terms.</p>
    </section>

    <section id="en-suspension">
      <h2><span class="num">10</span>Suspension</h2>
      <p>We may throttle, suspend or terminate access that violates these Terms, threatens service stability, or must be
      stopped at the request of a third-party data provider or under applicable law.</p>
    </section>

    <section id="en-changes">
      <h2><span class="num">11</span>Changes</h2>
      <p>These Terms take effect on 2026-10-07. We announce changes by updating the date above; continued use after a change
      means you accept the updated Terms. Previous versions are available in the
      <a href="https://github.com/wnsod/oneqaz/commits/main/terms.html" target="_blank" rel="noopener">public change history</a>.
      These Terms replace the Terms of Use dated 2026-07-08 previously published at <code>api.oneqaz.com/terms</code>.</p>
    </section>

    <section id="en-law">
      <h2><span class="num">12</span>Governing law</h2>
      <p>These Terms are governed by the laws of the Republic of Korea, and disputes arising from them are subject to the
      jurisdiction of the courts of the Republic of Korea.</p>
    </section>

    <section id="en-contact">
      <h2><span class="num">13</span>Contact</h2>
      <dl class="fields">
        <div class="field"><dt>Contact</dt><dd><a href="mailto:contact@oneqaz.com">contact@oneqaz.com</a></dd></div>
      </dl>
    </section>
  </div>

  <p class="doc-foot">These Terms are published identically at <a href="https://oneqaz.com/terms">oneqaz.com/terms</a>
  and <a href="https://api.oneqaz.com/terms">api.oneqaz.com/terms</a>.
  See also the <a href="https://oneqaz.com/privacy">Privacy Policy</a>.</p>
</article>

<!-- ═══════════════════════════ KO ═══════════════════════════ -->
<article data-l="ko" lang="ko">
  <div class="section-label">법적 고지</div>
  <h1 class="section-title">이용약관</h1>
  <p class="section-desc">OneQAZ 트레이딩 인텔리전스 — MCP 서버 및 API</p>
  <div class="doc-meta">최종 수정 2026-10-07 · 시행 2026-10-07</div>

  <div class="toc">
    <a href="#ko-service">서비스</a><a href="#ko-advice">투자자문 아님</a><a href="#ko-clients">AI 클라이언트</a>
    <a href="#ko-use">이용 조건</a><a href="#ko-access">키·호출 제한</a><a href="#ko-data">기록 데이터</a>
    <a href="#ko-availability">가용성</a><a href="#ko-ledger">원장 앵커</a><a href="#ko-ip">지식재산</a>
    <a href="#ko-suspension">이용 제한</a><a href="#ko-changes">변경</a><a href="#ko-law">준거법</a>
    <a href="#ko-contact">연락처</a>
  </div>

  <p class="lead"><code>api.oneqaz.com</code>에 직접 또는 이용자를 대신하는 AI 클라이언트를 통해 연결하면 본 약관에 동의한
  것으로 봅니다. OneQAZ에 연결하는 AI 클라이언트나 연동 서비스를 운영하는 경우, 해당 클라이언트를 대표하여 본 약관에
  동의한 것으로 봅니다.</p>

  <div class="note"><strong>투자 권유가 아닙니다.</strong> OneQAZ의 결과물은 페이퍼 트레이딩 기반 리서치 신호이며, 매수·매도를
  권유하지 않습니다. OneQAZ는 매매를 실행하거나 자금을 보관하거나 금융 거래를 받지 않습니다.</div>

  <div class="policy">
    <section id="ko-service">
      <h2><span class="num">01</span>서비스의 성격</h2>
      <p>OneQAZ는 MCP(Model Context Protocol)와 HTTP API를 통해 AI 클라이언트에 라이브 시장 정보(암호화폐·국내주식·미국주식)와
      글로벌 매크로 맥락을 제공하는 <strong>리서치 정보 서비스</strong>입니다.</p>
      <p>본 서비스는 계정 없이 이용할 수 있습니다. 이용은 개별 최종 사용자가 아니라 연결하는 클라이언트(AI 플랫폼·개발자·기관)
      또는 API 키를 사용한 경우 해당 키 단위로 귀속됩니다.</p>
    </section>

    <section id="ko-advice">
      <h2><span class="num">02</span>투자자문이 아님</h2>
      <p>본 약관에서 가장 중요한 조항입니다.</p>
      <ul>
        <li>OneQAZ의 결과물은 <strong>리서치 신호와 관측된 시장 상태</strong>이며, 어떤 자산의 매수·매도·보유 권유도 아닙니다.</li>
        <li>성과 수치는 <strong>페이퍼 트레이딩</strong>(실제 자금 없는 모의매매) 결과이며, 과거 모의 성과가 실제 수익을 보장하지 않습니다.</li>
        <li>OneQAZ는 투자자문업자·투자중개업자·금융기관이 <strong>아니며</strong>, 개인 맞춤형 투자자문을 제공하지 않습니다. 서비스 이용으로 자문 관계나 신탁 관계가 성립하지 않습니다.</li>
        <li>응답에는 <code>disclaimer</code>, <code>is_investment_advice=false</code>, <code>data_classification=research_information_only</code>가 포함됩니다.</li>
        <li><strong>모든 거래 및 재무 결정의 책임은 이용자 본인에게 있습니다.</strong></li>
      </ul>
    </section>

    <section id="ko-clients">
      <h2><span class="num">03</span>AI 클라이언트의 의무</h2>
      <p>OneQAZ 데이터를 사람에게 전달하는 AI 클라이언트나 서비스를 운영하는 경우 다음을 지켜야 합니다.</p>
      <ul>
        <li>OneQAZ 데이터를 사람에게 인용할 때 <code>disclaimer</code>를 함께 표시할 것.</li>
        <li>OneQAZ 결과물을 개인 맞춤형 투자자문이나 매수·매도 권유로 제시하지 말 것.</li>
        <li>성과 수치에서 페이퍼 트레이딩 맥락을 제거하지 말 것.</li>
        <li>OneQAZ가 하지 않은 권유를 OneQAZ 출처로 표시하지 말 것.</li>
        <li>데이터를 전달할 때 <code>disclaimer</code>, <code>data_classification</code>, 출처(<code>_source</code>) 필드를 제거하거나 바꾸지 말 것.</li>
      </ul>
    </section>

    <section id="ko-use">
      <h2><span class="num">04</span>이용 조건</h2>
      <p>리서치·분석·제품 연동 목적의 조회는 허용됩니다. 다음 행위는 <strong>금지</strong>됩니다.</p>
      <ul>
        <li>호출 제한을 우회하거나, 여러 신원·키를 이용해 추가 쿼터를 얻는 행위.</li>
        <li>응답의 전부 또는 상당 부분을 데이터 상품·피드로 재배포·재판매하거나, 데이터셋을 대량 수집하는 행위.</li>
        <li>응답으로부터 원천 시장 데이터(가격·거래량·호가 등)를 재구성하는 행위.</li>
        <li>비공개 엔드포인트, 다른 클라이언트의 데이터, 기반 인프라에 접근을 시도하는 행위.</li>
        <li>요청을 끝내 완료하지 않는 고빈도 자동 폴링 등 다른 클라이언트의 가용성을 저해하는 이용.</li>
      </ul>
      <p>이용자는 자신의 이용에 적용되는 법령과 원천 데이터 출처의 약관을 준수할 책임이 있습니다.</p>
    </section>

    <section id="ko-access">
      <h2><span class="num">05</span>접근, API 키 및 호출 제한</h2>
      <dl class="fields">
        <div class="field"><dt>Free — 키 없음</dt><dd>하루 1,500회 · 분당 60회</dd></div>
        <div class="field"><dt>Pro — API 키</dt><dd>하루 50,000회 · 분당 200회</dd></div>
      </dl>
      <ul>
        <li>키 없이 보낸 요청은 Free 등급으로 처리됩니다. <strong>유효하지 않거나 비활성·만료된 키로 보낸 요청은 거부</strong>되며
        HTTP 401과 <code>WWW-Authenticate</code> 헤더가 반환됩니다(무료 등급으로 조용히 낮춰 처리하지 않습니다).</li>
        <li>제한을 초과하면 HTTP 429와 <code>Retry-After</code> 헤더가 반환됩니다. 현재 한도는 <code>X-RateLimit-*</code> 응답 헤더로 안내됩니다.</li>
        <li>API 키는 요청자에게 발급되며 비밀로 유지해야 하고, 공유·재판매할 수 없습니다. 오용되거나 노출된 키는 폐기할 수 있습니다.</li>
        <li>Free 등급은 무료입니다. 유료(Pro) 이용이나 사용량 기반 과금 조건은 시행 전에 공개하며, 미납 시 Pro 이용이 중지될 수 있습니다.</li>
        <li>등급과 한도는 변경될 수 있으며, 중대한 변경은 시행 전에 알립니다.</li>
      </ul>
    </section>

    <section id="ko-data">
      <h2><span class="num">06</span>기록하는 데이터</h2>
      <p>서비스 운영·보안·개선에 필요한 최소한만 기록합니다 — 요청 메타데이터, 구조화된 도구 파라미터의 허용 목록 요약,
      <code>search</code> 검색어(최대 60자), API 키의 단방향 지문(키 자체는 저장하지 않음). 기록 항목·목적·보유 기간은
      본 약관의 일부인 <a href="https://oneqaz.com/privacy">개인정보 처리방침</a>에 따릅니다.</p>
    </section>

    <section id="ko-availability">
      <h2><span class="num">07</span>가용성 및 정확성</h2>
      <p>본 서비스는 <strong>있는 그대로(as is)</strong> 제공되며 어떠한 보증도 하지 않습니다.</p>
      <ul>
        <li>가동률·응답 속도·특정 도구나 필드의 존속을 보장하지 않습니다.</li>
        <li>시장 데이터는 지연·누락·정정·중단될 수 있습니다.</li>
        <li>도구·스키마·응답 형태는 변경될 수 있으며, 호환성을 깨는 변경은 가능한 범위에서 미리 알립니다.</li>
        <li>법령이 허용하는 범위에서, 서비스 이용 또는 이용 불가로 인한 손실에 대해 OneQAZ는 책임지지 않습니다.</li>
      </ul>
    </section>

    <section id="ko-ledger">
      <h2><span class="num">08</span>예측 원장과 공개 앵커</h2>
      <p>예측은 채점 전에 기록되며, 예측 원장은 매일 해시체인으로 연결됩니다. 체인 해시는 OpenTimestamps 증명과 함께
      <a href="https://blog.oneqaz.com/anchors/" target="_blank" rel="noopener">blog.oneqaz.com/anchors</a>에 주기적으로 공개되어,
      누구나 공개된 기록이 사후에 수정되지 않았음을 확인할 수 있습니다. 앵커가 증명하는 것은 <strong>공개 이후의 불변성</strong>이며,
      신호의 정확성이나 미래 성과가 아닙니다.</p>
    </section>

    <section id="ko-ip">
      <h2><span class="num">09</span>지식재산</h2>
      <p>서비스·분석 결과물·스키마·문서에 대한 권리는 OneQAZ에 있습니다. 본 약관은 제4조 범위 내에서 서비스를 조회하고
      반환 데이터를 귀하의 제품에 사용할 수 있는 제한적·철회 가능·비독점적 권리를 부여합니다. 응답은 파생 분석 결과이며 시장
      데이터 피드가 아니므로, 원천 제3자 시장 데이터에 대한 이용 허락은 부여되지 않습니다. 원천 시장 데이터는 제3자
      출처에서 비롯되며 해당 출처의 약관이 함께 적용됩니다.</p>
    </section>

    <section id="ko-suspension">
      <h2><span class="num">10</span>이용 제한</h2>
      <p>본 약관 위반, 서비스 안정성 위협, 제3자 데이터 제공자의 요구 또는 관계 법령에 따라 접근을 제한·중단·해지할 수 있습니다.</p>
    </section>

    <section id="ko-changes">
      <h2><span class="num">11</span>약관 변경</h2>
      <p>본 약관은 2026년 10월 7일부터 시행합니다. 변경 사항은 상단의 날짜를 갱신하여 알리며,
      변경 후 계속 이용하면 변경된 약관에 동의한 것으로 봅니다. 이전 약관은
      <a href="https://github.com/wnsod/oneqaz/commits/main/terms.html" target="_blank" rel="noopener">공개 변경 이력</a>에서 확인할 수 있습니다.
      본 약관은 <code>api.oneqaz.com/terms</code>에 게시돼 있던 2026-07-08 이용약관을 대체합니다.</p>
    </section>

    <section id="ko-law">
      <h2><span class="num">12</span>준거법 및 관할</h2>
      <p>본 약관은 대한민국 법률에 따르며, 본 약관과 관련된 분쟁은 대한민국 법원의 관할로 합니다.</p>
    </section>

    <section id="ko-contact">
      <h2><span class="num">13</span>연락처</h2>
      <dl class="fields">
        <div class="field"><dt>연락처</dt><dd><a href="mailto:contact@oneqaz.com">contact@oneqaz.com</a></dd></div>
      </dl>
    </section>
  </div>

  <p class="doc-foot">본 약관은 <a href="https://oneqaz.com/terms">oneqaz.com/terms</a> 와
  <a href="https://api.oneqaz.com/terms">api.oneqaz.com/terms</a> 에 동일하게 게시됩니다.
  <a href="https://oneqaz.com/privacy">개인정보 처리방침</a>도 함께 확인하세요.</p>
</article>

</div>
</main>

<footer>
  <div class="container">
    <div class="footer-links">
      <a href="https://github.com/wnsod/oneqaz-trading-mcp" target="_blank" rel="noopener">GitHub</a>
      <a href="https://pypi.org/project/oneqaz-trading-mcp/" target="_blank" rel="noopener">PyPI</a>
      <a href="https://blog.oneqaz.com/en/rss.xml" target="_blank" rel="noopener" data-blog-path="rss.xml">RSS</a>
      <a href="https://oneqaz.com/privacy">Privacy</a>
      <a href="https://oneqaz.com/terms" class="current">Terms</a>
      <a href="mailto:contact@oneqaz.com">Contact</a>
    </div>
    <div class="footer-copy">&copy; 2026 OneQAZ. All rights reserved.</div>
  </div>
</footer>
<!--/email_off-->

<script>
var BLOG_BASE = 'https://blog.oneqaz.com/';
var TITLES = { en: 'Terms of Service — OneQAZ', ko: '이용약관 — OneQAZ' };

function setLang(lang) {
  var root = document.documentElement;
  root.setAttribute('data-lang', lang);
  root.lang = lang;
  try { localStorage.setItem('oqz-lang', lang); } catch (e) {}
  document.getElementById('langEn').classList.toggle('active', lang === 'en');
  document.getElementById('langKo').classList.toggle('active', lang === 'ko');
  document.title = TITLES[lang];
  // Blog links follow the site language (EN → blog /en/ mirror) — same rule as the home page.
  var links = document.querySelectorAll('a[data-blog-path]');
  for (var i = 0; i < links.length; i++) {
    links[i].href = BLOG_BASE + (lang === 'en' ? 'en/' : '') + links[i].getAttribute('data-blog-path');
  }
}
setLang(document.documentElement.getAttribute('data-lang') === 'ko' ? 'ko' : 'en');

try {
  window.matchMedia('(prefers-color-scheme: light)').addEventListener('change', function (e) {
    document.documentElement.setAttribute('data-theme', e.matches ? 'light' : 'dark');
  });
} catch (e) {}

var NAV = document.querySelector('nav');
function onScroll() {
  var h = document.documentElement;
  var max = h.scrollHeight - h.clientHeight;
  document.getElementById('progressFill').style.width = (max > 0 ? h.scrollTop / max * 100 : 0) + '%';
  NAV.classList.toggle('scrolled', h.scrollTop > 24);
}
window.addEventListener('scroll', onScroll);
onScroll();
</script>
</body>
</html>"""
