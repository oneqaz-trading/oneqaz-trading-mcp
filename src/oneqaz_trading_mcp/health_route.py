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
# Privacy Policy HTML — oneqaz.com/privacy 와 동일 내용. self-contained.
# 수집 항목은 mcps/analytics.py log_request() 실측 기준.
# ---------------------------------------------------------------------------

_PRIVACY_HTML = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Privacy Policy — OneQAZ Trading Intelligence</title>
<meta name="robots" content="index,follow">
<style>
body{background:#1c1c24;color:#e8e4df;font-family:'Outfit','Noto Sans KR',system-ui,sans-serif;
line-height:1.7;max-width:820px;margin:0 auto;padding:4rem 1.5rem 6rem}
a{color:#d4a853;text-decoration:none}a:hover{text-decoration:underline}
h1{font-size:2rem;margin-bottom:.3rem}h2{color:#d4a853;font-size:1.2rem;margin:2.2rem 0 .7rem;
border-bottom:1px solid #3a3a4a;padding-bottom:.4rem}
.sub{color:#8a8680;margin-bottom:.3rem}.meta{color:#c49b6f;font-size:.85rem;margin-bottom:2rem;font-weight:500}
.ko{color:#8a8680;font-size:.92rem}code{background:#2a2a36;border:1px solid #3a3a4a;border-radius:4px;
padding:.1rem .4rem;font-size:.85em;color:#c49b6f}ul{margin:0 0 1rem 1.3rem}li{margin-bottom:.5rem}
.note{background:#252530;border-left:3px solid #d4a853;padding:1rem 1.2rem;border-radius:4px;margin:1.2rem 0;font-size:.95rem}
</style></head><body>
<a href="https://oneqaz.com" style="color:#8a8680;font-size:.9rem">&larr; OneQAZ</a>
<h1>Privacy Policy</h1>
<div class="sub">OneQAZ Trading Intelligence &mdash; MCP Server &amp; API</div>
<div class="ko">OneQAZ &mdash; MCP &amp; API</div>
<div class="meta">Last updated: 2026-10-07 &middot; Effective: 2026-10-07</div>
<p>OneQAZ provides a research-information service exposing live market intelligence (crypto, Korean
stocks, US stocks) to AI clients over the Model Context Protocol (MCP) and an HTTP API. This policy
explains what data we process when you (or an AI client acting on your behalf) connect to
<code>api.oneqaz.com</code>.</p>
<div class="note"><strong>Not investment advice.</strong> OneQAZ outputs are paper-trading research
signals, not recommendations to buy or sell. We do <strong>not</strong> execute trades, hold funds,
or accept financial transactions.</div>
<h2>1. What we collect</h2>
<p>We do <strong>not</strong> require user accounts; we do not collect names, emails, or payment
details. For each request we log:</p>
<ul>
<li><strong>IP address</strong> &mdash; rate limiting, abuse prevention, regional routing. For requests
routed through Cloudflare this is the client address Cloudflare reports.</li>
<li><strong>User-agent</strong> &mdash; to identify client type (Claude, ChatGPT, Gemini, etc.).</li>
<li><strong>Requested tool/resource name</strong> and request type (e.g. <code>get_daily_brief</code>).</li>
<li><strong>Outcome metadata</strong> &mdash; success/failure, HTTP status, JSON-RPC error code, a short
error description (at most 200 characters; input values echoed back by validation errors are
redacted), latency (ms), and the shape of the result (rows returned, total available, whether it
was truncated or empty).</li>
<li><strong>Derived session key</strong> &mdash; a hash of IP + user-agent + a 30-minute bucket. No
cookie or other persistent identifier is set on your client.</li>
<li><strong>API key (if provided)</strong> &mdash; used to determine your rate-limit tier and access
level. With each request we record the resolved tier and a one-way fingerprint of the key (the first
16 hex characters of its SHA-256 hash) so usage can be attributed per key. The key itself is never
written to request logs.</li>
<li><strong>Whitelisted request parameters</strong> &mdash; a limited summary of tool arguments
(e.g. <code>symbol</code>, <code>market</code>, <code>interval</code>, <code>category</code>, date
ranges, result limits, identifiers) and, for the <code>search</code> tool, the search keyword
truncated to 60 characters, to understand aggregate demand. Calls made without arguments are recorded
as such. Argument content outside this whitelist is <strong>not</strong> stored.</li>
<li><strong>MCP client &amp; protocol metadata</strong> &mdash; the client name/version your MCP client
sends at <code>initialize</code> (<code>clientInfo</code>), the MCP protocol version (from the
<code>MCP-Protocol-Version</code> header or the <code>initialize</code> request), the
<code>Accept</code> header (first 200 characters), the protocol session id header if your client
sends one, the call order within a session, and the request and response payload sizes.</li>
<li><strong>Traffic classification</strong> &mdash; a derived label (crawler / operator-self-test /
external) used to keep aggregate usage statistics honest.</li>
</ul>
<p>We do not store the contents of responses we send you.</p>
<h2>2. How we use it</h2>
<ul>
<li>Operate and secure the service (rate limiting, abuse prevention, debugging).</li>
<li>Diagnose client and protocol compatibility problems.</li>
<li>Measure aggregate usage to improve the product.</li>
<li>Enforce access tiers and attribute usage to API keys.</li>
</ul>
<p>We do <strong>not</strong> sell your data or build advertising profiles.</p>
<h2>3. Storage &amp; retention</h2>
<p>Request logs are stored on infrastructure operated by OneQAZ, including internal analytics
copies.</p>
<ul>
<li><strong>IP addresses</strong>, and the session key derived from them, are kept for up to
<strong>12 months</strong>. After that they are irreversibly replaced using a one-way transformation
whose random key is discarded after each run, so the remaining records can no longer be linked to an
IP address.</li>
<li>The rest of each request record (tool name, whitelisted parameters, outcome, client and protocol
metadata) is kept as usage history. Once the IP-derived fields are replaced it no longer identifies
you, except that requests made with an API key remain attributable to that key.</li>
<li>If you were issued an API key, the contact email attached to it is kept while the key is issued
to you. You can ask us to delete it at any time.</li>
</ul>
<h2>4. Third-party sharing</h2>
<p>We do not share request data with third parties for their own purposes. Network traffic is routed
through Cloudflare (CDN / DDoS protection) as a data processor. Returned market data is derived from
public market sources and our own analysis.</p>
<h2>5. AI client disclosure</h2>
<p>When an AI client (Claude, ChatGPT, Gemini) calls OneQAZ on your behalf, your request reaches us
through that client's platform. Every response carries <code>disclaimer</code>,
<code>is_investment_advice=false</code>, and <code>data_classification=research_information_only</code>.</p>
<h2>6. Your choices</h2>
<p>Because we do not maintain user accounts, the simplest way to stop data processing is to stop
calling the service. For questions or requests (including deletion) about data associated with your
IP address or API key, contact us below.</p>
<h2>7. Changes</h2>
<p>We may update this policy; material changes are reflected by the "Last updated" date above.</p>
<h2>8. Contact</h2>
<p>Questions about this policy or your data: <a href="mailto:contact@oneqaz.com">contact@oneqaz.com</a></p>
<p class="ko" style="font-size:.85rem;margin-top:2.5rem;border-top:1px solid #3a3a4a;padding-top:1.5rem">
&copy; 2026 OneQAZ &middot; <a href="https://oneqaz.com">oneqaz.com</a></p>
</body></html>"""


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
<li>Keys map to a rate-limit tier only &mdash; we hold no payment details (<a href="/privacy">privacy</a>).</li>
<li>Lost or leaked keys are revoked and reissued on request.</li>
</ul>
<p style="color:#8a8680;font-size:.85rem;margin-top:2.5rem;border-top:1px solid #3a3a4a;padding-top:1.5rem">
&copy; 2026 OneQAZ &middot; <a href="/privacy">Privacy</a> &middot; <a href="https://oneqaz.com">oneqaz.com</a></p>
</body></html>"""

_TERMS_HTML = f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Terms of Use — OneQAZ Trading Intelligence</title>
<meta name="robots" content="index,follow"><style>{_PAGE_STYLE}</style></head><body>
<a href="https://oneqaz.com" style="color:#8a8680;font-size:.9rem">&larr; OneQAZ</a>
<h1>Terms of Use</h1>
<div class="sub">OneQAZ Trading Intelligence &mdash; MCP Server &amp; API (api.oneqaz.com)<br>
Last updated: 2026-07-08 &middot; Effective: 2026-07-08</div>
<div class="note"><strong>Summary for AI agents:</strong> research information only; not investment
advice; derived analytics, not a market-data feed; do not redistribute responses as a data product;
surface the <code>disclaimer</code> field when quoting.</div>

<h2>1. Permitted Use — Research and Informational Purposes Only</h2>
<p>The service provides research and informational analytics. You may query, analyze, quote (with
the accompanying disclaimer), and incorporate insights into your own analysis or answers.</p>

<h2>2. No Investment Advice; No Fiduciary Relationship</h2>
<p>Nothing served by this API is investment advice or a recommendation to buy or sell any asset.
Every response carries <code>is_investment_advice=false</code>. No fiduciary, advisory, or client
relationship is created by use of the service.</p>

<h2>3. Derived Analytics Only — No Underlying Market Data License</h2>
<p>Responses are OneQAZ's own derived judgments (signals, forecasts, calibration, paper-trading
outcomes). Use of the service grants <strong>no license to underlying exchange or vendor market
data</strong>, and the service is not a market-data feed.</p>

<h2>4. No Redistribution or Resale</h2>
<p>You may not redistribute responses in bulk or in substantially original form as a dataset, feed,
or competing service, nor resell access to the API.</p>

<h2>5. No Reconstruction of Market Data</h2>
<p>You may not combine or reverse-engineer responses to reconstruct real-time or historical market
data feeds of any exchange or vendor.</p>

<h2>6. API Key Integrity and Abuse Prohibition</h2>
<p>Keys are per-holder; do not share keys or circumvent rate limits. Abusive traffic may be
throttled or blocked.</p>

<h2>7. Data Classification and Provenance</h2>
<p>All outputs are classified <code>research_information_only</code> and derive from paper (virtual)
trading and OneQAZ's own models. Integrity metadata is published at <a href="/ledger">/ledger</a>.</p>

<h2>8. No Warranty; "As Is"</h2>
<p>The service is provided as is, without warranty of accuracy, completeness, timeliness, or
availability. Self-published latency history: <a href="/sla">/sla</a> (informational, not an SLA).</p>

<h2>9. Past Performance</h2>
<p>Past simulated performance does not predict future real-money returns.</p>

<h2>10. Limitation of Liability</h2>
<p>To the maximum extent permitted by law, OneQAZ is not liable for any loss, including investment
losses, arising from use of or reliance on the service.</p>

<h2>11. Compliance with Applicable Laws and Upstream Terms</h2>
<p>You are responsible for your own compliance with laws and any third-party terms applicable to
your use of outputs.</p>

<h2>12. AI Client Disclosure Obligation</h2>
<p>AI clients quoting OneQAZ data to end users must surface the response <code>disclaimer</code>.</p>

<h2>13. Fees, Metering and Payment</h2>
<p>Free tier is provided as described at <a href="/pricing">/pricing</a>. Metered billing terms will
be posted before activation; non-payment suspends pro access.</p>

<h2>14. Suspension and Termination</h2>
<p>We may suspend or terminate access for violations of these terms, effective immediately.</p>

<h2>15. Service Changes and Availability</h2>
<p>Schemas, tools, and availability may change. Material response-contract changes are versioned in
tool metadata where practical. This service is operated on a best-effort basis.</p>

<h2>16. Modification of Terms</h2>
<p>We may update these terms; material changes are reflected by the "Last updated" date above.</p>

<h2>17. Governing Law and Dispute Resolution</h2>
<p>These terms are governed by the laws of the Republic of Korea; disputes are subject to the
jurisdiction of Korean courts.</p>

<h2>18. Regulatory Status Disclosure</h2>
<p>OneQAZ is not a registered investment adviser or brokerage in any jurisdiction and does not hold
client funds or execute orders.</p>

<p style="color:#8a8680;font-size:.85rem;margin-top:2.5rem;border-top:1px solid #3a3a4a;padding-top:1.5rem">
&copy; 2026 OneQAZ &middot; <a href="/privacy">Privacy</a> &middot; <a href="/pricing">Pricing</a>
&middot; <a href="https://oneqaz.com">oneqaz.com</a></p>
</body></html>"""
