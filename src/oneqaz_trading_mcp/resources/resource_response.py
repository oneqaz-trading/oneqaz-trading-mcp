import json
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional


# ── External-facing constants ──
# Single source of truth for the disclaimer that goes into every tool/error response.
# Referenced by server.py instructions, discovery_manifest, README, and every wrap.
DISCLAIMER_TEXT = (
    "Information only, not investment advice. OneQAZ outputs are paper-trading "
    "research signals, not recommendations to buy or sell. Past simulated "
    "performance does not predict future real-money returns."
)


def _serving_source() -> dict:
    """서빙 출처 메타. origin=home|unknown, data_source=postgres_live|unknown.

    [2026-07-06] AWS EC2 미러 배포 제거 완료 — api.oneqaz.com 은 홈 PG 직결
    단일 경로다. 과거의 aws_mirror/sqlite_mirror round-robin 분기 삭제.
    외부 AI 클라이언트가 이 필드로 서빙 출처를 식별하므로 필드 자체는 유지.
    MCP_SERVING_ORIGIN env override, 아니면 DB_BACKEND 추론.
    """
    import os as _os
    backend = _os.environ.get("DB_BACKEND", "").strip().lower()
    origin = _os.environ.get("MCP_SERVING_ORIGIN", "").strip().lower()
    if not origin:
        origin = "home" if backend in ("postgres", "postgresql", "pg") else "unknown"
    data_source = (
        "postgres_live" if backend in ("postgres", "postgresql", "pg") else "unknown"
    )
    return {"origin": origin, "data_source": data_source}


def _utc_now_iso() -> str:
    """RFC3339 / ISO8601 UTC timestamp with 'Z' suffix."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _new_request_id() -> str:
    """Per-response correlation id. Used for trace/log matching."""
    return uuid.uuid4().hex

try:
    from oneqaz_trading_mcp.api.insight.schema import (
        ExplanationPayloadV1,
        ExplanationSummary,
        ProvenanceV1,
        default_provenance,
        explanation_to_llm_text,
    )
except ImportError:
    ExplanationPayloadV1 = None  # type: ignore[assignment]
    ExplanationSummary = None  # type: ignore[assignment]
    ProvenanceV1 = None  # type: ignore[assignment]

    def default_provenance(**kwargs):  # type: ignore[misc]
        return {"schema_version": "1.0", **kwargs}

    def explanation_to_llm_text(payload: Any) -> str:  # type: ignore[misc]
        if isinstance(payload, dict):
            return str(payload.get("_llm_summary") or "")
        return ""


def _json_default(value: Any) -> str | float:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    return str(value)


def to_resource_text(data: Any) -> str:
    """FastMCP 3.x resource 응답은 text/blob이어야 하므로 JSON 문자열로 직렬화한다."""
    return json.dumps(data, ensure_ascii=False, default=_json_default)


# ── AX: Actionable Error Response ──


class MCPErrorCode(str, Enum):
    """MCP 에러 코드 표준 정의 (외부 도입자 노출용 안정 식별자)."""
    SIGNAL_STALE = "signal_stale"
    REGIME_UNAVAILABLE = "regime_unavailable"
    SYMBOL_NOT_FOUND = "symbol_not_found"
    DATA_COLLECTION_LAG = "data_collection_lag"
    DB_NOT_FOUND = "db_not_found"
    TABLE_NOT_FOUND = "table_not_found"
    TIMEOUT = "timeout"
    MARKET_NOT_FOUND = "market_not_found"
    NO_DATA = "no_data"
    INTERNAL_ERROR = "internal_error"
    # ── Auth / quota ──
    RATE_LIMITED = "rate_limited"
    TIER_BLOCKED = "tier_blocked"
    AUTH_FAILED = "auth_failed"
    # ── Input validation ──
    INVALID_MARKET = "invalid_market"
    INVALID_SYMBOL = "invalid_symbol"
    INVALID_INTERVAL = "invalid_interval"
    MISSING_REQUIRED_FIELD = "missing_required_field"
    # ── Protocol ──
    MCP_HANDSHAKE_FAILED = "mcp_handshake_failed"


# Default retry policy per error code (whether client should auto-retry).
_RETRYABLE_BY_CODE: dict[str, bool] = {
    MCPErrorCode.SIGNAL_STALE.value: True,
    MCPErrorCode.REGIME_UNAVAILABLE.value: True,
    MCPErrorCode.DATA_COLLECTION_LAG.value: True,
    MCPErrorCode.TIMEOUT.value: True,
    MCPErrorCode.NO_DATA.value: False,
    MCPErrorCode.INTERNAL_ERROR.value: True,
    MCPErrorCode.RATE_LIMITED.value: True,
    MCPErrorCode.TIER_BLOCKED.value: False,
    MCPErrorCode.AUTH_FAILED.value: False,
    MCPErrorCode.SYMBOL_NOT_FOUND.value: False,
    MCPErrorCode.DB_NOT_FOUND.value: False,
    MCPErrorCode.TABLE_NOT_FOUND.value: False,
    MCPErrorCode.MARKET_NOT_FOUND.value: False,
    MCPErrorCode.INVALID_MARKET.value: False,
    MCPErrorCode.INVALID_SYMBOL.value: False,
    MCPErrorCode.INVALID_INTERVAL.value: False,
    MCPErrorCode.MISSING_REQUIRED_FIELD.value: False,
    MCPErrorCode.MCP_HANDSHAKE_FAILED.value: True,
}


class MCPErrorAction(str, Enum):
    """에러 발생 시 AI에게 권장하는 대응 액션."""
    RETRY = "retry_after_seconds"
    FALLBACK = "use_fallback"
    CHECK = "check_availability"


def _record_envelope_obs(error_code: Optional[str] = None, payload_bytes: Optional[int] = None) -> None:
    """[2026-07-08] envelope 수준 관측을 현재 HTTP request scope 에 심는다.

    server.py 미들웨어가 응답 후 scope['state']['mcp_envelope_obs'] 를 회수해
    mcp_analytics 에 기록 — tool 내부 에러가 HTTP 200 으로 나가 success=true 로만
    남던 관측 사각지대 해소 + 응답 크기(과금 단위 근거) 축적.
    HTTP 컨텍스트 밖(내부 직접 호출/테스트)에서는 조용히 no-op (관측 전용이라 허용).
    """
    try:
        from fastmcp.server.dependencies import get_http_request
        req = get_http_request()
        if req is None:
            return
        obs = req.scope.setdefault("state", {}).setdefault("mcp_envelope_obs", {})
        if error_code:
            obs["error_code"] = str(error_code)[:60]
        if payload_bytes is not None:
            obs["payload_bytes"] = int(payload_bytes)
    except Exception:
        # 관측 실패가 응답을 깨면 안 됨 (명시적 사유: observability-only 경로)
        pass


def mcp_error(
    code: MCPErrorCode,
    reason: str,
    *,
    action: MCPErrorAction = MCPErrorAction.CHECK,
    action_value: str = "",
    fallback_tool: Optional[str] = None,
    fallback_note: Optional[str] = None,
    **extra_context,
) -> dict:
    """Actionable 에러 응답 생성. 기존 {"error": ...} 패턴을 대체한다.

    AI가 에러를 받았을 때 즉시 대응 가능하도록:
    - error_code: 에러 분류
    - reason: 사람이 읽을 수 있는 상세 이유
    - action: 권장 대응 (retry / fallback / check)
    - fallback_tool: 대안 도구/리소스
    """
    d: dict[str, Any] = {
        "error": True,
        "error_code": code.value,
        "reason": reason,
        "action": action.value,
        "action_value": action_value,
        "retryable": _RETRYABLE_BY_CODE.get(code.value, False),
        "timestamp": _utc_now_iso(),
        "request_id": _new_request_id(),
        "disclaimer": DISCLAIMER_TEXT,
        # [2026-08-18 위생] envelope 보증의 에러 경로 결손 수리 — 정상 응답에만
        # 실리던 분류 필드가 에러에선 빠져 있었다 (신뢰 평가 패널 적발).
        "is_investment_advice": False,
        "data_classification": "research_information_only",
        "_source": _serving_source(),
    }
    if fallback_tool:
        d["fallback_tool"] = fallback_tool
    if fallback_note:
        d["fallback_note"] = fallback_note
    if extra_context:
        d.update(extra_context)
    _record_envelope_obs(error_code=code.value)
    return d


# ── AX: AI Summary Wrapper ──


def _value_signals_static(resource_type: str, ttl: int) -> dict:
    """모든 응답에 박는 정적 가치 신호 — AI 가 OneQAZ 사용·추천·결제 판단에 쓸 메타.

    호출자별 동적 잔여량은 X-RateLimit-* HTTP 헤더로 전송 (server.py).
    여기는 시스템 차원 정적 정보만:
    - free 티어로 어디까지 되는지
    - pro 가 unlock 하는 것
    - 데이터 freshness 정책
    - 결제·키 발급 안내 (AI 가 user 에게 전달할 수 있는 URL)
    """
    return {
        "tier_default": "free",
        "tier_default_limits": {"daily": 1500, "minute": 60},
        "data_freshness_seconds": ttl,
        "what_pro_unlocks": "33x daily quota (50K), 3.3x burst (200/min) — same tools, higher volume",
        "pricing_url": "https://api.oneqaz.com/pricing",
        "key_signup_url": "https://api.oneqaz.com/keys",
        "self_correcting": True,
        # [2026-07-20 RCA] 1.1 — get_prediction_accuracy skill 필드군 + edge v2 +
        # get_signal_calibration 신설 (변경 로그는 get_prediction_accuracy meta)
        "schema_version": "1.1",
    }


def wrap_tool_response(
    data: dict,
    tool_type: str,
    summary_fn: Callable[[dict], str],
    user_summary_fn: Optional[Callable[[dict], str]] = None,
    next_actions_fn: Optional[Callable[[dict], list]] = None,
    followup_questions_fn: Optional[Callable[[dict], list]] = None,
    narrative_context: Optional[str] = None,
) -> dict:
    """tools/* 응답에 이중 청중 wrapper 적용. resources용 wrap_with_ai_summary 의 별칭.

    에러 응답({"error": True, ...})은 wrapping 하지 않고 통과 — actionable error
    필드(error_code, action, fallback_tool 등)를 그대로 노출하기 위함.

    next_actions_fn: optional. 응답 데이터를 받아 [{intent, tool, args, rationale,
    priority}, ...] 리스트를 반환. 박히면 _next_actions 키로 응답에 추가됨 (Phase 3).

    followup_questions_fn: optional. 응답 데이터를 받아 사용자한테 자연스럽게 던질
    후속 질문 list[str] 반환. 거대 AI 가 답변 끝에 옵션으로 제시 (Phase D).
    예: ["이 종목의 과거 비슷한 상황도 보시겠어요?", "현재 같은 패턴의 다른 종목들도 궁금하세요?"]

    narrative_context: optional. "daily_brief"/"explain_decision"/"prediction_accuracy"
    같은 식별자. 설정되면 mcps.narrative.generate_narrative() 호출해서
    `_market_state_narrative` 필드 박음 (Interpretation Layer, 2026-05-08).
    home (vLLM 가능) 에선 LLM 호출, AWS (vLLM 없음) 에선 templated fallback.
    """
    if isinstance(data, dict) and data.get("error") is True:
        # Error responses still must carry disclaimer/request_id/timestamp.
        # mcp_error() already injects these; this is a safety net for legacy
        # error dicts that bypassed mcp_error().
        if "disclaimer" not in data:
            data["disclaimer"] = DISCLAIMER_TEXT
        if "request_id" not in data:
            data["request_id"] = _new_request_id()
        if "timestamp" not in data:
            data["timestamp"] = _utc_now_iso()
        if "_source" not in data:
            data["_source"] = _serving_source()
        # legacy error dict 가 mcp_error() 를 우회한 경우도 관측은 남긴다
        _record_envelope_obs(error_code=str(data.get("error_code") or "legacy_error"))
        return data
    return wrap_with_ai_summary(
        data, tool_type, summary_fn, user_summary_fn,
        next_actions_fn, followup_questions_fn, narrative_context,
    )


def wrap_with_ai_summary(
    data: dict,
    resource_type: str,
    summary_fn: Callable[[dict], str],
    user_summary_fn: Optional[Callable[[dict], str]] = None,
    next_actions_fn: Optional[Callable[[dict], list]] = None,
    followup_questions_fn: Optional[Callable[[dict], list]] = None,
    narrative_context: Optional[str] = None,
) -> dict:
    """응답을 full_data + ai_summary + summary_for_user + _value_signals 로 래핑.

    이중 청중 정책 (방안 B, 2026-04-27):
    - AI 에이전트 → full_data + _contract + ai_summary + _llm_summary + _value_signals
        AI 가 신뢰 검증 + 의사결정 + 결제 판단에 필요한 raw + provenance + 가치 신호.
    - 인간 사용자 → summary_for_user 1줄
        Claude.ai 같은 AI 가 인간에게 답할 때 그대로 인용 가능한 jargon-free 한국어.

    Fields:
    - full_data: 기존 응답 전체 (하위 호환, AI 검증용 raw)
    - ai_summary: 1줄 AI 요약 (LLM 이 full_data 파싱 없이 핵심 파악)
    - ai_summary_ttl_seconds: 캐시 정책
    - summary_for_user: 인간용 1줄 (한국어, jargon-free)
    - _value_signals: B2AI 결제·추천 판단용 정적 메타 (가격/티어/freshness/upgrade URL)
    - _llm_summary: AI narrative (`ai_summary` 와 다른 더 긴 서술)
    """
    from oneqaz_trading_mcp.config import AI_SUMMARY_TTL
    ttl = AI_SUMMARY_TTL.get(resource_type, 120)
    try:
        ai_summary = summary_fn(data)
    except Exception:
        ai_summary = ""
    if user_summary_fn is not None:
        try:
            user_summary = user_summary_fn(data)
        except Exception:
            user_summary = ai_summary
    else:
        user_summary = ai_summary

    # Phase 3 (2026-05-07): 응답 데이터 기반 조건부 next_actions 추천.
    # 글로벌 dependency_graph 와 달리 이건 실제 response payload 에 따라 분기.
    # 예: get_prediction_accuracy 응답에 약한 카테고리가 있으면 그 카테고리로
    # get_monthly_accuracy_trend 호출을 추천.
    next_actions: list = []
    if next_actions_fn is not None:
        try:
            result = next_actions_fn(data)
            if isinstance(result, list):
                # 최대 3개로 제한, 각 항목 sanitize
                for item in result[:3]:
                    if not isinstance(item, dict):
                        continue
                    cleaned = {
                        "intent": str(item.get("intent", ""))[:120],
                        "tool": str(item.get("tool", ""))[:80],
                        "args": item.get("args") if isinstance(item.get("args"), dict) else {},
                        "rationale": str(item.get("rationale", ""))[:300],
                        "priority": item.get("priority", "normal"),
                    }
                    if cleaned["tool"]:
                        next_actions.append(cleaned)
        except Exception as e:
            # 절대 응답 자체를 깨지 않음
            import logging
            logging.getLogger("MarketMCP").debug("next_actions_fn failed for %s: %s", resource_type, e)

    # Phase D (2026-05-07): 사용자한테 던질 후속 질문.
    # 거대 AI 가 답변 끝에 "다음 질문 옵션" 으로 인용 가능. 클릭 시 다시 OneQAZ 호출.
    followup_questions: list = []
    if followup_questions_fn is not None:
        try:
            result = followup_questions_fn(data)
            if isinstance(result, list):
                # 최대 3개, 각 항목 string 으로 sanitize
                for item in result[:3]:
                    if isinstance(item, str) and item.strip():
                        followup_questions.append(item.strip()[:200])
        except Exception as e:
            import logging
            logging.getLogger("MarketMCP").debug("followup_questions_fn failed for %s: %s", resource_type, e)

    now_iso = _utc_now_iso()
    response = {
        "ai_summary": ai_summary,
        "summary_for_user": user_summary,
        "ai_summary_generated_at": now_iso,
        "ai_summary_ttl_seconds": ttl,
        "full_data": data,
        "_llm_summary": data.get("_llm_summary", ai_summary),
        "_value_signals": _value_signals_static(resource_type, ttl),
        # ── Compliance / observability fields ──
        "disclaimer": DISCLAIMER_TEXT,
        "request_id": _new_request_id(),
        "timestamp": now_iso,
        "is_investment_advice": False,
        "is_real_money": False,
        "data_classification": "research_information_only",
        "_source": _serving_source(),
    }
    if next_actions:
        response["_next_actions"] = next_actions
    if followup_questions:
        response["_followup_questions_for_user"] = followup_questions

    # Interpretation Layer (2026-05-08): vLLM 으로 시장 상태를 한국어 narrative
    # 로 번역. home 에서만 동작, AWS 는 templated fallback.
    # 거대 AI 가 사용자한테 인용할 만한 자연스러운 한국어 문장.
    # [2026-07-08] 비차단 전환 — 동기 vLLM 대기(35s timeout, 매번 콜드 캐시)가
    # daily_brief p95 79.7s 의 주범이었다. 응답 경로는 절대 vLLM 을 기다리지 않고
    # stale-while-revalidate 로 최신본을 서빙한다 (mcps/narrative.py 참조).
    if narrative_context:
        try:
            from oneqaz_trading_mcp.narrative import generate_narrative_nonblocking
            narr = generate_narrative_nonblocking(narrative_context, data)
            # bilingual: {ko: {...}, en: {...}}. ko 또는 en 중 하나라도 valid 하면 박음.
            if narr and isinstance(narr, dict):
                ko_ok = isinstance(narr.get("ko"), dict) and narr["ko"].get("headline")
                en_ok = isinstance(narr.get("en"), dict) and narr["en"].get("headline")
                if ko_ok or en_ok:
                    response["_market_state_narrative"] = narr
        except Exception as e:
            import logging
            logging.getLogger("MarketMCP").debug(
                "narrative gen failed for %s: %s", narrative_context, e
            )

    # [2026-07-08] 응답 크기 관측 (과금 단위·가치 측정 근거). 직렬화 실패 시 생략.
    try:
        import json as _json
        _record_envelope_obs(payload_bytes=len(_json.dumps(response, ensure_ascii=False, default=str)))
    except Exception:
        pass

    return response


# ── Explanation Contract Builder ──


def build_resource_explanation(
    *,
    market: str,
    entity_type: str,
    explanation_type: str,
    as_of_time: str | None = None,
    symbol: str | None = None,
    headline: str,
    why_text: str,
    bullet_points: list[str] | None = None,
    market_context_refs: dict[str, Any] | None = None,
    confidence: float = 0.5,
    note: str | None = None,
) -> dict[str, Any]:
    # PG backend가 datetime 객체를 돌려주므로 ExplanationPayloadV1(str) 계약에 맞춰 정규화
    if hasattr(as_of_time, "isoformat"):
        as_of_time = as_of_time.isoformat()
    as_of_time = as_of_time or datetime.utcnow().isoformat()
    bullet_points = bullet_points or []
    market_context_refs = market_context_refs or {}

    if ExplanationPayloadV1 is None or ExplanationSummary is None:
        return {
            "schema_version": "1.0",
            "entity": {
                "market": market,
                "symbol": symbol,
                "entity_type": entity_type,
            },
            "as_of_time": as_of_time,
            "explanation_type": explanation_type,
            "assessment": {
                "driver": {"primary": "unknown", "secondary": None, "confidence": confidence},
                "time_phase": "event",
                "relation_status": "watching",
                "movement_classification": "unconfirmed",
                "news_timing": "no_news_detected",
                "lead_lag": None,
                "explanation_status": "candidate",
            },
            "evidence": {
                "source_event_ids": [],
                "related_relation_ids": [],
                "signal_keys": [],
                "market_context_refs": market_context_refs,
                "state_refs": {},
                "memory_refs": {},
                "confidence": confidence,
            },
            "summary": {
                "headline": headline,
                "why_text": why_text,
                "short_reason": bullet_points[0] if bullet_points else None,
                "bullet_points": bullet_points,
                "market_details": {},
                "uncertainty_flags": [],
                "risk_assessment": {},
            },
            "provenance": default_provenance(note=note),
        }

    payload = ExplanationPayloadV1(
        schema_version="1.0",
        entity={
            "market": market,
            "symbol": symbol,
            "entity_type": entity_type,
        },
        as_of_time=as_of_time,
        explanation_type=explanation_type,
        assessment={
            "driver": {
                "primary": "unknown",
                "secondary": None,
                "confidence": confidence,
            },
            "time_phase": "event",
            "relation_status": "watching",
            "movement_classification": "unconfirmed",
            "news_timing": "no_news_detected",
            "lead_lag": None,
            "explanation_status": "candidate",
        },
        evidence={
            "source_event_ids": [],
            "related_relation_ids": [],
            "signal_keys": [],
            "market_context_refs": market_context_refs,
            "state_refs": {},
            "memory_refs": {},
            "confidence": confidence,
        },
        summary=ExplanationSummary(
            headline=headline,
            why_text=why_text,
            short_reason=bullet_points[0] if bullet_points else None,
            bullet_points=bullet_points,
            market_details={},
            uncertainty_flags=[],
            risk_assessment={},
        ),
        provenance=default_provenance(note=note),
    )
    return payload.model_dump(mode="json")


def with_explanation_contract(
    data: dict[str, Any],
    *,
    resource_type: str,
    explanation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    enriched = dict(data)
    if explanation:
        enriched["_contract"] = {
            "schema_version": "1.0",
            "resource_type": resource_type,
            "explanation": explanation,
        }
        if not enriched.get("_llm_summary"):
            enriched["_llm_summary"] = explanation_to_llm_text(explanation)
    else:
        enriched["_contract"] = {
            "schema_version": "1.0",
            "resource_type": resource_type,
        }
    return enriched
