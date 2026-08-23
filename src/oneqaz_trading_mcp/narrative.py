# -*- coding: utf-8 -*-
"""
Market State Narrative Helper (bilingual)
=========================================
Phase Interpretation Layer (2026-05-08): "Signal/Context/Evidence" 위에
"Interpretation Layer" 를 얹는 helper. **L2 domain interpretation 만 박음** —
L1 (raw data) 와 L3 (사용자 컨텍스트별 paraphrase) 는 거대 AI 자유.

설계:
    - 한 번의 vLLM 호출에 ko + en 동시 생성 (Qwen3-30B AWQ).
    - JSON schema 강제 — strict response_format.
    - timeout 35s (env VLLM_NARRATIVE_TIMEOUT), fallback templated (양 언어 모두).
    - 60s LRU cache (100 entries).
    - vLLM 부재 자동 감지 (미설정/unreachable 이면 항상 fallback).
    - 응답 형식:
        {
          "ko": {"headline", "evidence": [...], "what_to_watch"},
          "en": {"headline", "evidence": [...], "what_to_watch"}
        }

L2 정체성 (system prompt 에 박음):
    - OneQAZ specialist 역할 — 거대 AI 의 generic reasoning 위에 *"OneQAZ 의
      도메인 해석"* 만 추가.
    - 단정 금지: "오를 것이다" X, "감지된다 / 가능성이 있다 / 진행 중" O.
    - 숫자 → 의미 변환: "RSI 94" → "단기 과매수" / "near-term overbought".
    - 거대 AI 가 사용자한테 **인용 가능한 자연어** (거대 AI 가 paraphrase 자유).

호출 위치 (Tier 1):
    - get_daily_brief
    - explain_decision
    - get_prediction_accuracy

다른 tool 은 휴리스틱 narrative 로 충분 (LLM 호출 비용 회피).
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger("MarketMCP.Narrative")

# vLLM 호출 타임아웃 (초). bilingual ~700토큰 생성이 동시 부하 시 12s 를 종종 넘겨
# 시간당 1~5회 timeout → 템플릿 fallback degrade 되던 문제. env 로 조정 가능.
# (근본수정 2026-06-21: 12 → 35) 실측: 웜 2~4s, 콜드/큐잉 시 ~25s 까지 관측 →
# 콜드 + 동시부하 여유로 35s. 이 timeout 은 상한일 뿐 정상 응답은 바로 반환됨.
VLLM_NARRATIVE_TIMEOUT = float(os.getenv("VLLM_NARRATIVE_TIMEOUT", "35"))
# 가용성 빠른 health check 타임아웃 (초). 짧게 유지(가용성만 판정).
VLLM_HEALTH_TIMEOUT = float(os.getenv("VLLM_HEALTH_TIMEOUT", "2"))

# Cache: hash(input) → (narrative, timestamp). 60초 TTL.
_CACHE: Dict[str, Tuple[Dict[str, Any], float]] = {}
_CACHE_TTL = 60
_CACHE_MAX_ENTRIES = 100


def _is_vllm_available() -> bool:
    """vLLM 호출 가능 여부 (env 미설정 또는 unreachable 이면 false).

    환경변수 VLLM_BASE_URL 이 있고 빠른 health check 통과하면 true.
    매 호출마다 health check 하면 비효율 → 결과를 60s 메모이즈.
    """
    now = time.time()
    if hasattr(_is_vllm_available, "_cache_ts") and now - _is_vllm_available._cache_ts < 60:
        return _is_vllm_available._cache_val

    base_url = os.getenv("VLLM_BASE_URL")
    if not base_url:
        _is_vllm_available._cache_val = False
        _is_vllm_available._cache_ts = now
        return False

    try:
        import urllib.request
        req = urllib.request.Request(base_url.rstrip("/") + "/models")
        urllib.request.urlopen(req, timeout=VLLM_HEALTH_TIMEOUT).read(100)
        _is_vllm_available._cache_val = True
    except Exception as e:
        logger.debug("vLLM unavailable: %s", e)
        _is_vllm_available._cache_val = False

    _is_vllm_available._cache_ts = now
    return _is_vllm_available._cache_val


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    entry = _CACHE.get(key)
    if not entry:
        return None
    val, ts = entry
    if time.time() - ts > _CACHE_TTL:
        _CACHE.pop(key, None)
        return None
    return val


def _cache_set(key: str, val: Dict[str, Any]) -> None:
    if len(_CACHE) >= _CACHE_MAX_ENTRIES:
        oldest_key = min(_CACHE, key=lambda k: _CACHE[k][1])
        _CACHE.pop(oldest_key, None)
    _CACHE[key] = (val, time.time())


# ---------------------------------------------------------------------------
# System prompt (prefix cache 효과 위해 고정 + L2 정체성 박음)
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """You are OneQAZ Market Interpretation Engine.

Your role: produce **Layer-2 domain interpretation** — convert raw signal/context
data into specialist-grade Korean AND English narrative that a calling AI agent
can quote verbatim to a human user.

You are NOT a generalist news summarizer. You are NOT replacing the AI agent's
reasoning. You ARE the OneQAZ specialist voice — providing the domain
interpretation the AI agent does not have access to.

## Output format — ALWAYS strict JSON, NO other text

{
  "ko": {
    "headline": "한 줄 시장 상태 (15~30자, 단정 금지)",
    "evidence": ["근거 1 (한국어, 25자 내)", "근거 2", "근거 3"],
    "what_to_watch": "주의할 점 / 리스크 (30자 내, 한국어)"
  },
  "en": {
    "headline": "One-line market state (under 60 chars, no certainty)",
    "evidence": ["Evidence 1 (under 60 chars)", "Evidence 2", "Evidence 3"],
    "what_to_watch": "Risk / what to watch (under 80 chars)"
  }
}

## Rules (apply to BOTH languages)

1. **No certainty predictions**:
   - WRONG: "오를 것이다" / "BTC will rise"
   - RIGHT: "감지된다 / 진행 중 / 가능성" / "is detected / in progress / potential"

2. **Numbers → meaning translation**:
   - WRONG: "RSI 94.5" / "RSI is 94.5"
   - RIGHT: "단기 과매수" / "near-term overbought"
   - WRONG: "accuracy 0.87"
   - RIGHT: "정확도가 평균보다 우월" / "accuracy above average"

3. **Specialist voice — show the OneQAZ domain edge**:
   - Reference regime ("위험선호 회복" / "risk-on recovery"), structure
     ("유동성 확장" / "liquidity expansion"), divergence ("디커플링" / "decoupling")
     when supported by data.
   - Do NOT invent — only assert what the data supports.

4. **Both languages must say the SAME thing** — semantic parity.
   Korean is not a translation of English; both come from the same data.

5. **Insufficient data**: keep evidence short, set what_to_watch to
   "데이터 부족 — 추가 누적 필요" / "insufficient data — needs accumulation".

6. **3~5 evidence items** per language. NEVER copy raw numbers as text;
   always translate to specialist meaning."""


def _build_user_prompt(context_type: str, data: Dict[str, Any]) -> str:
    """context_type 별 user prompt. data 는 dict serialized."""
    data_text = json.dumps(data, ensure_ascii=False, default=str)
    if len(data_text) > 3500:
        data_text = data_text[:3500] + "...[truncated]"
    return (
        f"Context type: {context_type}\n"
        f"Data:\n{data_text}\n\n"
        f"Produce strict JSON with both 'ko' and 'en' fields. No other text."
    )


# ---------------------------------------------------------------------------
# vLLM 호출 (bilingual)
# ---------------------------------------------------------------------------

def _sanitize_lang_block(block: Any, max_headline: int, max_evidence: int, max_watch: int) -> Optional[Dict[str, Any]]:
    """언어 블록 검증 + 정리. 형식 안 맞으면 None."""
    if not isinstance(block, dict):
        return None
    headline = str(block.get("headline", ""))[:max_headline].strip()
    if not headline:
        return None
    evidence_raw = block.get("evidence", [])
    if not isinstance(evidence_raw, list):
        evidence_raw = []
    evidence = [str(e)[:max_evidence].strip() for e in evidence_raw if str(e).strip()][:5]
    what = str(block.get("what_to_watch", ""))[:max_watch].strip()
    return {"headline": headline, "evidence": evidence, "what_to_watch": what}


def _stamp_source(narr: Dict[str, Any], source: str) -> Dict[str, Any]:
    """narrative dict 에 source meta 박음. 'vllm' / 'templated' / 'mixed'.

    LB failover 디버깅 + admin /mcp-analytics 추적 + 사용자 알림
    (vLLM 부재 fallback 응답 hint) 용.
    """
    if isinstance(narr, dict):
        narr["_source"] = source
    return narr


def _call_vllm(user_prompt: str) -> Optional[Dict[str, Any]]:
    """vLLM 호출. VLLM_NARRATIVE_TIMEOUT(기본 35s) + JSON schema 검증. 실패 시 None."""
    import urllib.request

    base_url = os.getenv("VLLM_BASE_URL", "http://vllm:8000/v1")
    model_name = os.getenv("VLLM_MODEL_NAME", "qwen3.5-9b-finance")

    body = json.dumps({
        "model": model_name,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.35,
        "max_tokens": 700,  # bilingual 이라 두 배 가까이
        "response_format": {"type": "json_object"},
    }).encode()

    try:
        req = urllib.request.Request(
            base_url.rstrip("/") + "/chat/completions",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        resp = urllib.request.urlopen(req, timeout=VLLM_NARRATIVE_TIMEOUT).read()
        j = json.loads(resp)
        content = j["choices"][0]["message"]["content"]
        parsed = json.loads(content)

        # 두 언어 블록 검증
        ko = _sanitize_lang_block(parsed.get("ko"), max_headline=60, max_evidence=80, max_watch=120)
        en = _sanitize_lang_block(parsed.get("en"), max_headline=120, max_evidence=140, max_watch=200)

        if not ko and not en:
            return None
        out: Dict[str, Any] = {}
        if ko:
            out["ko"] = ko
        if en:
            out["en"] = en
        return out if out else None
    except Exception as e:
        logger.warning("vLLM narrative failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Templated fallback (양 언어)
# ---------------------------------------------------------------------------

def _fallback_narrative(context_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """vLLM 미사용 / 실패 시 템플릿 narrative.

    vLLM 부재 환경에서도 응답 구조가 깨지지 않게 항상 valid 한 dict 반환.
    한국어 + 영어 양쪽 모두 제공.
    """
    if context_type == "daily_brief":
        sigs = data.get("strong_signals") or []
        active_n = data.get("active_predictions_count", 0)
        trades = data.get("yesterday_trades", {}) or {}
        total = trades.get("total", 0)

        evidence_ko = []
        evidence_en = []
        if sigs:
            top = sigs[0] if isinstance(sigs[0], dict) else None
            if top:
                sym = top.get("symbol", "?")
                evidence_ko.append(f"강한 신호 {len(sigs)}건 (top: {sym})")
                evidence_en.append(f"{len(sigs)} strong signals (top: {sym})")
        if active_n > 0:
            evidence_ko.append(f"활성 예측 {active_n}건 record 중")
            evidence_en.append(f"{active_n} active predictions on record")
        if total > 0:
            evidence_ko.append(f"24h paper trade {total}건")
            evidence_en.append(f"{total} paper trades in last 24h")

        return {
            "ko": {
                "headline": "오늘 시장은 관찰 모드 (구조적 변화 없음)",
                "evidence": evidence_ko or ["데이터 누적 단계"],
                "what_to_watch": "강한 신호의 확정 여부 + 활성 예측 outcome",
            },
            "en": {
                "headline": "Market in observation mode (no structural shift)",
                "evidence": evidence_en or ["Data accumulation phase"],
                "what_to_watch": "Strong-signal confirmation + active-prediction outcomes",
            },
        }

    if context_type == "explain_decision":
        sym = data.get("symbol") or "this symbol"
        verdict = (data.get("overall_recommendation") or {}).get("verdict", "?")
        return {
            "ko": {
                "headline": f"{sym} — 시스템 판단: {verdict}",
                "evidence": ["multi-timeframe role 분석 기반", "최근 뉴스/이벤트 반영"],
                "what_to_watch": "verdict 의 confidence + score_trace",
            },
            "en": {
                "headline": f"{sym} — system verdict: {verdict}",
                "evidence": ["multi-timeframe role analysis", "recent news/event reflection"],
                "what_to_watch": "verdict confidence + score_trace",
            },
        }

    if context_type == "prediction_accuracy":
        cells = (data.get("meta") or {}).get("total_category_target_lag_cells", 0)
        return {
            "ko": {
                "headline": f"{cells}개 cell 의 적중률 추적 중",
                "evidence": ["sample_count + Wilson CI 모두 노출", "약한 카테고리 숨기지 않음"],
                "what_to_watch": "카테고리별 drift 신호",
            },
            "en": {
                "headline": f"Tracking hit rates across {cells} cells",
                "evidence": ["sample_count + Wilson CI exposed", "weak categories not hidden"],
                "what_to_watch": "Per-category drift signals",
            },
        }

    return {
        "ko": {"headline": "데이터 응답", "evidence": [], "what_to_watch": ""},
        "en": {"headline": "Data response", "evidence": [], "what_to_watch": ""},
    }


# ---------------------------------------------------------------------------
# 메인 엔트리
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# [2026-07-20 RCA T6] narrative 일관성 게이트
# ---------------------------------------------------------------------------
# 외부 감사에서 headline("위험선호 회복, 강신호 다수")과 evidence(승률 20%)가
# 모순되는 실사례 관측. LLM 산출 headline 의 방향이 구조화 데이터의 방향과
# 정면 충돌하면 해당 언어 블록을 templated fallback(데이터 파생이라 모순 불가)
# 으로 교체한다. 판정 불가(중립/불명) 시엔 손대지 않는다.

_GATE_POS_MARKERS = (
    "회복", "강세", "개선", "호전", "위험선호", "긍정",
    "bullish", "improving", "recovery", "risk-on", "rebound", "strengthen",
)
_GATE_NEG_MARKERS = (
    "악화", "약세", "손실", "위험회피", "부진", "하락세",
    "bearish", "deteriorat", "risk-off", "weaken", "losses", "declin",
)


def _gate_text_polarity(text: str) -> Optional[str]:
    t = (text or "").lower()
    pos = any(m in t for m in _GATE_POS_MARKERS)
    neg = any(m in t for m in _GATE_NEG_MARKERS)
    if pos and not neg:
        return "positive"
    if neg and not pos:
        return "negative"
    return None


def _gate_data_polarity(context_type: str, data: Dict[str, Any]) -> Optional[str]:
    """구조화 데이터가 명확한 방향을 가질 때만 판정 (아니면 None = 게이트 미적용)."""
    try:
        if context_type == "daily_brief":
            t = (data or {}).get("yesterday_trades") or {}
            total = int(t.get("total") or 0)
            win = int(t.get("winning") or 0)
            if total >= 20:
                wr = win / total
                if wr < 0.35:
                    return "negative"
                if wr > 0.65:
                    return "positive"
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    return None


def _consistency_gate(context_type: str, data: Dict[str, Any],
                      result: Dict[str, Any]) -> Dict[str, Any]:
    expected = _gate_data_polarity(context_type, data)
    if expected is None:
        return result
    replaced = []
    fb = None
    for lang in ("ko", "en"):
        blk = result.get(lang)
        if not isinstance(blk, dict):
            continue
        hp = _gate_text_polarity(blk.get("headline", ""))
        if hp is not None and hp != expected:
            if fb is None:
                fb = _fallback_narrative(context_type, data)
            if isinstance(fb.get(lang), dict):
                result[lang] = fb[lang]
                replaced.append(lang)
    if replaced:
        result["_consistency_gate"] = {
            "applied": True,
            "replaced_languages": replaced,
            "expected_polarity": expected,
            "reason": "headline direction contradicted structured evidence — templated fallback served",
        }
        logger.warning("[narrative] consistency gate fired (%s): headline vs data polarity "
                       "mismatch (%s) — replaced %s", context_type, expected, replaced)
    return result


def generate_narrative(context_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """context_type + data 로 한국어 + 영어 narrative 동시 생성.

    Args:
        context_type: "daily_brief" / "explain_decision" / "prediction_accuracy" 등.
        data: 요약 가능한 dict. 큰 객체는 알아서 truncate.

    Returns:
        {"ko": {"headline", "evidence", "what_to_watch"},
         "en": {"headline", "evidence", "what_to_watch"}}
        — 항상 valid dict. 실패 시 templated fallback.
    """
    # 1. cache 조회
    try:
        cache_key = context_type + "|" + json.dumps(data, sort_keys=True, default=str)[:500]
    except Exception:
        cache_key = context_type
    cached = _cache_get(cache_key)
    if cached:
        return cached

    # 2. vLLM 가능 여부 체크
    if not _is_vllm_available():
        result = _stamp_source(_fallback_narrative(context_type, data), "templated")
        _cache_set(cache_key, result)
        return result

    # 3. vLLM 호출
    user_prompt = _build_user_prompt(context_type, data)
    result = _call_vllm(user_prompt)
    if result is None or ("ko" not in result and "en" not in result):
        # vLLM 실패 → templated fallback (둘 다 제공)
        result = _stamp_source(_fallback_narrative(context_type, data), "templated")
    elif "ko" not in result or "en" not in result:
        # 한 언어만 성공한 경우 → 빠진 언어를 fallback 으로 채움
        fb = _fallback_narrative(context_type, data)
        if "ko" not in result:
            result["ko"] = fb["ko"]
        if "en" not in result:
            result["en"] = fb["en"]
        result = _stamp_source(result, "mixed")
    else:
        result = _stamp_source(result, "vllm")

    # [2026-07-20 RCA T6] LLM 산출 headline 이 구조화 데이터와 방향 모순이면
    # templated fallback 으로 교체 (templated 산출은 데이터 파생이라 게이트 불요)
    if result.get("_source") in ("vllm", "mixed"):
        result = _consistency_gate(context_type, data, result)

    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# [2026-07-08] Non-blocking narrative — stale-while-revalidate
# ---------------------------------------------------------------------------
# 근본 문제: generate_narrative() 는 동기 vLLM 호출(timeout 35s)이고 캐시 키가
# data 직렬화를 포함해 사실상 매번 콜드였다. 이것이 get_daily_brief avg 27.3s /
# p95 79.7s (GPU 큐잉 시)의 주범 — AI 에이전트 타임아웃(30~60s)을 초과해
# "첫 호출 타임아웃 → 서버 영구 회피"를 만들던 B2AI 퍼널의 목이다.
#
# 해법: 응답 경로에서는 절대 vLLM 을 기다리지 않는다.
# - 컨텍스트(+시장/심볼) 단위 최신본을 반환하고, 낡았으면 백그라운드 스레드가 갱신.
# - 최초 호출은 templated fallback (구조 항상 유효, _source='templated').
# - 시장 상태 내레이션은 60초마다 안 바뀌므로 STALE_TTL 기본 20분.
# - 캐시는 프로세스 메모리 전용 — agent_history RAG 인덱싱 금지(규칙 3)와 무관.

import threading as _threading

NARRATIVE_STALE_TTL = int(os.getenv("VLLM_NARRATIVE_STALE_TTL", "1200"))  # 20분

_LAST_GOOD: Dict[str, Tuple[Dict[str, Any], float]] = {}
_LAST_GOOD_MAX = 200
_INFLIGHT: set = set()
_INFLIGHT_LOCK = _threading.Lock()


def _nb_key(context_type: str, data: Dict[str, Any]) -> str:
    """비차단 캐시 키 — 컨텍스트 + 안정 부분키(시장/심볼).

    explain_decision 처럼 대상별 내용이 다른 컨텍스트에서 다른 대상의 내레이션을
    서빙하면 오정보가 되므로, data 의 안정 식별자(symbol/market)를 키에 포함한다.
    """
    sub = ""
    if isinstance(data, dict):
        sub = str(data.get("symbol") or data.get("market") or data.get("market_id") or "")
    return f"{context_type}|{sub}"


def _spawn_refresh(key: str, context_type: str, data: Dict[str, Any]) -> None:
    with _INFLIGHT_LOCK:
        if key in _INFLIGHT:
            return
        _INFLIGHT.add(key)

    def _job():
        try:
            narr = generate_narrative(context_type, data)
            if isinstance(narr, dict) and (narr.get("ko") or narr.get("en")):
                if len(_LAST_GOOD) >= _LAST_GOOD_MAX:
                    oldest = min(_LAST_GOOD, key=lambda k: _LAST_GOOD[k][1])
                    _LAST_GOOD.pop(oldest, None)
                _LAST_GOOD[key] = (narr, time.time())
        except Exception as e:
            logger.debug("narrative background refresh failed (%s): %s", key, e)
        finally:
            with _INFLIGHT_LOCK:
                _INFLIGHT.discard(key)

    _threading.Thread(target=_job, daemon=True, name=f"narr-{context_type}").start()


def generate_narrative_nonblocking(context_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """응답 경로용 — vLLM 을 기다리지 않는다 (stale-while-revalidate).

    Returns:
        최신(또는 stale) 내레이션. stale 이면 백그라운드 갱신을 트리거하고
        `_age_seconds` 를 박아 소비자가 신선도를 알 수 있게 한다.
        최초 호출(캐시 無)은 templated fallback.
    """
    key = _nb_key(context_type, data)
    now = time.time()
    entry = _LAST_GOOD.get(key)
    if entry is None or (now - entry[1]) >= NARRATIVE_STALE_TTL:
        _spawn_refresh(key, context_type, data)
    if entry is not None:
        narr = dict(entry[0])
        narr["_age_seconds"] = int(now - entry[1])
        return narr
    return _stamp_source(_fallback_narrative(context_type, data), "templated")
