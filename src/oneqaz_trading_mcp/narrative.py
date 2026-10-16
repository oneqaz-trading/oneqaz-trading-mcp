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
    - [2026-10-07] 근거 고정: 입력은 응답 원문이 아니라 컨텍스트별 사실 digest,
      빈 응답은 LLM 없이 '데이터 부족' 고정 문구, digest 에 없는 시장 상태 개념을 쓴
      문장은 근거 게이트가 제거, 캐시 키는 필터 범위(_scope_key)까지 포함.
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
import re
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

3. **Grounding — use ONLY facts present in Data**:
   - Every evidence item must restate a fact that is in Data (translated to meaning).
   - NEVER mention an indicator or market-state concept that Data does not contain:
     overbought/oversold, liquidity, risk-on/risk-off, decoupling, breakout,
     support/resistance, volatility, volume, RSI, MACD. If Data does not say it,
     you must not say it — even as "possible" or "detected".
   - Do NOT invent — if unsure, leave it out.

4. **Respect what the context IS**:
   - prediction_accuracy = the track record of OneQAZ's macro forecasts (hit rates vs
     baselines). It says NOTHING about current prices or market conditions.
   - explain_decision = one symbol's research signal, the system's latest decision and
     pooled historical outcomes for that signal type.
   - daily_brief = a cross-market overview (ranked signals, paper trades, predictions).

5. **Both languages must say the SAME thing** — semantic parity.
   Korean is not a translation of English; both come from the same data.

6. **Insufficient data**: keep evidence short, set what_to_watch to
   "데이터 부족 — 추가 누적 필요" / "insufficient data — needs accumulation".

7. **1~5 evidence items** per language — fewer when Data has fewer facts; never pad.
   NEVER copy raw numbers as text; always translate to specialist meaning.

8. **Describe, never advise**: no buy/sell/hold/avoid instructions or recommendations
   in any field — OneQAZ output is research information, not investment advice.
   Do not claim change over time (improving / rising / declining trend) unless Data
   contains a time comparison. Use market names exactly as given (crypto / kr_stock /
   us_stock) — do not guess exchanges such as KOSPI or NASDAQ."""


# ---------------------------------------------------------------------------
# [2026-10-07] 근거 고정 — digest · 빈 데이터 판정 · 범위 키
# ---------------------------------------------------------------------------
# 종전 프롬프트는 응답 dict 원문을 3,500자에서 잘라 넣었다. 실측(10-07): 3 컨텍스트 모두
# 데이터에 없는 "단기 과매수"·"유동성 확장"·"위험선호 회복" 을 근거로 냈고, 예측 적중률
# 응답(시장 상태 정보 없음)에도 "과매수" 가 붙었다. 잘린 원문은 무엇이 사실인지 모델이
# 구분하기 어렵고, 잘린 뒤쪽의 핵심(판정·기준선)이 빠지기도 한다.
# ⇒ 컨텍스트별로 **사실만 추린 digest** 를 만들어 넣고, 같은 digest 로 사후 게이트를 건다.

def _f(v: Any, nd: int = 3) -> Any:
    try:
        return round(float(v), nd)
    except (TypeError, ValueError):
        return None


def _forecast_cells(data: Dict[str, Any]) -> list:
    out = []
    for cat, markets in ((data or {}).get("summary") or {}).items():
        if not isinstance(markets, dict):
            continue
        for tgt, lags in markets.items():
            if not isinstance(lags, dict):
                continue
            for lag, info in lags.items():
                if isinstance(info, dict):
                    out.append((cat, tgt, lag, info))
    return out


def _prompt_digest(context_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """컨텍스트별 사실 요약 — LLM 입력이자 근거 게이트의 기준 텍스트."""
    data = data or {}
    if context_type == "prediction_accuracy":
        cells = _forecast_cells(data)
        fc = [c for c in cells if c[3].get("horizon_type") == "forecast"]

        def _avg(key):
            vals = [c[3][key] for c in fc if isinstance(c[3].get(key), (int, float))]
            return _f(sum(vals) / len(vals)) if vals else None
        maj = [c for c in fc if isinstance(c[3].get("majority_class_baseline"), (int, float))]
        meta = data.get("meta") or {}
        return {
            "what_this_is": (
                "Track record of OneQAZ macro regime-shift forecasts (category -> target market, "
                "lag). Not a description of current prices or market conditions."
            ),
            "categories_in_scope": sorted({c[0] for c in fc}),
            "target_markets_in_scope": sorted({c[1] for c in fc}),
            "forecast_cells": len(fc),
            # 라벨을 길게 쓴다 — 짧은 라벨은 오독됐다(10-07: skill 0.68 → "정확도 68%",
            # 전체 평균 → "채권 평균").
            "overall_avg_hit_rate_across_all_cells": _avg("accuracy"),
            "overall_avg_persistence_null_hit_rate": _avg("persistence_accuracy"),
            "overall_avg_majority_class_baseline_hit_rate": _avg("majority_class_baseline"),
            "cells_beating_majority_baseline": sum(1 for c in maj if c[3].get("beats_majority_baseline")),
            "cells_with_majority_baseline": len(maj),
            "edge_cells_beating_persistence": [
                f"{e.get('source_category')} -> {e.get('target_market')} (lag {e.get('lag_bucket')}): "
                f"skill score vs persistence {_f(e.get('skill_score'), 2)} "
                f"(a skill score, not a hit rate; 0 = no better than persistence)"
                for e in (data.get("edge_cells") or [])[:5]
            ],
            "anti_predictive_cells_count": len(data.get("anti_predictive_cells") or []),
            "persistence_method": meta.get("persistence_method"),
        }
    if context_type == "explain_decision":
        sig = data.get("signal") or {}
        rec = data.get("overall_recommendation") or {}
        basis = rec.get("basis") or {}
        trace = sig.get("score_trace") if isinstance(sig.get("score_trace"), dict) else {}
        roles = []
        for iv, r in ((trace or {}).get("stage1_role_scores") or {}).items():
            if isinstance(r, dict):
                roles.append(f"{iv} {r.get('role')} {_f(r.get('score'), 2)}")
        return {
            "what_this_is": "One symbol's latest research signal, the system's latest decision and pooled history.",
            "symbol": data.get("symbol"),
            "market_id": data.get("market_id"),
            "signal_action": sig.get("action"),
            "signal_score": _f(sig.get("signal_score")),
            "pattern_type": sig.get("pattern_type"),
            "market_condition": sig.get("market_condition"),
            "signal_reason": sig.get("reason"),
            "role_scores": roles,
            "verdict": rec.get("verdict"),
            "verdict_text": rec.get("text"),
            "latest_decision": basis.get("latest_decision"),
            "already_holding": basis.get("already_holding"),
            "historical_class": basis.get("historical_class"),
            "historical_hit_rate": _f(basis.get("historical_hit_rate")),
            "historical_avg_profit_pct": _f(basis.get("historical_avg_profit_pct")),
            "historical_scope": "pooled across all symbols with the same signal type (not this symbol)",
            "news": "market-wide and unverified — not evidence for this symbol; do not use",
        }
    if context_type == "daily_brief":
        trades = data.get("yesterday_trades") or {}
        total = int(trades.get("total") or 0)
        sigs = []
        for s in (data.get("strong_signals") or [])[:6]:
            if isinstance(s, dict):
                hr = s.get("score_bucket_hit_rate_24h") or {}
                sigs.append(
                    f"{s.get('symbol')} ({s.get('market_id')}) {s.get('action')} score "
                    f"{_f(s.get('signal_score'))} measured_24h_hit_rate_of_score_bucket {_f(hr.get('hit_rate'), 2)}"
                    f" (n={hr.get('n')}, market_base {_f(hr.get('market_base_rate'), 2)})"
                )
        macro = data.get("macro_regime") or {}
        cats = []
        for c in (macro.get("categories") or [])[:10]:
            if isinstance(c, dict):
                cats.append({k: c.get(k) for k in ("category", "sample_count") if c.get(k) is not None})
        return {
            "what_this_is": "Cross-market overview: ranked research signals, last-24h paper trades, active forecasts.",
            "market": data.get("market"),
            # [2026-10-14] 순서 = 시장별 |score|. 강도 판정 아님 — 구간 실측 적중률을 시장 평균과 비교해 말할 것.
            "example_signals_by_abs_score_not_strength": sigs,
            "engine_stance_by_market": {m: (b or {}).get("by_action") for m, b in (data.get("market_breadth") or {}).items()},
            "last_session_signals_count": len(data.get("last_session_signals") or []),
            "paper_trades_24h": {
                "total": total,
                "winning": trades.get("winning"),
                "losing": trades.get("losing"),
                "win_rate": _f((trades.get("winning") or 0) / total, 2) if total else None,
            },
            "active_predictions_count": data.get("active_predictions_count"),
            # [2026-10-14] 엔진 글로벌 레짐(매매 매크로 점수 입력). score 는 확률이 아니다 — 확률처럼 말하지 말 것.
            "engine_macro_regime": ({"regime": macro.get("regime"), "regime_signed_-1_to_1": macro.get("regime_signed"),
                                     "as_of": macro.get("as_of")} if macro.get("regime") else None),
            "macro_categories_data_coverage_not_regime": cats,
        }
    return {"data": data}


def _is_empty_payload(context_type: str, data: Dict[str, Any]) -> bool:
    """해석할 사실이 없는 응답 — LLM 을 부르지 않고 '데이터 부족' 고정 문구를 낸다.

    실사고(10-07): get_prediction_accuracy(target_market='crypto') 가 필터 버그로 0셀이었는데
    내레이션은 "암호화폐 시장 정확도 상승 추세" 등 근거 5줄을 달고 나갔다.
    """
    data = data or {}
    if data.get("error") is True:
        return True
    if context_type == "prediction_accuracy":
        return not _forecast_cells(data)
    if context_type == "explain_decision":
        sig = data.get("signal") or {}
        return not any(k for k in sig if k != "_error")
    if context_type == "daily_brief":
        trades = data.get("yesterday_trades") or {}
        return not (
            data.get("strong_signals") or data.get("last_session_signals")
            or int(trades.get("total") or 0) or int(data.get("active_predictions_count") or 0)
        )
    return False


def _scope_key(context_type: str, data: Dict[str, Any]) -> str:
    """캐시 범위 — 같은 키의 내레이션은 같은 대상·같은 필터 범위에만 서빙된다.

    종전 키는 symbol/market/market_id 만 봐서, 그런 필드가 없는 prediction_accuracy 는
    필터(target_market·category)와 무관하게 키 하나("prediction_accuracy|")를 공유했다 —
    전체 24셀 응답의 내레이션이 0셀 필터 응답에 최대 20분 서빙되는 경로.
    """
    data = data or {}
    if _is_empty_payload(context_type, data):
        return "empty"
    if context_type == "prediction_accuracy":
        import hashlib
        cells = sorted(f"{c[0]}/{c[1]}/{c[2]}" for c in _forecast_cells(data))
        return "cells:" + hashlib.sha1("|".join(cells).encode()).hexdigest()[:12]
    if context_type == "explain_decision":
        return f"{data.get('market_id') or ''}:{data.get('symbol') or ''}"
    return str(data.get("symbol") or data.get("market") or data.get("market_id") or "")


def _insufficient_narrative(context_type: str) -> Dict[str, Any]:
    """빈 응답용 결정적 문구 — 근거 항목 0개 (지어낼 사실이 없으므로)."""
    narr = {
        "ko": {"headline": "데이터 부족 — 해석 생략", "evidence": [],
               "what_to_watch": "데이터 부족 — 추가 누적 필요"},
        "en": {"headline": "Insufficient data — no interpretation", "evidence": [],
               "what_to_watch": "insufficient data — needs accumulation"},
        "_data_sufficiency": "empty",
    }
    return _stamp_source(narr, "templated")


def _build_user_prompt(context_type: str, data: Dict[str, Any]) -> str:
    """context_type 별 user prompt — 응답 원문이 아니라 사실 digest 를 넣는다."""
    data_text = json.dumps(_prompt_digest(context_type, data), ensure_ascii=False, default=str)
    if len(data_text) > 3500:
        data_text = data_text[:3500] + "...[truncated]"
    return (
        f"Context type: {context_type}\n"
        f"Data (the ONLY facts you may use):\n{data_text}\n\n"
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
    # [2026-10-07] 한/영 블록에 한자(CJK 통합 한자)가 섞이면 모델 출력 오염 — 실측 "流동성".
    # 헤드라인 오염은 블록 무효(빠진 언어는 템플릿으로 채워진다), 근거는 항목 단위로 버린다.
    if not headline or _HAN_RE.search(headline):
        return None
    evidence_raw = block.get("evidence", [])
    if not isinstance(evidence_raw, list):
        evidence_raw = []
    evidence = [
        str(e)[:max_evidence].strip() for e in evidence_raw
        if str(e).strip() and not _HAN_RE.search(str(e))
    ][:5]
    what = str(block.get("what_to_watch", ""))[:max_watch].strip()
    if _HAN_RE.search(what):
        what = ""
    return {"headline": headline, "evidence": evidence, "what_to_watch": what}


_HAN_RE = re.compile(r"[一-鿿㐀-䶿]")


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

        # [2026-10-07] 템플릿도 데이터에 없는 단정("구조적 변화 없음")을 하지 않는다 — 근거
        # 게이트가 LLM 블록을 이 템플릿으로 교체하므로 템플릿 자체가 사실만 말해야 한다.
        evidence_ko = []
        evidence_en = []
        if sigs:
            top = sigs[0] if isinstance(sigs[0], dict) else None
            if top:
                sym = top.get("symbol", "?")
                evidence_ko.append(f"신뢰도 상위 신호 {len(sigs)}건 (1위: {sym})")
                evidence_en.append(f"{len(sigs)} top-ranked signals (first: {sym})")
        if active_n > 0:
            evidence_ko.append(f"활성 예측 {active_n}건 record 중")
            evidence_en.append(f"{active_n} active predictions on record")
        if total > 0:
            win = int(trades.get("winning") or 0)
            evidence_ko.append(f"24h paper trade {total}건 (수익 {win}건)")
            evidence_en.append(f"{total} paper trades in last 24h ({win} winning)")

        return {
            "ko": {
                "headline": "오늘 시장 요약 (데이터 기반 템플릿)",
                "evidence": evidence_ko or ["데이터 누적 단계"],
                "what_to_watch": "상위 신호의 점수·보정 신뢰도 + 활성 예측 결과",
            },
            "en": {
                "headline": "Today's market summary (data-derived template)",
                "evidence": evidence_en or ["Data accumulation phase"],
                "what_to_watch": "Top signals' score and calibrated confidence + active-prediction outcomes",
            },
        }

    if context_type == "explain_decision":
        sym = data.get("symbol") or "this symbol"
        verdict = (data.get("overall_recommendation") or {}).get("verdict", "?")
        # [2026-10-07] 뉴스는 verdict 에 쓰지 않는다(시장 전체·UNVERIFIED) — "뉴스 반영" 문구 제거
        return {
            "ko": {
                "headline": f"{sym} — 시스템 판단: {verdict}",
                "evidence": ["멀티 타임프레임 역할 점수 기반", "최근 결정·과거 패턴(전 종목 합산) 반영"],
                "what_to_watch": "verdict 의 basis(판정 입력) 확인",
            },
            "en": {
                "headline": f"{sym} — system verdict: {verdict}",
                "evidence": ["multi-timeframe role scores", "latest decision + pooled historical pattern"],
                "what_to_watch": "check the verdict's basis (inputs used)",
            },
        }

    if context_type == "prediction_accuracy":
        # [2026-10-07 처방 ①] 이 컨텍스트의 유일한 서사(LLM 미사용). 서빙 시점 데이터에서 매번
        # 다시 그리므로 수치가 full_data 와 어긋나지 않는다 — 셀 개수만 쓰고 비율은 쓰지 않는다.
        d = _prompt_digest("prediction_accuracy", data)
        n = d.get("forecast_cells") or 0
        edges = data.get("edge_cells") or []
        n_edge = len(edges)
        n_anti = d.get("anti_predictive_cells_count") or 0
        beat, with_maj = d.get("cells_beating_majority_baseline") or 0, d.get("cells_with_majority_baseline") or 0
        ev_ko = [f"persistence 를 유의하게 이긴 셀 {n_edge}개, 유의하게 못한 셀 {n_anti}개"]
        ev_en = [f"{n_edge} cells significantly beat persistence, {n_anti} are significantly worse"]
        if with_maj:
            ev_ko.append(f"'가장 흔한 결과' 상수 예측을 넘는 셀 {beat}/{with_maj}")
            ev_en.append(f"{beat}/{with_maj} cells beat a constant most-common-outcome guess")
        if edges:
            names = ", ".join(f"{e.get('source_category')}→{e.get('target_market')}" for e in edges[:2])
            ev_ko.append(f"기준선을 이긴 셀: {names}")
            ev_en.append(f"cells beating persistence: {names}")
        if d.get("persistence_method") == "reconstructed_from_current_candle_labels":
            ev_ko.append("현재 persistence 값은 재계산 전 방식(과대 평가) — 교정 배치 대기")
            ev_en.append("persistence values are pre-fix (overstated skill) — corrected batch pending")
        return {
            "ko": {
                "headline": f"거시 예측 {n}개 셀의 적중률 — 기준선 대비 평가",
                "evidence": ev_ko,
                "what_to_watch": "셀별 skill_ci_95 와 beats_majority_baseline",
            },
            "en": {
                "headline": f"Hit rates of {n} macro forecast cells — judged against baselines",
                "evidence": ev_en,
                "what_to_watch": "per-cell skill_ci_95 and beats_majority_baseline",
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
        result["_source"] = "templated"  # 서빙 시 현재 데이터로 재렌더 (근거 게이트와 동일)
        result["_consistency_gate"] = {
            "applied": True,
            "replaced_languages": replaced,
            "expected_polarity": expected,
            "reason": "headline direction contradicted structured evidence — templated fallback served",
        }
        logger.warning("[narrative] consistency gate fired (%s): headline vs data polarity "
                       "mismatch (%s) — replaced %s", context_type, expected, replaced)
    return result


# ---------------------------------------------------------------------------
# [2026-10-07] 근거 게이트 — digest 에 없는 시장 상태 개념을 쓴 문장 차단
# ---------------------------------------------------------------------------
# 프롬프트 규칙만으로는 부족하다(규칙 3 이전부터 "Do NOT invent" 가 있었는데도 실측 4/4
# 위반). 같은 digest 텍스트를 기준으로 기계적으로 거른다. 개념 쌍(한/영) 중 하나라도
# digest 에 있으면 허용 — 데이터가 말한 개념은 어느 언어로든 쓸 수 있다.
_GROUNDED_CONCEPTS: Tuple[Tuple[Tuple[str, ...], Tuple[str, ...]], ...] = (
    (("과매수",), ("overbought",)),
    (("과매도",), ("oversold",)),
    (("유동성",), ("liquidity",)),
    (("위험선호", "리스크온"), ("risk-on", "risk on")),
    (("위험회피", "리스크오프"), ("risk-off", "risk off")),
    (("디커플링", "탈동조"), ("decoupl",)),
    (("돌파",), ("breakout",)),
    (("지지선", "저항선"), ("support level", "resistance")),
    (("변동성",), ("volatil",)),
    (("거래량",), ("trading volume", "volume")),
    (("rsi",), ("rsi",)),
    (("macd",), ("macd",)),
    # 시간에 따른 변화 주장 — 시계열이 없는 digest(예측 적중률)에서 "정확도 상승 추세" 가
    # 나간 실사고(10-07) 부류. digest 에 추세·방향 정보가 있을 때만 허용.
    (("추세", "향상", "개선", "악화", "상승세", "하락세"), ("trend", "improv", "deteriorat", "rising", "declin")),
    # 거래소 단정 — digest 는 kr_stock 만 말하는데 "코스피 290650" 처럼 거래소를 추측(10-07 실측)
    (("코스피", "코스닥"), ("kospi", "kosdaq")),
    (("나스닥", "뉴욕증권거래소"), ("nasdaq", "nyse")),
)

# 조언·지시 표현 — 데이터와 무관하게 항상 금지 (응답은 투자 조언이 아니다, 규칙 8).
# 실측(10-07): explain_decision what_to_watch "약한 신호이므로 추가 매수 회피 필요".
_ADVICE_MARKERS: Tuple[str, ...] = (
    "회피 필요", "피하세요", "피해야", "매수하세요", "매도하세요", "매수 권장", "매도 권장",
    "권장합니다", "추천합니다", "해야 합니다",
    "should buy", "should sell", "you should", "avoid buying", "avoid selling",
    "recommend buying", "recommend selling", "we recommend",
)


def _has_variant(text: str, variant: str) -> bool:
    # 영문 변형은 왼쪽 단어 경계 — "rsi" 가 "persistence" 에 부분일치하지 않게.
    # (오른쪽은 열어 둔다: "volatil" → volatility/volatile, "decoupl" → decoupling)
    if variant.isascii():
        return re.search(r"(?<![a-z0-9])" + re.escape(variant), text) is not None
    return variant in text


# [2026-10-07 처방 ②] LLM 서사에는 숫자를 싣지 않는다 — 숫자의 단일 출처는 full_data.
# 서사는 stale-while-revalidate 라 몇 시간~하루 묵을 수 있고(09-26 실측: full_data 승률 59% vs
# 서사 "수익률 64%", _age 86,415s), 같은 응답 안 두 숫자 중 어느 쪽이 실측인지 소비 AI 가
# 구분할 수 없다. 예외: 종목코드·심볼(005930, 1INCH), 인터벌·lag 표기(15m, 1d, 8h, 0.5h).
_NUM_TOKEN_RE = re.compile(r"[A-Za-z0-9_.%+\-]*\d[A-Za-z0-9_.%+\-]*")
_INTERVAL_LAG_RE = re.compile(r"^\d+(\.\d+)?[mhdw]$", re.IGNORECASE)


def _allowed_number_tokens(context_type: str, data: Dict[str, Any]) -> set:
    data = data or {}
    toks = {str(data.get("symbol") or "").upper()}
    for key in ("strong_signals", "last_session_signals"):
        for s in data.get(key) or []:
            if isinstance(s, dict) and s.get("symbol"):
                toks.add(str(s["symbol"]).upper())
    toks.discard("")
    return toks


def _numeric_tokens(text: str, allowed: set) -> list:
    bad = []
    for tok in _NUM_TOKEN_RE.findall(text or ""):
        core = tok.strip(".,%+-")
        if not core or core.upper() in allowed or _INTERVAL_LAG_RE.match(core):
            continue
        bad.append(tok)
    return bad


def _ungrounded_concepts(text: str, allowed_text: str) -> list:
    t = (text or "").lower()
    hits = []
    for kos, ens in _GROUNDED_CONCEPTS:
        variants = tuple(k.lower() for k in kos) + ens
        if any(_has_variant(t, v) for v in variants) and \
                not any(_has_variant(allowed_text, v) for v in variants):
            hits.append(ens[0])
    if any(m in t for m in _ADVICE_MARKERS):
        hits.append("advice")
    return hits


def _grounding_gate(context_type: str, data: Dict[str, Any],
                    result: Dict[str, Any]) -> Dict[str, Any]:
    allowed = json.dumps(_prompt_digest(context_type, data), ensure_ascii=False, default=str).lower()
    num_ok = _allowed_number_tokens(context_type, data)

    def _bad(text: str) -> list:
        hits = _ungrounded_concepts(text, allowed)
        if _numeric_tokens(text, num_ok):
            hits.append("number")
        return hits

    dropped: Dict[str, int] = {}
    concepts: set = set()
    replace_all = False
    for lang in ("ko", "en"):
        blk = result.get(lang)
        if not isinstance(blk, dict):
            continue
        head_bad = _bad(blk.get("headline", "")) + _bad(blk.get("what_to_watch", ""))
        if head_bad:
            replace_all = True  # 언어 간 의미 동등성(규칙 5) 유지 — 한쪽만 바꾸지 않는다
            concepts.update(head_bad)
        keep = []
        for ev in blk.get("evidence") or []:
            bad = _bad(ev)
            if bad:
                concepts.update(bad)
            else:
                keep.append(ev)
        if len(keep) != len(blk.get("evidence") or []):
            dropped[lang] = len(blk.get("evidence") or []) - len(keep)
            blk["evidence"] = keep
    if replace_all:
        fb = _fallback_narrative(context_type, data)
        for lang in ("ko", "en"):
            if isinstance(fb.get(lang), dict):
                result[lang] = fb[lang]
        # 템플릿은 서빙 시점에 현재 데이터로 다시 그린다 (generate_narrative_nonblocking) —
        # 생성 시점 수치를 담은 템플릿이 캐시에 남아 묵지 않게.
        result["_source"] = "templated"
    if replace_all or dropped:
        result["_grounding_gate"] = {
            "applied": True,
            "ungrounded_concepts": sorted(concepts),
            "dropped_evidence": dropped,
            "replaced_with_template": replace_all,
            "reason": "narrative used market-state concepts absent from the underlying data",
        }
        logger.warning("[narrative] grounding gate fired (%s): %s — dropped %s, replaced=%s",
                       context_type, sorted(concepts), dropped, replace_all)
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
    # 0. [2026-10-07] 해석할 사실이 없으면 LLM 을 부르지 않는다 (fail-closed)
    if _is_empty_payload(context_type, data):
        return _insufficient_narrative(context_type)
    # 0-b. [2026-10-07 처방 ①] 수치 표 컨텍스트는 결정적 템플릿만 — LLM 오해석 원천 차단
    if context_type in _TEMPLATE_ONLY_CONTEXTS:
        return _stamp_source(_fallback_narrative(context_type, data), "templated")

    # 1. cache 조회 — 범위 키 + digest 해시 (종전 원문 앞 500자는 필터가 달라도 겹칠 수 있었다)
    try:
        import hashlib
        _digest = json.dumps(_prompt_digest(context_type, data), sort_keys=True, default=str)
        cache_key = (f"{context_type}|{_scope_key(context_type, data)}|"
                     f"{hashlib.sha1(_digest.encode()).hexdigest()[:16]}")
    except Exception:
        cache_key = f"{context_type}|{_scope_key(context_type, data)}"
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
    if result is None or "ko" not in result or "en" not in result:
        # vLLM 실패 또는 한 언어만 성공 → 전체 템플릿.
        # [2026-10-07] 종전 'mixed'(빠진 언어만 템플릿)는 템플릿 블록이 생성 시점 수치를 담은 채
        # 캐시에 남았다. 이제 서사는 '게이트 통과한 순수 vllm' 또는 '서빙 시 재렌더하는 templated'
        # 둘 중 하나다 (언어 간 의미 동등성도 유지).
        result = _stamp_source(_fallback_narrative(context_type, data), "templated")
    else:
        result = _stamp_source(result, "vllm")

    # [2026-07-20 RCA T6] LLM 산출 headline 이 구조화 데이터와 방향 모순이면
    # templated fallback 으로 교체 (templated 산출은 데이터 파생이라 게이트 불요)
    # [2026-10-07] 그 앞에 근거 게이트 — digest 에 없는 시장 상태 개념·숫자 차단
    if result.get("_source") == "vllm":
        result = _grounding_gate(context_type, data, result)
        if result.get("_source") == "vllm":
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

# [2026-10-07 처방 ①] LLM 을 쓰지 않는 컨텍스트 — 수치 표라 해석 가치보다 오해석 위험이 크다
# (실측: skill 0.68 → "정확도 68%", edge 셀 → "정확도가 평균을 초월"). 신뢰 평가 AI 가
# 가장 먼저 부르는 tool 이라 오정보 비용도 가장 크다.
_TEMPLATE_ONLY_CONTEXTS = frozenset({"prediction_accuracy"})

_LAST_GOOD: Dict[str, Tuple[Dict[str, Any], float]] = {}
_LAST_GOOD_MAX = 200
_INFLIGHT: set = set()
_INFLIGHT_LOCK = _threading.Lock()


def _nb_key(context_type: str, data: Dict[str, Any]) -> str:
    """비차단 캐시 키 — 컨텍스트 + 안정 부분키(시장/심볼).

    explain_decision 처럼 대상별 내용이 다른 컨텍스트에서 다른 대상의 내레이션을
    서빙하면 오정보가 되므로, data 의 안정 식별자(symbol/market)를 키에 포함한다.
    """
    # [2026-10-07] 필터 범위까지 포함 (_scope_key 참조) — 종전 prediction_accuracy 는 키 1개.
    return f"{context_type}|{_scope_key(context_type, data if isinstance(data, dict) else {})}"


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
    # [2026-10-07] 빈 응답은 캐시·백그라운드 갱신 없이 즉시 '데이터 부족' 고정 문구
    if _is_empty_payload(context_type, data if isinstance(data, dict) else {}):
        return _insufficient_narrative(context_type)
    # [2026-10-07 처방 ①] 템플릿 전용 컨텍스트 — 백그라운드 LLM 갱신 자체를 하지 않는다
    if context_type in _TEMPLATE_ONLY_CONTEXTS:
        return _stamp_source(_fallback_narrative(context_type, data), "templated")
    key = _nb_key(context_type, data)
    now = time.time()
    entry = _LAST_GOOD.get(key)
    if entry is None or (now - entry[1]) >= NARRATIVE_STALE_TTL:
        _spawn_refresh(key, context_type, data)
    if entry is not None and entry[0].get("_source") == "vllm":
        narr = dict(entry[0])
        narr["_age_seconds"] = int(now - entry[1])
        return narr
    # 캐시가 없거나 마지막 갱신 결과가 템플릿이면 — 지금 데이터로 다시 그린다.
    # (템플릿 결과도 캐시 항목으로는 남겨 갱신 주기는 STALE_TTL 로 유지 = vLLM 재호출 폭주 방지)
    return _stamp_source(_fallback_narrative(context_type, data), "templated")
