# -*- coding: utf-8 -*-
"""
지표 Resource
=============
Fear & Greed Index, 4-Layer 레짐 분석 등 외부/계산 지표 제공

데이터 소스:
- market.coin_market.market_analyzer: Fear&Greed API
- trade.core.market.MarketAnalyzer: 4-Layer 레짐 분석
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from mcps.config import (
    CACHE_TTL_FEAR_GREED,
    CACHE_TTL_MARKET_STATUS,
    PROJECT_ROOT,
)
from mcps.resources.resource_response import (
    mcp_error,
    MCPErrorCode,
    MCPErrorAction,
    to_resource_text,
    wrap_with_ai_summary,
)

logger = logging.getLogger("MarketMCP")

# ---------------------------------------------------------------------------
# Fear & Greed Index 로더
# ---------------------------------------------------------------------------

def _fetch_fear_greed_from_api() -> Dict[str, Any]:
    """Alternative.me API에서 Fear & Greed Index 조회 + 캐시 파일 저장"""
    import json
    import time
    import urllib.request

    cache_path = PROJECT_ROOT / "market" / "coin_market" / "data_storage" / "fear_greed_cache.json"

    # 캐시 파일이 5분 이내이면 재사용
    try:
        if cache_path.exists():
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            cache_ts = cached.get("cache_timestamp", 0)
            if time.time() - cache_ts < 300 and cached.get("error") is None:
                return cached
    except Exception:
        pass

    # API 호출
    try:
        url = "https://api.alternative.me/fng/?limit=1&format=json"
        req = urllib.request.Request(url, headers={"User-Agent": "AutoTrader/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read().decode())
        entry = body.get("data", [{}])[0]
        result = {
            "value": int(entry.get("value", 50)),
            "classification": entry.get("value_classification", "Neutral"),
            "timestamp": int(entry.get("timestamp", 0)),
            "cache_timestamp": time.time(),
            "error": None,
        }
    except Exception as e:
        logger.warning(f"Fear & Greed API 호출 실패: {e}")
        # 캐시 파일이 있으면 stale 캐시라도 사용
        try:
            if cache_path.exists():
                return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            pass
        return {"value": 50, "classification": "Neutral", "error": str(e)}

    # 캐시 저장
    try:
        cache_path.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

    return result


def _get_fear_greed_adjustment(fg_value: int) -> Dict[str, Any]:
    """Fear & Greed 수치에 따른 전략 조정 계수 계산"""
    if fg_value <= 20:
        return {"threshold_adj": -0.15, "position_mult": 0.5, "strategy_hint": "defensive"}
    elif fg_value <= 35:
        return {"threshold_adj": -0.08, "position_mult": 0.7, "strategy_hint": "cautious"}
    elif fg_value >= 80:
        return {"threshold_adj": 0.10, "position_mult": 0.6, "strategy_hint": "take_profit"}
    elif fg_value >= 65:
        return {"threshold_adj": 0.05, "position_mult": 0.85, "strategy_hint": "trend"}
    else:
        return {"threshold_adj": 0.0, "position_mult": 1.0, "strategy_hint": "neutral"}


def _load_fear_greed_index() -> Dict[str, Any]:
    """Fear & Greed Index 로드 (Alternative.me API + 캐시)"""
    try:
        fg_data = _fetch_fear_greed_from_api()
        fg_value = fg_data.get("value", 50)
        fg_class = fg_data.get("classification", "Neutral")
        adjustment = _get_fear_greed_adjustment(fg_value)

        result = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "value": fg_value,
            "classification": fg_class,
            "adjustment": adjustment,
            "interpretation": _interpret_fear_greed(fg_value, fg_class),
        }
        result["_llm_summary"] = _generate_fear_greed_text(result)
        return result

    except Exception as e:
        logger.error(f"Failed to load Fear & Greed: {e}")
        return mcp_error(
            MCPErrorCode.NO_DATA,
            f"Fear & Greed 로드 실패: {e}",
            action=MCPErrorAction.RETRY,
            action_value="60",
        )

def _interpret_fear_greed(value: int, classification: str) -> str:
    """Fear & Greed 해석"""
    if value <= 25:
        return "극도의 공포 구간. 역발상 매수 기회 가능성. 단, 추가 하락 리스크 존재."
    elif value <= 45:
        return "공포 우세. 신중한 접근 필요. 손절선 타이트하게 설정 권장."
    elif value >= 75:
        return "극도의 탐욕 구간. 과열 경고. 조기 익절 또는 신규 진입 자제 권장."
    elif value >= 55:
        return "탐욕 우세. 차익 실현 기회 모색. 추세 추종 유효."
    else:
        return "중립 구간. 기술적 분석 기반 판단 권장."

def _generate_fear_greed_text(data: Dict[str, Any]) -> str:
    """Fear & Greed LLM 요약 텍스트"""
    value = data.get("value", 50)
    classification = data.get("classification", "Neutral")
    interpretation = data.get("interpretation", "")
    adj = data.get("adjustment", {})

    lines = [
        f"[Fear & Greed Index]",
        f"- 현재 수치: {value} ({classification})",
        f"- 해석: {interpretation}",
    ]
    if adj:
        threshold_adj = adj.get("threshold_adj", 0)
        position_mult = adj.get("position_mult", 1.0)
        strategy_hint = adj.get("strategy_hint", "trend")
        lines.append(f"- 전략 조정: 임계값 {threshold_adj:+.2f}, 포지션 배수 {position_mult:.2f}x, 힌트={strategy_hint}")
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# 4-Layer 레짐 분석 로더
# ---------------------------------------------------------------------------

def _load_market_regime_analysis() -> Dict[str, Any]:
    """4-Layer 시장 레짐 분석 로드"""
    try:
        import sys
        if str(PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(PROJECT_ROOT))

        from trade.core.market import MarketAnalyzer

        analyzer = MarketAnalyzer()
        result = analyzer.analyze_market_regime()
        analysis = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "score": result.get("score", 0.5),
            "regime": result.get("regime", "Neutral"),
            "volatility": result.get("volatility", 0.0),
            "raw_score": result.get("raw_score", 0.0),
            "details": result.get("details", {}),
        }
        analysis["_llm_summary"] = _generate_regime_analysis_text(analysis)
        return analysis

    except ImportError as e:
        logger.warning(f"MarketAnalyzer not available: {e}")
        return mcp_error(
            MCPErrorCode.NO_DATA,
            "MarketAnalyzer module not available",
            action=MCPErrorAction.CHECK,
            regime="Neutral",
            score=0.5,
            _llm_summary="[4-Layer 레짐] 모듈 로드 실패, 기본값 사용",
        )
    except Exception as e:
        logger.error(f"Failed to load market regime analysis: {e}")
        return mcp_error(
            MCPErrorCode.NO_DATA,
            f"시장 레짐 분석 로드 실패: {e}",
            action=MCPErrorAction.RETRY,
            action_value="60",
        )

def _generate_regime_analysis_text(data: Dict[str, Any]) -> str:
    """4-Layer 레짐 LLM 요약 텍스트"""
    regime = data.get("regime", "Neutral")
    score = data.get("score", 0.5)
    volatility = data.get("volatility", 0.0)
    details = data.get("details", {})

    lines = [
        f"[4-Layer 시장 레짐 분석]",
        f"- 레짐: {regime} (점수: {score:.2f})",
        f"- 변동성: {volatility:.4f}",
    ]
    if details:
        sl = details.get("sl", 0)
        long = details.get("long", 0)
        mid = details.get("mid", 0)
        short = details.get("short", 0)
        lines.append(f"- 레이어별: SL={sl:.2f}, Long={long:.2f}, Mid={mid:.2f}, Short={short:.2f}")

    regime_lower = regime.lower()
    if "bull" in regime_lower:
        lines.append("- 해석: 상승 추세. 추세 추종 전략 유효.")
    elif "bear" in regime_lower:
        lines.append("- 해석: 하락 추세. 숏 또는 관망 권장.")
    elif "high" in regime_lower and "volatility" in regime_lower:
        lines.append("- 해석: 고변동성. 변동성 돌파 전략 고려.")
    else:
        lines.append("- 해석: 중립/횡보. 레인지 트레이딩 또는 관망.")
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# 통합 시장 컨텍스트 (Fear&Greed + 4-Layer)
# ---------------------------------------------------------------------------

def _load_market_context() -> Dict[str, Any]:
    """통합 시장 컨텍스트 (Fear&Greed + 4-Layer)"""
    fear_greed = _load_fear_greed_index()
    regime_analysis = _load_market_regime_analysis()
    fg_value = fear_greed.get("value", 50)
    regime = regime_analysis.get("regime", "Neutral").lower()

    aligned = True
    alignment_note = ""
    if fg_value <= 30 and "bull" in regime:
        aligned = False
        alignment_note = "경고: 극도의 공포 상황에서 상승 레짐 감지. 신중한 판단 필요."
    elif fg_value >= 70 and "bear" in regime:
        aligned = False
        alignment_note = "경고: 극도의 탐욕 상황에서 하락 레짐 감지. 변동성 증가 예상."
    elif fg_value <= 30:
        alignment_note = "공포 구간. 역발상 매수 고려 가능."
    elif fg_value >= 70:
        alignment_note = "탐욕 구간. 차익 실현 고려 권장."
    else:
        alignment_note = "정상 범위. 기술적 분석 기반 판단."

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fear_greed": fear_greed,
        "regime_analysis": regime_analysis,
        "sentiment_regime_aligned": aligned,
        "alignment_note": alignment_note,
    }
    lines = [
        "[통합 시장 컨텍스트]",
        f"- Fear & Greed: {fg_value} ({fear_greed.get('classification', 'N/A')})",
        f"- 레짐: {regime_analysis.get('regime', 'N/A')} (점수: {regime_analysis.get('score', 0):.2f})",
        f"- 일치 여부: {'일치' if aligned else '불일치'}",
        f"- 판단: {alignment_note}",
    ]
    result["_llm_summary"] = "\n".join(lines)
    return result

# ---------------------------------------------------------------------------
# AI Summary
# ---------------------------------------------------------------------------

def _ai_summary_indicators(data: dict) -> str:
    fg = data.get("fear_greed", data)
    value = fg.get("value", "?")
    classification = fg.get("classification", "?")
    regime = data.get("regime_analysis", data.get("regime", {}))
    regime_label = regime.get("regime", "?") if isinstance(regime, dict) else "?"
    regime_score = regime.get("score", 0) if isinstance(regime, dict) else 0
    return f"시장지표: Fear&Greed={value}({classification}), 레짐={regime_label}(점수 {regime_score:.2f})."


# ---------------------------------------------------------------------------
# Resource 등록 함수
# ---------------------------------------------------------------------------

def register_indicator_resources(mcp, cache):
    """지표 관련 Resource 등록"""

    @mcp.resource("market://indicators/fear-greed")
    async def get_fear_greed() -> Dict[str, Any]:
        """[역할] Fear & Greed Index(시장 심리, 0-100). [호출 시점] 시장 심리 빠르게 파악 시. [선행 조건] 없음. [후속 추천] market://indicators/regime, market://indicators/context. [주의] Alternative.me API. TTL=300초. API 장애 시 캐시 사용.
        [출력 스키마] ai_summary 래핑. full_data: value(int:0-100), classification(str:Extreme Fear|Fear|Neutral|Greed|Extreme Greed), adjustment{threshold_adj,position_mult,strategy_hint}, interpretation(str), _llm_summary(str)."""
        cache_key = "fear_greed"
        cached = cache.get(cache_key, ttl=CACHE_TTL_FEAR_GREED)
        if cached:
            return to_resource_text(cached)
        data = await asyncio.to_thread(_load_fear_greed_index)
        data = wrap_with_ai_summary(data, "fear_greed", _ai_summary_indicators)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://indicators/regime")
    async def get_market_regime() -> Dict[str, Any]:
        """[역할] 4-Layer 시장 레짐(Short/Mid/Long/SuperLong 레이어별 레짐/점수/변동성). [호출 시점] 다중 시간프레임 레짐 분석 시. [선행 조건] 없음. [후속 추천] market://indicators/context, market://global/summary. [주의] MarketAnalyzer 의존.
        [출력 스키마] ai_summary 래핑. full_data: score(float), regime(str), volatility(float), raw_score(float), details{sl,long,mid,short}, _llm_summary(str)."""
        cache_key = "market_regime"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = await asyncio.to_thread(_load_market_regime_analysis)
        data = wrap_with_ai_summary(data, "market_regime", _ai_summary_indicators)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://indicators/context")
    async def get_market_context() -> Dict[str, Any]:
        """[역할] Fear&Greed + 4-Layer 레짐 결합 종합 시장 컨텍스트. [호출 시점] 시장 지표 한번에 확인 시. fear-greed+regime 2개 호출 대체. [선행 조건] 없음. [후속 추천] market://{market_id}/unified, market://all/summary. [주의] 한쪽 실패해도 나머지 반환.
        [출력 스키마] ai_summary 래핑. full_data: fear_greed{value,classification,adjustment{...}}, regime_analysis{score,regime,volatility,details{...}}, sentiment_regime_aligned(bool), alignment_note(str), _llm_summary(str)."""
        cache_key = "market_context"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = await asyncio.to_thread(_load_market_context)
        data = wrap_with_ai_summary(data, "market_context", _ai_summary_indicators)
        cache.set(cache_key, data)
        return to_resource_text(data)

    logger.info("  Indicator resources registered")
