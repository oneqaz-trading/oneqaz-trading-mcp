# -*- coding: utf-8 -*-
"""
글로벌 레짐 Resource
====================
원자재/국채/외환 등 마크로 레짐 데이터 제공

데이터 소스:
- global_regime_summary.json: 전체 요약
- *_analysis.db: 카테고리별 상세 분석
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from pathlib import Path

from mcps.config import (
    GLOBAL_REGIME_SUMMARY_JSON,
    ANALYSIS_DB_PATHS,
    CACHE_TTL_GLOBAL_REGIME,
    get_analysis_db_path,
)
from mcps.resources.resource_response import (
    to_resource_text,
    mcp_error,
    MCPErrorCode,
    MCPErrorAction,
    wrap_with_ai_summary,
)

logger = logging.getLogger("MarketMCP")

_RESOURCE_TIMEOUT = 30  # 개별 리소스 최대 처리 시간 (초)

# ---------------------------------------------------------------------------
# AI Summary
# ---------------------------------------------------------------------------

def _ai_summary_regime(data: dict) -> str:
    overall = data.get("overall", {})
    regime = overall.get("regime", "unknown")
    score = overall.get("score", 0)
    cats = data.get("categories", {})
    cat_parts = [f"{k}={v.get('regime_dominant', '?')}" for k, v in list(cats.items())[:4]]
    mtf = data.get("mtf_summary", {})
    aligned = mtf.get("aligned_count", 0)
    misaligned = mtf.get("misaligned_count", 0)
    return f"글로벌 레짐: {regime}(점수 {score:.2f}). 카테고리: {', '.join(cat_parts)}. MTF 일치/불일치: {aligned}/{misaligned}."

# ---------------------------------------------------------------------------
# 데이터 로더 함수
# ---------------------------------------------------------------------------

def _load_global_regime_summary() -> Dict[str, Any]:
    """global_regime_summary.json 로드"""
    try:
        if not GLOBAL_REGIME_SUMMARY_JSON.exists():
            logger.warning(f"Global regime summary not found: {GLOBAL_REGIME_SUMMARY_JSON}")
            return mcp_error(
                MCPErrorCode.DB_NOT_FOUND,
                "global_regime_summary.json not found",
                fallback_tool="market://global/categories",
            )

        with open(GLOBAL_REGIME_SUMMARY_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)

        # LLM을 위한 요약 텍스트 추가
        data["_llm_summary"] = _generate_regime_summary_text(data)
        return data

    except Exception as e:
        logger.error(f"Failed to load global regime summary: {e}")
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Failed to load global regime summary: {e}",
            fallback_tool="market://global/categories",
        )

def _generate_regime_summary_text(data: Dict[str, Any]) -> str:
    """LLM이 이해하기 쉬운 요약 텍스트 생성"""
    try:
        overall = data.get("overall", {})
        categories = data.get("categories", {})

        lines = [
            f"[글로벌 레짐 요약] 업데이트: {data.get('updated_at', 'N/A')}",
            f"- 전체 레짐: {overall.get('regime', 'N/A')} (점수: {overall.get('score', 0):.2f})",
        ]

        for cat, info in categories.items():
            regime = info.get("regime_dominant", "N/A")
            sentiment = info.get("sentiment_avg", 0)
            symbols = info.get("symbols", 0)
            lines.append(f"- {cat}: {regime}, 심리={sentiment:.2f}, 심볼수={symbols}")

        mtf = data.get("mtf_summary", {})
        if mtf:
            aligned = mtf.get("aligned_symbols", 0)
            misaligned = mtf.get("misaligned_symbols", 0)
            lines.append(f"- MTF 일치/불일치: {aligned}/{misaligned}")

        return "\n".join(lines)

    except Exception as e:
        return f"요약 생성 실패: {e}"

def _load_category_analysis(category: str, limit: int = 20) -> Dict[str, Any]:
    """카테고리별 분석 DB에서 최신 데이터 로드"""
    db_path = get_analysis_db_path(category)

    if not db_path or not db_path.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Analysis DB not found for category: {category}",
            action=MCPErrorAction.CHECK,
            action_value="market://global/categories",
            available_categories=list(ANALYSIS_DB_PATHS.keys()),
        )

    try:
        with sqlite3.connect(str(db_path), timeout=15) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=15000")
            conn.row_factory = sqlite3.Row

            # 최신 분석 데이터 조회
            query = """
                SELECT symbol, interval, timestamp,
                       regime_stage, regime_label,
                       sentiment, sentiment_label,
                       integrated_direction, volatility_level, risk_level,
                       regime_confidence, regime_transition_prob
                FROM analysis
                ORDER BY timestamp DESC
                LIMIT ?
            """
            cursor = conn.execute(query, (limit,))
            rows = [dict(row) for row in cursor.fetchall()]

            # 심볼별 그룹화
            by_symbol = {}
            for row in rows:
                symbol = row["symbol"]
                if symbol not in by_symbol:
                    by_symbol[symbol] = []
                by_symbol[symbol].append(row)

            # LLM용 요약 텍스트
            summary_text = _generate_category_summary_text(category, by_symbol)

            return {
                "category": category,
                "db_path": str(db_path),
                "total_rows": len(rows),
                "by_symbol": by_symbol,
                "_llm_summary": summary_text,
            }

    except Exception as e:
        logger.error(f"Failed to load {category} analysis: {e}")
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Failed to load {category} analysis: {e}",
            action=MCPErrorAction.CHECK,
            action_value="market://global/categories",
            category=category,
        )

def _generate_category_summary_text(category: str, by_symbol: Dict[str, List]) -> str:
    """카테고리별 LLM 요약 텍스트"""
    try:
        lines = [f"[{category.upper()} 분석 요약]"]

        for symbol, data_list in list(by_symbol.items())[:5]:  # 상위 5개 심볼
            if not data_list:
                continue
            latest = data_list[0]
            regime = latest.get("regime_label", "N/A")
            sentiment = latest.get("sentiment_label", "N/A")
            direction = latest.get("integrated_direction", "N/A")
            risk = latest.get("risk_level", "N/A")

            lines.append(f"- {symbol}: 레짐={regime}, 심리={sentiment}, 방향={direction}, 리스크={risk}")

        return "\n".join(lines)

    except Exception as e:
        return f"요약 생성 실패: {e}"

# ---------------------------------------------------------------------------
# Resource 등록 함수
# ---------------------------------------------------------------------------

def register_global_regime_resources(mcp, cache):
    """글로벌 레짐 관련 Resource 등록"""

    @mcp.resource("market://global/summary")
    async def get_global_regime_summary() -> Dict[str, Any]:
        """
        [역할] 원자재/국채/외환 등 전체 매크로 레짐 요약.
        [호출 시점] 시장 전체 방향성 파악 시 첫 번째로 호출.
        [선행 조건] 없음 (최상위 Resource).
        [후속 추천] market://global/category/{category}, market://{market_id}/unified.
        [주의] 캐시 TTL=300초.
        [출력 스키마] ai_summary 래핑. full_data: overall{regime,score}, categories{id→{regime_dominant,sentiment_avg,symbols}}, mtf_summary{aligned_symbols,misaligned_symbols}, _llm_summary(str).
        """
        cache_key = "global_regime_summary"
        cached = cache.get(cache_key, ttl=CACHE_TTL_GLOBAL_REGIME)
        if cached:
            logger.debug("Cache hit: global_regime_summary")
            return to_resource_text(cached)

        try:
            data = await asyncio.wait_for(
                asyncio.to_thread(_load_global_regime_summary),
                timeout=_RESOURCE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning("[Resource] _load_global_regime_summary timeout (%ds)", _RESOURCE_TIMEOUT)
            data = mcp_error(
                MCPErrorCode.TIMEOUT,
                f"Global regime summary timeout ({_RESOURCE_TIMEOUT}s)",
                action=MCPErrorAction.RETRY,
                action_value="30",
            )
            return to_resource_text(data)
        if not data.get("error"):
            data = wrap_with_ai_summary(data, "global_regime", _ai_summary_regime)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://global/category/{category}")
    async def get_category_analysis(category: str) -> Dict[str, Any]:
        """
        [역할] 특정 매크로 카테고리(bonds/forex/vix 등)의 심볼별 레짐/심리/방향.
        [호출 시점] global/summary에서 특정 카테고리 주목 시 drill-down.
        [선행 조건] market://global/summary 권장.
        [후속 추천] market://derived/cross-decoupling.
        [주의] category=bonds,commodities,forex,vix,credit,liquidity,inflation. TTL=300초.
        [출력 스키마] category(str), total_rows(int), by_symbol{symbol→[{regime_label,sentiment_label,integrated_direction,volatility_level,risk_level,regime_confidence}]}, _llm_summary(str).

        Args:
            category: Category name (bonds, commodities, forex, vix, credit, liquidity, inflation)
        """
        cache_key = f"category_analysis_{category}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_GLOBAL_REGIME)
        if cached:
            logger.debug(f"Cache hit: {cache_key}")
            return to_resource_text(cached)

        try:
            data = await asyncio.wait_for(
                asyncio.to_thread(_load_category_analysis, category),
                timeout=_RESOURCE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning("[Resource] _load_category_analysis(%s) timeout (%ds)", category, _RESOURCE_TIMEOUT)
            data = mcp_error(
                MCPErrorCode.TIMEOUT,
                f"Category analysis timeout ({_RESOURCE_TIMEOUT}s): {category}",
                action=MCPErrorAction.RETRY,
                action_value="30",
            )
            return to_resource_text(data)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://global/categories")
    def list_categories() -> Dict[str, Any]:
        """
        [역할] 사용 가능한 매크로 카테고리 목록과 DB 존재 여부.
        [호출 시점] 카테고리 확인 시.
        [선행 조건] 없음.
        [후속 추천] market://global/category/{category}.
        [주의] DB 존재 여부만 확인.
        [출력 스키마] categories[{id(str),db_path(str),exists(bool)}].
        """
        return to_resource_text({
            "categories": [
                {
                    "id": cat,
                    "db_path": str(db_path),
                    "exists": db_path.exists(),
                }
                for cat, db_path in ANALYSIS_DB_PATHS.items()
            ]
        })

    logger.info("  📊 Global Regime resources registered")
