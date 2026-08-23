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
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from pathlib import Path

from oneqaz_trading_mcp.config import (
    GLOBAL_REGIME_SUMMARY_JSON,
    ANALYSIS_DB_PATHS,
    CACHE_TTL_GLOBAL_REGIME,
    get_analysis_db_path,
    connect_readonly,
)
from oneqaz_trading_mcp.resources.resource_response import (
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


def _user_summary_regime(data: dict) -> str:
    """인간 사용자 1줄 — jargon-free 한국어."""
    overall = data.get("overall", {})
    regime = overall.get("regime", "unknown")
    regime_label = {
        "trending": "추세 형성",
        "ranging": "횡보",
        "volatile": "변동성 확대",
        "unknown": "판단 보류",
        "neutral": "중립",
    }.get(str(regime).lower(), str(regime))
    mtf = data.get("mtf_summary", {})
    aligned = mtf.get("aligned_count", 0)
    misaligned = mtf.get("misaligned_count", 0)
    total = aligned + misaligned
    align_msg = ""
    if total > 0:
        pct = round(aligned / total * 100)
        align_msg = f" 다중 시간대 신호 일치도는 {pct}%입니다."
    return f"현재 글로벌 시장은 '{regime_label}' 국면입니다.{align_msg}"

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

# [2026-06-12 근본수정] 매크로 카테고리(global.json 맵) — 4/17 PG 이관 후
# SQLite 분석 DB(와 PG 의 고아 analysis 테이블)는 writer 가 없어 4/16 에 동결.
# 살아있는 데이터는 market_global.candles 의 통합 컬럼(글로벌 레짐 파이프라인이
# 매 30분 갱신) — 매크로 카테고리는 그쪽을 직접 읽는다. 구조 카테고리
# (us/kr/coin_structure)는 기존 경로 유지.
_GLOBAL_CATEGORY_SYMBOLS: Dict[str, list] | None = None


def _category_symbols(category: str) -> list:
    """data_collection/symbols/global.json 의 카테고리→심볼 맵 (생산측과 동일 정본)."""
    global _GLOBAL_CATEGORY_SYMBOLS
    if _GLOBAL_CATEGORY_SYMBOLS is None:
        import json as _json
        from pathlib import Path as _P
        try:
            f = _P(__file__).resolve().parents[2] / "data_collection" / "symbols" / "global.json"
            data = _json.loads(f.read_text(encoding="utf-8"))
            _GLOBAL_CATEGORY_SYMBOLS = {
                k: v for k, v in data.items() if isinstance(v, list)
            }
        except Exception:
            _GLOBAL_CATEGORY_SYMBOLS = {}
    return _GLOBAL_CATEGORY_SYMBOLS.get(category.lower(), [])


# [2026-08-10] 구조 카테고리(us/kr/coin_structure)도 살아있는 PG 소스로 재배선.
# 기존 레거시 라우트(market_coin.group_analysis / market_{kr,us}.regime_group_analysis)는
# 매크로와 마찬가지로 4/17 PG 이관 때 writer 가 끊겨 4/16 동결된 고아인 데다
# volatility_level 등 4컬럼 부재로 레거시 SELECT 자체가 항상 실패했다
# (상시 db_not_found 실사고 — _update_log_2026-08-10c 6절). 살아있는 데이터는
# 구조 루프가 갱신하는 market_{m}_struct.candles 통합 컬럼 (PG 경로와 동일 컬럼 실측).
_STRUCT_CATEGORY_SCHEMAS = {
    "us_structure": "market_us_struct",
    "kr_structure": "market_kr_struct",
    "coin_structure": "market_coin_struct",
}


def _load_category_analysis_struct_pg(category: str, limit: int = 20) -> Dict[str, Any]:
    """market_{m}_struct.candles 통합 컬럼에서 구조 카테고리 최신 분석 로드 (살아있는 소스)."""
    schema = _STRUCT_CATEGORY_SCHEMAS[category.lower()]
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    with open_schema_connection(schema, readonly=True) as conn:
        cur = conn.execute(
            """
            SELECT * FROM (
                SELECT DISTINCT ON (symbol, "interval")
                       symbol, "interval", timestamp, close,
                       regime_stage, regime_label,
                       sentiment, sentiment_label,
                       integrated_direction, volatility_level, risk_level,
                       regime_confidence, regime_transition_prob
                FROM candles
                WHERE sentiment IS NOT NULL
                ORDER BY symbol, "interval", timestamp DESC
            ) t ORDER BY timestamp DESC LIMIT %s
            """,
            (int(limit),),
        )
        rows = [dict(r) for r in cur.fetchall()]

    by_symbol: Dict[str, list] = {}
    for row in rows:
        by_symbol.setdefault(row["symbol"], []).append(row)

    summary_text = _generate_category_summary_text(category, by_symbol)
    return {
        "_llm_summary": summary_text,
        "category": category,
        "total_rows": len(rows),
        "by_symbol": by_symbol,
        "source": f"pg:{schema}.candles",
    }


def _load_category_analysis_pg(category: str, limit: int = 20) -> Dict[str, Any]:
    """market_global.candles 통합 컬럼에서 카테고리 최신 분석 로드 (살아있는 소스)."""
    symbols = _category_symbols(category)
    if not symbols:
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"No symbols mapped for category: {category}",
            action=MCPErrorAction.CHECK,
            action_value="market://global/categories",
        )
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    with open_schema_connection("market_global", readonly=True) as conn:
        cur = conn.execute(
            """
            SELECT * FROM (
                SELECT DISTINCT ON (symbol, "interval")
                       symbol, "interval", timestamp, close,
                       regime_stage, regime_label,
                       sentiment, sentiment_label,
                       integrated_direction, volatility_level, risk_level,
                       regime_confidence, regime_transition_prob
                FROM candles
                WHERE symbol = ANY(%s) AND sentiment IS NOT NULL
                ORDER BY symbol, "interval", timestamp DESC
            ) t ORDER BY timestamp DESC LIMIT %s
            """,
            (list(symbols), int(limit)),
        )
        rows = [dict(r) for r in cur.fetchall()]

    by_symbol: Dict[str, list] = {}
    for row in rows:
        by_symbol.setdefault(row["symbol"], []).append(row)

    summary_text = _generate_category_summary_text(category, by_symbol)
    return {
        # [2026-08-10] _llm_summary 선두 배치 (키 순서 = JSON 직렬화 순서):
        # 소비자(ReactAgent 등)가 2000자에서 절단할 때 by_symbol 뒤의 요약이
        # 통째로 유실돼 재조회 루프를 유발하던 병목. 스키마 무변경.
        "_llm_summary": summary_text,
        "category": category,
        "total_rows": len(rows),
        "by_symbol": by_symbol,
        "source": "pg:market_global.candles",
    }


def _load_category_analysis(category: str, limit: int = 20) -> Dict[str, Any]:
    """카테고리별 분석 데이터 로드 — 매크로/구조 모두 살아있는 PG candles 소스."""
    if category.lower() in _STRUCT_CATEGORY_SCHEMAS:
        # 레거시 폴백 없음: 구조의 레거시 라우트 테이블은 동결 + 컬럼 부재로
        # 성공 가능성이 0 — degrade 대신 실제 원인으로 fail-loud.
        try:
            return _load_category_analysis_struct_pg(category, limit)
        except Exception as e:
            logger.error(f"Failed to load {category} structure analysis: {e}")
            return mcp_error(
                MCPErrorCode.DB_NOT_FOUND,
                f"Failed to load {category} analysis: {e}",
                action=MCPErrorAction.CHECK,
                action_value="market://global/categories",
                category=category,
            )
    if _category_symbols(category):
        try:
            return _load_category_analysis_pg(category, limit)
        except Exception as e:
            # PG 실패 시 기존 경로로 degrade (아래 — 단 4/16 동결 데이터임을 명심)
            import logging as _logging
            _logging.getLogger(__name__).warning(
                "[category] PG 로드 실패, 레거시 폴백: %s", e)
    db_path = get_analysis_db_path(category)

    if not db_path:  # [2026-07-03] PG 논리 키 — 파일 존재 검사 제거
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Analysis DB not found for category: {category}",
            action=MCPErrorAction.CHECK,
            action_value="market://global/categories",
            available_categories=list(ANALYSIS_DB_PATHS.keys()),
        )

    try:
        # Read-only URI: writer(analysis pipeline)와 lock contention 없이 동시 read
        with connect_readonly(db_path, timeout=15) as conn:

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
                # [2026-08-10] _llm_summary 선두 배치 — PG 경로와 동일한 이유
                "_llm_summary": summary_text,
                "category": category,
                "total_rows": len(rows),
                "by_symbol": by_symbol,
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
            # [2026-06-12] 실제 현재가 명시 — 에이전트가 낡은 가격(4월 VIX 12.8 등)을
            # 대화 히스토리에서 echo 하던 것을 실값 제공으로 차단
            close = latest.get("close")
            price_str = f", 현재가={float(close):,.2f}" if close is not None else ""

            lines.append(f"- {symbol}: 레짐={regime}, 심리={sentiment}, 방향={direction}, 리스크={risk}{price_str}")

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
            data = wrap_with_ai_summary(data, "global_regime", _ai_summary_regime, _user_summary_regime)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://global/category/{category}")
    async def get_category_analysis(category: str) -> Dict[str, Any]:
        """
        [역할] 특정 매크로 카테고리(bonds/forex/vix 등)의 심볼별 레짐/심리/방향.
        [호출 시점] global/summary에서 특정 카테고리 주목 시 drill-down.
        [선행 조건] market://global/summary 권장.
        [후속 추천] market://derived/cross-decoupling.
        [주의] category=bonds,commodities,forex,vix,credit,liquidity,inflation + 구조 3종(us_structure,kr_structure,coin_structure). TTL=300초.
        [출력 스키마] category(str), total_rows(int), by_symbol{symbol→[{regime_label,sentiment_label,integrated_direction,volatility_level,risk_level,regime_confidence}]}, _llm_summary(str).

        Args:
            category: Category name (bonds, commodities, forex, vix, credit, liquidity, inflation, us_structure, kr_structure, coin_structure)
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
                    "exists": db_path.exists(),
                }
                for cat, db_path in ANALYSIS_DB_PATHS.items()
            ]
        })

    logger.info("  📊 Global Regime resources registered")
