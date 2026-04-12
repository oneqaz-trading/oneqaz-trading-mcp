# -*- coding: utf-8 -*-
"""
시장 구조 Resource
===================
ETF/바스켓/도미넌스 구조 분석 데이터 제공

데이터 소스:
- market_structure_summary.json: 시장별 구조 요약
- group_analysis.db: 그룹별 상세 분석
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional
from pathlib import Path

from mcps.config import (
    STRUCTURE_SUMMARY_PATHS,
    ANALYSIS_DB_PATHS,
    get_structure_summary_path,
    get_analysis_db_path,
)
from mcps.resources.resource_response import to_resource_text, mcp_error, MCPErrorCode, MCPErrorAction, wrap_with_ai_summary

logger = logging.getLogger("MarketMCP")

# 경량 파일 I/O 전용 스레드풀 — 시그널 DB 스캔 등 무거운 작업이
# 기본 스레드풀을 점유해도 구조 요약(JSON 20KB) 읽기가 차단되지 않도록 분리
_light_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="mkt_structure")


def _load_structure_summary(market_id: str) -> Dict[str, Any]:
    path = get_structure_summary_path(market_id)
    if not path or not path.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Structure summary not found for {market_id}",
            action=MCPErrorAction.FALLBACK,
            fallback_tool="market://structure/all",
            fallback_note="전체 구조 요약에서 해당 시장 확인",
        )
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        data["_llm_summary"] = _generate_structure_summary_text(data)
        return data
    except Exception as e:
        logger.error("Failed to load structure summary for %s: %s", market_id, e)
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            str(e),
            action=MCPErrorAction.RETRY,
            action_value="30",
        )


def _generate_structure_summary_text(data: Dict[str, Any]) -> str:
    try:
        market = data.get("market", "unknown")
        overall = data.get("overall", {})
        groups = data.get("groups", {})

        lines = [
            f"[{market} 시장 구조 요약] 업데이트: {data.get('updated_at', 'N/A')}",
            f"- 전체 구조: {overall.get('regime', 'N/A')} (점수: {overall.get('score', 0):.2f})",
        ]

        for gid, ginfo in groups.items():
            dominant = ginfo.get("regime_dominant", "N/A")
            avg = ginfo.get("regime_avg", 0)
            confidence = ginfo.get("confidence", 0)
            lines.append(
                f"- {gid}: {dominant}, avg={avg:.2f}, confidence={confidence:.2f}"
            )
            timing = ginfo.get("timing_signal", "")
            if timing:
                lines.append(f"  timing: {timing}")

        for extra_key, label in [
            ("concentration", "집중도"),
            ("index_spread", "지수확산"),
            ("dominance", "도미넌스"),
            ("breadth", "시장폭"),
            ("alt_strength", "알트강도"),
        ]:
            extra = data.get(extra_key)
            if isinstance(extra, dict):
                state = extra.get("state", "N/A")
                lines.append(f"- {label}: {state}")

        return "\n".join(lines)
    except Exception as e:
        return f"요약 생성 실패: {e}"


def _load_group_analysis(market_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    key = f"{market_id.replace('_stock', '_structure').replace('crypto', 'coin_structure')}"
    db_path = get_analysis_db_path(key)
    if not db_path or not db_path.exists():
        return []
    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT symbol, interval, timestamp,
                          regime_stage, regime_label, sentiment, sentiment_label,
                          integrated_direction
                   FROM analysis
                   ORDER BY timestamp DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("Group analysis query failed for %s: %s", market_id, e)
        return []


# ---------------------------------------------------------------------------
# AI Summary
# ---------------------------------------------------------------------------

def _ai_summary_structure(data: dict) -> str:
    market = data.get("market", "unknown")
    overall = data.get("overall", {})
    regime = overall.get("regime", "unknown")
    score = overall.get("score", 0)
    groups = data.get("groups", {})
    group_count = len(groups)
    top = list(groups.items())[:2]
    top_parts = [f"{k}({v.get('regime_dominant', '?')})" for k, v in top]
    return f"{market} 구조: 레짐={regime}(점수 {score:.2f}), 그룹 {group_count}개. 상위: {', '.join(top_parts)}."


# ---------------------------------------------------------------------------
# 공개 함수 (server.py에서 호출)
# ---------------------------------------------------------------------------

def read_market_structure(market_id: str) -> str:
    """시장 구조 요약 조회."""
    data = _load_structure_summary(market_id)
    if data.get("error"):
        return to_resource_text(data)
    data = wrap_with_ai_summary(data, "market_structure", _ai_summary_structure)
    return to_resource_text(data)


def read_market_structure_group(market_id: str, group_id: str) -> str:
    """특정 그룹의 구조 분석 상세."""
    data = _load_structure_summary(market_id)
    if data.get("error"):
        return to_resource_text(data)
    groups = data.get("groups", {})
    group_data = groups.get(group_id)
    if not group_data:
        return to_resource_text(mcp_error(
            MCPErrorCode.NO_DATA,
            f"Group '{group_id}' not found in {market_id}",
            action=MCPErrorAction.FALLBACK,
            fallback_tool=f"market://{market_id}/structure",
            fallback_note="시장 구조에서 유효한 group_id 목록 확인",
        ))
    group_data["market"] = market_id
    group_data["groups"] = {group_id: group_data}
    wrapped = wrap_with_ai_summary(group_data, "market_structure", _ai_summary_structure)
    return to_resource_text(wrapped)


def read_all_market_structures() -> str:
    """모든 시장의 구조 요약 조회."""
    result: Dict[str, Any] = {}
    for key in ("us_stock", "kr_stock", "crypto"):
        data = _load_structure_summary(key)
        if not data.get("error"):
            result[key] = {
                "overall": data.get("overall"),
                "groups": {
                    gid: {
                        "regime_dominant": g.get("regime_dominant"),
                        "regime_avg": g.get("regime_avg"),
                        "confidence": g.get("confidence"),
                        "timing_signal": g.get("timing_signal"),
                    }
                    for gid, g in data.get("groups", {}).items()
                },
            }
    return to_resource_text(result)


# ---------------------------------------------------------------------------
# Resource 등록
# ---------------------------------------------------------------------------

def register_market_structure_resources(mcp, cache):
    """시장 구조 Resource 등록"""

    @mcp.resource("market://structure/all")
    async def get_all_structures() -> str:
        """[역할] 모든 시장의 ETF/바스켓 구조 분석 요약. [호출 시점] 시장 구조 전체 파악 시. [선행 조건] 없음. [후속 추천] market://{market_id}/structure, market://global/summary. [주의] 구조 요약 JSON 없는 시장은 건너뜀. [출력 스키마] market_id→{overall{regime,score}, groups{group_id→{regime_dominant,regime_avg,confidence,timing_signal}}}."""
        cache_key = "all_market_structures"
        cached = cache.get(cache_key, ttl=60)
        if cached:
            return cached
        try:
            loop = asyncio.get_running_loop()
            result = await asyncio.wait_for(
                loop.run_in_executor(_light_executor, read_all_market_structures),
                timeout=30,
            )
        except asyncio.TimeoutError:
            logger.error("all_market_structures read timed out")
            result = to_resource_text(mcp_error(
                MCPErrorCode.TIMEOUT,
                "All market structures read timed out",
                action=MCPErrorAction.RETRY,
                action_value="30",
            ))
        cache.set(cache_key, result)
        return result

    @mcp.resource("market://{market_id}/structure")
    async def get_market_structure(market_id: str) -> str:
        """[역할] 특정 시장의 ETF/바스켓 그룹 구조 분석. [호출 시점] 시장 내부 섹터/그룹 분석 시. [선행 조건] market://structure/all 존재 확인 권장. [후속 추천] market://{market_id}/structure/group/{group_id}. [주의] market_id=us_stock,kr_stock,crypto. [출력 스키마] ai_summary 래핑. full_data: market(str), overall{regime,score}, groups{group_id→{regime_dominant,regime_avg,confidence}}, concentration/index_spread/dominance/breadth/alt_strength{state,...}, _llm_summary(str)."""
        cache_key = f"structure_{market_id}"
        cached = cache.get(cache_key, ttl=60)
        if cached:
            return cached
        try:
            loop = asyncio.get_running_loop()
            result = await asyncio.wait_for(
                loop.run_in_executor(_light_executor, read_market_structure, market_id),
                timeout=30,
            )
        except asyncio.TimeoutError:
            logger.error("market_structure read timed out for %s", market_id)
            result = to_resource_text(mcp_error(
                MCPErrorCode.TIMEOUT,
                f"Structure read timed out for {market_id}",
                action=MCPErrorAction.RETRY,
                action_value="30",
            ))
        cache.set(cache_key, result)
        return result

    @mcp.resource("market://{market_id}/structure/group/{group_id}")
    async def get_structure_group(market_id: str, group_id: str) -> str:
        """[역할] 특정 그룹의 상세 구조 분석(심볼별 레짐/점수/방향). [호출 시점] 특정 섹터/그룹 개별 종목 현황 시. [선행 조건] market://{market_id}/structure에서 group_id 확인. [후속 추천] get_signals(market_id, coin). [주의] group_analysis.db 기반. [출력 스키마] ai_summary 래핑. full_data: market(str), groups{group_id→{regime_dominant,regime_avg,confidence,...}}, _llm_summary(str)."""
        try:
            loop = asyncio.get_running_loop()
            return await asyncio.wait_for(
                loop.run_in_executor(_light_executor, read_market_structure_group, market_id, group_id),
                timeout=30,
            )
        except asyncio.TimeoutError:
            logger.error("structure_group read timed out for %s/%s", market_id, group_id)
            return to_resource_text(mcp_error(
                MCPErrorCode.TIMEOUT,
                f"Structure group read timed out for {market_id}/{group_id}",
                action=MCPErrorAction.RETRY,
                action_value="30",
            ))

    logger.info("  🏗️ Market Structure resources registered")
