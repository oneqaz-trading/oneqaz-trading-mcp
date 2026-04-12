# -*- coding: utf-8 -*-
"""
Derived Signals Resources
=========================
고부가가치 파생 시그널 5종 조회 Resource.

1. event_leading_scores   — 이벤트 선행 점수
2. regime_transition_probs — 레짐 전환 확률
3. cross_market_decoupling — 크로스마켓 디커플링 지수
4. news_reaction_speed     — 뉴스 반응 속도
5. strategy_fitness        — 전략 적합도 점수
"""

from __future__ import annotations

import asyncio
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from mcps.config import (
    CACHE_TTL_MARKET_STATUS,
    EXTERNAL_CONTEXT_DATA_DIR,
    PROJECT_ROOT,
    get_external_db_path,
)
from mcps.resources.resource_response import (
    build_resource_explanation,
    mcp_error,
    MCPErrorCode,
    MCPErrorAction,
    to_resource_text,
    with_explanation_contract,
    wrap_with_ai_summary,
)

logger = logging.getLogger("MarketMCP")


# ── 공통 헬퍼 ──

def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _fetch_all(conn: sqlite3.Connection, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def _get_news_db_path():
    return EXTERNAL_CONTEXT_DATA_DIR / "news" / "external_context.db"


def _get_agent_history_db_path(market_id: str):
    """agent_history DB 경로 (coin/kr_stock/us_stock)."""
    ah_map = {
        "crypto": "coin", "coin": "coin", "coin_market": "coin",
        "kr_stock": "kr_stock", "kr": "kr_stock", "kr_market": "kr_stock",
        "us_stock": "us_stock", "us": "us_stock", "us_market": "us_stock",
    }
    ah_id = ah_map.get(market_id.lower(), market_id.lower())
    return PROJECT_ROOT / "agent_history" / "data_storage" / ah_id / "agent_history.db"


# ═══════════════════════════════════════════════════════════════════
# 1. 이벤트 선행 점수
# ═══════════════════════════════════════════════════════════════════

def _load_event_leading_scores() -> Dict[str, Any]:
    """news DB에서 이벤트 선행 점수 조회."""
    db_path = _get_news_db_path()
    if not db_path.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            "news DB not found",
            action=MCPErrorAction.CHECK,
            scores=[],
        )

    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            if not _table_exists(conn, "event_leading_scores"):
                return {"scores": [], "note": "테이블 미생성 (첫 사이클 대기)"}

            scores = _fetch_all(conn, """
                SELECT event_type, market_id, news_type,
                       leading_score, avg_lead_time_minutes,
                       avg_anticipation_ratio, avg_precursor_score,
                       sample_count, accuracy_pct, computed_at
                FROM event_leading_scores
                ORDER BY leading_score DESC
            """)

            summary = _build_leading_score_summary(scores)
            result = {"scores": scores, "_llm_summary": summary}

            explanation = build_resource_explanation(
                market="global",
                entity_type="market",
                explanation_type="mcp_event_leading_scores",
                as_of_time=scores[0]["computed_at"] if scores else None,
                headline="이벤트 선행 점수 스냅샷",
                why_text=summary,
                bullet_points=[
                    f"분석 그룹={len(scores)}",
                    f"최고 선행 점수={scores[0]['leading_score']:.2f}" if scores else "데이터 없음",
                ],
            )
            return with_explanation_contract(result, resource_type="event_leading_scores", explanation=explanation)
    except Exception as e:
        logger.warning("이벤트 선행 점수 로드 실패: %s", e)
        return mcp_error(
            MCPErrorCode.NO_DATA,
            f"이벤트 선행 점수 로드 실패: {e}",
            action=MCPErrorAction.RETRY,
            action_value="30",
            scores=[],
        )


def _build_leading_score_summary(scores: List[Dict]) -> str:
    if not scores:
        return "[이벤트 선행 점수] 데이터 없음"
    lines = ["[이벤트 선행 점수]"]
    for s in scores[:5]:
        lines.append(
            f"- {s['event_type']}({s['news_type']}): "
            f"선행={s['leading_score']:.2f}, "
            f"평균선행시간={s.get('avg_lead_time_minutes', 0):.0f}분, "
            f"정확도={s.get('accuracy_pct', 0) or 0:.0%}"
        )
    if len(scores) > 5:
        lines.append(f"  ... 외 {len(scores) - 5}건")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
# 2. 레짐 전환 확률
# ═══════════════════════════════════════════════════════════════════

def _load_regime_transition_probs(market_id: str) -> Dict[str, Any]:
    """마켓별 레짐 전환 확률 조회."""
    ec_map = {
        "crypto": "coin_market", "coin": "coin_market", "coin_market": "coin_market",
        "kr_stock": "kr_market", "kr": "kr_market", "kr_market": "kr_market",
        "us_stock": "us_market", "us": "us_market", "us_market": "us_market",
    }
    ec_mid = ec_map.get(market_id.lower(), market_id.lower())
    db_path = EXTERNAL_CONTEXT_DATA_DIR / ec_mid / "external_context.db"

    if not db_path.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"DB not found for {ec_mid}",
            action=MCPErrorAction.CHECK,
            market_id=market_id,
            transitions=[],
        )

    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            if not _table_exists(conn, "regime_transition_probs"):
                return {"market_id": market_id, "transitions": [], "note": "테이블 미생성"}

            rows = _fetch_all(conn, """
                SELECT current_regime, next_regime, probability,
                       avg_duration_hours, cross_market_boost, confidence, computed_at
                FROM regime_transition_probs
                WHERE market_id = ?
                ORDER BY probability DESC
            """, (ec_mid,))

            current = rows[0]["current_regime"] if rows else None
            summary = _build_transition_summary(market_id, current, rows)
            result = {
                "market_id": market_id,
                "current_regime": current,
                "transitions": rows,
                "_llm_summary": summary,
            }

            explanation = build_resource_explanation(
                market=market_id,
                entity_type="market",
                explanation_type="mcp_regime_transition_probs",
                as_of_time=rows[0]["computed_at"] if rows else None,
                headline=f"{market_id} 레짐 전환 확률",
                why_text=summary,
                bullet_points=[
                    f"현재 레짐={current or '알수없음'}",
                    f"전환 경로={len(rows)}개",
                ],
            )
            return with_explanation_contract(result, resource_type="regime_transition_probs", explanation=explanation)
    except Exception as e:
        logger.warning("레짐 전환 확률 로드 실패 [%s]: %s", market_id, e)
        return mcp_error(
            MCPErrorCode.NO_DATA,
            f"레짐 전환 확률 로드 실패 [{market_id}]: {e}",
            action=MCPErrorAction.RETRY,
            action_value="30",
            market_id=market_id,
            transitions=[],
        )


def _build_transition_summary(market_id: str, current: str | None, rows: List[Dict]) -> str:
    if not rows:
        return f"[{market_id} 레짐 전환] 데이터 없음"
    lines = [f"[{market_id} 레짐 전환] 현재={current or '?'}"]
    for r in rows[:5]:
        boost = f", 크로스마켓+{r['cross_market_boost']:.2f}" if r.get("cross_market_boost") else ""
        lines.append(f"- → {r['next_regime']}: {r['probability']:.1%} (신뢰={r['confidence']:.2f}{boost})")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
# 3. 크로스마켓 디커플링 지수
# ═══════════════════════════════════════════════════════════════════

def _load_cross_market_decoupling() -> Dict[str, Any]:
    """news DB에서 디커플링 지수 조회."""
    db_path = _get_news_db_path()
    if not db_path.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            "news DB not found",
            action=MCPErrorAction.CHECK,
            pairs=[],
        )

    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            if not _table_exists(conn, "cross_market_decoupling"):
                return {"pairs": [], "note": "테이블 미생성"}

            pairs = _fetch_all(conn, """
                SELECT source_market, target_market, decoupling_index,
                       correlation_breakdown, regime_divergence,
                       timing_lag_divergence, computed_at
                FROM cross_market_decoupling
                ORDER BY decoupling_index DESC
            """)

            avg_idx = sum(p["decoupling_index"] for p in pairs) / len(pairs) if pairs else 0
            summary = _build_decoupling_summary(pairs, avg_idx)
            result = {
                "pairs": pairs,
                "overall_decoupling": round(avg_idx, 4),
                "_llm_summary": summary,
            }

            explanation = build_resource_explanation(
                market="global",
                entity_type="market",
                explanation_type="mcp_cross_market_decoupling",
                as_of_time=pairs[0]["computed_at"] if pairs else None,
                headline="크로스마켓 디커플링 스냅샷",
                why_text=summary,
                bullet_points=[
                    f"분석 페어={len(pairs)}",
                    f"평균 디커플링={avg_idx:.2f}",
                ],
            )
            return with_explanation_contract(result, resource_type="cross_market_decoupling", explanation=explanation)
    except Exception as e:
        logger.warning("디커플링 지수 로드 실패: %s", e)
        return mcp_error(
            MCPErrorCode.NO_DATA,
            f"디커플링 지수 로드 실패: {e}",
            action=MCPErrorAction.RETRY,
            action_value="30",
            pairs=[],
        )


def _build_decoupling_summary(pairs: List[Dict], avg: float) -> str:
    if not pairs:
        return "[디커플링 지수] 데이터 없음"
    level = "높음(따로움직임)" if avg > 0.6 else "보통" if avg > 0.3 else "낮음(동조)"
    lines = [f"[디커플링 지수] 전체 수준={level} ({avg:.2f})"]
    for p in pairs:
        lines.append(
            f"- {p['source_market']}↔{p['target_market']}: "
            f"{p['decoupling_index']:.2f} "
            f"(상관붕괴={p.get('correlation_breakdown', 0) or 0:.2f}, "
            f"레짐괴리={p.get('regime_divergence', 0) or 0:.2f})"
        )
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
# 4. 뉴스 반응 속도
# ═══════════════════════════════════════════════════════════════════

def _load_news_reaction_speed() -> Dict[str, Any]:
    """news DB에서 뉴스 반응 속도 조회."""
    db_path = _get_news_db_path()
    if not db_path.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            "news DB not found",
            action=MCPErrorAction.CHECK,
            speed_bands=[],
        )

    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            if not _table_exists(conn, "news_reaction_speed"):
                return {"speed_bands": [], "note": "테이블 미생성"}

            bands = _fetch_all(conn, """
                SELECT news_type, market_id, speed_band, count,
                       avg_reaction_score, avg_lag_minutes,
                       absorption_time_minutes, direction_accuracy, computed_at
                FROM news_reaction_speed
                ORDER BY news_type, speed_band
            """)

            summary = _build_reaction_speed_summary(bands)
            result = {"speed_bands": bands, "_llm_summary": summary}

            explanation = build_resource_explanation(
                market="global",
                entity_type="market",
                explanation_type="mcp_news_reaction_speed",
                as_of_time=bands[0]["computed_at"] if bands else None,
                headline="뉴스 반응 속도 분류",
                why_text=summary,
                bullet_points=[f"분류 그룹={len(bands)}"],
            )
            return with_explanation_contract(result, resource_type="news_reaction_speed", explanation=explanation)
    except Exception as e:
        logger.warning("뉴스 반응 속도 로드 실패: %s", e)
        return mcp_error(
            MCPErrorCode.NO_DATA,
            f"뉴스 반응 속도 로드 실패: {e}",
            action=MCPErrorAction.RETRY,
            action_value="30",
            speed_bands=[],
        )


def _build_reaction_speed_summary(bands: List[Dict]) -> str:
    if not bands:
        return "[뉴스 반응 속도] 데이터 없음"
    lines = ["[뉴스 반응 속도]"]
    for b in bands:
        acc = f", 방향정확도={b['direction_accuracy']:.0%}" if b.get("direction_accuracy") else ""
        lines.append(
            f"- {b['news_type']}/{b['speed_band']}: "
            f"평균래그={b.get('avg_lag_minutes', 0) or 0:.0f}분, "
            f"흡수={b.get('absorption_time_minutes', 0) or 0:.0f}분{acc} "
            f"(N={b['count']})"
        )
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
# 5. 전략 적합도 점수
# ═══════════════════════════════════════════════════════════════════

def _load_strategy_fitness(market_id: str) -> Dict[str, Any]:
    """agent_history DB에서 전략 적합도 점수 조회."""
    db_path = _get_agent_history_db_path(market_id)
    if not db_path.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"agent_history DB not found for {market_id}",
            action=MCPErrorAction.CHECK,
            market_id=market_id,
            fitness=[],
        )

    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            if not _table_exists(conn, "strategy_fitness"):
                return {"market_id": market_id, "fitness": [], "note": "테이블 미생성"}

            fitness = _fetch_all(conn, """
                SELECT regime, strategy_role, fitness_score,
                       historical_win_rate, historical_pf,
                       regime_match_score, transition_risk,
                       sample_count, confidence, recommended_action, computed_at
                FROM strategy_fitness
                WHERE market_id = ?
                ORDER BY fitness_score DESC
            """, (market_id.lower(),))

            # market_id 정규화 시도
            if not fitness:
                ah_map = {
                    "crypto": "coin", "coin_market": "coin",
                    "kr": "kr_stock", "kr_market": "kr_stock",
                    "us": "us_stock", "us_market": "us_stock",
                }
                alt = ah_map.get(market_id.lower())
                if alt:
                    fitness = _fetch_all(conn, """
                        SELECT regime, strategy_role, fitness_score,
                               historical_win_rate, historical_pf,
                               regime_match_score, transition_risk,
                               sample_count, confidence, recommended_action, computed_at
                        FROM strategy_fitness
                        WHERE market_id = ?
                        ORDER BY fitness_score DESC
                    """, (alt,))

            prefer = [f for f in fitness if f.get("recommended_action") == "prefer"]
            avoid = [f for f in fitness if f.get("recommended_action") == "avoid"]
            summary = _build_fitness_summary(market_id, fitness, prefer, avoid)
            result = {
                "market_id": market_id,
                "fitness": fitness,
                "prefer_count": len(prefer),
                "avoid_count": len(avoid),
                "_llm_summary": summary,
            }

            explanation = build_resource_explanation(
                market=market_id,
                entity_type="market",
                explanation_type="mcp_strategy_fitness",
                as_of_time=fitness[0]["computed_at"] if fitness else None,
                headline=f"{market_id} 전략 적합도",
                why_text=summary,
                bullet_points=[
                    f"전략={len(fitness)}개",
                    f"추천={len(prefer)}, 회피={len(avoid)}",
                ],
            )
            return with_explanation_contract(result, resource_type="strategy_fitness", explanation=explanation)
    except Exception as e:
        logger.warning("전략 적합도 로드 실패 [%s]: %s", market_id, e)
        return mcp_error(
            MCPErrorCode.NO_DATA,
            f"전략 적합도 로드 실패 [{market_id}]: {e}",
            action=MCPErrorAction.RETRY,
            action_value="30",
            market_id=market_id,
            fitness=[],
        )


def _build_fitness_summary(market_id: str, fitness: List[Dict], prefer: List, avoid: List) -> str:
    if not fitness:
        return f"[{market_id} 전략 적합도] 데이터 없음"
    lines = [f"[{market_id} 전략 적합도] 총 {len(fitness)}개 (추천={len(prefer)}, 회피={len(avoid)})"]
    for f in fitness[:6]:
        lines.append(
            f"- {f['regime']}/{f['strategy_role']}: "
            f"적합도={f['fitness_score']:.2f} ({f['recommended_action']}) "
            f"WR={f.get('historical_win_rate', 0) or 0:.0%}, "
            f"PF={f.get('historical_pf', 0) or 0:.2f}"
        )
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
# 통합: 5개 시그널 한번에
# ═══════════════════════════════════════════════════════════════════

def _load_all_derived_signals(market_id: str) -> Dict[str, Any]:
    """5개 파생 시그널 일괄 조회 (병렬)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError

    tasks = {
        "event_leading_scores": lambda: _load_event_leading_scores(),
        "regime_transition_probs": lambda: _load_regime_transition_probs(market_id),
        "cross_market_decoupling": lambda: _load_cross_market_decoupling(),
        "news_reaction_speed": lambda: _load_news_reaction_speed(),
        "strategy_fitness": lambda: _load_strategy_fitness(market_id),
    }

    result: Dict[str, Any] = {"market_id": market_id}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fn): key for key, fn in tasks.items()}
        try:
            for future in as_completed(futures, timeout=20):
                key = futures[future]
                try:
                    result[key] = future.result(timeout=5)
                except Exception as e:
                    logger.warning("파생 시그널 %s 로드 실패: %s", key, e)
                    result[key] = {"error": str(e)}
        except (TimeoutError, FuturesTimeoutError):
            logger.warning("파생 시그널 일괄 조회 20초 초과, 미완료 태스크 스킵")
    # as_completed timeout 시 미완료 태스크 에러 처리
    for future, key in futures.items():
        if key not in result:
            result[key] = {"error": "timeout"}

    # 통합 LLM 요약
    summaries = []
    for key in ("event_leading_scores", "regime_transition_probs", "cross_market_decoupling",
                "news_reaction_speed", "strategy_fitness"):
        s = result[key].get("_llm_summary", "")
        if s:
            summaries.append(s)
    result["_llm_summary"] = "\n\n".join(summaries)

    explanation = build_resource_explanation(
        market=market_id,
        entity_type="market",
        explanation_type="mcp_derived_signals_all",
        headline=f"{market_id} 파생 시그널 5종 통합",
        why_text=result["_llm_summary"][:500],
        bullet_points=[
            "이벤트선행/레짐전환/디커플링/반응속도/전략적합도",
        ],
    )
    return with_explanation_contract(result, resource_type="derived_signals_all", explanation=explanation)


# ═══════════════════════════════════════════════════════════════════
# AI Summary
# ═══════════════════════════════════════════════════════════════════

def _ai_summary_derived(data: dict) -> str:
    parts = []
    el = data.get("event_leading", {})
    if el and not el.get("error"):
        scores = el.get("scores", [])
        parts.append(f"선행점수 {len(scores)}건")
    rt = data.get("regime_transitions", {})
    if rt and not rt.get("error"):
        transitions = rt.get("transitions", [])
        parts.append(f"레짐전환 {len(transitions)}건")
    cd = data.get("cross_decoupling", {})
    if cd and not cd.get("error"):
        pairs = cd.get("pairs", [])
        parts.append(f"디커플링 {len(pairs)}쌍")
    rs = data.get("reaction_speed", {})
    if rs and not rs.get("error"):
        bands = rs.get("speed_bands", [])
        parts.append(f"반응속도 {len(bands)}건")
    sf = data.get("strategy_fitness", {})
    if sf and not sf.get("error"):
        scores = sf.get("scores", [])
        parts.append(f"전략적합도 {len(scores)}건")
    market_id = data.get("market_id", "")
    return f"{market_id} 파생시그널: {', '.join(parts) if parts else '데이터 없음'}."


# ═══════════════════════════════════════════════════════════════════
# Registration
# ═══════════════════════════════════════════════════════════════════

def register_derived_signals_resources(mcp, cache):
    """파생 시그널 Resource 등록."""

    @mcp.resource("market://derived/event-leading")
    async def get_event_leading_scores():
        """[역할] 이벤트 선행 점수(시장의 이벤트 선반영 정도). [호출 시점] 이벤트 기반 매매 전략 수립 시. [선행 조건] external/causality 권장. [후속 추천] market://derived/reaction-speed. [주의] 데이터 축적 필요. [출력 스키마] ai_summary 래핑. full_data: scores[{event_type,market_id,news_type,leading_score,avg_lead_time_minutes,avg_anticipation_ratio,accuracy_pct,sample_count}], _contract{...}, _llm_summary(str)."""
        cache_key = "derived_event_leading"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = await asyncio.to_thread(_load_event_leading_scores)
        data = wrap_with_ai_summary(data, "event_leading_scores", _ai_summary_derived)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/derived/regime-transitions")
    async def get_regime_transition_probs(market_id: str):
        """[역할] 레짐 전환 확률(현재→다른 레짐 전환 가능성). [호출 시점] 레짐 변화 사전 감지 시. [선행 조건] market://{market_id}/status 권장. [후속 추천] market://global/summary, strategy-fitness. [주의] regime_transition_probs 기반. [출력 스키마] ai_summary 래핑. full_data: market_id(str), current_regime(str), transitions[{current_regime,next_regime,probability,avg_duration_hours,cross_market_boost,confidence}], _contract{...}, _llm_summary(str)."""
        cache_key = f"derived_regime_trans_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = await asyncio.to_thread(_load_regime_transition_probs, market_id)
        data = wrap_with_ai_summary(data, "regime_transition_probs", _ai_summary_derived)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://derived/cross-decoupling")
    async def get_cross_market_decoupling():
        """[역할] 크로스마켓 디커플링 지수(상관 시장간 이탈 감지). [호출 시점] 교차시장 이상 징후 감지 시. [선행 조건] market://unified/cross-market 권장. [후속 추천] market://global/summary. [주의] 계산 데이터 없으면 빈 결과. [출력 스키마] ai_summary 래핑. full_data: pairs[{source_market,target_market,decoupling_index,correlation_breakdown,regime_divergence,timing_lag_divergence}], overall_decoupling(float), _contract{...}, _llm_summary(str)."""
        cache_key = "derived_cross_decoupling"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = await asyncio.to_thread(_load_cross_market_decoupling)
        data = wrap_with_ai_summary(data, "cross_market_decoupling", _ai_summary_derived)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://derived/reaction-speed")
    async def get_news_reaction_speed():
        """[역할] 뉴스 반응 속도(시장의 뉴스 반응 시간/정확도). [호출 시점] 뉴스 기반 매매 타이밍 최적화 시. [선행 조건] external/summary 권장. [후속 추천] market://derived/event-leading. [주의] news_reaction_speed 기반. [출력 스키마] ai_summary 래핑. full_data: speed_bands[{news_type,market_id,speed_band,count,avg_reaction_score,avg_lag_minutes,absorption_time_minutes,direction_accuracy}], _contract{...}, _llm_summary(str)."""
        cache_key = "derived_reaction_speed"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = await asyncio.to_thread(_load_news_reaction_speed)
        data = wrap_with_ai_summary(data, "news_reaction_speed", _ai_summary_derived)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/derived/strategy-fitness")
    async def get_strategy_fitness(market_id: str):
        """[역할] 전략 적합도 점수(현재 레짐에 각 전략의 적합도). [호출 시점] 전략 선택/전환 결정 시. [선행 조건] market://{market_id}/status, regime-transitions 권장. [후속 추천] get_strategy_distribution. [주의] agent_history 기반. 거래 이력 필요. [출력 스키마] ai_summary 래핑. full_data: market_id(str), fitness[{regime,strategy_role,fitness_score,historical_win_rate,regime_match_score,transition_risk,confidence,recommended_action}], prefer_count(int), avoid_count(int), _contract{...}, _llm_summary(str)."""
        cache_key = f"derived_strategy_fitness_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        data = await asyncio.to_thread(_load_strategy_fitness, market_id)
        data = wrap_with_ai_summary(data, "strategy_fitness", _ai_summary_derived)
        cache.set(cache_key, data)
        return to_resource_text(data)

    @mcp.resource("market://{market_id}/derived/all")
    async def get_all_derived_signals(market_id: str):
        """[역할] 5가지 파생 시그널을 한번에 반환. [호출 시점] 파생 시그널 전체 파악 시. 개별 5회 호출 대체. [선행 조건] market://{market_id}/status 권장. [후속 추천] 개별 Resource로 상세 drill-down. [주의] 순차 호출로 응답 느릴 수 있음. TTL=60초. [출력 스키마] ai_summary 래핑. full_data: market_id(str), event_leading_scores{scores[...]}, regime_transition_probs{current_regime,transitions[...]}, cross_market_decoupling{pairs[...],overall_decoupling}, news_reaction_speed{speed_bands[...]}, strategy_fitness{fitness[...]}, _contract{...}, _llm_summary(str)."""
        cache_key = f"derived_all_{market_id}"
        cached = cache.get(cache_key, ttl=CACHE_TTL_MARKET_STATUS)
        if cached:
            return to_resource_text(cached)
        try:
            data = await asyncio.wait_for(
                asyncio.to_thread(_load_all_derived_signals, market_id),
                timeout=30,
            )
        except asyncio.TimeoutError:
            logger.warning("파생 시그널 전체 조회 타임아웃 [%s]", market_id)
            data = mcp_error(
                MCPErrorCode.TIMEOUT,
                "파생 시그널 전체 조회 타임아웃",
                action=MCPErrorAction.RETRY,
                action_value="60",
                market_id=market_id,
                _llm_summary="파생 시그널 조회 타임아웃",
            )
        data = wrap_with_ai_summary(data, "derived_signals_all", _ai_summary_derived)
        cache.set(cache_key, data)
        return to_resource_text(data)

    logger.info("  🔮 Derived Signals resources registered")
