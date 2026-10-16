# -*- coding: utf-8 -*-
"""
엔티티 컨텍스트 호출 원장 — `mcp_analytics.mcp_entity_calls` (req_mcp_entity_context 6.8 · 제안서 ED8·ED9)
========================================================================================================
엔티티 tool 호출 1건 = 1행: 진입 키 · resolver 결과 · 섹션별 반환 수 · gap 이유 · traffic_class ·
`question`(선택 파라미터) · question_type(결정론 키워드 분류).

**IP·session_key 는 저장하지 않는다**(ED8). traffic_class 만 남긴다. `mcp_requests` 의 IP 12개월 원장과
Mac mini 소비 경로에 질문 원문이 묶이지 않게 하려고 별도 표에 둔다(진단 §6-4).

스위치 2개 — **둘 다 기본 off** (2026-10-15 사용자 지시 "저장은 꺼 둔 상태로"):
  MCP_ENTITY_CALLS_LOG    = on → 호출 행 기록 (질문 원문 없이)
  MCP_ENTITY_QUESTION_LOG = on → question 원문(≤500자)·question_type 도 기록. 처리방침 개정 게시 뒤에만 켠다(ED9).
                                 CALLS_LOG 가 off 면 이 스위치도 효과 없음.
보존: question 원문은 30일 뒤 NULL 로 바꾼다(question_type 은 남는다). 기록 스레드가 6시간마다 확인한다
     — 별도 프로세스·잡이 아니다.

사용 금지 경로: 이 표를 학습·RAG·agent_history·엔진 피드백에 읽어 들이지 않는다(CLAUDE.md 금지항목 7 과
같은 이유 — 외부 호출자 사용 로그는 사람이 읽는 로드맵 입력이다). 권한도 그렇게 잘라 둔다(engine_rw·
collector_rw·mac_ro SELECT REVOKE — migrations/mcp_entity_calls.sql).

질문 텍스트는 응답에 영향을 주지 않는다(req E7) — 이 모듈은 응답이 만들어진 **뒤**에 요약만 받는다.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import re
import time
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.analytics import _DRYRUN, AnalyticsWriter

logger = logging.getLogger("MarketMCP")

ENV_CALLS = "MCP_ENTITY_CALLS_LOG"
ENV_QUESTION = "MCP_ENTITY_QUESTION_LOG"
QUESTION_MAX_CHARS = 500
INPUT_MAX_CHARS = 100
QUESTION_TTL_DAYS = 30
PURGE_EVERY_SEC = 6 * 3600

_ON = ("1", "on", "true", "yes")


def calls_enabled() -> bool:
    return os.getenv(ENV_CALLS, "off").strip().lower() in _ON


def question_enabled() -> bool:
    return calls_enabled() and os.getenv(ENV_QUESTION, "off").strip().lower() in _ON


# ─── 질문 유형 (결정론 키워드 — LLM 금지, req §3) ─────────────────────────
# 우선순위 순. 앞에서 걸리면 끝. 목적은 "에이전트가 어떤 질문을 들고 오는가" 의 분포이지 의도 해석이 아니다.
_QUESTION_RULES = (
    ("action_request", ("살까", "팔까", "사도 될", "팔아야", "매수", "매도", "들어가도", "손절", "익절", "목표가",
                        "should i", "buy", "sell", "entry", "exit", "target price")),
    ("why_moved", ("왜", "이유", "원인", "급등", "급락", "떨어", "올랐", "오른", "빠진", "내린",
                   "why", "reason", "cause", "drop", "fell", "surge", "jump", "rally", "plunge", "sell-off")),
    ("linkage", ("관련", "영향", "연관", "같이", "연결", "상관", "따라", "엮",
                 "related", "affect", "impact", "correlat", "link", "driven", "exposure", "spillover")),
    ("news", ("뉴스", "공시", "소식", "발표", "실적", "news", "announcement", "headline", "earnings")),
    ("outlook", ("전망", "앞으로", "향후", "예상", "될까", "오를까", "내릴까",
                 "outlook", "forecast", "will ", "next week", "expect")),
    ("state", ("어때", "상황", "분위기", "국면", "레짐", "지금", "how is", "how's", "status", "regime", "mood", "doing")),
)


def classify_question(text: Optional[str]) -> Optional[str]:
    if not text or not str(text).strip():
        return None
    t = str(text).lower()
    for label, words in _QUESTION_RULES:
        for w in words:
            if re.fullmatch(r"[a-z\- ']+", w):
                if re.search(rf"(?<![a-z]){re.escape(w.strip())}(?![a-z])", t):
                    return label
            elif w in t:
                return label
    return "other"


# ─── 요청 경로: 응답 요약 → request scope 에 맡긴다 (기록은 미들웨어가 IP 규칙과 함께) ─────────

def build_entry(tool: str, response: Dict[str, Any], *, input_value: Any, market_param: Optional[str],
                limit: Any, question: Optional[str]) -> Optional[Dict[str, Any]]:
    """원장 행에 들어갈 요약. 기록이 꺼져 있으면 None — 질문 원문을 어디에도 남기지 않는다."""
    if not calls_enabled():
        return None
    if not isinstance(response, dict) or response.get("error") is True:
        return None
    res = response.get("resolution") or {}
    ent = response.get("entity") or {}
    sections = ((response.get("meta") or {}).get("sections")) or {}
    links_returned = sum(int((v or {}).get("returned") or 0) for v in sections.values())
    links_total = sum(int((v or {}).get("total") or 0) for v in sections.values())
    q = str(question).strip() if question is not None else ""
    try:
        lim = int(limit)
    except (TypeError, ValueError):
        lim = None
    entry: Dict[str, Any] = {
        "tool": tool,
        "input_value": str(input_value or "")[:INPUT_MAX_CHARS],
        "market_param": (str(market_param)[:20] if market_param else None),
        "limit_param": lim,
        "resolution": res.get("status") or "unknown",
        "matched_by": res.get("matched_by"),
        "coverage": res.get("coverage"),
        "entity_type": ent.get("type"),
        "entity_market": ent.get("market"),
        "entity_id": (str(ent.get("id"))[:INPUT_MAX_CHARS] if ent.get("id") else None),
        "candidates": len(res.get("candidates") or []) if res.get("status") == "ambiguous" else None,
        "links_returned": links_returned,
        "links_total": links_total,
        "sections": sections,
        "gap_reasons": [f"{g.get('section')}:{g.get('reason')}" for g in (response.get("gaps") or [])],
        "question_provided": bool(q),
        "question": None,
        "question_type": None,
    }
    if q and question_enabled():
        entry["question"] = q[:QUESTION_MAX_CHARS]
        entry["question_type"] = classify_question(q)
    return entry


def stash_entry(entry: Optional[Dict[str, Any]]) -> None:
    """현재 HTTP 요청 scope 에 맡긴다 — 미들웨어가 mcp_requests 와 같은 IP 규칙(127.0.0.1·신뢰 대역 제외)으로
    기록한다. HTTP 컨텍스트 밖(테스트·내부 직접 호출)에서는 아무것도 하지 않는다."""
    if not entry:
        return
    try:
        from fastmcp.server.dependencies import get_http_request
        req = get_http_request()
        if req is None:
            return
        obs = req.scope.setdefault("state", {}).setdefault("mcp_envelope_obs", {})
        obs["entity_call"] = entry
    except Exception:  # HTTP 컨텍스트 없음 — 관측 전용 경로라 응답을 깨지 않는다 (명시적 사유)
        pass


# ─── 기록 스레드 (AnalyticsWriter 의 큐·묶음·드롭 계수 재사용) ─────────────────

_INSERT_SQL = (
    "INSERT INTO mcp_entity_calls "
    "(ts, tool, input_value, market_param, limit_param, resolution, matched_by, coverage, entity_type, "
    " entity_market, entity_id, candidates, links_returned, links_total, sections, gap_reasons, "
    " traffic_class, question_provided, question, question_type) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, ?, ?)"
)
_PURGE_SQL = (
    "UPDATE mcp_entity_calls SET question = NULL, question_purged_at = ? "
    "WHERE question IS NOT NULL AND ts < ?"
)


class EntityCallsWriter(AnalyticsWriter):
    """`mcp_entity_calls` 비동기 기록기. 상태 행(heartbeat)은 쓰지 않는다 — 그 자리에서 질문 원문 보존기한을 처리한다."""

    _schema_hint = "migrations/mcp_entity_calls.sql 적용 여부 확인"
    _thread_name = "mcp-entity-calls-writer"

    def __init__(self, **kw):
        kw.setdefault("heartbeat_sec", 300.0)
        super().__init__(**kw)
        self._last_purge = 0.0

    def log_entry(self, entry: Dict[str, Any], *, ts: int, traffic_class: str) -> None:
        if not entry:
            return
        try:
            row = (
                int(ts), entry["tool"], entry.get("input_value"), entry.get("market_param"),
                entry.get("limit_param"), entry.get("resolution"), entry.get("matched_by"),
                entry.get("coverage"), entry.get("entity_type"), entry.get("entity_market"),
                entry.get("entity_id"), entry.get("candidates"), entry.get("links_returned"),
                entry.get("links_total"), json.dumps(entry.get("sections") or {}, ensure_ascii=False),
                json.dumps(entry.get("gap_reasons") or [], ensure_ascii=False), (traffic_class or "")[:20],
                bool(entry.get("question_provided")), entry.get("question"), entry.get("question_type"),
            )
        except Exception:  # 행 조립 실패 = 코드 결함 — 크게 남기고 요청은 계속
            logger.error("entity calls 행 조립 실패(코드 결함)", exc_info=True)
            return
        self._ensure_started()
        try:
            self._queue.put_nowait(row)
        except queue.Full:
            with self._lock:
                self._stats["dropped_queue_full"] += 1
                self._last_drop_at = time.time()
            self._warn_throttled("queue_full", "entity calls 큐 가득 참 — 1건 버림(누적 %d)",
                                 self._stats["dropped_queue_full"])
            return
        with self._lock:
            self._stats["enqueued"] += 1

    def _insert_rows(self, conn, rows: List[tuple]) -> int:
        for row in rows:
            conn.execute(_INSERT_SQL, row)
        if _DRYRUN:  # 스테이징 인스턴스 — 스키마 계약만 검증하고 운영 원장엔 남기지 않는다(analytics 와 같은 규칙)
            conn.rollback()
        else:
            conn.commit_strict() if hasattr(conn, "commit_strict") else conn.commit()
        return 0

    def _write_heartbeat(self) -> None:
        """상태 행 대신 질문 원문 보존기한 처리 (PURGE_EVERY_SEC 마다)."""
        now = time.time()
        if now - self._last_purge < PURGE_EVERY_SEC:
            return
        self._last_purge = now
        conn = None
        try:
            conn = self._connect()
            cur = conn.execute(_PURGE_SQL, (int(now), int(now) - QUESTION_TTL_DAYS * 86400))
            if _DRYRUN:
                conn.rollback()
            else:
                conn.commit_strict() if hasattr(conn, "commit_strict") else conn.commit()
            n = getattr(cur, "rowcount", 0) or 0
            if n:
                logger.info("entity calls: 보존기한(%d일) 지난 question 원문 %d건 삭제(NULL)", QUESTION_TTL_DAYS, n)
        except Exception as e:
            try:
                if conn is not None:
                    conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            # 보존 약속 위반 가능성 — 경고가 아니라 오류로 남긴다
            logger.error("entity calls: question 보존기한 처리 실패: %s", e, exc_info=True)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass


entity_calls_writer = EntityCallsWriter(process_key="mcp-entity-calls")
