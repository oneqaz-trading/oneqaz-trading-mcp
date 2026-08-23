# -*- coding: utf-8 -*-
"""
MCP Analytics Writer
====================
외부 API/MCP 요청을 PG `mcp_analytics.mcp_requests` 에 기록 (Wave I 이후 PG 전용).
Admin 대시보드(8020)에서 읽기 전용으로 조회.
"""

from __future__ import annotations

import time
import threading
import logging

logger = logging.getLogger("MarketMCP")

# 스키마/마이그레이션은 shared/db/ddl/mcp_analytics.sql 가 단일 소스.

# ---------------------------------------------------------------------------
# Client/intent 분류 helpers (Phase 1)
# ---------------------------------------------------------------------------

def classify_client(user_agent: str | None) -> str:
    """User-Agent 헤더를 정규화된 client_type으로 분류."""
    if not user_agent:
        return "unknown"
    ua = user_agent.lower()
    # AI 클라이언트
    if "claude" in ua and "code" in ua:
        return "claude-code"
    if "claude" in ua:
        return "claude-desktop"
    if "chatgpt" in ua or "openai" in ua or "gpt-" in ua:
        return "chatgpt"
    if "perplexity" in ua or "perplexitybot" in ua or "comet" in ua:
        return "perplexity"
    if "cursor" in ua:
        return "cursor"
    if "windsurf" in ua:
        return "windsurf"
    if "atlas" in ua:
        return "atlas"
    if "gemini" in ua or "googleagent" in ua:
        return "gemini"
    if "copilot" in ua:
        return "copilot"
    # Dev tools
    if "mcp-publisher" in ua:
        return "mcp-publisher"
    if "mcp-inspector" in ua or "inspector" in ua:
        return "mcp-inspector"
    if "curl" in ua:
        return "curl"
    if "httpx" in ua or "requests" in ua or "urllib" in ua or "aiohttp" in ua:
        return "python-sdk"
    if "node-fetch" in ua or "axios" in ua:
        return "node-sdk"
    if "postman" in ua:
        return "postman"
    if "go-http-client" in ua:
        return "go-sdk"
    # Generic browser fallback (Chrome/Firefox/Safari) — likely manual testing or embed
    if "mozilla" in ua or "chrome" in ua or "safari" in ua or "firefox" in ua:
        return "browser"
    return "other"


# ---------------------------------------------------------------------------
# Traffic class (2026-07-08) — 봇·자체 트래픽이 KPI(trust-funnel, 인기도)를
# 오염시키는 문제의 근본 해소. 30일 실측: 단일 자체 폴러가 trust_eval 을 66배
# 부풀렸고, 레지스트리 크롤러 10곳+ 이 핸드셰이크 수만 건을 만든다.
# 소비는 admin 사람 리포트 전용 (엔진 자동 피드백 금지 — CLAUDE.md 규칙 7).
# ---------------------------------------------------------------------------

import os as _os

# 운영자 자체 모니터링 회선. env 로 교체/확장 가능 (comma-separated).
_SELF_TRAFFIC_IPS = frozenset(
    p.strip() for p in _os.getenv("MCP_SELF_TRAFFIC_IPS", "210.94.83.103").split(",") if p.strip()
)

# MCP 레지스트리/스코어링/보안감사 크롤러 UA 마커 (30일 실측 상위 + 일반 패턴)
_CRAWLER_UA_MARKERS = (
    "sentineloracle", "yellowmcp", "mcpscoringengine", "agent-tools",
    "aisec", "prsm-mcp", "agenstrybot", "mcp-catalog", "censys",
    "doppelops", "rugpull", "chiark", "mseep",
    "crawler", "spider", "healthcheck", "health-check", "scanner",
    "registry", "monitor", "uptime", "probe",
)


def classify_traffic(ip: str, user_agent: str | None) -> str:
    """호출을 self(자체)/crawler(생태계 봇)/external(진짜 외부 소비자)로 분류.

    external 만이 수요 KPI 의 분모가 된다. 판별 불가 시 external (보수적).
    """
    if ip in _SELF_TRAFFIC_IPS:
        return "self"
    if user_agent:
        ua = user_agent.lower()
        if any(m in ua for m in _CRAWLER_UA_MARKERS):
            return "crawler"
    return "external"


# Trust 검증 7-step 시퀀스 (instructions가 시키는 것)
_TRUST_EVAL_TOOLS = {
    "get_prediction_accuracy",
    "get_backtest_tuning_state",
    "get_monthly_accuracy_trend",
    "get_news_leading_indicator_performance",
    "get_news_causality_breakdown",
    "get_feature_governance_state",
    "get_structure_calibration",
    "get_structure_validation_history",
    "get_active_predictions",
    # [2026-07-08] 예측 원장 검증 도구 — 신뢰 검증 시퀀스의 새 관문
    "get_resolved_predictions",
    "get_ledger_integrity",
}

_DECISION_SUPPORT_TOOLS = {
    "explain_decision",
    "get_macro_influence_map",
    "get_strategy_leaderboard",
    "get_cross_market_correlation",
}

_DATA_QUERY_TOOLS = {
    "get_signals",
    "get_signal_detail",
    "get_role_analysis",
    "get_positions",
    "get_position_detail",
    "get_profitable_positions",
    "get_losing_positions",
    "get_strategy_distribution",
    "get_trade_history",
    "analyze_trades",
    "get_winning_trades",
    "get_losing_trades",
    "get_latest_decisions",
    "get_llm_trading_decisions",
    # [2026-07-08] 파이프라인 소비자용 벌크 export
    "get_trade_outcomes_bulk",
}

_DISCOVERY_METHODS = {
    "initialize",
    "tools/list",
    "resources/list",
    "prompts/list",
    "ping",
}


def classify_intent(request_type: str, name: str) -> str:
    """request_type/name으로 호출 의도 분류.

    Returns:
        - "trust_eval"       : 신뢰 검증 (B2AI funnel)
        - "decision_support" : 결정 지원 (explain, recommendation)
        - "data_query"       : 실제 사용 (signals, positions, trades)
        - "discovery"        : 발견 단계 (initialize, list)
        - "resource_read"    : MCP resource 읽기 (uri 기반)
        - "other"            : 기타
    """
    if not name:
        return "other"
    # request_type='mcp' 인 경우 method 자체 (initialize 등)
    if request_type == "mcp" and name in _DISCOVERY_METHODS:
        return "discovery"
    # tool 호출
    if request_type == "tool":
        # [2026-07-08] ChatGPT 커넥터 표준 search/fetch — 카탈로그/코퍼스 발견 단계.
        # (_DISCOVERY_METHODS 는 request_type=='mcp' 경로라 tool 경로에서 별도 처리)
        if name in ("search", "fetch"):
            return "discovery"
        if name in _TRUST_EVAL_TOOLS:
            return "trust_eval"
        if name in _DECISION_SUPPORT_TOOLS:
            return "decision_support"
        if name in _DATA_QUERY_TOOLS:
            return "data_query"
        return "other"
    # resource 읽기
    if request_type == "resource":
        return "resource_read"
    return "other"


def make_session_key(ip: str, user_agent: str | None, ts: int, bucket_seconds: int = 1800) -> str:
    """같은 IP + same UA + 같은 시간 버킷 = 같은 세션.

    bucket_seconds=1800 → 30분 단위 버킷.
    완벽한 session ID는 아니지만 80% 정확도로 funnel 분석 가능.
    """
    import hashlib
    bucket = ts // bucket_seconds
    key = f"{ip}|{user_agent or ''}|{bucket}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:12]

# ---------------------------------------------------------------------------
# AnalyticsWriter
# ---------------------------------------------------------------------------

class AnalyticsWriter:
    """Thread-safe analytics logger (PG mcp_analytics 스키마 직결).

    [2026-07-06] AWS EC2 배포 제거 완료 — SQLite write fallback
    (_AWS_SQLITE_PATH / _is_aws_deploy / _connect_sqlite) 일괄 삭제.
    api.oneqaz.com 은 홈 PG 직결 단일 경로.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._initialized = False

    def _ensure_db(self):
        """[Wave I] PG DDL 은 shared/db/ddl/mcp_analytics.sql 이 담당. no-op."""
        self._initialized = True

    def _connect(self):
        """PG mcp_analytics 스키마 직결 (단일 경로)."""
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
        return open_schema_connection("mcp_analytics")

    def log_request(
        self,
        ip: str,
        request_type: str,
        name: str,
        success: bool,
        response_ms: int,
        error_detail: str | None = None,
        source: str = "mcp",
        error_code: str | None = None,
        rate_limited: bool = False,
        user_agent: str | None = None,
        host_origin: str = "home",
        args_summary: str | None = None,
        client_name: str | None = None,
        client_version: str | None = None,
        mcp_session_id: str | None = None,
        session_seq: int | None = None,
        response_bytes: int | None = None,
    ):
        """기록 1건 INSERT. fire-and-forget 용도.

        Phase 1 추가:
            - user_agent: 원본 헤더 (저장)
            - client_type: 자동 분류 (claude/chatgpt/cursor/...)
            - intent: tool 이름 기반 자동 분류 (trust_eval/decision_support/...)
            - session_key: ip+ua+30분 버킷 hash
            - host_origin: 'home' 또는 'aws'

        [2026-07-08] B2AI 수요 원장 확장 (소급 생성 불가 필드):
            - args_summary: tools/call arguments 화이트리스트 요약 JSON 문자열
              (심볼/시장/기간만 — 원문 저장 금지, privacy 페이지에 명문화)
            - client_name/client_version: initialize params.clientInfo
            - mcp_session_id/session_seq: 프로토콜 세션 + 세션 내 호출 순서
            - response_bytes: 응답 크기 (과금 단위 설계 근거)
            - traffic_class: self/crawler/external 자동 분류 (KPI 오염 제거)
        """
        with self._lock:
            try:
                self._ensure_db()
                ts_now = int(time.time())
                # 자동 분류
                client_type = classify_client(user_agent)
                intent = classify_intent(request_type, name)
                session_key = make_session_key(ip, user_agent, ts_now)
                traffic_class = classify_traffic(ip, user_agent)

                conn = self._connect()
                try:
                    # INSERT OR IGNORE: ux_mcp_req_natural 유니크 인덱스 충돌 시 조용히 스킵.
                    # PG 는 shared/db/compat.py shim 이 ON CONFLICT DO NOTHING 으로 변환,
                    # SQLite 는 네이티브. (중복키 IntegrityError 로그오염 근본수정 2026-06-21)
                    conn.execute(
                        "INSERT OR IGNORE INTO mcp_requests "
                        "(ts, ip, request_type, name, success, response_ms, error_detail, source, error_code, rate_limited, "
                        " user_agent, client_type, intent, session_key, host_origin, "
                        " args_summary, client_name, client_version, mcp_session_id, session_seq, response_bytes, traffic_class) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            ts_now,
                            ip,
                            request_type,
                            name,
                            bool(success),
                            response_ms,
                            error_detail,
                            source,
                            error_code,
                            bool(rate_limited),
                            (user_agent or "")[:300],  # truncate to 300 chars
                            client_type,
                            intent,
                            session_key,
                            host_origin,
                            (args_summary or None) and args_summary[:600],
                            (client_name or None) and client_name[:120],
                            (client_version or None) and client_version[:60],
                            (mcp_session_id or None) and mcp_session_id[:80],
                            session_seq,
                            response_bytes,
                            traffic_class,
                        ),
                    )
                    conn.commit()
                finally:
                    conn.close()
            except Exception as e:
                logger.warning("Analytics log_request failed: %s", e)

    def get_stats(self) -> dict:
        """Admin 대시보드용 요약 통계 (PG / SQLite 둘 다 지원)."""
        try:
            conn = self._connect()
            today_start = int(time.time()) - (int(time.time()) % 86400)
            try:
                row = conn.execute(
                    "SELECT COUNT(*) as total, COUNT(DISTINCT ip) as active_ips "
                    "FROM mcp_requests WHERE ts >= ?",
                    (today_start,),
                ).fetchone()
            finally:
                conn.close()
            if not row:
                return {"total_today": 0, "active_ips": 0}
            if isinstance(row, sqlite_row_or_dict := row):
                # both PG dict-like and sqlite3.Row support indexing
                try:
                    return {"total_today": row["total"], "active_ips": row["active_ips"]}
                except (KeyError, IndexError, TypeError):
                    pass
            return {"total_today": row[0], "active_ips": row[1]}
        except Exception:
            return {"total_today": 0, "active_ips": 0}


# Singleton
analytics_writer = AnalyticsWriter()
