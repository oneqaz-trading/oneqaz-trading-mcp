# -*- coding: utf-8 -*-
"""
MCP Analytics Writer
====================
외부 API/MCP 요청을 PG `mcp_analytics.mcp_requests` 에 기록 (Wave I 이후 PG 전용).
Admin 대시보드(8020)에서 읽기 전용으로 조회.
"""

from __future__ import annotations

import atexit
import json
import os
import queue
import socket
import sys
import time
import threading
import logging

logger = logging.getLogger("MarketMCP")

# [2026-10-07] 스테이징(두 번째 MCP 인스턴스) 전용 — INSERT 를 실행한 뒤 ROLLBACK.
#   운영 사용 원장에 스테이징 요청이 섞이지 않게 하면서 행 조립·스키마 계약은 실물로 검증한다. 기본 OFF.
_DRYRUN = os.getenv("MCP_ANALYTICS_DRYRUN", "0") == "1"

# [2026-10-11] 비동기 기록 — 요청 경로는 큐에 넣기만 하고 전용 스레드가 PG 에 쓴다.
#   종전: async dispatch 안에서 전역 락 → 풀 getconn(기본 8s) → INSERT → commit 을 **동기**로 했다.
#   MCP 는 단일 프로세스라 그동안 이벤트 루프 전체가 멈췄다(평시 p50 3.3ms · 첫 호출 308ms ·
#   풀 고갈 시 8초 — mcp_server.log 연결 대기 실패 24건, 06-25~09-11). 실패는 경고로 삼켜 원장이
#   조용히 비었다. 이제 큐가 차거나 DB 가 실패하면 **버리되 세고**(드롭 카운터) 상태 행으로 남긴다.
#   ⚠️ PG 불가 시 파일 spool 은 두지 않는다 — CLAUDE.md "새 fallback 경로 추가 금지 · fail-loud".
_QUEUE_MAX = int(os.getenv("MCP_ANALYTICS_QUEUE_MAX", "10000"))
_BATCH_MAX = int(os.getenv("MCP_ANALYTICS_BATCH_MAX", "200"))
_FLUSH_SEC = float(os.getenv("MCP_ANALYTICS_FLUSH_SEC", "0.5"))
_HEARTBEAT_SEC = float(os.getenv("MCP_ANALYTICS_HEARTBEAT_SEC", "60"))
_WARN_INTERVAL_SEC = 300.0
_ATEXIT_FLUSH_SEC = 2.0

_INSERT_SQL = (
    "INSERT OR IGNORE INTO mcp_requests "
    "(ts, ip, request_type, name, success, response_ms, error_detail, source, error_code, rate_limited, "
    " user_agent, client_type, intent, session_key, host_origin, "
    " args_summary, client_name, client_version, mcp_session_id, session_seq, response_bytes, traffic_class, "
    " http_status, rpc_error_code, protocol_version, accept_header, client_id, request_bytes, tier, "
    " result_count, result_truncated, total_available, empty_result, envelope_error_code, "
    " principal_type, oauth_client_id, account_id, ref) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
    "        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

# 상태 행(프로세스당 1행) — admin 경보가 읽는다: 5분 넘게 갱신이 없으면 기록 스레드 정지·PG 장애,
# 최근 1시간 안에 드롭이 있으면 원장 결손. 카운터는 프로세스 기동 이후 누적(started_at 과 함께 읽을 것).
_HEARTBEAT_SQL = (
    "INSERT INTO analytics_writer_heartbeat "
    "(process_key, pid, started_at, updated_at, enqueued, written, dedup_skipped, "
    " dropped_queue_full, dropped_db_error, queue_depth, last_error, last_drop_at, last_write_at) "
    "VALUES (?, ?, to_timestamp(?), now(), ?, ?, ?, ?, ?, ?, ?, to_timestamp(?), to_timestamp(?)) "
    "ON CONFLICT (process_key) DO UPDATE SET "
    "pid = excluded.pid, started_at = excluded.started_at, updated_at = now(), "
    "enqueued = excluded.enqueued, written = excluded.written, dedup_skipped = excluded.dedup_skipped, "
    "dropped_queue_full = excluded.dropped_queue_full, dropped_db_error = excluded.dropped_db_error, "
    "queue_depth = excluded.queue_depth, last_error = excluded.last_error, "
    "last_drop_at = excluded.last_drop_at, last_write_at = excluded.last_write_at"
)


def _process_role() -> str:
    """역할 — MCP(run_mcp) / API(uvicorn api.main, 워커는 spawn 이 부모 argv 를 물려받는다) / 그 밖."""
    env_role = os.getenv("MCP_ANALYTICS_PROCESS_ROLE", "").strip()
    if env_role:
        return env_role[:20]
    argv = " ".join(sys.argv or [])
    if "run_mcp" in argv:
        return "mcp"
    if "api.main" in argv:
        return "api"
    entry = os.path.basename(sys.argv[0]) if sys.argv and sys.argv[0] else ""
    return (entry or "python")[:20]


def _process_key() -> str:
    """상태 행 키 = 역할:호스트명:pid — API 는 워커 4개가 각자 기록기를 가지므로 pid 로 갈라야
    서로의 카운터·마지막 드롭 시각을 덮어쓰지 않는다(덮어쓰면 드롭이 경보에서 사라진다)."""
    return f"{_process_role()}:{socket.gethostname()}:{os.getpid()}"[:120]


# 묶음 실패가 특정 행 탓일 수 없는 계열 — 연결(풀 대기 초과 PoolTimeout 은 OperationalError 하위)·
# 스키마 불일치. 이 계열에서 행마다 다시 쓰면 전부 같은 이유로 실패하고 시간만 쓴다.
_SYSTEMIC_ERRORS = frozenset({"OperationalError", "InterfaceError", "UndefinedColumn", "UndefinedTable"})


def _may_be_row_specific(e: BaseException) -> bool:
    return not ({c.__name__ for c in type(e).__mro__} & _SYSTEMIC_ERRORS)


def _rollback_quietly(conn) -> None:
    try:
        if conn is not None:
            conn.rollback()
    except Exception:  # noqa: BLE001 — 롤백 실패는 호출부의 드롭 집계로 이미 드러난다
        pass

# 스키마/마이그레이션은 shared/db/ddl/mcp_analytics.sql 가 단일 소스.

# args_summary 최대 길이. server._summarize_args 가 이 길이 안에서 **유효 JSON** 을 만든다.
_ARGS_SUMMARY_MAX = 1000


def api_key_client_id(api_key: str | None) -> str | None:
    """USAGE-001 client_id — API 키 sha256 앞 16자. **원문 저장 금지**.

    MCP(server.py)·REST(api/routers/data_api.py) 두 경로가 같은 함수를 쓴다 —
    해시 규칙이 갈리면 같은 키가 경로별로 다른 호출자로 집계된다.
    """
    if not api_key:
        return None
    import hashlib
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:16]

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
# [W2A T04-F5] 172.20.0.1 = docker 브리지 게이트웨이 — 호스트·LAN(Mac 대시보드 5분 주기 폴링) 발 호출이
# NAT 로 이 IP 로 도착해 8001(data_api) 원장에 external 로 적히던 것(7일 1,981행 = external 의 15.5%)의
# 차단. cloudflared 는 8010 전용이고 data_api 는 cf-connecting-ip 를 우선하므로 외부 수요 과소집계 위험 0.
_SELF_TRAFFIC_IPS = frozenset(
    p.strip() for p in _os.getenv("MCP_SELF_TRAFFIC_IPS", "").split(",") if p.strip()
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
    # [2026-10-15] 엔티티 컨텍스트 1차 묶음 — 미등재면 intent='other' 로 퍼널에서 사라진다
    "get_symbol_context",
    "get_group_context",
    "get_market_context",
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

    [2026-10-11] 비동기화 — log_request 는 행을 조립해 bounded 큐에 넣고 즉시 돌아온다.
    전용 데몬 스레드가 묶음(최대 _BATCH_MAX 행 · _FLUSH_SEC)으로 INSERT·commit 하고, 상태 행을
    _HEARTBEAT_SEC 마다 갱신한다. 큐 가득 참·DB 실패는 버리되 드롭 카운터에 센다(stats()).
    """

    def __init__(
        self,
        *,
        queue_max: int | None = None,
        batch_max: int | None = None,
        flush_sec: float | None = None,
        heartbeat_sec: float | None = None,
        connect=None,
        process_key: str | None = None,
    ):
        self._lock = threading.Lock()          # 카운터·스레드 기동 보호 — 요청 경로에선 짧게만 잡는다
        self._initialized = False
        self._queue: "queue.Queue[tuple]" = queue.Queue(maxsize=queue_max or _QUEUE_MAX)
        self._batch_max = batch_max or _BATCH_MAX
        self._flush_sec = _FLUSH_SEC if flush_sec is None else flush_sec
        self._heartbeat_sec = _HEARTBEAT_SEC if heartbeat_sec is None else heartbeat_sec
        self._connect_fn = connect
        self._process_key = process_key
        self._thread: threading.Thread | None = None
        self._started_at = time.time()
        self._stats = {
            "enqueued": 0, "written": 0, "dedup_skipped": 0,
            "dropped_queue_full": 0, "dropped_db_error": 0,
        }
        self._last_error: str | None = None
        self._last_drop_at: float | None = None
        self._last_write_at: float | None = None
        self._last_warn: dict[str, float] = {}

    # 하위 클래스(엔티티 호출 원장 기록기 EntityCallsWriter)가 바꾸는 표시용 값
    _thread_name = "mcp-analytics-writer"
    _schema_hint = ("migrations/mcp_requests_protocol_observability.sql · mcp_requests_principal.sql · "
                    "mcp_usage_billing_fields.sql 적용 여부 확인")

    def _ensure_db(self):
        """[Wave I] PG DDL 은 shared/db/ddl/mcp_analytics.sql 이 담당. no-op."""
        self._initialized = True

    def _connect(self):
        """PG mcp_analytics 스키마 직결 (단일 경로). 기록 스레드에서만 부른다."""
        if self._connect_fn is not None:
            return self._connect_fn()
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
        http_status: int | None = None,
        rpc_error_code: int | None = None,
        protocol_version: str | None = None,
        accept_header: str | None = None,
        client_id: str | None = None,
        request_bytes: int | None = None,
        tier: str | None = None,
        result_count: int | None = None,
        result_truncated: bool | None = None,
        total_available: int | None = None,
        empty_result: bool | None = None,
        envelope_error_code: str | None = None,
        principal_type: str | None = None,
        oauth_client_id: str | None = None,
        account_id: str | None = None,
        ref: str | None = None,
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

        [2026-10-06] 프로토콜 관측 확장 (Mac mini 요청서 R1~R4·R7, 전부 additive):
            - http_status / rpc_error_code: 실패의 1차 사실. 종전엔 HTTP 4xx 실패 행의
              error_code 가 99.6% NULL 이라 "왜 실패했나" 가 원장에 없었다.
            - protocol_version / accept_header: 406(Accept)·400(미지원 버전) 판별 근거.
            - client_id / tier / request_bytes: USAGE-001(2026-09-23) 과금 기준 3종.
              client_id 는 키 sha256 앞 16자 — **원문 저장 금지**.
            - result_count / result_truncated / total_available / empty_result:
              페이로드 계약(truncation_fields) 을 낸 tool 만 채운다. 나머지는 NULL
              (= 미측정. 0 이나 False 로 채우면 "빈 응답" 과 구분되지 않는다).
            - envelope_error_code: 성공(HTTP 200) 응답 안의 envelope 오류 코드.
              error_code 에도 종전대로 같은 값을 쓴다(기존 의미 유지).
            - name: 앞뒤 공백 제거 (실측 ' tools/list ' 2건).

        [2026-10-07 S1] 주체(principal) 칸 — OAuth 도입 전 **자리만** 만든다(동작 불변):
            - principal_type: 미지정이면 client_id(키 지문) 유무로 'api_key' / 'anonymous'. 새 수집 아님.
            - oauth_client_id / account_id: OAuth 이후에만 채운다(지금은 항상 NULL).
              ⚠️ oauth_client_id ≠ client_id — client_id 는 **API 키 지문**이다. 이름을 섞지 말 것.
              채우기 시작하는 순간 처리방침 갱신이 선행돼야 한다.

        [2026-10-16 req_mcp_onboarding OD6] ref — 연결 URL ?ref=<id> 를 고정 목록으로 정규화한 값
            (mcps/onboarding.resolve_ref: direct / 목록 id / other). MCP 경로만 채우고 REST(data_api)는 NULL.
            ⛔ 처리방침 고지 항목 — 목록 확장은 고지와 짝.
        """
        try:
            row = self._build_row(
                ip, request_type, name, success, response_ms, error_detail, source, error_code,
                rate_limited, user_agent, host_origin, args_summary, client_name, client_version,
                mcp_session_id, session_seq, response_bytes, http_status, rpc_error_code,
                protocol_version, accept_header, client_id, request_bytes, tier, result_count,
                result_truncated, total_available, empty_result, envelope_error_code,
                principal_type, oauth_client_id, account_id, ref=ref,
            )
        except Exception:  # 행 조립 실패 = 코드 결함 — 삼키지 않고 크게 남긴다(요청은 계속)
            logger.error("Analytics log_request 행 조립 실패(코드 결함)", exc_info=True)
            return
        self._ensure_started()
        try:
            self._queue.put_nowait(row)
        except queue.Full:
            with self._lock:
                self._stats["dropped_queue_full"] += 1
                self._last_drop_at = time.time()
            self._warn_throttled(
                "queue_full",
                "Analytics 큐 가득 참(%d) — 기록 1건 버림(누적 %d). 기록 스레드 정체·PG 지연 의심",
                self._queue.maxsize, self._stats["dropped_queue_full"],
            )
            return
        with self._lock:
            self._stats["enqueued"] += 1

    # ------------------------------------------------------------------
    # 행 조립 (요청 경로 — 순수 CPU, DB 접근 없음)
    # ------------------------------------------------------------------
    def _build_row(
        self, ip, request_type, name, success, response_ms, error_detail, source, error_code,
        rate_limited, user_agent, host_origin, args_summary, client_name, client_version,
        mcp_session_id, session_seq, response_bytes, http_status, rpc_error_code,
        protocol_version, accept_header, client_id, request_bytes, tier, result_count,
        result_truncated, total_available, empty_result, envelope_error_code,
        principal_type, oauth_client_id, account_id, ref=None,
    ) -> tuple:
        ts_now = int(time.time())               # 요청 시각 — 쓰기 시각이 아니다
        name = (name or "").strip() or "unparsed:empty_name"
        if principal_type is None:
            principal_type = "api_key" if client_id else "anonymous"
        # 자동 분류
        client_type = classify_client(user_agent)
        intent = classify_intent(request_type, name)
        session_key = make_session_key(ip, user_agent, ts_now)
        traffic_class = classify_traffic(ip, user_agent)
        return (
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
            # 직렬화 단계(_summarize_args)가 유효 JSON 을 보장하며 길이를 맞춘다.
            # 여기서 자르면 JSON 이 깨져 소비자(admin demand)가 행을 버린다.
            (args_summary or None) and args_summary[:_ARGS_SUMMARY_MAX],
            (client_name or None) and client_name[:120],
            (client_version or None) and client_version[:60],
            (mcp_session_id or None) and mcp_session_id[:80],
            session_seq,
            response_bytes,
            traffic_class,
            http_status,
            rpc_error_code,
            (protocol_version or None) and protocol_version[:40],
            (accept_header or None) and accept_header[:200],
            (client_id or None) and client_id[:16],
            request_bytes,
            (tier or None) and tier[:20],
            result_count,
            result_truncated,
            total_available,
            empty_result,
            (envelope_error_code or None) and envelope_error_code[:60],
            principal_type[:20],
            (oauth_client_id or None) and oauth_client_id[:300],
            (account_id or None) and str(account_id)[:64],
            (ref or None) and str(ref)[:16],
        )

    # ------------------------------------------------------------------
    # 기록 스레드
    # ------------------------------------------------------------------
    def start(self) -> None:
        """기록 스레드를 지금 띄운다(서버 기동 때 1회) — 첫 요청 전에도 상태 행이 돌아 admin 신선도
        프로브가 기동 직후부터 의미를 갖는다. 요청 경로는 첫 log_request 에서 알아서 띄운다."""
        self._ensure_started()

    def _ensure_started(self) -> None:
        t = self._thread
        if t is not None and t.is_alive():
            return
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            if self._thread is not None:   # 죽은 스레드 — 코드 결함이 있었다는 뜻이라 크게 남긴다
                logger.error("Analytics 기록 스레드가 죽어 있었다 — 재기동 (last_error=%s)", self._last_error)
            self._thread = threading.Thread(target=self._run, name=self._thread_name, daemon=True)
            self._thread.start()

    def _run(self) -> None:
        last_hb: float | None = None              # None = 아직 상태 행을 쓰지 않음(기동 직후 바로 1회 쓴다)
        while True:
            batch: list = []
            try:
                timeout = (0.05 if last_hb is None
                           else max(0.05, self._heartbeat_sec - (time.monotonic() - last_hb)))
                batch.append(self._queue.get(timeout=timeout))
                deadline = time.monotonic() + self._flush_sec
                while len(batch) < self._batch_max:
                    rem = deadline - time.monotonic()
                    if rem <= 0:
                        break
                    try:
                        batch.append(self._queue.get(timeout=rem))
                    except queue.Empty:
                        break
            except queue.Empty:
                pass
            try:
                if batch:
                    self._write_batch(batch)
                if last_hb is None or time.monotonic() - last_hb >= self._heartbeat_sec:
                    self._write_heartbeat()
                    last_hb = time.monotonic()
            except Exception:  # 여기까지 오면 코드 결함 — 스레드를 살려 두고 크게 남긴다
                logger.error("Analytics 기록 스레드 예외(코드 결함)", exc_info=True)
            finally:
                for _ in batch:
                    self._queue.task_done()

    def _insert_rows(self, conn, rows: list) -> int:
        """rows 를 한 tx 로 INSERT 하고 확정한다(dryrun 은 ROLLBACK). DO NOTHING 으로 합쳐진 행 수를 돌려준다."""
        skipped = 0
        for row in rows:
            cur = conn.execute(_INSERT_SQL, row)
            if getattr(cur, "rowcount", 1) == 0:
                skipped += 1                      # ux_mcp_req_natural 충돌 — DO NOTHING 으로 합쳐진 행
        if _DRYRUN:
            # 스테이징: 실제 스키마로 INSERT 를 실행해 컬럼·타입 계약은 검증하되 운영 원장엔 남기지 않는다.
            conn.rollback()
            for row in rows:
                logger.warning("[analytics dryrun] %s", json.dumps({
                    "ts": row[0], "request_type": row[2], "name": row[3], "success": row[4],
                    "error_code": row[8], "http_status": row[22], "rpc_error_code": row[23],
                    "client_name": row[16], "session_seq": row[19], "principal_type": row[34],
                    "traffic_class": row[21], "intent": row[12],
                }, ensure_ascii=False))
        else:
            conn.commit_strict() if hasattr(conn, "commit_strict") else conn.commit()
        return skipped

    def _write_batch(self, batch: list) -> None:
        conn = None
        written = skipped = dropped = 0
        err: Exception | None = None
        try:
            conn = self._connect()
            try:
                skipped = self._insert_rows(conn, batch)
                written = len(batch) - skipped
            except Exception as e:
                if len(batch) == 1 or not _may_be_row_specific(e):
                    raise
                # 독 행 격리 — 묶음 안 한 행(NUL 바이트·짝 없는 서로게이트·범위 초과 정수 등)이 tx 를
                # 깨면 같은 묶음의 정상 행까지 버리게 된다. 행마다 따로 다시 써서 손실을 그 행으로
                # 한정한다(종전 = 요청마다 별도 tx 와 같은 손실 범위). 연결·스키마 계열은 위에서 걸렀다.
                _rollback_quietly(conn)
                for row in batch:
                    try:
                        s = self._insert_rows(conn, [row])
                        written += 1 - s
                        skipped += s
                    except Exception as row_err:
                        _rollback_quietly(conn)
                        dropped += 1
                        err = row_err
        except Exception as e:  # 연결 실패 · 연결/스키마 계열 묶음 실패 · 1행 묶음 실패
            _rollback_quietly(conn)
            dropped, err = len(batch), e
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
        with self._lock:
            self._stats["written"] += written
            self._stats["dedup_skipped"] += skipped
            if written or skipped:
                self._last_write_at = time.time()
            if dropped:
                self._stats["dropped_db_error"] += dropped
                self._last_error = f"{type(err).__name__}: {err}"[:300]
                self._last_drop_at = time.time()
        if not dropped:
            return
        # 컬럼/테이블 부재 = 배포 순서 결함(마이그레이션 미적용 상태에서 코드 발효).
        if type(err).__name__ in ("UndefinedColumn", "UndefinedTable"):
            logger.error("Analytics 기록 스키마 불일치 — %d건 버림. %s: %s", dropped, self._schema_hint, err)
        else:
            self._warn_throttled(
                "db_error", "Analytics 기록 실패 — %d건 버림(누적 %d): %s",
                dropped, self._stats["dropped_db_error"], err,
            )

    def _write_heartbeat(self) -> None:
        snap = self.stats()
        conn = None
        try:
            conn = self._connect()
            conn.execute(_HEARTBEAT_SQL, (
                self._process_key or _process_key(), os.getpid(), self._started_at,
                snap["enqueued"], snap["written"], snap["dedup_skipped"],
                snap["dropped_queue_full"], snap["dropped_db_error"], snap["queue_depth"],
                snap["last_error"], snap["last_drop_at"], snap["last_write_at"],
            ))
            if _DRYRUN:
                conn.rollback()
            else:
                conn.commit_strict() if hasattr(conn, "commit_strict") else conn.commit()
        except Exception as e:
            _rollback_quietly(conn)
            self._warn_throttled("heartbeat", "Analytics 상태 행 갱신 실패(admin 경보가 정지로 볼 것): %s", e)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass

    def _warn_throttled(self, key: str, msg: str, *args) -> None:
        now = time.time()
        if now - self._last_warn.get(key, 0.0) >= _WARN_INTERVAL_SEC:
            self._last_warn[key] = now
            logger.warning(msg, *args)

    # ------------------------------------------------------------------
    # 관측·종료
    # ------------------------------------------------------------------
    def stats(self) -> dict:
        """기동 이후 누적 카운터 스냅샷 (+ 큐 깊이·마지막 오류·시각)."""
        with self._lock:
            out = dict(self._stats)
            out.update(
                queue_depth=self._queue.qsize(),
                last_error=self._last_error,
                last_drop_at=self._last_drop_at,
                last_write_at=self._last_write_at,
                started_at=self._started_at,
                thread_alive=bool(self._thread and self._thread.is_alive()),
            )
        return out

    def flush(self, timeout: float = 5.0) -> bool:
        """큐가 빌 때까지(진행 중 묶음 포함) 최대 timeout 초 기다린다. 비면 True."""
        if self._thread is None or not self._thread.is_alive():
            return self._queue.unfinished_tasks == 0
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._queue.unfinished_tasks == 0:
                return True
            time.sleep(0.02)
        return self._queue.unfinished_tasks == 0

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


def _atexit_flush() -> None:
    """정상 종료(uvicorn SIGTERM 처리 포함) 때 남은 큐를 짧게 비우고 상태 행을 한 번 더 쓴다."""
    try:
        if analytics_writer._thread is not None:
            analytics_writer.flush(_ATEXIT_FLUSH_SEC)
            analytics_writer._write_heartbeat()
    except Exception:  # noqa: BLE001 — 종료 경로: 남길 곳이 로그뿐이다
        logger.warning("Analytics 종료 flush 실패", exc_info=True)


atexit.register(_atexit_flush)
