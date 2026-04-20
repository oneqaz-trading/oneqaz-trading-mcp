# -*- coding: utf-8 -*-
"""MCP Analytics Writer (SQLite, local-only).

Logs each MCP tool/resource call to a local SQLite DB so operators can audit
usage patterns. This is a standalone port of the internal module without the
monorepo's PostgreSQL adapter — SQLite is the default for self-hosted users.

The internal OneQAZ deployment uses the Postgres variant in the private
monorepo; this module ships the public, simpler variant.
"""

from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
import threading
import time
from pathlib import Path

logger = logging.getLogger("MarketMCP")

# ---------------------------------------------------------------------------
# DB path (override via MCP_ANALYTICS_DB env var)
# ---------------------------------------------------------------------------

_DEFAULT_DIR = Path(__file__).resolve().parent / "data_storage"
DB_PATH = os.environ.get(
    "MCP_ANALYTICS_DB",
    str(_DEFAULT_DIR / "mcp_analytics.db"),
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS mcp_requests (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           INTEGER NOT NULL,
    ip           TEXT    NOT NULL,
    request_type TEXT    NOT NULL,
    name         TEXT    NOT NULL,
    success      INTEGER NOT NULL DEFAULT 1,
    response_ms  INTEGER NOT NULL DEFAULT 0,
    error_detail TEXT,
    source       TEXT    NOT NULL DEFAULT 'mcp',
    error_code   TEXT,
    rate_limited INTEGER NOT NULL DEFAULT 0,
    user_agent   TEXT,
    client_type  TEXT,
    intent       TEXT,
    session_key  TEXT,
    host_origin  TEXT    NOT NULL DEFAULT 'home'
);

CREATE INDEX IF NOT EXISTS idx_mcp_req_ts     ON mcp_requests(ts);
CREATE INDEX IF NOT EXISTS idx_mcp_req_ip     ON mcp_requests(ip);
CREATE INDEX IF NOT EXISTS idx_mcp_req_name   ON mcp_requests(name);
CREATE INDEX IF NOT EXISTS idx_mcp_req_source ON mcp_requests(source);
"""


# ---------------------------------------------------------------------------
# Client / intent classifiers
# ---------------------------------------------------------------------------


def classify_client(user_agent: str | None) -> str:
    """Normalize a User-Agent header into a client_type bucket."""
    if not user_agent:
        return "unknown"
    ua = user_agent.lower()
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
    if "mozilla" in ua or "chrome" in ua or "safari" in ua or "firefox" in ua:
        return "browser"
    return "other"


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
}

_DISCOVERY_METHODS = {
    "initialize",
    "tools/list",
    "resources/list",
    "prompts/list",
    "ping",
}


def classify_intent(request_type: str, name: str) -> str:
    """Classify a call into: trust_eval / decision_support / data_query / discovery / resource_read / other."""
    if not name:
        return "other"
    if request_type == "mcp" and name in _DISCOVERY_METHODS:
        return "discovery"
    if request_type == "tool":
        if name in _TRUST_EVAL_TOOLS:
            return "trust_eval"
        if name in _DECISION_SUPPORT_TOOLS:
            return "decision_support"
        if name in _DATA_QUERY_TOOLS:
            return "data_query"
        return "other"
    if request_type == "resource":
        return "resource_read"
    return "other"


def make_session_key(
    ip: str,
    user_agent: str | None,
    ts: int,
    bucket_seconds: int = 1800,
) -> str:
    """Derive a 30-minute session bucket hash from ip + UA + timestamp."""
    bucket = ts // bucket_seconds
    key = f"{ip}|{user_agent or ''}|{bucket}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


class AnalyticsWriter:
    """Thread-safe analytics logger writing to a local SQLite file (WAL mode)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._initialized = False

    def _ensure_db(self):
        if self._initialized:
            return
        db_path = Path(DB_PATH)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.executescript(_SCHEMA)
            conn.commit()
        finally:
            conn.close()
        self._initialized = True

    def _connect(self):
        conn = sqlite3.connect(str(DB_PATH), timeout=5.0)
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

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
    ):
        """Fire-and-forget request logger."""
        with self._lock:
            try:
                self._ensure_db()
                ts_now = int(time.time())
                client_type = classify_client(user_agent)
                intent = classify_intent(request_type, name)
                session_key = make_session_key(ip, user_agent, ts_now)
                conn = self._connect()
                try:
                    conn.execute(
                        "INSERT INTO mcp_requests "
                        "(ts, ip, request_type, name, success, response_ms, error_detail, "
                        " source, error_code, rate_limited, user_agent, client_type, "
                        " intent, session_key, host_origin) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            ts_now,
                            ip,
                            request_type,
                            name,
                            1 if success else 0,
                            response_ms,
                            error_detail,
                            source,
                            error_code,
                            1 if rate_limited else 0,
                            (user_agent or "")[:300],
                            client_type,
                            intent,
                            session_key,
                            host_origin,
                        ),
                    )
                    conn.commit()
                finally:
                    conn.close()
            except Exception as e:
                logger.warning("Analytics log_request failed: %s", e)

    def get_stats(self) -> dict:
        """Today's aggregate stats for admin dashboards."""
        try:
            self._ensure_db()
            conn = self._connect()
            today_start = int(time.time()) - (int(time.time()) % 86400)
            try:
                row = conn.execute(
                    "SELECT COUNT(*), COUNT(DISTINCT ip) FROM mcp_requests WHERE ts >= ?",
                    (today_start,),
                ).fetchone()
            finally:
                conn.close()
            if not row:
                return {"total_today": 0, "active_ips": 0}
            return {"total_today": row[0], "active_ips": row[1]}
        except Exception:
            return {"total_today": 0, "active_ips": 0}


analytics_writer = AnalyticsWriter()
