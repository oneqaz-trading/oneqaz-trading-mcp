"""SQLite ↔ PostgreSQL 호환성 shim.

기존 admin / 기타 모듈이 sqlite3 API 를 직접 사용하던 코드를 최소 수정으로
PG 로 포팅하기 위한 어댑터.

제공 기능:
1. ``?`` → ``%s`` placeholder 자동 변환 (따옴표 안 ``?`` 는 보존).
2. ``ON CONFLICT(x) DO UPDATE SET y=excluded.y`` → PG 호환 (이미 PG 문법이므로 통과).
3. ``INSERT OR REPLACE INTO t (...)`` → PG ``INSERT ... ON CONFLICT (pk) DO UPDATE``.
4. sqlite3.Row 스타일 dict-like row access (psycopg 의 dict_row 로 대체).
5. ``conn.execute()`` / ``conn.executemany()`` / ``conn.commit()`` / ``conn.rollback()`` /
   ``conn.close()`` API 동일성.
6. ``cur.lastrowid`` — PG 는 RETURNING id 필요하므로 INSERT 자동 변환.
7. ``PRAGMA ...`` 호출은 PG 모드에서 no-op.

제한:
- 완전한 방언 번역기가 아님. 복잡한 SQLite 특수 문법은 별도 수정 필요.
- admin 리팩토링 범위에서 발견된 패턴만 다룬다. 다른 모듈 이관 시 확장.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# ============================================================
# SQL 변환
# ============================================================

# 따옴표 밖 ``?`` 만 매칭 (단순하지만 admin 용 SQL 에는 충분)
_QMARK_OUTSIDE_QUOTES = re.compile(
    r"""(?x)
    (?P<quoted>'[^']*'|"[^"]*")   # 따옴표 안
    |
    (?P<qmark>\?)                 # 실제 placeholder
    """
)

# 따옴표 밖 pyformat ``%s`` 매칭 (혼합 paramstyle 가드용).
# ``%%`` (escape) 와 ``%(name)s`` (named) 는 placeholder 가 아니므로 제외.
_PYFORMAT_OUTSIDE_QUOTES = re.compile(
    r"""(?x)
    (?P<quoted>'[^']*'|"[^"]*")   # 따옴표 안
    |
    (?P<escaped>%%)               # %% (literal %, 매치만 하고 무시)
    |
    (?P<named>%\([^)]+\)s)        # %(name)s — named param (매치만 하고 무시)
    |
    (?P<pos>%s)                   # %s — positional placeholder
    """
)


def _has_qmark_outside_quotes(sql: str) -> bool:
    """따옴표 밖에 실제 ``?`` placeholder 가 있는지 검사."""
    for m in _QMARK_OUTSIDE_QUOTES.finditer(sql):
        if m.group("qmark") is not None:
            return True
    return False


def _has_pyformat_outside_quotes(sql: str) -> bool:
    """따옴표 밖에 실제 ``%s`` (positional pyformat) placeholder 가 있는지 검사."""
    for m in _PYFORMAT_OUTSIDE_QUOTES.finditer(sql):
        if m.group("pos") is not None:
            return True
    return False


def convert_qmark_to_pyformat(sql: str) -> str:
    """SQLite ``?`` ↔ PG ``%s`` paramstyle 정규화.

    두 가지 호출 스타일을 모두 수용한다:

    1. ``?`` placeholder 사용 (SQLite 스타일 caller):
       - 모든 ``%`` 를 ``%%`` 로 이스케이프 (LIKE ``'%foo%'`` 같은 리터럴 보호).
       - ``?`` → ``%s`` 치환 (따옴표 밖에서만).
    2. ``%s`` placeholder 사용 (PG-native caller, Phase 5+ 신규 모듈):
       - SQL 을 그대로 통과시킨다. caller 가 ``%`` 이스케이프 책임을 짐.
       - 이전 구현은 ``sql.replace("%", "%%")`` 로 무조건 이스케이프했기 때문에
         이미 PG-native 로 작성된 ``%s`` 가 ``%%s`` 로 망가져
         "the query has 0 placeholders but N parameters were passed" 에러를 유발했다.

    분기 기준은 "따옴표 밖에 ``?`` 가 한 번이라도 있는가" 이며,
    placeholder 가 전혀 없는 정적 SQL 도 PG-native 분기를 타서 안전하게 통과한다.

    혼합 paramstyle 가드:
        같은 SQL 안에 ``?`` 와 ``%s`` 가 동시에 있으면 ``ValueError`` 로 즉시 실패.
        (예전엔 silent 하게 ``?`` 분기를 타서 ``%s`` 의 ``%`` 도 ``%%`` 로 escape →
         psycopg 가 placeholder 개수 mismatch 로 뒤늦게 cryptic 에러를 냈다.)
    """
    if not _has_qmark_outside_quotes(sql):
        # PG-native (또는 placeholder 없음) — 그대로 반환.
        return sql

    # ``?`` 분기에서 ``%s`` 가 섞여 있으면 caller 버그 — 즉시 표면화.
    if _has_pyformat_outside_quotes(sql):
        raise ValueError(
            "Mixed paramstyle in SQL: both '?' and '%s' placeholders detected. "
            "Pick one — either use '?' everywhere (shim converts to %s) or use "
            "'%s' everywhere (shim passthrough; caller must escape literal % as %%).\n"
            f"SQL: {sql.strip()[:300]}"
        )

    # SQLite-style ``?`` 사용 — 기존 변환 적용.
    # 1) 모든 % 를 %% 로 이스케이프 (psycopg pyformat 파서가 quote 무시)
    sql = sql.replace("%", "%%")

    # 2) ? → %s (따옴표 밖에서만)
    def _replace(m: re.Match) -> str:
        if m.group("quoted") is not None:
            return m.group("quoted")
        return "%s"

    return _QMARK_OUTSIDE_QUOTES.sub(_replace, sql)


# INSERT OR REPLACE → PG upsert 변환 (단순 케이스)
_INSERT_OR_REPLACE_RE = re.compile(
    r"^\s*INSERT\s+OR\s+REPLACE\s+INTO\s+([A-Za-z_][A-Za-z_0-9]*)\s*\(([^)]+)\)",
    re.IGNORECASE,
)

# INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING (단순 케이스)
_INSERT_OR_IGNORE_RE = re.compile(
    r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\s+([A-Za-z_][A-Za-z_0-9]*)",
    re.IGNORECASE,
)

# PK 가 자동 증가 id 가 아닌 테이블 (RETURNING id 불가).
# admin + llm_factory 스키마 기준.
_PK_NOT_ID: frozenset[str] = frozenset({
    # admin 스키마
    "settings",              # PK(key)
    "admin_settings",        # PK(key)
    "_schema_version",       # PK(version, source_db)
    "external_daily_stats",  # PK(date)
    "health_daily_stats",    # PK(date)
    "ops_daily_stats",       # PK(date)
    "perf_daily_stats",      # PK(date, market)
    # llm_factory 스키마
    "runs",                  # PK(run_id)
    "agent_credibility",     # PK(agent_name)
    "debates",               # PK(debate_id)
    "snapshots",             # PK(snapshot_id)
    "llm_explanations",      # PK(snapshot_id)
    "llm_trading_decisions", # PK(market_id, symbol) — [Wave G]
    # api 스키마 (Phase 5)
    "latest_state",          # PK(state_key)
    "api_keys",              # PK(key)
    # agent_history 스키마 (Phase 4) — 모든 테이블이 TEXT PK 사용 (id / feature_id / 등).
    # RETURNING id 를 자동으로 붙이면 안 되므로 전부 _PK_NOT_ID 에 등록.
    "agent_runs",                # PK(id TEXT)
    "agent_run_outcomes",        # PK(run_id TEXT)
    "unified_snapshots",         # PK(id TEXT)
    "cross_market_events",       # PK(id TEXT)
    "learning_patterns",         # PK(id TEXT)
    "feature_governance",        # PK(feature_id TEXT)
    "feature_evaluation_log",    # PK(id TEXT)
    "market_events",             # PK(id TEXT)
    "market_states",             # PK(id TEXT)
    "market_insights",           # PK(id TEXT)
    "market_opportunities",      # PK(id TEXT)
    "memory_episodes",           # PK(id TEXT)
    "memory_outcomes",           # PK(episode_id TEXT)
    "memory_landmarks",          # PK(id TEXT)
    "replay_sessions",           # PK(id TEXT)
    "replay_snapshots",          # PK(id TEXT)
    "replay_findings",           # PK(id TEXT)
    "direction_forecasts",       # PK(id TEXT)
    "forecast_outcomes",         # PK(forecast_id TEXT)
    "llm_adjustments",           # PK(id TEXT)
    "event_patterns",            # PK(keyword TEXT)
    "strategy_fitness",          # PK(market_id, regime, strategy_role) — composite
    "evolution_seeds",           # PK(id AUTOINCREMENT) — 하지만 UNIQUE(market_id,regime,role) 로 upsert
    "evolution_hints",           # PK(id AUTOINCREMENT) — 동일
    "evolution_bridge_state",    # PK(market_id TEXT)
    # external_context 스키마 (Phase 3) — natural-key PK 사용 테이블 (RETURNING id 불가).
    # DDL 에 id INTEGER AUTOINCREMENT 를 가지지 않는 테이블만 등재.
    "fundamentals",              # PK(market_id, symbol) — migration 시 market_id 주입
    "symbol_master",             # PK(market_id, symbol)
    "symbol_aliases",            # PK(alias, canonical_symbol)
    "news_events",               # PK(id TEXT)
    "news_clusters",             # PK(cluster_id TEXT)
    "news_intel_analysis",       # PK(news_id TEXT)
    "news_signal_mapping",       # PK(id TEXT)
    "news_reaction_history",     # PK(news_id, market_id, window_minutes)
    "news_reaction_speed",       # PK(news_type, market_id, speed_band)
    "news_causality_analysis",   # PK(news_id, market_id)
    "cross_market_decoupling",   # PK(source_market, target_market)
    "event_leading_scores",      # PK(event_type, market_id, news_type)
    "event_price_tracks",        # PK(event_id, day_offset, timestamp)
    "macro_event_narratives",    # PK(event_id TEXT)
    "macro_prediction_accuracy", # PK(source_category, target_market, lag_bucket)
    "market_metrics",            # PK(symbol, metric_type, timestamp)
    "regime_transition_probs",   # PK(market_id, current_regime, next_regime)
    "scheduled_events",          # PK(id TEXT)
    "inference_candidates",      # PK(id TEXT, 비정수)
    # rl_pipeline 스키마 (Phase 6) — id BIGSERIAL 을 가진 테이블은 RETURNING id 가능하므로
    # 여기서는 **natural / composite PK** 테이블만 등재. BIGSERIAL + natural UNIQUE 가 병존하는
    # 테이블 (strategy_evolution 등) 은 _PK_NOT_ID 에서 제외 — RETURNING id 허용.
    "guardian_coin_config",          # PK(market_id, symbol, is_staging)
    "pattern_feedback_logs",         # PK(market_id, pattern, is_staging)
    "global_strategies",             # PK(market_id, id TEXT, is_staging)
    "optimal_thresholds",            # PK(market_id, pattern, is_staging)
    "symbol_global_weights",         # PK(market_id, market_type, market, symbol, is_staging)
    "runs",                          # PK(market_id, run_id, is_staging)  ※ llm_factory.runs 와 동명 — 둘 다 TEXT PK 라 동작 동일
    "run_records",                   # PK(market_id, run_id, is_staging)
    # market_coin 스키마 (Phase 8) — composite / natural PK 테이블 (RETURNING id 불가).
    # BIGSERIAL id 를 가진 테이블 (completed_trades, guardian_reports 등) 은 등재 안 함.
    "universe",                             # PK(symbol)
    "candles",                              # PK(symbol, interval, timestamp, kind)
    "regime_adaptive_params",               # PK(symbol, interval, param_name)
    "adaptive_thresholds",                  # PK(symbol, threshold_key)
    "ai_generated_strategies",              # PK(strategy_id TEXT)
    "big_profit_patterns",                  # PK(pattern TEXT)
    # 2026-04-28: live_trading 실전매매 테이블 (BIGSERIAL id 지만 natural PK 으로 upsert)
    "live_positions",                       # natural PK(symbol UNIQUE)
    "live_trade_decisions",                 # natural PK(symbol, timestamp UNIQUE)
    "live_trade_history",                   # natural PK(symbol, entry_timestamp, exit_timestamp)
    "live_trade_feedback",                  # natural PK(symbol, entry_timestamp, exit_timestamp)
    # 2026-04-28: deep_analysis 학습 테이블 7개 — RETURNING id 불가
    "consecutive_loss_patterns",            # PK(key TEXT)
    "analysis_combination_weights",         # PK(id INTEGER, single row)
    "pattern_risk_reward",                  # PK(pattern TEXT)
    "regime_transition_learning",           # PK(transition TEXT)
    "mae_mfe_analysis",                     # PK(id INTEGER, single row)
    "time_of_day_learning",                 # PK(hour INTEGER)
    "volume_pattern_learning",              # PK(category TEXT)
    "llm_weight_tracker",                   # PK(market_id)
    "role_params",                          # PK(param_key, symbol)
    "role_thresholds",                      # PK(param_key, symbol)
    "role_weights",                         # PK(role, symbol)
    "strategy_genes",                       # PK(gene_id TEXT)
    "system_status",                        # PK(key)
    "candle_signal_cache",                  # composite
    "pending_signal_cache",                 # composite
    "confirmed_role_cache",                 # composite
    "signal_learning_meta",                 # PK(key)
    "per_symbol_defense_streak",            # PK(symbol)
    "per_symbol_signal_feedback_scores",    # composite
    "per_symbol_score_calibration",         # composite
    "per_symbol_exit_calibration",          # composite
    "per_symbol_signal_trajectory",         # composite
    "learning_defense_streak",              # PK(symbol)
    "signal_feedback_scores",               # composite
    "score_calibration",                    # composite
    "signal_strategy_performance",          # composite
    "alignment_learning",                   # PK(alignment_bucket)
    "lookback_stage_learning",              # composite
    "role_context_learning",                # composite
    "role_combination_learning",            # composite
    "context_boost_learning",               # composite
    "learned_hierarchy_params",             # PK(param_key)
    "exit_calibration",                     # composite
    "signals",                              # composite (symbol,interval,timestamp,source_type)
    "accuracy_summary",                     # composite
    "global_accuracy",                      # PK(interval)
    "auxiliary_summary",                    # composite
    "group_analysis",                       # composite
    "group_candles",                        # composite
    "trajectory_learning_state",            # composite (symbol, key)
})


# 테이블별 INSERT OR REPLACE 충돌 키 매핑.
# admin 스키마 테이블에서 UNIQUE / PRIMARY KEY 가 무엇인지 DDL 에 정의되어 있으나,
# INSERT OR REPLACE 는 자연키 기준으로 upsert 를 의미하므로 명시적으로 매핑.
_UPSERT_CONFLICT_KEYS: dict[str, Tuple[str, ...]] = {
    # admin 스키마
    "daily_reports": ("report_date",),  # UNIQUE(report_date)
    "settings": ("key",),                # PRIMARY KEY(key)
    "admin_settings": ("key",),
    "external_daily_stats": ("date",),
    "health_daily_stats": ("date",),
    "ops_daily_stats": ("date",),
    "perf_daily_stats": ("date", "market"),
    # llm_factory 스키마
    "debates": ("debate_id",),           # PRIMARY KEY(debate_id)
    "snapshots": ("snapshot_id",),
    "llm_explanations": ("snapshot_id",),
    "runs": ("run_id",),
    "agent_credibility": ("agent_name",),
    "llm_trading_decisions": ("market_id", "symbol"),  # [Wave G] PK(market_id, symbol)
    # agent_history 스키마 (Phase 4) — 실제 DDL 확인 반영.
    "feature_governance": ("feature_id",),
    "strategy_fitness": ("market_id", "regime", "strategy_role"),  # composite PK (strategy_role!)
    "evolution_seeds": ("market_id", "regime", "role"),            # UNIQUE(market_id,regime,role)
    "evolution_hints": ("market_id", "regime", "role"),            # UNIQUE(market_id,regime,role)
    "evolution_bridge_state": ("market_id",),
    "event_patterns": ("keyword",),                                # PK(keyword)
    # [2026-07-04] wiring_integrity 스윕 첫 실행이 검출한 미매핑 2종 (agent_history)
    "agent_run_outcomes": ("run_id",),
    "memory_outcomes": ("episode_id",),
    # 아래 TEXT PK 테이블들은 INSERT OR REPLACE 시 id 를 재사용.
    "agent_runs": ("id",),
    "unified_snapshots": ("id",),
    "memory_episodes": ("id",),
    "memory_landmarks": ("id",),
    "market_events": ("id",),
    "market_states": ("id",),
    "market_insights": ("id",),
    "market_opportunities": ("id",),
    "cross_market_events": ("id",),
    "learning_patterns": ("id",),
    "direction_forecasts": ("id",),
    "llm_adjustments": ("id",),
    # replay_sessions/snapshots/findings 는 id PK 지만 update 없음 (INSERT OR REPLACE 미사용).

    # external_context 스키마 (Phase 3) — INSERT OR REPLACE / ON CONFLICT 대상.
    "fundamentals":             ("market_id", "symbol"),
    "symbol_master":            ("market_id", "symbol"),
    "symbol_aliases":           ("alias", "canonical_symbol"),
    "news_events":              ("id",),
    "news_clusters":            ("cluster_id",),
    "news_intel_analysis":      ("news_id",),
    "news_signal_mapping":      ("id",),
    "news_reaction_history":    ("news_id", "market_id", "window_minutes"),
    "news_reaction_speed":      ("news_type", "market_id", "speed_band"),
    "news_causality_analysis":  ("news_id", "market_id"),
    "cross_market_decoupling":  ("source_market", "target_market"),
    "event_leading_scores":     ("event_type", "market_id", "news_type"),
    "event_price_tracks":       ("event_id", "day_offset", "timestamp"),
    "macro_event_narratives":   ("event_id",),
    "macro_prediction_accuracy": ("source_category", "target_market", "lag_bucket"),
    "market_metrics":           ("symbol", "metric_type", "timestamp"),
    "regime_transition_probs":  ("market_id", "current_regime", "next_regime"),
    "scheduled_events":         ("id",),
    "inference_candidates":     ("id",),
    # id INTEGER AUTOINCREMENT + UNIQUE 를 가진 테이블 (RETURNING id 는 그대로 동작, upsert 시 UNIQUE 키 사용)
    "macro_event_market_impact": ("event_id", "market_id", "snapshot_at"),
    "market_regime_flow":       ("market_id", "interval", "snapshot_at"),
    "regime_snapshots":         ("market_id", "symbol", "interval", "snapshot_at"),
    "regime_transitions":       ("market_id", "scope", "scope_name", "from_regime", "to_regime", "volume_condition"),
    "sector_regime_flow":       ("market_id", "sector", "interval", "snapshot_at"),
    "cross_market_correlation": ("source_market", "source_scope", "target_market", "target_scope", "source_regime_change", "target_regime_change"),
    "inference_links":          ("entity_a", "entity_b", "relation_type"),

    # rl_pipeline 스키마 (Phase 6)
    "guardian_coin_config":       ("market_id", "symbol", "is_staging"),
    "pattern_feedback_logs":      ("market_id", "pattern", "is_staging"),
    "strategy_evolution":         ("market_id", "strategy", "regime", "is_staging"),
    "strategy_feedback":          ("market_id", "strategy_type", "market_condition",
                                    "signal_pattern", "is_staging"),
    "global_strategies":          ("market_id", "id", "is_staging"),
    "optimal_thresholds":         ("market_id", "pattern", "is_staging"),
    "symbol_global_weights":      ("market_id", "market_type", "market", "symbol", "is_staging"),
    "runs":                       ("market_id", "run_id", "is_staging"),
    "run_records":                ("market_id", "run_id", "is_staging"),
    "evolution_phases":           ("market_id", "symbol", "interval", "is_staging"),
    "analysis_ratios":            ("market_id", "market_type", "market", "symbol",
                                    "interval", "analysis_type", "is_staging"),
    "trajectory_score_calibration": ("market_id", "score_bin", "is_staging"),
    "learned_params":             ("param_key", "market_type", "symbol", "interval", "regime"),  # Phase 7 Wave B

    # market_coin 스키마 (Phase 8) — INSERT OR REPLACE 대상.
    # 주의: 일부 테이블명이 다른 스키마와 겹침:
    #   - regime_snapshots: external_context 스키마에도 있음 (전자: 위에서 이미 정의, PK 다름).
    #     market_coin.regime_snapshots 는 BIGSERIAL id 라 upsert 필요 시 UNIQUE(market, snapshot_at).
    #   - strategy_evolution: rl_pipeline 도 사용. market_coin 은 UNIQUE(strategy, regime).
    #   - strategy_feedback: rl_pipeline 도 사용. market_coin 은 UNIQUE(strategy_type, market_condition, signal_pattern).
    # 호출자가 search_path 로 스키마 분리하므로 "market_coin." prefix 붙은 호출은 이 매핑이 우선 적용됨.
    "universe":                       ("symbol",),
    # [2026-05-11] kind 제거 — 라이브 PG 3 시장 모두 candles 에 kind 컬럼 없음.
    # project_ddl_drift_sync_2026_04_30 의 dead spec 제거 결정과 일치.
    "candles":                        ("symbol", "interval", "timestamp"),
    "regime_adaptive_params":         ("symbol", "interval", "param_name"),
    "adaptive_thresholds":            ("symbol", "threshold_key"),
    "ai_generated_strategies":        ("strategy_id",),
    "big_profit_patterns":            ("pattern",),
    # 2026-04-28: deep_analysis 학습 테이블 7개 PG 누락 복구 (learning_tables.sql)
    "consecutive_loss_patterns":      ("key",),
    "analysis_combination_weights":   ("id",),
    "pattern_risk_reward":            ("pattern",),
    "regime_transition_learning":     ("transition",),
    "mae_mfe_analysis":               ("id",),
    "time_of_day_learning":           ("hour",),
    "volume_pattern_learning":        ("category",),
    "completed_trades":               ("symbol", "entry_timestamp", "exit_timestamp"),
    "llm_weight_tracker":             ("market_id",),
    "role_params":                    ("param_key", "symbol"),
    "role_thresholds":                ("param_key", "symbol"),
    "role_weights":                   ("role", "symbol"),
    "strategy_genes":                 ("gene_id",),
    "system_status":                  ("key",),
    "virtual_learning_trades":        ("symbol", "entry_timestamp", "exit_timestamp"),
    "virtual_positions":              ("symbol",),
    "virtual_trade_decisions":        ("symbol", "timestamp"),
    "virtual_trade_feedback":         ("symbol", "entry_timestamp", "exit_timestamp"),
    "virtual_trading_q_table":        ("state_key", "action"),
    # 2026-04-28: live_trading 실전매매 테이블 (live_trading.sql 로 PG 배포됨)
    # virtual_* 와 동일 구조라 동일한 PK 사용. RealTrader / live_trade_history INSERT 시 사용.
    # 다중 계좌 매매 (시나리오 A) — UNIQUE(account_id, ...) 으로 변경됨 (2026-04-29)
    "live_positions":                 ("account_id", "symbol",),
    "live_trade_decisions":           ("account_id", "symbol", "timestamp"),
    "live_trade_history":             ("account_id", "symbol", "entry_timestamp", "exit_timestamp"),
    "live_trade_feedback":            ("account_id", "symbol", "entry_timestamp", "exit_timestamp"),
    "candle_signal_cache":            ("symbol", "interval", "candle_timestamp"),
    "pending_signal_cache":           ("symbol", "interval", "candle_timestamp"),
    "confirmed_role_cache":           ("symbol", "interval", "role", "candle_timestamp"),
    "signal_learning_meta":           ("key",),
    "per_symbol_defense_streak":      ("symbol",),
    "per_symbol_signal_feedback_scores": ("symbol", "signal_pattern", "regime"),
    "per_symbol_score_calibration":   ("symbol", "score_bucket_low", "interval", "role_type", "regime"),
    "per_symbol_exit_calibration":    ("symbol", "score_bucket", "interval", "role_type", "regime"),
    "per_symbol_signal_trajectory":   ("symbol", "signal_id", "stage", "role"),
    "learning_defense_streak":        ("symbol",),
    "signal_feedback_scores":         ("symbol", "signal_pattern", "regime"),
    "score_calibration":              ("symbol", "score_bucket_low", "interval", "role_type", "regime"),
    "signal_strategy_performance":    ("strategy_type", "horizon_type"),
    # [2026-07-04] 매핑 누락 5종 보강 — 누락 시 convert_insert_or_replace 가 조용히
    # ON CONFLICT DO NOTHING 을 붙여 "첫 쓰기 이후 모든 갱신 무음 폐기"(키 포화 동결)
    # 였다. 실증: signal_theoretical_performance 가 coin 5/22 동결로 보였으나 실은
    # 매 딥사이클 33k건 분석 후 upsert 가 전부 no-op (신규 패턴키만 삽입됨).
    "signal_theoretical_performance": ("pattern",),
    "signal_error_corrections":       ("pattern_key", "role", "regime"),
    "defense_streak":                 ("symbol",),
    "regime_signal_coherence":        ("regime_label", "interval", "signal_direction"),
    "event_price_tracks":             ("event_id", "day_offset", "timestamp"),
    "alignment_learning":             ("alignment_bucket",),
    "lookback_stage_learning":        ("role", "stage"),
    "role_context_learning":          ("role", "context_used"),
    "role_combination_learning":      ("position_key", "regime"),
    "context_boost_learning":         ("role", "alignment_type"),
    "learned_hierarchy_params":       ("param_key",),
    "exit_calibration":               ("symbol", "score_bucket", "interval", "role_type", "regime"),
    "signals":                        ("symbol", "interval", "timestamp", "source_type"),
    "accuracy_summary":               ("symbol", "interval"),
    "global_accuracy":                ("interval",),
    "auxiliary_summary":              ("symbol", "interval"),
    "group_analysis":                 ("symbol", "interval", "timestamp"),
    "group_candles":                  ("symbol", "interval", "timestamp"),
    "profit_history":                 ("symbol", "timestamp"),  # BIGSERIAL id 있지만 (symbol,timestamp) 자연키 upsert 가능
    "trajectory_exit_timing":         ("symbol", "regime", "hours_bucket"),
    "trajectory_trailing_params":     ("symbol", "regime", "peak_range"),
    "trajectory_recovery_params":     ("symbol", "regime", "drawdown_range"),
    "trajectory_regime_curves":       ("symbol", "regime"),
    "trajectory_holding_risk":        ("symbol", "hours_bucket"),
    "trajectory_learning_state":      ("symbol", "key"),

    # crow 스키마 (Pipeline G)
    "crow_observations":              ("id",),
    "crow_predictions":               ("id",),
    "crow_utterances":                ("id",),
    "crow_lead_time_history":         ("id",),
}


# [2026-07-04] 동적 PK 조회 캐시 + DO NOTHING 강등 경고 (테이블당 1회).
# 매핑 미스가 조용한 DO NOTHING 이 되는 것이 "학습 테이블 첫값 동결" 병소 공장이었다.
_PK_RESOLVE_CACHE: Dict[Tuple[str, str], Optional[Tuple[str, ...]]] = {}
_DO_NOTHING_WARNED: set = set()


def _warn_do_nothing_once(table: str) -> None:
    if table in _DO_NOTHING_WARNED:
        return
    _DO_NOTHING_WARNED.add(table)
    logging.getLogger(__name__).warning(
        "INSERT OR REPLACE → ON CONFLICT DO NOTHING 강등: table=%s "
        "(_UPSERT_CONFLICT_KEYS 매핑 없음 + PK 동적조회 실패). "
        "기존 키 갱신이 전부 무시된다 — 매핑 추가 필요.", table,
    )


def convert_insert_or_replace(sql: str, pk_resolver=None) -> str:
    """INSERT OR REPLACE → INSERT ... ON CONFLICT (...) DO UPDATE SET col=EXCLUDED.col.

    충돌 컬럼 결정 우선순위:
    1. _UPSERT_CONFLICT_KEYS 명시 매핑 (자연키가 PK 와 다른 테이블 대응)
    2. [2026-07-04] pk_resolver 콜백 — PG 카탈로그에서 PK 동적 조회 (커서 shim 이 주입).
       단 PK 컬럼이 INSERT 컬럼 목록에 전부 포함될 때만 사용 (BIGSERIAL id 처럼
       목록에 없는 PK 는 충돌이 영원히 안 일어나 중복 삽입되므로 배제).
    3. 둘 다 실패 → ON CONFLICT DO NOTHING + 경고 1회 (구버전은 이 강등이 무음이라
       학습 테이블들이 '첫 쓰기 값'에 영구 동결되는 병소였다).
    """
    m = _INSERT_OR_REPLACE_RE.match(sql)
    if not m:
        return sql
    table = m.group(1).lower()
    columns_str = m.group(2)
    columns = [c.strip() for c in columns_str.split(",")]

    conflict_cols = _UPSERT_CONFLICT_KEYS.get(table)
    if not conflict_cols and pk_resolver is not None:
        try:
            resolved = pk_resolver(table)
        except Exception:
            resolved = None
        if resolved:
            _cols_lower = {c.lower() for c in columns}
            if all(c.lower() in _cols_lower for c in resolved):
                conflict_cols = tuple(resolved)

    # ``INSERT OR REPLACE INTO`` → ``INSERT INTO``
    new_sql = re.sub(
        r"^\s*INSERT\s+OR\s+REPLACE\b",
        "INSERT",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )

    if not conflict_cols:
        # 매핑도 PK 도 없음 → 안전을 위해 ON CONFLICT DO NOTHING (경고 1회).
        _warn_do_nothing_once(table)
        return new_sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"

    # 업데이트 대상: 충돌 컬럼을 제외한 나머지
    update_cols = [c for c in columns if c not in conflict_cols]
    if not update_cols:
        set_clause = ""
        on_conflict = f" ON CONFLICT ({', '.join(conflict_cols)}) DO NOTHING"
    else:
        set_clause = ", ".join(f"{c}=EXCLUDED.{c}" for c in update_cols)
        on_conflict = (
            f" ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET {set_clause}"
        )

    return new_sql.rstrip().rstrip(";") + on_conflict


def convert_insert_or_ignore(sql: str) -> str:
    """``INSERT OR IGNORE INTO t(...) VALUES(...)`` → ``INSERT INTO t(...) VALUES(...) ON CONFLICT DO NOTHING``.

    UNIQUE 제약 충돌 시 조용히 스킵하는 SQLite 문법을 PG 동치로 변환.
    PG ``ON CONFLICT DO NOTHING`` 은 unique key 지정 없이도 동작 (모든 unique constraint 적용).
    """
    m = _INSERT_OR_IGNORE_RE.match(sql)
    if not m:
        return sql
    # ``INSERT OR IGNORE INTO`` → ``INSERT INTO``
    new_sql = re.sub(
        r"^\s*INSERT\s+OR\s+IGNORE\b",
        "INSERT",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    # 이미 ON CONFLICT 가 있는지 확인 (호출자가 직접 작성한 경우)
    if re.search(r"\bON\s+CONFLICT\b", new_sql, re.IGNORECASE):
        return new_sql
    return new_sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"


def is_pragma_or_script(sql: str) -> bool:
    """PRAGMA / executescript 내용 판단 (PG 모드에서 skip 용)."""
    s = sql.strip().lstrip("-- \n").upper()
    return s.startswith("PRAGMA") or s.startswith("CREATE INDEX IF NOT EXISTS IDX_") and "ON (" not in s.upper()


# ============================================================
# SQLite → PG 자동 변환 (function / comparison / meta query)
# ============================================================

# SQLite datetime(...) 함수 → PG 동등 함수 변환.
# datetime('now') 는 호출 빈도 높으므로 CURRENT_TIMESTAMP 로 단순 치환.
# datetime('now','-N days') 같은 modifier 는 NOW() - INTERVAL 'N days' 로 변환.
_SQLITE_DATETIME_NOW_RE = re.compile(
    r"""datetime\s*\(\s*['"]now['"]\s*(?:,\s*['"]([-+]?\d+)\s+(second|minute|hour|day|week|month|year)s?['"])?\s*\)""",
    re.IGNORECASE,
)


def _replace_datetime(m: re.Match) -> str:
    offset = m.group(1)
    unit = m.group(2)
    if offset is None:
        return "CURRENT_TIMESTAMP"
    sign = "+" if int(offset) >= 0 else "-"
    abs_n = abs(int(offset))
    return f"(CURRENT_TIMESTAMP {sign} INTERVAL '{abs_n} {unit}')"


# sqlite_master → information_schema 변환
# SQLite: SELECT name FROM sqlite_master WHERE type='table' AND name='X'
# PG:     SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema() AND table_name='X'
# sqlite_master 변환.
# caller 가 결과를 ``row["name"]`` 또는 ``row[0]`` 로 접근하므로 ``table_name AS name`` alias.
# caller 가 ``WHERE type='table'`` 을 명시하는 경우가 많아 사전 제거.
_SQLITE_MASTER_PATTERNS = [
    # SELECT name FROM sqlite_master WHERE type='table' AND name=?
    (re.compile(
        r"""SELECT\s+name\s+FROM\s+sqlite_master\s+WHERE\s+type\s*=\s*['"]table['"]\s+AND\s+name\s*=\s*""",
        re.IGNORECASE,
     ),
     "SELECT table_name AS name FROM information_schema.tables "
     "WHERE table_schema = current_schema() AND table_name = "),
    # SELECT name FROM sqlite_master WHERE name=? (type 명시 안 된 경우)
    (re.compile(
        r"""SELECT\s+name\s+FROM\s+sqlite_master\s+WHERE\s+name\s*=\s*""",
        re.IGNORECASE,
     ),
     "SELECT table_name AS name FROM information_schema.tables "
     "WHERE table_schema = current_schema() AND table_name = "),
    # SELECT name FROM sqlite_master WHERE type='table' (전체 목록)
    (re.compile(
        r"""SELECT\s+name\s+FROM\s+sqlite_master\s+WHERE\s+type\s*=\s*['"]table['"]""",
        re.IGNORECASE,
     ),
     "SELECT table_name AS name FROM information_schema.tables "
     "WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'"),
    # SELECT name FROM sqlite_master (no WHERE)
    (re.compile(
        r"""SELECT\s+name\s+FROM\s+sqlite_master""",
        re.IGNORECASE,
     ),
     "SELECT table_name AS name FROM information_schema.tables "
     "WHERE table_schema = current_schema()"),
]


# PRAGMA table_info(X) → information_schema.columns 으로 변환
# 호출자가 PRAGMA 결과를 [(cid, name, type, notnull, dflt, pk), ...] 로 기대하므로
# column_name 을 2번째 필드로 보내는 SELECT 로 치환 (cid=0 더미).
_PRAGMA_TABLE_INFO_RE = re.compile(
    r"""PRAGMA\s+table_info\s*\(\s*([A-Za-z_][A-Za-z_0-9]*)\s*\)""",
    re.IGNORECASE,
)

# SQLite virtual-table form: `SELECT ... FROM pragma_table_info('tbl')` 는 컬럼 목록을
# 일반 쿼리처럼 리턴한다. PG 에선 subquery 로 치환 (column_name AS name 으로 alias).
_PRAGMA_TABLE_INFO_VTAB_RE = re.compile(
    r"""\bFROM\s+pragma_table_info\s*\(\s*['"]([A-Za-z_][A-Za-z_0-9]*)['"]\s*\)""",
    re.IGNORECASE,
)


def _replace_pragma_table_info_vtab(m: re.Match) -> str:
    tbl = m.group(1).lower()
    return (
        f"FROM (SELECT 0 AS cid, column_name AS name, data_type AS type, "
        f"CASE is_nullable WHEN 'NO' THEN 1 ELSE 0 END AS notnull, "
        f"column_default AS dflt, 0 AS pk "
        f"FROM information_schema.columns "
        f"WHERE table_schema = current_schema() AND table_name = '{tbl}' "
        f"ORDER BY ordinal_position) AS _pragma_ti"
    )


def _replace_pragma_table_info(m: re.Match) -> str:
    tbl = m.group(1).lower()
    return (
        f"SELECT 0 AS cid, column_name AS name, data_type AS type, "
        f"CASE is_nullable WHEN 'NO' THEN 1 ELSE 0 END AS notnull, "
        f"column_default AS dflt, 0 AS pk "
        f"FROM information_schema.columns "
        f"WHERE table_schema = current_schema() AND table_name = '{tbl}' "
        f"ORDER BY ordinal_position"
    )


# AUTOINCREMENT → (제거, PG 에서는 SERIAL/BIGSERIAL 이나 GENERATED ALWAYS AS IDENTITY 필요)
# 주로 CREATE TABLE 내부에서 등장. "INTEGER PRIMARY KEY AUTOINCREMENT" 전체를 BIGSERIAL PRIMARY KEY 로.
_AUTOINCREMENT_RE = re.compile(
    r"""\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b""",
    re.IGNORECASE,
)


# strftime('%s', 'now') → EXTRACT(EPOCH FROM NOW())::BIGINT (unix epoch 초)
# SQLite: strftime('%s', 'now') / strftime('%s', 'now', '-1 day')
# PG:     EXTRACT(EPOCH FROM NOW())::BIGINT / EXTRACT(EPOCH FROM (NOW() - INTERVAL '1 day'))::BIGINT
_SQLITE_STRFTIME_EPOCH_RE = re.compile(
    r"""strftime\s*\(\s*['"]%s['"]\s*,\s*['"]now['"]\s*(?:,\s*['"]([-+]?\d+)\s+(second|minute|hour|day|week|month|year)s?['"])?\s*\)""",
    re.IGNORECASE,
)


def _replace_strftime_epoch(m: re.Match) -> str:
    offset = m.group(1)
    unit = m.group(2)
    if offset is None:
        return "EXTRACT(EPOCH FROM NOW())::BIGINT"
    sign = "+" if int(offset) >= 0 else "-"
    abs_n = abs(int(offset))
    return f"EXTRACT(EPOCH FROM (NOW() {sign} INTERVAL '{abs_n} {unit}'))::BIGINT"


# boolean = 1 / = 0 비교 자동 교정.
# PG 의 boolean 컬럼에 `= 1` 비교하면 "operator does not exist: boolean = integer" 에러.
# 안전한 컬럼 목록만 치환 (WHERE is_active = 1 → WHERE is_active = TRUE).
_BOOL_EQ_1_COLUMNS = (
    "is_active", "is_staging", "is_learned_strategy", "is_improved_variant",
    "is_archived", "was_blocked", "was_correct",
    # admin 스키마 (Wave H 후, OpenClaw 가 채우던 컬럼들 — 2026-05-11 OpenClaw 제거)
    "resolved", "dismissed", "alive", "output_fresh",
)

# WHERE / AND 뒤의 `<col> = <0|1>` 또는 `<col>=<0|1>` 패턴
_BOOL_EQ_RE = re.compile(
    r"""\b(?P<col>""" + "|".join(_BOOL_EQ_1_COLUMNS) + r""")\s*=\s*(?P<val>[01])\b""",
    re.IGNORECASE,
)


def _replace_bool_eq(m: re.Match) -> str:
    col = m.group("col")
    val = m.group("val")
    return f"{col} = {'TRUE' if val == '1' else 'FALSE'}"


def apply_sqlite_isms_to_pg(sql: str) -> str:
    """SQLite 전용 구문을 PG 호환으로 변환.

    다음 변환을 순서대로 적용:
        1. ``datetime('now', ...)`` → ``CURRENT_TIMESTAMP`` / ``NOW() - INTERVAL ...``
        2. ``strftime('%s', 'now', ...)`` → ``EXTRACT(EPOCH FROM NOW())::BIGINT``
        3. ``sqlite_master`` → ``information_schema.tables``
        4. ``PRAGMA table_info(X)`` → ``information_schema.columns`` SELECT
        5. ``INTEGER PRIMARY KEY AUTOINCREMENT`` → ``BIGSERIAL PRIMARY KEY``
        6. ``is_active = 1`` 등 boolean=integer 비교 → ``is_active = TRUE``

    이 함수는 caller 가 ``cur.execute(sql, params)`` 전에 sql 을 한 번 전처리하는 용도.
    """
    if not sql:
        return sql

    # 1. datetime('now')
    sql = _SQLITE_DATETIME_NOW_RE.sub(_replace_datetime, sql)

    # 2. strftime('%s', 'now')
    sql = _SQLITE_STRFTIME_EPOCH_RE.sub(_replace_strftime_epoch, sql)

    # 3. sqlite_master
    for pat, repl in _SQLITE_MASTER_PATTERNS:
        sql = pat.sub(repl, sql)

    # 3. PRAGMA table_info(X) — 한 문장 전체가 이 형태면 치환
    sql = _PRAGMA_TABLE_INFO_RE.sub(_replace_pragma_table_info, sql)
    # 3b. FROM pragma_table_info('tbl') — virtual table form
    sql = _PRAGMA_TABLE_INFO_VTAB_RE.sub(_replace_pragma_table_info_vtab, sql)

    # 4. AUTOINCREMENT
    sql = _AUTOINCREMENT_RE.sub("BIGSERIAL PRIMARY KEY", sql)

    # 5. boolean = integer
    sql = _BOOL_EQ_RE.sub(_replace_bool_eq, sql)

    return sql


# ============================================================
# Cursor / Connection shim (PG 모드에서 sqlite3 API 흉내)
# ============================================================

class PgCursorShim:
    """psycopg cursor 를 sqlite3 cursor 스타일로 감싼다.

    주요 차이:
    - ``execute("SELECT ... WHERE x = ?", (v,))`` → ``%s`` 변환 + psycopg execute.
    - ``cur.lastrowid`` — INSERT 시 자동으로 ``RETURNING id`` 추가 후 캐시.
    - row_factory = Row 스타일 dict 접근: ``row["col"]`` 및 ``row[0]`` 모두 동작.
    """

    def __init__(self, pg_cursor):
        self._cur = pg_cursor
        self._lastrowid: Optional[int] = None
        self._has_returning: bool = False

    # -- core exec --

    def execute(self, sql: str, params: Sequence[Any] = ()) -> "PgCursorShim":
        # 0) SQLite 전용 구문 (PRAGMA / sqlite_master / datetime / AUTOINCREMENT /
        #    boolean=1 비교) 을 PG 호환으로 변환.
        # PRAGMA 는 이 단계에서 no-op 로 변환 (is_pragma_or_script 판정).
        stripped_check = sql.lstrip().upper()
        if stripped_check.startswith("PRAGMA"):
            # PRAGMA table_info 는 실제 SELECT 로 번역, 나머지는 no-op.
            if "TABLE_INFO" in stripped_check:
                sql = apply_sqlite_isms_to_pg(sql)
            else:
                # no-op
                self._lastrowid = None
                self._has_returning = False
                self._cur = _FakeCursor()
                return self
        else:
            sql = apply_sqlite_isms_to_pg(sql)

        # 1) INSERT OR REPLACE / INSERT OR IGNORE 는 qmark 변환 전에 먼저 rewrite.
        #    (컬럼 목록 파싱이 ? → %s 변환보다 단순하기 때문)
        if re.match(r"^\s*INSERT\s+OR\s+REPLACE\b", sql, re.IGNORECASE):
            sql = convert_insert_or_replace(sql, pk_resolver=self._resolve_pk)
        elif re.match(r"^\s*INSERT\s+OR\s+IGNORE\b", sql, re.IGNORECASE):
            sql = convert_insert_or_ignore(sql)

        converted = convert_qmark_to_pyformat(sql)

        # 2) INSERT RETURNING id 자동 주입은 **비활성화** (의도하지 않은 에러 유발).
        #    caller 가 명시적으로 RETURNING 을 쓴 경우에만 lastrowid 캐시.
        self._has_returning = bool(
            re.search(r"\bRETURNING\b", converted, re.IGNORECASE)
        )

        # params 가 비어있으면 psycopg 에 단일 인자로 넘긴다.
        # psycopg 는 params 인자가 주어지면 (빈 튜플이라도) SQL 의 ``%`` 를 placeholder
        # 후보로 파싱하므로, LIKE ``'a%c'`` / ROUND ``100.0 * ...`` 같은 리터럴 ``%`` 가
        # 본문에 있고 caller 가 params=() 로 호출하면
        # ``only '%s', '%b', '%t' are allowed as placeholders, got '%c'`` 에러가 난다.
        # 단일 인자로 호출하면 psycopg 가 ``%`` 파싱을 skip 한다.
        try:
            if not params:
                self._cur.execute(converted)
            else:
                self._cur.execute(converted, params)
        except Exception:
            # psycopg 는 실패 후 같은 tx 에서 후속 쿼리 모두 에러. 호출자 rollback 필요.
            self._lastrowid = None
            raise

        if self._has_returning:
            try:
                row = self._cur.fetchone()
                self._lastrowid = row[0] if row else None
            except Exception:
                self._lastrowid = None
        else:
            self._lastrowid = None
        return self

    def _resolve_pk(self, table: str) -> Optional[Tuple[str, ...]]:
        """[2026-07-04] _UPSERT_CONFLICT_KEYS 미스 시 PG 카탈로그에서 PK 동적 조회.

        (current_schema, table) 별 프로세스 캐시. 트랜잭션이 이미 aborted 면 조회가
        실패하는데 그 경우 캐시하지 않고 None 반환 (기존 DO NOTHING 폴백 동작 유지).
        """
        try:
            raw_conn = self._cur.connection
            with raw_conn.cursor() as c:
                c.execute("SELECT current_schema()")
                schema = (c.fetchone() or ["?"])[0]
                key = (str(schema), table)
                if key in _PK_RESOLVE_CACHE:
                    return _PK_RESOLVE_CACHE[key]
                c.execute(
                    "SELECT a.attname FROM pg_index i "
                    "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                    "WHERE i.indrelid = to_regclass(%s) AND i.indisprimary "
                    "ORDER BY array_position(i.indkey::int[], a.attnum::int)",
                    (table,),
                )
                cols = tuple(r[0] for r in c.fetchall()) or None
        except Exception:
            return None
        _PK_RESOLVE_CACHE[key] = cols
        return cols

    def executemany(self, sql: str, seq_of_params: Iterable[Sequence[Any]]) -> "PgCursorShim":
        # SQLite-ism 변환 + INSERT OR REPLACE/IGNORE rewrite
        sql = apply_sqlite_isms_to_pg(sql)
        if re.match(r"^\s*INSERT\s+OR\s+REPLACE\b", sql, re.IGNORECASE):
            sql = convert_insert_or_replace(sql, pk_resolver=self._resolve_pk)
        elif re.match(r"^\s*INSERT\s+OR\s+IGNORE\b", sql, re.IGNORECASE):
            sql = convert_insert_or_ignore(sql)
        converted = convert_qmark_to_pyformat(sql)
        self._cur.executemany(converted, list(seq_of_params))
        return self

    def executescript(self, sql: str) -> "PgCursorShim":
        """세미콜론으로 분리된 여러 SQL 문을 순차 실행. SQLite 호환."""
        # PRAGMA 및 SQLite 전용은 skip
        for stmt in filter(None, (s.strip() for s in sql.split(";"))):
            if is_pragma_or_script(stmt):
                continue
            self._cur.execute(stmt)
        return self

    # -- fetch --

    def fetchone(self):
        if self._has_returning:
            # RETURNING 이미 소비됐음
            return None
        row = self._cur.fetchone()
        return _wrap_row(row, self._cur)

    def fetchall(self):
        rows = self._cur.fetchall()
        return [_wrap_row(r, self._cur) for r in rows]

    def fetchmany(self, size: int = 1):
        rows = self._cur.fetchmany(size)
        return [_wrap_row(r, self._cur) for r in rows]

    def __iter__(self):
        # psycopg cursor 는 iterable
        return (_wrap_row(r, self._cur) for r in self._cur)

    # -- attrs --

    @property
    def lastrowid(self) -> Optional[int]:
        return self._lastrowid

    @property
    def rowcount(self) -> int:
        return self._cur.rowcount

    @property
    def description(self):
        return self._cur.description

    def close(self) -> None:
        self._cur.close()

    # -- context manager (sqlite3 cursor 호환) --

    def __enter__(self) -> "PgCursorShim":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            self.close()
        except Exception:
            pass


class _RowView:
    """psycopg tuple 을 ``row[0]`` / ``row["col"]`` 모두 지원하도록 감싸기.

    sqlite3.Row 호환 interface.
    """

    __slots__ = ("_values", "_names")

    def __init__(self, values: Sequence[Any], names: Sequence[str]):
        self._values = tuple(values)
        self._names = names

    def __getitem__(self, k):
        if isinstance(k, int):
            return self._values[k]
        # column name 검색
        idx = self._names.index(k)
        return self._values[idx]

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def keys(self):
        return list(self._names)

    def values(self):
        return list(self._values)

    def items(self):
        return list(zip(self._names, self._values))

    def get(self, key, default=None):
        try:
            return self[key]
        except (KeyError, ValueError, IndexError):
            return default

    def __contains__(self, key) -> bool:
        return key in self._names

    def __repr__(self) -> str:
        return f"Row({dict(zip(self._names, self._values))!r})"


def _wrap_row(row, cur):
    if row is None:
        return None
    names = [d[0] for d in cur.description] if cur.description else []
    # PG psycopg 는 numeric/decimal 컬럼 (AVG/SUM 등) 을 Python ``decimal.Decimal`` 로 반환.
    # SQLite 는 항상 float 반환 → caller 가 float 연산 기대. 호환성 위해 Decimal 만 float 로 강등.
    # 정밀도 손실 가능성 있지만 ML/분석 코드 전제상 허용 가능.
    try:
        from decimal import Decimal
        coerced = tuple(
            float(v) if isinstance(v, Decimal) else v for v in row
        )
    except Exception:
        coerced = row
    return _RowView(coerced, names)


class PgConnectionShim:
    """psycopg Connection 을 sqlite3.Connection 스타일로 감싼다.

    ``conn.execute(sql)`` / ``conn.commit()`` / ``conn.rollback()`` / ``conn.close()`` 지원.
    ``conn.row_factory = sqlite3.Row`` 호환 (이미 dict-like row 반환).
    ``PRAGMA ...`` 는 no-op.

    ``_pool`` 이 세팅돼 있으면 ``close()`` 가 실제 disconnect 대신 풀에 반납한다.
    그렇지 않고 ``_owns_conn=True`` 면 직접 disconnect (테스트/단발 용도).
    """

    def __init__(self, pg_conn, *, owns_conn: bool = True, pool=None):
        self._conn = pg_conn
        self._owns_conn = owns_conn
        self._pool = pool
        # sqlite3.Connection 인터페이스 호환 속성
        self.row_factory = None
        # [2026-08-03] 누수 진단용 대출 지점 기록 — close() 없이 GC 되면
        # __del__ 경고에 함께 출력해 누수 caller 를 특정할 수 있게 한다.
        # (external_context 풀 고갈 사고: 슬롯 유실 지점을 못 찾아 py-spy 로
        # 추적해야 했음. extract_stack 은 conn borrow 비용 대비 무시 가능.)
        try:
            import traceback
            frames = traceback.extract_stack(limit=6)[:-1]
            self._borrow_origin = " <- ".join(
                f"{f.filename.rsplit('/', 1)[-1].rsplit(chr(92), 1)[-1]}:{f.lineno}"
                for f in reversed(frames)
            )
        except Exception:
            self._borrow_origin = "unknown"

    def __del__(self):
        # [2026-08-03] 풀 슬롯 유실 안전망: close() 를 못 타고 GC 되면 풀 장부상
        # 슬롯이 영구 유실된다 (psycopg_pool 은 putconn 없이는 자리를 회수하지
        # 않음 — external_context 프로세스가 ~12h 만에 풀 고갈된 실사고).
        # 여기서 반납해 슬롯을 회수하고, 누수 지점을 WARNING 으로 남긴다.
        # 정상 경로는 close() 가 _pool/_conn 을 None 으로 비워 no-op.
        pool = getattr(self, "_pool", None)
        conn = getattr(self, "_conn", None)
        if pool is None or conn is None:
            if conn is not None and getattr(self, "_owns_conn", False):
                try:
                    conn.close()
                except Exception:
                    pass
            return
        try:
            logger.warning(
                "PgConnectionShim GC'd without close() — returning conn to pool "
                "(borrowed at: %s)", getattr(self, "_borrow_origin", "unknown"),
            )
        except Exception:
            pass
        try:
            pool.putconn(conn)  # 트랜잭션 오염은 풀 reset 콜백이 정리
        except Exception:
            pass
        self._pool = None
        self._conn = None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> PgCursorShim:
        # PRAGMA → no-op
        s = sql.lstrip().upper()
        # PRAGMA table_info 는 PgCursorShim.execute 가 information_schema SELECT 로
        # 재작성하므로 실제 cursor 로 내려보낸다. 그 외 PRAGMA 는 no-op.
        if s.startswith("PRAGMA") and "TABLE_INFO" not in s:
            return PgCursorShim(_FakeCursor())
        if s.startswith("BEGIN") and "IMMEDIATE" in s:
            return PgCursorShim(_FakeCursor())
        cur = self._conn.cursor()
        shim = PgCursorShim(cur)
        return shim.execute(sql, params)

    def executemany(self, sql: str, seq_of_params: Iterable[Sequence[Any]]) -> PgCursorShim:
        cur = self._conn.cursor()
        shim = PgCursorShim(cur)
        return shim.executemany(sql, seq_of_params)

    def executescript(self, sql: str) -> PgCursorShim:
        cur = self._conn.cursor()
        shim = PgCursorShim(cur)
        return shim.executescript(sql)

    def cursor(self) -> PgCursorShim:
        return PgCursorShim(self._conn.cursor())

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        if self._pool is not None:
            # 풀에 돌려주기 전 열린 트랜잭션을 정리한다.
            # 명시적 commit 하지 않은 SELECT 만 있어도 psycopg autocommit=False
            # 라 INTRANS 로 남으므로 rollback 으로 세션 상태를 초기화한다.
            # (데이터 변경은 호출자가 이미 commit 했다는 전제.)
            try:
                if self._conn is not None and not self._conn.closed:
                    self._conn.rollback()
            except Exception:
                pass
            try:
                self._pool.putconn(self._conn)
            except Exception:
                # [2026-08-03] putconn 실패(풀 close 후 등) 시 raw conn 이라도
                # 닫아 서버측 세션 잔류를 막는다. 이 경로는 프로세스 종료
                # 국면이라 슬롯 계수 복구는 무의미.
                try:
                    if self._conn is not None and not self._conn.closed:
                        self._conn.close()
                except Exception:
                    pass
            finally:
                self._pool = None
                self._conn = None
            return
        if self._owns_conn:
            self._conn.close()

    # context manager
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            try:
                self._conn.rollback()
            except Exception:
                pass
        else:
            try:
                self._conn.commit()
            except Exception:
                pass
        self.close()
        return False


class _FakeCursor:
    """no-op cursor (PRAGMA 등에 사용)."""

    description = None
    rowcount = 0

    def execute(self, *a, **kw):
        pass

    def executemany(self, *a, **kw):
        pass

    def fetchone(self):
        return None

    def fetchall(self):
        return []

    def fetchmany(self, size=1):
        return []

    def close(self):
        pass


# ============================================================
# Entry point — SQLite 스타일 커넥션 (백엔드 라우팅)
# ============================================================

def _open_pg_shim(schema: str, *, getconn_timeout: Optional[float] = None) -> PgConnectionShim:
    """PG 커넥션 + search_path 설정 + PgConnectionShim wrap.

    ``psycopg_pool.ConnectionPool`` 에서 커넥션을 빌려온다. close() 호출 시
    실제 disconnect 가 아닌 풀 반납이 일어나므로 호출자가 정상적으로
    try/finally 또는 context manager 로 닫아주기만 하면 된다.

    search_path / statement_timeout 은 풀의 ``configure`` 콜백이 세팅한다
    (``shared.db.pg_pool._make_pool`` 참조).

    Args:
        getconn_timeout: ``pool.getconn(timeout=...)`` 에 넘길 초 단위 timeout.
            None 이면 풀 생성 시 설정된 기본 timeout(``PG_POOL_GETCONN_TIMEOUT_SEC``,
            기본 30s)이 적용된다. 2026-05-21: 이전엔 ``getconn()`` 을 timeout 인자
            없이 호출 → causality_analyzer 같은 ``_conn_scope`` caller 는
            ``_pg_readers`` 와 달리 명시 timeout 보호가 없어, 죽은 step thread 가
            커넥션을 반납 안 해 풀이 고갈되면 메인 사이클이 무한 대기했다.
            ``PoolTimeout`` 을 raise 시켜 caller 가 사이클을 abort 처리하게 한다.
    """
    from oneqaz_trading_mcp.shared.db.pg_pool import get_pool

    pool = get_pool(schema)
    if getconn_timeout is not None:
        conn = pool.getconn(timeout=getconn_timeout)
    else:
        conn = pool.getconn()
    return PgConnectionShim(conn, owns_conn=False, pool=pool)


# [2026-07-06] AWS EC2 배포 제거 완료 — SQLite readonly fallback 서브시스템
# (_SQLITE_READONLY_MAP_DEFAULTS / _SqliteReadonlyShim / MCP_ALLOW_SQLITE_READONLY
#  게이트) 일괄 삭제. 모든 스키마 접근은 PG 단일 경로.


def open_schema_connection(
    schema: str,
    *,
    readonly: bool = False,
    sqlite_logical_name: Optional[str] = None,
    sqlite_path_hint: Optional[str] = None,
    getconn_timeout: Optional[float] = None,
):
    """제네릭: 스키마별 커넥션 반환 (PG 단일 경로).

    [2026-07-06] AWS EC2 배포 제거 완료 — SQLite readonly fallback 삭제.

    Args:
        schema: PG 스키마명 (``admin``, ``llm_factory``, ``market_coin``, ...).
        readonly: 읽기 전용 모드 (시그니처 호환용 — PG 에서 별도 처리 없음).
        sqlite_logical_name: (deprecated) 무시됨.
        sqlite_path_hint: (deprecated, AWS fallback 용이었음) 무시됨.
        getconn_timeout: PG 풀에서 커넥션을 빌릴 때의 초 단위 timeout. None 이면
            풀 기본값(``PG_POOL_GETCONN_TIMEOUT_SEC``). 풀 고갈 시 무한 대기를
            막아야 하는 hot-path caller 가 명시적으로 짧게 지정할 수 있다.

    Returns:
        PgConnectionShim.
    """
    from oneqaz_trading_mcp.shared.db.config import DBBackend, get_config

    cfg = get_config()
    if cfg.backend == DBBackend.POSTGRES:
        return _open_pg_shim(schema, getconn_timeout=getconn_timeout)

    raise RuntimeError(
        f"[Wave I] DB_BACKEND must be 'postgres'. Got: {cfg.backend!r}. "
        "SQLite/DUAL backends are no longer supported "
        "(AWS SQLite readonly fallback 은 2026-07-06 배포 제거와 함께 삭제됨)."
    )


def open_admin_connection(*, readonly: bool = False):
    """admin 모듈 전용 래퍼. 기존 호출자 호환."""
    return open_schema_connection("admin", readonly=readonly)


def open_llm_factory_connection(
    *, readonly: bool = False, logical_name: Optional[str] = None,
    getconn_timeout: Optional[float] = None,
):
    """llm_factory 모듈 전용 래퍼.

    Args:
        logical_name: [deprecated] PG 모드에서는 단일 스키마이므로 무시.
        getconn_timeout: [2026-06-14] PG 풀 getconn timeout(초). hot read path 가
            풀 고갈 시 무한 대기하지 않도록 짧게 지정. None 이면 풀 기본값(8s).
    """
    return open_schema_connection(
        "llm_factory",
        readonly=readonly,
        sqlite_logical_name=logical_name,
        getconn_timeout=getconn_timeout,
    )


# ============================================================
# Pipeline F — consumer-side read path helper
# ============================================================

def connect_readonly(db_path, timeout: float = 5.0):
    """[Wave I] PG 라우팅 전용. SQLite fallback 폐기.

    llm_factory / api 등 consumer 가 `sqlite3.connect(db_path)` 대신 사용.
    `mcps.config._pg_route_for_db_path` 에 등록된 경로는 PG 로 리라우팅된다.

    등록되지 않은 경로는 RuntimeError — 경로를 등록하거나 상위 호출자를 PG 경로로 수정하라.

    Args:
        db_path: SQLite DB 파일 경로 (sentinel, PG 라우팅 키).
        timeout: 호환용 인자. PG 에서는 사용 안 함.

    Returns:
        PgConnectionShim / _FilteredConnectionShim.
    """
    try:
        from oneqaz_trading_mcp.config import connect_readonly as _mcps_connect_readonly
    except ImportError as e:
        raise RuntimeError(
            "[Wave I] mcps.config import 실패. PG 라우팅 불가. "
            "SQLite fallback 은 폐기됨."
        ) from e
    return _mcps_connect_readonly(db_path, timeout=timeout)
