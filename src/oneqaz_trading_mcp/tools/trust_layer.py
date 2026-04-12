# -*- coding: utf-8 -*-
"""
Trust Layer Tools (P0)
======================
B2AI 구매 여정을 위한 신뢰 증명 도구. AI 에이전트가 OneQAZ MCP를 검증하고
사용자에게 추천할 수 있는 정량 근거를 제공한다.

5-layer Trust 피라미드 중 Layer A/B/C 커버 (가장 강력한 증거):
- Layer A (예견 능력): get_news_leading_indicator_performance, get_news_causality_breakdown
- Layer B (거시-미시 인과): get_prediction_accuracy, get_backtest_tuning_state, get_monthly_accuracy_trend
- Layer C (거버넌스 투명성): get_feature_governance_state

데이터 소스:
- global_predictions.db: macro_prediction_accuracy, backtest_tuning, backtest_results
- external_context.db: event_leading_scores, news_causality_analysis
- agent_history.db: feature_governance

관련 문서:
- ai_brain/07_mcp_llm_api/mcp_trust_layer_design.md (v2)
- ai_brain/06_external_context/internal_external_mapping.md
- ai_brain/07_mcp_llm_api/trust_layer_refactoring_plan.md
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcps.config import (
    COIN_DATA_DIR,
    EXTERNAL_CONTEXT_DATA_DIR,
    EXTERNAL_DB_PATHS,
    GLOBAL_REGIME_DIR,
    KR_DATA_DIR,
    PROJECT_ROOT,
    SIGNAL_DIR_PATHS,
    US_DATA_DIR,
    get_market_db_path,
)
from mcps.resources.resource_response import (
    MCPErrorAction,
    MCPErrorCode,
    mcp_error,
)

logger = logging.getLogger("MarketMCP")

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

GLOBAL_PREDICTIONS_DB = GLOBAL_REGIME_DIR / "global_predictions.db"

# Level 2 구조 학습 DB (단일, per-market 아님 — market_id 컬럼으로 구분)
STRUCTURE_LEARNING_DB = PROJECT_ROOT / "market" / "market_structure" / "data_storage" / "structure_learning.db"

# Level 1 학습 전략 디렉터리 (per-symbol DB)
LEARNING_STRATEGIES_DIRS = {
    "crypto": COIN_DATA_DIR / "learning_strategies",
    "coin": COIN_DATA_DIR / "learning_strategies",
    "kr_stock": KR_DATA_DIR / "learning_strategies",
    "kr": KR_DATA_DIR / "learning_strategies",
    "us_stock": US_DATA_DIR / "learning_strategies",
    "us": US_DATA_DIR / "learning_strategies",
}

# agent_history DB 경로 매핑 (market_id alias 포함)
_AGENT_HISTORY_ROOT = PROJECT_ROOT / "agent_history" / "data_storage"
AGENT_HISTORY_DB_PATHS = {
    "crypto": _AGENT_HISTORY_ROOT / "coin" / "agent_history.db",
    "coin": _AGENT_HISTORY_ROOT / "coin" / "agent_history.db",
    "kr_stock": _AGENT_HISTORY_ROOT / "kr_stock" / "agent_history.db",
    "kr": _AGENT_HISTORY_ROOT / "kr_stock" / "agent_history.db",
    "us_stock": _AGENT_HISTORY_ROOT / "us_stock" / "agent_history.db",
    "us": _AGENT_HISTORY_ROOT / "us_stock" / "agent_history.db",
}

# AWS gateway용 작은 export DB (feature_governance + feature_evaluation_log only)
# scripts/export_feature_governance.py가 생성. agent_history.db 본체가 442MB라 sync 못해서
# 두 테이블만 별도 작은 DB로 export. AWS는 이걸 봄.
FEATURE_GOVERNANCE_DB_PATHS = {
    "crypto": _AGENT_HISTORY_ROOT / "coin" / "feature_governance.db",
    "coin": _AGENT_HISTORY_ROOT / "coin" / "feature_governance.db",
    "kr_stock": _AGENT_HISTORY_ROOT / "kr_stock" / "feature_governance.db",
    "kr": _AGENT_HISTORY_ROOT / "kr_stock" / "feature_governance.db",
    "us_stock": _AGENT_HISTORY_ROOT / "us_stock" / "feature_governance.db",
    "us": _AGENT_HISTORY_ROOT / "us_stock" / "feature_governance.db",
}


def _resolve_agent_history_db(market_id: Optional[str]) -> Optional[Path]:
    """market_id를 agent_history DB 경로로 변환. fallback 우선순위:
    1. agent_history.db (full DB, 집 PC) — 존재하고 0 byte 아닌 경우
    2. feature_governance.db (small export, AWS gateway)
    None이면 coin이 기본 (feature_governance는 per-market 집계).
    """
    key = (market_id or "coin").lower()

    # 1순위: full agent_history.db
    full_path = AGENT_HISTORY_DB_PATHS.get(key)
    if full_path and full_path.exists() and full_path.stat().st_size > 0:
        return full_path

    # 2순위: small export feature_governance.db (AWS gateway)
    fg_path = FEATURE_GOVERNANCE_DB_PATHS.get(key)
    if fg_path and fg_path.exists() and fg_path.stat().st_size > 0:
        return fg_path

    # 둘 다 없으면 None (caller가 에러 처리)
    return None


def _resolve_external_db(market_id: str) -> Optional[Path]:
    return EXTERNAL_DB_PATHS.get(market_id.lower())


def _normalize_market_id_for_structure(market_id: str) -> str:
    """market_id를 structure_learning.db의 market_id 컬럼 형식으로 정규화.
    실제 DB에는 'crypto', 'kr_stock', 'us_stock'이 저장됨.
    """
    mapping = {
        "coin": "crypto",
        "coin_market": "crypto",
        "crypto": "crypto",
        "kr": "kr_stock",
        "kr_market": "kr_stock",
        "kr_stock": "kr_stock",
        "us": "us_stock",
        "us_market": "us_stock",
        "us_stock": "us_stock",
    }
    return mapping.get((market_id or "").lower(), market_id)


def _safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def _safe_int(val, default: int = 0) -> int:
    try:
        return int(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def _open_ro(db_path) -> sqlite3.Connection:
    """
    외부 마운트 볼륨(E 드라이브)의 SQLite를 안정적으로 열기 위한 helper.
    POSIX file locking이 외부 마운트에서 실패하므로 immutable=1 URI 사용.
    **주의**: immutable은 외부 변경 감지를 포기하는 대신 잠금을 우회한다.
    라이브 쓰기 DB의 경우 일시적으로 stale 데이터가 보일 수 있으나 Trust 메타정보는
    읽기 전용 조회이므로 영향 없음.
    """
    db_str = str(db_path)
    # Windows path → SQLite URI: backslash → forward slash
    uri = f"file:{db_str}?mode=ro&immutable=1"
    return sqlite3.connect(uri, uri=True, timeout=10)


# ---------------------------------------------------------------------------
# P0 Tool 1: get_prediction_accuracy — Layer B (거시 예측 적중률)
# ---------------------------------------------------------------------------

def _get_prediction_accuracy(
    category: Optional[str] = None,
    target_market: Optional[str] = None,
) -> Dict[str, Any]:
    """macro_prediction_accuracy 테이블 직접 쿼리.

    Note: accuracy_reporter.get_prediction_accuracy_summary()를 재사용하려 했으나,
    그 함수는 일반 sqlite3.connect()를 사용하여 외부 마운트 볼륨에서 파일 잠금 문제 발생.
    대신 동일 스키마를 _open_ro (immutable URI)로 직접 쿼리한다.
    """
    if not GLOBAL_PREDICTIONS_DB.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"global_predictions.db not found at {GLOBAL_PREDICTIONS_DB}",
            fallback_tool="market://global/summary",
        )

    try:
        with _open_ro(GLOBAL_PREDICTIONS_DB) as conn:
            conn.row_factory = sqlite3.Row

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='macro_prediction_accuracy'"
            )
            if not cursor.fetchone():
                return {
                    "summary": {},
                    "meta": {
                        "message": "macro_prediction_accuracy table not populated yet",
                        "source_table": "global_predictions.db::macro_prediction_accuracy",
                    },
                }

            query = """
                SELECT source_category, target_market, lag_bucket,
                       accuracy_ema, sample_count, last_updated
                FROM macro_prediction_accuracy
                WHERE sample_count >= 3
            """
            params: List[Any] = []
            if category:
                query += " AND source_category = ?"
                params.append(category)
            if target_market:
                query += " AND target_market = ?"
                params.append(target_market)

            rows = conn.execute(query, params).fetchall()

            summary: Dict[str, Any] = {}
            for row in rows:
                src = row["source_category"]
                tgt = row["target_market"]
                lag = row["lag_bucket"]
                if src not in summary:
                    summary[src] = {}
                if tgt not in summary[src]:
                    summary[src][tgt] = {}
                summary[src][tgt][lag] = {
                    "accuracy": round(_safe_float(row["accuracy_ema"]), 3),
                    "samples": _safe_int(row["sample_count"]),
                    "last_updated": row["last_updated"],
                }

            total_cells = sum(
                len(lag_buckets)
                for targets in summary.values()
                for lag_buckets in targets.values()
            )
            total_samples = sum(
                bucket.get("samples", 0)
                for targets in summary.values()
                for lag_buckets in targets.values()
                for bucket in lag_buckets.values()
            )

            return {
                "summary": summary,
                "meta": {
                    "total_category_target_lag_cells": total_cells,
                    "total_samples": total_samples,
                    "sample_count_filter": "sample_count >= 3 (statistical significance)",
                    "source_table": "global_predictions.db::macro_prediction_accuracy",
                    "interpretation": (
                        "accuracy_ema is exponentially weighted hit rate. Higher = more reliable. "
                        "Filter cells by samples >= 10 for high-confidence evidence."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_prediction_accuracy failed")
        return mcp_error(
            MCPErrorCode.INTERNAL_ERROR,
            f"Failed to fetch prediction accuracy: {exc}",
        )


# ---------------------------------------------------------------------------
# P0 Tool 2: get_backtest_tuning_state — Layer B (자기 보정 증명)
# ---------------------------------------------------------------------------

def _get_backtest_tuning_state(
    category: Optional[str] = None,
    target_market: Optional[str] = None,
) -> Dict[str, Any]:
    """backtest_tuning 테이블에서 자기 보정 상태 조회."""
    if not GLOBAL_PREDICTIONS_DB.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"global_predictions.db not found at {GLOBAL_PREDICTIONS_DB}",
        )

    try:
        with _open_ro(GLOBAL_PREDICTIONS_DB) as conn:
            conn.row_factory = sqlite3.Row

            # 테이블 존재 확인
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='backtest_tuning'"
            )
            if not cursor.fetchone():
                return {
                    "tuning_entries": [],
                    "meta": {
                        "message": "backtest_tuning table not yet populated (run historical_backtester first)",
                        "source_table": "global_predictions.db::backtest_tuning",
                    },
                }

            query = "SELECT * FROM backtest_tuning WHERE 1=1"
            params: List[Any] = []
            if category:
                query += " AND category = ?"
                params.append(category)
            if target_market:
                query += " AND target_market = ?"
                params.append(target_market)
            query += " ORDER BY last_backtest DESC"

            rows = conn.execute(query, params).fetchall()

            entries = [
                {
                    "category": row["category"],
                    "target_market": row["target_market"],
                    "tuned_lag_hours": _safe_float(row["tuned_lag_hours"]),
                    "tuned_sensitivity": _safe_float(row["tuned_sensitivity"]),
                    "confidence": _safe_float(row["confidence"]),
                    "sample_count": _safe_int(row["sample_count"]),
                    "last_backtest": row["last_backtest"],
                }
                for row in rows
            ]

            return {
                "tuning_entries": entries,
                "meta": {
                    "total_entries": len(entries),
                    "source_table": "global_predictions.db::backtest_tuning",
                    "interpretation": (
                        "Each entry shows how our system auto-tuned lag_hours and sensitivity "
                        "based on real backtest results. High confidence + recent last_backtest "
                        "proves continuous self-calibration."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_backtest_tuning_state failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P0 Tool 3: get_monthly_accuracy_trend — Layer B (월별 성과 시계열)
# ---------------------------------------------------------------------------

def _get_monthly_accuracy_trend(
    category: Optional[str] = None,
    target_market: Optional[str] = None,
) -> Dict[str, Any]:
    """backtest_results 테이블에서 월별 정확도 시계열 조회."""
    if not GLOBAL_PREDICTIONS_DB.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"global_predictions.db not found at {GLOBAL_PREDICTIONS_DB}",
        )

    try:
        with _open_ro(GLOBAL_PREDICTIONS_DB) as conn:
            conn.row_factory = sqlite3.Row

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='backtest_results'"
            )
            if not cursor.fetchone():
                return {
                    "trend": [],
                    "meta": {
                        "message": "backtest_results table not yet populated",
                        "source_table": "global_predictions.db::backtest_results",
                    },
                }

            query = "SELECT * FROM backtest_results WHERE month IS NOT NULL AND month != 'all'"
            params: List[Any] = []
            if category:
                query += " AND category = ?"
                params.append(category)
            if target_market:
                query += " AND target_market = ?"
                params.append(target_market)
            query += " ORDER BY month ASC, category ASC, target_market ASC"

            rows = conn.execute(query, params).fetchall()

            trend = [
                {
                    "month": row["month"],
                    "category": row["category"],
                    "target_market": row["target_market"],
                    "lag_bucket": row["lag_bucket"],
                    "accuracy": _safe_float(row["accuracy"]),
                    "sample_count": _safe_int(row["sample_count"]),
                }
                for row in rows
            ]

            return {
                "trend": trend,
                "meta": {
                    "total_points": len(trend),
                    "source_table": "global_predictions.db::backtest_results",
                    "interpretation": (
                        "Monthly accuracy evolution. Use to verify sustained performance "
                        "(no recent degradation). Look for month-over-month stability or improvement."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_monthly_accuracy_trend failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P0 Tool 4: get_news_leading_indicator_performance — Layer A (예견 능력)
# ---------------------------------------------------------------------------

def _get_news_leading_indicator_performance(
    market_id: Optional[str] = None,
    min_sample_count: int = 3,
) -> Dict[str, Any]:
    """news/external_context.db::event_leading_scores에서 뉴스 선행 지표 성과 조회.

    데이터는 글로벌 news DB에 저장되며 market_id 컬럼으로 구분. market_id 미지정 시
    전체 시장 집계 반환.
    """
    # market_id 정규화 (None 허용 — 전체 조회)
    mid_normalized = None
    if market_id:
        mid_normalized = {
            "crypto": "coin_market",
            "coin": "coin_market",
            "coin_market": "coin_market",
            "kr": "kr_market",
            "kr_stock": "kr_market",
            "kr_market": "kr_market",
            "us": "us_market",
            "us_stock": "us_market",
            "us_market": "us_market",
        }.get(market_id.lower(), market_id)

    news_db = EXTERNAL_CONTEXT_DATA_DIR / "news" / "external_context.db"
    if not news_db.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"news/external_context.db not found at {news_db}",
        )

    try:
        with _open_ro(news_db) as conn:
            conn.row_factory = sqlite3.Row

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='event_leading_scores'"
            )
            if not cursor.fetchone():
                return {
                    "indicators": [],
                    "meta": {
                        "message": "event_leading_scores table not populated yet",
                        "source_table": "news/external_context.db::event_leading_scores",
                    },
                }

            cursor = conn.execute("PRAGMA table_info(event_leading_scores)")
            columns = {col[1] for col in cursor.fetchall()}

            select_cols = []
            for opt in (
                "event_type",
                "market_id",
                "news_type",
                "leading_score",
                "avg_lead_time_minutes",
                "avg_anticipation_ratio",
                "accuracy_pct",
                "sample_count",
                "updated_at",
            ):
                if opt in columns:
                    select_cols.append(opt)

            if not select_cols:
                return {
                    "indicators": [],
                    "meta": {"message": "event_leading_scores has no recognized columns"},
                }

            query = f"SELECT {', '.join(select_cols)} FROM event_leading_scores WHERE 1=1"
            params: List[Any] = []
            if "sample_count" in columns:
                query += " AND sample_count >= ?"
                params.append(min_sample_count)
            if mid_normalized and "market_id" in columns:
                query += " AND market_id = ?"
                params.append(mid_normalized)
            if "leading_score" in columns:
                query += " ORDER BY leading_score DESC"

            rows = conn.execute(query, params).fetchall()

            indicators = [dict(row) for row in rows]

            return {
                "indicators": indicators,
                "meta": {
                    "total_indicators": len(indicators),
                    "market_id": mid_normalized,
                    "market_id_original": market_id,
                    "min_sample_count": min_sample_count,
                    "source_table": "news/external_context.db::event_leading_scores",
                    "interpretation": (
                        "leading_score measures how often prices moved BEFORE news publication. "
                        "avg_lead_time_minutes shows average early detection window (higher = earlier). "
                        "accuracy_pct is direction accuracy. "
                        "Key evidence for 'we detect events before they happen'. "
                        "Pass market_id=None to see cross-market aggregate."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_news_leading_indicator_performance failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P0 Tool 5: get_news_causality_breakdown — Layer A (예견 vs 돌발 3분류)
# ---------------------------------------------------------------------------

def _get_news_causality_breakdown(
    market_id: str = "coin_market",
    days: int = 7,
) -> Dict[str, Any]:
    """news/external_context.db::news_causality_analysis에서 news_type 집계.

    주의: 뉴스 인과 데이터는 per-market DB가 아니라 news/external_context.db (글로벌)에
    저장되며, market_id 컬럼으로 구분된다. 따라서 market_id는 필터 조건으로 사용된다.

    market_id 정규화: "crypto", "coin" → "coin_market", "kr" → "kr_market", etc.
    """
    # market_id 정규화
    mid_normalized = {
        "crypto": "coin_market",
        "coin": "coin_market",
        "coin_market": "coin_market",
        "kr": "kr_market",
        "kr_stock": "kr_market",
        "kr_market": "kr_market",
        "us": "us_market",
        "us_stock": "us_market",
        "us_market": "us_market",
    }.get((market_id or "").lower(), market_id)

    # 뉴스 DB는 글로벌
    news_db = EXTERNAL_CONTEXT_DATA_DIR / "news" / "external_context.db"
    if not news_db.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"news/external_context.db not found at {news_db}",
        )

    try:
        with _open_ro(news_db) as conn:
            conn.row_factory = sqlite3.Row

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='news_causality_analysis'"
            )
            if not cursor.fetchone():
                return {
                    "breakdown": {},
                    "meta": {
                        "message": "news_causality_analysis table not populated yet",
                        "source_table": "news/external_context.db::news_causality_analysis",
                    },
                }

            cursor = conn.execute("PRAGMA table_info(news_causality_analysis)")
            columns = {col[1] for col in cursor.fetchall()}

            from datetime import datetime, timedelta, timezone
            cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)
            cutoff_iso = cutoff_dt.isoformat()

            wheres = []
            params: List[Any] = []
            if "market_id" in columns:
                wheres.append("market_id = ?")
                params.append(mid_normalized)
            if "computed_at" in columns:
                wheres.append("COALESCE(computed_at, '') >= ?")
                params.append(cutoff_iso)
            where_clause = ("WHERE " + " AND ".join(wheres)) if wheres else ""

            query = f"""
                SELECT news_type, COUNT(*) as cnt,
                       AVG(CASE WHEN anticipation_ratio IS NOT NULL THEN anticipation_ratio ELSE 0 END) as avg_anticipation,
                       AVG(CASE WHEN lead_time_minutes IS NOT NULL THEN lead_time_minutes ELSE 0 END) as avg_lead_time,
                       AVG(CASE WHEN confidence IS NOT NULL THEN confidence ELSE 0 END) as avg_confidence,
                       AVG(CASE WHEN precursor_score IS NOT NULL THEN precursor_score ELSE 0 END) as avg_precursor_score
                FROM news_causality_analysis
                {where_clause}
                GROUP BY news_type
                ORDER BY cnt DESC
            """
            rows = conn.execute(query, params).fetchall()

            breakdown = {
                row["news_type"] or "UNKNOWN": {
                    "count": _safe_int(row["cnt"]),
                    "avg_anticipation_ratio": round(_safe_float(row["avg_anticipation"]), 3),
                    "avg_lead_time_minutes": round(_safe_float(row["avg_lead_time"]), 1),
                    "avg_confidence": round(_safe_float(row["avg_confidence"]), 3),
                    "avg_precursor_score": round(_safe_float(row["avg_precursor_score"]), 3),
                }
                for row in rows
            }

            total = sum(v["count"] for v in breakdown.values())

            return {
                "breakdown": breakdown,
                "meta": {
                    "total_analyzed_news": total,
                    "window_days": days,
                    "market_id": mid_normalized,
                    "market_id_original": market_id,
                    "source_table": "news/external_context.db::news_causality_analysis",
                    "interpretation": (
                        "News type classification distinguishing 'anticipated' (scheduled + pre-move detected) "
                        "from 'surprise' (unexpected). Each row includes precursor_score measuring "
                        "pre-event cascade anomaly strength (macro→ETF→stock). Higher anticipation_ratio + "
                        "positive lead_time_minutes = stronger evidence of predictive capability."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_news_causality_breakdown failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P0 Tool 6: get_feature_governance_state — Layer C (거버넌스 투명성)
# ---------------------------------------------------------------------------

def _get_feature_governance_state(
    market_id: Optional[str] = None,
    status_filter: Optional[str] = None,
) -> Dict[str, Any]:
    """agent_history.db::feature_governance 테이블 조회."""
    db_path = _resolve_agent_history_db(market_id)
    if not db_path or not db_path.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"agent_history.db not found for market: {market_id or 'default'}",
            available_markets=list(AGENT_HISTORY_DB_PATHS.keys()),
        )

    try:
        with _open_ro(db_path) as conn:
            conn.row_factory = sqlite3.Row

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='feature_governance'"
            )
            if not cursor.fetchone():
                return {
                    "features": [],
                    "status_summary": {},
                    "meta": {
                        "message": "feature_governance table not populated yet (requires feature_gate_evaluator cycles)",
                        "source_table": "agent_history.db::feature_governance",
                    },
                }

            cursor = conn.execute("PRAGMA table_info(feature_governance)")
            columns = {col[1] for col in cursor.fetchall()}

            select_cols = ["feature_id", "feature_type", "source", "status"]
            for opt in ("sample_size", "delta_pf", "updated_at", "last_evaluated_at"):
                if opt in columns:
                    select_cols.append(opt)

            query = f"SELECT {', '.join(select_cols)} FROM feature_governance WHERE 1=1"
            params: List[Any] = []
            if status_filter:
                query += " AND status = ?"
                params.append(status_filter)
            if "updated_at" in columns:
                query += " ORDER BY updated_at DESC"

            rows = conn.execute(query, params).fetchall()
            features = [dict(row) for row in rows]

            # 상태별 카운트
            status_counts: Dict[str, int] = {}
            for f in features:
                st = f.get("status", "UNKNOWN") or "UNKNOWN"
                status_counts[st] = status_counts.get(st, 0) + 1

            return {
                "features": features,
                "status_summary": status_counts,
                "meta": {
                    "total_features": len(features),
                    "market_id": market_id or "coin (default)",
                    "source_table": "agent_history.db::feature_governance",
                    "interpretation": (
                        "3-track statistical validation (p-value based) lifecycle: "
                        "OBSERVATION → CONDITIONAL → ACTIVE (validated) or DEPRECATED (no edge). "
                        "Each feature goes through independent validation. ACTIVE status proves "
                        "the feature passed p-value test vs baseline."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_feature_governance_state failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P1 Tool 7: get_structure_calibration — Layer D (섹터/바스켓 적중률)
# ---------------------------------------------------------------------------

def _get_structure_calibration(
    market_id: Optional[str] = None,
    group_name: Optional[str] = None,
) -> Dict[str, Any]:
    """structure_calibration 테이블에서 섹터/바스켓 예측 적중률 집계."""
    if not STRUCTURE_LEARNING_DB.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"structure_learning.db not found at {STRUCTURE_LEARNING_DB}",
        )

    mid_norm = _normalize_market_id_for_structure(market_id) if market_id else None

    try:
        with _open_ro(STRUCTURE_LEARNING_DB) as conn:
            conn.row_factory = sqlite3.Row

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='structure_calibration'"
            )
            if not cursor.fetchone():
                return {
                    "calibration": [],
                    "meta": {
                        "message": "structure_calibration table not yet populated",
                        "source_table": "structure_learning.db::structure_calibration",
                    },
                }

            query = """
                SELECT market_id, group_name, interval, regime_bucket,
                       hit_rate_ema, avg_return_ema, sample_count, updated_at
                FROM structure_calibration
                WHERE sample_count >= 1
            """
            params: List[Any] = []
            if mid_norm:
                query += " AND market_id = ?"
                params.append(mid_norm)
            if group_name:
                query += " AND group_name = ?"
                params.append(group_name)
            query += " ORDER BY hit_rate_ema DESC, sample_count DESC"

            rows = conn.execute(query, params).fetchall()

            calibration = [
                {
                    "market_id": row["market_id"],
                    "group_name": row["group_name"],
                    "interval": row["interval"],
                    "regime_bucket": row["regime_bucket"],
                    "hit_rate_ema": round(_safe_float(row["hit_rate_ema"]), 3),
                    "avg_return_ema": round(_safe_float(row["avg_return_ema"]), 4),
                    "sample_count": _safe_int(row["sample_count"]),
                    "updated_at": row["updated_at"],
                }
                for row in rows
            ]

            total_samples = sum(c["sample_count"] for c in calibration)

            return {
                "calibration": calibration,
                "meta": {
                    "total_entries": len(calibration),
                    "total_samples": total_samples,
                    "market_id": mid_norm,
                    "group_name_filter": group_name,
                    "source_table": "structure_learning.db::structure_calibration",
                    "interpretation": (
                        "Level 2 (ETF/basket/sector) prediction calibration. Each row shows "
                        "hit_rate_ema (EMA hit rate) per (market, group, interval, regime_bucket). "
                        "avg_return_ema is exponentially-weighted average return. "
                        "Key evidence: we predict sector rotations and measure actual outcomes."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_structure_calibration failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P1 Tool 8: get_structure_validation_history — Layer D (일별 검증 트렌드)
# ---------------------------------------------------------------------------

def _get_structure_validation_history(
    market_id: Optional[str] = None,
    days: int = 90,
) -> Dict[str, Any]:
    """structure_validation_history 테이블에서 일별 검증 결과 시계열 조회."""
    if not STRUCTURE_LEARNING_DB.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"structure_learning.db not found at {STRUCTURE_LEARNING_DB}",
        )

    mid_norm = _normalize_market_id_for_structure(market_id) if market_id else None

    try:
        with _open_ro(STRUCTURE_LEARNING_DB) as conn:
            conn.row_factory = sqlite3.Row

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='structure_validation_history'"
            )
            if not cursor.fetchone():
                return {
                    "history": [],
                    "meta": {
                        "message": "structure_validation_history table not yet populated",
                        "source_table": "structure_learning.db::structure_validation_history",
                    },
                }

            from datetime import datetime, timedelta, timezone
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

            query = """
                SELECT market_id, date, total_predictions, total_validated,
                       total_correct, hit_rate, avg_return
                FROM structure_validation_history
                WHERE date >= ?
            """
            params: List[Any] = [cutoff]
            if mid_norm:
                query += " AND market_id = ?"
                params.append(mid_norm)
            query += " ORDER BY date DESC, market_id ASC"

            rows = conn.execute(query, params).fetchall()

            history = [
                {
                    "market_id": row["market_id"],
                    "date": row["date"],
                    "total_predictions": _safe_int(row["total_predictions"]),
                    "total_validated": _safe_int(row["total_validated"]),
                    "total_correct": _safe_int(row["total_correct"]),
                    "hit_rate": round(_safe_float(row["hit_rate"]), 3),
                    "avg_return": round(_safe_float(row["avg_return"]), 4),
                }
                for row in rows
            ]

            # 집계 통계 (overall 트렌드)
            total_predictions_all = sum(h["total_predictions"] for h in history)
            total_correct_all = sum(h["total_correct"] for h in history)
            overall_hit_rate = (
                round(total_correct_all / total_predictions_all, 3)
                if total_predictions_all > 0
                else 0.0
            )

            return {
                "history": history,
                "summary": {
                    "window_days": days,
                    "total_predictions": total_predictions_all,
                    "total_correct": total_correct_all,
                    "overall_hit_rate": overall_hit_rate,
                },
                "meta": {
                    "total_days": len(history),
                    "market_id": mid_norm,
                    "source_table": "structure_learning.db::structure_validation_history",
                    "interpretation": (
                        "Daily Level 2 prediction validation history. Use to verify sustained "
                        "performance over time. No degradation = consistent edge."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_structure_validation_history failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P1 Tool 9: get_strategy_leaderboard — Layer E (Level 1 실제 수익)
# ---------------------------------------------------------------------------

def _get_strategy_leaderboard(
    market_id: str = "crypto",
    top_n: int = 20,
    min_trades: int = 10,
) -> Dict[str, Any]:
    """_global_predictions.db::global_strategies에서 상위 전략 조회.

    빠른 경로: 미리 median 합성된 글로벌 전략 (~20-100 rows per market).
    ORDER BY profit_factor DESC (sharpe_ratio 컬럼이 없음).

    per-symbol DB 순회는 444+ DB라 너무 느려서 제외. 필요 시 별도 endpoint로 분리.
    """
    strategies_dir = LEARNING_STRATEGIES_DIRS.get(market_id.lower())
    if not strategies_dir or not strategies_dir.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"learning_strategies dir not found for market: {market_id}",
            available_markets=list(LEARNING_STRATEGIES_DIRS.keys()),
        )

    global_db = strategies_dir / "_global_predictions.db"
    if not global_db.exists():
        return {
            "leaderboard": [],
            "meta": {
                "message": "_global_predictions.db not found yet",
                "source_table": "learning_strategies/_global_predictions.db::global_strategies",
            },
        }

    try:
        with _open_ro(global_db) as conn:
            conn.row_factory = sqlite3.Row

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='global_strategies'"
            )
            if not cursor.fetchone():
                return {
                    "leaderboard": [],
                    "meta": {
                        "message": "global_strategies table not yet populated",
                        "source_table": "_global_predictions.db::global_strategies",
                    },
                }

            pragma = conn.execute("PRAGMA table_info(global_strategies)")
            cols = {c[1] for c in pragma.fetchall()}

            # 컬럼 가변성 대응: 있는 것만 select
            select_cols = []
            for opt in (
                "symbol",
                "interval",
                "strategy_type",
                "regime",
                "market_condition",
                "profit",
                "profit_factor",
                "win_rate",
                "trades_count",
                "quality_grade",
                "league",
                "direction_accuracy",
                "volatility_accuracy",
                "description",
                "created_at",
            ):
                if opt in cols:
                    select_cols.append(opt)

            if not select_cols:
                return {
                    "leaderboard": [],
                    "meta": {"message": "global_strategies has no recognized columns"},
                }

            # profit_factor가 ranking 기준. 없으면 profit, 없으면 win_rate
            order_col = None
            for c in ("profit_factor", "profit", "win_rate"):
                if c in cols:
                    order_col = c
                    break

            wheres = [f"{order_col} IS NOT NULL"] if order_col else []
            params: List[Any] = []
            if "trades_count" in cols:
                wheres.append("trades_count >= ?")
                params.append(min_trades)
            where_clause = ("WHERE " + " AND ".join(wheres)) if wheres else ""

            order_clause = f"ORDER BY {order_col} DESC" if order_col else ""

            query = f"""
                SELECT {', '.join(select_cols)}
                FROM global_strategies
                {where_clause}
                {order_clause}
                LIMIT ?
            """
            params.append(top_n)

            rows = conn.execute(query, params).fetchall()
            leaderboard = [dict(row) for row in rows]

            return {
                "leaderboard": leaderboard,
                "meta": {
                    "total_entries": len(leaderboard),
                    "market_id": market_id,
                    "min_trades_filter": min_trades,
                    "ranking_by": order_col or "unordered",
                    "source_table": "learning_strategies/_global_predictions.db::global_strategies",
                    "interpretation": (
                        "Top strategies (pre-aggregated global pool, synthesized from per-symbol "
                        "strategies via median). Each represents a pattern proven across many symbols. "
                        "Ranked by profit_factor (or fallback). Key evidence of Level 1 (individual "
                        "symbol/strategy) edge — these are the best patterns our RL engine learned."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_strategy_leaderboard failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P1 Tool 10: get_active_predictions — Layer B/C (현재 검증 대기 중)
# ---------------------------------------------------------------------------

def _get_active_predictions(
    target_market: Optional[str] = None,
    limit: int = 20,
) -> Dict[str, Any]:
    """macro_regime_predictions 테이블에서 검증 대기 중인 (outcome IS NULL) 예측 조회."""
    if not GLOBAL_PREDICTIONS_DB.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"global_predictions.db not found at {GLOBAL_PREDICTIONS_DB}",
        )

    try:
        with _open_ro(GLOBAL_PREDICTIONS_DB) as conn:
            conn.row_factory = sqlite3.Row

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='macro_regime_predictions'"
            )
            if not cursor.fetchone():
                return {
                    "predictions": [],
                    "meta": {
                        "message": "macro_regime_predictions table not populated yet",
                        "source_table": "global_predictions.db::macro_regime_predictions",
                    },
                }

            query = """
                SELECT source_category, source_regime_change, target_market,
                       predicted_regime_shift, lag_hours, confidence, created_at
                FROM macro_regime_predictions
                WHERE outcome IS NULL
            """
            params: List[Any] = []
            if target_market:
                query += " AND target_market = ?"
                params.append(target_market)
            query += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)

            rows = conn.execute(query, params).fetchall()

            predictions = [
                {
                    "source_category": row["source_category"],
                    "regime_change": row["source_regime_change"],
                    "target_market": row["target_market"],
                    "predicted_shift": row["predicted_regime_shift"],
                    "lag_hours": _safe_float(row["lag_hours"]),
                    "confidence": _safe_float(row["confidence"]),
                    "created_at": row["created_at"],
                }
                for row in rows
            ]

            return {
                "predictions": predictions,
                "meta": {
                    "total_active": len(predictions),
                    "limit": limit,
                    "target_market_filter": target_market,
                    "source_table": "global_predictions.db::macro_regime_predictions (outcome IS NULL)",
                    "interpretation": (
                        "Currently pending predictions waiting for validation. Shows that OneQAZ "
                        "is actively making forecasts right now. Combined with get_prediction_accuracy "
                        "this proves we don't just show past wins — we're on the record for future outcomes."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_active_predictions failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# P2 Tool 11: get_macro_influence_map — Transparency (인과 가설 노출)
# ---------------------------------------------------------------------------

# Inline fallback for AWS gateway deployment.
# AWS는 market/global_regime/ 코드 없이 데이터 디렉터리만 sync되므로,
# profiles.py import가 실패한다. 그 경우 이 inline copy를 사용.
# 변경 시 market/global_regime/profiles.py의 _MACRO_INFLUENCE_MAP과 동기화 필요.
_MACRO_INFLUENCE_MAP_FALLBACK: Dict[str, Dict[str, Dict[str, float]]] = {
    "vix": {
        "coin_market": {"lag_hours": 2, "sensitivity": 0.8},
        "us_market": {"lag_hours": 0.5, "sensitivity": 0.9},
        "kr_market": {"lag_hours": 4, "sensitivity": 0.6},
    },
    "bonds": {
        "us_market": {"lag_hours": 4, "sensitivity": 0.7},
        "kr_market": {"lag_hours": 8, "sensitivity": 0.5},
        "coin_market": {"lag_hours": 8, "sensitivity": 0.3},
    },
    "forex": {
        "kr_market": {"lag_hours": 2, "sensitivity": 0.8},
        "coin_market": {"lag_hours": 4, "sensitivity": 0.4},
        "us_market": {"lag_hours": 4, "sensitivity": 0.5},
    },
    "commodities": {
        "coin_market": {"lag_hours": 8, "sensitivity": 0.3},
        "kr_market": {"lag_hours": 8, "sensitivity": 0.4},
        "us_market": {"lag_hours": 4, "sensitivity": 0.5},
    },
    "credit": {
        "us_market": {"lag_hours": 24, "sensitivity": 0.6},
        "kr_market": {"lag_hours": 24, "sensitivity": 0.4},
        "coin_market": {"lag_hours": 24, "sensitivity": 0.3},
    },
    "liquidity": {
        "coin_market": {"lag_hours": 4, "sensitivity": 0.7},
        "us_market": {"lag_hours": 8, "sensitivity": 0.5},
        "kr_market": {"lag_hours": 8, "sensitivity": 0.4},
    },
    "inflation": {
        "us_market": {"lag_hours": 24, "sensitivity": 0.5},
        "kr_market": {"lag_hours": 24, "sensitivity": 0.4},
        "coin_market": {"lag_hours": 24, "sensitivity": 0.3},
    },
    "energy": {
        "us_market": {"lag_hours": 2, "sensitivity": 0.7},
        "kr_market": {"lag_hours": 6, "sensitivity": 0.5},
        "coin_market": {"lag_hours": 8, "sensitivity": 0.3},
    },
}


def _get_macro_influence_map(
    market_id: Optional[str] = None,
) -> Dict[str, Any]:
    """_MACRO_INFLUENCE_MAP을 직렬화하여 반환. 시장 필터 지원.

    auto_trader 환경에서는 market.global_regime.profiles의 live 버전을 사용하고,
    AWS gateway 환경(코드 없음)에서는 inline fallback을 사용한다.
    """
    source_label = "market/global_regime/profiles.py::_MACRO_INFLUENCE_MAP (live)"
    try:
        from market.global_regime.profiles import _MACRO_INFLUENCE_MAP
    except Exception:
        _MACRO_INFLUENCE_MAP = _MACRO_INFLUENCE_MAP_FALLBACK
        source_label = "trust_layer.py::_MACRO_INFLUENCE_MAP_FALLBACK (inline copy)"

    # 정규화: 'coin'/'crypto' → 'coin_market', etc.
    target_filter = None
    if market_id:
        target_filter = {
            "coin": "coin_market",
            "crypto": "coin_market",
            "coin_market": "coin_market",
            "kr": "kr_market",
            "kr_stock": "kr_market",
            "kr_market": "kr_market",
            "us": "us_market",
            "us_stock": "us_market",
            "us_market": "us_market",
        }.get(market_id.lower(), market_id)

    # dict 직렬화 (JSON-serializable 보장)
    serialized: Dict[str, Any] = {}
    for category, targets in _MACRO_INFLUENCE_MAP.items():
        if not isinstance(targets, dict):
            continue
        entry: Dict[str, Any] = {}
        for tgt_market, params in targets.items():
            if target_filter and tgt_market != target_filter:
                continue
            if isinstance(params, dict):
                entry[tgt_market] = {
                    "lag_hours": _safe_float(params.get("lag_hours")),
                    "sensitivity": _safe_float(params.get("sensitivity")),
                }
        if entry:
            serialized[category] = entry

    return {
        "influence_map": serialized,
        "meta": {
            "category_count": len(serialized),
            "target_market_filter": target_filter,
            "source": source_label,
            "interpretation": (
                "Causal hypothesis map: each macro category (bonds, forex, vix, etc.) mapped to "
                "target market with lag_hours (time to propagate) and sensitivity (strength 0-1). "
                "This is OneQAZ's pre-defined causal model. It is continuously tuned by "
                "backtest_tuning table (see get_backtest_tuning_state). Highest transparency "
                "evidence: our causal reasoning is visible and measurable."
            ),
            "related_tools": [
                "get_backtest_tuning_state (to see runtime calibration)",
                "get_prediction_accuracy (to see accuracy per category)",
            ],
        },
    }


# ---------------------------------------------------------------------------
# P2 Tool 12: explain_decision — Multi-source explanation
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# explain_decision helpers — narrative + meta builders
# ---------------------------------------------------------------------------

def _build_signal_narrative(sig: Dict[str, Any]) -> str:
    """시그널 dict를 사용자 친화 텍스트로 변환."""
    if not sig or sig.get("_error"):
        return "Signal data not available."
    action = (sig.get("action") or "unknown").lower()
    score = _safe_float(sig.get("signal_score"))
    confidence = _safe_float(sig.get("confidence"))
    pattern = sig.get("pattern_type") or ""
    cond = sig.get("market_condition") or ""

    # score_trace에서 alignment 추출
    alignment = 0.0
    trace = sig.get("score_trace")
    if isinstance(trace, dict):
        stage2 = trace.get("stage2_combined")
        if isinstance(stage2, dict):
            alignment = _safe_float(stage2.get("alignment"))

    parts = [f"Action: {action.upper()} (score={score:.3f}, confidence={confidence:.0%})."]
    if pattern:
        parts.append(f"Pattern: {pattern.replace('_', ' ')}.")
    if cond and cond != pattern:
        parts.append(f"Market condition: {cond}.")
    if isinstance(trace, dict):
        if abs(alignment) < 0.2:
            parts.append("Multi-timeframe disagreement (alignment near 0) — direction unclear.")
        elif alignment > 0.5:
            parts.append("Strong multi-timeframe alignment supporting the signal.")
        elif alignment < -0.5:
            parts.append("Strong multi-timeframe alignment against the signal — caution.")
    return " ".join(parts)


def _build_decisions_meta(decisions: List[Dict[str, Any]], signal: Dict[str, Any]) -> Dict[str, Any]:
    """recent_decisions가 비어있을 때 그 의미를 명시적으로 설명.

    핵심: virtual_trade_decisions는 sparse — 매매 조건이 충족된 경우만 row가 생긴다.
    빈 배열 = 시스템이 모니터링 중이지만 매매 안 함 (정상).
    """
    count = len(decisions or [])
    if count > 0:
        return {
            "count": count,
            "reason": "recent_trades_present",
            "interpretation": (
                f"System executed {count} recent trade decisions for this symbol. "
                "Each entry shows the decision (buy/sell/hold), thompson_score, and reason."
            ),
        }

    # 빈 경우 — signal action에 따라 의미 부여
    sig_action = (signal or {}).get("action", "") if isinstance(signal, dict) else ""
    sig_action = (sig_action or "").lower()

    if sig_action == "hold":
        reason = "no_trade_consistent_with_hold_signal"
        interp = (
            "No recent trade decisions, which is consistent with the current 'hold' signal. "
            "The signal engine continuously monitors this symbol but no entry/exit conditions "
            "have been met. This is normal for sideways/consolidation markets where the system "
            "intentionally stays out."
        )
    elif sig_action in ("buy", "sell"):
        reason = "signal_present_no_trade_executed"
        interp = (
            f"Signal is '{sig_action}' but no recent trade was executed. "
            "Possible reasons: position limit reached, risk gate triggered, "
            "Thompson sampling weighted the signal too low, or signal too recent. "
            "Check positions/limits before assuming an issue."
        )
    elif not sig_action:
        reason = "no_signal_data"
        interp = "No signal data found for this symbol; cannot interpret decision absence."
    else:
        reason = "no_recent_activity"
        interp = (
            f"No recent trade decisions for this symbol. Current signal action: '{sig_action}'."
        )

    return {"count": 0, "reason": reason, "interpretation": interp}


def _build_news_narrative(news: List[Dict[str, Any]]) -> str:
    """최근 뉴스 인과 데이터를 사용자 친화 요약으로 변환."""
    if not news:
        return "No recent news causality data for this market."
    anticipated = sum(1 for n in news if (n.get("news_type") or "").lower() == "anticipated")
    surprise = sum(1 for n in news if (n.get("news_type") or "").lower() == "surprise")
    valid_lead = [
        _safe_float(n.get("lead_time_minutes"))
        for n in news
        if n.get("lead_time_minutes") is not None
    ]
    parts = [
        f"Recent news for this market ({len(news)} items): "
        f"{anticipated} anticipated, {surprise} surprise."
    ]
    if valid_lead:
        avg_lead = sum(valid_lead) / len(valid_lead)
        parts.append(f"Average pre-event lead time: {avg_lead:.0f} minutes.")
    else:
        parts.append("Lead time data not available for these events.")
    return " ".join(parts)


def _build_overall_recommendation(
    signal: Dict[str, Any],
    decisions: List[Dict[str, Any]],
    news: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """3개 데이터를 결합해서 사용자 친화 권장 verdict 생성.

    Verdict 카테고리:
        STRONG_BUY     — buy signal + executed trades
        WEAK_BUY       — buy signal but no execution (suspicious)
        STRONG_SELL    — sell signal + executed trades
        WEAK_SELL      — sell signal but no execution
        WAIT_SIDEWAYS  — hold signal, sideways
        WAIT_NEWS      — hold signal but recent surprise news (정보 부족)
        UNCLEAR        — 데이터 부족
    """
    sig_action = ((signal or {}).get("action") or "").lower() if isinstance(signal, dict) else ""
    has_trades = len(decisions or []) > 0
    has_surprise_news = any(
        (n.get("news_type") or "").lower() == "surprise" for n in (news or [])
    )

    if sig_action == "buy" and has_trades:
        verdict = "STRONG_BUY"
        text = (
            "Buy signal aligned with recent executed trades. The system is actively "
            "buying — high conviction signal."
        )
    elif sig_action == "buy" and not has_trades:
        verdict = "WEAK_BUY"
        text = (
            "Buy signal present but the system has not executed recent trades. "
            "Verify position limits or risk gates before acting."
        )
    elif sig_action == "sell" and has_trades:
        verdict = "STRONG_SELL"
        text = "Sell signal aligned with recent sell executions. Active position exit."
    elif sig_action == "sell" and not has_trades:
        verdict = "WEAK_SELL"
        text = (
            "Sell signal present but no recent execution — possibly no open position to close."
        )
    elif sig_action == "hold":
        if has_surprise_news:
            verdict = "WAIT_NEWS"
            text = (
                "Sideways with recent surprise news. Wait for direction confirmation "
                "before acting — news impact still unfolding."
            )
        else:
            verdict = "WAIT_SIDEWAYS"
            text = (
                "Sideways consolidation with no clear edge. System recommends waiting "
                "until a directional signal emerges."
            )
    else:
        verdict = "UNCLEAR"
        text = "Insufficient signal data for a clear recommendation."

    return {"verdict": verdict, "text": text}


def _explain_decision(
    market_id: str,
    symbol: str,
) -> Dict[str, Any]:
    """특정 심볼의 최근 시그널/결정/뉴스 인과를 조합해서 설명.

    원천:
    - signals.score_trace (JSON) — 지표별 기여도
    - virtual_trade_decisions — thompson_score + regime_score + reason
    - news_causality_analysis — 해당 심볼 관련 최근 뉴스 (news DB)
    """
    import json as json_lib

    # 1) per-symbol signal DB에서 최신 시그널
    sig_dir = SIGNAL_DIR_PATHS.get(market_id.lower())
    if not sig_dir:
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"Signal dir not found for market: {market_id}",
            available_markets=list(SIGNAL_DIR_PATHS.keys()),
        )

    # signal DB는 symbol 소문자_signal.db
    sig_db = sig_dir / f"{symbol.lower()}_signal.db"
    if not sig_db.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"No signal DB for symbol {symbol} in {market_id}",
            hint=f"Try with lowercase symbol (e.g., btc, eth)",
        )

    signal_info: Dict[str, Any] = {}
    try:
        with _open_ro(sig_db) as conn:
            conn.row_factory = sqlite3.Row

            # 실제 컬럼 파악 (스키마 가변성 대응)
            pragma = conn.execute("PRAGMA table_info(signals)")
            sig_cols = {c[1] for c in pragma.fetchall()}

            wanted = [
                "timestamp",
                "signal_score",
                "confidence",
                "action",
                "reason",
                "score_trace",
            ]
            for opt in ("pattern_type", "market_regime", "market_condition", "recommended_strategy", "behavior_action"):
                if opt in sig_cols:
                    wanted.append(opt)

            select_clause = ", ".join(c for c in wanted if c in sig_cols)
            row = conn.execute(
                f"SELECT {select_clause} FROM signals ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            if row:
                signal_info = {k: (_safe_float(row[k]) if k in ("signal_score", "confidence") else row[k]) for k in row.keys() if k != "score_trace"}
                # score_trace JSON 파싱 (있으면)
                if "score_trace" in row.keys():
                    raw_trace = row["score_trace"]
                    if raw_trace:
                        try:
                            signal_info["score_trace"] = json_lib.loads(raw_trace)
                        except Exception:
                            signal_info["score_trace_raw"] = str(raw_trace)[:500]
    except Exception as exc:
        logger.debug(f"signal DB read failed: {exc}")
        signal_info["_error"] = str(exc)

    # 2) trading_system.db::virtual_trade_decisions 최근 결정
    trading_db = get_market_db_path(market_id)
    decision_info: List[Dict[str, Any]] = []
    if trading_db and trading_db.exists():
        try:
            with _open_ro(trading_db) as conn:
                conn.row_factory = sqlite3.Row
                # 테이블 존재 확인
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='virtual_trade_decisions'"
                )
                if cursor.fetchone():
                    pragma = conn.execute("PRAGMA table_info(virtual_trade_decisions)")
                    cols = {c[1] for c in pragma.fetchall()}
                    sym_col = "symbol" if "symbol" in cols else "coin"
                    select_cols = [sym_col, "decision", "signal_score", "timestamp", "reason"]
                    for opt in ("thompson_score", "regime_score", "regime_name", "ai_score", "ai_reason"):
                        if opt in cols:
                            select_cols.append(opt)
                    rows = conn.execute(
                        f"""
                        SELECT {', '.join(select_cols)}
                        FROM virtual_trade_decisions
                        WHERE {sym_col} = ? OR {sym_col} = ?
                        ORDER BY timestamp DESC
                        LIMIT 3
                        """,
                        (symbol, symbol.upper()),
                    ).fetchall()
                    decision_info = [dict(r) for r in rows]
        except Exception as exc:
            logger.debug(f"decisions read failed: {exc}")

    # 3) news_causality_analysis에서 해당 시장의 최근 ANTICIPATED 뉴스 3건
    news_db = EXTERNAL_CONTEXT_DATA_DIR / "news" / "external_context.db"
    news_info: List[Dict[str, Any]] = []
    if news_db.exists():
        try:
            mid_norm = {
                "coin": "coin_market", "crypto": "coin_market", "coin_market": "coin_market",
                "kr": "kr_market", "kr_stock": "kr_market", "kr_market": "kr_market",
                "us": "us_market", "us_stock": "us_market", "us_market": "us_market",
            }.get(market_id.lower(), market_id)

            with _open_ro(news_db) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='news_causality_analysis'"
                )
                if cursor.fetchone():
                    pragma = conn.execute("PRAGMA table_info(news_causality_analysis)")
                    cols = {c[1] for c in pragma.fetchall()}
                    select_bits = ["news_id", "news_type"]
                    for opt in (
                        "anticipation_ratio",
                        "lead_time_minutes",
                        "precursor_score",
                        "confidence",
                        "computed_at",
                    ):
                        if opt in cols:
                            select_bits.append(opt)
                    rows = conn.execute(
                        f"""
                        SELECT {', '.join(select_bits)}
                        FROM news_causality_analysis
                        WHERE market_id = ?
                        ORDER BY computed_at DESC
                        LIMIT 5
                        """,
                        (mid_norm,),
                    ).fetchall()
                    news_info = [dict(r) for r in rows]
        except Exception as exc:
            logger.debug(f"news causality read failed: {exc}")

    # Narrative + meta builders (사용자 친화 텍스트 + 빈 응답 의미 부여)
    signal_narrative = _build_signal_narrative(signal_info)
    decisions_meta = _build_decisions_meta(decision_info, signal_info)
    news_narrative = _build_news_narrative(news_info)
    overall_recommendation = _build_overall_recommendation(
        signal_info, decision_info, news_info
    )

    return {
        "symbol": symbol,
        "market_id": market_id,
        "signal": signal_info,
        "signal_narrative": signal_narrative,
        "recent_decisions": decision_info,
        "recent_decisions_meta": decisions_meta,
        "recent_news_causality": news_info,
        "news_narrative": news_narrative,
        "overall_recommendation": overall_recommendation,
        "meta": {
            "sources": [
                f"signals/{symbol.lower()}_signal.db::signals (score_trace JSON)",
                "trading_system.db::virtual_trade_decisions",
                "news/external_context.db::news_causality_analysis",
            ],
            "interpretation": (
                "Multi-layer explanation combining: (1) technical indicators via score_trace, "
                "(2) Thompson + regime scores from decision log, (3) news causality context. "
                "Use `overall_recommendation.verdict` for a quick verdict, or read the narrative "
                "fields (signal_narrative / news_narrative / recent_decisions_meta.interpretation) "
                "to present to end users without further LLM processing. Empty `recent_decisions` "
                "is normal — it means the symbol is being monitored but no trade conditions met."
            ),
        },
    }


# ---------------------------------------------------------------------------
# P2 Tool 13: get_cross_market_correlation
# ---------------------------------------------------------------------------

def _get_cross_market_correlation(
    source_market: Optional[str] = None,
    target_market: Optional[str] = None,
) -> Dict[str, Any]:
    """cross_market_correlation + cross_market_decoupling 테이블에서 크로스 마켓 관계 조회.

    데이터는 news/external_context.db에 있다.
    """
    news_db = EXTERNAL_CONTEXT_DATA_DIR / "news" / "external_context.db"
    if not news_db.exists():
        return mcp_error(
            MCPErrorCode.DB_NOT_FOUND,
            f"news/external_context.db not found at {news_db}",
        )

    try:
        with _open_ro(news_db) as conn:
            conn.row_factory = sqlite3.Row

            # cross_market_correlation
            correlations: List[Dict[str, Any]] = []
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='cross_market_correlation'"
            )
            if cursor.fetchone():
                query = """
                    SELECT source_market, source_scope, target_market, target_scope,
                           source_regime_change, target_regime_change,
                           lag_hours, count, correlation_score, last_seen_at, updated_at
                    FROM cross_market_correlation
                    WHERE 1=1
                """
                params: List[Any] = []
                if source_market:
                    query += " AND source_market = ?"
                    params.append(source_market)
                if target_market:
                    query += " AND target_market = ?"
                    params.append(target_market)
                query += " ORDER BY ABS(correlation_score) DESC LIMIT 100"

                for row in conn.execute(query, params).fetchall():
                    correlations.append({
                        "source_market": row["source_market"],
                        "source_scope": row["source_scope"],
                        "target_market": row["target_market"],
                        "target_scope": row["target_scope"],
                        "source_regime_change": row["source_regime_change"],
                        "target_regime_change": row["target_regime_change"],
                        "lag_hours": _safe_float(row["lag_hours"]),
                        "count": _safe_int(row["count"]),
                        "correlation_score": round(_safe_float(row["correlation_score"]), 3),
                        "last_seen_at": row["last_seen_at"],
                    })

            # cross_market_decoupling
            decoupling: List[Dict[str, Any]] = []
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='cross_market_decoupling'"
            )
            if cursor.fetchone():
                query = """
                    SELECT source_market, target_market, decoupling_index,
                           correlation_breakdown, regime_divergence, timing_lag_divergence, computed_at
                    FROM cross_market_decoupling
                    WHERE 1=1
                """
                params: List[Any] = []
                if source_market:
                    query += " AND source_market = ?"
                    params.append(source_market)
                if target_market:
                    query += " AND target_market = ?"
                    params.append(target_market)
                query += " ORDER BY computed_at DESC LIMIT 50"

                for row in conn.execute(query, params).fetchall():
                    decoupling.append({
                        "source_market": row["source_market"],
                        "target_market": row["target_market"],
                        "decoupling_index": round(_safe_float(row["decoupling_index"]), 3),
                        "correlation_breakdown": _safe_float(row["correlation_breakdown"]),
                        "regime_divergence": row["regime_divergence"],
                        "timing_lag_divergence": _safe_float(row["timing_lag_divergence"]),
                        "computed_at": row["computed_at"],
                    })

            return {
                "correlations": correlations,
                "decoupling": decoupling,
                "meta": {
                    "correlation_count": len(correlations),
                    "decoupling_count": len(decoupling),
                    "source_market_filter": source_market,
                    "target_market_filter": target_market,
                    "source_tables": [
                        "news/external_context.db::cross_market_correlation",
                        "news/external_context.db::cross_market_decoupling",
                    ],
                    "interpretation": (
                        "Cross-market lead-lag relationships and decoupling events. "
                        "correlation_score > 0 = co-movement, < 0 = inverse. "
                        "decoupling_index tracks divergence (BTC up + stocks down scenarios). "
                        "Key evidence for 'we understand how markets influence each other'."
                    ),
                },
            }
    except Exception as exc:
        logger.exception("get_cross_market_correlation failed")
        return mcp_error(MCPErrorCode.INTERNAL_ERROR, f"Query failed: {exc}")


# ---------------------------------------------------------------------------
# Tool 등록 함수
# ---------------------------------------------------------------------------

def register_trust_layer_tools(mcp, cache):
    """Trust Layer P0 Tool 등록 (6개)"""

    @mcp.tool()
    async def get_prediction_accuracy(
        category: str = None,
        target_market: str = None,
    ) -> Dict[str, Any]:
        """
        [역할] Retrieve OneQAZ's historical prediction accuracy across macro categories. Returns hit rate EMA with sample counts, filtered for statistical significance (sample_count >= 3). [호출 시점] AI agents evaluating OneQAZ credibility should call this FIRST. [선행 조건] 없음. [후속 추천] get_backtest_tuning_state to see self-calibration, get_monthly_accuracy_trend for time series. [주의] Returns empty if no backtests have completed yet. Use category/target_market filters to drill down.

        Args:
            category: Optional macro category filter (bonds, forex, vix, commodities, credit, liquidity, inflation, energy)
            target_market: Optional target market filter (coin_market, kr_market, us_market)
        """
        return await asyncio.to_thread(
            _get_prediction_accuracy,
            category=category,
            target_market=target_market,
        )

    @mcp.tool()
    async def get_backtest_tuning_state(
        category: str = None,
        target_market: str = None,
    ) -> Dict[str, Any]:
        """
        [역할] Show OneQAZ's continuous self-calibration state. Each entry shows how the system auto-tuned lag_hours and sensitivity based on real backtest outcomes. Proves we adapt parameters based on measured reality, not static heuristics. [호출 시점] After get_prediction_accuracy, to show the system updates itself. [선행 조건] get_prediction_accuracy 권장. [후속 추천] get_monthly_accuracy_trend. [주의] last_backtest timestamp indicates freshness of tuning.

        Args:
            category: Optional category filter
            target_market: Optional target market filter
        """
        return await asyncio.to_thread(
            _get_backtest_tuning_state,
            category=category,
            target_market=target_market,
        )

    @mcp.tool()
    async def get_monthly_accuracy_trend(
        category: str = None,
        target_market: str = None,
    ) -> Dict[str, Any]:
        """
        [역할] Monthly accuracy time series per (category, target, lag_bucket). Use to verify sustained performance — no recent degradation. [호출 시점] After get_prediction_accuracy and get_backtest_tuning_state. [선행 조건] get_prediction_accuracy 권장. [후속 추천] 없음 (trust chain 완료). [주의] Excludes 'all' month aggregates. Empty if backtest_results not populated.

        Args:
            category: Optional category filter
            target_market: Optional target market filter
        """
        return await asyncio.to_thread(
            _get_monthly_accuracy_trend,
            category=category,
            target_market=target_market,
        )

    @mcp.tool()
    async def get_news_leading_indicator_performance(
        market_id: str = "crypto",
        target_market: str = None,
        min_sample_count: int = 3,
    ) -> Dict[str, Any]:
        """
        [역할] Evidence that OneQAZ detects price moves BEFORE news publication. Returns leading_score, avg_lead_time_minutes, and accuracy_pct per event type. This is the strongest Trust Layer A evidence — proves we are not just reactive. [호출 시점] When AI evaluating predictive capability. [선행 조건] 없음. [후속 추천] get_news_causality_breakdown for 3-type classification. [주의] Table may be empty if no news events processed recently.

        Args:
            market_id: Market identifier (crypto, kr_stock, us_stock, etc.)
            target_market: Alias for market_id (backward compat)
            min_sample_count: Minimum sample count for statistical significance (default 3)
        """
        if target_market and not market_id:
            market_id = target_market
        elif target_market:
            market_id = target_market
        return await asyncio.to_thread(
            _get_news_leading_indicator_performance,
            market_id=market_id,
            min_sample_count=min_sample_count,
        )

    @mcp.tool()
    async def get_news_causality_breakdown(
        market_id: str = "crypto",
        days: int = 7,
    ) -> Dict[str, Any]:
        """
        [역할] 3-type news classification breakdown proving systematic discrimination between anticipated vs surprise events. ANTICIPATED = scheduled + pre-move detected, SURPRISE_WITH_PRECURSOR = cascade anomaly (macro→ETF→stock) caught early, SURPRISE = pure unexpected. [호출 시점] After get_news_leading_indicator_performance. [선행 조건] 없음. [후속 추천] market://{market_id}/external/causality for raw causality data. [주의] Window limited to recent days.

        Args:
            market_id: Market identifier
            days: Lookback window in days (default 7)
        """
        return await asyncio.to_thread(
            _get_news_causality_breakdown,
            market_id=market_id,
            days=days,
        )

    @mcp.tool()
    async def get_feature_governance_state(
        market_id: str = None,
        target_market: str = None,
        status_filter: str = None,
    ) -> Dict[str, Any]:
        """
        [역할] Current lifecycle state of external features (news, events) under 3-track statistical validation. Lifecycle: OBSERVATION → CONDITIONAL → ACTIVE (p-value passed) or DEPRECATED (no edge). Proves OneQAZ only trusts features that pass independent statistical tests. [호출 시점] When AI wants to verify meta-level trust (do they validate their own inputs?). [선행 조건] 없음. [후속 추천] 없음 (meta evidence). [주의] Empty if feature_gate_evaluator has not run cycles yet.

        Args:
            market_id: Optional market filter (defaults to coin)
            target_market: Alias for market_id (backward compat)
            status_filter: Optional status filter (OBSERVATION, CONDITIONAL, ACTIVE, DEPRECATED)
        """
        if target_market and not market_id:
            market_id = target_market
        return await asyncio.to_thread(
            _get_feature_governance_state,
            market_id=market_id,
            status_filter=status_filter,
        )

    # =======================================================================
    # P1 Tools (Layer D + E + active forecasts)
    # =======================================================================

    @mcp.tool()
    async def get_structure_calibration(
        market_id: str = None,
        group_name: str = None,
    ) -> Dict[str, Any]:
        """
        [역할] Level 2 (ETF/basket/sector) prediction calibration. Returns hit_rate_ema per (market, group, interval, regime_bucket) with sample counts. Proves systematic edge at sector rotation level. [호출 시점] When AI wants to see Layer D (structure) evidence. [선행 조건] 없음. [후속 추천] get_structure_validation_history for daily trend. [주의] Empty until structure learning cycles complete.

        Args:
            market_id: Optional market filter (crypto, kr_stock, us_stock)
            group_name: Optional group/sector filter (e.g., layer1, defi, sector, broad_index)
        """
        return await asyncio.to_thread(
            _get_structure_calibration,
            market_id=market_id,
            group_name=group_name,
        )

    @mcp.tool()
    async def get_structure_validation_history(
        market_id: str = None,
        days: int = 90,
    ) -> Dict[str, Any]:
        """
        [역할] Daily validation history of Level 2 structure predictions. Each row shows hit_rate for a specific day, enabling time-series verification of sustained performance. [호출 시점] After get_structure_calibration. [선행 조건] 없음. [후속 추천] get_monthly_accuracy_trend for macro-level comparison. [주의] Returns overall_hit_rate summary across the window.

        Args:
            market_id: Optional market filter
            days: Lookback window in days (default 90)
        """
        return await asyncio.to_thread(
            _get_structure_validation_history,
            market_id=market_id,
            days=days,
        )

    @mcp.tool()
    async def get_strategy_leaderboard(
        market_id: str = "crypto",
        target_market: str = None,
        top_n: int = 20,
        min_trades: int = 10,
    ) -> Dict[str, Any]:
        """
        [역할] Top RL-learned strategies ranked by sharpe_ratio across all symbols in a market. Layer E evidence — proves Level 1 edge at individual symbol/strategy level. Scans per-symbol learning_strategies DBs. [호출 시점] Final trust validation step — show actual profitable strategies. [선행 조건] 없음. [후속 추천] market://{market_id}/signals/summary for live signals. [주의] min_trades filter ensures statistical validity.

        Args:
            market_id: Market identifier (crypto, kr_stock, us_stock)
            target_market: Alias for market_id (backward compat)
            top_n: Top N strategies to return (default 20)
            min_trades: Minimum trades count for inclusion (default 10)
        """
        if target_market and not market_id:
            market_id = target_market
        return await asyncio.to_thread(
            _get_strategy_leaderboard,
            market_id=market_id,
            top_n=top_n,
            min_trades=min_trades,
        )

    @mcp.tool()
    async def get_active_predictions(
        target_market: str = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """
        [역할] Currently pending predictions (outcome IS NULL) — shows OneQAZ is actively making forecasts right now. Combined with get_prediction_accuracy, proves we don't cherry-pick past wins; we're on record for future outcomes. [호출 시점] To verify ongoing prediction activity. [선행 조건] 없음. [후속 추천] get_prediction_accuracy to see historical hit rates on similar predictions. [주의] Returns most recent first.

        Args:
            target_market: Optional target market filter (coin_market, kr_market, us_market)
            limit: Max active predictions to return (default 20)
        """
        return await asyncio.to_thread(
            _get_active_predictions,
            target_market=target_market,
            limit=limit,
        )

    # =======================================================================
    # P2 Tools (Transparency / Explanation)
    # =======================================================================

    @mcp.tool()
    async def get_macro_influence_map(
        market_id: str = None,
    ) -> Dict[str, Any]:
        """
        [역할] Expose OneQAZ's pre-defined causal hypothesis map: each macro category (bonds, forex, vix, credit, liquidity, inflation, commodities, energy) mapped to target market with lag_hours + sensitivity. Highest transparency — our causal reasoning is visible and measurable. [호출 시점] When AI wants to understand WHY we make certain predictions. [선행 조건] 없음. [후속 추천] get_backtest_tuning_state to see runtime calibration of these hypotheses. [주의] Static hypothesis — see tuning state for current adjustments.

        Args:
            market_id: Optional target market filter (coin_market, kr_market, us_market)
        """
        return await asyncio.to_thread(
            _get_macro_influence_map,
            market_id=market_id,
        )

    @mcp.tool()
    async def explain_decision(
        market_id: str,
        symbol: str,
    ) -> Dict[str, Any]:
        """
        [역할] Multi-layer explanation for a specific symbol's recent signal. Combines (1) technical score_trace from signals DB, (2) Thompson + regime scores from virtual_trade_decisions, (3) news causality context. Full 'why' for AI to present to users. [호출 시점] When user asks 'why is this a buy/sell?'. [선행 조건] get_signals 또는 get_latest_decisions로 대상 심볼 파악. [후속 추천] 없음 (설명 완료). [주의] Symbol must match signal DB filename (lowercase).

        Args:
            market_id: Market identifier (crypto, kr_stock, us_stock)
            symbol: Symbol to explain (e.g., btc, eth, 005930)
        """
        return await asyncio.to_thread(
            _explain_decision,
            market_id=market_id,
            symbol=symbol,
        )

    @mcp.tool()
    async def get_cross_market_correlation(
        source_market: str = None,
        target_market: str = None,
    ) -> Dict[str, Any]:
        """
        [역할] Cross-market lead-lag relationships and decoupling events. Shows how markets influence each other (correlations) and when they diverge (decoupling, e.g., BTC up + stocks down). [호출 시점] When analyzing macro regime changes or divergent signals. [선행 조건] 없음. [후속 추천] get_macro_influence_map for static causal hypotheses. [주의] Correlation data may be empty until sufficient regime changes accumulate.

        Args:
            source_market: Optional source market filter
            target_market: Optional target market filter
        """
        return await asyncio.to_thread(
            _get_cross_market_correlation,
            source_market=source_market,
            target_market=target_market,
        )

    logger.info("  [OK] Trust Layer Tools registered (6 P0 + 4 P1 + 3 P2 = 13 tools)")
