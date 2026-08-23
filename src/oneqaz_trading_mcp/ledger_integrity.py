# -*- coding: utf-8 -*-
"""
Prediction Ledger Integrity (해시 체인)
=======================================
[2026-07-08] B2AI 신뢰 루프의 빠진 반쪽 — "예측 원장이 사후 수정되지 않았음"의
외부 검증 수단. market_global.macro_regime_predictions 는 resolve 시 같은 행을
UPDATE 하는 mutable 원장이라, 외부 AI 가 "backfill/사후수정 아님"을 믿을 근거가
없었다 (감사 판정 3자 공통 인용거절 1순위 사유).

동작:
- 매일 1회, "완결된 날"(UTC 오늘 이전)의 created/resolved 행들을 정규화 문자열로
  직렬화해 SHA-256 해시를 계산하고, 전일 chain_hash 와 연결해 체인을 만든다.
- 체인은 mcp_analytics.prediction_ledger_hashes 에 append-only 로 쌓이고
  /ledger 라우트 + get_ledger_integrity tool 로 외부에 공개된다.
- 외부 관찰자(AI/사람)가 해시를 아카이브해두면, 이후 원장 행이 조용히 바뀔 경우
  get_resolved_predictions 원시 행으로 재계산한 해시가 어긋나 탬퍼링이 드러난다.

정규화 레시피(CANONICAL_RECIPE)는 공개 계약이다 — 외부에서 재계산 가능해야
하므로 변경 시 recipe_version 을 올리고 구버전 행은 재계산하지 않는다.

일자 버킷: 저장된 ISO-8601 문자열의 앞 10자 (created_at/resolved_at 은 전 행
UTC '+00:00' 포맷, 2026-07-08 실측). timezone 해석 없이 문자열 prefix 로만
버킷팅해 모호성을 제거한다.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger("MarketMCP")

RECIPE_VERSION = "v1"

CANONICAL_RECIPE = (
    "recipe v1 — For UTC day D (= first 10 chars of stored ISO-8601 timestamp): "
    "created_hash = SHA256 of '\\n'-joined canonical CREATED rows sorted by id, where each row is "
    "'id|source_category|source_regime_change|target_market|predicted_regime_shift|"
    "lag_hours(%.10g)|confidence(%.10g, empty if null)|created_at' for rows with created_at day = D. "
    "resolved_hash = SHA256 of '\\n'-joined 'id|resolved_at|outcome|actual_regime_shift' "
    "sorted by id, for rows with resolved_at day = D. Empty set hashes to SHA256 of empty string. "
    "chain_hash = SHA256 of 'prev_chain_hash|D|created_count|created_hash|resolved_count|resolved_hash' "
    "(genesis prev_chain_hash = 'GENESIS'). All strings UTF-8. "
    "Raw rows are independently fetchable via the get_resolved_predictions tool, "
    "so any third party can recompute and verify the chain."
)


def _fmt_float(v: Any) -> str:
    if v is None:
        return ""
    try:
        return format(float(v), ".10g")
    except (TypeError, ValueError):
        return str(v)


def _canon_created(row: Dict[str, Any]) -> str:
    return "|".join([
        str(row["id"]),
        str(row["source_category"] or ""),
        str(row["source_regime_change"] or ""),
        str(row["target_market"] or ""),
        str(row["predicted_regime_shift"] or ""),
        _fmt_float(row["lag_hours"]),
        _fmt_float(row["confidence"]),
        str(row["created_at"] or ""),
    ])


def _canon_resolved(row: Dict[str, Any]) -> str:
    return "|".join([
        str(row["id"]),
        str(row["resolved_at"] or ""),
        str(row["outcome"] or ""),
        str(row["actual_regime_shift"] or ""),
    ])


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _open_predictions_ro():
    """market_global 예측 원장 읽기 연결 (trust_layer 와 동일한 PG 라우팅)."""
    from oneqaz_trading_mcp.shared.db.compat import connect_readonly
    from oneqaz_trading_mcp.config import GLOBAL_REGIME_DIR
    return connect_readonly(str(GLOBAL_REGIME_DIR / "global_predictions.db"), timeout=10)


def _open_ledger_rw():
    """mcp_analytics 해시 체인 연결 (mcps 는 이 스키마에 이미 write — 교차 write 신설 아님)."""
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    return open_schema_connection("mcp_analytics")


def compute_day(pred_conn, day: str) -> Dict[str, Any]:
    """하루치 created/resolved 해시 계산 (체인 제외 — 순수 함수)."""
    created_rows = pred_conn.execute(
        "SELECT id, source_category, source_regime_change, target_market, "
        "       predicted_regime_shift, lag_hours, confidence, created_at "
        "FROM macro_regime_predictions "
        "WHERE substr(created_at, 1, 10) = ? ORDER BY id",
        (day,),
    ).fetchall()
    resolved_rows = pred_conn.execute(
        "SELECT id, resolved_at, outcome, actual_regime_shift "
        "FROM macro_regime_predictions "
        "WHERE resolved_at IS NOT NULL AND substr(resolved_at, 1, 10) = ? ORDER BY id",
        (day,),
    ).fetchall()
    created_canon = "\n".join(_canon_created(dict(r)) for r in created_rows)
    resolved_canon = "\n".join(_canon_resolved(dict(r)) for r in resolved_rows)
    return {
        "day": day,
        "created_count": len(created_rows),
        "resolved_count": len(resolved_rows),
        "created_hash": _sha256(created_canon),
        "resolved_hash": _sha256(resolved_canon),
    }


def _chain_hash(prev: str, d: Dict[str, Any]) -> str:
    return _sha256(
        f"{prev}|{d['day']}|{d['created_count']}|{d['created_hash']}|"
        f"{d['resolved_count']}|{d['resolved_hash']}"
    )


def _next_day(day: str) -> str:
    """tz-free 날짜 +1 (mktime 은 로컬타임 해석이라 컨테이너 tz 에 따라 어긋남)."""
    from datetime import date, timedelta
    return (date.fromisoformat(day) + timedelta(days=1)).isoformat()


def run_due_days(max_days: int = 400) -> List[str]:
    """미계산 완결일 전부를 순서대로 계산·체인 연결·INSERT.

    완결일 = 문자열 day < 오늘(UTC). 저장 포맷이 UTC 라 오늘 이전 날짜의
    행 집합은 더 이상 늘지 않는다 (resolved_at 은 항상 기록 시각 = 과거로
    소급 불가). 반환: 새로 계산된 day 목록.
    """
    today_utc = time.strftime("%Y-%m-%d", time.gmtime())
    computed: List[str] = []

    ledger = _open_ledger_rw()
    try:
        last = ledger.execute(
            "SELECT day, chain_hash FROM prediction_ledger_hashes ORDER BY day DESC LIMIT 1"
        ).fetchone()
        prev_day = last["day"] if last else None
        prev_chain = last["chain_hash"] if last else "GENESIS"

        preds = _open_predictions_ro()
        try:
            if prev_day is None:
                first = preds.execute(
                    "SELECT MIN(substr(created_at, 1, 10)) AS d FROM macro_regime_predictions"
                ).fetchone()
                if not first or not first["d"]:
                    return computed
                start_day = first["d"]
            else:
                start_day = _next_day(prev_day)

            day = start_day
            while day < today_utc and len(computed) < max_days:
                d = compute_day(preds, day)
                chained = _chain_hash(prev_chain, d)
                ledger.execute(
                    "INSERT INTO prediction_ledger_hashes "
                    "(day, created_count, resolved_count, created_hash, resolved_hash, "
                    " prev_chain_hash, chain_hash, computed_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        d["day"], d["created_count"], d["resolved_count"],
                        d["created_hash"], d["resolved_hash"],
                        prev_chain, chained, int(time.time()),
                    ),
                )
                ledger.commit()
                prev_chain = chained
                computed.append(day)
                day = _next_day(day)
        finally:
            preds.close()
    finally:
        ledger.close()

    if computed:
        logger.info("[ledger] prediction ledger hashes computed: %s..%s (%d days)",
                    computed[0], computed[-1], len(computed))
    return computed


def snapshot_accuracy_cells() -> int:
    """[2026-07-08] 정확도 셀 일별 스냅샷 — Wilson CI 수축 이력의 원료.

    오늘(UTC) 스냅샷이 이미 있으면 no-op. macro_prediction_accuracy 현재 상태를
    (day, cell) 키로 INSERT (ON CONFLICT DO NOTHING — 하루 1회 동결).
    """
    import math

    today_utc = time.strftime("%Y-%m-%d", time.gmtime())
    ledger = _open_ledger_rw()
    try:
        exists = ledger.execute(
            "SELECT 1 FROM accuracy_cell_snapshots WHERE day = ? LIMIT 1", (today_utc,)
        ).fetchone()
        if exists:
            return 0

        preds = _open_predictions_ro()
        try:
            rows = preds.execute(
                "SELECT source_category, target_market, lag_bucket, accuracy_ema, "
                "       sample_count, cumulative_correct, cumulative_total "
                "FROM macro_prediction_accuracy WHERE sample_count >= 3"
            ).fetchall()
        finally:
            preds.close()

        n_inserted = 0
        now = int(time.time())
        for r in rows:
            total = int(r["cumulative_total"] or 0)
            correct = int(r["cumulative_correct"] or 0)
            acc = (correct / total) if total > 0 else None
            lo = hi = None
            if acc is not None and total > 0:
                z = 1.96
                denom = 1 + z * z / total
                center = (acc + z * z / (2 * total)) / denom
                half = (z * math.sqrt(acc * (1 - acc) / total + z * z / (4 * total * total))) / denom
                lo, hi = max(0.0, center - half), min(1.0, center + half)
            ledger.execute(
                "INSERT OR IGNORE INTO accuracy_cell_snapshots "
                "(day, source_category, target_market, lag_bucket, sample_count, correct, "
                " accuracy, wilson_low, wilson_high, accuracy_ema, snapshot_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    today_utc, r["source_category"], r["target_market"], r["lag_bucket"],
                    total, correct, acc, lo, hi,
                    float(r["accuracy_ema"]) if r["accuracy_ema"] is not None else None,
                    now,
                ),
            )
            n_inserted += 1
        ledger.commit()
        if n_inserted:
            logger.info("[ledger] accuracy cell snapshot: %s (%d cells)", today_utc, n_inserted)
        return n_inserted
    finally:
        ledger.close()


_CALIBRATION_MARKETS = {
    "crypto": "market_coin",
    "kr_stock": "market_kr",
    "us_stock": "market_us",
}

_CALIBRATION_BUCKET_SQL = """
    CASE WHEN s.confidence < 0.5 THEN '[0.0,0.5)'
         WHEN s.confidence < 0.6 THEN '[0.5,0.6)'
         WHEN s.confidence < 0.7 THEN '[0.6,0.7)'
         WHEN s.confidence < 0.8 THEN '[0.7,0.8)'
         WHEN s.confidence < 0.9 THEN '[0.8,0.9)'
         ELSE '[0.9,1.0]' END
"""


def snapshot_signal_calibration() -> int:
    """[2026-07-20 RCA T5] 시그널 confidence 캘리브레이션 일별 스냅샷.

    signal_predictions(판정 원장: is_correct ∈ {0,1}) ⋈ signals(surface confidence)
    를 (market, interval, confidence 버킷) 별 집계해 signal_calibration_daily 에
    동결. 관측창 = 스냅샷 시점의 signal_predictions 보존 창 (ts_min/ts_max 기록).
    조인 집계가 수십 초 걸리므로 요청 경로가 아닌 이 일별 스레드에서 수행.
    """
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection

    today_utc = time.strftime("%Y-%m-%d", time.gmtime())
    ledger = _open_ledger_rw()
    try:
        # [2026-07-21 RCA C2] v1(raw confidence) + v2(outcome 섀도우 confidence_v2)
        # 이원 집계 — 승격 판정(ECE 비교)의 채점판. v2 는 confidence_v2 NOT NULL 행만.
        _VARIANT_COL = {"v1": "s.confidence", "v2": "s.confidence_v2"}

        n_inserted = 0
        now = int(time.time())
        for market_id, schema in _CALIBRATION_MARKETS.items():
            # [2026-07-29] 멱등 가드를 day 전역 → (day, market_id) 단위로 수리.
            # 과거 day 전역 체크는 한 시장의 집계 실패 후 다른 시장이 커밋되면
            # 그 날이 잠겨 실패 시장이 영구 결손되는 구조였다 (crypto 07-22/27/28 실증).
            # 시간당 루프가 재진입하므로 실패 시장은 당일 내 자동 재시도된다.
            # [2026-08-12 A2] variant 스코프 — score_* variant(별도 스냅샷)가 먼저
            # 커밋돼도 이 confidence 스냅샷이 그 날을 '완료'로 오판하지 않게 한다.
            exists = ledger.execute(
                "SELECT 1 FROM signal_calibration_daily WHERE day = ? AND market_id = ? "
                "AND variant IN ('v1', 'v2') LIMIT 1",
                (today_utc, market_id),
            ).fetchone()
            # [2026-08-10 r3] 멱등 SELECT 가 연 트랜잭션을 즉시 종료 — 이대로 두면
            # 아래 시장 집계(최대 120s × 2변형) 동안 ledger 세션이 INTRANS 로 방치돼
            # idle_in_tx 60s 가드(영구고정)에 킬 → INSERT 시점 폭발이 잔존했다
            # (r2 적재 후에도 재현 — 08-10 21:48 실측). 집계 동안 ledger 는
            # 무트랜잭션 idle 로 대기시킨다 (idle 은 킬 대상 아님).
            ledger.commit()
            if exists:
                continue
            try:
                conn = open_schema_connection(schema, readonly=True)
            except Exception:
                logger.exception("[ledger] calibration: %s open failed", schema)
                continue
            try:
                # [2026-08-04] 풀 기본 statement_timeout(30s) < 집계 비용(coin signals
                # COUNT 단독 15s+ 실측)이라 시장별 성장 순서대로 매시간 타임아웃→continue
                # 무음 결손 (crypto 08-02+/us 07-30+ 실증). 이 집계 한정 국소 상향.
                # 롤백: env CALIB_SNAPSHOT_STMT_TIMEOUT='30s'.
                try:
                    conn.execute(
                        f"SET statement_timeout = '{os.environ.get('CALIB_SNAPSHOT_STMT_TIMEOUT', '120s')}'"
                    )
                except Exception:
                    logger.warning("[ledger] calibration: %s statement_timeout 설정 실패", schema)
                # [2026-08-10 r2] 관측창 하한 = signal_predictions 보존창(env 연동)
                # + 10일 여유 — 보존창보다 넓어 결과 불변, signals(무보존 누적) 풀
                # 조인 스캔만 차단 (coin 08-04 타임아웃 실증의 근본 원인). PG 는
                # 등가류에서 부등호를 전파하지 않으므로 sp/s 양쪽에 명시적으로 건다.
                _retention_sec = int(
                    os.environ.get("ACCURACY_LEDGER_RETENTION_SEC", str(30 * 86400))
                )
                _bound_ts = int(time.time()) - (_retention_sec + 10 * 86400)
                variant_rows = {}
                for variant, col in _VARIANT_COL.items():
                    bucket_sql = _CALIBRATION_BUCKET_SQL.replace("s.confidence", col)
                    variant_rows[variant] = conn.execute(
                        f"""
                        SELECT sp."interval" AS itv,
                               {bucket_sql} AS bucket,
                               COUNT(*) AS n,
                               SUM(sp.is_correct) AS hits,
                               MIN(sp.timestamp) AS ts_min,
                               MAX(sp.timestamp) AS ts_max
                          FROM signal_predictions sp
                          JOIN signals s
                            ON s.symbol = sp.symbol
                           AND s."interval" = sp."interval"
                           AND s.timestamp = sp.timestamp
                         WHERE sp.is_correct IN (0, 1)
                           AND {col} IS NOT NULL
                           AND sp.timestamp > {_bound_ts}
                           AND s.timestamp > {_bound_ts}
                         GROUP BY 1, 2
                        """
                    ).fetchall()
            except Exception:
                logger.exception("[ledger] calibration aggregate failed: %s", market_id)
                continue
            finally:
                # [2026-08-04] 풀 반환 전 원복 — 국소 상향이 다른 소비자에 누수되지 않게.
                try:
                    conn.execute("SET statement_timeout = DEFAULT")
                except Exception:
                    pass  # 원복 실패 시 conn.close 가 세션을 정리 — 명시적 무해 삼킴
                conn.close()

            for variant, rows in variant_rows.items():
                for r in rows:
                    ledger.execute(
                        "INSERT OR IGNORE INTO signal_calibration_daily "
                        "(day, market_id, \"interval\", bucket, variant, n, hits, ts_min, ts_max, snapshot_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (today_utc, market_id, r["itv"], r["bucket"], variant,
                         int(r["n"]), int(r["hits"] or 0),
                         r["ts_min"], r["ts_max"], now),
                    )
                    n_inserted += 1
            ledger.commit()

        if n_inserted:
            logger.info("[ledger] signal calibration snapshot: %s (%d rows)",
                        today_utc, n_inserted)
        return n_inserted
    finally:
        ledger.close()


# [2026-08-12 3단계 A2] POLICY 2026-08-11-r1 활성 시각 (trade/core/score_v2.py
# _ERA_START_TS 와 동일 값 — trade→mcps 역방향 import 회피의 의도적 중복,
# confidence_v2 PAV 사본 관례). 이 경계 이전은 라벨 정직화 전 오염 시대.
_SCORE_ERA_START_TS = 1786441260


def snapshot_score_calibration() -> int:
    """[2026-08-12 3단계 A2] score_v2 채점판 일별 스냅샷 (설계 정본 _update_log_2026-08-12 §2).

    X2(raw 점수)와 score_v2(p̂ 섀도우, 기록 시점 동결값)의 실현 적중률을
    (day, market, bucket, variant) 로 동결 — A3 승격 게이트(ECE+선별력 스프레드
    7연속 우위 + 사람 판정)의 원장. variant 3종:
      score_raw_h30 / score_raw_h24 — bucket = X2 0.1 버킷. 공정 v1 기준선
        (버킷 경험 적중률 — "raw X2 를 확률로 간주" 허수아비 비교 금지, r2 리뷰).
        30m/24h 이원은 판정 지평 택일(§A1)의 비교 원장.
      score_v2_h24 — bucket = p̂ 0.05 버킷. 'NULL' 버킷 행 = score_v2 미기록
        행 수 (fail-open 폴백 발동률 관찰 — A3 의 NULL 폴백 설계 입력).
    코호트: combined · predicted_direction='UP' · score>=0 · era 이후 ·
      (symbol, 30분 창) 클러스터당 최신 1행 — 같은 판정 결과를 공유하는
      사이클 중복 행의 자기상관 보정 (r2 리뷰: 유효 표본 과대 방지).
    """
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection

    today_utc = time.strftime("%Y-%m-%d", time.gmtime())
    ledger = _open_ledger_rw()
    try:
        # [리뷰 r1 반영] 이동 바운드 — era 고정 하한만 두면 signals(무보존 누적) 스캔이
        # 선형 성장해 120s 타임아웃 무음 결손 재발 (confidence 스냅샷 08-10 r2 동일 함정).
        # sp 보존창(30d)이 실코호트를 물리 바운드하므로 결과 불변.
        _retention_sec = int(os.environ.get("ACCURACY_LEDGER_RETENTION_SEC", str(30 * 86400)))
        _bound_ts = max(_SCORE_ERA_START_TS, int(time.time()) - (_retention_sec + 10 * 86400))
        _DEDUP_CTE = f"""
            WITH dedup AS (
                SELECT DISTINCT ON (sp.symbol, FLOOR(sp.timestamp / 1800.0))
                       sp.signal_score, sp.is_correct, sp.is_correct_24h,
                       sp.timestamp, s.score_v2
                  FROM signal_predictions sp
                  LEFT JOIN signals s
                    ON s.symbol = sp.symbol
                   AND s."interval" = sp."interval"
                   AND s.timestamp = sp.timestamp
                   AND s.timestamp > {_bound_ts}
                 WHERE sp."interval" = 'combined'
                   AND sp.predicted_direction = 'UP'
                   AND sp.signal_score >= 0
                   AND sp.timestamp > {_bound_ts}
                 ORDER BY sp.symbol, FLOOR(sp.timestamp / 1800.0), sp.timestamp DESC, sp.id DESC
            )
        """
        # 마지막 버킷은 [0.6,∞) 개방 — 라벨도 정직하게 (0.7+ 실존 182행 오독 방지)
        _X2_BUCKET = ("CASE WHEN signal_score >= 0.6 THEN 'x2[0.6,inf)' "
                      "ELSE 'x2[' || ROUND((FLOOR(signal_score * 10) / 10.0)::numeric, 1) || ','"
                      " || ROUND((FLOOR(signal_score * 10) / 10.0 + 0.1)::numeric, 1) || ')' END")
        _VARIANT_SQL = {
            "score_raw_h30": f"""{_DEDUP_CTE}
                SELECT {_X2_BUCKET} AS bucket, COUNT(*) AS n, SUM(is_correct) AS hits,
                       MIN(timestamp) AS ts_min, MAX(timestamp) AS ts_max
                  FROM dedup WHERE is_correct IN (0, 1) GROUP BY 1""",
            "score_raw_h24": f"""{_DEDUP_CTE}
                SELECT {_X2_BUCKET} AS bucket, COUNT(*) AS n, SUM(is_correct_24h) AS hits,
                       MIN(timestamp) AS ts_min, MAX(timestamp) AS ts_max
                  FROM dedup WHERE is_correct_24h IN (0, 1) GROUP BY 1""",
            "score_v2_h24": f"""{_DEDUP_CTE}
                SELECT CASE WHEN score_v2 IS NULL THEN 'NULL'
                            ELSE 'p[' || ROUND((FLOOR(score_v2 * 20) / 20.0)::numeric, 2) || ','
                                 || ROUND((FLOOR(score_v2 * 20) / 20.0 + 0.05)::numeric, 2) || ')' END AS bucket,
                       COUNT(*) AS n, SUM(is_correct_24h) AS hits,
                       MIN(timestamp) AS ts_min, MAX(timestamp) AS ts_max
                  FROM dedup WHERE is_correct_24h IN (0, 1) GROUP BY 1""",
        }

        n_inserted = 0
        now = int(time.time())
        for market_id, schema in _CALIBRATION_MARKETS.items():
            # 멱등 가드 — score_* variant 스코프 (confidence v1/v2 스냅샷과 독립)
            exists = ledger.execute(
                "SELECT 1 FROM signal_calibration_daily WHERE day = ? AND market_id = ? "
                "AND variant LIKE 'score_%' LIMIT 1",
                (today_utc, market_id),
            ).fetchone()
            ledger.commit()  # 집계 동안 무트랜잭션 idle (idle_in_tx 60s 가드 회피 — 08-10 r3 선례)
            if exists:
                continue
            try:
                conn = open_schema_connection(schema, readonly=True)
            except Exception:
                logger.exception("[ledger] score calibration: %s open failed", schema)
                continue
            try:
                try:
                    conn.execute(
                        f"SET statement_timeout = '{os.environ.get('CALIB_SNAPSHOT_STMT_TIMEOUT', '120s')}'"
                    )
                except Exception:
                    logger.warning("[ledger] score calibration: %s statement_timeout 설정 실패", schema)
                variant_rows = {}
                for variant, sql in _VARIANT_SQL.items():
                    variant_rows[variant] = conn.execute(sql).fetchall()
            except Exception:
                logger.exception("[ledger] score calibration aggregate failed: %s", market_id)
                continue
            finally:
                try:
                    conn.execute("SET statement_timeout = DEFAULT")
                except Exception:
                    pass  # 원복 실패 시 conn.close 가 세션 정리 — 명시적 무해 삼킴
                conn.close()

            for variant, rows in variant_rows.items():
                for r in rows:
                    ledger.execute(
                        "INSERT OR IGNORE INTO signal_calibration_daily "
                        "(day, market_id, \"interval\", bucket, variant, n, hits, ts_min, ts_max, snapshot_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (today_utc, market_id, "combined", str(r["bucket"]), variant,
                         int(r["n"]), int(r["hits"] or 0),
                         r["ts_min"], r["ts_max"], now),
                    )
                    n_inserted += 1
            ledger.commit()

        if n_inserted:
            logger.info("[ledger] score calibration snapshot: %s (%d rows)",
                        today_utc, n_inserted)
        return n_inserted
    finally:
        ledger.close()


def snapshot_ledger_calibration() -> int:
    """[2026-08-20 Phase 2a] ledger 확률 캘리브레이터 채점판 일별 스냅샷.

    signal_path_ledger 단독 GROUP BY (signals 조인 불요 — 발행 시점 p̂ 이
    pred_p_hit/pred_p_dd 로 원장에 스탬프되는 자기충족 설계). variant 2종:
      ledger_hit_h24 — bucket = pred_p_hit 0.05 버킷 ('NULL' = 스탬프 부재),
        hits = 실현 도달 (path_r_max >= θ, 기본 +1%)
      ledger_dd_h24  — bucket = pred_p_dd 0.05 버킷,
        hits = 실현 낙폭 (path_r_min <= -ψ, 기본 -1%)
    코호트: path_status='done' · action='buy' · score>=0 · era 이후.
    임계 θ/ψ 는 trade/core/ledger_calibration.py 와 동일 env
    (LEDGER_CAL_HIT_PCT/LEDGER_CAL_DD_PCT) 미러 — theory_v2 상수 미러 관례.
    승격(p̂ 의 매매 소비) 판정 원장 — 7일+ 우위 + 사용자 승인 별도 세션.
    """
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection

    _hit_pct = float(os.environ.get("LEDGER_CAL_HIT_PCT", "0.01"))
    _dd_pct = float(os.environ.get("LEDGER_CAL_DD_PCT", "0.01"))
    today_utc = time.strftime("%Y-%m-%d", time.gmtime())
    ledger = _open_ledger_rw()
    try:
        def _bucket_expr(col: str) -> str:
            return (f"CASE WHEN {col} IS NULL THEN 'NULL' "
                    f"ELSE 'p[' || ROUND((FLOOR({col} * 20) / 20.0)::numeric, 2) || ','"
                    f" || ROUND((FLOOR({col} * 20) / 20.0 + 0.05)::numeric, 2) || ')' END")

        _COMMON_WHERE = (f"path_status = 'done' AND LOWER(COALESCE(action,'')) = 'buy' "
                         f"AND signal_score >= 0 AND created_at > {_SCORE_ERA_START_TS}")
        _VARIANT_SQL = {
            "ledger_hit_h24": f"""
                SELECT {_bucket_expr('pred_p_hit')} AS bucket, COUNT(*) AS n,
                       COUNT(*) FILTER (WHERE path_r_max >= {_hit_pct}) AS hits,
                       MIN(created_at) AS ts_min, MAX(created_at) AS ts_max
                  FROM signal_path_ledger WHERE {_COMMON_WHERE} GROUP BY 1""",
            "ledger_dd_h24": f"""
                SELECT {_bucket_expr('pred_p_dd')} AS bucket, COUNT(*) AS n,
                       COUNT(*) FILTER (WHERE path_r_min <= -{_dd_pct}) AS hits,
                       MIN(created_at) AS ts_min, MAX(created_at) AS ts_max
                  FROM signal_path_ledger WHERE {_COMMON_WHERE} GROUP BY 1""",
        }

        n_inserted = 0
        now = int(time.time())
        for market_id, schema in _CALIBRATION_MARKETS.items():
            # 멱등 가드 — ledger_* variant 독립 스코프 (score_%/confidence 스냅샷과 분리)
            exists = ledger.execute(
                "SELECT 1 FROM signal_calibration_daily WHERE day = ? AND market_id = ? "
                "AND variant LIKE 'ledger_%' LIMIT 1",
                (today_utc, market_id),
            ).fetchone()
            ledger.commit()  # 집계 동안 무트랜잭션 idle (idle_in_tx 60s 가드 회피)
            if exists:
                continue
            try:
                conn = open_schema_connection(schema, readonly=True)
            except Exception:
                logger.exception("[ledger] ledger calibration: %s open failed", schema)
                continue
            try:
                try:
                    conn.execute(
                        f"SET statement_timeout = '{os.environ.get('CALIB_SNAPSHOT_STMT_TIMEOUT', '120s')}'"
                    )
                except Exception:
                    logger.warning("[ledger] ledger calibration: %s statement_timeout 설정 실패", schema)
                variant_rows = {}
                for variant, sql in _VARIANT_SQL.items():
                    variant_rows[variant] = conn.execute(sql).fetchall()
            except Exception:
                logger.exception("[ledger] ledger calibration aggregate failed: %s", market_id)
                continue
            finally:
                try:
                    conn.execute("SET statement_timeout = DEFAULT")
                except Exception:
                    pass  # 원복 실패 시 conn.close 가 세션 정리 — 명시적 무해 삼킴
                conn.close()

            for variant, rows in variant_rows.items():
                for r in rows:
                    ledger.execute(
                        "INSERT OR IGNORE INTO signal_calibration_daily "
                        "(day, market_id, \"interval\", bucket, variant, n, hits, ts_min, ts_max, snapshot_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (today_utc, market_id, "combined", str(r["bucket"]), variant,
                         int(r["n"]), int(r["hits"] or 0),
                         r["ts_min"], r["ts_max"], now),
                    )
                    n_inserted += 1
            ledger.commit()

        if n_inserted:
            logger.info("[ledger] ledger calibration snapshot: %s (%d rows)",
                        today_utc, n_inserted)
        return n_inserted
    finally:
        ledger.close()


def snapshot_tool_latency(max_days: int = 400) -> int:
    """[2026-07-08] per-tool 지연/에러 일별 SLA 스냅샷.

    mcp_requests 에서 미계산 완결일(UTC)의 tool/method 별 p50/p95/avg/max·에러수를
    집계해 tool_latency_daily 에 INSERT. 크롤러들이 외부에서 채점 중인 가용성을
    자체 발행하기 위한 원료 (/sla 라우트).
    """
    from datetime import date, timedelta

    today_utc = time.strftime("%Y-%m-%d", time.gmtime())
    conn = _open_ledger_rw()
    try:
        last = conn.execute(
            "SELECT MAX(day) AS d FROM tool_latency_daily"
        ).fetchone()
        if last and last["d"]:
            start_day = _next_day(last["d"])
        else:
            first = conn.execute("SELECT MIN(ts) AS t FROM mcp_requests").fetchone()
            if not first or not first["t"]:
                return 0
            start_day = time.strftime("%Y-%m-%d", time.gmtime(int(first["t"])))

        n_days = 0
        day = start_day
        while day < today_utc and n_days < max_days:
            d0 = date.fromisoformat(day)
            # tz-free: epoch = days since 1970-01-01 (UTC)
            ts0 = (d0 - date(1970, 1, 1)).days * 86400
            ts1 = ts0 + 86400
            conn.execute(
                "INSERT OR IGNORE INTO tool_latency_daily "
                "(day, name, calls, errors, p50_ms, p95_ms, avg_ms, max_ms, snapshot_at) "
                "SELECT ?, name, COUNT(*), "
                "       COUNT(*) FILTER (WHERE success = FALSE OR error_code IS NOT NULL), "
                "       percentile_cont(0.5) WITHIN GROUP (ORDER BY response_ms)::int, "
                "       percentile_cont(0.95) WITHIN GROUP (ORDER BY response_ms)::int, "
                "       AVG(response_ms)::int, MAX(response_ms), ? "
                "FROM mcp_requests "
                "WHERE ts >= ? AND ts < ? AND request_type IN ('tool', 'resource', 'mcp') "
                "GROUP BY name",
                (day, int(time.time()), ts0, ts1),
            )
            conn.commit()
            n_days += 1
            day = _next_day(day)
        if n_days:
            logger.info("[ledger] tool latency snapshots: %d day(s) through %s", n_days, day)
        return n_days
    finally:
        conn.close()


def get_sla_history(days: int = 30) -> Dict[str, Any]:
    """/sla 라우트 서빙 — 최근 N일 per-tool 지연/에러 이력."""
    conn = _open_ledger_rw()
    try:
        rows = conn.execute(
            "SELECT day, name, calls, errors, p50_ms, p95_ms, avg_ms, max_ms "
            "FROM tool_latency_daily "
            "WHERE day >= to_char(now() AT TIME ZONE 'UTC' - make_interval(days => ?), 'YYYY-MM-DD') "
            "ORDER BY day DESC, calls DESC",
            (days,),
        ).fetchall()
    finally:
        conn.close()
    by_day: Dict[str, list] = {}
    for r in rows:
        by_day.setdefault(r["day"], []).append({
            "name": r["name"], "calls": r["calls"], "errors": r["errors"],
            "p50_ms": r["p50_ms"], "p95_ms": r["p95_ms"],
            "avg_ms": r["avg_ms"], "max_ms": r["max_ms"],
        })
    return {
        "days": days,
        "daily": [{"day": d, "tools": v} for d, v in sorted(by_day.items(), reverse=True)],
        "note": (
            "Self-published per-tool latency/error history (UTC days). Includes handshake "
            "methods. errors counts HTTP failures AND envelope-level error_codes."
        ),
    }


def get_chain(days: int = 30) -> Dict[str, Any]:
    """최근 N일 체인 조회 (서빙용 — /ledger 라우트 + get_ledger_integrity tool)."""
    conn = _open_ledger_rw()
    try:
        rows = conn.execute(
            "SELECT day, created_count, resolved_count, created_hash, resolved_hash, "
            "       prev_chain_hash, chain_hash, computed_at "
            "FROM prediction_ledger_hashes ORDER BY day DESC LIMIT ?",
            (days,),
        ).fetchall()
        head = conn.execute(
            "SELECT COUNT(*) AS n, MIN(day) AS first_day, MAX(day) AS last_day "
            "FROM prediction_ledger_hashes"
        ).fetchone()
    finally:
        conn.close()
    # [2026-08-18 외부앵커] 실재하는 앵커 층만 서빙 (정직 원칙 — 존재하지 않는
    # 검증 경로 주장 금지). blog 층은 Mac 발행 개시 후 ANCHOR_BLOG_URL_PATTERN
    # env 로 활성화. 스펙: ai_brain/07_mcp_llm_api/spec_external_anchor.md
    _latest = dict(rows[0]) if rows else {}
    _anchor_layers = [
        {
            "type": "dual_machine_git",
            "since": "2026-07-20",
            "cadence": "daily",
            "what": ("full chain CSV committed to a git repository replicated to a "
                     "second physical machine — rewriting history requires forging "
                     "both machines' git logs"),
            "visibility": "operator-owned machines (honest: not third-party verifiable)",
        },
        {
            "type": "opentimestamps",
            "since": "2026-08-18",
            "cadence": "daily stamp, weekly bitcoin-attestation upgrade",
            "what": ("each day's latest chain_hash is fixed in a small dated file and "
                     "timestamped via OpenTimestamps calendar servers onto Bitcoin — "
                     "cryptographic proof the hash existed no later than that time"),
            "visibility": ("attestation is public (Bitcoin); the .ots proof files are "
                           "distributed with the weekly public anchor posts once the "
                           "blog layer is live — until then, proof distribution pending"),
        },
    ]
    _blog_pattern = os.environ.get("ANCHOR_BLOG_URL_PATTERN", "").strip()
    if _blog_pattern:
        _anchor_layers.append({
            "type": "public_blog",
            "cadence": "weekly",
            "url_pattern": _blog_pattern,
            "what": ("weekly public post embedding the latest chain_hash (JSON-LD) plus "
                     "the dated .ots proof files — a single weekly anchor covers all "
                     "prior history because each entry commits to the previous one"),
            "visibility": "public, web-archivable",
        })
    return {
        "recipe_version": RECIPE_VERSION,
        "recipe": CANONICAL_RECIPE,
        "chain_length": (head["n"] if head else 0),
        "first_day": (head["first_day"] if head else None),
        "last_day": (head["last_day"] if head else None),
        "entries": [dict(r) for r in reversed(rows)],
        "external_anchors": {
            "layers": _anchor_layers,
            "latest_anchor": {
                "day": _latest.get("day"),
                "chain_hash": _latest.get("chain_hash"),
            },
            "honesty_note": (
                "Anchors prove immutability from their own start date forward. History "
                "before the first public anchor remains reproducible-but-unproven "
                "(self-hosted). Anchoring began: dual-machine 2026-07-20, "
                "opentimestamps 2026-08-18."
            ),
        },
        "verification_hint": (
            "Archive any entry's chain_hash today. Recompute it later from raw rows "
            "(get_resolved_predictions) using the recipe — a mismatch proves post-hoc edits. "
            "Each entry commits to the previous one, so one archived hash anchors all history before it. "
            "For first-time visitors: compare against a public anchor (see external_anchors) "
            "instead of your own archive."
        ),
    }


def start_daily_thread(interval_seconds: int = 3600) -> None:
    """시간당 due-check 데몬 스레드 (나이 기반 — cron 아님, 재시작 내성)."""
    import threading

    def _loop():
        while True:
            try:
                run_due_days()
            except Exception:
                logger.exception("[ledger] daily hash computation failed")
            try:
                snapshot_accuracy_cells()
            except Exception:
                logger.exception("[ledger] accuracy cell snapshot failed")
            try:
                snapshot_tool_latency()
            except Exception:
                logger.exception("[ledger] tool latency snapshot failed")
            try:
                snapshot_signal_calibration()
            except Exception:
                logger.exception("[ledger] signal calibration snapshot failed")
            try:
                snapshot_score_calibration()
            except Exception:
                logger.exception("[ledger] score calibration snapshot failed")
            try:
                snapshot_ledger_calibration()
            except Exception:
                logger.exception("[ledger] ledger calibration snapshot failed")
            time.sleep(interval_seconds)

    t = threading.Thread(target=_loop, name="prediction-ledger-hash", daemon=True)
    t.start()
    logger.info("[ledger] prediction ledger hash thread started (interval=%ss)", interval_seconds)
