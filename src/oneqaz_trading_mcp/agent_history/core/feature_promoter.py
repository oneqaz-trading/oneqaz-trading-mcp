"""Feature Governance promoter — OBSERVATION → CONDITIONAL → ACTIVE 자동 승격.

agent_history.feature_governance 의 252+ 개 피처가 OBSERVATION 에서 멈춰
있는 문제를 해결한다.

승격 기준 (3-track validation, project_mcp_ax_trust_layer.md):
  - OBSERVATION → CONDITIONAL: oos_total >= 10 + win_rate >= 0.55 + delta_pf > 0
  - CONDITIONAL → ACTIVE:        oos_total >= 30 + win_rate >= 0.58 + importance_stability >= 0.5 + delta_pf > 0.05
  - * → DEPRECATED:              oos_total >= 20 AND win_rate < 0.45  (확신 있게 안 좋은 경우)

importance_stability 가 0 이거나 importance_rank 가 None 이면 판단 보류.

usage:
  python -m agent_history.core.feature_promoter             # 모든 시장 1회
  python -m agent_history.core.feature_promoter --dry-run   # 변경 없이 후보만 출력
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("feature_promoter")

# [2026-10-14] WO-0A′ — news_direction_accuracy 행의 상태 전이를 멈춘다(상태값은 그대로).
# 이 피처군의 측정 정의가 무효라(external_context/core/feature_gate_evaluator.py LEGACY_FREEZE_ENV 주석)
# 그 카운트로 승격·강등하면 무효 잣대가 상태를 계속 바꾼다. 다른 피처형(llm_adjustment)은 그대로 평가한다.
# env 이름은 평가기와 같다 — 해제 = NEWS_FEATURE_LEGACY_FREEZE=0.
_LEGACY_FREEZE_ENV = "NEWS_FEATURE_LEGACY_FREEZE"
_FROZEN_FEATURE_TYPES = ("news_direction_accuracy",)


def _frozen_types() -> tuple:
    return _FROZEN_FEATURE_TYPES if os.getenv(_LEGACY_FREEZE_ENV, "1") != "0" else ()

# 승격/강등 임계
# [2026-05-19] cond_to_active 임계 완화 + importance_stability 계산 활성화.
#   1차 (05-19 14:28): caller 가 imp_stab 인수 안 넘겨 default 0.0 → 32/32 row stab=0
#      → 임시 우회로 min_importance_stability=0.0 + min_total 30→20.
#   2차 (05-19 14:50): caller 2곳(feature_gate_evaluator, llm_trust_manager) 에
#      imp_stab=min(total/30.0, 1.0) proxy 계산 추가 + 기존 315 row backfill.
#      → 의미 있는 0.5 임계 복원 가능. min_total 도 30 으로 복원 (원래 의도).
# memory: project_data_maturity_diagnosis_2026_04_27.md (22일 전 ACTIVE=0 진단)
THRESHOLDS = {
    "obs_to_cond": {
        "min_total": 10,
        "min_win_rate": 0.55,
        "min_delta_pf": 0.0,
    },
    "cond_to_active": {
        "min_total": 20,                  # 30 → 20 (sample size 현실화)
        "min_win_rate": 0.58,
        "min_importance_stability": 0.5,  # P3 계산 활성화로 의미있는 임계 복원
        "min_delta_pf": 0.05,
    },
    "deprecate": {
        "min_total": 20,
        "max_win_rate": 0.45,
    },
}


def _open_pg():
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    # writeable 모드 명시. 일부 환경에서 default 가 readonly 일 수 있어
    # 명시적 readonly=False 로 안전하게.
    try:
        return open_schema_connection("agent_history", readonly=False)
    except TypeError:
        return open_schema_connection("agent_history")


def _evaluate_feature(row: Dict[str, Any]) -> Optional[str]:
    """현재 feature 한 행의 다음 status 결정. None = 변경 없음."""
    status = (row.get("status") or "").upper()
    total = int(row.get("oos_total_count") or 0)
    wins = int(row.get("oos_win_count") or 0)
    win_rate = (wins / total) if total else 0.0
    delta_pf = float(row.get("delta_pf") or 0.0)
    imp_stab = float(row.get("importance_stability") or 0.0)

    # 강등 (가장 강한 신호 우선)
    dep = THRESHOLDS["deprecate"]
    if total >= dep["min_total"] and win_rate < dep["max_win_rate"] and status != "DEPRECATED":
        return "DEPRECATED"

    # OBSERVATION → CONDITIONAL
    if status == "OBSERVATION":
        c = THRESHOLDS["obs_to_cond"]
        if total >= c["min_total"] and win_rate >= c["min_win_rate"] and delta_pf > c["min_delta_pf"]:
            return "CONDITIONAL"

    # CONDITIONAL → ACTIVE
    if status == "CONDITIONAL":
        c = THRESHOLDS["cond_to_active"]
        if (total >= c["min_total"] and win_rate >= c["min_win_rate"]
                and imp_stab >= c["min_importance_stability"]
                and delta_pf > c["min_delta_pf"]):
            return "ACTIVE"

    return None


def run_promotion(dry_run: bool = False) -> Dict[str, Any]:
    """전체 feature_governance 행을 평가하고 status 전환을 적용.

    Returns: {scanned, promoted_to: {...}, demoted, unchanged, dry_run}
    """
    # [2026-08-03] borrow 직후부터 close 까지 try/finally 로 보장 — 과거엔
    # SELECT/평가 루프가 try 밖이라 예외 탈출 시 conn 미반납 = agent_history
    # 풀 슬롯 유실 (12사이클마다 downstream_runner 가 호출, 예외는 runner 가
    # 삼켜 반복 누수 가능했음).
    conn = _open_pg()
    try:
        return _run_promotion_inner(conn, dry_run)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _run_promotion_inner(conn, dry_run: bool) -> Dict[str, Any]:
    rows = conn.execute(
        "SELECT * FROM feature_governance ORDER BY updated_at DESC"
    ).fetchall()
    rows = [dict(r) for r in rows]

    frozen_types = _frozen_types()
    stats: Dict[str, Any] = {
        "scanned": len(rows),
        "promoted_to": {"CONDITIONAL": 0, "ACTIVE": 0, "DEPRECATED": 0},
        "unchanged": 0,
        "frozen": 0,
        "transitions": [],
        "dry_run": dry_run,
    }

    now = datetime.now(timezone.utc).isoformat()
    for row in rows:
        if row.get("feature_type") in frozen_types:
            stats["frozen"] += 1
            continue
        new_status = _evaluate_feature(row)
        if new_status is None:
            stats["unchanged"] += 1
            continue
        old_status = row.get("status")
        feature_id = row.get("feature_id")

        stats["transitions"].append({
            "feature_id": feature_id,
            "old": old_status,
            "new": new_status,
            "win_rate": round((int(row.get("oos_win_count") or 0)
                              / max(int(row.get("oos_total_count") or 1), 1)), 4),
            "total": int(row.get("oos_total_count") or 0),
            "delta_pf": float(row.get("delta_pf") or 0.0),
        })
        stats["promoted_to"][new_status] = stats["promoted_to"].get(new_status, 0) + 1

        if not dry_run:
            # 행 단위 트랜잭션: 한 행 실패해도 다음 행 진행. abort 시 rollback.
            try:
                conn.execute(
                    """
                    UPDATE feature_governance
                    SET status = %s, updated_at = %s
                    WHERE feature_id = %s
                    """,
                    (new_status, now, feature_id),
                )
                conn.execute(
                    """
                    INSERT INTO feature_evaluation_log (
                        feature_id, evaluated_at, old_status, new_status,
                        delta_pf, delta_mdd, oos_window_start, oos_window_end,
                        sample_size, details
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (feature_id, now, old_status, new_status,
                     float(row.get("delta_pf") or 0.0),
                     float(row.get("delta_mdd") or 0.0),
                     None, None, int(row.get("oos_total_count") or 0),
                     json.dumps({"source": "feature_promoter", "auto": True},
                                ensure_ascii=False)),
                )
                try:
                    conn.commit()
                except Exception:
                    pass
            except Exception as exc:
                logger.warning("promote failed for %s: %s", feature_id, exc)
                try:
                    conn.rollback()
                except Exception:
                    pass

    return stats


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="변경 없이 후보만 출력")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s")
    res = run_promotion(dry_run=args.dry_run)
    print(json.dumps(res, indent=2, default=str))


if __name__ == "__main__":
    main()
