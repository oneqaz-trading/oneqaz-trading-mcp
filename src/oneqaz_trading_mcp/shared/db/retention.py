"""보존 정책(retention) 공용 헬퍼 — FK 자식 선삭제 + 배치 삭제.

[2026-09-20] retention 3층 사고 수리에서 신설.

## 왜 필요한가

retention 은 "오래된 행을 지운다" 는 단순한 일이지만, PG 에서는 두 가지가
조용히 무너진다.

1. **FK 순서** — 자식이 부모를 참조하는데 부모를 먼저 지우면 FK 위반으로
   문장이 실패한다. 각 테이블의 보존일수만 보고 순서를 정하면 안 되는 이유는
   **부모와 자식의 시계가 다르기** 때문이다 (예: `inference_candidates.created_at`
   90일 vs `inference_outcomes.evaluated_at` 60일 — 90일 전에 만들어진 후보가
   어제 평가될 수 있으므로, 자식을 자기 시계로 먼저 지워도 부모의 자식은 남는다).
   → 자식은 **부모의 삭제 대상 집합을 기준으로** 지워야 한다.

2. **트랜잭션 abort** — 한 문장이 실패하면 트랜잭션 전체가 INERROR 가 되고
   이후 모든 문장이 `current transaction is aborted` 로 죽는다. 더 나쁜 것은
   그 상태에서 보낸 COMMIT 을 PG 가 **조용히 ROLLBACK 으로 처리**한다는 점이다
   (에러도 안 난다). 이 조합이 "160만행 정리 완료" 로그를 찍으면서 실제로는
   한 행도 지우지 못하는 거짓 성공을 2주 넘게 방송했다.

## 설계 원칙

- **FK 목록을 하드코딩하지 않는다.** `pg_constraint` 를 실행 시점에 조회해
  부모→자식 맵을 만든다. DDL 이 바뀌어도 이 파일이 화석이 되지 않는다.
  (`tests/validation/test_migration_ddl_sync.py` 와 같은 철학)
- **배치마다 commit.** 100만행을 한 트랜잭션에 넣으면 WAL 이 부풀고 락이 길어진다.
  배치 단위로 끊어 커밋하면 중간에 실패해도 그때까지는 실제로 지워져 있다.
- **commit 은 strict.** abort 상태의 COMMIT 은 예외로 만든다 (조용한 롤백 금지).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Sequence

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 50_000
DEFAULT_MAX_BATCHES = 400


@dataclass(frozen=True)
class FKChild:
    """부모 테이블을 참조하는 자식 테이블 한 건 (단일 컬럼 FK)."""

    child_table: str
    child_col: str
    parent_col: str
    constraint: str


def fk_children(conn, schema: str) -> Dict[str, List[FKChild]]:
    """스키마 내 FK 를 카탈로그에서 읽어 {부모테이블: [자식...]} 으로 만든다.

    복합키 FK 는 지원하지 않는다 — 발견되면 WARNING 을 남기고 건너뛴다
    (조용히 빠뜨리면 그 테이블의 retention 이 영구 실패한다).
    """
    rows = conn.execute(
        """
        SELECT con.conname                    AS constraint_name,
               crel.relname                   AS child_table,
               ca.attname                     AS child_col,
               prel.relname                   AS parent_table,
               pa.attname                     AS parent_col,
               array_length(con.conkey, 1)    AS key_width
        FROM pg_constraint con
        JOIN pg_class crel ON crel.oid = con.conrelid
        JOIN pg_class prel ON prel.oid = con.confrelid
        JOIN pg_attribute ca ON ca.attrelid = crel.oid AND ca.attnum = con.conkey[1]
        JOIN pg_attribute pa ON pa.attrelid = prel.oid AND pa.attnum = con.confkey[1]
        WHERE con.contype = 'f'
          AND con.connamespace = %s::regnamespace
        """,
        (schema,),
    ).fetchall()

    out: Dict[str, List[FKChild]] = {}
    for r in rows:
        rd = dict(r) if not isinstance(r, dict) else r
        if (rd.get("key_width") or 1) > 1:
            logger.warning(
                "retention: 복합키 FK 는 미지원 — %s (%s → %s). "
                "이 부모의 자식 선삭제가 누락된다.",
                rd["constraint_name"], rd["child_table"], rd["parent_table"],
            )
            continue
        out.setdefault(rd["parent_table"], []).append(
            FKChild(
                child_table=rd["child_table"],
                child_col=rd["child_col"],
                parent_col=rd["parent_col"],
                constraint=rd["constraint_name"],
            )
        )
    return out


def deleted_total(stats: Dict[str, Any]) -> int:
    """run_retention 결과에서 실제 삭제 합계를 구한다.

    메타 키(``_`` 시작)와 생성 카운터(``*_created``)는 제외한다 — 과거 호출부가
    ``sum(v for v in stats.values() if v > 0)`` 로 계산해 메타 키가 들어오면
    dict > int 비교로 TypeError 가 난다.
    """
    return sum(
        v
        for k, v in stats.items()
        if not k.startswith("_")
        and not k.endswith("_created")  # landmarks_created 등 — 생성이지 삭제가 아니다
        and isinstance(v, int)
        and v > 0
    )


def count_matching(conn, table: str, where_sql: str, params: Sequence[Any]) -> int:
    """삭제 대상 건수만 센다 (dry-run 용). 실패는 caller 가 처리."""
    row = conn.execute(
        f"SELECT count(*) AS n FROM {table} WHERE {where_sql}", tuple(params)
    ).fetchone()
    if row is None:
        return 0
    try:
        return int(row["n"])
    except (TypeError, KeyError, IndexError):
        return int(row[0])


def _delete_batched(
    conn,
    table: str,
    where_sql: str,
    params: Sequence[Any],
    *,
    batch_size: int,
    max_batches: int,
    label: str,
) -> int:
    """ctid 배치 삭제. 배치마다 commit 하여 트랜잭션을 짧게 유지한다."""
    total = 0
    # crow/db/crow_db_manager.py 와 동일한 ctid 배치 관용구를 쓴다.
    sql = (
        f"DELETE FROM {table} WHERE ctid IN ("
        f"SELECT ctid FROM {table} WHERE {where_sql} LIMIT {int(batch_size)})"
    )
    for _ in range(max_batches):
        cur = conn.execute(sql, tuple(params))
        deleted = cur.rowcount or 0
        total += deleted
        # 배치 커밋 — abort 상태면 여기서 fail-loud (조용한 롤백 금지).
        conn.commit_strict()
        if deleted < batch_size:
            return total
    logger.warning(
        "retention: %s — max_batches(%d) 도달, 남은 행은 다음 사이클로 이월 "
        "(이번에 %d행 삭제)",
        label, max_batches, total,
    )
    return total


def delete_with_cascade(
    conn,
    *,
    table: str,
    where_sql: str,
    params: Sequence[Any] = (),
    fk_map: Dict[str, List[FKChild]] | None = None,
    protect: Iterable[str] = (),
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_batches: int = DEFAULT_MAX_BATCHES,
    dry_run: bool = False,
    _seen: set[str] | None = None,
) -> Dict[str, int]:
    """부모의 삭제 대상 집합을 기준으로 자식을 먼저 지운 뒤 부모를 지운다.

    Args:
        table: 부모 테이블 (스키마는 search_path 로 해결).
        where_sql: 삭제 조건. placeholder 는 ``%s``.
        params: where_sql 의 바인딩 값.
        fk_map: :func:`fk_children` 결과. None 이면 자식 처리를 생략한다.
        protect: 자식 테이블 이름 집합. 여기 속한 자식이 **참조 중인 부모 행은
            삭제하지 않는다** (자식까지 지우면 안 되는 관계용 —
            예: ``memory_landmarks`` 가 대표로 잡은 ``memory_episodes``).
        dry_run: True 면 건수만 세고 삭제하지 않는다.

    Returns:
        {"<table>": n, "<child>(cascade)": n, ...}
    """
    fk_map = fk_map or {}
    protect_set = set(protect)
    seen = _seen if _seen is not None else set()
    stats: Dict[str, int] = {}

    if table in seen:  # 순환 FK 방어
        return stats
    seen.add(table)

    children = [c for c in fk_map.get(table, []) if c.child_table != table]

    # ── 1) 보호 대상 자식이 참조하는 부모 행은 삭제 집합에서 제외 ──
    effective_where = where_sql
    for child in children:
        if child.child_table not in protect_set:
            continue
        effective_where = (
            f"({effective_where}) AND NOT EXISTS (SELECT 1 FROM {child.child_table} pc "
            f"WHERE pc.{child.child_col} = {table}.{child.parent_col})"
        )

    # ── 2) 자식 선삭제 (부모의 삭제 집합 기준) ──
    for child in children:
        if child.child_table in protect_set:
            continue
        child_where = (
            f"{child.child_col} IN (SELECT {child.parent_col} FROM {table} "
            f"WHERE {effective_where})"
        )
        # 손자까지 재귀 (자식이 또 다른 테이블의 부모일 수 있다)
        sub = delete_with_cascade(
            conn,
            table=child.child_table,
            where_sql=child_where,
            params=params,
            fk_map=fk_map,
            protect=protect_set,
            batch_size=batch_size,
            max_batches=max_batches,
            dry_run=dry_run,
            _seen=seen,
        )
        for k, v in sub.items():
            key = k if k.endswith("(cascade)") else f"{k}(cascade)"
            stats[key] = stats.get(key, 0) + v

    # ── 3) 부모 삭제 ──
    if dry_run:
        n = count_matching(conn, table, effective_where, params)
    else:
        n = _delete_batched(
            conn,
            table,
            effective_where,
            params,
            batch_size=batch_size,
            max_batches=max_batches,
            label=table,
        )
    if n:
        stats[table] = stats.get(table, 0) + n
    return stats
