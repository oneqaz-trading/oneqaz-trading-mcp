"""DB 백엔드 설정.

환경변수에서 PG 접속 정보 + 백엔드 모드를 읽어 ``DBConfig`` 로 반환.
앱 부팅 시 1회 로드 후 프로세스 수명 동안 재사용 (immutable).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class DBBackend(str, Enum):
    """DB 백엔드 모드.

    Strangler Fig 마이그레이션 중 세 가지 상태 지원.
    """

    SQLITE = "sqlite"
    DUAL = "dual"
    POSTGRES = "postgres"

    @classmethod
    def from_env(cls, value: Optional[str]) -> "DBBackend":
        if not value:
            return cls.SQLITE
        v = value.strip().lower()
        try:
            return cls(v)
        except ValueError:
            raise ValueError(
                f"DB_BACKEND must be one of {[m.value for m in cls]}, got {value!r}"
            )


@dataclass(frozen=True)
class DBConfig:
    """DB 접속 + 풀 설정. ``load_config()`` 로 생성."""

    backend: DBBackend = DBBackend.SQLITE

    # PostgreSQL 접속
    pg_host: str = "postgres"
    pg_port: int = 5432
    pg_db: str = "auto_trader"
    pg_user: str = "auto_trader"
    pg_password: str = "auto_trader_dev"

    # 커넥션 풀
    # pg_pool_min=0: idle 연결 영구 점유 방지 (2026-04-19 PgBouncer 제거 후).
    #   프로세스 × 스키마 12 × min 으로 고정 점유되던 연결이 사라져 PG 부담 완화.
    # pg_pool_max=50: 학습 self-play + 캔들 수집 + 시그널 + 매매 동시 실행 시
    #   pool starvation 방지. PG max_connections=500 대비 충분히 안전. (2026-04-26)
    pg_pool_min: int = 0
    pg_pool_max: int = 50

    # 쿼리 timeout (ms)
    pg_statement_timeout_ms: int = 30_000

    # application_name (pg_stat_activity 식별용)
    pg_application_name: str = "auto_trader"

    @property
    def pg_conninfo(self) -> str:
        """libpq conninfo 문자열."""
        return (
            f"host={self.pg_host} "
            f"port={self.pg_port} "
            f"dbname={self.pg_db} "
            f"user={self.pg_user} "
            f"password={self.pg_password} "
            f"application_name={self.pg_application_name}"
        )

    @property
    def pg_enabled(self) -> bool:
        """PG 접속이 필요한 모드인가 (dual 또는 postgres)."""
        return self.backend in (DBBackend.DUAL, DBBackend.POSTGRES)

    @property
    def sqlite_enabled(self) -> bool:
        """SQLite 접속이 필요한 모드인가 (sqlite 또는 dual)."""
        return self.backend in (DBBackend.SQLITE, DBBackend.DUAL)


def load_config() -> DBConfig:
    """환경변수에서 ``DBConfig`` 생성."""
    return DBConfig(
        backend=DBBackend.from_env(os.getenv("DB_BACKEND")),
        pg_host=os.getenv("PG_HOST", "postgres"),
        pg_port=int(os.getenv("PG_PORT", "5432")),
        pg_db=os.getenv("PG_DB", "auto_trader"),
        pg_user=os.getenv("PG_USER", "auto_trader"),
        pg_password=os.getenv("PG_PASSWORD", "auto_trader_dev"),
        pg_pool_min=int(os.getenv("PG_POOL_MIN", "0")),
        pg_pool_max=int(os.getenv("PG_POOL_MAX", "50")),
        pg_statement_timeout_ms=int(os.getenv("PG_STATEMENT_TIMEOUT_MS", "30000")),
        pg_application_name=os.getenv("PG_APPLICATION_NAME", "auto_trader"),
    )


# 프로세스 전역 config (lazy init)
_config: Optional[DBConfig] = None


def get_config() -> DBConfig:
    """프로세스 전역 ``DBConfig`` 반환 (첫 호출 시 env 파싱)."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reset_config_for_tests() -> None:
    """테스트용: 전역 config 초기화."""
    global _config
    _config = None
