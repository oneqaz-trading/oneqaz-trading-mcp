"""PostgreSQL 커넥션 풀 관리.

스키마별로 ``psycopg_pool.ConnectionPool`` 을 하나씩 생성 후 프로세스 전역에 캐시.

설계 (2026-04-19, PgBouncer 제거 후):

- PG 직결 구조. 스키마별 ``SET search_path / SET statement_timeout /
  SET idle_in_transaction_session_timeout`` 를 ``configure`` 에서 세션 단위로 주입.
- ``min_size=0`` 기본값 — idle 연결 영구 점유 방지. 프로세스 × 스키마 12 로
  고정 점유되던 연결이 사라져 PG ``max_connections`` 여유 확보.
- prepared statement 자동 PREPARE (``prepare_threshold=5``, 2026-04-26).
  5회 이상 동일 쿼리는 server-side PREPARE 캐시 → 매 호출 파싱/플래닝 비용 제거.

부팅 race 보호 (2026-04-26):
  PC 재부팅 → docker daemon 이 6 컨테이너를 동시 기동 → PG 가 startup 단계
  (WAL recovery 등) 인 동안 다른 컨테이너가 풀을 즉시 열어 ``the database
  system is starting up`` 에러가 모든 모듈에서 일제히 누출되던 이슈가 있었다.
  ``_make_pool`` 은 이제 lazy ``open=False`` 로 풀을 만든 뒤, **선행 readiness
  probe** (단일 connect + ``SELECT 1``) 를 짧은 backoff 로 돌려 PG 가 실제
  쿼리를 받을 수 있는 상태인지 확인한 다음 ``pool.open() + wait()`` 한다.
  probe 단계 에러는 외부로 누출되지 않고 디버그 로그만 남는다.

과거 이력: PgBouncer 1.25.1 session pooling 을 경유했으나 이중 풀링 +
unqualified table reference 129+ 건 때문에 transaction mode 전환 불가,
session mode 에서는 서버 연결 영구 점유로 포화 → 제거.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from typing import Dict, Optional

from oneqaz_trading_mcp.shared.db.config import DBConfig, get_config

logger = logging.getLogger(__name__)

# 부팅 race 보호용 readiness probe 파라미터.
#   PG startup (WAL recovery + initdb 후속 작업) 은 정상적으로 5~30초 사이.
#   total budget 60초면 충분하고, 초과 시엔 PG 자체가 비정상이라 빨리 실패하는 게 낫다.
_PG_READY_TOTAL_TIMEOUT_S = 60.0
_PG_READY_BACKOFF_S = 1.0
_PG_READY_PROBE_TIMEOUT_S = 3.0

# 인식되는 스키마 목록 (00_schemas.sql 과 동기화)
KNOWN_SCHEMAS = frozenset({
    "admin",
    "llm_factory",
    "external_context",
    "agent_history",
    "api",
    "rl_pipeline",
    "market_coin",
    "market_us",
    "market_kr",
    "market_global",
    "market_coin_struct",
    "market_kr_struct",
    "market_us_struct",
    "crow",
    "mcp_analytics",
    "public",
})

_pools: Dict[str, "object"] = {}  # schema -> ConnectionPool
_lock = threading.Lock()


# ════════════════════════════════════════════════════════════════════
# psycopg.pool 부팅 race 메시지 필터
# ════════════════════════════════════════════════════════════════════
# psycopg_pool 내부 background worker 는 reconnect 실패 시
# ``logger.warning("error connecting in %r: %s", ...)`` 으로 stderr 누출.
# readiness probe 가 통과한 직후엔 거의 없지만, multi-schema pool 이 동시에
# 만들어지는 첫 1~2 초 동안 잔여 race 가 있을 수 있다. 부팅 후 N 초간만
# "starting up" / "shutting down" / "connection refused" 패턴을 필터로 차단.
_BOOT_FILTER_WINDOW_S = 90.0
_BOOT_FILTER_PATTERNS = (
    "the database system is starting up",
    "the database system is shutting down",
    "connection refused",
)
# idle-in-tx 로 죽은 conn 회수는 풀의 정상 동작이지만 stderr 가 시끄러워진다.
# psycopg / psycopg_pool 양쪽 logger 의 WARNING 메시지 중 아래 패턴은 DEBUG 로 격하.
# 진짜 누수(트래픽 정지 후에도 발생) 는 PG 서버측 로그 + 모니터링 supervisor 에서 본다.
_NOISE_DEMOTE_PATTERNS = (
    "error ignored in rollback",
    "discarding closed connection",
    "terminating connection due to idle-in-transaction",
)
# 2026-05-17: PG 비밀번호 drift 사고 대응.
# 비번이 어긋나면 psycopg.pool 백그라운드 reconnect 가 60s 간격으로 영구 재시도
# 하면서 매 사이클당 5~6개 WARNING 을 stderr 로 누출. 30분간 1000줄 넘게 쌓여
# 다른 문제들이 다 묻혔다. 이 패턴은 **단발 CRITICAL 로 한 번만 출력** 후 N초간
# 중복 차단. drift 회복되면 다음 사이클부터 다시 통과.
_AUTH_FAIL_PATTERNS = (
    "password authentication failed",
    "role does not exist",
    "no pg_hba.conf entry",
)
_AUTH_FAIL_THROTTLE_S = 30.0
_AUTH_FAIL_LAST_EMIT: Dict[str, float] = {}  # pattern -> last monotonic time
_AUTH_FAIL_LOCK = threading.Lock()

_boot_filter_installed = False
_boot_filter_install_lock = threading.Lock()
_process_start_time = time.monotonic()


class _BootRaceFilter(logging.Filter):
    """부팅 직후 PG startup race 메시지만 일시 차단.

    윈도우 종료 후엔 모든 메시지를 통과시켜 운영 중 진짜 장애는 보이게 한다.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if time.monotonic() - _process_start_time > _BOOT_FILTER_WINDOW_S:
            return True
        try:
            msg = record.getMessage().lower()
        except Exception:
            return True
        return not any(pat in msg for pat in _BOOT_FILTER_PATTERNS)


class _NoiseDropFilter(logging.Filter):
    """idle-in-tx 회수 같은 정상 노이즈 WARNING 을 차단.

    풀의 ``check_connection`` 이 dead conn 을 회수할 때 발생하는
    ``error ignored in rollback`` / ``discarding closed connection`` /
    ``terminating connection due to idle-in-transaction`` 은
    풀이 제대로 작동하고 있다는 정상 signal. WARNING 레벨로 stderr 로 새면
    운영 노이즈가 너무 크다. 진짜 누수는 PG 서버 로그 + 풀 stats() 로 본다.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno < logging.WARNING:
            return True
        try:
            msg = record.getMessage().lower()
        except Exception:
            return True
        if any(pat in msg for pat in _NOISE_DEMOTE_PATTERNS):
            return False
        return True


class _AuthFailThrottleFilter(logging.Filter):
    """PG 인증 실패 / role missing / pg_hba 거부 WARNING 을 throttle.

    2026-05-17 사고: mcp_ro 비번 drift 로 풀이 30분간 60초 사이클로 재시도하며
    매 사이클 WARNING 6줄 → 1000줄 넘게 누적. 사용자가 다른 에러 못 봤음.

    동작:
      - 매칭되는 패턴이면 같은 패턴 기준 ``_AUTH_FAIL_THROTTLE_S`` 초 안에는
        ``return False`` 로 차단. (handler level filter 로 부착되므로 차단 가능.)
      - 첫 출현 시 record level 을 CRITICAL 로 승격 + 메시지 prefix 에 마커 추가
        → 운영자가 즉시 인지하게.
      - drift 가 회복되면 throttle 윈도우 지나서 자동으로 다음 사이클부터 정상.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage().lower()
        except Exception:
            return True
        matched = next((p for p in _AUTH_FAIL_PATTERNS if p in msg), None)
        if matched is None:
            return True
        now = time.monotonic()
        with _AUTH_FAIL_LOCK:
            last = _AUTH_FAIL_LAST_EMIT.get(matched, 0.0)
            should_emit = now - last >= _AUTH_FAIL_THROTTLE_S
            if should_emit:
                _AUTH_FAIL_LAST_EMIT[matched] = now
        if not should_emit:
            return False  # 차단
        # 첫 출현 → CRITICAL 로 승격하고 액션 가이드를 메시지에 묻음
        record.levelno = logging.CRITICAL
        record.levelname = "CRITICAL"
        try:
            original = record.getMessage()
        except Exception:
            original = str(record.msg)
        record.msg = (
            "[PG AUTH DRIFT] %s — run `bash scripts/sync_pg_passwords.sh` to fix"
            % original
        )
        record.args = ()
        return True


_BOOT_FILTER_OBJS: Dict[str, logging.Filter] = {}
_BOOT_FILTER_HANDLER_IDS: set = set()  # 이미 filter 부착된 handler id() 집합


class _ThrottledStderrHandler(logging.StreamHandler):
    """psycopg.pool 전용 stderr handler — auth-fail throttle filter 가 부착된 채로
    fresh interpreter (uvicorn workers) 에서도 filter 가 가장 처음 record 를 받게 한다.

    설계:
      - ``psycopg.pool`` logger 에 이 handler 를 부착하면, propagate=False 로 끊고
        오직 이 handler 만이 stderr 출력. 우리 filter 들이 record 를 통과/차단/격하
        결정.
      - workers 가 spawn 으로 만들어지면 import 단계에서 한 번 호출되어 부착.
    """
    def __init__(self) -> None:
        super().__init__(stream=sys.stderr)
        self.setLevel(logging.WARNING)
        # 표준 uvicorn 의 stderr 출력 포맷과 통일 (level:logger:message).
        self.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))


def _install_boot_filter_once() -> None:
    """``psycopg.pool`` / ``psycopg`` 로거 + 모든 stderr handler 에 idempotent 부착.

    중요: filter 가 logger 인스턴스에만 붙으면 propagation 경로에서 root 가
    handler 0개일 때 ``logging.lastResort`` (StreamHandler stderr level=30) 가
    직접 record 를 받아 emit. 우리 logger filter 는 propagation 단계에서 통과
    여부만 결정 → ``return False`` 가 효과 있지만, record.levelno 격하만 했을
    때는 lastResort 가 자체 level 검사 후 출력. **handler-level filter** 부착이
    가장 확실한 차단 지점.

    부팅 순서 race: uvicorn 이 import 시점 이후 logger handler 를 추가하므로,
    매 ``_make_pool`` 호출마다 새 handler 가 있는지 확인 후 부착. 비용은 무시
    가능 (handler 수 한 자릿수, id 집합 lookup).
    """
    global _boot_filter_installed
    with _boot_filter_install_lock:
        if not _BOOT_FILTER_OBJS:
            _BOOT_FILTER_OBJS["boot"] = _BootRaceFilter()
            _BOOT_FILTER_OBJS["noise"] = _NoiseDropFilter()
            _BOOT_FILTER_OBJS["auth"] = _AuthFailThrottleFilter()
        # [2026-05-17] psycopg.pool / psycopg 로거를 격리 전용 handler 로 분리.
        # propagate=False 로 root 경로를 끊고, 우리 단독 handler 가 stderr 로
        # 출력. filter 는 **handler 에만** 부착. logger 와 handler 양쪽에 같은
        # 필터를 두면 logger 단계에서 throttle window 가 갱신되고 handler 단계
        # 에서 같은 record 가 throttle hit 으로 차단되어 첫 1줄도 못 떨어진다.
        for name in ("psycopg.pool", "psycopg"):
            lg = logging.getLogger(name)
            has_own = any(
                isinstance(h, _ThrottledStderrHandler) for h in lg.handlers
            )
            if not has_own:
                h = _ThrottledStderrHandler()
                for f in _BOOT_FILTER_OBJS.values():
                    h.addFilter(f)
                lg.addHandler(h)
                lg.propagate = False  # root 로 안 새게
        # lastResort 에도 filter 부착 (다른 경로로 새는 record 방어). 같은 filter
        # 인스턴스를 쓰면 propagation 안 되도록 한 우리 handler 와 throttle 카운트
        # 공유 — 하지만 propagate=False 이라 lastResort 경로는 안 탐. 안전망용.
        if logging.lastResort is not None:
            lr_id = id(logging.lastResort)
            if lr_id not in _BOOT_FILTER_HANDLER_IDS:
                for f in _BOOT_FILTER_OBJS.values():
                    logging.lastResort.addFilter(f)
                _BOOT_FILTER_HANDLER_IDS.add(lr_id)
        # uvicorn 자체 logger 들의 현재 handler 들에도 부착 — psycopg 가 아닌 다른
        # 경로로 password authentication 메시지가 새는 경우 (예: SQLAlchemy 등).
        candidate_loggers = (
            logging.getLogger(),
            logging.getLogger("uvicorn"),
            logging.getLogger("uvicorn.error"),
            logging.getLogger("uvicorn.access"),
        )
        for lg in candidate_loggers:
            for h in lg.handlers:
                hid = id(h)
                if hid in _BOOT_FILTER_HANDLER_IDS:
                    continue
                for f in _BOOT_FILTER_OBJS.values():
                    h.addFilter(f)
                _BOOT_FILTER_HANDLER_IDS.add(hid)
        _boot_filter_installed = True


# 모듈 임포트 시점에 즉시 필터 설치 — _make_pool 호출보다 먼저 워커가 psycopg 를
# 쓸 수 있는 race 방어. ProcessPool fork 자식이 부모 풀 없이 자체 풀을 만들기
# 직전에 첫 dead conn 메시지가 새던 케이스 차단.
_install_boot_filter_once()


def _silence_psycopg_idle_noise() -> None:
    """추가 안전망 — logging filter 가 어떤 이유로든 작동 안 할 때 대비.

    psycopg 의 Connection.__exit__ / pool 의 putconn 경로에서 정상 dead conn 회수
    노이즈가 발생할 때, 메시지 자체를 logger 단에서 출력하기 전에 가로챈다.
    logging propagation 동작에 의존하지 않고 logger 의 ``handle()`` 을 monkey-patch.
    """
    targets = ("psycopg", "psycopg.pool")
    for name in targets:
        lg = logging.getLogger(name)
        if getattr(lg, "_oneqaz_silenced", False):
            continue
        orig_handle = lg.handle

        def _patched(record, _orig=orig_handle):
            try:
                if record.levelno >= logging.WARNING:
                    msg = record.getMessage().lower()
                    if any(pat in msg for pat in _NOISE_DEMOTE_PATTERNS):
                        return
                    # 2026-05-17: auth-fail throttle 은 handler 단의 _AuthFailThrottleFilter
                    # 에 위임. 여기서 또 throttle 카운트 갱신하면 filter 가 두 번째 호출
                    # 받을 때 차단해 첫 1줄도 안 떨어지는 race 발생.
            except Exception:
                pass
            return _orig(record)

        lg.handle = _patched
        lg._oneqaz_silenced = True


_silence_psycopg_idle_noise()


def _write_degraded_marker(kind: str, detail: str) -> None:
    """프로세스 헬스 상태를 파일로 노출 (degraded marker).

    2026-05-17: PG 인증 실패가 60초 안에 풀리지 않으면 즉시 이 마커를 작성.
    supervisor 스크립트(start_*.sh) / restart_*.sh / admin healthz 가 이 파일을
    읽어 사용자에게 ``HTTP listening but PG auth FAILED`` 같은 정확한 상태를
    보일 수 있다.

    회복되면 ``_clear_degraded_marker`` 가 지운다.
    """
    try:
        marker_dir = os.environ.get(
            "HEALTHZ_DIR", "/workspace/logs/healthz"
        )
        os.makedirs(marker_dir, exist_ok=True)
        app = os.environ.get("PG_APPLICATION_NAME", "unknown")
        path = os.path.join(marker_dir, f"{app}.degraded")
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"{kind}\n{detail}\n{int(time.time())}\n")
    except Exception:
        pass


def _clear_degraded_marker() -> None:
    try:
        marker_dir = os.environ.get(
            "HEALTHZ_DIR", "/workspace/logs/healthz"
        )
        app = os.environ.get("PG_APPLICATION_NAME", "unknown")
        path = os.path.join(marker_dir, f"{app}.degraded")
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def _classify_pg_error(exc: BaseException) -> Optional[str]:
    """psycopg 예외 메시지를 사고 분류로 매핑.

    Returns:
        ``"auth_drift"`` — 비밀번호/역할/pg_hba 인증 실패 (sync_pg_passwords.sh 필요)
        ``"unreachable"`` — connection refused / 호스트 없음 (PG 컨테이너 점검)
        ``"starting_up"`` — PG WAL recovery 중 (대기로 회복)
        ``None``         — 분류 안 됨
    """
    try:
        msg = str(exc).lower()
    except Exception:
        return None
    if any(p in msg for p in _AUTH_FAIL_PATTERNS):
        return "auth_drift"
    if "the database system is starting up" in msg:
        return "starting_up"
    if "connection refused" in msg or "could not translate host" in msg:
        return "unreachable"
    return None


def _wait_for_pg_ready(config: DBConfig) -> None:
    """PG 가 query 를 받을 수 있는 상태가 될 때까지 대기.

    docker-compose 의 ``depends_on: service_healthy`` 가 ``pg_isready`` 통과를
    보장하긴 하지만, ``pg_isready`` 는 connection accept 시점만 본다. 그 직후
    클라이언트가 SELECT 를 보내면 ``the database system is starting up`` (PG
    startup recovery 잔여 윈도우) 또는 ``the database system is shutting down``
    이 떨어질 수 있다. 이 함수는 가벼운 ``SELECT 1`` 으로 실제 쿼리 수용
    가능 여부를 확인한다.

    실패해도 풀 생성은 진행한다 — 이후 ``pool.open()`` 이 자체 retry 를 가지므로
    여기서 죽이지 않는다. 다만 실패 메시지를 누출하지 않고 디버그 로그만 남긴다.

    2026-05-17 강화:
      - 매 시도마다 예외를 분류 (``_classify_pg_error``). ``auth_drift`` 가
        2회 이상 연속 떨어지면 즉시 degraded marker + CRITICAL 한 줄로 누출
        → 운영자가 1초 안에 인지하고 ``sync_pg_passwords.sh`` 실행 가능.
      - probe 통과 시 marker 자동 삭제 → 자가 복구.
    """
    import psycopg

    deadline = time.monotonic() + _PG_READY_TOTAL_TIMEOUT_S
    last_err: Optional[BaseException] = None
    attempt = 0
    auth_drift_seen = 0
    auth_drift_announced = False
    while time.monotonic() < deadline:
        attempt += 1
        try:
            conninfo = (
                f"{config.pg_conninfo} "
                f"connect_timeout={int(_PG_READY_PROBE_TIMEOUT_S)}"
            )
            with psycopg.connect(conninfo, autocommit=True) as probe:
                with probe.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            if attempt > 1:
                logger.info(
                    "pg readiness probe ok after %d attempts (host=%s:%d)",
                    attempt, config.pg_host, config.pg_port,
                )
            _clear_degraded_marker()
            return
        except Exception as exc:
            last_err = exc
            kind = _classify_pg_error(exc)
            if kind == "auth_drift":
                auth_drift_seen += 1
                # 2번 연속 인증 실패면 즉시 사용자에게 알림. 일시적 race 가 아닌
                # 진짜 drift 라는 신호.
                if auth_drift_seen >= 2 and not auth_drift_announced:
                    auth_drift_announced = True
                    user = os.environ.get("PG_USER", "?")
                    logger.critical(
                        "[PG AUTH DRIFT] readiness probe failed user=%s "
                        "host=%s:%d — run `bash scripts/sync_pg_passwords.sh` "
                        "(error=%s)",
                        user, config.pg_host, config.pg_port, exc,
                    )
                    _write_degraded_marker(
                        "pg_auth_drift",
                        f"user={user} host={config.pg_host}:{config.pg_port} "
                        f"error={exc}",
                    )
            logger.debug(
                "pg readiness probe attempt %d failed (kind=%s): %s",
                attempt, kind, exc,
            )
            time.sleep(_PG_READY_BACKOFF_S)
    final_kind = _classify_pg_error(last_err) if last_err else None
    if final_kind == "auth_drift":
        user = os.environ.get("PG_USER", "?")
        logger.critical(
            "[PG AUTH DRIFT] readiness probe timed out (%.1fs) user=%s "
            "host=%s:%d — run `bash scripts/sync_pg_passwords.sh` "
            "(last_err=%s)",
            _PG_READY_TOTAL_TIMEOUT_S, user, config.pg_host, config.pg_port,
            last_err,
        )
        _write_degraded_marker(
            "pg_auth_drift",
            f"user={user} host={config.pg_host}:{config.pg_port} "
            f"timeout_after={_PG_READY_TOTAL_TIMEOUT_S}s",
        )
    else:
        logger.warning(
            "pg readiness probe timed out after %.1fs (host=%s:%d, last_err=%s) — "
            "proceeding with pool open; psycopg_pool will retry in background",
            _PG_READY_TOTAL_TIMEOUT_S, config.pg_host, config.pg_port, last_err,
        )


def _make_pool(schema: str, config: DBConfig):
    """스키마 전용 ``ConnectionPool`` 생성.

    Lazy import: psycopg_pool 은 PG 모드가 아닐 때 설치 안 돼 있을 수도 있으므로.

    부팅 race 보호: ``_wait_for_pg_ready`` 로 PG 가 query 수용 가능한 상태인지
    먼저 확인 후 풀을 ``open=True`` + ``wait()`` 로 명시적 대기. 이전엔
    ``open=True`` 만으로 즉시 연결 시도 → PG startup 윈도우와 충돌 → 에러 누출.
    """
    from psycopg_pool import ConnectionPool

    # 부팅 race 필터를 한 번만 설치 (운영 중엔 자동으로 영향 없음).
    _install_boot_filter_once()

    # 첫 풀 생성 시점에 1회 readiness probe. 같은 프로세스에서 두 번째 풀부터는
    # PG 가 이미 ready 상태라 빠르게 통과한다 (1회 SELECT 1).
    _wait_for_pg_ready(config)

    def _configure(conn) -> None:
        """풀에서 커넥션 발급 시 매번 호출.

        search_path / statement_timeout / idle_in_transaction_session_timeout 을
        세션에 설정.

        prepared statement 자동 prepare: 5회 이상 동일 쿼리 → server-side PREPARE
        캐시. 매 호출 파싱/플래닝 비용 (~3-5ms) 제거. (2026-04-26 무손실 가속)
        """
        conn.prepare_threshold = 5
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {schema}, public")
            cur.execute(f"SET statement_timeout = {config.pg_statement_timeout_ms}")
            # idle_in_transaction 60초 — 학습 self-play 같은 long-running 트랜잭션
            # 중간에 잘려서 재연결 비용 발생하던 문제 해결. dead session 은 pool 의
            # check_connection + _reset rollback 으로 회수. (2026-04-26)
            # PG_IDLE_IN_TX_TIMEOUT_MS env 로 override 가능 (백필/풀 재계산 잡 한정).
            _idle_ms = os.getenv("PG_IDLE_IN_TX_TIMEOUT_MS", "60000")
            cur.execute(f"SET idle_in_transaction_session_timeout = {int(_idle_ms)}")
            # TimescaleDB 압축 청크 UPDATE 시 decompression 한도. 0=unlimited.
            # 풀 재계산 잡(예: candles_calculate 1d FULL) 에서 50만+ row 압축 해제 필요.
            _decomp_limit = os.getenv("TS_DECOMP_LIMIT_PER_TX", "")
            if _decomp_limit:
                cur.execute(f"SET timescaledb.max_tuples_decompressed_per_dml_transaction = {int(_decomp_limit)}")
        conn.commit()

    def _reset(conn) -> None:
        """풀에 커넥션을 돌려받을 때마다 호출.

        psycopg 는 autocommit=False 기본값이라 단순 SELECT 도 INTRANS 로 남는다.
        putconn 시점에 rollback 을 걸어 다음 사용자가 깔끔한 세션을 받도록 보장.

        [2026-07-13 SAVEPOINT 25P01 사고 — systemic backstop]
        read_only caller (trade.core.database._PGConnShim) 가 conn.autocommit=True
        로 세팅한 뒤 원복 없이 반납하면, 오염된 conn 이 풀에 남아 다음 writer 에게
        전달된다. non-autocommit 을 전제로 SAVEPOINT 를 쓰는 writer
        (insert_signals_batch) 가 그 conn 을 받으면 "SAVEPOINT can only be used in
        transaction blocks" (SQLSTATE 25P01) 로 전 행 실패 → 시그널 전량 유실.
        _configure 는 새 물리 conn 생성 시에만 돌고 autocommit 을 안 건드리므로
        재사용 conn 은 영구 오염. 여기서 반납 시마다 autocommit 을 기본(False)으로
        정규화해 어떤 caller 도 오염을 누출하지 못하게 한다. 순서 주의: 반드시
        rollback 이후에 토글(열린 트랜잭션 중 autocommit 변경은 암묵 commit 유발).
        autocommit 을 원하는 caller 는 __enter__ 에서 매번 자체적으로 재설정한다.
        """
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            if conn.autocommit:
                conn.autocommit = False
        except Exception:
            pass

    # getconn 기본 timeout — caller 가 `pool.connection()` 을 timeout 인자 없이
    # 호출해도 무한 대기하지 않도록 풀 레벨에서 cap. 2026-05-12 intel/external_context
    # 메인 스레드가 `pool.connection()` 에서 무한 wait → causality_analyzer 사이클
    # 정지 → Stage 1+2(agent_history) 15h~5.6일 stale 사고 재발 방지.
    # 정상 사이클은 ms 단위로 conn 반환되므로 짧은 cap 으로 충분.
    # PG_POOL_GETCONN_TIMEOUT_SEC env 로 override 가능.
    # ※ 풀 레벨 timeout 은 일부 race 케이스(in-use counter leak)에서 효과 없는 것이
    # 2026-05-12 두 번째 hang 사고에서 확인됨. 따라서 hot path caller 는 명시적
    # `pool.connection(timeout=N)` 인자를 추가로 사용 (external_context._pg_readers).
    # [2026-06-14] 기본 30s→8s 하향. uvicorn(public) 워커 4개가 getconn 에서 30s 무한
    # 대기하며 동시 행(futex_wait)에 걸리던 반복 hang 의 1차 차단. 풀 고갈 시 8s 안에
    # PoolTimeout 으로 빠져나가 워커가 풀리고 버려진 스레드 슬롯도 회수된다(WS fan-out
    # 누적 방지). 정상 사이클은 ms 단위라 8s 도 충분한 마진.
    _pool_timeout = float(os.getenv("PG_POOL_GETCONN_TIMEOUT_SEC", "8"))

    # 2026-05-12: max_lifetime + reconnect_timeout 명시적 설정.
    # - max_lifetime: 풀 안에서 N초 이상 살아있는 conn 은 강제 폐기 → 풀 in-use
    #   카운터가 leak/race 로 깨졌어도 1시간이면 conn 이 자연 교체되어 자가 회복.
    # - reconnect_timeout: PG 가 죽었거나 네트워크 끊겼을 때 풀이 retry 하는 cap.
    #   기본 300s → 60s 로 단축해 hang 잠재 시간 축소.
    _max_lifetime = float(os.getenv("PG_POOL_MAX_LIFETIME_SEC", "3600"))
    _reconnect_timeout = float(os.getenv("PG_POOL_RECONNECT_TIMEOUT_SEC", "60"))

    pool = ConnectionPool(
        conninfo=config.pg_conninfo,
        min_size=config.pg_pool_min,
        max_size=config.pg_pool_max,
        timeout=_pool_timeout,
        max_lifetime=_max_lifetime,
        reconnect_timeout=_reconnect_timeout,
        configure=_configure,
        reset=_reset,
        # getconn 시 conn 살아있는지 검증. 서버가 idle_in_transaction_timeout
        # 으로 죽인 dead socket 을 돌려주지 않음. 실패 시 풀이 자동으로 새 conn
        # 생성. 핵심 버그픽스.
        check=ConnectionPool.check_connection,
        # idle conn 을 15초 보관 후 닫음 (기본 600s).
        # 2026-05-01: 30s → 15s 단축. 서버측 idle_in_transaction_session_timeout=60s
        # 보다 ShrinkPool tick(=max_idle)이 충분히 짧아야 dead conn 발생 전 회수.
        # 시그널 사이클의 ProcessPool 워커가 종목 간 9~10s gap 으로 풀에 conn 반환
        # → 서버 60s 도달 전 ShrinkPool 이 닫게 한다.
        max_idle=15.0,
        name=f"pool-{schema}",
        # 부팅 race 보호: open=False 로 만들고 readiness probe 통과 후 명시적
        # open(). True 로 하면 생성자에서 즉시 백그라운드 연결 시도 → PG startup
        # 윈도우와 충돌 시 stderr 로 에러가 그대로 누출됨.
        open=False,
    )
    pool.open()
    # 첫 conn 이 실제로 살아 열릴 때까지 대기. min_size=0 인 경우 wait() 는
    # 즉시 반환 (요구할 conn 이 없으므로) — 하지만 타임아웃 내 풀 자체의 healthy
    # 상태 확립은 보장된다.
    try:
        pool.wait(timeout=10.0)
    except Exception as exc:
        # readiness probe 가 통과했다면 여기 도달 가능성 낮지만, 만약 실패해도
        # 풀은 백그라운드 retry 를 계속 한다. 누출은 막는다.
        logger.debug("pool.wait() failed schema=%s: %s", schema, exc)
    logger.info(
        "psycopg pool opened schema=%s min=%d max=%d host=%s:%d",
        schema,
        config.pg_pool_min,
        config.pg_pool_max,
        config.pg_host,
        config.pg_port,
    )
    return pool


def get_pool(schema: str, config: Optional[DBConfig] = None):
    """주어진 스키마의 커넥션 풀 반환 (없으면 생성)."""
    if schema not in KNOWN_SCHEMAS:
        raise ValueError(
            f"Unknown schema {schema!r}. Must be one of {sorted(KNOWN_SCHEMAS)}"
        )
    cfg = config or get_config()

    # double-checked locking
    pool = _pools.get(schema)
    if pool is not None:
        return pool
    with _lock:
        pool = _pools.get(schema)
        if pool is None:
            pool = _make_pool(schema, cfg)
            _pools[schema] = pool
        return pool


def close_all() -> None:
    """모든 풀 닫기 (앱 종료 시)."""
    with _lock:
        for schema, pool in list(_pools.items()):
            try:
                pool.close()
                logger.info("psycopg pool closed schema=%s", schema)
            except Exception:
                logger.exception("failed to close pool schema=%s", schema)
        _pools.clear()


def stats() -> Dict[str, dict]:
    """각 풀의 통계 (모니터링용)."""
    result = {}
    for schema, pool in _pools.items():
        try:
            result[schema] = pool.get_stats()
        except Exception:
            result[schema] = {"error": "unavailable"}
    return result


def _reset_after_fork() -> None:
    """fork() 직후 자식 프로세스에서 호출.

    부모의 ConnectionPool 객체는 자식이 상속하지만 PG 서버측 세션은 부모 PID
    소속이라 자식이 사용하면 "SSL connection has been closed unexpectedly"
    또는 INTRANS 누수가 발생한다. 자식은 풀 dict 만 비우고, 다음 ``get_pool()``
    호출 때 새 풀을 만들도록 한다 (부모 풀 close 는 호출하지 않음 — 부모와 fd
    공유 상태이므로 close 가 부모 세션을 끊을 수 있음).
    """
    _pools.clear()


# fork-safe 보장: ProcessPoolExecutor 등에서 워커가 부모의 PG 풀을 잘못 재사용하지
# 않도록 한다. POSIX 전용 (Windows 는 spawn 만 가능하므로 무관).
if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)
