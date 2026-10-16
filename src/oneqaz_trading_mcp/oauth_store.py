# -*- coding: utf-8 -*-
"""OAuth 인가 서버 저장소 — OAuth P2 (2026-10-07).

설계 정본: ai_brain/mac_mini_tasks/HANDOFF_2026_10_07_oauth_design.md §4-2. 스키마 = migrations/oauth_as_p2.sql.

규칙:
  - 토큰·코드·대기 요청 ID 는 **sha256 hex 만** 받는다(원문은 이 모듈에 들어오지도 않는다 — 해시는 호출부가 만든다).
    예외 하나: `account_for_key` 는 API 키 원문으로 api.api_keys 를 조회한다(키 표 자체가 원문 키 PK 다). 저장·로그 없음.
  - "1회용"·"회전" 판정은 전부 **조건부 UPDATE 의 rowcount** 로 원자 처리한다.
    ⚠️ compat shim 은 RETURNING 첫 행을 스스로 소비해 fetchone() 이 None 이 된다 — RETURNING 을 쓰지 말 것.
  - PgOAuthStore 와 MemoryOAuthStore 는 같은 인터페이스다. Memory 는 테스트 전용(운영 경로에서 쓰지 않는다).
"""

from __future__ import annotations

import json
import os
import secrets
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

PENDING_TTL_SEC = 600
CODE_TTL_SEC = 60
ACCESS_TTL_SEC = 3600
REFRESH_TTL_SEC = 30 * 86400


def new_id(prefix: str) -> str:
    return f"{prefix}{secrets.token_hex(16)}"  # 128bit


@dataclass(frozen=True)
class ClientRecord:
    oauth_client_id: str
    registration: str  # cimd / dcr / static
    redirect_uris: List[str]
    client_name: Optional[str] = None
    platform_key: Optional[str] = None
    auth_method: str = "none"
    metadata: Optional[Dict[str, Any]] = None
    cache_until: Optional[float] = None  # epoch — CIMD 캐시 만료


@dataclass(frozen=True)
class PendingRecord:
    req_hash: str
    oauth_client_id: str
    params: Dict[str, Any]
    attempts: int
    expires_at: float


@dataclass(frozen=True)
class CodeRecord:
    code_hash: str
    grant_id: str
    oauth_client_id: str
    account_id: str
    scopes: List[str]
    resource: str
    redirect_uri: str
    redirect_uri_explicit: bool
    code_challenge: str
    expires_at: float
    used: bool
    grant_revoked: bool


@dataclass(frozen=True)
class TokenRecord:
    token_hash: str
    kind: str  # access / refresh
    grant_id: str
    family_id: str
    oauth_client_id: str
    account_id: str
    account_status: str
    account_plan: str
    scopes: List[str]
    resource: str
    expires_at: float
    revoked: bool
    grant_revoked: bool


class StoreUnavailable(RuntimeError):
    """저장소 조회 자체 실패 — "없음" 으로 위장하지 않는다(key_store.KeyStoreUnavailable 와 같은 원칙)."""


# ─────────────────────────────────────────────────────────────────────────────
# PG 구현
# ─────────────────────────────────────────────────────────────────────────────


class PgOAuthStore:
    """api 스키마 OAuth 표. 모든 공개 메서드는 자체 트랜잭션으로 커밋한다."""

    def _conn(self):
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
        try:
            return open_schema_connection("api", getconn_timeout=5.0)
        except Exception as e:  # 풀 고갈·인증 실패 등 — 호출부가 503 으로 바꾼다
            raise StoreUnavailable(str(e)) from e

    def _run(self, fn):
        conn = self._conn()
        try:
            out = fn(conn)
            conn.commit()
            return out
        except StoreUnavailable:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── clients ──
    def get_client(self, client_id: str) -> Optional[ClientRecord]:
        def q(conn):
            row = conn.execute(
                "SELECT oauth_client_id, registration, redirect_uris, client_name, platform_key, auth_method, "
                "metadata, extract(epoch from cache_until) AS cache_until FROM oauth_clients WHERE oauth_client_id = ?",
                (client_id,),
            ).fetchone()
            if not row:
                return None
            uris = row["redirect_uris"]
            meta = row["metadata"]
            return ClientRecord(
                oauth_client_id=row["oauth_client_id"], registration=row["registration"],
                redirect_uris=list(uris if isinstance(uris, list) else json.loads(uris)),
                client_name=row["client_name"], platform_key=row["platform_key"], auth_method=row["auth_method"],
                metadata=(meta if isinstance(meta, dict) or meta is None else json.loads(meta)),
                cache_until=(float(row["cache_until"]) if row["cache_until"] is not None else None),
            )
        return self._run(q)

    def upsert_client(self, rec: ClientRecord) -> None:
        def q(conn):
            conn.execute(
                "INSERT INTO oauth_clients (oauth_client_id, registration, platform_key, client_name, redirect_uris, "
                " auth_method, metadata, cache_until) "
                "VALUES (?, ?, ?, ?, ?::jsonb, ?, ?::jsonb, to_timestamp(?)) "
                "ON CONFLICT (oauth_client_id) DO UPDATE SET platform_key = EXCLUDED.platform_key, "
                " client_name = EXCLUDED.client_name, redirect_uris = EXCLUDED.redirect_uris, "
                " auth_method = EXCLUDED.auth_method, metadata = EXCLUDED.metadata, cache_until = EXCLUDED.cache_until",
                (rec.oauth_client_id, rec.registration, rec.platform_key, rec.client_name,
                 json.dumps(rec.redirect_uris), rec.auth_method,
                 (json.dumps(rec.metadata) if rec.metadata is not None else None), rec.cache_until),
            )
        self._run(q)

    def touch_client(self, client_id: str) -> None:
        self._run(lambda c: c.execute("UPDATE oauth_clients SET last_used_at = now() WHERE oauth_client_id = ?", (client_id,)))

    # ── pending (동의 화면 대기) ──
    def save_pending(self, req_hash: str, client_id: str, params: Dict[str, Any], ttl: int = PENDING_TTL_SEC) -> None:
        self._run(lambda c: c.execute(
            "INSERT INTO oauth_pending (req_hash, oauth_client_id, params, expires_at) "
            "VALUES (?, ?, ?::jsonb, now() + make_interval(secs => ?))",
            (req_hash, client_id, json.dumps(params), ttl),
        ))

    def get_pending(self, req_hash: str) -> Optional[PendingRecord]:
        def q(conn):
            row = conn.execute(
                "SELECT req_hash, oauth_client_id, params, attempts, extract(epoch from expires_at) AS exp "
                "FROM oauth_pending WHERE req_hash = ? AND consumed_at IS NULL AND expires_at > now()",
                (req_hash,),
            ).fetchone()
            if not row:
                return None
            p = row["params"]
            return PendingRecord(row["req_hash"], row["oauth_client_id"],
                                 p if isinstance(p, dict) else json.loads(p), int(row["attempts"]), float(row["exp"]))
        return self._run(q)

    def bump_pending_attempts(self, req_hash: str, max_attempts: int) -> int:
        """잘못된 키 입력 1회. 상한 도달 시 소비(재시도 불가). 반환 = 새 attempts."""
        def q(conn):
            conn.execute("UPDATE oauth_pending SET attempts = attempts + 1 WHERE req_hash = ? AND consumed_at IS NULL",
                         (req_hash,))
            row = conn.execute("SELECT attempts FROM oauth_pending WHERE req_hash = ?", (req_hash,)).fetchone()
            n = int(row["attempts"]) if row else max_attempts
            if n >= max_attempts:
                conn.execute("UPDATE oauth_pending SET consumed_at = now() WHERE req_hash = ? AND consumed_at IS NULL",
                             (req_hash,))
            return n
        return self._run(q)

    def consume_pending(self, req_hash: str) -> bool:
        def q(conn):
            cur = conn.execute(
                "UPDATE oauth_pending SET consumed_at = now() "
                "WHERE req_hash = ? AND consumed_at IS NULL AND expires_at > now()", (req_hash,))
            return cur.rowcount == 1
        return self._run(q)

    # ── accounts ──
    def account_for_key(self, api_key: str, plan_id: str) -> tuple[str, str, str]:
        """API 키로 로그인 → (account_id, plan_id, status). 키에 계정이 없으면 만들고 연결한다.

        계정 플랜 = 연결 시점의 키 플랜(설계: 키가 계정에 연결되면 계정 플랜이 정본 — 두 값을 같게 맞춘다).
        """
        def q(conn):
            row = conn.execute("SELECT account_id FROM api_keys WHERE key = ?", (api_key,)).fetchone()
            if row is None:
                raise LookupError("api key vanished")  # 호출 직전 lookup_key 가 valid 였다 — 경합
            acc = row["account_id"]
            if acc is None:
                acc = new_id("acc_")
                conn.execute("INSERT INTO accounts (account_id, plan_id) VALUES (?, ?)", (acc, plan_id))
                cur = conn.execute("UPDATE api_keys SET account_id = ? WHERE key = ? AND account_id IS NULL", (acc, api_key))
                if cur.rowcount != 1:  # 동시 로그인이 먼저 연결했다 — 그쪽 계정을 쓴다(방금 만든 행은 롤백 대상)
                    raise _Retry()
            arow = conn.execute("SELECT plan_id, status FROM accounts WHERE account_id = ?", (acc,)).fetchone()
            return acc, arow["plan_id"], arow["status"]
        for _ in range(2):
            try:
                return self._run(q)
            except _Retry:
                continue
        raise RuntimeError("account link contention")

    def create_free_account(self) -> tuple[str, str, str]:
        """[2026-10-14 R1] 키 없는 무료 계정 — 동의 화면 "무료로 연결". 어떤 키·이메일·개인정보와도 연결되지 않는다.

        연결(grant)이 모두 사라지면 보존 정리(`free_accounts_unlinked_1d`)가 삭제한다.
        """
        acc = new_id("acc_")
        self._run(lambda c: c.execute("INSERT INTO accounts (account_id, plan_id) VALUES (?, 'free')", (acc,)))
        return acc, "free", "active"

    # ── grants / codes ──
    def create_grant(self, account_id: str, client_id: str, scopes: List[str], resource: str) -> str:
        gid = new_id("grt_")
        self._run(lambda c: c.execute(
            "INSERT INTO oauth_grants (grant_id, account_id, oauth_client_id, scopes, resource) VALUES (?, ?, ?, ?, ?)",
            (gid, account_id, client_id, " ".join(scopes), resource)))
        return gid

    def save_code(self, code_hash: str, grant_id: str, redirect_uri: str, explicit: bool, code_challenge: str,
                  ttl: int = CODE_TTL_SEC) -> None:
        self._run(lambda c: c.execute(
            "INSERT INTO oauth_codes (code_hash, grant_id, redirect_uri, redirect_uri_explicit, code_challenge, expires_at) "
            "VALUES (?, ?, ?, ?, ?, now() + make_interval(secs => ?))",
            (code_hash, grant_id, redirect_uri, bool(explicit), code_challenge, ttl)))

    def load_code(self, code_hash: str) -> Optional[CodeRecord]:
        def q(conn):
            row = conn.execute(
                "SELECT c.code_hash, c.grant_id, g.oauth_client_id, g.account_id, g.scopes, g.resource, c.redirect_uri, "
                " c.redirect_uri_explicit, c.code_challenge, extract(epoch from c.expires_at) AS exp, "
                " (c.used_at IS NOT NULL) AS used, (g.revoked_at IS NOT NULL) AS grant_revoked "
                "FROM oauth_codes c JOIN oauth_grants g ON g.grant_id = c.grant_id WHERE c.code_hash = ?",
                (code_hash,),
            ).fetchone()
            if not row:
                return None
            return CodeRecord(row["code_hash"], row["grant_id"], row["oauth_client_id"], row["account_id"],
                              row["scopes"].split(), row["resource"], row["redirect_uri"], bool(row["redirect_uri_explicit"]),
                              row["code_challenge"], float(row["exp"]), bool(row["used"]), bool(row["grant_revoked"]))
        return self._run(q)

    def consume_code(self, code_hash: str) -> bool:
        def q(conn):
            cur = conn.execute("UPDATE oauth_codes SET used_at = now() WHERE code_hash = ? AND used_at IS NULL", (code_hash,))
            return cur.rowcount == 1
        return self._run(q)

    # ── tokens ──
    def issue_tokens(self, grant_id: str, family_id: str, access_hash: str, refresh_hash: Optional[str],
                     access_ttl: int = ACCESS_TTL_SEC, refresh_ttl: int = REFRESH_TTL_SEC) -> None:
        def q(conn):
            conn.execute(
                "INSERT INTO oauth_tokens (token_hash, kind, grant_id, family_id, expires_at) "
                "VALUES (?, 'access', ?, ?, now() + make_interval(secs => ?))", (access_hash, grant_id, family_id, access_ttl))
            if refresh_hash:
                conn.execute(
                    "INSERT INTO oauth_tokens (token_hash, kind, grant_id, family_id, expires_at) "
                    "VALUES (?, 'refresh', ?, ?, now() + make_interval(secs => ?))", (refresh_hash, grant_id, family_id, refresh_ttl))
        self._run(q)

    def load_token(self, token_hash: str, kind: str) -> Optional[TokenRecord]:
        def q(conn):
            row = conn.execute(
                "SELECT t.token_hash, t.kind, t.grant_id, t.family_id, g.oauth_client_id, g.account_id, "
                " a.status AS account_status, a.plan_id AS account_plan, g.scopes, g.resource, "
                " extract(epoch from t.expires_at) AS exp, (t.revoked_at IS NOT NULL) AS revoked, "
                " (g.revoked_at IS NOT NULL) AS grant_revoked "
                "FROM oauth_tokens t JOIN oauth_grants g ON g.grant_id = t.grant_id "
                "JOIN accounts a ON a.account_id = g.account_id WHERE t.token_hash = ? AND t.kind = ?",
                (token_hash, kind),
            ).fetchone()
            if not row:
                return None
            return TokenRecord(row["token_hash"], row["kind"], row["grant_id"], row["family_id"], row["oauth_client_id"],
                               row["account_id"], row["account_status"], row["account_plan"], row["scopes"].split(),
                               row["resource"], float(row["exp"]), bool(row["revoked"]), bool(row["grant_revoked"]))
        try:
            return self._run(q)
        except StoreUnavailable:
            raise
        except Exception as e:
            raise StoreUnavailable(str(e)) from e

    def revoke_token_if_active(self, token_hash: str) -> bool:
        def q(conn):
            cur = conn.execute("UPDATE oauth_tokens SET revoked_at = now() WHERE token_hash = ? AND revoked_at IS NULL",
                               (token_hash,))
            return cur.rowcount == 1
        return self._run(q)

    def revoke_family(self, family_id: str) -> int:
        def q(conn):
            cur = conn.execute("UPDATE oauth_tokens SET revoked_at = now() WHERE family_id = ? AND revoked_at IS NULL",
                               (family_id,))
            return cur.rowcount
        return self._run(q)

    def revoke_grant(self, grant_id: str) -> None:
        def q(conn):
            conn.execute("UPDATE oauth_grants SET revoked_at = now() WHERE grant_id = ? AND revoked_at IS NULL", (grant_id,))
            conn.execute("UPDATE oauth_tokens SET revoked_at = now() WHERE grant_id = ? AND revoked_at IS NULL", (grant_id,))
        self._run(q)


class _Retry(Exception):
    pass


# ─────────────────────────────────────────────────────────────────────────────
# 보존 기간 정리 (설계 §4-5 · 처리방침 초안 03절 — 이 배선 없이 처리방침을 게시하면 약속 위반)
# ─────────────────────────────────────────────────────────────────────────────

# (라벨, 표, 조건) — 순서대로 실행. 조건은 파라미터 없는 SQL(now() 기준). 자식(FK)은 delete_with_cascade 가
# pg_constraint 로 찾아 **부모 삭제 집합 기준**으로 먼저 지운다(목록 하드코딩 금지 — shared/db/retention.py).
OAUTH_RETENTION_RULES = [
    ("pending_1d", "oauth_pending",
     "consumed_at < now() - interval '1 day' OR expires_at < now() - interval '1 day'"),
    ("codes_1d", "oauth_codes",
     "used_at < now() - interval '1 day' OR expires_at < now() - interval '1 day'"),
    ("tokens_7d", "oauth_tokens",  # 재사용 탐지 여유 7일 — 회전으로 폐기된 refresh 가 다시 오면 계열을 폐기해야 한다
     "revoked_at < now() - interval '7 days' OR expires_at < now() - interval '7 days'"),
    ("grants_revoked_30d", "oauth_grants",
     "revoked_at < now() - interval '30 days'"),
    ("grants_unused_12m", "oauth_grants",  # 마지막 토큰 발급(=사용·갱신) 후 12개월
     "revoked_at IS NULL AND created_at < now() - interval '365 days' AND NOT EXISTS "
     "(SELECT 1 FROM oauth_tokens t WHERE t.grant_id = oauth_grants.grant_id AND t.created_at > now() - interval '365 days')"),
    ("dcr_unlinked_30d", "oauth_clients",
     "registration = 'dcr' AND created_at < now() - interval '30 days' AND NOT EXISTS "
     "(SELECT 1 FROM oauth_grants g WHERE g.oauth_client_id = oauth_clients.oauth_client_id)"),
    # [2026-10-14 R1] 키 없는 무료 계정 — 남은 연결(grant)이 없으면 삭제. 위 grant 규칙들 **뒤**에 둬야 같은 회차에 정리된다.
    #   1일 여유 = 계정 생성과 grant 생성이 별도 트랜잭션이라 그 사이에 지우지 않기 위해.
    ("free_accounts_unlinked_1d", "accounts",
     "created_at < now() - interval '1 day' "
     "AND NOT EXISTS (SELECT 1 FROM api_keys k WHERE k.account_id = accounts.account_id) "
     "AND NOT EXISTS (SELECT 1 FROM oauth_grants g WHERE g.account_id = accounts.account_id)"),
]
# 키에 연결된 계정은 자동 삭제하지 않는다(삭제 요청 처리 = 운영자 수동). api_keys 는 accounts 의 자식이라 보호한다 —
# 어떤 경로로든 계정 삭제가 키 행으로 번지면 안 된다(위 규칙의 NOT EXISTS 와 이중 방어).
_PROTECT = ("api_keys",)


def run_oauth_retention(conn=None, *, dry_run: bool = False) -> Dict[str, int]:
    """OAuth 표 보존 정리. 반환 = {라벨/표: 삭제(또는 dry_run 시 대상) 건수}. 실패는 삼키지 않는다."""
    from oneqaz_trading_mcp.shared.db.retention import delete_with_cascade, fk_children

    own = conn is None
    if own:
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
        conn = open_schema_connection("api")
    try:
        if not conn.execute("SELECT to_regclass('api.oauth_tokens') IS NOT NULL AS ok").fetchone()["ok"]:
            return {}
        fk_map = fk_children(conn, "api")
        out: Dict[str, int] = {}
        for label, table, where in OAUTH_RETENTION_RULES:
            stats = delete_with_cascade(conn, table=table, where_sql=f"({where})", params=(), fk_map=fk_map,
                                        protect=_PROTECT, dry_run=dry_run)
            for k, v in stats.items():
                out[f"{label}:{k}"] = out.get(f"{label}:{k}", 0) + v
        return out
    finally:
        if own:
            conn.close()


_retention_started = False


def start_oauth_retention_thread(first_delay_sec: int = 600, every_sec: int = 86400) -> bool:
    """하루 1회 정리 스레드(데몬). 표가 없거나 비어 있으면 아무것도 지우지 않는다.
    프로세스 감시·재기동 장치가 아니다(금지항목 1 무관) — 실패는 ERROR 로 남기고 다음 날 다시 시도한다."""
    global _retention_started
    if _retention_started or os.getenv("OAUTH_RETENTION_DISABLED", "0") == "1":
        return False
    _retention_started = True
    import logging

    log = logging.getLogger("MarketMCP")

    def loop():
        time.sleep(first_delay_sec)
        while True:
            try:
                stats = run_oauth_retention()
                n = sum(stats.values())
                if n:
                    log.info("[OAUTH] 보존 정리 %d건 %s", n, {k: v for k, v in stats.items() if v})
            except Exception:
                log.error("[OAUTH] 보존 정리 실패 — 다음 주기에 재시도", exc_info=True)
            time.sleep(every_sec)

    threading.Thread(target=loop, name="oauth-retention", daemon=True).start()
    return True


# ─────────────────────────────────────────────────────────────────────────────
# 메모리 구현 — 테스트 전용 (tests/validation/test_oauth_as_p2.py)
# ─────────────────────────────────────────────────────────────────────────────


class MemoryOAuthStore:
    """PgOAuthStore 와 같은 인터페이스·같은 원자성(락) — DB 없는 흐름 검증용. 운영 경로에서 쓰지 않는다."""

    def __init__(self, api_keys: Optional[Dict[str, Optional[str]]] = None):
        self._lock = threading.Lock()
        self.clients: Dict[str, ClientRecord] = {}
        self.pending: Dict[str, Dict[str, Any]] = {}
        self.accounts: Dict[str, Dict[str, Any]] = {}
        self.api_keys: Dict[str, Optional[str]] = dict(api_keys or {})  # key → account_id
        self.grants: Dict[str, Dict[str, Any]] = {}
        self.codes: Dict[str, Dict[str, Any]] = {}
        self.tokens: Dict[str, Dict[str, Any]] = {}
        self.client_touches: Dict[str, int] = {}

    def get_client(self, client_id):
        return self.clients.get(client_id)

    def upsert_client(self, rec):
        self.clients[rec.oauth_client_id] = rec

    def touch_client(self, client_id):
        self.client_touches[client_id] = self.client_touches.get(client_id, 0) + 1

    def save_pending(self, req_hash, client_id, params, ttl=PENDING_TTL_SEC):
        self.pending[req_hash] = {"client": client_id, "params": dict(params), "attempts": 0,
                                  "exp": time.time() + ttl, "consumed": False}

    def get_pending(self, req_hash):
        p = self.pending.get(req_hash)
        if not p or p["consumed"] or p["exp"] <= time.time():
            return None
        return PendingRecord(req_hash, p["client"], dict(p["params"]), p["attempts"], p["exp"])

    def bump_pending_attempts(self, req_hash, max_attempts):
        with self._lock:
            p = self.pending.get(req_hash)
            if not p:
                return max_attempts
            p["attempts"] += 1
            if p["attempts"] >= max_attempts:
                p["consumed"] = True
            return p["attempts"]

    def consume_pending(self, req_hash):
        with self._lock:
            p = self.pending.get(req_hash)
            if not p or p["consumed"] or p["exp"] <= time.time():
                return False
            p["consumed"] = True
            return True

    def account_for_key(self, api_key, plan_id):
        with self._lock:
            if api_key not in self.api_keys:
                raise LookupError("api key vanished")
            acc = self.api_keys[api_key]
            if acc is None:
                acc = new_id("acc_")
                self.accounts[acc] = {"plan_id": plan_id, "status": "active"}
                self.api_keys[api_key] = acc
            a = self.accounts[acc]
            return acc, a["plan_id"], a["status"]

    def create_free_account(self):
        with self._lock:
            acc = new_id("acc_")
            self.accounts[acc] = {"plan_id": "free", "status": "active"}
            return acc, "free", "active"

    def create_grant(self, account_id, client_id, scopes, resource):
        gid = new_id("grt_")
        self.grants[gid] = {"account": account_id, "client": client_id, "scopes": list(scopes),
                            "resource": resource, "revoked": False}
        return gid

    def save_code(self, code_hash, grant_id, redirect_uri, explicit, code_challenge, ttl=CODE_TTL_SEC):
        self.codes[code_hash] = {"grant": grant_id, "redirect_uri": redirect_uri, "explicit": bool(explicit),
                                 "challenge": code_challenge, "exp": time.time() + ttl, "used": False}

    def load_code(self, code_hash):
        c = self.codes.get(code_hash)
        if not c:
            return None
        g = self.grants[c["grant"]]
        return CodeRecord(code_hash, c["grant"], g["client"], g["account"], list(g["scopes"]), g["resource"],
                          c["redirect_uri"], c["explicit"], c["challenge"], c["exp"], c["used"], g["revoked"])

    def consume_code(self, code_hash):
        with self._lock:
            c = self.codes.get(code_hash)
            if not c or c["used"]:
                return False
            c["used"] = True
            return True

    def issue_tokens(self, grant_id, family_id, access_hash, refresh_hash, access_ttl=ACCESS_TTL_SEC,
                     refresh_ttl=REFRESH_TTL_SEC):
        now = time.time()
        self.tokens[access_hash] = {"kind": "access", "grant": grant_id, "family": family_id,
                                    "exp": now + access_ttl, "revoked": False}
        if refresh_hash:
            self.tokens[refresh_hash] = {"kind": "refresh", "grant": grant_id, "family": family_id,
                                         "exp": now + refresh_ttl, "revoked": False}

    def load_token(self, token_hash, kind):
        t = self.tokens.get(token_hash)
        if not t or t["kind"] != kind:
            return None
        g = self.grants[t["grant"]]
        a = self.accounts[g["account"]]
        return TokenRecord(token_hash, kind, t["grant"], t["family"], g["client"], g["account"], a["status"],
                           a["plan_id"], list(g["scopes"]), g["resource"], t["exp"], t["revoked"], g["revoked"])

    def revoke_token_if_active(self, token_hash):
        with self._lock:
            t = self.tokens.get(token_hash)
            if not t or t["revoked"]:
                return False
            t["revoked"] = True
            return True

    def revoke_family(self, family_id):
        n = 0
        with self._lock:
            for t in self.tokens.values():
                if t["family"] == family_id and not t["revoked"]:
                    t["revoked"] = True
                    n += 1
        return n

    def revoke_grant(self, grant_id):
        with self._lock:
            self.grants[grant_id]["revoked"] = True
            for t in self.tokens.values():
                if t["grant"] == grant_id:
                    t["revoked"] = True
