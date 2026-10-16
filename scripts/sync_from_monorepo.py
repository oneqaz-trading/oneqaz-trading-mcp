# -*- coding: utf-8 -*-
"""Sync this public package from the OneQAZ monorepo (source of truth).

This repository mirrors the monorepo's ``mcps/`` module — the code serving
https://api.oneqaz.com/mcp — plus a small vendored subset of monorepo helpers
so the package imports cleanly outside the monorepo. Curated rewrites (English
translation, SQLite demo backend) were retired in 0.4.0: they made every sync a
manual porting project, which is how this repo went stale between 2026-04 and
2026-08 (and again between 0.4.0 and 0.5.0, when the old tier-patch anchor
stopped matching and this script could not run).

Usage (from the public repo root, monorepo checked out at C:\\auto_trader):

    python scripts/sync_from_monorepo.py            # sync + checks + report
    ONEQAZ_MONOREPO=/path/to/auto_trader python scripts/sync_from_monorepo.py

What it does:
  1. Mirrors ``mcps/*.py`` + ``mcps/tools/`` + ``mcps/resources/`` +
     ``mcps/entity/`` into ``src/oneqaz_trading_mcp/`` (non-Python files are
     never copied). Top-level public-only files (``PRESERVE``) are kept, and
     mirrored files that no longer exist in the monorepo are removed.
  2. Vendors the monorepo helpers the mirrored code hard-requires (``VENDOR``).
     Everything else the code imports lazily behind try/except guards
     (agent_history RAG, trade.core.market, market.global_regime.*,
     external_context.core.db_utils) is *not* vendored — those call paths
     degrade with explicit errors outside the monorepo.
  3. Rewrites import prefixes so the package is self-contained
     (``mcps.`` / ``shared.`` / vendored paths → ``oneqaz_trading_mcp.``).
     The hosted API-key store (``api.marketplace.key_store``) is rewritten to
     the public-only ``self_host_auth`` module, which implements the
     ``MCP_TIER_RESOLVER`` hook for self-hosters. (Until 0.4.x this was a text
     patch on ``server.py``; the anchor broke when the hosted server started
     rejecting unknown keys, so it is now an import rewrite with no anchor.)
  4. Applies ``PUBLIC_PATCHES`` (regex substitutions that must match, or the
     sync fails): today only the operator's self-traffic IP default in
     ``analytics.py``, which is hosted-ledger configuration and must not ship.
  5. Runs fail-loud checks: no import may still point at a monorepo-only
     module the rewrites should have handled, no credential-shaped literal and
     no public IPv4 literal may be published (0.4.0 shipped a hard-coded
     PostgreSQL password default and the operator's IP). Softer findings
     (private addresses, host paths) are printed for review.
"""

from __future__ import annotations

import ipaddress
import os
import re
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PKG = REPO_ROOT / "src" / "oneqaz_trading_mcp"
MONOREPO = Path(os.environ.get("ONEQAZ_MONOREPO", r"C:\auto_trader"))

# 공개 패키지 전용 파일 — 미러가 절대 덮어쓰거나 지우지 않는다 (최상위 기준).
PRESERVE = {"__init__.py", "__main__.py", "cli.py", "init_db.py", "self_host_auth.py"}

# 미러 대상 mcps 하위 디렉터리 ("" = mcps/ 최상위). 하위 디렉터리의 __init__.py 는 미러한다.
MIRROR_DIRS = ("", "tools", "resources", "entity")

# 벤더링: (모노레포 상대경로, 패키지 상대경로)
VENDOR = [
    ("shared/db/compat.py", "shared/db/compat.py"),
    ("shared/db/pg_pool.py", "shared/db/pg_pool.py"),
    ("shared/db/config.py", "shared/db/config.py"),
    ("shared/db/retention.py", "shared/db/retention.py"),  # oauth_store 보존 정리
    ("shared/policy_version.py", "shared/policy_version.py"),
    ("data_collection/core/intervals.py", "data_collection/core/intervals.py"),
    ("api/insight/schema.py", "api/insight/schema.py"),
    # resources/external_context.fetch_scheduled_events (무가드 import)
    ("external_context/core/news_scope.py", "external_context/core/news_scope.py"),
    ("external_context/core/keyword_match.py", "external_context/core/keyword_match.py"),
    # tools/trust_layer.governance_frozen_types (무가드 import)
    ("agent_history/core/feature_promoter.py", "agent_history/core/feature_promoter.py"),
]

# 벤더 네임스페이스 __init__.py (빈 파일 — 모노레포 __init__ 은 무겁다)
VENDOR_NAMESPACES = (
    "shared",
    "shared/db",
    "data_collection",
    "data_collection/core",
    "api",
    "api/insight",
    "external_context",
    "external_context/core",
    "agent_history",
    "agent_history/core",
)

# import 접두어 재작성 (문자열 치환 — from/import 문만 노린 보수적 패턴)
REWRITES = [
    ("from mcps.", "from oneqaz_trading_mcp."),
    ("from mcps import", "from oneqaz_trading_mcp import"),
    ("import mcps.", "import oneqaz_trading_mcp."),
    ("from shared.", "from oneqaz_trading_mcp.shared."),
    (
        "from data_collection.core.intervals",
        "from oneqaz_trading_mcp.data_collection.core.intervals",
    ),
    ("from api.insight.schema", "from oneqaz_trading_mcp.api.insight.schema"),
    (
        "from external_context.core.news_scope",
        "from oneqaz_trading_mcp.external_context.core.news_scope",
    ),
    (
        "from external_context.core.keyword_match",
        "from oneqaz_trading_mcp.external_context.core.keyword_match",
    ),
    (
        "from agent_history.core.feature_promoter",
        "from oneqaz_trading_mcp.agent_history.core.feature_promoter",
    ),
    # 공개 패치: 호스팅 키 저장소 → 자체 호스팅 해석기 (MCP_TIER_RESOLVER)
    ("from api.marketplace.key_store", "from oneqaz_trading_mcp.self_host_auth"),
]

# 동기화 뒤 남아 있으면 안 되는 import (재작성 누락 = 공개 패키지에서 ImportError)
FORBIDDEN_IMPORTS = re.compile(
    r"^\s*(from|import)\s+(mcps\b|api\.marketplace\b|shared\.|"
    r"external_context\.core\.(news_scope|keyword_match)\b|agent_history\.core\.feature_promoter\b)",
    re.M,
)

# 공개 패치: (패키지 상대경로, 정규식, 대체) — 동기화 뒤 적용. 정규식이 안 맞으면 실패한다(드리프트 = 사람 검토).
#   운영자 자체 트래픽 IP 기본값은 호스팅 원장 전용이다. 공개본에 실으면 운영자 회선 주소가 공개되고,
#   자체 호스팅 쪽에는 쓸모도 없다(남의 회선을 자기 원장에서 빼게 된다) → 공개본 기본값은 빈 값.
PUBLIC_PATCHES = [
    (
        "analytics.py",
        re.compile(r"""getenv\(\s*"MCP_SELF_TRAFFIC_IPS"\s*,\s*"[^"]*"\s*\)"""),
        'getenv("MCP_SELF_TRAFFIC_IPS", "")',
    ),
]

# 공개 금지 — 자격증명 모양의 리터럴 (발견 시 실패)
SECRET_PATTERNS = [
    ("private key block", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("URI with inline password", re.compile(r"\b(postgres(ql)?|mysql|redis|amqp)://[^\s:/@'\"]+:[^\s@'\"]+@")),
    (
        "secret-named env var with a non-empty default",
        re.compile(r"getenv\(\s*['\"]\w*(PASSWORD|PASSWD|SECRET|TOKEN)\w*['\"]\s*,\s*['\"][^'\"]+['\"]"),
    ),
    (
        "password-named field with a non-empty literal",
        re.compile(r"\b\w*(password|passwd)\w*\s*(:\s*[\w\[\], ]+)?\s*=\s*['\"][^'\"]+['\"]", re.I),
    ),
    ("token-shaped literal", re.compile(r"\b(oqzk|oqzat|oqzrt|ghp|gho|github_pat|sk)_[A-Za-z0-9]{20,}")),
]

# IPv4 리터럴 — 공인 주소는 실패(운영자 회선 등), 사설 주소는 검토 출력
IPV4_LITERAL = re.compile(r"(?<![\d.])(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(?![\d.])")

# 검토용 출력만 (실패 아님)
REVIEW_PATTERNS = [
    ("host path", re.compile(r"\b[A-Z]:\\\\?(auto_trader|cloudflared|Users)\b")),
]


def rewrite(text: str) -> str:
    for old, new in REWRITES:
        text = text.replace(old, new)
    return text


def copy_py(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(rewrite(src.read_text(encoding="utf-8")), encoding="utf-8")


def _check(pkg: Path) -> int:
    """동기화 결과 검사. 실패 건수를 돌려준다 (0 = 통과)."""
    failures = 0
    if not (pkg / "self_host_auth.py").is_file():
        print("ERROR: src/oneqaz_trading_mcp/self_host_auth.py is missing - the key-store rewrite needs it.")
        failures += 1
    for f in sorted(pkg.rglob("*.py")):
        if "__pycache__" in f.parts:
            continue
        rel = f.relative_to(pkg)
        text = f.read_text(encoding="utf-8")
        for m in FORBIDDEN_IMPORTS.finditer(text):
            line = text.count("\n", 0, m.start()) + 1
            print(f"ERROR: {rel}:{line}: unresolved monorepo import: {m.group(0).strip()}")
            failures += 1
        for label, pat in SECRET_PATTERNS:
            for m in pat.finditer(text):
                line = text.count("\n", 0, m.start()) + 1
                print(f"ERROR: {rel}:{line}: {label}: {m.group(0)[:80]}")
                failures += 1
        for m in IPV4_LITERAL.finditer(text):
            try:
                addr = ipaddress.ip_address(m.group(1))
            except ValueError:
                continue  # 버전 문자열 등 (예: 1.2.300.4)
            line = text.count("\n", 0, m.start()) + 1
            if addr.is_global:
                print(f"ERROR: {rel}:{line}: public IPv4 literal: {addr} - add a PUBLIC_PATCHES rule or remove it upstream")
                failures += 1
            elif addr.is_private and not addr.is_loopback:
                print(f"review: {rel}:{line}: private IPv4 literal: {addr}")
        for label, pat in REVIEW_PATTERNS:
            for m in pat.finditer(text):
                line = text.count("\n", 0, m.start()) + 1
                print(f"review: {rel}:{line}: {label}: {m.group(0)}")
    return failures


def main() -> int:
    mcps = MONOREPO / "mcps"
    if not mcps.is_dir():
        print(f"ERROR: monorepo mcps/ not found at {mcps} (set ONEQAZ_MONOREPO)")
        return 1

    copied = 0
    removed = []
    # 1) mcps/ 미러 + 모노레포에서 사라진 미러 파일 제거
    for sub in MIRROR_DIRS:
        src_dir = mcps / sub if sub else mcps
        dst_dir = PKG / sub if sub else PKG
        names = set()
        for f in sorted(src_dir.glob("*.py")):
            if not sub and f.name in PRESERVE:
                continue
            copy_py(f, dst_dir / f.name)
            names.add(f.name)
            copied += 1
        for f in sorted(dst_dir.glob("*.py")) if dst_dir.is_dir() else []:
            if f.name in names or (not sub and f.name in PRESERVE):
                continue
            f.unlink()
            removed.append(str(f.relative_to(PKG)))

    # 2) 벤더링
    for rel_src, rel_dst in VENDOR:
        src = MONOREPO / rel_src
        if not src.is_file():
            print(f"ERROR: vendor source missing: {src}")
            return 1
        copy_py(src, PKG / rel_dst)
        copied += 1
    for d in VENDOR_NAMESPACES:
        init = PKG / d / "__init__.py"
        init.parent.mkdir(parents=True, exist_ok=True)
        if not init.exists():
            init.write_text("", encoding="utf-8")

    # 3) 공개 패치 (fail-loud)
    for rel, pat, new in PUBLIC_PATCHES:
        f = PKG / rel
        text = f.read_text(encoding="utf-8")
        patched, n = pat.subn(new, text)
        if n == 0:
            print(f"ERROR: public patch did not match in {rel} ({pat.pattern}) - monorepo drifted; review the patch.")
            return 1
        f.write_text(patched, encoding="utf-8")
        print(f"public patch: {rel} ({n} replacement)")

    # 4) 스테일 캐시 제거
    for pyc in PKG.rglob("__pycache__"):
        shutil.rmtree(pyc, ignore_errors=True)

    print(f"synced {copied} files from {MONOREPO} -> {PKG}")
    for r in removed:
        print(f"removed (gone from monorepo): {r}")

    # 5) 검사 (fail-loud)
    failures = _check(PKG)
    if failures:
        print(f"ERROR: {failures} check failure(s) - do not commit or release this sync.")
        return 1
    print("checks: ok")
    print("next: bump version in src/oneqaz_trading_mcp/__init__.py + "
          "pyproject.toml + server.json, update README/CHANGELOG, run smoke test")
    return 0


if __name__ == "__main__":
    sys.exit(main())
