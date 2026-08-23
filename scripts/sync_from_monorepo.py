# -*- coding: utf-8 -*-
"""Sync this public package from the OneQAZ monorepo (source of truth).

As of 0.4.0 this repository is a **faithful mirror** of the monorepo's
``mcps/`` module — the exact code serving https://api.oneqaz.com/mcp — plus a
small vendored subset of ``shared/`` helpers so the package imports cleanly
outside the monorepo. Curated rewrites (English translation, SQLite demo
backend) were retired in 0.4.0: they made every sync a manual porting project,
which is how this repo went stale between 2026-04 and 2026-08.

Usage (from the public repo root, monorepo checked out at C:\\auto_trader):

    python scripts/sync_from_monorepo.py            # sync + report
    ONEQAZ_MONOREPO=/path/to/auto_trader python scripts/sync_from_monorepo.py

What it does:
  1. Mirrors ``mcps/*.py`` + ``mcps/tools/`` + ``mcps/resources/`` into
     ``src/oneqaz_trading_mcp/`` (excluding ``__init__.py``, which keeps the
     package version, and non-Python files).
  2. Vendors the minimal monorepo helpers the mirrored code hard-requires:
     ``shared/db/{compat,pg_pool,config}.py``, ``shared/policy_version.py``,
     ``data_collection/core/intervals.py``, ``api/insight/schema.py``.
     Everything else the code imports lazily behind try/except guards
     (agent_history RAG, trade.core.market, market.global_regime.profiles,
     external_context.core.db_utils, api.marketplace.key_store) is *not*
     vendored — those call paths degrade gracefully outside the monorepo.
  3. Rewrites import prefixes so the package is self-contained
     (``mcps.`` / ``shared.`` / vendored paths → ``oneqaz_trading_mcp.``).
  4. Re-applies the public-only patch: the tier resolver in ``server.py``
     gains the ``MCP_TIER_RESOLVER`` env hook for self-hosters.
     If the patch anchor is not found the script FAILS LOUDLY — that means
     the monorepo code drifted and the patch needs manual review.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PKG = REPO_ROOT / "src" / "oneqaz_trading_mcp"
MONOREPO = Path(os.environ.get("ONEQAZ_MONOREPO", r"C:\auto_trader"))

# 공개 패키지 전용 파일 — 미러가 절대 덮어쓰지 않는다.
PRESERVE = {"__init__.py", "__main__.py", "cli.py", "init_db.py"}

# 벤더링: (모노레포 상대경로, 패키지 상대경로)
VENDOR = [
    ("shared/db/compat.py", "shared/db/compat.py"),
    ("shared/db/pg_pool.py", "shared/db/pg_pool.py"),
    ("shared/db/config.py", "shared/db/config.py"),
    ("shared/policy_version.py", "shared/policy_version.py"),
    ("data_collection/core/intervals.py", "data_collection/core/intervals.py"),
    ("api/insight/schema.py", "api/insight/schema.py"),
]

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
]

# server.py 공개 패치: hosted key_store 다음에 MCP_TIER_RESOLVER env 훅 삽입.
TIER_ANCHOR = """\
                try:
                    from api.marketplace.key_store import get_tier_for_key
                    tier = get_tier_for_key(api_key)
                except Exception:
                    pass  # Fall back to free tier if key_store unavailable
"""
TIER_PATCH = """\
                try:
                    from api.marketplace.key_store import get_tier_for_key
                    tier = get_tier_for_key(api_key)
                except Exception:
                    pass  # Fall back to free tier if key_store unavailable
                # [public package] self-hosters plug their own resolver via
                # MCP_TIER_RESOLVER="module:function" (returns tier for a key).
                if tier == "free":
                    resolver_path = os.environ.get("MCP_TIER_RESOLVER", "").strip()
                    if resolver_path:
                        try:
                            mod_name, fn_name = resolver_path.rsplit(":", 1)
                            import importlib
                            mod = importlib.import_module(mod_name)
                            tier = getattr(mod, fn_name)(api_key) or "free"
                        except Exception:
                            tier = "free"
"""


def rewrite(text: str) -> str:
    for old, new in REWRITES:
        text = text.replace(old, new)
    return text


def copy_py(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(rewrite(src.read_text(encoding="utf-8")), encoding="utf-8")


def main() -> int:
    mcps = MONOREPO / "mcps"
    if not mcps.is_dir():
        print(f"ERROR: monorepo mcps/ not found at {mcps} (set ONEQAZ_MONOREPO)")
        return 1

    copied = 0
    # 1) mcps/ 미러 (top-level + tools/ + resources/)
    for sub in ("", "tools", "resources"):
        src_dir = mcps / sub if sub else mcps
        dst_dir = PKG / sub if sub else PKG
        for f in sorted(src_dir.glob("*.py")):
            if not sub and f.name in PRESERVE:
                continue
            if sub and f.name in PRESERVE and f.name != "__init__.py":
                continue
            copy_py(f, dst_dir / f.name)
            copied += 1

    # 2) 벤더링
    for rel_src, rel_dst in VENDOR:
        src = MONOREPO / rel_src
        if not src.is_file():
            print(f"ERROR: vendor source missing: {src}")
            return 1
        copy_py(src, PKG / rel_dst)
        copied += 1
    # 벤더 네임스페이스 __init__.py (빈 파일 — 모노레포 __init__ 은 무겁다)
    for d in (
        "shared",
        "shared/db",
        "data_collection",
        "data_collection/core",
        "api",
        "api/insight",
    ):
        init = PKG / d / "__init__.py"
        init.parent.mkdir(parents=True, exist_ok=True)
        if not init.exists():
            init.write_text("", encoding="utf-8")

    # 3) server.py 공개 패치 (fail-loud)
    server = PKG / "server.py"
    text = server.read_text(encoding="utf-8")
    if TIER_PATCH.splitlines()[6].strip() in text:
        pass  # 이미 패치됨 (재실행 멱등)
    elif TIER_ANCHOR in text:
        server.write_text(text.replace(TIER_ANCHOR, TIER_PATCH, 1), encoding="utf-8")
    else:
        print("ERROR: tier-resolver patch anchor not found in server.py — "
              "monorepo drifted; update TIER_ANCHOR in this script.")
        return 1

    # 4) 스테일 캐시 제거
    for pyc in PKG.rglob("__pycache__"):
        shutil.rmtree(pyc, ignore_errors=True)

    print(f"synced {copied} files from {MONOREPO} → {PKG}")
    print("next: bump version in src/oneqaz_trading_mcp/__init__.py + "
          "pyproject.toml + server.json, update README/CHANGELOG, run smoke test")
    return 0


if __name__ == "__main__":
    sys.exit(main())
