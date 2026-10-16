# -*- coding: utf-8 -*-
"""CLI entry point for oneqaz-trading-mcp."""

from __future__ import annotations

import argparse
import sys


def _console_safe() -> None:
    """Windows consoles (cp949/cp1252) cannot encode the emoji in server logs.

    Without this, every such log line prints a ``--- Logging error ---`` traceback.
    Unencodable characters are replaced instead; UTF-8 consoles are unaffected.
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="replace")
        except (AttributeError, ValueError):
            pass  # not a reconfigurable text stream (e.g. replaced by a test harness)


def main():
    _console_safe()
    parser = argparse.ArgumentParser(
        prog="oneqaz-trading-mcp",
        description="MCP server for trading signal analysis and market monitoring",
    )
    sub = parser.add_subparsers(dest="command")

    # serve
    serve_parser = sub.add_parser("serve", help="Start the MCP server")
    serve_parser.add_argument("--port", type=int, default=None, help="Server port (default: 8010)")
    serve_parser.add_argument("--host", type=str, default=None, help="Bind host (default: 0.0.0.0)")

    # init
    sub.add_parser("init", help="Deprecated (SQLite demo backend retired in 0.4.0)")

    # check
    sub.add_parser("check", help="Check data path configuration")

    args = parser.parse_args()

    if args.command == "serve":
        import os
        # PostgreSQL is the only supported backend; the mirrored config treats an unset
        # DB_BACKEND as the retired SQLite backend and refuses every query.
        os.environ.setdefault("DB_BACKEND", "postgres")
        if args.port:
            os.environ["MCP_SERVER_PORT"] = str(args.port)
        if args.host:
            os.environ["MCP_SERVER_HOST"] = args.host

        print("=" * 60)
        print("  oneqaz-trading-mcp starting...")
        print("=" * 60)

        from oneqaz_trading_mcp.config import check_paths, MCP_SERVER_HOST, MCP_SERVER_PORT
        check_paths()
        print()
        print(f"  Server: http://{args.host or MCP_SERVER_HOST}:{args.port or MCP_SERVER_PORT}")
        print(f"  Docs:   http://localhost:{args.port or MCP_SERVER_PORT}/docs")
        print()
        print("=" * 60)

        from oneqaz_trading_mcp.server import run_server
        run_server()

    elif args.command == "init":
        print(
            "DEPRECATED (0.4.0): the SQLite demo backend was retired when this\n"
            "package switched to mirroring the production server code, which is\n"
            "PostgreSQL-only. Self-hosting now requires a OneQAZ-compatible\n"
            "PostgreSQL (see README > Self-hosting). For evaluation, use the\n"
            "hosted endpoint instead: https://api.oneqaz.com/mcp"
        )
        sys.exit(1)

    elif args.command == "check":
        from oneqaz_trading_mcp.config import check_paths
        check_paths()

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
