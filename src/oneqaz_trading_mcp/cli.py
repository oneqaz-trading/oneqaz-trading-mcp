# -*- coding: utf-8 -*-
"""CLI entry point for oneqaz-trading-mcp."""

from __future__ import annotations

import argparse
import sys


def main():
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
    sub.add_parser("init", help="Initialize database schema and sample data")

    # check
    sub.add_parser("check", help="Check data path configuration")

    args = parser.parse_args()

    if args.command == "serve":
        import os
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
        from oneqaz_trading_mcp.init_db import init_databases
        init_databases()

    elif args.command == "check":
        from oneqaz_trading_mcp.config import check_paths
        check_paths()

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
