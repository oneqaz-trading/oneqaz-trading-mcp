#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Market MCP Server 실행 진입점
=============================

실행 방법:
    # Docker 환경 (auto_trader 컨테이너)
    docker exec -it auto_trader bash
    cd /workspace
    python mcps/run_mcp.py

    # 또는 모듈로 실행
    python -m mcps.run_mcp

    # 백그라운드 실행
    nohup python mcps/run_mcp.py > /tmp/mcp_server.log 2>&1 &

환경변수:
    MCP_SERVER_PORT: 서버 포트 (기본: 8010)
    MCP_SERVER_HOST: 바인드 호스트 (기본: 0.0.0.0)
    MCP_LOG_LEVEL: 로그 레벨 (기본: INFO)
"""

from __future__ import annotations

import os
import sys

# Windows cp949 환경에서 이모지 print 크래시 방지
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# 프로젝트 루트를 PYTHONPATH에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

def main():
    """서버 실행"""
    print("=" * 60)
    print("🚀 Market MCP Server Starting...")
    print("=" * 60)

    # 설정 확인
    from oneqaz_trading_mcp.config import check_paths, MCP_SERVER_HOST, MCP_SERVER_PORT
    check_paths()

    print()
    print(f"📡 Server: http://{MCP_SERVER_HOST}:{MCP_SERVER_PORT}")
    print(f"📖 Docs:   http://localhost:{MCP_SERVER_PORT}/docs")
    print()
    print("=" * 60)

    # 서버 실행
    from oneqaz_trading_mcp.server import run_server
    run_server()

if __name__ == "__main__":
    main()
