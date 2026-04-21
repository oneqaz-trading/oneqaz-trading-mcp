# -*- coding: utf-8 -*-
"""Package entrypoint: ``python -m oneqaz_trading_mcp`` → CLI.

PyPI 에서 설치한 사용자가 ``python -m oneqaz_trading_mcp --help`` 만으로
바로 CLI 를 쓸 수 있게 하는 얇은 shim. ``[project.scripts]`` 로 등록된
``oneqaz-trading-mcp`` 바이너리와 동일한 ``cli.main`` 을 호출한다.
"""
from __future__ import annotations

from .cli import main

if __name__ == "__main__":
    main()
