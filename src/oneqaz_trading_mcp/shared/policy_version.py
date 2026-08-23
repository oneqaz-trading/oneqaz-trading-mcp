# -*- coding: utf-8 -*-
"""정책 버전 (policy_version) — 자가학습 시스템의 성과 코호트 식별자.

[2026-07-04 도입 배경]
코드/학습 파라미터가 바뀌어도 성과 시계열에 구분이 없어 "이 수리가 실제로
성과를 개선했는가"에 답할 수 없었다. 매매 기록(virtual_trade_history /
virtual_trade_feedback / decision_fingerprint)에 이 버전을 태깅하면
정책 버전별 코호트 성과 비교가 가능해진다 (policy_cohort_performance 뷰).

운영 규칙:
- 버전 원천 = 레포 루트의 ``POLICY_VERSION`` 파일 첫 줄 첫 토큰.
- **의미 있는 변경(재시작 배치, 학습 산식 변경, 임계값 정책 변경) 때 수동으로
  올린다.** 형식 권장: ``YYYY-MM-DD-rN`` (예: 2026-07-04-r1). 뒤에 설명 자유.
- 환경변수 ``POLICY_VERSION`` 이 있으면 파일보다 우선 (테스트/실험용).
- 파일이 없거나 읽기 실패 시 'unversioned' — 태깅은 계속되므로 기록이 죽지 않는다.

프로세스 시작 시 1회 캐시 — 코호트 안정성을 위해 런타임 중 재읽기하지 않는다
(파일을 올려도 재시작 전까지는 이전 버전으로 태깅되는 것이 의도된 동작:
버전 경계 = 프로세스 경계).
"""
from __future__ import annotations

import os

_CACHED: str | None = None

_VERSION_FILE_CANDIDATES = (
    '/workspace/POLICY_VERSION',
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'POLICY_VERSION'),
)


def get_policy_version() -> str:
    """현재 프로세스의 정책 버전 문자열 (프로세스 수명 동안 불변)."""
    global _CACHED
    if _CACHED is not None:
        return _CACHED

    env = (os.environ.get('POLICY_VERSION') or '').strip()
    if env:
        _CACHED = env[:64]
        return _CACHED

    for path in _VERSION_FILE_CANDIDATES:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
            if first_line:
                _CACHED = first_line.split()[0][:64]
                return _CACHED
        except Exception:
            continue

    _CACHED = 'unversioned'
    return _CACHED
