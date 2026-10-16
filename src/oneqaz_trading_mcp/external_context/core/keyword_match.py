# -*- coding: utf-8 -*-
"""
키워드 일치 — 영문은 단어 경계, 한글 등 비ASCII 는 부분 문자열 (2026-10-13)
==========================================================================
뉴스 분류기(카테고리·impact·시장 범위)가 공유하는 단일 규칙.

왜: 종전엔 전부 `term in text` 부분 문자열이었다. 영문에서는 오탐이 컸다 —
    실측(2026-10-13, 최근 7일 영어 impact=high 2,748건): 'war' 가 다른 단어의 일부
    (warning·Edwards·warming·thwart)로만 들어 있는 행 449건, 'fed' 가 같은 식인 행 224건.
    그 단어 하나로 rates/geopolitics 카테고리 → impact high 가 됐다.

규칙:
  - ASCII 키워드: 앞뒤가 영숫자가 아니어야 한다(단어 경계). 복수형 접미 s/es 는 허용
    ("tariff" ⊂ "tariffs", "war" ⊂ "wars", "fed" ⊂ "Fed's"), 그 밖의 접미는 불허
    ("warning"·"fedex"·"europe" 는 각각 war·fed·euro 가 아니다).
  - 비ASCII(한글) 키워드: 부분 문자열 — 한국어는 조사·어미가 붙어("금리가", "연준의")
    단어 경계가 성립하지 않는다.
  - 대소문자 무시(호출부가 소문자화하지 않아도 된다).
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Iterable, List


def _is_ascii(term: str) -> bool:
    return all(ord(c) < 128 for c in term)


@lru_cache(maxsize=4096)
def _pattern(term: str) -> "re.Pattern[str]":
    return re.compile(r"(?<![a-z0-9])" + re.escape(term) + r"(?:s|es)?(?![a-z0-9])")


def contains_term(text: str, term: str) -> bool:
    """text 에 term 이 (영문은 단어로, 한글은 부분 문자열로) 들어 있는가."""
    t = (term or "").strip().lower()
    if not t:
        return False
    s = (text or "").lower()
    if _is_ascii(t):
        return _pattern(t).search(s) is not None
    return t in s


def any_term(text: str, terms: Iterable[str]) -> bool:
    s = (text or "").lower()
    return any(contains_term(s, t) for t in terms)


def matched_terms(text: str, terms: Iterable[str]) -> List[str]:
    s = (text or "").lower()
    return [t for t in terms if contains_term(s, t)]
