# -*- coding: utf-8 -*-
"""
뉴스 → 시장 범위 (2026-10-13, Mac 요청서 '엔진 산출물 검증 rev.2' H7#2 + impact_level 포화 2단계)
=========================================================================================
`external_context.news_events.target_markets` 를 정하는 **단일 정의**. 생산자 어휘(수집기의 source
태그·행 id 형식)를 아는 쪽이 여기다 — 소비자(MCP·LLM)는 저장된 target_markets 만 읽는다.

왜 새로 만들었나 (종전 `collection_runner._distribute_news_to_markets` 의 3중 결함):
  1. **분배 결과가 사라졌다** — SQLite 시절 "시장별 DB 에 복사" 방식 그대로라 PG 에서는 같은
     테이블·같은 id 를 3번 upsert 하며 symbol 만 'GLOBAL' 로 덮었고, 어느 시장인지는 저장되지
     않았다. ⇒ MCP 시장 요약 3종이 같은 전역 최신 30건을 보여줬다(미장 요약 1위 = 국내 '5만원권' 기사).
  2. **'impact=high 면 전 시장' 규칙** — impact_level 은 실측 영향도가 아니라 매크로 주제 키워드
     일치라(GLOBAL 의 약 90% 가 high) 사실상 "매크로 피드 전부 전 시장" 이었다.
  3. **분배 사전 자체의 누수** — 미장 키워드에 '금리'·'인플레이션'·'cpi' 같은 일반 매크로어,
     카테고리 rates/general_macro → 미장·국장 일괄. 국내 금리 기사가 미장으로 갔다.
     게다가 덮어쓰기가 DART·매크로 종목 행의 symbol(005930, CL=F)까지 'GLOBAL' 로 바꿨다.

규칙 (출처가 1차, 키워드는 **추가만**):
  ① 알려진 source → 고정 범위 (_SOURCE_SCOPE). 영어 매크로 RSS·키워드 검색 = 전역 매크로(3시장).
  ② macro_news(yfinance 매크로 종목 뉴스)는 source 가 매체명(Reuters 등 120여 종)이라 출처로
     못 가른다 → 행 id 접두사 `{category}:{symbol}:…` 로 식별 → 전역 매크로.
  ③ 6자리 숫자 종목코드 행(DART·KRX 수급) → kr_market.
  ④ 본문에 특정 자산·시장이 **명시**되면 그 시장을 추가 (비트코인 → coin, 나스닥 → us, 코스피 → kr).
     일반 매크로어(금리·인플레이션·CPI)는 시장을 정하지 않는다 — 출처가 정한다.
  ⑤ 글로벌 통화정책 주체(연준·FOMC·ECB·BOJ·OPEC)가 명시되면 3시장 전부.
  ⑥ 어느 것에도 안 걸린 미등재 source → 빈 범위 + 출처별 1회 경고(조용한 기본값 금지 — 패키지
     경계 문자열 계약, CLAUDE.md 조용한 사망 패턴 ⑤). 새 수집기를 붙이면 _SOURCE_SCOPE 에 등재할 것.

시장 어휘 = external_context 관례 `coin_market` / `kr_market` / `us_market`
(news_causality_analysis.market_id 와 같다. MCP 는 mcps/resources/external_context.py::_norm_market_id).
"""

from __future__ import annotations

import logging
import re
import threading
from typing import Dict, Optional, Sequence, Tuple

from oneqaz_trading_mcp.external_context.core.keyword_match import any_term

logger = logging.getLogger(__name__)

MARKETS: Tuple[str, ...] = ("coin_market", "kr_market", "us_market")
_ALL = MARKETS

# ① 수집기 source 태그 → 범위. 근거 = 수집기 설정의 그룹 주석
#   (external_context/core/news_feed.py 영어 매크로 RSS·영어 키워드 그룹·크립토 RSS·한국 금융 언론,
#    collectors/research_news_collector.py, collectors/disclosure_collector.py, investor_flow_collector.py)
_SOURCE_SCOPE: Dict[str, Tuple[str, ...]] = {
    # 전역 매크로 — Fed·CPI·금리·환율·원자재·고용·지정학 키워드로 검색한 영어 피드
    "google_rss_en_kw": _ALL,
    "google_rss_en_macro": _ALL,
    "google_rss_en_econ": _ALL,
    "google_rss_en": _ALL,
    "cnbc_economy": _ALL,
    # 미국 시장 리서치
    "bloomberg": ("us_market",),
    "seeking_alpha": ("us_market",),
    "marketwatch": ("us_market",),
    "wsj": ("us_market",),
    # 한국 — 국내 매크로 검색·금융 언론·공시·수급
    "google_rss_ko": ("kr_market",),
    "hankyung": ("kr_market",),
    "maeil_business": ("kr_market",),
    "dart": ("kr_market",),
    "krx_investor_flow": ("kr_market",),
    # 크립토 RSS
    "coindesk": ("coin_market",),
    "cointelegraph": ("coin_market",),
    "theblock": ("coin_market",),
}

# ② macro_news 행 id 접두사 (macro_news._stable_news_id = f"{category}:{symbol}:{digest}")
_MACRO_ID_PREFIX = re.compile(
    r"^(bonds|commodities|forex|vix|credit|liquidity|inflation|energy):", re.IGNORECASE
)
# ③ KRX 종목코드
_KR_TICKER = re.compile(r"^\d{6}$")

# ④ 특정 자산·시장 명시 → 그 시장 추가. 일반 매크로어는 넣지 않는다(위 3번 결함).
_ASSET_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "coin_market": (
        "bitcoin", "btc", "ethereum", "ether", "crypto", "cryptocurrency", "blockchain",
        "binance", "coinbase", "defi", "stablecoin", "altcoin", "xrp", "solana", "dogecoin",
        "비트코인", "이더리움", "암호화폐", "가상화폐", "가상자산", "블록체인", "스테이블코인",
        "업비트", "빗썸", "바이낸스", "알트코인",
    ),
    "kr_market": (
        "kospi", "kosdaq", "korea exchange", "samsung", "sk hynix", "hyundai", "posco",
        "kakao", "naver", "lg electronics",
        "코스피", "코스닥", "한국 증시", "국내 증시", "유가증권시장", "삼성전자", "sk하이닉스",
        "현대차", "카카오", "네이버", "한국은행", "금통위",
    ),
    "us_market": (
        "s&p 500", "s&p500", "nasdaq", "dow jones", "wall street", "nyse", "russell 2000",
        "apple", "microsoft", "amazon", "tesla", "nvidia", "alphabet", "google", "meta platforms",
        "나스닥", "뉴욕증시", "미국 증시", "미 증시", "다우지수",
        "애플", "마이크로소프트", "아마존", "테슬라", "엔비디아",
    ),
}
# ⑤ 글로벌 통화정책·공급 주체 — 명시되면 3시장
_GLOBAL_MACRO_KEYWORDS: Tuple[str, ...] = (
    "fomc", "federal reserve", "fed", "powell", "ecb", "bank of japan", "boj", "opec",
    "연준", "연방준비", "파월", "유럽중앙은행", "일본은행",
)

_warned_sources: set = set()
_warn_lock = threading.Lock()


def _warn_unknown(source: str) -> None:
    with _warn_lock:
        if source in _warned_sources:
            return
        _warned_sources.add(source)
    logger.warning(
        "[news_scope] 미등재 source=%r — 시장 범위 없음(target_markets={}). "
        "새 수집기면 external_context/core/news_scope.py::_SOURCE_SCOPE 에 등재할 것", source,
    )


def news_target_markets(
    source: Optional[str],
    news_id: Optional[str],
    symbol: Optional[str],
    text: Optional[str],
) -> Tuple[str, ...]:
    """뉴스 1행의 시장 범위 (MARKETS 순서, 중복 없음). 빈 튜플 = 어느 시장에도 귀속 안 됨."""
    src = (source or "").strip()
    sym = (symbol or "").strip()
    scope = set(_SOURCE_SCOPE.get(src, ()))
    known = bool(scope)

    if not known and _MACRO_ID_PREFIX.match(news_id or ""):
        scope.update(_ALL)
        known = True
    if _KR_TICKER.match(sym):
        scope.add("kr_market")
        known = True

    body = text or ""
    if any_term(body, _GLOBAL_MACRO_KEYWORDS):
        scope.update(_ALL)
    for mkt, terms in _ASSET_KEYWORDS.items():
        if mkt not in scope and any_term(body, terms):
            scope.add(mkt)

    if not scope and not known:
        _warn_unknown(src)
    return tuple(m for m in MARKETS if m in scope)


# ─────────────────────────────────────────────────────────────────────────────
# 예정 이벤트(external_context.scheduled_events) 시장 범위 — 2026-10-14, Mac 요청서 rev.3 H7#2-b
#   scheduled_events 에도 시장 컬럼이 없어 MCP 시장 요약 3종이 같은 목록을 보였다(미장 요약에 DART 공시).
#   읽기 시점 판정이라 저장 컬럼은 두지 않는다(행 수 수천, 읽을 때 걸러도 충분).
#   실측(10-14): dart = KRX 코드(6자리, 신형 영숫자 0004V0 포함) · yfinance = 영문 티커(미장) + 6자리 KRX 코드(국장,
#   접미사 없음) · economic_calendar/federalreserve.gov/bls.gov = GLOBAL · opec.org = CL=F.
# ─────────────────────────────────────────────────────────────────────────────
_EVENT_SOURCE_SCOPE: Dict[str, Tuple[str, ...]] = {
    "dart": ("kr_market",),
    "economic_calendar": _ALL,
    "federalreserve.gov": _ALL,
    "bls.gov": _ALL,
    "opec.org": _ALL,
}
_KRX_CODE = re.compile(r"^[0-9][0-9A-Z]{5}$")   # 005930 · 신형 영숫자 코드 0004V0


def event_target_markets(source: Optional[str], symbol: Optional[str]) -> Tuple[str, ...]:
    """예정 이벤트 1행의 시장 범위 (MARKETS 순서). 빈 튜플 = 귀속 없음(미등재 출처 — 경고)."""
    src = (source or "").strip()
    sym = (symbol or "").strip().upper()
    if src in _EVENT_SOURCE_SCOPE:
        return _EVENT_SOURCE_SCOPE[src]
    if sym == "GLOBAL":
        return _ALL
    if src == "yfinance":
        if _KRX_CODE.match(sym) or sym.endswith((".KS", ".KQ")):
            return ("kr_market",)
        if sym.endswith("-USD"):
            return ("coin_market",)
        return ("us_market",)
    _warn_unknown(f"scheduled_events:{src}")
    return ()


def row_text(row: Dict[str, object]) -> str:
    """분류에 쓰는 본문 — 제목·요약(한/영). collection_runner 와 백필이 같은 정의를 쓴다."""
    parts: Sequence[object] = (
        row.get("title"), row.get("summary"), row.get("title_en"), row.get("summary_en"),
    )
    return " ".join(str(p) for p in parts if p)
