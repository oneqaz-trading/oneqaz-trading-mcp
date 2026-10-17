# -*- coding: utf-8 -*-
"""
엔티티 resolver — 입력 문자열 → {status, entity, candidates}
============================================================
제안서 ED10(2026-10-15 승인): 기존 `mcps/symbol_coverage.py` 를 엔진으로 두고
① `external_context.symbol_aliases` ② 구조 바스켓 멤버(basket_only + 소속 그룹) ③ 그룹 id·라벨 사전을 더한다.

원칙: **단일 확정 매치만 자동 해석한다.** 그 외는 추측하지 않고 `ambiguous`(후보 목록, 본문 비움)
또는 `not_covered`(지원 범위 안내). 기존 tool(get_signals 등)의 symbol_not_found 동작은 바꾸지 않는다.

`not_covered` 는 "판정에 필요한 원천을 **다 읽고** 없었다" 일 때만 낸다. 원천(시그널 유니버스 프로브·
종목명·별칭·구조 요약)을 못 읽어 매치가 없으면 `unavailable`(재시도 가능)이다 — DB 장애를
"OneQAZ 가 추적하지 않는 종목" 으로 답하던 조용한 사망(2026-10-17 실측: DB 불통 시 BTC → not_covered)의 수리.
매치는 찾았지만 일부 원천을 못 읽었으면 결과는 그대로 두고 `incomplete_sources` 와 note 로 알린다.

확정(strong) 경로: 코드·티커 정확 일치(시그널 유니버스) / master 이름 정확 일치 / 별칭 정확 일치.
단 **별칭이 다른 종목명의 접두사이기도 하면** 별칭 경로는 약한 후보로 강등한다 —
예: *"삼성"*(manual → 005930)은 삼성SDI·삼성물산 등의 접두사라 그룹명이다(req E9, 진단 §3-2).
"""

from __future__ import annotations

import logging
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

from oneqaz_trading_mcp.entity import sources as S

logger = logging.getLogger("MarketMCP")

SUPPORTED_ENTITY_TYPES = ("symbol", "group", "market")
SUPPORTED_MARKETS = S.MARKETS

_MARKET_WORDS = {
    "crypto": ("crypto", "coin", "coins", "coin_market", "cryptocurrency", "코인", "암호화폐", "가상자산", "가상화폐"),
    "kr_stock": ("kr", "kr_stock", "kr_market", "korea", "korean", "kospi", "kosdaq", "krx",
                 "한국", "국장", "국내", "코스피", "코스닥", "한국장", "한국 주식"),
    "us_stock": ("us", "us_stock", "us_market", "usa", "u.s.", "nyse", "nasdaq", "s&p", "s&p500",
                 "미국", "미장", "나스닥", "미국장", "미국 주식"),
}

# 그룹 라벨 사전 (한/영 → 그룹 id 토큰). 매치는 "토큰이 그룹 id 의 '_' 토큰으로 들어있는가".
_GROUP_WORDS: Dict[str, Tuple[str, ...]] = {
    "반도체": ("semiconductor",), "semis": ("semiconductor",), "semiconductors": ("semiconductor",),
    "금융": ("financials", "financial"), "은행": ("financials", "financial"),
    "바이오": ("bio",), "헬스케어": ("healthcare", "health"), "제약": ("bio", "healthcare"),
    "화학": ("chemical",), "소재": ("materials",), "철강": ("materials",),
    "소비재": ("consumer",), "산업재": ("industrials",), "유틸리티": ("utilities",),
    "에너지": ("energy",), "기술": ("technology",), "테크": ("technology",), "부동산": ("real", "estate"),
    "통신": ("communication",), "미디어": ("media",), "방산": ("defense",), "방위산업": ("defense",),
    "원전": ("nuclear",), "원자력": ("nuclear",), "배당": ("dividend",), "채권": ("bond", "fixed"),
    "원자재": ("commodity",), "레이어1": ("layer1",), "레이어2": ("layer2",), "디파이": ("defi",),
    "밈": ("meme",), "밈코인": ("meme",), "게임": ("gaming",), "결제": ("payment",),
    "스토리지": ("storage",), "ai": ("ai",), "인공지능": ("ai",), "지수": ("broad", "index"),
    "대표지수": ("broad", "index"), "index": ("index",), "etf": ("etf",),
}


def _norm_text(raw: Any) -> str:
    return re.sub(r"\s+", " ", str(raw or "")).strip()


def _call_symbol(m: str, sym: str) -> str:
    return f"get_symbol_context(symbol='{sym}', market_id='{m}')"


def _call_group(m: str, gid: str) -> str:
    return f"get_group_context(group='{gid}', market_id='{m}')"


def _result(status: str, raw: str, *, entity: Optional[Dict[str, Any]] = None,
            matched_by: Optional[str] = None, coverage: Optional[str] = None,
            candidates: Optional[List[Dict[str, Any]]] = None, note: Optional[str] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {"status": status, "input": raw}
    if entity is not None:
        out["entity"] = entity
    if matched_by:
        out["matched_by"] = matched_by
    if coverage:
        out["coverage"] = coverage
    if candidates is not None:
        out["candidates"] = candidates[:6]
        if len(candidates) > 6:
            out["candidates_total"] = len(candidates)
    if note:
        out["note"] = note
    return out


def not_covered(raw: str, what: str) -> Dict[str, Any]:
    return _result(
        "not_covered", raw, candidates=[],
        note=(f"'{raw}' did not match any {what} that OneQAZ tracks. Supported markets: crypto, kr_stock, "
              "us_stock. Supported entity types: symbol (tickers, KRX 6-digit codes, names), group "
              "(OneQAZ market-structure groups), market. Korean stocks use 6-digit codes (e.g. 005930)."),
    )


def unavailable(raw: str, what: str, sources: List[str]) -> Dict[str, Any]:
    """조회 실패 — '추적하지 않음' 이 아니라 '지금 확인하지 못함'. 같은 입력으로 다시 부르면 풀릴 수 있다."""
    res = _result(
        "unavailable", raw, candidates=[],
        note=(f"Could not check '{raw}' against OneQAZ {what} coverage right now: {', '.join(sources)} "
              "could not be read. This is a temporary lookup failure, not a coverage answer — retry shortly."),
    )
    res["unavailable_sources"] = list(sources)
    res["retryable"] = True
    return res


def _mark_incomplete(res: Dict[str, Any], sources: List[str]) -> Dict[str, Any]:
    """매치는 냈지만 일부 원천을 못 읽은 결과 — 후보 목록이 완전하지 않을 수 있다고 적는다."""
    if sources:
        res["incomplete_sources"] = list(sources)
        res["note"] = ((res.get("note") + " ") if res.get("note") else "") + (
            f"Some sources could not be read ({', '.join(sources)}), so this result may be incomplete.")
    return res


# ─── 시장 ───────────────────────────────────────────────────────────────

def resolve_market(raw: Any) -> Dict[str, Any]:
    text = _norm_text(raw)
    q = text.lower().replace("-", "_")
    hits = [m for m, words in _MARKET_WORDS.items() if q in words]
    if len(hits) == 1:
        return _result("resolved", text, entity={"id": hits[0], "type": "market", "market": hits[0]},
                       matched_by="market_name")
    return not_covered(text, "market")


# ─── 종목 ───────────────────────────────────────────────────────────────

def _alias_rows(raw: str) -> List[Dict[str, Any]]:
    return S._rows(
        "external_context",
        "SELECT alias, canonical_symbol, market_id, alias_type FROM external_context.symbol_aliases "
        "WHERE lower(alias) = lower(?) AND market_id = ANY(?)",
        (raw, list(S.MASTER_SIGNAL.values())),
    )


_MASTER_TO_MARKET = {v: k for k, v in S.MASTER_SIGNAL.items()}


def resolve_symbol(raw: Any, market: Optional[str] = None, *,
                   exists: Optional[Callable[[str, str], Optional[bool]]] = None,
                   lookup_names: Optional[Callable[..., List[Dict[str, Any]]]] = None,
                   alias_rows: Optional[Callable[[str], List[Dict[str, Any]]]] = None,
                   structure_index: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None) -> Dict[str, Any]:
    """종목 해석. 의존 함수는 테스트에서 주입할 수 있다(기본 = 실 DB·JSON).

    exists 는 True/False/None(판정 불가) — None 을 '없음' 으로 읽지 않는다.
    lookup_names·alias_rows·structure_index 는 실패 시 예외를 올린다(기본 lookup_names 는 raise_errors=True).
    """
    from oneqaz_trading_mcp import symbol_coverage as SC
    exists = exists or SC.symbol_exists
    lookup_names = lookup_names or (lambda t: SC.lookup_names(t, raise_errors=True))
    alias_rows = alias_rows or _alias_rows
    structure_index = structure_index or S.structure_index

    text = _norm_text(raw)
    if not text:
        return not_covered(text, "symbol")
    sym = SC.normalize_symbol(text)
    scope = [market] if market else list(S.MARKETS)

    strong: Dict[Tuple[str, str], Dict[str, Any]] = {}
    weak: Dict[Tuple[str, str], Dict[str, Any]] = {}
    unread: List[str] = []  # 판정에 필요한데 못 읽은 원천 — 비어 있어야 '매치 없음' 을 '추적 안 함' 으로 읽을 수 있다

    def _unread(src: str) -> None:
        if src not in unread:
            unread.append(src)

    def _in_universe(m: str, s: str) -> bool:
        r = exists(m, s)
        if r is None:  # 범위 안 시장을 못 읽은 것과 다른 시장(착오 안내용)을 못 읽은 것은 무게가 다르다
            _unread("signal universe" if m in scope else "signal universe (other markets)")
        return r is True

    def _add(bucket: Dict, m: str, s: str, reason: str, name: Optional[str] = None) -> None:
        key = (m, s)
        if key in strong:
            if name and not strong[key].get("name"):
                strong[key]["name"] = name
            return
        cur = bucket.get(key)
        if cur is None:
            bucket[key] = {"id": s, "type": "symbol", "market": m, "reason": reason,
                           **({"name": name} if name else {}), "call": _call_symbol(m, s)}
        elif name and not cur.get("name"):
            cur["name"] = name

    # ① 코드·티커 정확 일치 — 모든 시장에서 본다(시장 착오를 후보로 알려주기 위해)
    probe_syms = [sym]
    if sym.endswith("-USD"):
        probe_syms.append(sym[: -len("-USD")])
    for m in S.MARKETS:
        for s in probe_syms:
            if _in_universe(m, s):
                _add(strong, m, s, "code")

    # ② 이름 — 정확 일치는 확정, 접두 일치는 후보
    names = []
    try:
        names = lookup_names(text) or []
    except Exception as e:  # 이름 조회 장애 — 코드 일치만으로 판정하되, 매치가 없으면 '못 읽음' 으로 끝낸다
        logger.warning("entity.resolver: name lookup failed (%r): %s", text, e)
        _unread("symbol names")
    prefix_symbols = set()
    for row in names:
        m = row.get("market_id")
        s = SC.normalize_symbol(row.get("symbol"))
        if not m or not s or row.get("structure_only"):
            continue
        nm = row.get("name_ko") or row.get("name_en")
        if row.get("match") == "name_prefix":
            prefix_symbols.add((m, s))
            if _in_universe(m, s):
                _add(weak, m, s, "name_prefix", nm)
        elif row.get("match") == "name" and _in_universe(m, s):
            _add(strong, m, s, "name", nm)
        elif row.get("match") == "code" and _in_universe(m, s):
            _add(strong, m, s, "code", nm)

    # ③ 별칭 — 다른 종목명의 접두사이기도 하면(그룹명 류) 약한 후보로 강등
    try:
        arows = alias_rows(text) or []
    except Exception as e:
        logger.warning("entity.resolver: alias lookup failed (%r): %s", text, e)
        _unread("symbol aliases")
        arows = []
    alias_targets = {(_MASTER_TO_MARKET.get(r.get("market_id")), SC.normalize_symbol(r.get("canonical_symbol")))
                     for r in arows}
    alias_targets = {t for t in alias_targets if t[0] and t[1]}
    for m, s in alias_targets:
        if not _in_universe(m, s):
            continue
        others = {p for p in prefix_symbols if p != (m, s)}
        if len(alias_targets) > 1 or others:
            _add(weak, m, s, "alias_ambiguous")
        else:
            _add(strong, m, s, "alias")

    for k in list(weak):
        if k in strong:
            weak.pop(k)

    in_scope = {k: v for k, v in strong.items() if k[0] in scope}
    if len(in_scope) == 1:
        (m, s), c = next(iter(in_scope.items()))
        ent = {"id": s, "type": "symbol", "market": m}
        if c.get("name"):
            ent["name"] = c["name"]
        # 시그널 유니버스에서 확인된 단일 매치 — 다른 원천을 못 읽었어도 이 결론은 서 있다(표기하지 않는다)
        return _result("resolved", text, entity=ent, matched_by=c["reason"], coverage="signal")
    if len(in_scope) > 1:
        return _mark_incomplete(
            _result("ambiguous", text, candidates=list(in_scope.values()) + list(weak.values()),
                    note="More than one tracked symbol matches; pick one and call again."), unread)

    # 여기부터는 "범위 안 시그널 유니버스에 확정 매치가 없다" 는 결론 위에 선다(바스켓 전용·다른 시장 후보).
    # 유니버스 프로브를 못 읽었다면 그 결론부터 성립하지 않는다 — 추측하지 않고 멈춘다.
    if "signal universe" in unread:
        return unavailable(text, "symbol", unread)

    # ④ 시그널 유니버스엔 없고 구조 바스켓에만 있는 것 (예: QQQ ∈ us broad_index)
    basket: List[Dict[str, Any]] = []
    for m in scope:
        try:
            idx = structure_index(m)
        except Exception as e:
            logger.warning("entity.resolver: structure index failed (%s): %s", m, e)
            idx = None
        if not idx:  # 구조 요약 파일 부재·손상 — 운영 3시장은 상시 존재(2026-10-17 실측)
            _unread("structure baskets")
        for s in probe_syms:
            gids = (idx or {}).get("member_groups", {}).get(s)
            if gids:
                basket.append({"id": s, "type": "symbol", "market": m, "reason": "basket_only",
                               "groups": sorted(set(gids)), "call": _call_symbol(m, s)})
    if len(basket) == 1 and not weak:
        b = basket[0]
        return _mark_incomplete(
            _result("resolved", text, entity={"id": b["id"], "type": "symbol", "market": b["market"]},
                    matched_by="basket_member", coverage="basket_only"), unread)

    other_market = [dict(v, reason="other_market") for k, v in strong.items() if k[0] not in scope]
    cands = other_market + basket + list(weak.values())
    if cands:
        return _mark_incomplete(
            _result("ambiguous", text, candidates=cands,
                    note=("No unique match in the requested scope; candidates are listed "
                          "instead of a guess.")), unread)
    if unread:
        return unavailable(text, "symbol", unread)
    return not_covered(text, "symbol")


# ─── 그룹 ───────────────────────────────────────────────────────────────

def _group_tokens(q: str) -> Tuple[str, ...]:
    words = _GROUP_WORDS.get(q.lower())
    if words:
        return words
    return tuple(t for t in re.split(r"[_\s]+", q.lower()) if t)


def resolve_group(raw: Any, market: Optional[str] = None, *,
                  structure_index: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None) -> Dict[str, Any]:
    structure_index = structure_index or S.structure_index
    text = _norm_text(raw)
    if not text:
        return not_covered(text, "group")
    q = text.lower().replace("-", "_").replace(" ", "_")
    scope = [market] if market else list(S.MARKETS)

    exact: List[Dict[str, Any]] = []
    token: List[Dict[str, Any]] = []
    member: List[Dict[str, Any]] = []
    available: Dict[str, List[str]] = {}
    unread: List[str] = []  # 구조 요약을 못 읽은 시장
    toks = _group_tokens(text)
    for m in scope:
        try:
            idx = structure_index(m)
        except Exception as e:
            logger.warning("entity.resolver: structure index failed (%s): %s", m, e)
            idx = None
        if not idx:  # 파일 부재·손상 — 이 시장의 그룹은 '없다' 가 아니라 '못 읽었다'
            unread.append(f"{m} structure groups")
            continue
        gids = list(idx.get("groups") or {})
        available[m] = gids
        for gid in gids:
            c = {"id": gid, "type": "group", "market": m, "call": _call_group(m, gid)}
            g_toks = set(gid.lower().split("_"))
            if gid.lower() == q:
                exact.append(dict(c, reason="group_id"))
            elif toks and all(t in g_toks or any(gt.startswith(t) for gt in g_toks) for t in toks):
                token.append(dict(c, reason="group_label"))
        # 그룹 멤버 티커(ETF 등)로 들어온 경우
        key = S._member_key(m, text)
        for gid in sorted(set((idx.get("member_groups") or {}).get(key, []))):
            member.append({"id": gid, "type": "group", "market": m, "reason": f"contains_member:{key}",
                           "call": _call_group(m, gid)})

    seen = set()
    cands: List[Dict[str, Any]] = []
    for c in exact + token + member:
        k = (c["market"], c["id"])
        if k not in seen:
            seen.add(k)
            cands.append(c)
    if len(cands) == 1:
        c = cands[0]
        return _mark_incomplete(
            _result("resolved", text, entity={"id": c["id"], "type": "group", "market": c["market"]},
                    matched_by=c["reason"].split(":")[0]), unread)
    if cands:
        return _mark_incomplete(
            _result("ambiguous", text, candidates=cands,
                    note="More than one OneQAZ structure group matches; pick one and call again."), unread)
    if unread:
        res = unavailable(text, "group", unread)
        if available:
            res["available_groups"] = available
        return res
    res = not_covered(text, "group")
    res["available_groups"] = available
    return res
