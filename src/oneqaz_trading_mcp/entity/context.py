# -*- coding: utf-8 -*-
"""
엔티티 타입별 조립 — symbol · group · market (Stage A 1차 묶음, 제안서 ED3)
===========================================================================
섹션의 뜻 (req 6.2):
  upstream   — 이 엔티티를 움직이는 것 (소속 그룹·상위계층 lead·앞서는 종목 / 그룹이면 소속 시장 / 시장이면 거시 가설)
  peers      — 같은 층위 (종목: 이 종목을 뒤따르는 종목 · 그룹: 함께 움직이는 그룹)
  downstream — 이 엔티티가 움직이는 것 (그룹: 멤버·바스켓이 앞서는 종목 · 시장: 그룹)
  news       — 연결된 뉴스

섹션 하나의 원천이 실패해도 응답 전체를 깨지 않는다 — error 로그(스택 포함) + 그 섹션 gap 으로 정직하게 표기.
원천·필터 근거 = diag_mcp_entity_context_phase0_2026_10_15.md, 설계 = proposal_mcp_entity_context_phase1 §2.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from oneqaz_trading_mcp.compliance import INTERPRETATION, to_bias
from oneqaz_trading_mcp.entity import links as L
from oneqaz_trading_mcp.entity import sources as S
from oneqaz_trading_mcp.entity.assembler import SECTIONS, assemble, gap, headline_item, unresolved
from oneqaz_trading_mcp.entity.resolver import resolve_group, resolve_market, resolve_symbol
from oneqaz_trading_mcp.entity.session import (
    data_basis, epoch_from_any, iso_from_any, iso_utc, kst_label, session_state, structure_basis,
)

logger = logging.getLogger("MarketMCP")

LIMIT_DEFAULT = 8
LIMIT_MAX = 50
MEMBERS_DEFAULT = 10
MEMBERS_MAX = 100
NEWS_DEFAULT = 5
NEWS_DAYS = 7
MARKET_NEWS_HOURS = 24
MARKET_WIDE_FALLBACK = 3   # 직접 연결 0 일 때만 붙이는 시장 뉴스 수 (req 6.4)
STRUCTURE_MIN_N = 30       # 첫접촉 제안 §3-6 구조 예측 MIN_N

SIGNAL_HORIZON = "signal judged at 30m and 24h"


def _limits(limit: Any) -> Tuple[int, int, int]:
    try:
        lim = int(limit)
    except (TypeError, ValueError):
        lim = LIMIT_DEFAULT
    lim = max(1, min(lim, LIMIT_MAX))
    members = max(MEMBERS_DEFAULT, min(lim, MEMBERS_MAX)) if lim > LIMIT_DEFAULT else MEMBERS_DEFAULT
    news = NEWS_DEFAULT if lim == LIMIT_DEFAULT else max(1, min(lim, 30))
    return lim, members, news


def _safe(section: str, gaps: List[Dict[str, str]], what: str, fn: Callable[[], Any], default: Any) -> Any:
    """원천 1개 호출 — 실패는 error 로그 + gap(data_unavailable, 원인 명시). 조용히 0 으로 만들지 않는다."""
    try:
        return fn()
    except Exception as e:
        logger.error("entity_context: %s source failed for section %s: %s", what, section, e, exc_info=True)
        gaps.append(gap(section, "data_unavailable",
                        f"{what} could not be read right now ({type(e).__name__}); this is a temporary "
                        "source failure, not an absence of links."))
        return default


def _structure_calibration_text(market: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    try:
        sk = S.structure_skill(market, "1d", 30)
    except Exception as e:
        logger.error("entity_context: structure skill failed (%s): %s", market, e, exc_info=True)
        return "unavailable", None
    if not sk or (sk.get("n") or 0) < STRUCTURE_MIN_N:
        return "insufficient_sample", sk
    return (f"structure 1d calls, last 30d: hit {sk['hit_rate']:.0%} vs majority baseline "
            f"{sk['baseline']:.0%} (skill {sk['skill']:+.0%}, n={sk['n']})"), sk


def _signal_calibration_text(market: str, interval: str) -> str:
    try:
        c = S.signal_calibration_cell(market, interval)
    except Exception as e:
        logger.error("entity_context: signal calibration failed (%s/%s): %s", market, interval, e, exc_info=True)
        return "unavailable"
    if not c or c.get("ece") is None:
        if c and c.get("status") == "withheld":
            return "rank-only score; confidence calibration withheld for this cell (artifact)"
        return "rank-only score; confidence calibration insufficient_sample"
    return f"rank-only score; confidence ECE {c['ece']:.2f} (n={int(c.get('n') or 0):,}, {c.get('snapshot_day')})"


def _attach_names(market: str, links: List[Dict[str, Any]]) -> None:
    """symbol 표적에 이름을 붙인다(한 번의 배치 조회)."""
    syms = [lk["target"]["id"] for lk in links if lk["target"].get("type") == "symbol"
            and lk["target"].get("market") == market and not lk["target"].get("name")]
    if not syms:
        return
    names = S.names_for([S.MASTER_SIGNAL[market]], syms)
    for lk in links:
        t = lk["target"]
        if t.get("type") == "symbol" and not t.get("name"):
            nm = L._pick_name(market, names.get((S.MASTER_SIGNAL[market], str(t["id"]).upper())))
            if nm:
                t["name"] = nm


def _attach_symbol_observations(market: str, links: List[Dict[str, Any]]) -> None:
    syms = [lk["target"]["id"] for lk in links if lk["target"].get("type") == "symbol"
            and "target_observation" not in lk]
    if not syms:
        return
    obs = S.latest_combined_many(market, syms)
    for lk in links:
        t = lk["target"]
        if t.get("type") == "symbol" and "target_observation" not in lk:
            o = L.signal_observation(obs.get(t["id"]))
            if o:
                lk["target_observation"] = o


def _nav(intent: str, tool: str, args: Dict[str, Any], rationale: str) -> Dict[str, Any]:
    return {"intent": intent, "tool": tool, "args": args, "rationale": rationale}


# ─── symbol ─────────────────────────────────────────────────────────────

def symbol_context(symbol: Any, market_id: Optional[str] = None, limit: Any = LIMIT_DEFAULT) -> Tuple[Dict, List]:
    """(payload, next_actions)."""
    tool = "get_symbol_context"
    res = resolve_symbol(symbol, market_id)
    if res["status"] != "resolved":
        return unresolved(tool, res, "symbol"), []
    ent = dict(res["entity"])
    m, sym = ent["market"], ent["id"]
    lim, _members_lim, news_lim = _limits(limit)
    gaps: List[Dict[str, str]] = []
    sections: Dict[str, List[Dict[str, Any]]] = {s: [] for s in SECTIONS}
    session = session_state(m)
    basket_only = res.get("coverage") == "basket_only"

    structure = _safe("upstream", gaps, "OneQAZ structure groups", lambda: S.structure_index(m), None)
    gids = sorted(set(((structure or {}).get("member_groups") or {}).get(sym, [])))

    # 이름
    if not ent.get("name"):
        try:
            nm = L._pick_name(m, S.names_for([S.MASTER_SIGNAL[m], S.MASTER_STRUCT[m]], [sym]).get(
                (S.MASTER_SIGNAL[m], sym.upper())))
            if nm:
                ent["name"] = nm
        except Exception as e:
            logger.warning("entity_context: name lookup failed (%s/%s): %s", m, sym, e)

    # self
    self_block: Dict[str, Any]
    data_ts: Optional[float] = None
    if basket_only:
        self_block = {
            "coverage": "basket_only", "signal": None,
            "note": (f"{sym} has no per-symbol research signal in OneQAZ; it is tracked only as a member of "
                     f"structure group(s) {', '.join(gids) or '(none)'} in {m}. The group state is in upstream."),
        }
        gaps.append(gap("self", "data_unavailable", f"{sym} is outside the per-symbol signal universe of {m}."))
        data_ts = (structure or {}).get("updated_ts")
    else:
        timing = S.timing_interval(m)
        rows = {}
        for iv in ("combined", timing):
            r = _safe("self", gaps, f"latest {iv} signal", lambda iv=iv: S.latest_signal(m, sym, iv), None)
            if r:
                rows[iv] = r
        sig: Dict[str, Any] = {}
        for iv, r in rows.items():
            score = S.num(r.get("signal_score"))
            try:
                from oneqaz_trading_mcp.confidence_calibration import calibrated_confidence
                cal = calibrated_confidence(m, iv, r.get("confidence"))
            except Exception as e:
                logger.warning("entity_context: calibrated_confidence failed (%s/%s): %s", m, iv, e)
                cal = None
            sig[iv] = {
                "signal_score": round(score, 3) if score is not None else None,
                "bias": to_bias(score),
                "confidence_calibrated": cal,
                "regime_label": r.get("regime_label"),
                "as_of": iso_from_any(r.get("timestamp")),
                "role": "combined" if iv == "combined" else "timing",
            }
        comb = rows.get("combined")
        data_ts = S.num(comb.get("timestamp")) if comb else None
        self_block = {"coverage": "signal", "signal": sig or None}
        if not sig:
            gaps.append(gap("self", "data_unavailable", f"No signal row found for {sym} in {m}."))

    basis = structure_basis(m, data_ts, session) if basket_only else data_basis(m, data_ts, session)

    # upstream ① 소속 그룹 (definitional)
    for gid in gids:
        g = structure["groups"].get(gid) or {}
        raw_id = next((raw for raw, key, _ in structure["members"].get(gid, []) if key == sym), sym)
        sections["upstream"].append(L.membership_link(m, gid, g, structure.get("updated_at"), raw_id))
    if structure is not None and not gids:
        gaps.append(gap("upstream", "not_linked", f"{sym} is not a member of any OneQAZ structure group in {m}."))

    # upstream ② 상위계층(거시·ETF·바스켓) lead-lag
    cl_rows = _safe("upstream", gaps, "cross-layer lead-lag links", lambda: S.cross_layer_for_follow(m, sym), None)
    if cl_rows is not None:
        skill = _safe("upstream", gaps, "cross-layer forward test", lambda: S.cross_layer_skill(m), None)
        mm = _safe("upstream", gaps, "macro category members", S.macro_category_members, {}) or {}
        cl_links, cl_excl = L.cross_layer_links(m, cl_rows, L.cross_layer_validation(m, skill),
                                                direction="follow", macro_members=mm, structure=structure)
        sections["upstream"] += cl_links
        if not cl_links:
            cov = _safe("upstream", gaps, "cross-layer coverage", lambda: S.cross_layer_coverage(m), {}) or {}
            gaps.append(gap(
                "upstream", "data_unavailable",
                f"No stable macro/ETF/basket lead-lag link is stored for {sym}. Cross-layer links are computed "
                f"only for top-volume symbols ({int(cov.get('follows') or 0)} {m} symbols currently have one) "
                "and kept only after multiple-testing correction, so this is not evidence of no relationship."))

    # upstream ③ 앞서는 종목 / peers: 뒤따르는 종목 (symbol_peer_links 최신 배치)
    pl = _safe("peers", gaps, "symbol lead-lag links", lambda: S.peer_links(m, sym), None)
    if pl is not None:
        up, peers, excl = L.peer_links(m, sym, pl["rows"])
        sections["upstream"] += up
        sections["peers"] += peers
        n_stale = len(pl["stale_rows"])
        if n_stale:
            gaps.append(gap("peers", "stale",
                            f"{n_stale} older symbol lead-lag link(s) for {sym} were not reconfirmed in the latest "
                            f"run ({iso_from_any(pl['batch_at'])}) and are not served."))
        if not pl["rows"] and not n_stale:
            gaps.append(gap("peers", "data_unavailable",
                            f"Symbol lead-lag links are computed only for the top 30 {m} symbols by 15m volume "
                            f"(14-day window); {sym} has none."))
        elif pl["rows"] and not peers:
            gaps.append(gap("peers", "not_linked",
                            f"In the latest run no symbol followed {sym} with |ρ| ≥ 0.30."))

    # downstream — 단일 종목엔 해당 없음
    gaps.append(gap("downstream", "not_applicable",
                    "A single symbol has no members; symbols that follow it are listed in peers."))

    # news
    totals: Dict[str, int] = {}
    sn = _safe("news", gaps, "symbol-tagged news", lambda: S.symbol_news(m, sym, NEWS_DAYS, news_lim), None)
    if sn is not None:
        if sn["total"]:
            totals["news"] = sn["total"]
            sections["news"] += L.news_links(sn["rows"], metric="news_symbol_tag", scope="direct")
        else:
            cov = _safe("news", gaps, "news tagging coverage", lambda: S.news_tagging_coverage(m), None)
            if cov == 0:
                gaps.append(gap("news", "data_unavailable",
                                f"The news pipeline does not tag {m} symbols individually (0 tagged symbols in "
                                "the last 30 days); market-wide items are included with scope=market_wide."))
            elif cov is not None:
                gaps.append(gap("news", "not_linked",
                                f"No news tagged with {sym} in the last {NEWS_DAYS} days; market-wide items are "
                                "included with scope=market_wide."))
            mw = _safe("news", gaps, "market news", lambda: S.market_news(m, MARKET_NEWS_HOURS, MARKET_WIDE_FALLBACK), None)
            if mw:
                sections["news"] += L.news_links(mw["rows"], metric="news_market_tag", scope="market_wide", via=m)

    _attach_names(m, sections["upstream"] + sections["peers"])
    _safe("peers", gaps, "target observations",
          lambda: _attach_symbol_observations(m, sections["upstream"] + sections["peers"]), None)

    # headline
    head: List[Dict[str, Any]] = []
    label = f"{sym} ({ent['name']}, {m})" if ent.get("name") else f"{sym} ({m})"
    if basket_only:
        head.append(headline_item(
            f"{label} has no per-symbol research signal; OneQAZ tracks it as a member of structure group "
            f"{', '.join(gids)}", as_of=iso_utc(data_ts), basis=basis, horizon="not_applicable",
            calibration="not_applicable"))
    elif self_block.get("signal") and (self_block["signal"].get("combined") or {}).get("signal_score") is not None:
        c = self_block["signal"]["combined"]
        head.append(headline_item(
            f"{label}: combined signal score {c['signal_score']:+.2f} ({c['bias']} bias), regime label "
            f"{c['regime_label'] or 'n/a'}, computed {kst_label(data_ts)} ({basis.replace('_', ' ')} data)",
            as_of=c["as_of"], basis=basis, horizon=SIGNAL_HORIZON,
            calibration=_signal_calibration_text(m, "combined")))
    if gids:
        g = structure["groups"][gids[0]]
        ob = L.group_observation(g, structure.get("updated_at"))
        cal, _ = _structure_calibration_text(m)
        head.append(headline_item(
            f"{sym} belongs to OneQAZ structure group {gids[0]} ({g.get('symbols', '?')} members); group 1d "
            f"direction {ob['direction_1d'] or 'n/a'}, regime_avg {ob['regime_avg']} as of "
            f"{kst_label(structure.get('updated_ts'))}",
            as_of=structure.get("updated_at"),
            basis=structure_basis(m, structure.get("updated_ts"), session), horizon="1d group direction",
            calibration=cal))
    stat = [lk for lk in sections["upstream"] if lk["evidence"]["kind"] == "statistical"]
    if stat:
        top = max(stat, key=lambda lk: abs(lk["evidence"]["value"]))
        ev, t = top["evidence"], top["target"]
        lag = f"{ev['lag_days']}d" if "lag_days" in ev else f"{ev.get('lag_minutes')}m"
        val = ev.get("validation") or {}
        verdict = val.get("verdict") if val.get("status") == "forward_tested" else "not run"
        head.append(headline_item(
            f"Highest-|ρ| tracked lead-lag link: {sym} lags {t['id']} by {lag}, ρ={ev['value']:+.2f}, "
            f"n={ev['n']}; forward test of such links: {verdict}",
            as_of=ev["as_of"], basis="historical_returns", horizon=f"{lag} lag",
            calibration=(f"market-level forward test {verdict}" if verdict != "not run" else "not_forward_tested")))

    nav = []
    if gids:
        nav.append(_nav("see_group", "get_group_context", {"group": gids[0], "market_id": m},
                        f"State, members and co-moving groups of {gids[0]}."))
    nav.append(_nav("see_market", "get_market_context", {"market": m}, f"Market-level structure of {m}."))
    if not basket_only:
        nav.append(_nav("signal_detail", "get_signal_detail", {"market_id": m, "symbol": sym},
                        "Indicator-level detail of the signal."))

    payload = assemble(
        tool=tool, entity=ent, resolution=res, self_block=self_block, sections=sections, totals=totals,
        limits={"upstream": lim, "peers": lim, "downstream": lim, "news": news_lim + MARKET_WIDE_FALLBACK},
        gaps=gaps, headline=head, data_as_of=iso_utc(data_ts), session_state=session, basis=basis,
        extra_interpretation=[INTERPRETATION["impact_level"]] if sections["news"] else None,
    )
    return payload, nav


# ─── group ──────────────────────────────────────────────────────────────

def group_context(group: Any, market_id: Optional[str] = None, limit: Any = LIMIT_DEFAULT) -> Tuple[Dict, List]:
    tool = "get_group_context"
    res = resolve_group(group, market_id)
    if res["status"] != "resolved":
        return unresolved(tool, res, "group"), []
    ent = dict(res["entity"])
    m, gid = ent["market"], ent["id"]
    lim, members_lim, _ = _limits(limit)
    gaps: List[Dict[str, str]] = []
    sections: Dict[str, List[Dict[str, Any]]] = {s: [] for s in SECTIONS}
    totals: Dict[str, int] = {}
    session = session_state(m)

    structure = S.structure_index(m)  # resolver 가 방금 읽었다 — 실패면 resolver 단계에서 이미 드러난다
    g = structure["groups"][gid]
    upd, upd_ts = structure.get("updated_at"), structure.get("updated_ts")
    basis = structure_basis(m, upd_ts, session)
    per = {}
    for iv, blk in (g.get("per_interval") or {}).items():
        ra = S.num(blk.get("regime_avg"))
        per[iv] = {"role": blk.get("role"), "direction": blk.get("direction"),
                   "regime_avg": round(ra, 2) if ra is not None else None,
                   "symbol_count": blk.get("symbol_count")}
    ra = S.num(g.get("regime_avg"))
    self_block = {
        "group_id": gid, "market": m, "members": g.get("symbols"),
        "regime_dominant": g.get("regime_dominant"),
        "regime_avg": round(ra, 2) if ra is not None else None,
        "per_interval": per, "interval_conflict": bool(g.get("conflicts")),
        "as_of": upd,
    }

    # upstream — 소속 시장(정의) · 거시→그룹은 매핑 원천 없음
    sections["upstream"].append(L.make_link(
        target={"id": m, "type": "market"}, relation="member_of", kind="definitional",
        metric="structure_group", value=None, n=None, as_of=upd,
        source_table="market_structure_summary.json:groups"))
    gaps.append(gap("upstream", "data_unavailable",
                    "OneQAZ does not map macro factors to structure groups; macro links exist only at "
                    "market level (get_market_context) and for some individual symbols (get_symbol_context)."))

    # peers — 그룹 상관 최신 스냅샷
    sp = _safe("peers", gaps, "group correlation snapshot", lambda: S.sector_pairs(m, gid), None)
    if sp is not None:
        links, nan, low = L.sector_links(m, gid, sp["rows"], structure)
        sections["peers"] += links
        if not sp["in_snapshot"]:
            gaps.append(gap("peers", "data_unavailable",
                            f"{gid} is not part of the latest group-correlation snapshot for {m}."))
        if nan:
            gaps.append(gap("peers", "data_quality",
                            f"{nan} correlation value(s) for {gid} in the latest snapshot are NaN and are not served."))
        if low:
            gaps.append(gap("peers", "insufficient_sample",
                            f"{low} pair(s) for {gid} have fewer than {L.SECTOR_MIN_N} daily samples."))

    # downstream — 바스켓이 앞서는 종목(통계) + 멤버(정의)
    cl = _safe("downstream", gaps, "basket lead-lag links", lambda: S.cross_layer_for_basket(m, gid), None)
    if cl:
        skill = _safe("downstream", gaps, "cross-layer forward test", lambda: S.cross_layer_skill(m), None)
        b_links, _ = L.cross_layer_links(m, cl, L.cross_layer_validation(m, skill), direction="basket",
                                         macro_members={}, structure=structure)
        sections["downstream"] += b_links
    weights = g.get("volume_weights") or {}
    mems = sorted(structure["members"].get(gid, []),
                  key=lambda t: (-(S.num(weights.get(t[0])) or 0.0), t[1]))
    totals["downstream"] = len(sections["downstream"]) + len(mems)
    keep = mems[:max(0, members_lim - len(sections["downstream"]))]
    names = _safe("downstream", gaps, "member names",
                  lambda: S.names_for([S.MASTER_SIGNAL[m], S.MASTER_STRUCT[m]], [k for _, k, _ in keep]), {}) or {}
    obs = _safe("downstream", gaps, "member signals",
                lambda: S.latest_combined_many(m, [k for _, k, _ in keep]), {}) or {}
    sections["downstream"] += L.member_links(m, gid, g, keep, upd, names, obs)
    if not mems:
        gaps.append(gap("downstream", "data_unavailable", f"No member list is recorded for {gid}."))
    _attach_names(m, sections["downstream"])

    # news — 그룹 단위 태깅 없음
    gaps.append(gap("news", "data_unavailable",
                    "News items are not tagged with sectors or structure groups (news_events.sector is empty)."))

    cal, _sk = _structure_calibration_text(m)
    head = [headline_item(
        f"{gid} ({m}, {g.get('symbols', '?')} members): 1d direction {per.get('1d', {}).get('direction') or 'n/a'}, "
        f"regime_avg {self_block['regime_avg']} as of {kst_label(upd_ts)}",
        as_of=upd, basis=basis, horizon="1d group direction", calibration=cal)]
    if keep:
        top = ", ".join(k for _, k, _ in keep[:3])
        head.append(headline_item(
            f"{gid} has {len(mems)} member symbols; first by volume weight: {top}",
            as_of=upd, basis=basis, horizon="not_applicable", calibration="not_applicable (definition)"))
    if sections["peers"]:
        top = max(sections["peers"], key=lambda lk: abs(lk["evidence"]["value"]))
        ev = top["evidence"]
        head.append(headline_item(
            f"Highest-|ρ| tracked group correlation: {gid} co_moves {top['target']['id']}, ρ={ev['value']:+.2f}, "
            f"n={ev['n']} days ({ev.get('window')})",
            as_of=ev["as_of"], basis="historical_returns", horizon="same-day co-movement",
            calibration="not_forward_tested"))

    nav = [_nav("see_market", "get_market_context", {"market": m}, f"Market-level structure of {m}.")]
    if keep:
        nav.insert(0, _nav("see_member", "get_symbol_context", {"symbol": keep[0][1], "market_id": m},
                           f"Context of member {keep[0][1]}."))
    payload = assemble(
        tool=tool, entity=ent, resolution=res, self_block=self_block, sections=sections, totals=totals,
        limits={"upstream": lim, "peers": lim, "downstream": members_lim, "news": lim},
        gaps=gaps, headline=head, data_as_of=upd, session_state=session, basis=basis,
        extra_interpretation=[INTERPRETATION["regime_avg"], INTERPRETATION["structure_calibration"]],
    )
    return payload, nav


# ─── market ─────────────────────────────────────────────────────────────

def market_context(market: Any, limit: Any = LIMIT_DEFAULT) -> Tuple[Dict, List]:
    tool = "get_market_context"
    res = resolve_market(market)
    if res["status"] != "resolved":
        return unresolved(tool, res, "market"), []
    ent = dict(res["entity"])
    m = ent["id"]
    lim, members_lim, news_lim = _limits(limit)
    gaps: List[Dict[str, str]] = []
    sections: Dict[str, List[Dict[str, Any]]] = {s: [] for s in SECTIONS}
    session = session_state(m)

    structure = _safe("downstream", gaps, "OneQAZ structure summary", lambda: S.structure_index(m), None)
    glob = _safe("self", gaps, "global regime summary", S.global_regime_overall, None)
    upd, upd_ts = (structure or {}).get("updated_at"), (structure or {}).get("updated_ts")
    basis = structure_basis(m, upd_ts, session)
    overall = (structure or {}).get("overall") or {}
    self_block: Dict[str, Any] = {
        "structure_regime": {"regime": overall.get("regime"), "score": S.num(overall.get("score")), "as_of": upd},
        "groups_tracked": len((structure or {}).get("groups") or {}),
    }
    if (structure or {}).get("extra", {}).get("index_spread"):
        self_block["index_spread"] = structure["extra"]["index_spread"]
    if glob:
        go = glob.get("overall") or {}
        self_block["global_macro_regime"] = {"regime": go.get("regime"), "score": S.num(go.get("score")),
                                             "as_of": iso_from_any(glob.get("updated_at"))}

    # upstream — 거시 카테고리 가설
    mh = _safe("upstream", gaps, "macro influence map", lambda: S.macro_hypotheses(m), None)
    if mh is not None:
        sections["upstream"] += L.macro_hypothesis_links(m, mh["cells"], mh["tuned_at"])
        if not mh["cells"]:
            gaps.append(gap("upstream", "data_unavailable", f"No macro influence prior is defined for {m}."))

    # peers — 시장 간 공출현 점수는 상관이 아니다
    gaps.append(gap("peers", "data_quality",
                    "Cross-market regime co-occurrence scores (get_cross_market_correlation) are count ramps, "
                    "not correlations, so they are not served as market-to-market links."))

    # downstream — 그룹 (중립 4 로부터의 거리 순)
    order_note = None
    if structure:
        groups = structure["groups"]
        order = sorted(groups, key=lambda k: (-abs((S.num(groups[k].get("regime_avg")) or 4.0) - 4.0), k))
        sections["downstream"] += L.market_group_links(m, structure, order)
        order_note = {"downstream": "abs(regime_avg - 4) descending (distance from the middle regime stage)"}

    # news — 시장 태깅 뉴스
    mn = _safe("news", gaps, "market news", lambda: S.market_news(m, MARKET_NEWS_HOURS, news_lim), None)
    totals = {}
    if mn is not None:
        sections["news"] += L.news_links(mn["rows"], metric="news_market_tag", scope="direct")
        totals["news"] = mn["total"]
        if not mn["total"]:
            gaps.append(gap("news", "not_linked", f"No news tagged with {m} in the last {MARKET_NEWS_HOURS}h."))

    cal, _ = _structure_calibration_text(m)
    head = [headline_item(
        f"{m} structure regime {overall.get('regime') or 'n/a'} (score {S.num(overall.get('score'))}), "
        f"{self_block['groups_tracked']} groups tracked as of {kst_label(upd_ts)}; session {session}",
        as_of=upd, basis=basis, horizon="1d group direction", calibration=cal)]
    if self_block.get("global_macro_regime"):
        gm = self_block["global_macro_regime"]
        head.append(headline_item(
            f"Global macro regime {gm['regime']} (score {gm['score']}) as of {kst_label(epoch_from_any(gm['as_of']))}",
            as_of=gm["as_of"], basis="latest_macro_cycle", horizon="macro category lag hours",
            calibration="tracked separately by get_prediction_accuracy"))
    if structure and sections["downstream"]:
        far = [lk["target"]["id"] + f" ({lk['target_observation']['regime_avg']})" for lk in sections["downstream"][:3]]
        head.append(headline_item(
            f"Groups furthest from the middle regime stage: {', '.join(far)}",
            as_of=upd, basis=basis, horizon="1d group direction", calibration=cal))

    nav = []
    if sections["downstream"]:
        g0 = sections["downstream"][0]["target"]["id"]
        nav.append(_nav("see_group", "get_group_context", {"group": g0, "market_id": m}, f"Context of group {g0}."))
    payload = assemble(
        tool=tool, entity=ent, resolution=res, self_block=self_block, sections=sections, totals=totals,
        limits={"upstream": lim, "peers": lim, "downstream": max(lim, members_lim), "news": news_lim},
        gaps=gaps, headline=head, data_as_of=upd, session_state=session, basis=basis,
        extra_interpretation=[INTERPRETATION["regime_avg"], INTERPRETATION["structure_calibration"]]
        + ([INTERPRETATION["impact_level"]] if sections["news"] else []),
        order_note=order_note,
    )
    return payload, nav
