# -*- coding: utf-8 -*-
"""
연결 객체 빌더 — 원천 행 → 공통 연결 스키마 (req 6.4 + 제안서 ED5·ED6)
=======================================================================
읽는 법: 연결 하나는 **"<entity> <relation> <target>"** 으로 읽는다.
  예) 005930 lags HYG (lag_days=1)  = HYG 의 움직임이 하루 앞선다
      005930 member_of kr_semiconductor
      kr_semiconductor co_moves kr_chemical

evidence 필수 키(E1): kind · metric · value · n · as_of · source_table — 값이 없으면 null 이지 키가 빠지지 않는다.
  kind = statistical | definitional | tag | hypothesis. n·value 는 statistical 에서만 의미가 있다(E3 은 statistical 에만).
관계 어휘는 kind 별로 고정(`mcps.compliance.RELATIONS_BY_KIND`) — 어기면 ValueError(개발 중 즉시 드러나게).

NaN·표본 미달 행은 여기서 버리고 버린 수를 돌려준다(호출부가 gaps 로 말한다). 연결을 지어내지 않는다.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from oneqaz_trading_mcp.compliance import RELATIONS_BY_KIND, to_bias
from oneqaz_trading_mcp.entity import sources as S
from oneqaz_trading_mcp.entity.session import epoch_from_any, iso_from_any

# 원천 writer 의 자체 저장 기준을 그대로 쓴다(새 숫자를 만들지 않는다 — 제안서 §2-10)
PEER_MIN_N = 100         # agent_history/core/symbol_peer.py DEFAULTS["min_samples"]
CROSS_LAYER_MIN_N = 60   # agent_history/core/cross_layer_leadlag.py DEFAULTS["min_samples"]
SECTOR_MIN_N = 20        # agent_history/core/sector_correlation.py DEFAULTS["min_samples"]

NOT_FORWARD_TESTED = {"status": "not_forward_tested"}


def make_link(*, target: Dict[str, Any], relation: str, kind: str, metric: str,
              value: Optional[float], n: Optional[int], as_of: Any, source_table: str,
              scope: str = "direct", lag_minutes: Optional[int] = None, lag_days: Optional[int] = None,
              extra: Optional[Dict[str, Any]] = None, validation: Optional[Dict[str, Any]] = None,
              target_observation: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    allowed = RELATIONS_BY_KIND.get(kind)
    if not allowed or relation not in allowed:
        raise ValueError(f"relation {relation!r} is not allowed for evidence kind {kind!r}")
    if scope not in ("direct", "group", "market_wide"):
        raise ValueError(f"invalid scope {scope!r}")
    ev: Dict[str, Any] = {"kind": kind, "metric": metric, "value": value}
    if lag_minutes is not None:
        ev["lag_minutes"] = int(lag_minutes)
    if lag_days is not None:
        ev["lag_days"] = int(lag_days)
    ev["n"] = n
    for k, v in (extra or {}).items():
        if v is not None:
            ev[k] = v
    ev["as_of"] = iso_from_any(as_of)
    ev["source_table"] = source_table
    if validation is not None:
        ev["validation"] = validation
    link: Dict[str, Any] = {"target": target, "relation": relation, "scope": scope, "evidence": ev}
    if target_observation:
        link["target_observation"] = target_observation
    return link


def sort_links(links: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """정렬: member_of → statistical(|value| 내림차순) → hypothesis → tag(최신순) → has_member(입력 순) → market_wide."""
    def key(lk: Dict[str, Any]) -> Tuple:
        ev = lk["evidence"]
        if lk.get("scope") == "market_wide":
            return (9, 0.0, "")
        if lk["relation"] == "member_of":
            return (0, 0.0, "")
        kind = ev["kind"]
        if kind == "statistical":
            return (1, -abs(ev.get("value") or 0.0), "")
        if kind == "hypothesis":
            return (2, -abs(ev.get("value") or 0.0), "")
        if kind == "tag":
            ts = epoch_from_any(ev.get("as_of")) or 0.0
            return (3, -ts, "")
        return (4, 0.0, "")
    return sorted(links, key=key)  # 안정 정렬 — has_member 는 호출부가 정한 순서를 유지


# ─── 표적 표기·관측값 ────────────────────────────────────────────────────

def symbol_target(market: str, sym: str, names: Optional[Dict] = None, raw_id: Optional[str] = None) -> Dict[str, Any]:
    t: Dict[str, Any] = {"id": sym, "type": "symbol", "market": market}
    nm = _pick_name(market, (names or {}).get((S.MASTER_SIGNAL[market], sym.upper())))
    if nm:
        t["name"] = nm
    if raw_id and raw_id.upper() != sym.upper():
        t["structure_id"] = raw_id
    return t


def _pick_name(market: str, d: Optional[Dict[str, Optional[str]]]) -> Optional[str]:
    if not d:
        return None
    if market == "us_stock":
        return d.get("name_en") or d.get("name_ko")
    return d.get("name_ko") or d.get("name_en")


def signal_observation(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    score = S.num(row.get("signal_score"))
    return {
        "signal_score": round(score, 3) if score is not None else None,
        "bias": to_bias(score),
        "regime_label": row.get("regime_label"),
        "as_of": iso_from_any(row.get("timestamp")),
        "basis_interval": "combined",
    }


def group_observation(g: Dict[str, Any], updated_at: Optional[str]) -> Dict[str, Any]:
    d1 = ((g.get("per_interval") or {}).get("1d") or {})
    ra = S.num(g.get("regime_avg"))
    return {
        "regime_dominant": g.get("regime_dominant"),
        "regime_avg": round(ra, 2) if ra is not None else None,
        "direction_1d": d1.get("direction"),
        "as_of": updated_at,
    }


def group_target(market: str, gid: str, g: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    t = {"id": gid, "type": "group", "market": market}
    if g is not None and isinstance(g.get("symbols"), int):
        t["members"] = g["symbols"]
    return t


# ─── 종목↔종목 lead-lag ──────────────────────────────────────────────────

def peer_links(market: str, entity: str, rows: List[Dict[str, Any]]) -> Tuple[List, List, int]:
    """(upstream: 이 종목보다 앞서는 종목 = 'entity lags target', peers: 이 종목을 뒤따르는 종목 =
    'entity leads target', 제외 수). 같은 상대·방향은 |ρ| 최대 lag 1개만."""
    best: Dict[Tuple[str, str], Dict[str, Any]] = {}
    excluded = 0
    for r in rows:
        corr, n = S.num(r.get("correlation")), int(r.get("sample_size") or 0)
        if corr is None or n < PEER_MIN_N:
            excluded += 1
            continue
        if r.get("symbol_follow") == entity:
            other, rel = r.get("symbol_lead"), "lags"
        elif r.get("symbol_lead") == entity:
            other, rel = r.get("symbol_follow"), "leads"
        else:
            continue
        k = (other, rel)
        if k not in best or abs(corr) > abs(S.num(best[k]["correlation"]) or 0):
            best[k] = r
    up, peers = [], []
    for (other, rel), r in best.items():
        lk = make_link(
            target={"id": other, "type": "symbol", "market": market},
            relation=rel, kind="statistical", metric="pearson_rho_lagged_15m_return",
            value=round(S.num(r["correlation"]), 3), n=int(r["sample_size"]),
            lag_minutes=int(r["lag_minutes"]), extra={"window": "14d", "bar": "15m"},
            as_of=r.get("computed_at"), source_table="agent_history.symbol_peer_links",
            validation=NOT_FORWARD_TESTED,
        )
        (up if rel == "lags" else peers).append(lk)
    return up, peers, excluded


# ─── 상위계층 → 종목 (cross_layer_links) ─────────────────────────────────

def cross_layer_validation(market: str, skill: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """시장 단위 전향 검증 — **링크 단위가 아니다**(scope 에 명시해 오독을 막는다)."""
    if not skill:
        return dict(NOT_FORWARD_TESTED)
    return {
        "status": "forward_tested",
        "scope": f"all stable cross-layer links in {market} (market-level, not this link alone)",
        "verdict": skill.get("verdict_hint"),
        "skill": S.num(skill.get("skill")),
        "t": S.num(skill.get("t_cluster")),
        "n_resolved": int(skill.get("n_resolved") or 0),
        "fire_days": int(skill.get("n_fire_days") or 0),
        "source": "agent_history.cross_layer_market_skill",
    }


def _lead_target(market: str, r: Dict[str, Any], macro_members: Dict[str, Dict[str, Any]],
                 structure: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    layer, lead = r.get("lead_layer"), str(r.get("lead_symbol") or "")
    if layer == "basket" and lead.startswith("basket:"):
        gid = lead[len("basket:"):]
        g = ((structure or {}).get("groups") or {}).get(gid)
        return group_target(market, gid, g)
    if layer == "macro":
        mm = macro_members.get(lead.upper()) or {}
        t = {"id": lead, "type": "macro_instrument", "category": r.get("lead_category") or mm.get("category")}
        if mm.get("name_ko"):
            t["name"] = mm["name_ko"]
        return t
    return {"id": lead, "type": "etf"}


def cross_layer_links(market: str, rows: List[Dict[str, Any]], validation: Dict[str, Any], *,
                      direction: str, macro_members: Dict[str, Dict[str, Any]],
                      structure: Optional[Dict[str, Any]] = None) -> Tuple[List, int]:
    """direction='follow' : 이 종목이 뒤따르는 상위계층 (entity lags target)
       direction='basket' : 이 그룹 바스켓이 앞서는 종목 (entity leads target)
    stable=true · n≥60 · 유한값만 (writer 의 FDR·반분 안정 통과분)."""
    out, excluded = [], 0
    for r in rows:
        corr, n = S.num(r.get("correlation")), int(r.get("sample_size") or 0)
        if corr is None or n < CROSS_LAYER_MIN_N or not r.get("stable"):
            excluded += 1
            continue
        if direction == "follow":
            target, rel = _lead_target(market, r, macro_members, structure), "lags"
        else:
            target, rel = {"id": r.get("follow_symbol"), "type": "symbol", "market": market}, "leads"
        out.append(make_link(
            target=target, relation=rel, kind="statistical", metric="pearson_rho_lagged_daily_return",
            value=round(corr, 3), n=n, lag_days=int(r.get("lag_days") or 0),
            extra={"p_value": _round_p(r.get("p_value")), "stable": True,
                   "window": f"{int(r.get('lookback_days') or 0)}d", "bar": "1d",
                   "lead_layer": r.get("lead_layer")},
            as_of=r.get("computed_at"), source_table="agent_history.cross_layer_links",
            validation=validation,
        ))
    return out, excluded


def _round_p(v: Any) -> Optional[float]:
    p = S.num(v)
    if p is None:
        return None
    return float(f"{p:.2g}") if p < 0.001 else round(p, 4)


# ─── 그룹↔그룹 상관 ──────────────────────────────────────────────────────

def sector_links(market: str, gid: str, rows: List[Dict[str, Any]],
                 structure: Optional[Dict[str, Any]] = None) -> Tuple[List, int, int]:
    """(links, NaN 수, 표본 미달 수)."""
    out, nan, low = [], 0, 0
    groups = (structure or {}).get("groups") or {}
    for r in rows:
        other = r["symbol_b"] if r.get("symbol_a") == gid else r.get("symbol_a")
        if S.is_nan_value(r.get("correlation")):
            nan += 1
            continue
        corr, n = S.num(r.get("correlation")), int(r.get("sample_size") or 0)
        if corr is None or n < SECTOR_MIN_N:
            low += 1
            continue
        out.append(make_link(
            target=group_target(market, other, groups.get(other)), relation="co_moves",
            kind="statistical", metric="pearson_rho_daily_group_return", value=round(corr, 3), n=n,
            extra={"window": f"{int(r.get('lookback_days') or 0)}d", "bar": r.get("interval") or "1d"},
            as_of=r.get("computed_at"), source_table="agent_history.sector_correlation_matrix",
            validation=NOT_FORWARD_TESTED,
        ))
    return out, nan, low


# ─── 정의형 (구조 그룹 소속) ──────────────────────────────────────────────

def _structure_source(gid: Optional[str] = None) -> str:
    return "market_structure_summary.json" + (f":groups.{gid}" if gid else ":groups")


def membership_link(market: str, gid: str, g: Dict[str, Any], updated_at: Optional[str],
                    member_raw_id: Optional[str] = None) -> Dict[str, Any]:
    """entity(종목) member_of 그룹."""
    w = (g.get("volume_weights") or {}).get(member_raw_id) if member_raw_id else None
    return make_link(
        target=group_target(market, gid, g), relation="member_of", kind="definitional",
        metric="basket_membership", value=S.num(w), n=None, as_of=updated_at,
        source_table=_structure_source(gid),
        extra={"value_meaning": "member volume weight within the group (null = not among weighted top members)"},
        target_observation=group_observation(g, updated_at),
    )


def member_links(market: str, gid: str, g: Dict[str, Any], members: List[Tuple[str, str, Optional[str]]],
                 updated_at: Optional[str], names: Optional[Dict] = None,
                 observations: Optional[Dict[str, Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """그룹 has_member 종목들 — 입력 순서 유지(호출부가 volume_weight 순으로 정렬)."""
    weights = g.get("volume_weights") or {}
    out = []
    for raw_id, key, nm_struct in members:
        t = symbol_target(market, key, names, raw_id=raw_id)
        if "name" not in t and nm_struct:
            t["name"] = nm_struct
        out.append(make_link(
            target=t, relation="has_member", kind="definitional", metric="basket_membership",
            value=S.num(weights.get(raw_id)), n=None, as_of=updated_at, source_table=_structure_source(gid),
            target_observation=signal_observation((observations or {}).get(key)),
        ))
    return out


def market_group_links(market: str, structure: Dict[str, Any], order: List[str]) -> List[Dict[str, Any]]:
    groups, upd = structure.get("groups") or {}, structure.get("updated_at")
    return [make_link(
        target=group_target(market, gid, groups[gid]), relation="has_member", kind="definitional",
        metric="structure_group", value=None, n=None, as_of=upd, source_table=_structure_source(),
        target_observation=group_observation(groups[gid], upd),
    ) for gid in order]


# ─── 뉴스 태그 ──────────────────────────────────────────────────────────

_ITEM_KIND = {"dart": "disclosure", "krx_investor_flow": "investor_flow"}


def news_links(rows: List[Dict[str, Any]], *, metric: str, scope: str,
               via: Optional[str] = None) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        title = (r.get("title_en") or r.get("title") or "").strip()
        t: Dict[str, Any] = {
            "id": f"news:{r.get('id')}", "type": "news",
            "item_kind": _ITEM_KIND.get(str(r.get("source") or "").lower(), "news"),
            "title": title[:160], "source": r.get("source"),
            "published_at": iso_from_any(r.get("published_at")),
        }
        for k in ("sentiment_label", "impact_level", "event_type"):
            if r.get(k):
                t[k] = r[k]
        out.append(make_link(
            target=t, relation="mentioned_in", kind="tag", metric=metric, value=None, n=None,
            as_of=r.get("published_at"), source_table="external_context.news_events", scope=scope,
            extra={"via": via} if via else None,
        ))
    return out


# ─── 거시 가설 (influence map) ───────────────────────────────────────────

def macro_hypothesis_links(market: str, cells: Dict[str, Dict[str, Any]],
                           tuned_at: Dict[str, Any]) -> List[Dict[str, Any]]:
    """entity(시장) affected_by 거시 카테고리 — 엔진이 쓰는 사전값/튜닝값(가설)이지 측정된 인과가 아니다."""
    out = []
    for cat, cell in cells.items():
        eff = cell.get("effective") or {}
        tuned = eff.get("source") == "backtest_tuning"
        out.append(make_link(
            target={"id": cat, "type": "macro_category"}, relation="affected_by", kind="hypothesis",
            metric="prior_sensitivity", value=S.num(eff.get("sensitivity")),
            n=int(eff.get("sample_count") or 0) if tuned else None,
            extra={"lag_hours": S.num(eff.get("lag_hours")), "value_source": eff.get("source")},
            as_of=tuned_at.get(cat) if tuned else None,
            source_table="market_global.backtest_tuning" if tuned else "market/global_regime/profiles.py",
            validation={"status": "tracked_separately", "tool": "get_prediction_accuracy",
                        "note": "Forward hit rates of macro-category regime calls are reported per "
                                "(category, market, lag) by get_prediction_accuracy."},
        ))
    return out
