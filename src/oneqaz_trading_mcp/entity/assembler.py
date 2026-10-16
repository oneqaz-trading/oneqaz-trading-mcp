# -*- coding: utf-8 -*-
"""
엔티티 응답 조립·점검 — 공통 스키마(역피라미드) + probe 점검 함수
==================================================================
최상위 키 순서 = 중요도 순서(ED11). 앞이 살아남는다:
    headline · meta · entity · resolution · self · upstream · peers · downstream · news · gaps
    → 끝에 컴플라이언스 꼬리(disclaimer · is_investment_advice · data_classification ·
      request_id · timestamp · _source · _next_actions) — `resource_response.wrap_entity_response`

`check_response()` 는 req §8 probe(E1·E3·E4·E5·E6·E10·E11·E12)의 결정론 점검이다.
단위 테스트(역검증 — 결함 입력에서 먼저 실패하는지)와 라이브 probe 가 같은 함수를 쓴다.
"""

from __future__ import annotations

import logging
import math
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from oneqaz_trading_mcp.compliance import (
    DISCLAIMER_SHORT_EN, FORBIDDEN_CLAIM_TERMS_EN, FORBIDDEN_CLAIM_TERMS_KO, INTERPRETATION,
    RELATIONS_BY_KIND,
)
from oneqaz_trading_mcp.entity.links import sort_links

logger = logging.getLogger("MarketMCP")

SCHEMA_VERSION = "entity_context.v1"
SECTIONS = ("upstream", "peers", "downstream", "news")
BODY_KEYS = ("headline", "meta", "entity", "resolution", "self") + SECTIONS + ("gaps",)
TAIL_KEYS = ("disclaimer", "is_investment_advice", "data_classification", "request_id", "timestamp", "_source")
GAP_REASONS = ("not_linked", "data_unavailable", "insufficient_sample", "data_quality", "stale", "not_applicable")
HEADLINE_KEYS = ("claim", "as_of", "basis", "horizon", "calibration", "source")
EVIDENCE_KEYS = ("kind", "metric", "value", "n", "as_of", "source_table")


def gap(section: str, reason: str, detail: str) -> Dict[str, str]:
    if reason not in GAP_REASONS:
        raise ValueError(f"unknown gap reason {reason!r}")
    return {"section": section, "reason": reason, "detail": detail}


def headline_item(claim: str, *, as_of: Optional[str], basis: str, horizon: str, calibration: str) -> Dict[str, Any]:
    return {"claim": claim, "as_of": as_of, "basis": basis, "horizon": horizon,
            "calibration": calibration, "source": "OneQAZ"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def assemble(*, tool: str, entity: Optional[Dict[str, Any]], resolution: Dict[str, Any],
             self_block: Optional[Dict[str, Any]], sections: Dict[str, List[Dict[str, Any]]],
             totals: Optional[Dict[str, int]] = None, limits: Dict[str, int],
             gaps: List[Dict[str, str]], headline: List[Dict[str, Any]],
             data_as_of: Optional[str], session_state: Optional[str], basis: Optional[str],
             extra_interpretation: Optional[List[str]] = None,
             order_note: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """섹션 정렬·잘림·개수 표기, 빈 섹션 ↔ gaps 대응 보장, meta 조립. 반환 dict 의 키 순서가 응답 순서다."""
    out_sections: Dict[str, List[Dict[str, Any]]] = {}
    counts: Dict[str, Dict[str, Any]] = {}
    for s in SECTIONS:
        links = sort_links(list(sections.get(s) or []))
        total = max(len(links), int((totals or {}).get(s, 0)))
        lim = max(0, int(limits.get(s, 8)))
        kept = links[:lim]
        out_sections[s] = kept
        counts[s] = {"total": total, "returned": len(kept), "truncated": len(kept) < total}

    gaps = list(gaps)
    covered = {g["section"] for g in gaps}
    for s in SECTIONS:
        if not out_sections[s] and s not in covered:
            # 조립 단계의 안전망 — 원래 context 가 이유를 정해 넣어야 한다(E6). 이유를 지어내지 않는다.
            logger.warning("entity assemble: %s/%s 빈 섹션에 gap 이 없어 일반 사유로 채움", tool, s)
            gaps.append(gap(s, "data_unavailable", "No tracked source returned links for this section."))

    any_links = any(out_sections[s] for s in SECTIONS)
    interp: List[str] = []
    if self_block and self_block.get("signal"):
        interp += [INTERPRETATION["signal_score"], INTERPRETATION["confidence_calibrated"],
                   INTERPRETATION["regime_label"]]
    if any_links:
        interp += [INTERPRETATION["relation"], INTERPRETATION["evidence_kind"]]
        if any("validation" in lk["evidence"] for s in SECTIONS for lk in out_sections[s]):
            interp.append(INTERPRETATION["validation"])
        if any(lk.get("scope") == "market_wide" for s in SECTIONS for lk in out_sections[s]):
            interp.append(INTERPRETATION["market_wide"])
    interp += list(extra_interpretation or [])
    if gaps:
        interp.append(INTERPRETATION["gaps"])

    served_at = _now_iso()
    age = None
    if data_as_of:
        try:
            age = int(datetime.now(timezone.utc).timestamp()
                      - datetime.fromisoformat(data_as_of.replace("Z", "+00:00")).timestamp())
        except ValueError:
            age = None
    meta: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "tool": tool,
        "data_type": "entity_context",
        "execution": None,
        "served_at": served_at,
        "data_as_of": data_as_of,
        "data_age_seconds": age,
        "session_state": session_state,
        "basis": basis,
        "sections": counts,
    }
    if order_note:
        meta["order_by"] = order_note
    meta["interpretation"] = interp
    meta["disclaimer"] = DISCLAIMER_SHORT_EN

    resp: Dict[str, Any] = {"headline": headline, "meta": meta, "entity": entity, "resolution": resolution,
                            "self": self_block}
    for s in SECTIONS:
        resp[s] = out_sections[s]
    resp["gaps"] = gaps
    return resp


def unresolved(tool: str, resolution: Dict[str, Any], what: str) -> Dict[str, Any]:
    """ambiguous / not_covered — 본문은 비우고 후보·지원 범위만 (추측하지 않는다)."""
    status = resolution.get("status")
    n = len(resolution.get("candidates") or [])
    if status == "ambiguous":
        claim = (f"'{resolution.get('input')}' matches {resolution.get('candidates_total', n)} tracked "
                 f"entities; no {what} context is returned until one is chosen from resolution.candidates")
        detail = "Entity not resolved (ambiguous input) — choose a candidate and call again."
    else:
        claim = f"'{resolution.get('input')}' is not a {what} OneQAZ tracks; see resolution.note"
        detail = "Entity not covered by OneQAZ."
    head = [headline_item(claim, as_of=_now_iso(), basis="not_applicable", horizon="not_applicable",
                          calibration="not_applicable")]
    gaps = [gap(s, "not_applicable", detail) for s in SECTIONS]
    return assemble(tool=tool, entity=None, resolution=resolution, self_block=None,
                    sections={}, limits={s: 0 for s in SECTIONS}, gaps=gaps, headline=head,
                    data_as_of=None, session_state=None, basis=None)


# ─── 점검 (probe) ───────────────────────────────────────────────────────

_TERM_RES = [re.compile(rf"(?<![a-z]){re.escape(t)}(?![a-z])", re.IGNORECASE) for t in FORBIDDEN_CLAIM_TERMS_EN]


def _finite(v: Any) -> bool:
    try:
        return v is not None and math.isfinite(float(v))
    except (TypeError, ValueError):
        return False


def check_response(resp: Dict[str, Any], *, min_n: Optional[Dict[str, int]] = None) -> List[str]:
    """위반 목록(빈 리스트 = 통과). E1·E3·E4·E5·E6·E10·E11·E12.

    min_n: source_table → 최소 n (E3). 기본 = 원천 writer 저장 기준.
    """
    from oneqaz_trading_mcp.entity.links import CROSS_LAYER_MIN_N, PEER_MIN_N, SECTOR_MIN_N
    min_n = min_n or {
        "agent_history.symbol_peer_links": PEER_MIN_N,
        "agent_history.cross_layer_links": CROSS_LAYER_MIN_N,
        "agent_history.sector_correlation_matrix": SECTOR_MIN_N,
    }
    v: List[str] = []
    if resp.get("error") is True:
        return v  # 에러 envelope 은 이 점검 대상이 아니다
    keys = list(resp.keys())
    body = [k for k in keys if k in BODY_KEYS]
    if tuple(body) != BODY_KEYS:
        v.append(f"E4 body key order {body} != {list(BODY_KEYS)}")
    if keys[: len(BODY_KEYS)] != list(BODY_KEYS):
        v.append(f"E4 body keys must come first: {keys[:len(BODY_KEYS)]}")
    for k in TAIL_KEYS:
        if k not in resp:
            v.append(f"E4 missing compliance key {k}")

    heads = resp.get("headline") or []
    if not heads:
        v.append("E5 headline empty")
    for i, h in enumerate(heads):
        for k in HEADLINE_KEYS:
            if not h.get(k):
                v.append(f"E5 headline[{i}] missing {k}")
        claim = str(h.get("claim") or "")
        for rx in _TERM_RES:
            if rx.search(claim):
                v.append(f"E11 headline[{i}] forbidden term {rx.pattern!r}: {claim[:80]}")
        for t in FORBIDDEN_CLAIM_TERMS_KO:
            if t in claim:
                v.append(f"E11 headline[{i}] forbidden term {t!r}: {claim[:80]}")

    gaps = resp.get("gaps") or []
    gap_sections = {g.get("section") for g in gaps}
    for g in gaps:
        if g.get("reason") not in GAP_REASONS:
            v.append(f"E6 unknown gap reason {g.get('reason')!r}")
    for s in SECTIONS:
        links = resp.get(s)
        if not isinstance(links, list):
            v.append(f"E4 section {s} is not a list")
            continue
        if not links and s not in gap_sections:
            v.append(f"E6 empty section {s} has no gap")
        seen_wide = False
        for j, lk in enumerate(links):
            ev = lk.get("evidence") or {}
            for k in EVIDENCE_KEYS:
                if k not in ev:
                    v.append(f"E1 {s}[{j}] evidence missing {k}")
            kind, rel = ev.get("kind"), lk.get("relation")
            if rel not in RELATIONS_BY_KIND.get(kind, ()):
                v.append(f"E10 {s}[{j}] relation {rel!r} not allowed for kind {kind!r}")
            if kind == "statistical":
                if not _finite(ev.get("value")):
                    v.append(f"E3 {s}[{j}] non-finite statistical value {ev.get('value')!r}")
                need = min_n.get(ev.get("source_table"), 1)
                if not isinstance(ev.get("n"), int) or ev["n"] < need:
                    v.append(f"E3 {s}[{j}] n={ev.get('n')!r} < {need}")
                if not ev.get("as_of"):
                    v.append(f"E1 {s}[{j}] statistical link without as_of")
            if lk.get("scope") == "market_wide":
                seen_wide = True
            elif seen_wide:
                v.append(f"E12 {s}[{j}] direct link after a market_wide link")
        if seen_wide and s not in gap_sections:
            v.append(f"E12 section {s} has market_wide items without a gap explaining the missing direct link")
    return v
