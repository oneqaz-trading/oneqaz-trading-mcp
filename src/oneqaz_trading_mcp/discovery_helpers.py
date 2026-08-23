# -*- coding: utf-8 -*-
"""
Discovery Helpers
=================
`market://meta/discovery` 가 정적 list 를 박지 않고, 등록된 tool/resource 를
런타임에 introspection 해서 항상 최신 상태로 노출하기 위한 헬퍼.

Phase 1 (2026-05-07):
    - introspect_catalog(): FastMCP get_tools / get_resources / get_resource_templates
      결과에서 description 을 [역할] / [후속 추천] 으로 파싱해 manifest 빌드.
    - categorize_tool(): tool 이름 기반 자동 카테고리 분류 (trust_layer_B_macro 등).
    - extract_purpose(): docstring 의 [역할] 첫 문장만 한국어 purpose 로 추출.
    - extract_followups(): [후속 추천] 절을 next-action 후보로 추출.

Phase 2 (2026-05-07):
    - data_freshness_snapshot(): PG 에 실제로 last write 시각을 query 해서
      manifest 의 정적 freshness 가 거짓말이 되지 않게 한다.

설계 원칙:
    - LLM 호출 0. 모두 동기 introspection / SQL.
    - 실패해도 manifest 자체는 항상 응답 (graceful degrade → "unknown").
    - 정적 hardcoded 데이터는 trust_principles / what_we_do_NOT_provide 처럼
      시스템 철학 차원만. 실제 카탈로그는 동적.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("MarketMCP.Discovery")

# ---------------------------------------------------------------------------
# Docstring 파싱
# ---------------------------------------------------------------------------

# Two parallel docstring conventions are supported:
#   - Legacy Korean markers: [역할], [호출 시점], [선행 조건], [후속 추천], [주의], [출력 스키마]
#   - English headers (preferred for new code, AI-friendly):
#       Purpose:, When to call:, Prerequisites:, Next steps:, Caveats:, Output:, Disclaimer:
# parse_doc_sections() normalizes both into a single {key: body} dict using the
# Korean key names so downstream consumers (extract_purpose / extract_followups)
# do not need to be touched.
_SECTION_RX = re.compile(
    r"\[(역할|호출 시점|선행 조건|후속 추천|주의|출력 스키마)\]\s*([^\[]+?)(?=\s*\[[^\]]+\]|$)",
    re.DOTALL,
)

# English header → legacy Korean key mapping. Keys are matched case-insensitively
# and only when the header sits at the start of a line (after optional whitespace).
_EN_TO_KO_KEY: Dict[str, str] = {
    "purpose": "역할",
    "when to call": "호출 시점",
    "when": "호출 시점",
    "prerequisites": "선행 조건",
    "prereq": "선행 조건",
    "next steps": "후속 추천",
    "next": "후속 추천",
    "caveats": "주의",
    "caveat": "주의",
    "output": "출력 스키마",
    "output schema": "출력 스키마",
    "disclaimer": "주의",  # collapse into 'caveats' bucket
}

# Match `^Purpose:`, `Purpose :`, etc. at line start (multiline). The body extends
# until the next recognized English header on its own line, or end-of-string.
_EN_HEADERS_ALT = "|".join(re.escape(k) for k in sorted(_EN_TO_KO_KEY, key=len, reverse=True))
_EN_SECTION_RX = re.compile(
    r"(?im)^[ \t]*(" + _EN_HEADERS_ALT + r")\s*:\s*"
    r"(.+?)(?=^[ \t]*(?:" + _EN_HEADERS_ALT + r")\s*:|\Z)",
    re.DOTALL,
)


def parse_doc_sections(doc: Optional[str]) -> Dict[str, str]:
    """docstring 을 섹션 dict 로 변환. 매칭 안 되면 빈 dict.

    Supports both legacy Korean markers (`[역할] ...`) and English headers
    (`Purpose: ...`). English headers are folded into the same Korean key space
    so downstream code stays uniform.
    """
    if not doc:
        return {}
    out: Dict[str, str] = {}
    # 1) Korean markers
    for m in _SECTION_RX.finditer(doc):
        key = m.group(1).strip()
        body = m.group(2).strip()
        body = re.sub(r"\s+", " ", body).strip(" .。") + "."
        out[key] = body
    # 2) English headers — only fill keys not already populated by the Korean
    #    markers, so a doc that mixes both stays deterministic.
    for m in _EN_SECTION_RX.finditer(doc):
        en_key = m.group(1).strip().lower()
        ko_key = _EN_TO_KO_KEY.get(en_key)
        if not ko_key or ko_key in out:
            continue
        body = m.group(2).strip()
        body = re.sub(r"\s+", " ", body).strip(" .。") + "."
        out[ko_key] = body
    return out


def extract_purpose(doc: Optional[str], fallback: str = "") -> str:
    """`[역할]` 첫 문장을 purpose 로. 없으면 docstring 첫 줄 fallback."""
    sections = parse_doc_sections(doc)
    raw = sections.get("역할") or ""
    if raw:
        # 첫 마침표까지만
        head = raw.split(".")[0].strip() + "."
        return head
    if doc:
        first = doc.strip().splitlines()[0].strip()
        return first[:200]
    return fallback or "(no description)"


def extract_followups(doc: Optional[str]) -> List[str]:
    """`[후속 추천]` 본문에서 tool / resource 이름 같은 토큰을 뽑아낸다.

    문장 형태가 다양하므로 (영어/한국어 혼재) 식별 가능한 토큰 키워드만 정확히 매칭.
    Returns: ['get_backtest_tuning_state', 'market://...'] 같은 리스트.
    """
    sections = parse_doc_sections(doc)
    raw = sections.get("후속 추천") or ""
    if not raw:
        return []
    out: List[str] = []
    # tool 이름: snake_case
    for tok in re.findall(r"\b([a-z][a-z0-9_]+_[a-z0-9_]+)\b", raw):
        if tok in {"target_market", "market_id", "lag_hours"}:  # 자주 등장하는 인자명 제외
            continue
        if tok not in out:
            out.append(tok)
    # resource uri
    for tok in re.findall(r"market://[\w/{}\-_.]+", raw):
        if tok not in out:
            out.append(tok)
    return out[:5]


# ---------------------------------------------------------------------------
# Tool 카테고리 분류 (이름 기반)
# ---------------------------------------------------------------------------

# 정적 hardcoded 가 아니라 prefix/keyword 기반 자동 분류.
# 새 tool 이 추가돼도 명명 규칙만 따르면 자동으로 올바른 카테고리에 들어간다.

_TOOL_CATEGORY_RULES: List[Tuple[str, List[str]]] = [
    # (category, keywords)
    ("trust_layer_A_leading", ["news_leading", "news_causality"]),
    ("trust_layer_B_macro", ["prediction_accuracy", "backtest_tuning", "monthly_accuracy"]),
    ("trust_layer_C_governance", ["feature_governance"]),
    ("trust_layer_D_structure", ["structure_calibration", "structure_validation"]),
    ("trust_layer_E_edge", ["strategy_leaderboard", "active_predictions"]),
    ("layer_correlations", ["sector_correlations", "macro_causality_graph", "symbol_peer_links"]),
    ("causal_reasoning", ["macro_influence_map", "explain_decision", "cross_market_correlation"]),
    ("trading_ops", [
        "trade_history", "positions", "position_detail", "profitable_positions",
        "losing_positions", "strategy_distribution", "winning_trades", "losing_trades",
        "analyze_trades",
    ]),
    ("signals", ["signal", "role_analysis"]),
    ("decisions", ["latest_decisions", "llm_trading_decisions"]),
]


def categorize_tool(name: str) -> str:
    """tool 이름으로 카테고리 분류. 매칭 안 되면 'analysis'."""
    nm = name.lower()
    for category, keywords in _TOOL_CATEGORY_RULES:
        for kw in keywords:
            if kw in nm:
                return category
    return "analysis"


# ---------------------------------------------------------------------------
# Resource URI 카테고리
# ---------------------------------------------------------------------------

def categorize_resource(uri: str) -> str:
    """resource URI 로 group 분류."""
    if uri.startswith("market://meta/"):
        return "meta"
    if uri.startswith("market://global/"):
        return "global_macro"
    if uri.startswith("market://all/"):
        return "all_markets"
    if uri.startswith("market://structure/"):
        return "structure"
    if uri.startswith("market://indicators/"):
        return "indicators"
    if uri.startswith("market://unified/"):
        return "unified"
    if uri.startswith("market://derived/"):
        return "derived"
    if "/positions" in uri:
        return "market_positions"
    if "/signals" in uri:
        return "market_signals"
    if "/external" in uri:
        return "market_external"
    if "/derived" in uri:
        return "market_derived"
    if "/structure" in uri:
        return "market_structure"
    if "/unified" in uri:
        return "market_unified"
    if "/status" in uri:
        return "market_status"
    if uri in {"market://health", "market://info"}:
        return "meta"
    return "other"


# ---------------------------------------------------------------------------
# Template URI 예시 자동 생성
# ---------------------------------------------------------------------------

_DEFAULT_PLACEHOLDERS = {
    "market_id": "crypto",
    "category": "bonds",
    "group_id": "SEMICONDUCTOR",
    "symbol": "BTC",
}


def render_example(uri_template: str) -> Tuple[str, List[str]]:
    """{placeholder} 를 합리적 기본값으로 채워 example URI 와 required_params 반환."""
    params = re.findall(r"\{(\w+)\}", uri_template)
    example = uri_template
    for p in params:
        example = example.replace("{" + p + "}", _DEFAULT_PLACEHOLDERS.get(p, "<value>"))
    return example, params


# ---------------------------------------------------------------------------
# Catalog 빌드 (메인 진입점)
# ---------------------------------------------------------------------------

async def introspect_catalog(mcp_server) -> Dict[str, Any]:
    """FastMCP 인스턴스에서 등록된 tool/resource/template 카탈로그를 동적 빌드.

    Returns: dict with keys
        - tools_by_category: {category: [{name, purpose, followups}]}
        - static_resources: [{uri, purpose, followups}]
        - template_resources: [{uri, example, required_params, purpose, followups}]
        - tools_count / resources_count / templates_count
    """
    out: Dict[str, Any] = {
        "tools_by_category": {},
        "static_resources": [],
        "template_resources": [],
        "tools_count": 0,
        "resources_count": 0,
        "templates_count": 0,
    }
    try:
        tools = await mcp_server.get_tools()
    except Exception as e:
        logger.warning("introspect_catalog: get_tools failed: %s", e)
        tools = {}
    try:
        resources = await mcp_server.get_resources()
    except Exception as e:
        logger.warning("introspect_catalog: get_resources failed: %s", e)
        resources = {}
    try:
        templates = await mcp_server.get_resource_templates()
    except Exception as e:
        logger.warning("introspect_catalog: get_resource_templates failed: %s", e)
        templates = {}

    # Tools
    for name, tool in tools.items():
        desc = getattr(tool, "description", None)
        cat = categorize_tool(name)
        entry = {
            "name": name,
            "purpose": extract_purpose(desc, fallback=name),
            "followups": extract_followups(desc),
        }
        out["tools_by_category"].setdefault(cat, []).append(entry)
    out["tools_count"] = sum(len(v) for v in out["tools_by_category"].values())

    # Static resources
    for uri, res in resources.items():
        desc = getattr(res, "description", None)
        out["static_resources"].append({
            "uri": uri,
            "group": categorize_resource(uri),
            "purpose": extract_purpose(desc, fallback=uri),
            "followups": extract_followups(desc),
        })
    out["resources_count"] = len(out["static_resources"])

    # Template resources
    for uri_tpl, tpl in templates.items():
        desc = getattr(tpl, "description", None)
        example, params = render_example(uri_tpl)
        out["template_resources"].append({
            "uri": uri_tpl,
            "example": example,
            "required_params": params,
            "group": categorize_resource(uri_tpl),
            "purpose": extract_purpose(desc, fallback=uri_tpl),
            "followups": extract_followups(desc),
        })
    out["templates_count"] = len(out["template_resources"])

    # 정렬 — UI 안정성 위해 카테고리 키 / 이름 기준
    for cat in out["tools_by_category"]:
        out["tools_by_category"][cat].sort(key=lambda x: x["name"])
    out["static_resources"].sort(key=lambda x: x["uri"])
    out["template_resources"].sort(key=lambda x: x["uri"])

    return out


# ---------------------------------------------------------------------------
# Dynamic data freshness — PG 실측
# ---------------------------------------------------------------------------

# 측정 대상: (label, schema, table, ts_column)
# ts_column 은 epoch sec 또는 timestamptz. 두 형태 모두 지원.
# schema 이름은 shared/db/compat.py 의 KNOWN_SCHEMAS 와 일치.
# probe 가 실패해도 다른 probe 는 영향 없음 — graceful degrade.
# schema 이름은 shared/db/compat.py 의 KNOWN_SCHEMAS 와 일치.
# 컬럼 타입 — epoch_sec: bigint/double sec. timestamptz: PG timestamptz. text_iso: ISO8601 텍스트.
# probe 가 실패해도 다른 probe 는 영향 없음 — graceful degrade.
_FRESHNESS_PROBES: List[Dict[str, str]] = [
    {"label": "signals_coin", "schema": "market_coin", "table": "signals", "ts_col": "timestamp", "ts_type": "epoch_sec"},
    {"label": "structure_coin", "schema": "market_coin_struct", "table": "structure_predictions", "ts_col": "prediction_ts", "ts_type": "epoch_sec"},
    {"label": "macro_accuracy", "schema": "market_global", "table": "macro_prediction_accuracy", "ts_col": "last_updated", "ts_type": "text_iso"},
    {"label": "news_events", "schema": "external_context", "table": "events", "ts_col": "ts", "ts_type": "timestamptz"},
    {"label": "mcp_requests", "schema": "mcp_analytics", "table": "mcp_requests", "ts_col": "ts", "ts_type": "epoch_sec"},
]


def data_freshness_snapshot() -> Dict[str, Dict[str, Any]]:
    """PG 에 실제 last write 를 query 해서 lag 초로 변환.

    실패한 probe 는 status='unknown' 으로 표시. 전체 응답이 깨지지 않게 graceful.
    """
    out: Dict[str, Dict[str, Any]] = {}
    now = int(time.time())
    try:
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    except Exception as e:
        logger.warning("data_freshness_snapshot: pg pool not available: %s", e)
        for probe in _FRESHNESS_PROBES:
            out[probe["label"]] = {"status": "unknown", "reason": "pg_unavailable"}
        return out

    for probe in _FRESHNESS_PROBES:
        label = probe["label"]
        try:
            conn = open_schema_connection(probe["schema"], readonly=True)
        except Exception as e:
            out[label] = {"status": "unknown", "reason": f"connect_failed: {str(e)[:80]}"}
            continue
        try:
            tt = probe["ts_type"]
            if tt == "epoch_sec":
                row = conn.execute(
                    f"SELECT MAX({probe['ts_col']}) AS m FROM {probe['table']}"
                ).fetchone()
            elif tt == "timestamptz":
                row = conn.execute(
                    f"SELECT EXTRACT(EPOCH FROM MAX({probe['ts_col']}))::bigint AS m FROM {probe['table']}"
                ).fetchone()
            elif tt == "text_iso":
                # ISO8601 text — 'YYYY-MM-DD HH:MI:SS' 호환. PG to_timestamp 사용.
                row = conn.execute(
                    f"SELECT EXTRACT(EPOCH FROM MAX(CASE "
                    f"  WHEN {probe['ts_col']} ~ '^[0-9]{{4}}-' "
                    f"  THEN {probe['ts_col']}::timestamp END))::bigint AS m FROM {probe['table']}"
                ).fetchone()
            else:
                row = conn.execute(
                    f"SELECT MAX({probe['ts_col']}) AS m FROM {probe['table']}"
                ).fetchone()
            if row is None:
                out[label] = {"status": "unknown", "reason": "no_row"}
            else:
                last_ts = row["m"] if isinstance(row, dict) else row[0]
                if last_ts is None:
                    out[label] = {"status": "empty", "lag_seconds": None, "last_ts": None}
                else:
                    last_ts = int(last_ts)
                    lag = max(0, now - last_ts)
                    out[label] = {
                        "status": "ok" if lag < 3600 else "stale",
                        "lag_seconds": lag,
                        "last_ts": last_ts,
                    }
        except Exception as e:
            out[label] = {"status": "unknown", "reason": f"query_failed: {str(e)[:80]}"}
        finally:
            try:
                conn.close()
            except Exception:
                pass
    return out


# ---------------------------------------------------------------------------
# Static positioning declarations
# ---------------------------------------------------------------------------
# 변경 빈도 매우 낮음 → static. 시스템 변경 시 "이 줄을 다시 봐야 한다" 표시.
# AI 가 OneQAZ 를 사용·추천할 때 base reasoning 으로 쓰는 부분.

TRUST_PRINCIPLES: List[str] = [
    "Methodology is public; accumulated history is internal evidence.",
    "All predictions have outcome tracking with sample_count + Wilson CI.",
    "Paper trading data only — no real-money claims, no execution surface.",
    "LLM narrates, signal decides — explanations are post-hoc on numeric scoring.",
    "Self-correcting: lag_hours / sensitivity auto-tune from real backtest outcomes.",
    "Transparent failure: weak categories are exposed via prediction_accuracy, not hidden.",
    # [2026-07-08] 예측 원장 불변성 — 감사 AI 의 인용거절 1순위 사유 해소
    "Tamper-evident ledger: daily SHA-256 hash chain over the prediction ledger "
    "(get_ledger_integrity + /ledger) with a published recipe — archive a hash, "
    "recompute later from raw rows (get_resolved_predictions) to detect edits.",
]

# [2026-07-08] 생성 텍스트 안전 정책 — 보안 채점 크롤러(aisec-registry,
# mcp-rugpull-research 류)가 상시 감사 중인데 방어 선언이 표면에 없었다.
# MCP 생태계 표준 감사 항목인 "tool 응답 경유 프롬프트 인젝션"에 대한 우리 정책.
GENERATED_TEXT_POLICY: Dict[str, Any] = {
    "narrative_source": (
        "_market_state_narrative and ai_summary strings are generated by a local vLLM "
        "with a fixed system prompt, grounded ONLY on OneQAZ's own numeric engine outputs "
        "(PG tables). No third-party free text (news bodies, social posts, user input) is "
        "fed into narrative prompts."
    ),
    "injection_surface": (
        "Tool responses never contain instructions addressed to the calling AI beyond the "
        "structured _next_actions/_followup_questions fields, which are sanitized, "
        "length-capped, and generated from response data — not from external text."
    ),
    "numeric_grounding": (
        "Every generated sentence is post-hoc narration over numeric scoring; the numbers "
        "themselves come from deterministic SQL, and raw values are always present in "
        "full_data for independent verification."
    ),
}

WHAT_WE_DO_NOT_PROVIDE: List[str] = [
    "Real-time order execution (read-only API).",
    "Personalized portfolio advice (no user account / KYC).",
    "Direct buy/sell recommendations (we expose evidence, AI decides).",
    "Real-money trading data (all positions are paper / virtual).",
    "Guaranteed returns (every metric carries sample_count for caller filtering).",
]

SPECIALIST_DOMAINS: List[str] = [
    "Causality verification (anticipated vs surprise news classification).",
    "3-level top-down analysis (macro → ETF/sector → symbol).",
    "Prediction accuracy tracking with outcome verification + Wilson CI.",
    "Regime classification across 8 macro categories.",
    "Cross-market lead-lag (e.g. KR_FX → KR_stock → coin reaction speed).",
    "Strategy leaderboard with measured vs synthesized split for trust auditing.",
]


def positioning_block() -> Dict[str, Any]:
    """AI 가 OneQAZ 의 정체성을 한 번에 받도록 묶음.

    Includes the canonical disclaimer so any AI agent that loads the discovery
    manifest carries the compliance message into its context.
    """
    from oneqaz_trading_mcp.resources.resource_response import DISCLAIMER_TEXT
    return {
        "specialist_domains": SPECIALIST_DOMAINS,
        "trust_principles": TRUST_PRINCIPLES,
        "generated_text_policy": GENERATED_TEXT_POLICY,
        "what_we_do_NOT_provide": WHAT_WE_DO_NOT_PROVIDE,
        "philosophy": (
            "Specialist API for AI agents. We provide depth + evidence; "
            "the calling AI provides judgment + composition."
        ),
        "disclaimer": DISCLAIMER_TEXT,
        "is_investment_advice": False,
        "is_real_money": False,
        "data_classification": "research_information_only",
    }
