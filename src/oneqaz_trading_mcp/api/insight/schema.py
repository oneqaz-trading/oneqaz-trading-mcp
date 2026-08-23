from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ExplanationEntity(BaseModel):
    market: str = "unknown"
    symbol: Optional[str] = None
    interval: Optional[str] = None
    entity_type: str = "market"


class ExplanationDriver(BaseModel):
    primary: str = "unknown"
    secondary: Optional[str] = None
    confidence: float = 0.5


class ExplanationLeadLag(BaseModel):
    lead_entity: Optional[str] = None
    lagging_entity: Optional[str] = None
    lead_lag_minutes: Optional[float] = None


class ExplanationAssessment(BaseModel):
    driver: ExplanationDriver = Field(default_factory=ExplanationDriver)
    time_phase: str = "event"
    relation_status: str = "watching"
    movement_classification: str = "unconfirmed"
    news_timing: str = "no_news_detected"
    lead_lag: Optional[ExplanationLeadLag] = None
    explanation_status: str = "candidate"


class ExplanationSignalKey(BaseModel):
    symbol: str
    interval: Optional[str] = None
    timestamp: Optional[int] = None


class ExplanationEvidence(BaseModel):
    source_event_ids: List[str] = Field(default_factory=list)
    related_relation_ids: List[str] = Field(default_factory=list)
    signal_keys: List[ExplanationSignalKey] = Field(default_factory=list)
    market_context_refs: Dict[str, Any] = Field(default_factory=dict)
    state_refs: Dict[str, Any] = Field(default_factory=dict)
    memory_refs: Dict[str, Any] = Field(default_factory=dict)
    confidence: float = 0.5


class ExplanationSummary(BaseModel):
    headline: str
    why_text: str
    short_reason: Optional[str] = None
    bullet_points: List[str] = Field(default_factory=list)
    market_details: Dict[str, Any] = Field(default_factory=dict)
    uncertainty_flags: List[str] = Field(default_factory=list)
    risk_assessment: Dict[str, Any] = Field(default_factory=dict)


class ProvenanceLLM(BaseModel):
    model: str
    model_version: Optional[str] = None
    prompt_version: Optional[str] = None


class ProvenanceV1(BaseModel):
    fact_version: str = "1.0"
    schema_version: str = "1.0"
    llm: Optional[ProvenanceLLM] = None
    generated_at: Optional[str] = None
    source: Dict[str, Any] = Field(default_factory=dict)
    note: Optional[str] = None


class ExplanationPayloadV1(BaseModel):
    schema_version: str = "1.0"
    entity: ExplanationEntity
    as_of_time: str
    explanation_type: str = "snapshot_explanation"
    assessment: ExplanationAssessment = Field(default_factory=ExplanationAssessment)
    evidence: ExplanationEvidence = Field(default_factory=ExplanationEvidence)
    summary: ExplanationSummary
    provenance: ProvenanceV1


class FactResponse(BaseModel):
    snapshot_id: str
    created_at: str
    scope: str = "all"
    fact: Dict[str, Any]


class ExplainResponse(BaseModel):
    snapshot_id: str
    explain: Optional[ExplanationPayloadV1] = None
    provenance: ProvenanceV1
    generated_at: Optional[str] = None


class InsightResponse(BaseModel):
    snapshot_id: str
    created_at: str
    scope: str = "all"
    fact: Dict[str, Any]
    explain: Optional[Dict[str, Any]] = None
    provenance: Optional[Dict[str, Any]] = None


def default_provenance(
    *,
    fact_version: str = "1.0",
    note: Optional[str] = None,
    llm: Optional[Dict[str, Any]] = None,
    source: Optional[Dict[str, Any]] = None,
) -> ProvenanceV1:
    return ProvenanceV1(
        fact_version=fact_version,
        schema_version="1.0",
        llm=ProvenanceLLM.model_validate(llm) if llm else None,
        generated_at=now_iso(),
        source=source or {},
        note=note,
    )


def coerce_explanation_payload(
    explain: Dict[str, Any] | ExplanationPayloadV1,
    provenance: Dict[str, Any] | ProvenanceV1 | None = None,
    *,
    scope: str = "all",
    created_at: Optional[str] = None,
    explanation_type: str = "snapshot_explanation",
    fact: Optional[Dict[str, Any]] = None,
) -> ExplanationPayloadV1:
    if isinstance(explain, ExplanationPayloadV1):
        return explain

    if isinstance(explain, dict) and {"schema_version", "entity", "summary", "provenance"} <= set(explain.keys()):
        return ExplanationPayloadV1.model_validate(explain)

    provenance_model = (
        provenance
        if isinstance(provenance, ProvenanceV1)
        else ProvenanceV1.model_validate(provenance or default_provenance().model_dump())
    )

    legacy_summary = str(explain.get("summary") or "시장 분석 완료")
    bullets = explain.get("bullets") or []
    if not isinstance(bullets, list):
        bullets = [str(bullets)]

    market = "multi_market" if scope == "all" else scope
    market_context_refs: Dict[str, Any] = {}
    if isinstance(fact, dict):
        global_regime = fact.get("global_regime", {})
        if isinstance(global_regime, dict):
            for key in ("overall_regime", "sentiment", "mtf_direction", "fear_greed"):
                if key in global_regime:
                    market_context_refs[key] = global_regime[key]

    return ExplanationPayloadV1(
        schema_version="1.0",
        entity=ExplanationEntity(
            market=market,
            entity_type="portfolio" if scope == "all" else "market",
        ),
        as_of_time=created_at or now_iso(),
        explanation_type=explanation_type,
        assessment=ExplanationAssessment(),
        evidence=ExplanationEvidence(
            market_context_refs=market_context_refs,
            confidence=0.5,
        ),
        summary=ExplanationSummary(
            headline=legacy_summary,
            why_text=legacy_summary,
            short_reason=bullets[0] if bullets else None,
            bullet_points=[str(item) for item in bullets],
            market_details=explain.get("market_details") or {},
            uncertainty_flags=[str(item) for item in (explain.get("uncertainty_flags") or [])],
            risk_assessment=explain.get("risk_assessment") or {},
        ),
        provenance=provenance_model,
    )


def explanation_to_llm_text(payload: Dict[str, Any] | ExplanationPayloadV1 | None) -> str:
    if payload is None:
        return ""
    if isinstance(payload, dict):
        try:
            payload = ExplanationPayloadV1.model_validate(payload)
        except Exception:
            summary = payload.get("summary")
            if isinstance(summary, str):
                return summary
            return ""

    lines = [payload.summary.headline]
    if payload.summary.why_text and payload.summary.why_text != payload.summary.headline:
        lines.append(payload.summary.why_text)
    for bullet in payload.summary.bullet_points[:5]:
        lines.append(f"- {bullet}")
    return "\n".join(line for line in lines if line).strip()
