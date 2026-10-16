# -*- coding: utf-8 -*-
"""
엔티티 컨텍스트 (2026-10-15, req_mcp_entity_context Stage A)
=============================================================
진입 엔티티(종목·그룹·시장)를 받아 근거가 붙은 연결(upstream / peers / downstream / news)과
빈 구간(gaps)을 하나의 공통 스키마로 돌려준다.

- 요청 시점 조립이다 — 새 스키마·빌더·백그라운드 잡 없음(제안서 ED2). 원천은 기존 테이블·JSON 을 읽기만 한다.
- LLM 은 응답 내용에 개입하지 않는다. LLM 산출 연결(inference_links 등)은 원천으로 쓰지 않는다(ED4).
- 연결을 지어내지 않는다 — 원천에 없는 관계는 반환하지 않고, 모르는 칸은 gaps 로 말한다.

모듈:
  session   — 세션 상태·기준(basis) 판정
  sources   — 원천 읽기(PG·JSON) + TTL 캐시
  resolver  — 입력 문자열 → 엔티티 (ambiguous 면 추측하지 않는다)
  links     — 원천 행 → 연결 객체 (kind × relation 고정)
  assembler — 공통 스키마 조립·headline·gaps·크기 예산·응답 점검(probe)
  context   — 엔티티 타입별 조립 (symbol · group · market)

정본 설계 = ai_brain/main_pc_tasks/proposal_mcp_entity_context_phase1_2026_10_15.md
"""
