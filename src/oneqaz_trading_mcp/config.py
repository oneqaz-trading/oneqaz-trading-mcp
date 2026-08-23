# -*- coding: utf-8 -*-
"""
Market MCP 서버 설정
===================
Docker 환경: /workspace (auto_trader 루트)
로컬 환경: 프로젝트 루트 자동 감지
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

# ---------------------------------------------------------------------------
# 경로 설정
# ---------------------------------------------------------------------------

# 프로젝트 루트 감지 (Docker: /workspace, 로컬: 상위 폴더)
def _detect_project_root() -> Path:
    """프로젝트 루트 자동 감지"""
    # Docker 환경 (프로젝트 폴더가 /workspace에 마운트된 경우)
    docker_root = Path("/workspace")
    if docker_root.exists() and (docker_root / "mcps").exists():
        return docker_root
    # 로컬 환경 (mcps 폴더의 상위)
    return Path(__file__).parent.parent.resolve()

PROJECT_ROOT = _detect_project_root()

# ---------------------------------------------------------------------------
# 데이터 경로 (모든 경로는 PROJECT_ROOT 기준)
# ---------------------------------------------------------------------------


def _market_data_dir(default_rel: str, env_key: str) -> Path:
    """
    시장별 데이터 디렉터리 경로를 반환.
    - 기본: PROJECT_ROOT/{default_rel}
    - 오버라이드: 환경변수 env_key (절대/상대 모두 허용)
    """
    raw = (os.environ.get(env_key) or "").strip()
    if not raw:
        return PROJECT_ROOT / default_rel

    override = Path(raw)
    if not override.is_absolute():
        override = (PROJECT_ROOT / override).resolve()
    return override

# 글로벌 레짐 데이터
GLOBAL_REGIME_DIR = PROJECT_ROOT / "market" / "global_regime" / "data_storage"
GLOBAL_REGIME_SUMMARY_JSON = GLOBAL_REGIME_DIR / "global_regime_summary.json"
BONDS_ANALYSIS_DB = GLOBAL_REGIME_DIR / "bonds_analysis.db"
COMMODITIES_ANALYSIS_DB = GLOBAL_REGIME_DIR / "commodities_analysis.db"
FOREX_ANALYSIS_DB = GLOBAL_REGIME_DIR / "forex_analysis.db"
VIX_ANALYSIS_DB = GLOBAL_REGIME_DIR / "vix_analysis.db"
CREDIT_ANALYSIS_DB = GLOBAL_REGIME_DIR / "credit_analysis.db"
LIQUIDITY_ANALYSIS_DB = GLOBAL_REGIME_DIR / "liquidity_analysis.db"
INFLATION_ANALYSIS_DB = GLOBAL_REGIME_DIR / "inflation_analysis.db"

# 시장별 구조 데이터 (ETF/바스켓 분석)
US_STRUCTURE_DIR = PROJECT_ROOT / "market" / "us_market" / "data_storage" / "regime"
KR_STRUCTURE_DIR = PROJECT_ROOT / "market" / "kr_market" / "data_storage" / "regime"
COIN_STRUCTURE_DIR = PROJECT_ROOT / "market" / "coin_market" / "data_storage" / "regime"
US_STRUCTURE_SUMMARY_JSON = US_STRUCTURE_DIR / "market_structure_summary.json"
KR_STRUCTURE_SUMMARY_JSON = KR_STRUCTURE_DIR / "market_structure_summary.json"
COIN_STRUCTURE_SUMMARY_JSON = COIN_STRUCTURE_DIR / "market_structure_summary.json"
US_STRUCTURE_ANALYSIS_DB = US_STRUCTURE_DIR / "group_analysis.db"
KR_STRUCTURE_ANALYSIS_DB = KR_STRUCTURE_DIR / "group_analysis.db"
COIN_STRUCTURE_ANALYSIS_DB = COIN_STRUCTURE_DIR / "group_analysis.db"

# 시장별 데이터 루트 (환경변수 오버라이드 지원)
COIN_DATA_DIR = _market_data_dir("market/coin_market/data_storage", "MCP_COIN_DATA_DIR")
KR_DATA_DIR = _market_data_dir("market/kr_market/data_storage", "MCP_KR_DATA_DIR")
US_DATA_DIR = _market_data_dir("market/us_market/data_storage", "MCP_US_DATA_DIR")
EXTERNAL_CONTEXT_DATA_DIR = _market_data_dir("external_context/data_storage", "MCP_EXTERNAL_CONTEXT_DATA_DIR")

# 시장별 trading_system.db (매매/포지션)
COIN_TRADING_DB = COIN_DATA_DIR / "trading_system.db"
KR_TRADING_DB = KR_DATA_DIR / "trading_system.db"
US_TRADING_DB = US_DATA_DIR / "trading_system.db"

# 🆕 시장별 시그널 디렉터리 (종목별 개별 DB)
COIN_SIGNALS_DIR = COIN_DATA_DIR / "signals"
KR_SIGNALS_DIR = KR_DATA_DIR / "signals"
US_SIGNALS_DIR = US_DATA_DIR / "signals"

# 시장 ID → 매매 DB 경로 매핑
MARKET_DB_PATHS = {
    "crypto": COIN_TRADING_DB,
    "coin": COIN_TRADING_DB,  # alias
    "kr_stock": KR_TRADING_DB,
    "kr": KR_TRADING_DB,  # alias
    "us_stock": US_TRADING_DB,
    "us": US_TRADING_DB,  # alias
}

# 🆕 시장 ID → 시그널 디렉터리 매핑 (종목별 DB)
SIGNAL_DIR_PATHS = {
    "crypto": COIN_SIGNALS_DIR,
    "coin": COIN_SIGNALS_DIR,
    "kr_stock": KR_SIGNALS_DIR,
    "kr": KR_SIGNALS_DIR,
    "us_stock": US_SIGNALS_DIR,
    "us": US_SIGNALS_DIR,
}

# external_context DB 경로 매핑
EXTERNAL_DB_PATHS = {
    "crypto": EXTERNAL_CONTEXT_DATA_DIR / "coin_market" / "external_context.db",
    "coin": EXTERNAL_CONTEXT_DATA_DIR / "coin_market" / "external_context.db",
    "coin_market": EXTERNAL_CONTEXT_DATA_DIR / "coin_market" / "external_context.db",
    "kr_stock": EXTERNAL_CONTEXT_DATA_DIR / "kr_market" / "external_context.db",
    "kr": EXTERNAL_CONTEXT_DATA_DIR / "kr_market" / "external_context.db",
    "kr_market": EXTERNAL_CONTEXT_DATA_DIR / "kr_market" / "external_context.db",
    "us_stock": EXTERNAL_CONTEXT_DATA_DIR / "us_market" / "external_context.db",
    "us": EXTERNAL_CONTEXT_DATA_DIR / "us_market" / "external_context.db",
    "us_market": EXTERNAL_CONTEXT_DATA_DIR / "us_market" / "external_context.db",
    "bonds": EXTERNAL_CONTEXT_DATA_DIR / "bonds" / "external_context.db",
    "bond": EXTERNAL_CONTEXT_DATA_DIR / "bonds" / "external_context.db",
    "forex": EXTERNAL_CONTEXT_DATA_DIR / "forex" / "external_context.db",
    "commodities": EXTERNAL_CONTEXT_DATA_DIR / "commodities" / "external_context.db",
    "commodity": EXTERNAL_CONTEXT_DATA_DIR / "commodities" / "external_context.db",
    "news": EXTERNAL_CONTEXT_DATA_DIR / "news" / "external_context.db",
    "vix": EXTERNAL_CONTEXT_DATA_DIR / "vix" / "external_context.db",
    "credit": EXTERNAL_CONTEXT_DATA_DIR / "credit" / "external_context.db",
    "liquidity": EXTERNAL_CONTEXT_DATA_DIR / "liquidity" / "external_context.db",
    "inflation": EXTERNAL_CONTEXT_DATA_DIR / "inflation" / "external_context.db",
    "us_structure": EXTERNAL_CONTEXT_DATA_DIR / "us_structure" / "external_context.db",
    "kr_structure": EXTERNAL_CONTEXT_DATA_DIR / "kr_structure" / "external_context.db",
    "coin_structure": EXTERNAL_CONTEXT_DATA_DIR / "coin_structure" / "external_context.db",
}

# 카테고리별 분석 DB 매핑
ANALYSIS_DB_PATHS = {
    "bonds": BONDS_ANALYSIS_DB,
    "commodities": COMMODITIES_ANALYSIS_DB,
    "forex": FOREX_ANALYSIS_DB,
    "vix": VIX_ANALYSIS_DB,
    "credit": CREDIT_ANALYSIS_DB,
    "liquidity": LIQUIDITY_ANALYSIS_DB,
    "inflation": INFLATION_ANALYSIS_DB,
    "us_structure": US_STRUCTURE_ANALYSIS_DB,
    "kr_structure": KR_STRUCTURE_ANALYSIS_DB,
    "coin_structure": COIN_STRUCTURE_ANALYSIS_DB,
}

STRUCTURE_SUMMARY_PATHS = {
    "us_stock": US_STRUCTURE_SUMMARY_JSON,
    "us": US_STRUCTURE_SUMMARY_JSON,
    "us_structure": US_STRUCTURE_SUMMARY_JSON,
    "kr_stock": KR_STRUCTURE_SUMMARY_JSON,
    "kr": KR_STRUCTURE_SUMMARY_JSON,
    "kr_structure": KR_STRUCTURE_SUMMARY_JSON,
    "crypto": COIN_STRUCTURE_SUMMARY_JSON,
    "coin": COIN_STRUCTURE_SUMMARY_JSON,
    "coin_structure": COIN_STRUCTURE_SUMMARY_JSON,
}

# ---------------------------------------------------------------------------
# 서버 설정
# ---------------------------------------------------------------------------

# FastMCP 서버 포트 (기본: 8010, 환경변수로 오버라이드 가능)
MCP_SERVER_PORT = int(os.environ.get("MCP_SERVER_PORT", "8010"))
MCP_SERVER_HOST = os.environ.get("MCP_SERVER_HOST", "0.0.0.0")

# 서버 모드
# stateless=True: 세션 관리 없이 각 요청이 독립적으로 처리됨
# 클라이언트 재시작/MCP 재시작 후 세션 불일치로 인한 404 방지
MCP_STATELESS = True
MCP_JSON_RESPONSE = True

# ---------------------------------------------------------------------------
# 캐싱 설정
# ---------------------------------------------------------------------------

# Resource 캐시 TTL (초) - 빈번한 DB 조회 방지
CACHE_TTL_GLOBAL_REGIME = 300  # 글로벌 레짐: 5분
CACHE_TTL_MARKET_STATUS = 60   # 시장 상태: 1분
CACHE_TTL_FEAR_GREED = 300    # Fear&Greed: 5분 (외부 API 호출이므로 길게)

# Tool 결과 캐시 TTL (초)
CACHE_TTL_TRADE_HISTORY = 10  # 거래 내역: 10초
CACHE_TTL_POSITIONS = 10      # 포지션: 10초

# AI Summary TTL (초) - resource/tool별 ai_summary 유효 시간
AI_SUMMARY_TTL = {
    "global_regime": 300,
    "market_status": 60,
    "market_structure": 300,
    "signal_system": 120,
    "external_context": 120,
    "unified_context": 120,
    "derived_signals": 120,
    "indicators": 300,
    # tools — 이중 청중(AI raw + 인간 1줄) wrapper 적용 대상
    "positions": 30,
    "decisions": 30,
    "signals": 60,
    "trade_history": 30,
    "trust_layer": 300,
}

# ---------------------------------------------------------------------------
# 로깅 설정
# ---------------------------------------------------------------------------

LOG_LEVEL = os.environ.get("MCP_LOG_LEVEL", "INFO")
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [MCP] %(message)s"

# ---------------------------------------------------------------------------
# 유틸리티 함수
# ---------------------------------------------------------------------------

def get_market_db_path(market_id: str) -> Path | None:
    """시장 ID로 매매 DB 경로 반환"""
    market_key = market_id.lower().replace("-", "_")
    return MARKET_DB_PATHS.get(market_key)

def get_signals_dir(market_id: str) -> Path | None:
    """시장 ID로 시그널 디렉터리 경로 반환"""
    market_key = market_id.lower().replace("-", "_")
    return SIGNAL_DIR_PATHS.get(market_key)

def get_signal_db_path(market_id: str, symbol: str = None) -> Path | None:
    """시장 ID (+ 선택적 종목)으로 시그널 DB 경로 반환

    Args:
        market_id: 시장 식별자 (crypto, kr, us 등)
        symbol: 종목 심볼 (지정 시 종목별 DB 반환)
    Returns:
        symbol 지정 시: .../signals/{symbol}_signal.db
        symbol 미지정 시: .../signals/ 디렉터리 (None이 아님)
    """
    sig_dir = get_signals_dir(market_id)
    if sig_dir is None:
        return None
    if symbol:
        norm = symbol.upper().replace('-KRW', '').replace('_KRW', '').replace('KRW-', '')
        return sig_dir / f"{norm.lower()}_signal.db"
    return sig_dir

def list_signal_db_files(market_id: str) -> list:
    """시장의 모든 종목별 시그널 DB 파일 목록"""
    sig_dir = get_signals_dir(market_id)
    if sig_dir is None or not sig_dir.exists():
        return []
    return sorted(sig_dir.glob("*_signal.db"))

def get_symbol_from_signal_db(db_path: Path) -> str:
    """시그널 DB 파일에서 종목 심볼 추출"""
    return db_path.stem.replace('_signal', '').upper()

def get_analysis_db_path(category: str) -> Path | None:
    """카테고리로 분석 DB 경로 반환"""
    return ANALYSIS_DB_PATHS.get(category.lower())


def get_structure_summary_path(market_id: str) -> Path | None:
    """시장 ID로 구조 요약 JSON 경로 반환"""
    return STRUCTURE_SUMMARY_PATHS.get(market_id.lower().replace("-", "_"))

def get_external_db_path(market_id: str) -> Path | None:
    """시장/카테고리 ID로 external_context DB 경로 반환."""
    market_key = market_id.lower().replace("-", "_")
    return EXTERNAL_DB_PATHS.get(market_key)

# ---------------------------------------------------------------------------
# SQLite → PG read routing (Strangler Fig 전환 중)
# ---------------------------------------------------------------------------
# 기존 writer 이관 완료 DB 에 대해 read 도 PG 에서 수행하도록 경로 매핑.
# 아직 PG 로 이관되지 않은 DB 는 SQLite 폴백.

def _pg_route_for_db_path(db_path: Path) -> Optional[Dict[str, Any]]:
    """db_path 를 PG 라우팅 정보로 변환.

    반환:
        {"schema": "market_coin", "filter": {"symbol": "BTC"}} — 심볼 필터 주입 필요
        {"schema": "market_coin"} — 필터 불필요 (단일 테이블 read)
        None — PG 이관 안됨, SQLite 폴백
    """
    try:
        p = Path(db_path).resolve()
    except Exception:
        return None
    name = p.name.lower()
    parts = [x.lower() for x in p.parts]

    # 1) trading_system.db → market_{coin/kr/us}
    if name == "trading_system.db":
        if "coin_market" in parts:
            return {"schema": "market_coin"}
        if "kr_market" in parts:
            return {"schema": "market_kr"}
        if "us_market" in parts:
            return {"schema": "market_us"}

    # 1b) {coin,kr,us}_candles.db → market_{coin,kr,us} (단일 candles 테이블)
    #     dashboard market-pulse, signal_calculator 등이 캔들 DB 를 readonly 로 참조.
    if name in ("coin_candles.db", "kr_candles.db", "us_candles.db"):
        if name == "coin_candles.db":
            return {"schema": "market_coin"}
        if name == "kr_candles.db":
            return {"schema": "market_kr"}
        if name == "us_candles.db":
            return {"schema": "market_us"}

    # 2) signals/{symbol}_signal.db → market_{coin/kr/us} + symbol 필터
    if name.endswith("_signal.db") and "signals" in parts:
        symbol = name.replace("_signal.db", "").upper()
        if "coin_market" in parts:
            return {"schema": "market_coin", "filter": {"symbol": symbol}}
        if "kr_market" in parts:
            return {"schema": "market_kr", "filter": {"symbol": symbol}}
        if "us_market" in parts:
            return {"schema": "market_us", "filter": {"symbol": symbol}}

    # 2b) trade/{symbol}_profit.db → market_{coin/kr/us} + symbol 필터
    # 8 tables: profit_history + trajectory_* (exit_timing, trailing_params,
    # recovery_params, regime_curves, holding_risk, patterns, learning_state)
    if name.endswith("_profit.db") and "trade" in parts:
        symbol_raw = name.replace("_profit.db", "")
        # kr 은 숫자 코드 (006800), coin/us 는 대문자. kr 판별은 parts 로.
        symbol = symbol_raw if "kr_market" in parts else symbol_raw.upper()
        if "coin_market" in parts:
            return {"schema": "market_coin", "filter": {"symbol": symbol}}
        if "kr_market" in parts:
            return {"schema": "market_kr", "filter": {"symbol": symbol}}
        if "us_market" in parts:
            return {"schema": "market_us", "filter": {"symbol": symbol}}

    # 3) external_context/*/external_context.db → external_context schema
    #    대부분의 테이블은 글로벌(news_events, macro_event_narratives 등) —
    #    per-market 구분은 `symbol`/`market_id`/`sector` 컬럼으로 caller 쿼리가 처리.
    #    per-market DB 파일과 news/commodities 등 카테고리 DB 를 모두 동일 PG 스키마로 라우팅.
    if name == "external_context.db" and "data_storage" in parts:
        return {"schema": "external_context"}

    # 3b) agent_history/data_storage/{coin,kr_stock,us_stock}/agent_history.db
    #     → agent_history schema (Phase 4 이관 완료, 단일 스키마 + market_id 컬럼).
    #     feature_governance.db (구 AWS export, 배포 제거됨) 경로도 호환 라우팅 유지.
    if name in ("agent_history.db", "feature_governance.db") and "agent_history" in parts:
        return {"schema": "agent_history"}

    # 4pre) market/global_regime/data_storage/global_predictions.db → market_global
    #       macro_prediction_accuracy 등 prediction 관련 테이블이 모여 있는 단일 DB.
    if name == "global_predictions.db" and "global_regime" in parts:
        return {"schema": "market_global"}

    # 4) market/global_regime/{category}_analysis.db → market_global.analysis
    #    단일 PG 테이블 + category discriminator. SQLite 쿼리 `FROM analysis` 를
    #    `FROM market_global.analysis WHERE category = <cat>` 로 자동 변환하기 위해
    #    category 필터를 shim 에 주입.
    if name.endswith("_analysis.db") and "global_regime" in parts:
        cat = name.replace("_analysis.db", "").lower()
        _GLOBAL_REGIME_CATEGORIES = (
            "bonds", "commodities", "forex", "vix",
            "credit", "liquidity", "inflation", "energy",
        )
        if cat in _GLOBAL_REGIME_CATEGORIES:
            return {"schema": "market_global", "filter": {"category": cat}}

    # 5) market/{coin,kr,us}_market/data_storage/regime/group_analysis.db
    #    - coin: market_coin.group_analysis (테이블명 동일)
    #    - kr/us: market_{kr,us}.regime_group_analysis (테이블명 다름)
    #    SQLite 쿼리 `FROM analysis` 를 적절한 테이블로 리라우팅해야 한다.
    if name == "group_analysis.db" and "regime" in parts:
        if "coin_market" in parts:
            return {"schema": "market_coin", "table_alias": {"analysis": "group_analysis"}}
        if "kr_market" in parts:
            return {"schema": "market_kr",   "table_alias": {"analysis": "regime_group_analysis"}}
        if "us_market" in parts:
            return {"schema": "market_us",   "table_alias": {"analysis": "regime_group_analysis"}}

    # 6) crow/data_storage/crow_context.db → crow schema (Pipeline G)
    if name == "crow_context.db":
        return {"schema": "crow"}

    # 7) llm_factory/store/conversation.db → llm_factory schema (Pipeline F consumer)
    if name == "conversation.db" and "llm_factory" in parts:
        return {"schema": "llm_factory"}

    # 8) llm_factory/store/insight.db → llm_factory schema
    if name == "insight.db" and "llm_factory" in parts:
        return {"schema": "llm_factory"}

    # 8b) market/market_structure/data_storage/structure_learning.db
    #     → market_{coin,kr,us}_struct 3 스키마로 분리 이관됨.
    #     SQLite 시절 단일 DB + `market_id` 컬럼 ('crypto'/'kr_stock'/'us_stock') 으로
    #     discriminate. PG 에서는 스키마 자체가 market 을 식별하고 `market_id` 컬럼은
    #     존재하지 않는다. caller (mcps/tools/trust_layer.py 의 get_structure_*) 가
    #     `FROM structure_calibration` / `FROM structure_validation_history` 를
    #     단일 테이블 가정으로 쿼리하므로 shim 이 3 스키마 UNION ALL + market_id
    #     리터럴 합성으로 자동 변환해야 한다.
    if name == "structure_learning.db" and "market_structure" in parts:
        return {"union_struct": True}

    # 9) market/{m}_market/data_storage/learning_strategies[_staging]/_global_predictions.db
    #    → rl_pipeline schema.
    #    SQLite: per-market 디렉토리로 분리 + live/staging 디렉토리로 분리 (파일 4개 × 3 = 12 개).
    #    PG: 단일 스키마 + `market_id` + `is_staging` 컬럼으로 discriminate.
    #    caller 쿼리가 이 2개 컬럼을 몰라도 동작하도록 filter 를 shim 으로 주입.
    if name == "_global_predictions.db":
        is_staging_dir = "learning_strategies_staging" in parts
        market_id: Optional[str] = None
        if "coin_market" in parts:
            market_id = "coin"
        elif "kr_market" in parts:
            market_id = "kr"
        elif "us_market" in parts:
            market_id = "us"
        if market_id is not None:
            return {
                "schema": "rl_pipeline",
                "filter": {"market_id": market_id, "is_staging": is_staging_dir},
            }

    return None


def _make_symbol_filtered_shim(pg_shim, symbol: str):
    """PgConnectionShim 을 감싸서 signals 테이블 쿼리에 symbol 필터 자동 주입.

    SQLite 는 per-symbol DB (`{symbol}_signal.db`) 이므로 쿼리에 심볼 조건이 없지만
    PG 통합 `signals` 테이블은 `symbol` 컬럼으로 discriminate 한다. caller 쿼리를
    바꾸지 않고도 올바른 결과가 나오도록 `WHERE symbol=%s` 를 자동 주입.
    """
    return _SymbolFilterConnectionShim(pg_shim, symbol)


def _make_category_filtered_shim(pg_shim, category: str):
    """PgConnectionShim 을 감싸서 analysis 테이블 쿼리에 category 필터 자동 주입.

    SQLite 는 per-category DB (`{category}_analysis.db`) 이고 쿼리는 `FROM analysis`.
    PG 통합 `market_global.analysis` 는 category 컬럼으로 discriminate 하므로
    `WHERE category=?` 를 자동 주입하여 caller 쿼리 수정 없이 동일 결과 보장.
    """
    return _CategoryFilterConnectionShim(pg_shim, category)


def _make_table_aliased_shim(pg_shim, alias_map: dict):
    """PgConnectionShim 을 감싸서 FROM/JOIN 테이블명을 PG 실제 테이블명으로 치환.

    예) group_analysis.db 의 `FROM analysis` → kr/us 는 `FROM regime_group_analysis`.
    """
    return _TableAliasConnectionShim(pg_shim, alias_map)


class _TableAliasConnectionShim:
    """SQLite 테이블명 → PG 테이블명 자동 치환."""

    def __init__(self, inner, alias_map: dict):
        self._inner = inner
        # 소문자 키로 정규화
        self._alias_map = {k.lower(): v for k, v in alias_map.items()}
        self.row_factory = None

    def _rewrite(self, sql: str) -> str:
        import re as _re
        out = sql
        for src, dst in self._alias_map.items():
            # FROM <src> / JOIN <src> 를 <dst> 로 치환 (단어 경계)
            pattern = _re.compile(
                r"(\b(?:FROM|JOIN)\s+)" + _re.escape(src) + r"\b",
                _re.IGNORECASE,
            )
            out = pattern.sub(lambda m: m.group(1) + dst, out)
        return out

    def execute(self, sql: str, params=()):
        return self._inner.execute(self._rewrite(sql), params)

    def cursor(self):
        return self._inner.cursor()

    def commit(self):
        self._inner.commit()

    def rollback(self):
        self._inner.rollback()

    def close(self):
        self._inner.close()

    def __enter__(self):
        self._inner.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._inner.__exit__(exc_type, exc, tb)


class _StructureLearningUnionShim:
    """structure_learning.db 단일 테이블 → 3개 PG 스키마 UNION ALL 자동 변환.

    SQLite 시절:
        FROM structure_calibration          + WHERE market_id = 'crypto'/'kr_stock'/'us_stock'
        FROM structure_validation_history   + WHERE market_id = ...

    PG 이관 후:
        market_coin_struct.structure_calibration  (market_id 컬럼 없음)
        market_kr_struct.structure_calibration    (market_id 컬럼 없음)
        market_us_struct.structure_calibration    (market_id 컬럼 없음)

    이 shim 은 caller SQL 을 가로채서:
      1) `sqlite_master` 조회 → "table exists" 가짜 응답
      2) `FROM <대상테이블>` → 3 스키마 UNION ALL 서브쿼리로 rewrite, `market_id`
         리터럴 컬럼 합성 ('crypto'/'kr_stock'/'us_stock').
      3) caller 의 `WHERE market_id = ?` 는 그대로 통과 (UNION 결과에 컬럼 존재).
    """

    _TABLES_TO_UNION = ("structure_calibration", "structure_validation_history")

    # SQLite caller 의 market_id 값 → PG 스키마명
    _MARKET_SCHEMA_MAP = (
        ("crypto",   "market_coin_struct"),
        ("kr_stock", "market_kr_struct"),
        ("us_stock", "market_us_struct"),
    )

    def __init__(self, inner):
        self._inner = inner
        self.row_factory = None

    def _build_union_subquery(self, table: str) -> str:
        # SELECT *, '<market_id>' AS market_id FROM <schema>.<table>
        parts = []
        for mid, schema in self._MARKET_SCHEMA_MAP:
            parts.append(
                f"SELECT *, '{mid}'::text AS market_id "
                f"FROM {schema}.{table}"
            )
        # 괄호로 감싸서 FROM (...) AS <table> 형태로 alias 보존.
        return "(" + " UNION ALL ".join(parts) + ") AS " + table

    def _rewrite(self, sql: str) -> str:
        import re as _re
        out = sql
        for tbl in self._TABLES_TO_UNION:
            # FROM <table> (단어 경계). JOIN 도 동일 처리.
            pattern = _re.compile(
                r"(\b(?:FROM|JOIN)\s+)" + _re.escape(tbl) + r"\b",
                _re.IGNORECASE,
            )
            sub = self._build_union_subquery(tbl)
            out = pattern.sub(lambda m: m.group(1) + sub, out)
        return out

    def execute(self, sql: str, params=()):
        s = sql.lstrip()
        s_upper = s.upper()
        # caller 가 호환성 체크로 sqlite_master 를 조회하는 케이스: "테이블 존재"
        # 가짜 응답을 반환해 진짜 PG 쿼리에 도달하지 못 하게 막는다.
        if "SQLITE_MASTER" in s_upper:
            return _FakeCursorWithRow(("structure_calibration",))
        if not s_upper.startswith("SELECT"):
            # 이 shim 은 readonly 용도. write 는 호출 가능성 없음.
            return self._inner.execute(sql, params)
        return self._inner.execute(self._rewrite(sql), params)

    def cursor(self):
        return self._inner.cursor()

    def commit(self):
        self._inner.commit()

    def rollback(self):
        self._inner.rollback()

    def close(self):
        self._inner.close()

    def __enter__(self):
        self._inner.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._inner.__exit__(exc_type, exc, tb)


class _FakeCursorWithRow:
    """sqlite_master "테이블 존재" 응답용 단발 fetchone 커서."""

    description = None
    rowcount = 1

    def __init__(self, row):
        self._row = row
        self._consumed = False

    def fetchone(self):
        if self._consumed:
            return None
        self._consumed = True
        return self._row

    def fetchall(self):
        if self._consumed:
            return []
        self._consumed = True
        return [self._row]

    def fetchmany(self, size=1):
        return self.fetchall()

    def close(self):
        pass


class _CategoryFilterConnectionShim:
    """per-category SQLite DB 에뮬레이션용 — analysis 테이블 쿼리에 category 필터 주입."""

    _ANALYSIS_TABLES = ("analysis",)

    def __init__(self, inner, category: str):
        self._inner = inner
        self._category = category
        self.row_factory = None

    def _inject_category_filter(self, sql: str, params):
        import re as _re
        s_upper = sql.lstrip().upper()
        if not s_upper.startswith("SELECT"):
            return sql, params
        m = _re.search(
            r"\bFROM\s+(" + "|".join(self._ANALYSIS_TABLES) + r")\b",
            sql, _re.IGNORECASE,
        )
        if not m:
            return sql, params
        if _re.search(r"\bcategory\s*=", sql, _re.IGNORECASE):
            return sql, params

        if _re.search(r"\bWHERE\b", sql, _re.IGNORECASE):
            new_sql = _re.sub(
                r"\bWHERE\b", "WHERE category = ? AND ", sql, count=1, flags=_re.IGNORECASE
            )
        else:
            tail_match = _re.search(
                r"\b(ORDER\s+BY|GROUP\s+BY|LIMIT)\b", sql, _re.IGNORECASE
            )
            if tail_match:
                idx = tail_match.start()
                new_sql = sql[:idx] + "WHERE category = ? " + sql[idx:]
            else:
                new_sql = sql.rstrip().rstrip(";") + " WHERE category = ?"

        new_params = (self._category,) + tuple(params or ())
        return new_sql, new_params

    def execute(self, sql: str, params=()):
        new_sql, new_params = self._inject_category_filter(sql, params)
        return self._inner.execute(new_sql, new_params)

    def cursor(self):
        return self._inner.cursor()

    def commit(self):
        self._inner.commit()

    def rollback(self):
        self._inner.rollback()

    def close(self):
        self._inner.close()

    def __enter__(self):
        self._inner.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._inner.__exit__(exc_type, exc, tb)


class _SymbolFilterConnectionShim:
    """per-symbol SQLite DB 에뮬레이션용 — signals 관련 테이블 쿼리에 symbol 필터 주입."""

    _SIGNAL_TABLES = ("signals", "signals_15m", "signals_30m", "signals_240m",
                      "signals_1d", "signals_other", "signal_predictions",
                      "signal_trajectory", "per_symbol_signal_trajectory",
                      "per_symbol_signal_feedback_scores",
                      "per_symbol_exit_calibration",
                      "per_symbol_score_calibration",
                      "per_symbol_defense_streak")

    def __init__(self, inner, symbol: str):
        self._inner = inner
        self._symbol = symbol
        self.row_factory = None

    def _inject_symbol_filter(self, sql: str, params):
        """SQL 에 symbol 필터 주입 (대상 테이블 SELECT 만)."""
        import re as _re
        s_upper = sql.lstrip().upper()
        if not s_upper.startswith("SELECT"):
            return sql, params
        # FROM <signal_table> 탐지
        m = _re.search(
            r"\bFROM\s+(" + "|".join(self._SIGNAL_TABLES) + r")\b",
            sql, _re.IGNORECASE,
        )
        if not m:
            return sql, params
        # WHERE symbol = ... 이 이미 있으면 주입 skip
        if _re.search(r"\bsymbol\s*=", sql, _re.IGNORECASE):
            return sql, params

        # WHERE 존재 여부에 따라 주입 방식 분기
        if _re.search(r"\bWHERE\b", sql, _re.IGNORECASE):
            new_sql = _re.sub(
                r"\bWHERE\b", f"WHERE symbol = ? AND ", sql, count=1, flags=_re.IGNORECASE
            )
        else:
            # ORDER BY / LIMIT / GROUP BY 앞에 WHERE 삽입
            tail_match = _re.search(
                r"\b(ORDER\s+BY|GROUP\s+BY|LIMIT)\b", sql, _re.IGNORECASE
            )
            if tail_match:
                idx = tail_match.start()
                new_sql = sql[:idx] + f"WHERE symbol = ? " + sql[idx:]
            else:
                new_sql = sql.rstrip().rstrip(";") + f" WHERE symbol = ?"

        new_params = (self._symbol,) + tuple(params or ())
        return new_sql, new_params

    def execute(self, sql: str, params=()):
        new_sql, new_params = self._inject_symbol_filter(sql, params)
        return self._inner.execute(new_sql, new_params)

    def cursor(self):
        return self._inner.cursor()

    def commit(self):
        self._inner.commit()

    def rollback(self):
        self._inner.rollback()

    def close(self):
        self._inner.close()

    def __enter__(self):
        self._inner.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._inner.__exit__(exc_type, exc, tb)


class _MarketStagingFilterConnectionShim:
    """per-market + live/staging 분리 SQLite DB 에뮬레이션.

    `_global_predictions.db` 가 대상. SQLite 는 디렉토리(market 별) + 파일
    (live/staging) 로 분리되어 있지만 PG 통합 `rl_pipeline.*` 테이블은
    `market_id` + `is_staging` 컬럼으로 discriminate 한다.

    caller 쿼리가 WHERE 절에 두 컬럼을 언급하지 않아도 동일 결과가 나오도록
    SELECT 에 자동 주입한다. sqlite_master / PRAGMA 는 compat shim 이 이미
    처리하므로 여기서는 SELECT 만 대상으로 한다.
    """

    # rl_pipeline 스키마 내 market_id + is_staging 컬럼이 모두 존재하는 테이블
    _RL_TABLES = (
        "global_strategies",
        "global_strategy_predictions",
        "global_strategy_results",
        "optimal_thresholds",
        "symbol_global_weights",
    )

    def __init__(self, inner, market_id: str, is_staging: bool):
        self._inner = inner
        self._market_id = market_id
        self._is_staging = bool(is_staging)
        self.row_factory = None

    def _inject_filter(self, sql: str, params):
        import re as _re
        s_upper = sql.lstrip().upper()
        if not s_upper.startswith("SELECT"):
            return sql, params
        m = _re.search(
            r"\bFROM\s+(" + "|".join(self._RL_TABLES) + r")\b",
            sql, _re.IGNORECASE,
        )
        if not m:
            return sql, params
        # 이미 market_id 또는 is_staging 조건이 있으면 중복 주입 skip
        if _re.search(r"\bmarket_id\s*=", sql, _re.IGNORECASE):
            return sql, params

        injected = "market_id = ? AND is_staging = ?"
        extra_params = (self._market_id, self._is_staging)

        if _re.search(r"\bWHERE\b", sql, _re.IGNORECASE):
            new_sql = _re.sub(
                r"\bWHERE\b", f"WHERE {injected} AND ", sql,
                count=1, flags=_re.IGNORECASE,
            )
        else:
            tail_match = _re.search(
                r"\b(ORDER\s+BY|GROUP\s+BY|LIMIT)\b", sql, _re.IGNORECASE,
            )
            if tail_match:
                idx = tail_match.start()
                new_sql = sql[:idx] + f"WHERE {injected} " + sql[idx:]
            else:
                new_sql = sql.rstrip().rstrip(";") + f" WHERE {injected}"

        new_params = extra_params + tuple(params or ())
        return new_sql, new_params

    def execute(self, sql: str, params=()):
        new_sql, new_params = self._inject_filter(sql, params)
        return self._inner.execute(new_sql, new_params)

    def cursor(self):
        return self._inner.cursor()

    def commit(self):
        self._inner.commit()

    def rollback(self):
        self._inner.rollback()

    def close(self):
        self._inner.close()

    def __enter__(self):
        self._inner.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._inner.__exit__(exc_type, exc, tb)


def connect_readonly(db_path: Union[Path, str], timeout: float = 5.0):
    """[Wave I] PG read-only 연결 전용. SQLite fallback 폐기.

    이관된 경로는 PG 로 라우팅. 미등록 경로는 RuntimeError.

    사용:
        with connect_readonly(db_path) as conn:
            rows = conn.execute(query).fetchall()
    """
    p = Path(db_path) if not isinstance(db_path, Path) else db_path
    route = _pg_route_for_db_path(p)
    if route is None:
        raise RuntimeError(
            f"[Wave I] PG route not registered for db_path={p}. "
            "Add route in mcps.config._pg_route_for_db_path or migrate caller."
        )
    from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
    # structure_learning.db: 3개 PG 스키마(market_{coin,kr,us}_struct) UNION 으로
    # 단일 SQLite 테이블처럼 보이게 한다. search_path 가 무의미하므로 임의로
    # market_coin_struct 를 연 뒤 shim 이 모든 FROM 을 fully-qualified 로 rewrite.
    if route.get("union_struct"):
        pg_conn = open_schema_connection(
            "market_coin_struct", readonly=True, sqlite_path_hint=str(p),
        )
        return _StructureLearningUnionShim(pg_conn)
    # sqlite_path_hint 는 구 AWS fallback 용 (2026-07-06 배포 제거, compat 에서 no-op).
    # 시그니처 호환을 위해 전달만 유지.
    pg_conn = open_schema_connection(
        route["schema"], readonly=True, sqlite_path_hint=str(p),
    )
    flt = route.get("filter") or {}
    if "symbol" in flt:
        return _make_symbol_filtered_shim(pg_conn, flt["symbol"])
    if "category" in flt:
        return _make_category_filtered_shim(pg_conn, flt["category"])
    if "market_id" in flt and "is_staging" in flt:
        return _MarketStagingFilterConnectionShim(
            pg_conn, flt["market_id"], flt["is_staging"]
        )
    alias_map = route.get("table_alias")
    if alias_map:
        return _make_table_aliased_shim(pg_conn, alias_map)
    return pg_conn


def check_paths():
    """경로 존재 여부 확인 (디버깅용).

    2026-05-17: SQLite 시그널 샤드 (per-symbol *_signal.db) 카운트 출력은 legacy
    잔재 — 현재 PG cutover 완료 상태라 실 사용 안 함. 매 부팅마다 출력되면
    "PG 가 정말 동작 중인지" 헷갈리게 함. 대신 PG 스키마/active 상태를 INFO 로
    출력하고 SQLite 샤드는 DEBUG (verbose 모드에서만) 으로 격하.
    """
    print(f"[Config] PROJECT_ROOT: {PROJECT_ROOT}")
    print(f"[Config] GLOBAL_REGIME_SUMMARY_JSON exists: {GLOBAL_REGIME_SUMMARY_JSON.exists()}")
    print(f"[Config] DB_BACKEND: {os.environ.get('DB_BACKEND', 'postgres')}")

    # PG 스키마 개수 (실제 사용 중인 경로)
    try:
        from oneqaz_trading_mcp.shared.db.pg_pool import KNOWN_SCHEMAS
        print(f"[Config] PG schemas registered: {len(KNOWN_SCHEMAS)}")
    except Exception:
        pass

    # SQLite legacy 샤드는 verbose 모드에서만
    if os.environ.get("MCP_VERBOSE_BOOT") == "1":
        print(f"[Config] COIN_TRADING_DB exists: {COIN_TRADING_DB.exists()}")
        print(f"[Config] BONDS_ANALYSIS_DB exists: {BONDS_ANALYSIS_DB.exists()}")
        for market_id, db_path in MARKET_DB_PATHS.items():
            print(f"[Config] (legacy SQLite) trading {market_id}: {db_path.exists()}")
        for market_id, sig_dir in SIGNAL_DIR_PATHS.items():
            db_count = len(list(sig_dir.glob("*_signal.db"))) if sig_dir.exists() else 0
            print(f"[Config] (legacy SQLite) signals {market_id}: {sig_dir.exists()} ({db_count} DBs)")

if __name__ == "__main__":
    check_paths()
