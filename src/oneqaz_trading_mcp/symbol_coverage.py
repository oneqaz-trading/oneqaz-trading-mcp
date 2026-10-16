# -*- coding: utf-8 -*-
"""
심볼 커버리지 확인 (2026-10-07 VALID-001 symbol 재오픈)
=====================================================
symbol 을 받는 tool 의 진입부 공용 검사.

종전에는 없는 종목이 경로마다 다른 거짓 답으로 끝났다 (Mac 실호출 실측):
    get_signals(us_stock, XYZQ)          → 0건 "조건에 맞는 시그널이 없습니다"
    get_signals(kr_stock, 삼성전자)       → 0건, 경고 없음 (종목명 → 코드 해석 없음)
    get_signals(crypto, AAPL)            → 0건, 시장 착오 안내 없음
    get_signal_detail(crypto, NOTACOIN)  → signal_stale + retryable=true (AI 가 재시도)
    explain_decision(us_stock, XYZQ)     → 30초 뒤 verdict=UNCLEAR
0건은 "지금 신호가 없다" 라는 **유효한 답**과 구분되지 않는다 — 이 레포에서 제일
비싼 버그 유형(조용한 0)이다. 여기서 한 번 확인해 symbol_not_found(retryable=false)
와 후보를 돌려준다.

존재의 정의: **PG market_{m}.signals 에 그 심볼의 행이 한 번이라도 있는가.**
    - 종목 마스터(external_context.symbol_master)는 정의로 쓰지 않는다 — 수집기 부산물이라
      쓰레기 행이 섞여 있다 (실측: coin_market 에 005930 이 CRYPTO 로 2026-03 부터 잔존).
      마스터는 **이름 → 코드 후보** 를 찾는 데만 쓰고, 후보도 signals 프로브로 재확인한다.
    - 상장폐지 등으로 과거 행만 있는 심볼은 '존재' 로 본다 → 하류의 신선도 표기가 처리.
비용: 심볼 인덱스 1회 프로브 (실측 1~12ms, 3시장 모두 symbol 선두 인덱스 보유).

실패 정책: 프로브 자체가 실패(DB 장애)하면 **검사를 건너뛴다**(경고 로그).
    DB 장애를 "그런 종목 없음" 으로 단정하는 쪽이 더 나쁜 거짓이기 때문이다.
    하류 조회가 같은 장애로 실패하면 그 에러가 그대로 나간다.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("MarketMCP")

_MARKET_SCHEMA = {"crypto": "market_coin", "kr_stock": "market_kr", "us_stock": "market_us"}
# symbol_master.market_id — 시그널 유니버스 계열 / 구조분석(ETF·지수 바스켓) 계열
_MASTER_SIGNAL_MARKET = {"crypto": "coin_market", "kr_stock": "kr_market", "us_stock": "us_market"}
_MASTER_STRUCTURE_MARKET = {"crypto": "coin_structure", "kr_stock": "kr_structure", "us_stock": "us_structure"}
_MASTER_TO_MARKET = {v: k for k, v in _MASTER_SIGNAL_MARKET.items()}
_MASTER_TO_MARKET.update({v: k for k, v in _MASTER_STRUCTURE_MARKET.items()})
_MARKET_LABEL = {"crypto": "crypto", "kr_stock": "Korean stocks", "us_stock": "US stocks"}

# 마스터 행 신선도 — 수집기가 매 사이클 upsert 하므로 활성 유니버스는 수 시간 내 갱신된다
# (2026-10-07 실측: 2일 내 갱신 coin 461 / kr 349 / us 503 = 각 시장 활성 유니버스 크기).
# 오래된 행(폐지·오분류 잔재)은 후보에서 뺀다.
_MASTER_FRESH_DAYS = 3

_POS_TTL = 600.0   # 존재 확인 캐시 — 심볼은 사라지지 않는다
_NEG_TTL = 60.0    # 부재 캐시 — 신규 상장 반영이 늦지 않게 짧게
_cache: Dict[Tuple[str, str], Tuple[float, bool]] = {}
_lock = threading.Lock()


def normalize_symbol(raw: Any) -> str:
    """대소문자·KRW 마켓 접두/접미 정규화 (mcps.config.get_signal_db_path 와 같은 규칙)."""
    s = str(raw or "").strip().upper()
    for token in ("KRW-", "-KRW", "_KRW"):
        s = s.replace(token, "")
    return s


def _row_get(row: Any, key: str, idx: int) -> Any:
    if row is None:
        return None
    try:
        return row[key]
    except (KeyError, TypeError, IndexError):
        return row[idx]


def _probe(market: str, symbol: str) -> Optional[bool]:
    """market_{m}.signals 에 심볼 행이 있는가. DB 실패 시 None."""
    key = (market, symbol)
    now = time.time()
    with _lock:
        hit = _cache.get(key)
        if hit and hit[0] > now:
            return hit[1]
    schema = _MARKET_SCHEMA.get(market)
    if not schema or not symbol:
        return False
    try:
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
        conn = open_schema_connection(schema, readonly=True)
        try:
            row = conn.execute(
                f"SELECT 1 FROM {schema}.signals WHERE symbol = ? LIMIT 1", (symbol,)
            ).fetchone()
        finally:
            conn.close()
    except Exception as e:  # 운영성 장애 — 검사 생략 (모듈 docstring 실패 정책)
        logger.warning("symbol_coverage probe failed (%s/%s): %s", market, symbol, e)
        return None
    exists = row is not None
    with _lock:
        _cache[key] = (now + (_POS_TTL if exists else _NEG_TTL), exists)
    return exists


def symbol_exists(market: str, symbol: str) -> Optional[bool]:
    """공개 API — True/False, DB 실패 시 None (판정 불가)."""
    return _probe(market, normalize_symbol(symbol))


def lookup_names(raw: str, markets: Optional[List[str]] = None, limit: int = 8) -> List[Dict[str, Any]]:
    """종목명(한/영) 또는 코드로 마스터 후보 조회. 정확 일치 우선, 그다음 접두 일치.

    반환 행: {master_market, market_id, symbol, name_ko, name_en, match}
    signals 존재 여부는 확인하지 않는다 — 호출부가 용도에 맞게 거른다.
    """
    q = str(raw or "").strip()
    if not q:
        return []
    mkts = markets or list(_MARKET_SCHEMA)
    master_ids = [_MASTER_SIGNAL_MARKET[m] for m in mkts] + [_MASTER_STRUCTURE_MARKET[m] for m in mkts]
    try:
        from oneqaz_trading_mcp.shared.db.compat import open_schema_connection
        conn = open_schema_connection("external_context", readonly=True)
        try:
            rows = conn.execute(
                "SELECT market_id, symbol, name_ko, name_en, "
                "       CASE WHEN upper(symbol) = upper(?) THEN 0 "
                "            WHEN name_ko = ? OR lower(name_en) = lower(?) THEN 1 "
                "            ELSE 2 END AS rank "
                "  FROM external_context.symbol_master "
                " WHERE market_id = ANY(?) "
                "   AND updated_at > now() - make_interval(days => ?) "
                "   AND (upper(symbol) = upper(?) OR name_ko = ? OR lower(name_en) = lower(?) "
                "        OR (char_length(?::text) >= 2 AND (name_ko LIKE ? OR lower(name_en) LIKE lower(?)))) "
                " ORDER BY rank, char_length(coalesce(name_ko, name_en, symbol)), symbol "
                " LIMIT ?",
                (q, q, q, master_ids, _MASTER_FRESH_DAYS, q, q, q, q, q + "%", q + " %", limit),
            ).fetchall()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("symbol_coverage name lookup failed (%r): %s", q, e)
        return []
    out = []
    for r in rows:
        master = _row_get(r, "market_id", 0)
        out.append({
            "master_market": master,
            "market_id": _MASTER_TO_MARKET.get(master),
            "symbol": _row_get(r, "symbol", 1),
            "name_ko": _row_get(r, "name_ko", 2),
            "name_en": _row_get(r, "name_en", 3),
            "match": ("code", "name", "name_prefix")[min(int(_row_get(r, "rank", 4) or 0), 2)],
            "structure_only": str(master or "").endswith("_structure"),
        })
    return out


def _display_name(row: Dict[str, Any]) -> Optional[str]:
    for k in ("name_ko", "name_en"):
        v = row.get(k)
        if v and str(v).upper() != str(row.get("symbol") or "").upper():
            return v
    return None


def find_candidates(market: str, raw: str, symbol: str) -> List[Dict[str, Any]]:
    """없는 심볼에 대한 후보 — (1) 코드 보정 (2) 종목명 (3) 다른 시장 (4) 구조분석 바스켓."""
    cands: List[Dict[str, Any]] = []
    seen = set()

    def _add(mkt: str, sym: str, reason: str, name: Optional[str] = None) -> None:
        if (mkt, sym) in seen:
            return
        seen.add((mkt, sym))
        c = {
            "market_id": mkt,
            "symbol": sym,
            "reason": reason,
            "call": f"get_signals(market_id='{mkt}', symbol='{sym}', interval='combined', limit=1)",
        }
        if name:
            c["name"] = name
        cands.append(c)

    # (1) 한국 종목코드 앞자리 0 누락 ("5930" → "005930")
    if market == "kr_stock" and symbol.isdigit() and len(symbol) < 6:
        padded = symbol.zfill(6)
        if _probe("kr_stock", padded):
            _add("kr_stock", padded, "zero_padded_code")

    # (2) 종목명 → 코드 (모든 시장 — 시장 착오와 이름 입력을 함께 잡는다)
    structure_rows: List[Dict[str, Any]] = []
    for row in lookup_names(raw):
        if row["structure_only"]:
            structure_rows.append(row)
            continue
        mkt, sym = row["market_id"], normalize_symbol(row["symbol"])
        if mkt and sym and _probe(mkt, sym):
            reason = "name_match" if row["match"] != "code" else (
                "same_symbol_other_market" if mkt != market else "code_match")
            _add(mkt, sym, reason, _display_name(row))
        if len(cands) >= 5:
            break

    # (3) 같은 코드가 다른 시장에 있는가 (crypto 에 AAPL 등)
    for other in _MARKET_SCHEMA:
        if other != market and _probe(other, symbol):
            _add(other, symbol, "same_symbol_other_market")

    # (4) 시그널 유니버스엔 없지만 시장구조 ETF/지수 바스켓에 있는 경우 (us SPY 등)
    for row in structure_rows:
        if normalize_symbol(row["symbol"]) != symbol or not row["market_id"]:
            continue
        mkt = row["market_id"]
        key = (mkt, f"structure:{symbol}")
        if key in seen or (mkt, symbol) in seen:  # 같은 시장 시그널 후보가 이미 있으면 생략
            continue
        seen.add(key)
        name = _display_name(row)
        cands.append({
            "market_id": mkt,
            "symbol": symbol,
            "reason": "market_structure_basket_only",
            "name": name,
            "note": (
                f"{symbol}{f' ({name})' if name else ''} is tracked in the {mkt} market-structure "
                "ETF/index basket, not as a per-symbol research signal."
            ),
            "call": f"fetch(id='resource:market://{mkt}/structure')",
        })
    return cands[:6]


def check_symbol(market: str, raw_symbol: Any, tool: str = "") -> Optional[Dict[str, Any]]:
    """None = 통과(존재 또는 판정 불가) / dict = symbol_not_found mcp_error.

    market 은 정규화된 값(crypto/kr_stock/us_stock)이어야 한다.
    """
    symbol = normalize_symbol(raw_symbol)
    if market not in _MARKET_SCHEMA or not symbol:
        return None
    exists = _probe(market, symbol)
    if exists is not False:  # True 또는 None(판정 불가 — 검사 생략)
        return None

    from oneqaz_trading_mcp.resources.resource_response import mcp_error, MCPErrorCode, MCPErrorAction
    raw = str(raw_symbol).strip()
    cands = find_candidates(market, raw, symbol)
    label = _MARKET_LABEL.get(market, market)
    reason = f"'{raw}' is not a covered symbol in {market} ({label})."
    if cands:
        top = cands[0]
        if top.get("reason") == "market_structure_basket_only":
            reason += " " + top["note"]
        else:
            nm = f" ({top['name']})" if top.get("name") else ""
            reason += f" Did you mean {top['symbol']}{nm} in {top['market_id']}?"
        action_value = top["call"]
    else:
        reason += (
            " No matching code or name was found in any OneQAZ market — it is outside coverage. "
            "Korean stocks use 6-digit KRX codes (e.g. 005930); crypto uses the base ticker (e.g. BTC)."
        )
        action_value = f"get_signals(market_id='{market}', interval='combined')"
    return mcp_error(
        MCPErrorCode.SYMBOL_NOT_FOUND,
        reason,
        action=MCPErrorAction.FALLBACK,
        action_value=action_value,
        fallback_tool=action_value,
        market_id=market,
        symbol=raw,
        normalized_symbol=symbol,
        suggestions=cands,
        coverage_definition=(
            "covered = the symbol has at least one row in OneQAZ's research-signal store for this "
            "market. This is a permanent condition for the given input, not a transient delay — "
            "do not retry the same call."
        ),
        **({"tool": tool} if tool else {}),
    )
