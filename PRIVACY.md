# Privacy

This document covers two different situations:

1. **You call the hosted OneQAZ service** at `api.oneqaz.com` (directly or through an AI client such
   as Claude, ChatGPT or Gemini). OneQAZ processes your request data as described in
   [§1](#1-the-hosted-service-apioneqazcom).
2. **You run this package yourself.** OneQAZ receives nothing from your deployment — see
   [§2](#2-self-hosting-this-package).

---

## 1. The hosted service (`api.oneqaz.com`)

**The authoritative policy is <https://api.oneqaz.com/privacy>** (served by this package's
`health_route.py`, same text). This section mirrors it; if the two ever differ, the hosted page
governs. Last updated: **2026-10-07**.

OneQAZ is a research-information service. Outputs are paper-trading research signals, **not
investment advice**. OneQAZ does not execute trades, hold funds, or accept financial transactions.

### What we collect

We do **not** require user accounts and do not collect names, emails or payment details from callers.
For each request we log:

| Item | Purpose / detail |
|---|---|
| IP address | Rate limiting, abuse prevention, regional routing. For requests routed through Cloudflare this is the client address Cloudflare reports. |
| User-agent | To identify the client type (Claude, ChatGPT, Gemini, …). |
| Tool / resource name and request type | e.g. `get_daily_brief`. |
| Outcome metadata | Success/failure, HTTP status, JSON-RPC error code, a short error description (≤ 200 characters; input values echoed back by validation errors are redacted), latency (ms), and the shape of the result (rows returned, total available, truncated / empty). |
| Derived session key | A hash of IP + user-agent + a 30-minute bucket. No cookie or other persistent identifier is set on your client. |
| API key (if provided) | Used to determine your rate-limit tier and access level. With each request we record the resolved tier and a one-way fingerprint of the key (first 16 hex characters of its SHA-256 hash) so usage can be attributed per key. **The key itself is never written to request logs.** |
| Whitelisted request parameters | A limited summary of tool arguments (e.g. `symbol`, `market`, `interval`, `category`, date ranges, result limits, identifiers) and, for the `search` tool, the search keyword truncated to 60 characters. Calls without arguments are recorded as such. Argument content outside the whitelist is **not** stored. |
| MCP client & protocol metadata | `clientInfo` name/version sent at `initialize`, the MCP protocol version (`MCP-Protocol-Version` header or the `initialize` request), the `Accept` header (first 200 characters), the protocol session id header if your client sends one, call order within a session, and request / response payload sizes. |
| Traffic classification | A derived label (crawler / operator-self-test / external) used to keep aggregate statistics honest. |

We do not store the contents of responses we send you.

### How we use it

Operate and secure the service (rate limiting, abuse prevention, debugging) · diagnose client and
protocol compatibility problems · measure aggregate usage to improve the product · enforce access
tiers and attribute usage to API keys. We do **not** sell your data or build advertising profiles.

### Storage & retention

Request logs are stored on infrastructure operated by OneQAZ, including internal analytics copies.

- **IP addresses, and the session key derived from them, are kept for up to 12 months.** After that
  they are irreversibly replaced using a one-way transformation whose random key is discarded after
  each run, so the remaining records can no longer be linked to an IP address.
- The rest of each request record (tool name, whitelisted parameters, outcome, client and protocol
  metadata) is kept as usage history. Once the IP-derived fields are replaced it no longer identifies
  you, except that requests made with an API key remain attributable to that key.
- If you were issued an API key, the contact email attached to it is kept while the key is issued to
  you. You can ask us to delete it at any time.

### Third parties

Request data is not shared with third parties for their own purposes. Traffic is routed through
Cloudflare (CDN / DDoS protection) as a data processor. Returned market data is derived from public
market sources and OneQAZ's own analysis. When an AI client calls OneQAZ on your behalf, your request
reaches us through that client's platform; every response carries `disclaimer`,
`is_investment_advice=false` and `data_classification=research_information_only`.

### Your choices & contact

Because there are no user accounts, the simplest way to stop data processing is to stop calling the
service. For questions or requests (including deletion) about data associated with your IP address
or API key: **contact@oneqaz.com**.

---

## 2. Self-hosting this package

When you run `oneqaz-trading-mcp` yourself, **you** are the operator of everything it records.

- **No telemetry to OneQAZ.** The package does not send request data, usage logs or analytics to
  OneQAZ. The `oneqaz.com` URLs that appear in responses are links, not calls.
- **Outbound network calls** made by the server itself:
  - the vLLM endpoint you configure in `VLLM_BASE_URL` (optional; used to phrase market-state
    narratives — it receives market-state summaries, not your callers' request data);
  - `https://api.alternative.me/fng/` (public crypto Fear & Greed index; a plain GET with no caller
    data).
- **Request logging goes to your own PostgreSQL.** The request middleware writes request metadata of
  the kind listed in §1 (the exact column set depends on the package version — see `analytics.py`)
  to the `mcp_analytics.mcp_requests` table of the database you point it at. If that table does not
  exist, the write fails and is logged as a warning; nothing is stored elsewhere.
- **Retention is yours to implement.** The 12-month IP anonymization job described in §1 runs in
  OneQAZ's own infrastructure and is not part of this package.
- **Replace `/privacy`.** The bundled `/privacy` route serves *OneQAZ's hosted-service policy*. If you
  expose your deployment to other people, serve your own privacy notice there instead.
