# Privacy

This document covers two different situations:

1. **You call the hosted OneQAZ service** at `api.oneqaz.com` (directly or through an AI client such
   as Claude, ChatGPT or Gemini). OneQAZ processes your request data as described in
   [§1](#1-the-hosted-service-apioneqazcom).
2. **You run this package yourself.** OneQAZ receives nothing from your deployment — see
   [§2](#2-self-hosting-this-package).

---

## 1. The hosted service (`api.oneqaz.com`)

**The full policy (English / 한국어) is <https://api.oneqaz.com/privacy>**, published identically at
<https://oneqaz.com/privacy> and served by this package's `health_route.py`. It is written to meet
Korea's Personal Information Protection Act (PIPA) and to inform users in the EU/EEA and UK.
This section is a summary only; the hosted page governs. Last updated: **2026-10-07**.

OneQAZ is a research-information service. Outputs are paper-trading research signals, **not
investment advice**. OneQAZ does not execute trades, hold funds, or accept financial transactions.

| Topic | Summary |
|---|---|
| Items | Per request: IP address, user-agent, tool/resource name, outcome metadata (HTTP / JSON-RPC codes, short redacted error text, latency, result shape), derived session key, API-key fingerprint + tier (never the key), whitelisted arguments (`search` keyword ≤ 60 chars), MCP client & protocol metadata, traffic class. API-key holders: contact email. No accounts, no names, no payment details, no response contents. |
| Purpose & legal basis | Operate and secure the service, diagnose compatibility, measure aggregate usage, manage API keys. Request logs: legitimate interest (PIPA Art. 15(1)(6); GDPR Art. 6(1)(f)). Key-holder email: issuing the key you asked for (PIPA Art. 15(1)(4); GDPR Art. 6(1)(b)). Not sold, not used for advertising profiles, AI-model training or automated decisions. |
| Retention & destruction | IP addresses and IP-derived session keys: up to **12 months**, then irreversibly replaced (one-way transformation, random key discarded each run). Key-holder email: until the key is revoked or you ask for deletion. Database backups rotate out within about four weeks. |
| Outsourcing & overseas transfer | Cloudflare, Inc. (US — CDN, DDoS protection, tunnel), GitHub, Inc. (US — website hosting), BunnyWay d.o.o. (Slovenia — web fonts). Basis: PIPA Art. 28-8(1)(3). Contacts and items are listed on the hosted page. |
| Third parties | None, except where the law requires it. |
| Cookies | None. The website keeps only your language choice in the browser's local storage. |
| Your rights | Access, correction, deletion, suspension (PIPA Arts. 35–37; GDPR rights where applicable) via **contact@oneqaz.com**; answered within 10 days. |
| Privacy officer | OneQAZ Privacy Office — contact@oneqaz.com |
| Remedies (Korea) | Personal Information Dispute Mediation Committee 1833-6972 · KISA Infringement Report Center 118 · Supreme Prosecutors' Office 1301 · National Police Agency 182 |

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
