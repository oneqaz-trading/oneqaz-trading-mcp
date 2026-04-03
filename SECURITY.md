# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅        |
| < 0.1   | ❌        |

## Reporting a Vulnerability

If you discover a security vulnerability in oneqaz-trading-mcp, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, please email: **security@oneqaz.com**

Include:
- Description of the vulnerability
- Steps to reproduce
- Impact assessment (what data/systems are affected)
- Any suggested fixes

## Response Timeline

- **Acknowledgment**: within 48 hours
- **Initial assessment**: within 5 business days
- **Fix or mitigation**: as soon as possible, depending on severity

## Scope

This policy covers:
- The `oneqaz-trading-mcp` PyPI package
- The live API at `api.oneqaz.com/mcp`
- The official Docker image

## Security Design

- **Read-only by design**: All tools and resources are read-only queries against SQLite databases. No tool modifies data.
- **No authentication data stored**: The server does not handle user credentials, API keys, or tokens.
- **Rate limiting**: The live API enforces per-IP rate limits (1,500/day, 30/min) to prevent abuse.
- **Input validation**: All query parameters are validated and sanitized before database queries.
- **No arbitrary SQL**: Queries use parameterized statements only.
