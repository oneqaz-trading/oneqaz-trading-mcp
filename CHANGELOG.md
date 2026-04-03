# Changelog

All notable changes to this project will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/).

## [0.1.5] - 2026-04-03

### Changed
- README rewritten — positioned as "the context layer for financial AI"
- Added "Why OneQAZ" comparison table and AI developer-focused use cases
- Quick Start reordered: Live API first, local install second
- Added SECURITY.md, CHANGELOG.md, CONTRIBUTING.md, CODE_OF_CONDUCT.md
- Tool annotations added (readOnlyHint, title) for MCP spec compliance
- pyproject.toml status updated to Production/Stable

### Improved
- server.json description aligned with new positioning

## [0.1.4] - 2026-04-02

### Added
- Market Coverage section in README (exchanges and symbol universe)
- Rate limiting: 1,500 requests/day + 30 requests/min per IP
- Disclaimer section (not financial advice)
- Live API connection option in README

### Changed
- Repository URL moved to oneqaz-trading org

## [0.1.3] - 2026-04-01

### Added
- 5 MCP prompt templates for guided usage

### Fixed
- PROJECT_ROOT alias in config.py — fixes ImportError on fresh install

## [0.1.2] - 2026-03-31

### Added
- Use Cases, Sample Response, and conversation examples in README

## [0.1.1] - 2026-03-31

### Fixed
- mcp-name ownership tag for MCP Registry
- Build backend configuration

## [0.1.0] - 2026-03-31

### Added
- Initial release
- 19 Resources: global regime, market status, market structure, indicators, signals, external context, unified context, cross-market analysis
- 4 Tool types: trade history, positions, signals, trading decisions
- Multi-market support: crypto (Bithumb), US stocks (S&P 500), Korean stocks (KOSPI 200)
- SQLite-based data layer with configurable paths
- Caching with configurable TTL per resource type
- `_llm_summary` field on every response for AI agent consumption
- CLI: `oneqaz-trading-mcp init` (sample data) and `oneqaz-trading-mcp serve`
- Docker support
- Live API at api.oneqaz.com/mcp
