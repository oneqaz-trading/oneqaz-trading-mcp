# Contributing to oneqaz-trading-mcp

Thanks for your interest in contributing.

## How to Contribute

### Bug Reports

Open a [GitHub Issue](https://github.com/oneqaz-trading/oneqaz-trading-mcp/issues) with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Your environment (Python version, OS, FastMCP version)

### Feature Requests

Open an issue with the `enhancement` label. Describe the use case, not just the solution.

### Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/your-feature`)
3. Make your changes
4. Run tests: `pytest`
5. Submit a PR against `main`

### Code Style

- Python 3.11+
- Type hints on public functions
- Use `logging` module (no `print()`)
- English for code identifiers
- Parameterized SQL queries only (no string formatting)

### Commit Messages

Follow conventional commits:
- `feat:` new feature
- `fix:` bug fix
- `docs:` documentation
- `chore:` maintenance

### Architecture

- **Resources** (read-only): Go in `src/oneqaz_trading_mcp/resources/`
- **Tools** (read-only queries with parameters): Go in `src/oneqaz_trading_mcp/tools/`
- All tools must include `annotations={"readOnlyHint": True, "destructiveHint": False}`
- Every response must include an `_llm_summary` field

## Questions?

Open an issue or reach out at support@oneqaz.com.
