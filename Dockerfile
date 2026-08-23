FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY src/ src/

RUN pip install --no-cache-dir .

# PostgreSQL-only since 0.4.0: pass DB_BACKEND=postgres + PG_* env at runtime
EXPOSE 8010

CMD ["oneqaz-trading-mcp", "serve"]
