FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY src/ src/

RUN pip install --no-cache-dir .

# Initialize sample data
RUN oneqaz-trading-mcp init

EXPOSE 8010

CMD ["oneqaz-trading-mcp", "serve"]
