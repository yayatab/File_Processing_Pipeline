FROM python:3.12-slim

WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install uv
RUN pip install uv

COPY pyproject.toml uv.lock ./

# Install dependencies using uv
RUN uv sync --frozen

COPY . .

ENV PYTHONPATH=/app

ENTRYPOINT ["uv", "run"]
