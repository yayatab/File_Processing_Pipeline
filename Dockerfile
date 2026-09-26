FROM python:3.12-slim

WORKDIR /app

# Install uv
RUN pip install uv

COPY pyproject.toml uv.lock ./

# Install dependencies using uv
RUN uv sync --frozen

COPY . .

ENV PYTHONPATH=/app
