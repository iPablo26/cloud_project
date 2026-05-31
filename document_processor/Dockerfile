# Stage 1: Build & Test
FROM python:3.11-slim as builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Set up virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install uv to speed up installs
RUN pip install --no-cache-dir --upgrade pip uv

# Copy configuration
COPY pyproject.toml .

# Install all dependencies (including dev groups for testing)
RUN uv pip install -r pyproject.toml --extra dev

# Copy files
COPY schema.proto .
COPY src/ ./src/
COPY tests/ ./tests/

# Compile Protobuf
RUN python -m grpc_tools.protoc -I. --python_out=. schema.proto

# Set environment variables for tests
ENV PROJECT_ID=test-project
ENV DATASET_ID=test-dataset
ENV TABLE_ID=test-table

# Run tests
RUN pytest

# Stage 2: Final Production Image
FROM python:3.11-slim

WORKDIR /app

# Copy dependencies and source from builder
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app /app

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

EXPOSE 8080

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "8", "--timeout", "0", "src.main:app"]
