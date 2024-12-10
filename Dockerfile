# Base image
FROM python:3.10-slim AS base

# Dev level
FROM base AS dev
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config/ /build/config/
COPY src/ /build/src/
COPY tests/ /build/tests/
RUN python -m pytest tests/test_extract_and_load.py

# Prod level
FROM base AS prod
WORKDIR /build
COPY --from=dev /build /build/
RUN pip install --no-cache-dir -r requirements.txt
COPY cli.py /build/cli.py
ENTRYPOINT ["python", "cli.py"]
