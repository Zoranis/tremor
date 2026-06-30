# Tremor gdelt-vendor mock.
#
# Single image, two roles (selected by `command:` in compose.yml):
#   * data-init     → python /app/download_and_curate.py /cache
#   * gdelt-vendor  → uvicorn app.main:app --host 0.0.0.0 --port 8000
FROM python:3.13-slim

# uv for fast, reproducible installs. We pin via the lockless `uv pip install`
# path on top of the project's pyproject; no separate lockfile shipped.
RUN pip install --no-cache-dir uv==0.5.11

WORKDIR /app

# System deps:
#  * ca-certificates — TLS to data.gdeltproject.org.
#  * curl            — ad-hoc debugging.
# (No `unzip`: GDELT ZIPs use standard Deflate, which Python's stdlib zipfile
# decodes natively.)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first (cacheable layer).
COPY pyproject.toml /app/pyproject.toml
RUN uv pip install --system --no-cache -r /app/pyproject.toml

# Then the code.
COPY download_and_curate.py /app/download_and_curate.py
COPY app /app/app

# No default CMD — compose picks one per service.
