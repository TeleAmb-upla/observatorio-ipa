FROM python:3.12-slim-trixie AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/


# Keeps Python from generating .pyc files in the container
# Turns off buffering for easier container logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git=1:2.47.3-0+deb13u1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/ 


# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Ensure installed tools can be executed out of the box
ENV UV_TOOL_BIN_DIR=/usr/local/bin

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# create standard paths for db, manifest and logs
RUN mkdir -p /var/lib/observatorio_ipa/manifests /var/log/observatorio_ipa

# Add full project source code and install it
# Installing separately from its dependencies for optimal layer caching
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev


# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Set APP specific environment variables for container
ENV IPA_CONTAINERIZED="true"

# Reset the entrypoint, don't invoke `uv`
# ENTRYPOINT []

# Use supervisor to manage cron

# Healthcheck endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD curl -s -f http://localhost:8080/healthz > /dev/null 2>&1 || exit 1

CMD ["python", "-m", "observatorio_ipa.core.scheduler"]
