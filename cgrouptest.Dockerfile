# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the cgrouptest.Dockerfile reference guide at
# https://docs.docker.com/engine/reference/builder/

ARG PYTHON_VERSION=3.7
FROM python:${PYTHON_VERSION} AS base

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Create a non-privileged user that the app will run under.
# See https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#user
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser


COPY . .
RUN --mount=type=cache,target=/root/.cache/pip,from=pip-cache pip install -e '.[dev]'

# Switch to the non-privileged user to run the application.
USER appuser


# Run the application.
CMD pytest tests/unit/neptune/legacy/internal/hardware/gauges/test_cpu_gauges.py tests/unit/neptune/legacy/internal/hardware/gauges/test_memory_gauges.py tests/unit/neptune/legacy/internal/hardware/resources/test_system_resource_info_factory.py
