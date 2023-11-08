FROM almalinux:9

# Reference: https://github.com/hadolint/hadolint/wiki/DL4006

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Configure the build.

ARG PY_EXE=python3.9
ARG PY_PKG=python3

# Configure the environment.

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# Install essential packages and utilities.

USER root
WORKDIR /tmp
RUN true \
    && dnf update -y \
    && dnf install -y --allowerasing \
        ca-certificates \
        curl \
        glibc-langpack-en \
        procps \
        ${PY_PKG}-pip \
    && dnf clean all \
    && rm -rf /var/cache/dnf/* \
    #
    && ${PY_EXE} -m pip install -U --no-cache-dir pip setuptools wheel \
    && true

# Install the application.

COPY poetry.lock pyproject.toml requirements.txt /srv/
RUN ${PY_EXE} -m pip install --no-cache-dir -r /srv/requirements.txt

COPY app /srv/app/

# Configure container startup.

WORKDIR /srv
