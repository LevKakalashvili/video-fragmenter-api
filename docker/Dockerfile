FROM python:3.11-slim-bookworm AS base

ARG PROJECT_DIR=/opt/app
ARG PYTHON_VENV=/opt/.venv

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive
ENV PROJECT_DIR=${PROJECT_DIR}
ENV PYTHON_VENV=${PYTHON_VENV}


FROM base AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


FROM builder AS python-env

ARG REQUIREMENTS_FILE=requirements.txt

RUN python -m venv "${PYTHON_VENV}" && \
    "${PYTHON_VENV}/bin/python" -m pip install --no-cache-dir --upgrade pip setuptools wheel

COPY ${REQUIREMENTS_FILE} /tmp/requirements.txt
RUN "${PYTHON_VENV}/bin/pip" install --no-cache-dir -r /tmp/requirements.txt && \
    rm -f /tmp/requirements.txt


FROM builder AS application

COPY --from=python-env ${PYTHON_VENV} ${PYTHON_VENV}
ENV PATH="${PYTHON_VENV}/bin:${PATH}"

COPY ./entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR ${PROJECT_DIR}
COPY ./src ${PROJECT_DIR}/src

ENTRYPOINT ["/entrypoint.sh"]


FROM application AS local


FROM application AS server

RUN groupadd -r appuser && useradd -r -g appuser appuser && \
    chown -R appuser:appuser ${PROJECT_DIR}

USER appuser
