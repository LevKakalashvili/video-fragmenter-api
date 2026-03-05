FROM python:3.11-slim-bullseye

WORKDIR /backend

COPY ./ /backend

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv

RUN uv pip install --system --no-cache-dir -r /backend/requirements.txt
