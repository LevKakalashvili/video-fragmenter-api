FROM python:3.11-slim-bullseye

WORKDIR /backend

COPY ./ /backend

RUN pip install uv

RUN uv pip install --system --no-cache-dir -r /backend/requirements.txt