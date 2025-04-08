#!/bin/bash

# remove containers antigos
docker rm -f worker-dispatcher 2>/dev/null || true
docker rm -f worker-processor 2>/dev/null || true

docker network create net

# cria o processor
# docker build -f worker_processor/Dockerfile -t worker-processor .

docker compose up -d --build