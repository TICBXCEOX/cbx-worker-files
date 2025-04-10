#!/bin/bash

# remove containers antigos
docker rm -f worker-dispatcher 2>/dev/null || true

docker network create net

docker compose up -d --build