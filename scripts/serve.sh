#!/usr/bin/env bash
set -euo pipefail
exec uvicorn src.api.main:app --host "${API_HOST:-0.0.0.0}" --port "${API_PORT:-8000}"
