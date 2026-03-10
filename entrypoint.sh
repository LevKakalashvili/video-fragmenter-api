#!/bin/bash
set -e

if [ "$#" -eq 0 ]; then
  exec python -m src.main
fi

exec "$@"

