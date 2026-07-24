#!/usr/bin/env bash
# Compatibilidade: prefira `python start.py`
cd "$(dirname "$0")"
exec python start.py "$@"
