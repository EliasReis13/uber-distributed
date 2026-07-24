#!/usr/bin/env bash
# Compatibilidade: prefira `python start.py 1` (ou 2…6)
cd "$(dirname "$0")"
if [ -z "$1" ]; then
  echo "Uso: bash start_server.sh config/servidor_XX.env"
  echo "  ou: python start.py 1"
  exit 1
fi
exec python start.py "$(basename "$1" .env)"
