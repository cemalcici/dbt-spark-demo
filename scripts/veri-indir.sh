#!/usr/bin/env bash
# Olist verisini Kaggle'dan ./data altına indirir. Token .env dosyasından okunur.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo "HATA: .env yok. 'cp .env.example .env' yapıp Kaggle token'ını yazın." >&2
  exit 1
fi
set -a; . ./.env; set +a

if [ -z "${KAGGLE_API_TOKEN:-}" ] && { [ -z "${KAGGLE_USERNAME:-}" ] || [ -z "${KAGGLE_KEY:-}" ]; }; then
  echo "HATA: .env içinde KAGGLE_API_TOKEN ya da KAGGLE_USERNAME + KAGGLE_KEY olmalı." >&2
  exit 1
fi
# Boş değişkenler CLI'ı şaşırtmasın
[ -n "${KAGGLE_API_TOKEN:-}" ] || unset KAGGLE_API_TOKEN
[ -n "${KAGGLE_USERNAME:-}" ] || unset KAGGLE_USERNAME KAGGLE_KEY

mkdir -p data
if command -v uvx >/dev/null 2>&1; then
  # six: bazı kaggle sürümlerinde eksik bağımlılık
  uvx --with six kaggle datasets download -d olistbr/brazilian-ecommerce -p data
else
  # uv yoksa: geçici Python konteyneri içinde Kaggle CLI
  docker run --rm -e KAGGLE_API_TOKEN -e KAGGLE_USERNAME -e KAGGLE_KEY \
    -v "$PWD/data:/data" python:3.11-slim \
    sh -c "pip install -q kaggle && kaggle datasets download -d olistbr/brazilian-ecommerce -p /data"
fi

python3 -m zipfile -e data/brazilian-ecommerce.zip data/
rm -f data/brazilian-ecommerce.zip
ls -lh data/*.csv
