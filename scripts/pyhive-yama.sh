#!/usr/bin/env bash
# PyHive 0.7 + Spark 3.5 Thrift: DDL cevabı boş kolon listesiyle gelir, PyHive
# 'NoneType object is not iterable' verir. docker/dbt.Dockerfile'daki yamanın aynısı,
# bu sefer .venv içindeki pyhive'a. 'uv sync' sonrası bir kez çalıştırın (tekrar çalışması zararsız).
set -euo pipefail
cd "$(dirname "$0")/.."
f=$(uv run --no-sync python -c "import pyhive.hive as h; print(h.__file__)")
if grep -q "response.results.columns or \[\]" "$f"; then echo "PyHive yaması zaten uygulanmış: $f"; exit 0; fi
grep -q "zip(response.results.columns, schema)" "$f" || { echo "HATA: yama kalıbı bulunamadı: $f" >&2; exit 1; }
sed -i.bak 's/zip(response.results.columns, schema)/zip(response.results.columns or [], schema)/' "$f" && rm -f "$f.bak"
grep -q "response.results.columns or \[\]" "$f" || { echo "HATA: yama uygulanamadı: $f" >&2; exit 1; }
echo "PyHive yaması uygulandı: $f"
