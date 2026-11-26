#!/usr/bin/env bash
# Canlı derste kurtarma: canli/olist içindeki models/macros/tests + dbt_project.yml'ı
# çözümdeki perde-NN tag'inin hâliyle değiştirir.  Kullanım: scripts/perdeye-atla.sh 05
set -euo pipefail
nn=${1:?kullanım: scripts/perdeye-atla.sh NN  (ör. 05)}
root=$(cd "$(dirname "$0")/.." && pwd)
dst="$root/canli/olist"
[ -f "$dst/dbt_project.yml" ] || { echo "HATA: $dst yok; önce 'uv run dbt init olist' (CANLI.md Perde 01)" >&2; exit 1; }
git -C "$root" rev-parse -q --verify "refs/tags/perde-$nn" >/dev/null || { echo "HATA: perde-$nn tag'i yok" >&2; exit 1; }
rm -rf "$dst/models" "$dst/macros" "$dst/tests"
git -C "$root" archive "perde-$nn" models macros tests dbt_project.yml | tar -x -C "$dst"
# Çözüm projesinin adı olist_demo, canlıda 'dbt init olist' ile adı olist
grep -rl olist_demo "$dst/models" "$dst/macros" "$dst/tests" "$dst/dbt_project.yml" | xargs perl -pi -e 's/olist_demo/olist/g' || true
echo "canli/olist artık perde-$nn hâlinde. Şimdi: uv run dbt build"
