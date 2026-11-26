# dbt + Spark demo · "Gecikme memnuniyeti düşürüyor mu?"

VBO Data Engineering Bootcamp, 11. hafta, dbt dersi için canlı demo reposu.

> **Spark hesaplar, dbt hesabı güvenilir kılar.**
> dbt bir motor değil: SQL'i Spark Thrift Server'a gönderir; üstüne DAG, test, doküman, seçim ve kontrat ekler.
> Her dbt komutunun gerçek işini Spark UI'da (http://localhost:4040) görürsünüz.

- **Canlı ders:** [CANLI.md](CANLI.md): boş `canli/` klasöründe `dbt init`'ten başlayıp adım adım (yerel dbt, uv).
- **Çözüm:** repo kökündeki bitmiş proje; her perdenin bitmiş hâli bir git tag'i: `perde-00` ... `perde-10`.
- **Yedek akış:** [DEMO.md](DEMO.md): her şey container içinde (`./dbt ...`).

## Mimari

```
┌──────────────── docker compose ────────────────┐
│  dbt (python:3.11 + dbt-core/dbt-spark)        │
│    ./dbt <komut>  ── thrift:10000 ──►  spark   │
│    docs: :8080                       (Thrift   │
│                                       Server,  │
│  ./data (CSV, salt okunur) ─────────► UI:4040) │
│  Derby metastore + warehouse: named volume     │
└────────────────────────────────────────────────┘
```

| Bileşen | Sürüm |
|---|---|
| Spark | `apache/spark:3.5.5` (Thrift Server, `local[*]`, driver 3 GB) |
| dbt-core | 1.11.17 |
| dbt-spark | 1.11.1 (`method: thrift`, PyHive) |

## Kurulum

Gereksinimler: Docker (Compose v2), `uv` (yoksa script Kaggle CLI'ı geçici bir konteynerde çalıştırır), ~3 GB boş RAM.

### 1. Kaggle token

Olist verisi Kaggle'dan indirilir:

1. https://www.kaggle.com → sağ üst profil → **Settings** → **API** bölümü.
2. Yeni bir API token oluşturun ve değerini kopyalayın.
   Eski usul indirilen `kaggle.json` da olur: içindeki `username` ve `key` değerlerini kullanın.
3. Token'ı `.env` dosyasına yazın:

```bash
cp .env.example .env
# .env içinde KAGGLE_API_TOKEN=...  ya da  KAGGLE_USERNAME=... + KAGGLE_KEY=...
```

`.env` git'e girmez (`.gitignore`).

### 2. Ayağa kaldırma ve veri

```bash
docker compose up -d --build --wait   # ilk sefer ~1-2 dk, sonra ~15 sn
./scripts/veri-indir.sh               # ~45 MB zip, ./data altına açılır
./dbt debug                           # "All checks passed!"
./dbt run-operation create_raw_tables # raw.* external tabloları
./dbt build                           # tüm modeller + testler
```

`./dbt` küçük bir kısayol: `docker compose exec dbt dbt "$@"`.

### 3. Yerel dbt (uv) · Spark "uzak sunucu" gibi

dbt host'ta `.venv` içinde çalışır, Spark Thrift Server'a `localhost:10000` üzerinden bağlanır (compose portu açıyor).

```bash
uv sync                    # .venv: Python 3.12, dbt-core 1.11.17, dbt-spark[PyHive] 1.11.1 (pyproject.toml)
scripts/pyhive-yama.sh     # uv sync'ten sonra bir kez (PyHive yaması, aşağıya bakın)
uv run dbt debug --profiles-dir . --target local
uv run dbt build --profiles-dir . --target local
```

`profiles.yml` iki target taşır: `dev` (container, host `spark`) ve `local` (host `localhost`). `~/.dbt` hiç kullanılmaz.

**Canlı ders klasörü:** `canli/` (git'e girmez). [CANLI.md](CANLI.md) `rm -rf canli && mkdir canli` ile başlar; `uv run dbt init olist`
profili `canli/profiles.yml`'a yazar (`DBT_PROFILES_DIR`), modeller Spark'ta `canli` şemasına kurulur (çözüm `demo` şemasında).
Takılırsanız `scripts/perdeye-atla.sh NN`: `canli/olist`'i çözümün `perde-NN` hâline getirir (proje adı `olist_demo` → `olist` çevrilir).

### VS Code

dbt Labs'in resmî VS Code eklentisi (*dbt*) dbt Fusion motoruyla çalışır; Fusion şu an Spark adaptörünü desteklemiyor (muhtemelen hatalar bundan;
destek listesi için docs.getdbt.com'a bakın). Bu projede o eklentiyi kapatın. **dbt Power User** eklentisi dbt Core ile çalışır:
Python yorumlayıcısı olarak `uv sync`'in kurduğu `.venv/bin/python`'ı seçin.

### Lisans notu

Olist verisi **CC BY-NC-SA 4.0** lisanslı (Olist / Kaggle: `olistbr/brazilian-ecommerce`). Yeniden dağıtım koşullu olduğu için veri bu repoda **yok**; herkes kendi Kaggle hesabıyla indirir. `data/` klasörü git'e girmez.

## Proje yapısı

```
models/
  staging/       stg_* view'lar + sources.yml + testler
  intermediate/  int_order_delays (sipariş başına gecikme) + unit test
  marts/         mart_delivery_satisfaction (tablo, kontratlı) + exposure
macros/          create_raw_tables (run-operation), gun_farki
tests/           singular test
scripts/         veri-indir.sh, pyhive-yama.sh, perdeye-atla.sh
pyproject.toml   yerel dbt (uv)
docker/          dbt imajı
```

## Perdeler (git tag'leri)

| Tag | İçerik |
|---|---|
| `perde-00` | İskelet: compose, profil, boş proje; `dbt debug` geçer |
| `perde-01` | init + profil (dosya değişikliği yok, bağlantı gösterimi) |
| `perde-02` | `create_raw_tables` makrosu, `sources.yml`, 4 `stg_` modeli |
| `perde-03` | `int_order_delays` + `mart_delivery_satisfaction` (`ref()`) |
| `perde-04` | Materialization: staging/int = view, mart = parquet table |
| `perde-05` | Generic + singular testler; `review_id` unique → `severity: warn` |
| `perde-06` | Açıklamalar, docs + lineage |
| `perde-07` | Seçiciler, `state:modified+` (int modeline yeni kolon) |
| `perde-08` | `unit_tests:` ile gecikme mantığı testi |
| `perde-09` | Mart kontratı + "Teslimat memnuniyeti panosu" exposure |
| `perde-10` | Source freshness + `gun_farki` makrosu + Jinja döngüsü |

Bir perdenin bitmiş hâline atlamak (yapıştırılmış dosyalar varsa da çalışır):

```bash
git checkout -f perde-05 && git clean -fdq models macros tests
./dbt build
```

## Bilinen sınırlamalar / notlar

- **PyHive yaması:** PyHive 0.7, Spark 3.5 Thrift Server'ın DDL cevabını (boş kolon listesi) okuyamıyor ve `'NoneType' object is not iterable` veriyor. dbt unit test'leri `run_query` ile DDL çalıştırdığı için `docker/dbt.Dockerfile` içinde tek satırlık bir yama var; yerel `.venv` için aynısı `scripts/pyhive-yama.sh`.
- **Constraint'ler:** Spark'ta parquet tablolarda `not_null` vb. constraint'ler uygulanmaz (Delta Lake gerekir). Kontrat burada build anında kolon adı + tip kontrolü yapar.
- **Freshness:** Veri 2018'de bittiği için `dbt source freshness` bilerek `ERROR STALE` verir.
- **Metastore:** Derby, tek Thrift Server süreci için yeterli. Sıfırlamak için `docker compose down -v`.

## Sorun giderme

| Belirti | Çözüm |
|---|---|
| `./dbt debug` bağlanamıyor | `docker compose ps` → spark `healthy` mi? Değilse `docker compose logs spark` |
| `Table or view not found: raw.orders` | `./dbt run-operation create_raw_tables` |
| `Path does not exist: /data/...` | `./scripts/veri-indir.sh` çalıştırın |
| Docs açılmıyor | `./dbt docs generate` sonra `./dbt docs serve --host 0.0.0.0 --port 8080 --no-browser` |
| Her şeyi sıfırla | `docker compose down -v && docker compose up -d --wait` |
