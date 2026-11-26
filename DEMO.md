# DEMO · dbt + Spark canlı ders akışı

> **Ana mesaj:** Spark hesaplar, dbt hesabı güvenilir kılar.
> dbt bir motor değildir: SQL'i Spark'a gönderir. Üstüne DAG, test, doküman, seçim ve kontrat ekler.
> Her dbt komutundan sonra **Spark UI'da (http://localhost:4040 → SQL / Jobs)** gerçek işleri gösterin.

**İş sorusu:** *Gecikme memnuniyeti düşürüyor mu?*

Kod bloklarını olduğu gibi kopyalayıp yapıştırın. Tüm komutlar repo kökünden çalışır.
`./dbt <komut>` = `docker compose exec dbt dbt <komut>`.

## Zamanlama (60 dk)

| Perde | Konu | Süre |
|---|---|---|
| 00 | Ön hazırlık (dersten önce) | - |
| 01 | init + profil | ~4 dk |
| 02 | source + staging | ~7 dk |
| 03 | ref() + mart | ~7 dk |
| 04 | materialization | ~5 dk |
| 05 | testler | ~7 dk |
| 06 | docs + lineage | ~5 dk |
| 07 | seçim + build + state | ~7 dk |
| 08 | unit test | ~6 dk |
| 09 | contract + exposure (*opsiyonel*) | ~5 dk |
| 10 | freshness + Jinja (*opsiyonel*) | ~5 dk |

Zaman daralırsa 09 ve 10'u atlayın; `git checkout perde-10` ile bitmiş hâli gösterip üzerinden geçin.

**Kurtarma:** Bir perde bozulursa o perdenin bitmiş hâline atlayın:
`git checkout -f perde-NN && git clean -fdq models macros tests`
- `-f`: sahnede yapıştırdığınız (commit edilmemiş) dosyalar tag'deki dosyalarla çakışır; `-f` onları tag'deki hâliyle ezer.
- `git clean`: geri giderken sonraki perdelerin dosyaları ortada kalmasın (yoksa dbt onları da okur). `data/`, `target/`, `state/` silinmez.

Ardından `./dbt build` modelleri Spark'ta yeniden kurar.

---

## Ön hazırlık (dersten önce, ~5 dk)

Konteynerleri başlatın (ilk seferde dbt imajı build edilir, ~1-2 dk; sonra ~15 sn):

```bash
docker compose up -d --build --wait
```

Veriyi indirin (`.env` içinde Kaggle token'ı olmalı, bkz. README):

```bash
./scripts/veri-indir.sh
```

Başlangıç noktasına dönün (sahnede `perde-00`'dan başlıyoruz):

```bash
git checkout -f perde-00 && git clean -fdq models macros tests
```

Tarayıcıda iki sekme açık olsun: http://localhost:4040 (Spark UI) ve ileride http://localhost:8080 (dbt docs).

> Ham tablolar Perde 02'de `run-operation` ile kurulur. Doğrudan ileri bir perdeye atlarsanız bir kez
> `./dbt run-operation create_raw_tables` çalıştırın (tablolar Spark volume'unda kalıcıdır).

---

## Perde 01 · init + profil

**Amaç:** dbt'nin Spark Thrift Server'a bağlandığını göstermek.

**Neden Spark tek başına yetmez:** Spark SQL'i çalıştırır ama hangi SQL'in hangi sırayla, hangi ortamda çalışacağını bilmez; dbt bu "proje" katmanıdır.

Projede şu an sadece iskelet var (`dbt init` sonrası gibi). Bağlantı ayarı:

**Dosya:** `profiles.yml`

```yaml
# dbt veri tutmaz: sadece Spark Thrift Server'a SQL gönderir.
olist_demo:
  target: dev
  outputs:
    dev:
      type: spark
      method: thrift
      host: spark        # docker compose servis adı
      port: 10000
      user: dbt
      schema: demo       # Spark'ta modellerin yazılacağı veritabanı
      threads: 4
      connect_retries: 3
      connect_timeout: 10
```

**Dosya:** `dbt_project.yml`

```yaml
name: olist_demo
version: "1.0.0"
config-version: 2
profile: olist_demo

model-paths: ["models"]
macro-paths: ["macros"]
test-paths: ["tests"]
seed-paths: ["seeds"]
analysis-paths: ["analyses"]
snapshot-paths: ["snapshots"]

target-path: target
clean-targets: [target, dbt_packages]
```

```bash
./dbt debug
```

```bash
./dbt show --inline "select current_database() as db, version() as spark_surumu"
```

**Ne görülür:** `Connection test: OK connection ok` ve `All checks passed!`. `show` sorgusu Spark sürümünü döndürür.
Spark UI → **SQL** sekmesinde bu sorgu görünür. Vurgu: **dbt veri tutmaz**; dbt konteynerinde tek bir satır veri yok, sadece SQL üretip Spark'a gönderiyor.

Kurtarma: `git checkout -f perde-01 && git clean -fdq models macros tests`

---

## Perde 02 · source + staging

**Amaç:** Ham CSV'leri Spark'a tanıtmak, `source()` ile bildirmek ve temiz `stg_` modelleri kurmak.

**Neden Spark tek başına yetmez:** Spark tabloyu okur ama "bu tablo nereden geliyor, kimler kullanıyor" bilgisini tutmaz; `source()` bu bağı kayıt altına alır.

Önce ham tabloları kuran makro (dbt'nin içinden Spark'a DDL gönderiyoruz):

**Dosya:** `macros/create_raw_tables.sql`

```sql
{# Ham CSV'leri Spark'a external table olarak tanıtır. Veri kopyalanmaz; Spark /data altındaki dosyayı okur. #}
{# DDL sonuç döndürmediği için run_query yerine statement kullanılır. #}
{% macro create_raw_tables() %}
  {% set tables = {
      'orders':        'olist_orders_dataset.csv',
      'order_items':   'olist_order_items_dataset.csv',
      'customers':     'olist_customers_dataset.csv',
      'order_reviews': 'olist_order_reviews_dataset.csv'
  } %}
  {% call statement('raw_db') %} CREATE DATABASE IF NOT EXISTS raw {% endcall %}
  {% for name, file in tables.items() %}
    {% call statement('raw_' ~ name) %}
      CREATE TABLE IF NOT EXISTS raw.{{ name }}
      USING csv
      OPTIONS (
        path '/data/{{ file }}',
        header 'true',
        multiLine 'true',  -- yorumlarda satır sonu var
        quote '"',
        escape '"'
      )
    {% endcall %}
    {% do log("raw." ~ name ~ " hazır  <-  /data/" ~ file, info=True) %}
  {% endfor %}
{% endmacro %}
```

```bash
./dbt run-operation create_raw_tables
```

Kaynakları bildirin:

**Dosya:** `models/staging/sources.yml`

```yaml
version: 2

sources:
  - name: olist
    schema: raw
    tables:
      - name: orders
      - name: order_items
      - name: customers
      - name: order_reviews
```

Staging modelleri (tip dönüşümü, yeniden adlandırma, hafif temizlik):

**Dosya:** `models/staging/stg_orders.sql`

```sql
select
    order_id,
    customer_id,
    order_status,
    cast(order_purchase_timestamp      as timestamp) as purchased_at,
    cast(order_approved_at             as timestamp) as approved_at,
    cast(order_delivered_carrier_date  as timestamp) as delivered_to_carrier_at,
    cast(order_delivered_customer_date as timestamp) as delivered_at,
    cast(order_estimated_delivery_date as timestamp) as estimated_delivery_at
from {{ source('olist', 'orders') }}
```

**Dosya:** `models/staging/stg_order_items.sql`

```sql
select
    order_id,
    cast(order_item_id as int)            as order_item_id,
    product_id,
    seller_id,
    cast(price         as decimal(10, 2)) as price,
    cast(freight_value as decimal(10, 2)) as freight_value
from {{ source('olist', 'order_items') }}
```

**Dosya:** `models/staging/stg_customers.sql`

```sql
select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix as zip_code_prefix,
    initcap(customer_city)   as city,
    customer_state           as state
from {{ source('olist', 'customers') }}
```

**Dosya:** `models/staging/stg_order_reviews.sql`

```sql
select
    review_id,
    order_id,
    cast(review_score as int)                   as review_score,
    review_comment_message                      as comment,
    cast(review_creation_date    as timestamp)  as reviewed_at,
    cast(review_answer_timestamp as timestamp)  as answered_at
from {{ source('olist', 'order_reviews') }}
```

```bash
./dbt run
```

```bash
./dbt show --select stg_orders --limit 5
```

**Ne görülür:** `raw.orders hazır <- /data/...` logları; `dbt run` ile 4 view (`demo.stg_*`). `show` Spark'ta gerçek bir sorgu çalıştırır: Spark UI → SQL sekmesinde CSV taraması görünür.
Not: yorum CSV'si çok satırlı metin içeriyor, bu yüzden makroda `multiLine` + `escape '"'` var.

Kurtarma: `git checkout -f perde-02 && git clean -fdq models macros tests`

---

## Perde 03 · ref() + mart

**Amaç:** `ref()` ile modelleri bağlayıp iş sorusunu cevaplamak.

**Neden Spark tek başına yetmez:** Spark'a bu üç sorguyu doğru sırayla elle göndermeniz gerekir; dbt `ref()`'lerden DAG'ı çıkarır ve sırayı kendisi belirler.

**Dosya:** `models/intermediate/int_order_delays.sql`

```sql
-- Sipariş başına teslimat gecikmesi (gün) + sepet tutarı
with orders as (
    select * from {{ ref('stg_orders') }}
    where order_status = 'delivered'
      and delivered_at is not null
),

items as (
    select
        order_id,
        count(*)   as item_count,
        sum(price) as order_amount
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.purchased_at,
    o.estimated_delivery_at,
    o.delivered_at,
    -- pozitif = tahmini tarihten sonra geldi (gecikti)
    datediff(o.delivered_at, o.estimated_delivery_at) as delay_days,
    i.item_count,
    i.order_amount
from orders o
left join items i on o.order_id = i.order_id
```

**Dosya:** `models/marts/mart_delivery_satisfaction.sql`

```sql
-- İş sorusu: Gecikme memnuniyeti düşürüyor mu?
with delays as (
    select * from {{ ref('int_order_delays') }}
),

-- Bazı siparişlerin birden fazla yorumu var: sipariş başına ortalama puan
reviews as (
    select order_id, avg(review_score) as review_score
    from {{ ref('stg_order_reviews') }}
    group by order_id
),

joined as (
    select
        case
            when d.delay_days <= 0 then '1. erken/zamanında'
            when d.delay_days <= 3 then '2. 1-3 gün'
            when d.delay_days <= 7 then '3. 4-7 gün'
            else '4. 7+ gün'
        end as delay_bucket,
        r.review_score
    from delays d
    join reviews r on d.order_id = r.order_id
)

select
    delay_bucket,
    count(*)                                                   as order_count,
    round(avg(review_score), 2)                                as avg_review_score,
    round(avg(case when review_score <= 2 then 1.0 else 0.0 end) * 100, 1) as bad_review_pct
from joined
group by delay_bucket
order by delay_bucket
```

```bash
./dbt run
```

```bash
./dbt show --select mart_delivery_satisfaction
```

**Ne görülür:** `dbt run` 6 modeli bağımlılık sırasıyla kurar (önce stg, sonra int, sonra mart). `show` sonucu:

| delay_bucket | order_count | avg_review_score | bad_review_pct |
|---|---|---|---|
| 1. erken/zamanında | 89443 | 4.29 | 9.2 |
| 2. 1-3 gün | 1852 | 3.29 | 32.2 |
| 3. 4-7 gün | 1748 | 2.11 | 67.6 |
| 4. 7+ gün | 2781 | 1.70 | 79.2 |

Cevap: **evet**, 7+ gün geciken siparişlerde ortalama puan 4.29'dan 1.70'e düşüyor; kötü yorum oranı %9.2'den %79.2'ye çıkıyor.
Spark UI → **SQL**: `show` sorgusunun planında join + aggregate görünür. Şu an mart bir *view*: her sorgu hesabı baştan yapar (bir sonraki perdenin konusu).

Kurtarma: `git checkout -f perde-03 && git clean -fdq models macros tests`

---

## Perde 04 · materialization

**Amaç:** Staging = view, mart = table; dbt'nin Spark için ürettiği DDL'i görmek.

**Neden Spark tek başına yetmez:** `CREATE TABLE ... AS` / `CREATE OR REPLACE VIEW` boilerplate'ini her model için elle yazmak yerine dbt tek satır config'den üretir.

**Dosya:** `dbt_project.yml`

```yaml
name: olist_demo
version: "1.0.0"
config-version: 2
profile: olist_demo

model-paths: ["models"]
macro-paths: ["macros"]
test-paths: ["tests"]
seed-paths: ["seeds"]
analysis-paths: ["analyses"]
snapshot-paths: ["snapshots"]

target-path: target
clean-targets: [target, dbt_packages]

models:
  olist_demo:
    staging:
      +materialized: view      # ucuz: sadece tanım, veri yazılmaz
    intermediate:
      +materialized: view
    marts:
      +materialized: table     # pahalı hesap bir kez yapılır, parquet olarak yazılır
      +file_format: parquet
```

```bash
./dbt run
```

```bash
cat target/compiled/olist_demo/models/marts/mart_delivery_satisfaction.sql
```

```bash
cat target/run/olist_demo/models/marts/mart_delivery_satisfaction.sql
```

```bash
cat target/run/olist_demo/models/staging/stg_orders.sql
```

**Ne görülür:** `compiled` = sadece `ref()`'lerin `demo.xxx` olduğu SELECT. `run` = dbt'nin sardığı DDL: mart için `create table demo.mart_delivery_satisfaction using parquet as ...`, staging için `create or replace view`.
Spark UI → **Jobs**: mart tablosu yazılırken gerçek job'lar (CSV okuma, shuffle, parquet yazma); view'lar için job yok.

Kurtarma: `git checkout -f perde-04 && git clean -fdq models macros tests`

---

## Perde 05 · testler

**Amaç:** Generic + singular testlerle veriye "sözleşme" koymak.

**Neden Spark tek başına yetmez:** Spark yanlış veriyle de mutlu mutlu hesap yapar; dbt test'leri her test için bir Spark sorgusu üretip sonucu "geçti/kaldı" olarak raporlar.

**Dosya:** `models/staging/schema.yml`

```yaml
version: 2

models:
  - name: stg_orders
    columns:
      - name: order_id
        data_tests: [unique, not_null]
      - name: customer_id
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('stg_customers')
                field: customer_id
      - name: order_status
        data_tests:
          - accepted_values:
              arguments:
                values: [delivered, shipped, canceled, unavailable, invoiced, processing, created, approved]

  - name: stg_customers
    columns:
      - name: customer_id
        data_tests: [unique, not_null]

  - name: stg_order_items
    columns:
      - name: order_id
        data_tests:
          - relationships:
              arguments:
                to: ref('stg_orders')
                field: order_id

  - name: stg_order_reviews
    columns:
      - name: review_id
        data_tests:
          - unique
      - name: review_score
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: [1, 2, 3, 4, 5]
                quote: false
```

Tekil (singular) test: SQL dosyası, dönen her satır bir hata.

**Dosya:** `tests/assert_delivered_after_purchase.sql`

```sql
-- Tekil (singular) test: dönen her satır bir hatadır.
-- Müşteriye teslim tarihi, satın alma tarihinden önce olamaz.
select order_id, purchased_at, delivered_at
from {{ ref('stg_orders') }}
where delivered_at < purchased_at
```

```bash
./dbt test
```

**Ne görülür:** `FAIL 789 unique_stg_order_reviews_review_id`. Gerçek veride **789** `review_id` birden fazla satırda geçiyor. Bakalım:

```bash
./dbt show --inline "select review_id, count(distinct order_id) as siparis_sayisi from {{ ref('stg_order_reviews') }} group by review_id having count(*) > 1 order by siparis_sayisi desc" --limit 5
```

Öğretici an: Bu bir kaynak sorunu (aynı yorum birden çok siparişe bağlanmış), bizim hatamız değil. Mart zaten sipariş başına ortalama puan alıyor. Karar: testi silmek yerine `severity: warn` ile görünür tutuyoruz. `schema.yml`'ı şu hâliyle değiştirin:

**Dosya:** `models/staging/schema.yml`

```yaml
version: 2

models:
  - name: stg_orders
    columns:
      - name: order_id
        data_tests: [unique, not_null]
      - name: customer_id
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('stg_customers')
                field: customer_id
      - name: order_status
        data_tests:
          - accepted_values:
              arguments:
                values: [delivered, shipped, canceled, unavailable, invoiced, processing, created, approved]

  - name: stg_customers
    columns:
      - name: customer_id
        data_tests: [unique, not_null]

  - name: stg_order_items
    columns:
      - name: order_id
        data_tests:
          - relationships:
              arguments:
                to: ref('stg_orders')
                field: order_id

  - name: stg_order_reviews
    columns:
      - name: review_id
        data_tests:
          # Gerçek veride 789 review_id tekrar ediyor (aynı yorum birden çok siparişe bağlı).
          # Bilinen kaynak sorunu: pipeline'ı durdurmasın ama her koşuda görünür kalsın.
          - unique:
              config:
                severity: warn
      - name: review_score
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: [1, 2, 3, 4, 5]
                quote: false
```

```bash
./dbt test
```

**Ne görülür:** `WARN 789 unique_stg_order_reviews_review_id`, geri kalan 11 test PASS. Spark UI → **SQL**: her test ayrı bir Spark sorgusu (12 sorgu, 4 thread ile paralel).

Kurtarma: `git checkout -f perde-05 && git clean -fdq models macros tests`

---

## Perde 06 · docs + lineage

**Amaç:** YAML açıklamalarından otomatik doküman ve lineage grafiği üretmek.

**Neden Spark tek başına yetmez:** Spark metastore tabloları bilir ama "bu kolon ne demek, hangi modelden geliyor" bilmez; dbt docs bunu koddan üretir, kod değişince doküman da değişir.

**Dosya:** `models/staging/sources.yml`

```yaml
version: 2

sources:
  - name: olist
    description: "Olist Brezilya e-ticaret verisi (Kaggle). Spark'ta CSV üzerine external table olarak duruyor."
    schema: raw
    tables:
      - name: orders
        description: "Sipariş başlıkları: durum ve tarih alanları (2016-2018)."
      - name: order_items
        description: "Sipariş kalemleri: ürün, satıcı, fiyat, kargo ücreti."
      - name: customers
        description: "Müşteriler. customer_id siparişe özel; gerçek kişi customer_unique_id."
      - name: order_reviews
        description: "Müşteri yorumları ve 1-5 puan. Yorum metni çok satırlı olabilir."
```

**Dosya:** `models/staging/schema.yml`

```yaml
version: 2

models:
  - name: stg_orders
    description: "Temizlenmiş siparişler: tarih alanları timestamp'e çevrildi, kolonlar yeniden adlandırıldı."
    columns:
      - name: order_id
        description: "Sipariş anahtarı."
        data_tests: [unique, not_null]
      - name: customer_id
        description: "Siparişe özel müşteri anahtarı."
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('stg_customers')
                field: customer_id
      - name: order_status
        description: "Sipariş durumu (delivered, shipped, canceled ...)."
        data_tests:
          - accepted_values:
              arguments:
                values: [delivered, shipped, canceled, unavailable, invoiced, processing, created, approved]
      - name: delivered_at
        description: "Müşteriye teslim zamanı (teslim edilmediyse null)."
      - name: estimated_delivery_at
        description: "Siparişte müşteriye söz verilen tahmini teslim tarihi."

  - name: stg_customers
    description: "Müşteriler; şehir adı düzeltildi."
    columns:
      - name: customer_id
        description: "Siparişe özel müşteri anahtarı."
        data_tests: [unique, not_null]

  - name: stg_order_items
    description: "Sipariş kalemleri; fiyatlar decimal."
    columns:
      - name: order_id
        description: "Ait olduğu sipariş."
        data_tests:
          - relationships:
              arguments:
                to: ref('stg_orders')
                field: order_id

  - name: stg_order_reviews
    description: "Müşteri yorumları; puan int'e çevrildi."
    columns:
      - name: review_id
        description: "Yorum anahtarı. Dikkat: kaynakta tekil değil."
        data_tests:
          # Gerçek veride 789 review_id tekrar ediyor (aynı yorum birden çok siparişe bağlı).
          # Bilinen kaynak sorunu: pipeline'ı durdurmasın ama her koşuda görünür kalsın.
          - unique:
              config:
                severity: warn
      - name: review_score
        description: "1 (çok kötü) - 5 (çok iyi)."
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: [1, 2, 3, 4, 5]
                quote: false
```

**Dosya:** `models/intermediate/schema.yml`

```yaml
version: 2

models:
  - name: int_order_delays
    description: "Teslim edilmiş her sipariş için gecikme günü ve sepet tutarı."
    columns:
      - name: order_id
        data_tests: [unique, not_null]
      - name: delay_days
        description: "Teslim tarihi - tahmini teslim tarihi (gün). Pozitif = gecikti, 0/negatif = zamanında/erken."
        data_tests: [not_null]
      - name: order_amount
        description: "Sipariş kalemlerinin toplam fiyatı."
```

**Dosya:** `models/marts/schema.yml`

```yaml
version: 2

models:
  - name: mart_delivery_satisfaction
    description: >
      **Gecikme memnuniyeti düşürüyor mu?** Gecikme grubuna göre sipariş sayısı,
      ortalama yorum puanı ve kötü yorum (puan ≤ 2) oranı.
    columns:
      - name: delay_bucket
        description: "Gecikme grubu: erken/zamanında, 1-3 gün, 4-7 gün, 7+ gün."
        data_tests: [unique, not_null]
      - name: order_count
        description: "Gruptaki yorumlu sipariş sayısı."
      - name: avg_review_score
        description: "Ortalama yorum puanı (sipariş başına ortalama alınarak)."
      - name: bad_review_pct
        description: "Puanı ≤ 2 olan siparişlerin yüzdesi."
```

```bash
./dbt build
```

```bash
./dbt docs generate
```

```bash
./dbt docs serve --host 0.0.0.0 --port 8080 --no-browser
```

**Ne görülür:** http://localhost:8080 → `mart_delivery_satisfaction` → açıklamalar, kolonlar, testler; sağ alttaki **lineage** düğmesi: `olist.* → stg_* → int_order_delays → mart`. `docs generate` sırasında dbt kolon tiplerini Spark metastore'undan okur (Spark UI → SQL'de metadata sorguları). Bitince terminalde **Ctrl+C**.

Kurtarma: `git checkout -f perde-06 && git clean -fdq models macros tests`

---

## Perde 07 · seçim + build + state

**Amaç:** Sadece gereken parçayı çalıştırmak: graf seçiciler ve `state:modified+`.

**Neden Spark tek başına yetmez:** El yazımı PySpark job'larında "bu değişiklik neyi etkiler, sadece onları çalıştır" diye bir şey yok; dbt DAG'ı bildiği için bunu tek bayrakla yapar.

Mart'a giden her şey (atalar + kendisi + testleri):

```bash
./dbt build --select +mart_delivery_satisfaction
```

`stg_orders` ve ondan etkilenen her şey (çocuklar):

```bash
./dbt build --select stg_orders+
```

Şimdiki hâlin fotoğrafını alın (production manifest'i gibi düşünün):

```bash
mkdir -p state && cp target/manifest.json state/
```

Bir modeli değiştirin (yeni kolon: toplam teslim süresi):

**Dosya:** `models/intermediate/int_order_delays.sql`

```sql
-- Sipariş başına teslimat gecikmesi (gün) + sepet tutarı
with orders as (
    select * from {{ ref('stg_orders') }}
    where order_status = 'delivered'
      and delivered_at is not null
),

items as (
    select
        order_id,
        count(*)   as item_count,
        sum(price) as order_amount
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.purchased_at,
    o.estimated_delivery_at,
    o.delivered_at,
    -- pozitif = tahmini tarihten sonra geldi (gecikti)
    datediff(o.delivered_at, o.estimated_delivery_at) as delay_days,
    datediff(o.delivered_at, o.purchased_at)          as delivery_days,  -- yeni: toplam teslim süresi
    i.item_count,
    i.order_amount
from orders o
left join items i on o.order_id = i.order_id
```

Ne değişmiş, dbt'ye soralım:

```bash
./dbt ls --select state:modified+ --state state
```

```bash
./dbt build --select state:modified+ --state state
```

**Ne görülür:** `ls` sadece `int_order_delays`, `mart_delivery_satisfaction` ve onların testlerini listeler. `build` 7 node çalıştırır (2 model + 5 test); staging'e dokunmaz. Spark UI → **Jobs**: sadece bu iki modelin işleri. Punch: "Değişen = 1 dosya, çalışan = etkilenen alt graf."

Kurtarma: `git checkout -f perde-07 && git clean -fdq models macros tests`

---

## Perde 08 · unit test

**Amaç:** Gecikme hesaplama mantığını sahte satırlarla, gerçek veriye dokunmadan test etmek (dbt ≥ 1.8).

**Neden Spark tek başına yetmez:** Spark'ta mantığı test etmek için test verisi tablosu kurup karşılaştırma yazmanız gerekir; dbt girdileri mock'layıp beklenen çıktıyla karşılaştırmayı kendisi kurar.

**Dosya:** `models/intermediate/unit_tests.yml`

```yaml
version: 2

unit_tests:
  - name: gecikme_gunu_dogru_hesaplanir
    description: "Sahte satırlarla gecikme mantığını test eder; gerçek veriye dokunmaz."
    model: int_order_delays
    given:
      - input: ref('stg_orders')
        rows:
          - {order_id: erken,    order_status: delivered, purchased_at: "2018-01-01 10:00:00", estimated_delivery_at: "2018-01-10 00:00:00", delivered_at: "2018-01-08 15:00:00"}
          - {order_id: zamaninda, order_status: delivered, purchased_at: "2018-01-01 10:00:00", estimated_delivery_at: "2018-01-10 00:00:00", delivered_at: "2018-01-10 23:59:00"}
          - {order_id: gec,      order_status: delivered, purchased_at: "2018-01-01 10:00:00", estimated_delivery_at: "2018-01-10 00:00:00", delivered_at: "2018-01-15 09:00:00"}
          - {order_id: iptal,    order_status: canceled,  purchased_at: "2018-01-01 10:00:00", estimated_delivery_at: "2018-01-10 00:00:00", delivered_at: null}
      - input: ref('stg_order_items')
        rows:
          - {order_id: gec, price: 10.00}
          - {order_id: gec, price: 5.50}
    expect:
      rows:
        - {order_id: erken,     delay_days: -2, delivery_days: 7,  item_count: null, order_amount: null}
        - {order_id: zamaninda, delay_days: 0,  delivery_days: 9,  item_count: null, order_amount: null}
        - {order_id: gec,       delay_days: 5,  delivery_days: 14, item_count: 2,    order_amount: 15.50}
```

```bash
./dbt test --select test_type:unit
```

Şimdi mantığı bilerek bozalım (datediff argümanlarının yerini değiştiriyoruz):

```bash
docker compose exec dbt sed -i 's/datediff(o.delivered_at, o.estimated_delivery_at)/datediff(o.estimated_delivery_at, o.delivered_at)/' models/intermediate/int_order_delays.sql
```

```bash
./dbt test --select test_type:unit
```

Düzeltin (aynı değişikliği geri alıyoruz):

```bash
docker compose exec dbt sed -i 's/datediff(o.estimated_delivery_at, o.delivered_at)/datediff(o.delivered_at, o.estimated_delivery_at)/' models/intermediate/int_order_delays.sql
```

```bash
./dbt test --select test_type:unit
```

**Ne görülür:** İlk koşu PASS. Bozulunca FAIL ve beklenen/gerçek farkı tablo olarak (`delay_days` -2 yerine 2, 5 yerine -5 ...). Düzeltince tekrar PASS. Spark UI → **SQL**: sorgu sadece sahte satırlardan oluşan CTE'lerle çalışır, CSV taranmaz.

Kurtarma: `git checkout -f perde-08 && git clean -fdq models macros tests`

---

## Perde 09 · contract + exposure (opsiyonel)

**Amaç:** Mart'ın kolon adı/tipini sözleşmeye bağlamak, panoyu lineage'a eklemek.

**Neden Spark tek başına yetmez:** Spark `CREATE TABLE AS SELECT` ile ne gelirse onu yazar; tip değişikliği panoyu sessizce bozar. dbt kontratı bunu tablo yazılmadan önce yakalar.

**Dosya:** `models/marts/schema.yml`

```yaml
version: 2

models:
  - name: mart_delivery_satisfaction
    description: >
      **Gecikme memnuniyeti düşürüyor mu?** Gecikme grubuna göre sipariş sayısı,
      ortalama yorum puanı ve kötü yorum (puan ≤ 2) oranı.
    config:
      contract:
        enforced: true   # kolon adı + tipi build sırasında kontrol edilir
    columns:
      - name: delay_bucket
        data_type: string
        description: "Gecikme grubu: erken/zamanında, 1-3 gün, 4-7 gün, 7+ gün."
        data_tests: [unique, not_null]
      - name: order_count
        data_type: bigint
        description: "Gruptaki yorumlu sipariş sayısı."
      - name: avg_review_score
        data_type: double
        description: "Ortalama yorum puanı (sipariş başına ortalama alınarak)."
      - name: bad_review_pct
        data_type: decimal(25,1)
        description: "Puanı ≤ 2 olan siparişlerin yüzdesi."
```

**Dosya:** `models/marts/exposures.yml`

```yaml
version: 2

exposures:
  - name: teslimat_memnuniyeti_panosu
    label: "Teslimat memnuniyeti panosu"
    type: dashboard
    maturity: medium
    url: http://localhost:8080/#!/exposure/exposure.olist_demo.teslimat_memnuniyeti_panosu
    description: "Operasyon ekibinin haftalık baktığı pano: gecikme grubuna göre memnuniyet."
    depends_on:
      - ref('mart_delivery_satisfaction')
    owner:
      name: Veri Ekibi
      email: veri@example.com
```

```bash
./dbt build --select mart_delivery_satisfaction
```

Şimdi biri `order_count`'u `int`'e çevirsin (kırıcı değişiklik):

```bash
docker compose exec dbt sed -i 's/ count(\*) / cast(count(*) as int) /' models/marts/mart_delivery_satisfaction.sql
```

```bash
./dbt build --select mart_delivery_satisfaction
```

Geri alın ve dokümanı yenileyin:

```bash
docker compose exec dbt sed -i 's/ cast(count(\*) as int) / count(*) /' models/marts/mart_delivery_satisfaction.sql
```

```bash
./dbt build --select mart_delivery_satisfaction
```

```bash
./dbt docs generate
```

**Ne görülür:** Kırıcı değişiklikte `This model has an enforced contract that failed.` ve `order_count | INT_TYPE | BIGINT_TYPE | data type mismatch` tablosu; tablo **yazılmaz**. Docs'ta (`./dbt docs serve --host 0.0.0.0 --port 8080 --no-browser`) lineage'ın ucunda **Teslimat memnuniyeti panosu** exposure düğümü.

Adaptör notu: Spark'ta `parquet` tablolarda `not_null` gibi **constraint**'ler uygulanmaz (Delta Lake gerekir). Burada kontrat, build anında kolon adı + tip kontrolü olarak çalışıyor; demo için bu yeterli.

Kurtarma: `git checkout -f perde-09 && git clean -fdq models macros tests`

---

## Perde 10 · freshness + Jinja (opsiyonel)

**Amaç:** Kaynak tazeliğini ölçmek; tekrar eden SQL'i makroya, gecikme gruplarını Jinja döngüsüne almak.

**Neden Spark tek başına yetmez:** Spark verinin bayat olduğunu söylemez, eski veriyle de hesaplar; dbt freshness bunu ölçer. Jinja ile SQL'i DRY tutarız, Spark yine aynı düz SQL'i görür.

**Dosya:** `models/staging/sources.yml`

```yaml
version: 2

sources:
  - name: olist
    description: "Olist Brezilya e-ticaret verisi (Kaggle). Spark'ta CSV üzerine external table olarak duruyor."
    schema: raw
    tables:
      - name: orders
        description: "Sipariş başlıkları: durum ve tarih alanları (2016-2018)."
        config:
          # CSV'de her şey string: tazelik için timestamp'e çeviriyoruz
          loaded_at_field: "cast(order_purchase_timestamp as timestamp)"
          freshness:
            warn_after: {count: 1, period: day}
            error_after: {count: 7, period: day}
      - name: order_items
        description: "Sipariş kalemleri: ürün, satıcı, fiyat, kargo ücreti."
      - name: customers
        description: "Müşteriler. customer_id siparişe özel; gerçek kişi customer_unique_id."
      - name: order_reviews
        description: "Müşteri yorumları ve 1-5 puan. Yorum metni çok satırlı olabilir."
```

```bash
./dbt source freshness
```

**Ne görülür:** `ERROR STALE freshness of olist.orders`. Öğretici an: **veri 2018'de kalmış**. Spark bu veriyle mutlu mutlu hesap yapar; dbt "bu panoya bakan bugünü gördüğünü sanıyor" diye uyarır. (Komut bilerek hata koduyla biter.)

Makro:

**Dosya:** `macros/gun_farki.sql`

```sql
{# İki tarih arasındaki gün farkı: bitis - baslangic. Aynı SQL'i her modelde tekrar yazmayalım. #}
{% macro gun_farki(bitis, baslangic) -%}
    datediff({{ bitis }}, {{ baslangic }})
{%- endmacro %}
```

**Dosya:** `models/intermediate/int_order_delays.sql`

```sql
-- Sipariş başına teslimat gecikmesi (gün) + sepet tutarı
with orders as (
    select * from {{ ref('stg_orders') }}
    where order_status = 'delivered'
      and delivered_at is not null
),

items as (
    select
        order_id,
        count(*)   as item_count,
        sum(price) as order_amount
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.purchased_at,
    o.estimated_delivery_at,
    o.delivered_at,
    -- pozitif = tahmini tarihten sonra geldi (gecikti)
    {{ gun_farki('o.delivered_at', 'o.estimated_delivery_at') }} as delay_days,
    {{ gun_farki('o.delivered_at', 'o.purchased_at') }}          as delivery_days,  -- yeni: toplam teslim süresi
    i.item_count,
    i.order_amount
from orders o
left join items i on o.order_id = i.order_id
```

Jinja döngüsüyle gecikme grupları:

**Dosya:** `models/marts/mart_delivery_satisfaction.sql`

```sql
-- İş sorusu: Gecikme memnuniyeti düşürüyor mu?
{#- Gruplar tek yerde: yeni eşik eklemek = listeye bir satır eklemek -#}
{%- set gecikme_gruplari = [
    (0, '1. erken/zamanında'),
    (3, '2. 1-3 gün'),
    (7, '3. 4-7 gün'),
] %}
with delays as (
    select * from {{ ref('int_order_delays') }}
),

-- Bazı siparişlerin birden fazla yorumu var: sipariş başına ortalama puan
reviews as (
    select order_id, avg(review_score) as review_score
    from {{ ref('stg_order_reviews') }}
    group by order_id
),

joined as (
    select
        case
        {%- for ust_sinir, grup in gecikme_gruplari %}
            when d.delay_days <= {{ ust_sinir }} then '{{ grup }}'
        {%- endfor %}
            else '4. 7+ gün'
        end as delay_bucket,
        r.review_score
    from delays d
    join reviews r on d.order_id = r.order_id
)

select
    delay_bucket,
    count(*)                                                   as order_count,
    round(avg(review_score), 2)                                as avg_review_score,
    round(avg(case when review_score <= 2 then 1.0 else 0.0 end) * 100, 1) as bad_review_pct
from joined
group by delay_bucket
order by delay_bucket
```

```bash
./dbt build
```

```bash
cat target/compiled/olist_demo/models/marts/mart_delivery_satisfaction.sql
```

```bash
./dbt show --select mart_delivery_satisfaction
```

**Ne görülür:** `compiled` SQL'de makro `datediff(...)`'e, döngü düz `case when ...`'e açılmış: Spark Jinja görmez. Unit test ve kontrat hâlâ PASS (`build` özeti: `PASS=23 WARN=1 NO-OP=1`; NO-OP = exposure); sonuç tablosu Perde 03 ile birebir aynı.

Kurtarma: `git checkout -f perde-10 && git clean -fdq models macros tests`

---

## Kapanış

`./dbt build` tek komutla: kaynaktan marta tüm modeller + 17 data test + 1 unit test + kontrat. Spark UI'daki her job dbt'nin ürettiği bir SQL. **Spark hesaplar, dbt hesabı güvenilir kılar.**
