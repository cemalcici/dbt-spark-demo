# CANLI · dbt + Spark, boş klasörden adım adım

> Spark hesaplar, dbt hesabı güvenilir kılar. İş sorusu: *Gecikme memnuniyeti düşürüyor mu?*

Yukarıdan aşağı akar. Her adım = tek dosya **ya da** tek komut.

Tek VS Code penceresi, repo kökünde (`code .`). Çözüm (bitmiş proje) kökte; canlı çalışma **boş başlayan** `canli/` alt klasöründe (gitignore'da).
`canli/` kendi uv ortamını kurar; Spark onun için "uzak sunucu": `localhost:10000`. Profil `canli/profiles.yml`'da durur (`DBT_PROFILES_DIR`); `~/.dbt`'ye dokunulmaz.
Container akışı yedek: [DEMO.md](DEMO.md).

## Ön hazırlık (19:50)

- [ ] `cd ~/Documents/projects/vbo-dbt-spark-demo && docker compose up -d --wait` (spark `healthy`)
- [ ] `rm -rf canli && mkdir canli` (temiz başlangıç)
- [ ] VS Code: `code .` (repo kökü); terminal repo kökünde
- [ ] Sekmeler: http://localhost:4040 (Spark UI); docs sonra http://localhost:8081

---

## Perde 01 · init + profil

Amaç: boş klasörden dbt projesi; dbt'nin Spark Thrift Server'a (uzak sunucu gibi, `localhost:10000`) bağlandığını göstermek.

### Adım 1 · boş klasör

```bash
cd ~/Documents/projects/vbo-dbt-spark-demo/canli && ls -A
```

**Söyle:** Elimizde boş bir klasör ve uzakta çalışan bir Spark var.  
**Göster:** VS Code explorer: `canli/` boş.

### Adım 2 · Python projesi

```bash
uv init --bare --no-workspace --python 3.12
```

**Söyle:** dbt bir Python paketi; ortamı uv ile kuruyoruz.  
**Göster:** VS Code: `canli/pyproject.toml`.

### Adım 3 · dbt + Spark adapter

```bash
uv add "dbt-core==1.11.17" "dbt-spark[PyHive]==1.11.1"
```

**Söyle:** dbt-core motor değil; dbt-spark, SQL'i Spark'a taşıyan adapter.  
**Göster:** Terminal: paketler kuruldu.

### Adım 4 · PyHive yaması

```bash
../scripts/pyhive-yama.sh
```

**Söyle:** Spark 3.5 ile PyHive'ın bilinen bir hatası var; tek satırlık yama.  
**Göster:** Terminal: "PyHive yaması uygulandı".

### Adım 5 · dbt init

```bash
export DBT_PROFILES_DIR=$PWD && uv run dbt init olist --project-dir .
```

**Söyle:** init iki şey yapar: proje iskeleti + bağlantı profili.  
**Göster:** Soruları aşağıdaki tabloya göre cevapla.

| Soru | Yaz |
|---|---|
| Which database would you like to use? | `1` (spark) |
| host | `localhost` |
| Desired authentication method | `3` (thrift) |
| poll_interval [5] | Enter |
| query_timeout | `300` (boş geçilemez) |
| query_retries [1] | Enter |
| port [443] | `10000` |
| schema | `canli` |
| threads [1] | `4` |

`--project-dir .`: canli/ çözüm projesinin içinde; bu bayrak olmadan init üstteki `dbt_project.yml`'ı bulup yalnız profil kurar.

### Adım 6 · profil dosyası

```bash
cat profiles.yml
```

**Söyle:** dbt veri tutmaz: sadece Spark'a nereden bağlanacağını bilir.  
**Göster:** Terminal: host localhost, port 10000, method thrift.

### Adım 7 · iskelete bak, örnekleri sil

```bash
cd olist && rm -rf models/example && ls
```

**Söyle:** models, macros, tests, dbt_project.yml: proje bundan ibaret.  
**Göster:** VS Code: `olist/` klasörü.

### Adım 8 · dbt_project.yml (sade hâli)

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/dbt_project.yml`

```yaml
name: olist
version: "1.0.0"
config-version: 2
profile: olist

model-paths: ["models"]
macro-paths: ["macros"]
test-paths: ["tests"]
seed-paths: ["seeds"]
analysis-paths: ["analyses"]
snapshot-paths: ["snapshots"]

target-path: target
clean-targets: [target, dbt_packages]
```

**Söyle:** Proje adı olist, profil olist; init'in yorumlarını sadeleştirdik.  
**Göster:** VS Code.

### Adım 9 · dbt debug

```bash
cd ~/Documents/projects/vbo-dbt-spark-demo/canli/olist && export DBT_PROFILES_DIR=..
uv run dbt debug
```

**Söyle:** Bağlantı testi: dbt Spark Thrift Server'a SQL gönderebiliyor mu?  
**Göster:** Terminal: `All checks passed!`

### Adım 10 · ilk sorgu

```bash
uv run dbt show --inline "select current_database() as db, version() as spark_surumu"
```

**Söyle:** Bu sorguyu Spark çalıştırdı, dbt sadece gönderdi.  
**Göster:** Spark UI → SQL: sorgu orada.

Kurtarma (canli/olist içinden): `../../scripts/perdeye-atla.sh 01 && uv run dbt build`


---

## Perde 02 · source + staging

Amaç: ham veriyi `source()` ile bildirmek, temiz `stg_` modelleri kurmak.

### Adım 11 · ham tabloları tanıtan makro

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/macros/create_raw_tables.sql`

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

**Söyle:** dbt EL yapmaz, T yapar: veri Spark'a (lake'e) zaten inmiş; dbt yalnız SELECT çalıştırıp sonucu yine Spark'a yazdırır.  
**Göster:** Kaynak `raw.*`: CSV üzerine external tablolar (gerçek hayatta lake/Postgres'ten EL ile gelir); hedef: aynı Spark warehouse'u.

### Adım 12 · run-operation

```bash
cd ~/Documents/projects/vbo-dbt-spark-demo/canli/olist && export DBT_PROFILES_DIR=..
uv run dbt run-operation create_raw_tables
```

**Söyle:** Makro Spark'a DDL gönderir; veri kopyalanmaz.  
**Göster:** Terminal: `raw.orders hazır <- /data/...` satırları.

### Adım 13 · sources.yml

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/staging/sources.yml`

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

**Söyle:** Kaynağı bir kez bildiriyoruz; modeller `source()` ile ona bağlanır.  
**Göster:** VS Code.

### Adım 14 · stg_orders

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/staging/stg_orders.sql`

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

**Söyle:** Staging: tip dönüşümü + yeniden adlandırma, iş mantığı yok.  
**Göster:** VS Code.

### Adım 15 · tek modeli çalıştır

```bash
uv run dbt run -s stg_orders
```

**Söyle:** dbt SELECT'i view DDL'ine sarıp Spark'a yolladı.  
**Göster:** Terminal: `canli.stg_orders` view OK.

### Adım 16 · sonuca bak

```bash
uv run dbt show -s stg_orders --limit 5
```

**Söyle:** show = gerçek Spark sorgusu.  
**Göster:** Spark UI → SQL: CSV taraması.

### Adım 17 · stg_order_items

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/staging/stg_order_items.sql`

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

**Söyle:** Fiyatlar decimal'e.  

### Adım 18 · çalıştır

```bash
uv run dbt run -s stg_order_items
```

**Göster:** Terminal: OK.

### Adım 19 · stg_customers

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/staging/stg_customers.sql`

```sql
select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix as zip_code_prefix,
    initcap(customer_city)   as city,
    customer_state           as state
from {{ source('olist', 'customers') }}
```

**Söyle:** Şehir adı initcap.  

### Adım 20 · çalıştır

```bash
uv run dbt run -s stg_customers
```

**Göster:** Terminal: OK.

### Adım 21 · stg_order_reviews

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/staging/stg_order_reviews.sql`

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

**Söyle:** Puan int'e çevrildi.  

### Adım 22 · çalıştır

```bash
uv run dbt run -s stg_order_reviews
```

**Göster:** Terminal: OK.

### Adım 23 · yorumlara bak

```bash
uv run dbt show -s stg_order_reviews --limit 5
```

**Söyle:** Yorum metni çok satırlı: makrodaki multiLine bu yüzden.  
**Göster:** Terminal.

Kurtarma (canli/olist içinden): `../../scripts/perdeye-atla.sh 02 && uv run dbt build`


---

## Perde 03 · ref() + mart

Amaç: `ref()` ile modelleri bağlayıp iş sorusunu cevaplamak.

### Adım 24 · int_order_delays

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/intermediate/int_order_delays.sql`

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

**Söyle:** `ref()` = bağımlılık; dbt sırayı buradan çıkarır.  
**Göster:** VS Code: iki ref.

### Adım 25 · çalıştır

```bash
cd ~/Documents/projects/vbo-dbt-spark-demo/canli/olist && export DBT_PROFILES_DIR=..
uv run dbt run -s int_order_delays
```

**Göster:** Terminal: OK.

### Adım 26 · gecikmelere bak

```bash
uv run dbt show -s int_order_delays --limit 5
```

**Söyle:** Pozitif delay_days = geç geldi.  
**Göster:** Terminal.

### Adım 27 · mart

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/marts/mart_delivery_satisfaction.sql`

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

**Söyle:** İş sorusunun cevabı tek tablo.  
**Göster:** VS Code.

### Adım 28 · hepsini çalıştır

```bash
uv run dbt run
```

**Söyle:** dbt 6 modeli DAG sırasıyla kurar: stg → int → mart.  
**Göster:** Terminal: sıralama.

### Adım 29 · cevap

```bash
uv run dbt show -s mart_delivery_satisfaction
```

**Söyle:** Evet: 7+ gün gecikmede puan 4.29'dan 1.70'e düşüyor.  
**Göster:** Spark UI → SQL: join + aggregate planı.

Kurtarma (canli/olist içinden): `../../scripts/perdeye-atla.sh 03 && uv run dbt build`


---

## Perde 04 · materialization

Amaç: staging/int = view, mart = parquet table; dbt'nin ürettiği DDL'i görmek.

### Adım 30 · materialization ayarı

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/dbt_project.yml`

```yaml
name: olist
version: "1.0.0"
config-version: 2
profile: olist

model-paths: ["models"]
macro-paths: ["macros"]
test-paths: ["tests"]
seed-paths: ["seeds"]
analysis-paths: ["analyses"]
snapshot-paths: ["snapshots"]

target-path: target
clean-targets: [target, dbt_packages]

models:
  olist:
    staging:
      +materialized: view      # ucuz: sadece tanım, veri yazılmaz
    intermediate:
      +materialized: view
    marts:
      +materialized: table     # pahalı hesap bir kez yapılır, parquet olarak yazılır
      +file_format: parquet
```

**Söyle:** Tek satır config, dbt DDL'i kendisi yazar.  
**Göster:** VS Code: `models:` bloğu.

### Adım 31 · çalıştır

```bash
cd ~/Documents/projects/vbo-dbt-spark-demo/canli/olist && export DBT_PROFILES_DIR=..
uv run dbt run
```

**Söyle:** Mart artık tablo: hesap bir kez yapılır.  
**Göster:** Spark UI → Jobs: parquet yazan job'lar.

### Adım 32 · compiled SQL

```bash
cat target/compiled/olist/models/marts/mart_delivery_satisfaction.sql
```

**Söyle:** compiled = ref'ler çözülmüş düz SELECT.  
**Göster:** Terminal.

### Adım 33 · run SQL

```bash
cat target/run/olist/models/marts/mart_delivery_satisfaction.sql
```

**Söyle:** run = dbt'nin sardığı `create table ... using parquet as`.  
**Göster:** Terminal.

Kurtarma (canli/olist içinden): `../../scripts/perdeye-atla.sh 04 && uv run dbt build`


---

## Perde 05 · testler

Amaç: generic + singular testlerle veriye sözleşme koymak.

### Adım 34 · generic testler

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/staging/schema.yml`

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

**Söyle:** unique, not_null, relationships, accepted_values: her biri bir Spark sorgusu.  
**Göster:** VS Code.

### Adım 35 · singular test

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/tests/assert_delivered_after_purchase.sql`

```sql
-- Tekil (singular) test: dönen her satır bir hatadır.
-- Müşteriye teslim tarihi, satın alma tarihinden önce olamaz.
select order_id, purchased_at, delivered_at
from {{ ref('stg_orders') }}
where delivered_at < purchased_at
```

**Söyle:** Dönen her satır bir hata.  
**Göster:** VS Code.

### Adım 36 · test (bilerek kırmızı)

```bash
cd ~/Documents/projects/vbo-dbt-spark-demo/canli/olist && export DBT_PROFILES_DIR=..
uv run dbt test
```

**Söyle:** Gerçek veri: 789 review_id tekrar ediyor.  
**Göster:** Terminal: `FAIL 789 unique_stg_order_reviews_review_id`.

### Adım 37 · sorunu incele

```bash
uv run dbt show --inline "select review_id, count(distinct order_id) as siparis_sayisi from {{ ref('stg_order_reviews') }} group by review_id having count(*) > 1 order by siparis_sayisi desc" --limit 5
```

**Söyle:** Aynı yorum birden çok siparişe bağlı: kaynak sorunu, bizim hatamız değil.  
**Göster:** Terminal.

### Adım 38 · düzeltme: severity warn

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/staging/schema.yml`

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

**Söyle:** Testi silmiyoruz; görünür kalsın ama pipeline'ı durdurmasın.  
**Göster:** VS Code: `severity: warn`.

### Adım 39 · tekrar test

```bash
uv run dbt test
```

**Göster:** Terminal: `WARN 789`, diğerleri PASS; Spark UI → SQL: test başına bir sorgu.

Kurtarma (canli/olist içinden): `../../scripts/perdeye-atla.sh 05 && uv run dbt build`


---

## Perde 06 · docs + lineage

Amaç: YAML açıklamalarından doküman ve lineage.

### Adım 40 · kaynak açıklamaları

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/staging/sources.yml`

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

**Söyle:** Doküman kodla birlikte yaşar.  

### Adım 41 · staging açıklamaları

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/staging/schema.yml`

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


### Adım 42 · intermediate açıklama + test

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/intermediate/schema.yml`

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


### Adım 43 · mart açıklaması

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/marts/schema.yml`

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


### Adım 44 · build

```bash
cd ~/Documents/projects/vbo-dbt-spark-demo/canli/olist && export DBT_PROFILES_DIR=..
uv run dbt build
```

**Söyle:** build = run + test, DAG sırasıyla.  
**Göster:** Terminal: özet.

### Adım 45 · docs generate

```bash
uv run dbt docs generate
```

**Söyle:** Kolon tiplerini Spark metastore'undan okur.  
**Göster:** Spark UI → SQL: metadata sorguları.

### Adım 46 · docs serve

```bash
uv run dbt docs serve --port 8081 --no-browser
```

**Söyle:** 8080 container akışına ayrılmış, yerelde 8081.  
**Göster:** http://localhost:8081 → mart → lineage; bitince Ctrl+C.

Kurtarma (canli/olist içinden): `../../scripts/perdeye-atla.sh 06 && uv run dbt build`


---

## Perde 07 · seçim + build + state

Amaç: sadece gereken parçayı çalıştırmak.

### Adım 47 · atalar + mart

```bash
cd ~/Documents/projects/vbo-dbt-spark-demo/canli/olist && export DBT_PROFILES_DIR=..
uv run dbt build --select +mart_delivery_satisfaction
```

**Söyle:** `+model` = model ve tüm ataları.  
**Göster:** Terminal.

### Adım 48 · stg_orders ve çocukları

```bash
uv run dbt build --select stg_orders+
```

**Söyle:** `model+` = model ve etkilediği her şey.  
**Göster:** Terminal.

### Adım 49 · state fotoğrafı

```bash
mkdir -p state && cp target/manifest.json state/
```

**Söyle:** Production manifest'i gibi düşünün.  

### Adım 50 · yeni kolon: delivery_days

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/intermediate/int_order_delays.sql`

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

**Söyle:** Bir dosya değişiyor.  
**Göster:** VS Code: delivery_days satırı.

### Adım 51 · ne değişti?

```bash
uv run dbt ls --select state:modified+ --state state
```

**Söyle:** dbt farkı manifest'ten bulur.  
**Göster:** Terminal: sadece int + mart + testleri.

### Adım 52 · sadece etkilenenleri kur

```bash
uv run dbt build --select state:modified+ --state state
```

**Söyle:** Değişen = 1 dosya, çalışan = etkilenen alt graf.  
**Göster:** Spark UI → Jobs: sadece iki model.

Kurtarma (canli/olist içinden): `../../scripts/perdeye-atla.sh 07 && uv run dbt build`


---

## Perde 08 · unit test

Amaç: gecikme mantığını sahte satırlarla test etmek.

### Adım 53 · unit test

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/intermediate/unit_tests.yml`

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

**Söyle:** Girdiler mock, gerçek veriye dokunmaz.  
**Göster:** VS Code: given / expect.

### Adım 54 · unit test

```bash
cd ~/Documents/projects/vbo-dbt-spark-demo/canli/olist && export DBT_PROFILES_DIR=..
uv run dbt test --select test_type:unit
```

**Göster:** Terminal: PASS.

### Adım 55 · mantığı bilerek boz

```bash
perl -pi -e 's/datediff\(o.delivered_at, o.estimated_delivery_at\)/datediff(o.estimated_delivery_at, o.delivered_at)/' models/intermediate/int_order_delays.sql
```

**Söyle:** datediff argümanlarının yerini değiştiriyoruz.  
**Göster:** VS Code: değişen satır.

### Adım 56 · unit test (bilerek kırmızı)

```bash
uv run dbt test --select test_type:unit
```

**Söyle:** Unit test mantık hatasını veriye bakmadan yakaladı.  
**Göster:** Terminal: FAIL, beklenen/gerçek farkı.

### Adım 57 · düzelt

```bash
perl -pi -e 's/datediff\(o.estimated_delivery_at, o.delivered_at\)/datediff(o.delivered_at, o.estimated_delivery_at)/' models/intermediate/int_order_delays.sql
```


### Adım 58 · tekrar unit test

```bash
uv run dbt test --select test_type:unit
```

**Göster:** Terminal: PASS; Spark UI → SQL: CSV taranmadı.

Kurtarma (canli/olist içinden): `../../scripts/perdeye-atla.sh 08 && uv run dbt build`


---

## Perde 09 · contract + exposure (vakit kalırsa)

Amaç: mart'ın kolon adı/tipini sözleşmeye bağlamak, panoyu lineage'a eklemek.

### Adım 59 · kontrat

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/marts/schema.yml`

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

**Söyle:** Tip değişirse tablo yazılmadan yakalanır.  
**Göster:** VS Code: `contract: enforced`.

### Adım 60 · exposure

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/marts/exposures.yml`

```yaml
version: 2

exposures:
  - name: teslimat_memnuniyeti_panosu
    label: "Teslimat memnuniyeti panosu"
    type: dashboard
    maturity: medium
    url: http://localhost:8080/#!/exposure/exposure.olist.teslimat_memnuniyeti_panosu
    description: "Operasyon ekibinin haftalık baktığı pano: gecikme grubuna göre memnuniyet."
    depends_on:
      - ref('mart_delivery_satisfaction')
    owner:
      name: Veri Ekibi
      email: veri@example.com
```

**Söyle:** Pano lineage'ın ucunda.  

### Adım 61 · build

```bash
cd ~/Documents/projects/vbo-dbt-spark-demo/canli/olist && export DBT_PROFILES_DIR=..
uv run dbt build --select mart_delivery_satisfaction
```

**Göster:** Terminal: PASS.

### Adım 62 · kırıcı değişiklik

```bash
perl -pi -e 's/ count\(\*\) / cast(count(*) as int) /' models/marts/mart_delivery_satisfaction.sql
```

**Söyle:** Biri order_count'u int'e çevirdi.  

### Adım 63 · build (bilerek kırmızı)

```bash
uv run dbt build --select mart_delivery_satisfaction
```

**Söyle:** Kontrat tabloyu korudu: yazılmadı.  
**Göster:** Terminal: `data type mismatch` tablosu.

### Adım 64 · geri al

```bash
perl -pi -e 's/ cast\(count\(\*\) as int\) / count(*) /' models/marts/mart_delivery_satisfaction.sql
```


### Adım 65 · tekrar build

```bash
uv run dbt build --select mart_delivery_satisfaction
```

**Göster:** Terminal: PASS.

Kurtarma (canli/olist içinden): `../../scripts/perdeye-atla.sh 09 && uv run dbt build`


---

## Perde 10 · freshness + Jinja (vakit kalırsa)

Amaç: kaynak tazeliği; tekrar eden SQL'i makroya, grupları Jinja döngüsüne almak.

### Adım 66 · freshness

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/staging/sources.yml`

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

**Söyle:** Spark bayat veriyle de hesaplar; dbt ölçer.  
**Göster:** VS Code: loaded_at_field.

### Adım 67 · freshness (bilerek kırmızı)

```bash
cd ~/Documents/projects/vbo-dbt-spark-demo/canli/olist && export DBT_PROFILES_DIR=..
uv run dbt source freshness
```

**Söyle:** Veri 2018'de kalmış.  
**Göster:** Terminal: `ERROR STALE`.

### Adım 68 · makro

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/macros/gun_farki.sql`

```sql
{# İki tarih arasındaki gün farkı: bitis - baslangic. Aynı SQL'i her modelde tekrar yazmayalım. #}
{% macro gun_farki(bitis, baslangic) -%}
    datediff({{ bitis }}, {{ baslangic }})
{%- endmacro %}
```

**Söyle:** Aynı SQL'i tek yerde tut.  

### Adım 69 · makroyu kullan

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/intermediate/int_order_delays.sql`

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

**Göster:** VS Code: `gun_farki(...)`.

### Adım 70 · Jinja döngüsü

Dosya: `~/Documents/projects/vbo-dbt-spark-demo/canli/olist/models/marts/mart_delivery_satisfaction.sql`

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

**Söyle:** Yeni eşik = listeye bir satır.  
**Göster:** VS Code: `for` döngüsü.

### Adım 71 · build

```bash
uv run dbt build
```

**Göster:** Terminal: `PASS=23 WARN=1`.

### Adım 72 · compiled SQL

```bash
cat target/compiled/olist/models/marts/mart_delivery_satisfaction.sql
```

**Söyle:** Spark Jinja görmez: düz case when.  
**Göster:** Terminal.

### Adım 73 · sonuç

```bash
uv run dbt show -s mart_delivery_satisfaction
```

**Söyle:** Perde 03 ile birebir aynı.  
**Göster:** Terminal.

Kurtarma (canli/olist içinden): `../../scripts/perdeye-atla.sh 10 && uv run dbt build`


---

**Kapanış:** `uv run dbt build` tek komutla kaynaktan marta her şey + testler + kontrat. Spark hesaplar, dbt hesabı güvenilir kılar.
