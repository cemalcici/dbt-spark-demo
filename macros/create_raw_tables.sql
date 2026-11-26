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
