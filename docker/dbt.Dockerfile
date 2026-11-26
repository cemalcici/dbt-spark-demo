FROM python:3.11-slim
# git: 'dbt debug' ve 'dbt deps' için gerekli
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
# Sürümler sabit: canlı derste sürpriz istemiyoruz
RUN pip install --no-cache-dir "dbt-core==1.11.17" "dbt-spark[PyHive]==1.11.1"
# PyHive 0.7 + Spark 3.5 Thrift: DDL (CREATE VIEW vb.) sonucu boş kolon listesiyle döner ve
# PyHive 'NoneType is not iterable' hatası verir. dbt unit test'leri run_query ile DDL çalıştırdığı
# için bu tek satırlık yama gerekli: boş sonucu boş liste say.
RUN f=$(python -c "import pyhive.hive as h; print(h.__file__)") && \
    sed -i 's/zip(response.results.columns, schema)/zip(response.results.columns or [], schema)/' "$f" && \
    grep -q "response.results.columns or \[\]" "$f"
WORKDIR /usr/app
