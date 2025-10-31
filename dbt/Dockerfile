
FROM python:3.9-slim

RUN apt-get update && apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-core dbt-postgres dbt-snowflake

WORKDIR /usr/app/dbt

CMD ["bash"]
