FROM python:3.11-slim

# Install git (often needed for cloning dbt dependencies or helper scripts)
RUN apt-get update && apt-get install -y git

# Install ClickHouse driver and dbt adapter
# dbt-clickhouse is the adapter, clickhouse-connect is the necessary underlying connector
RUN pip install --no-cache-dir dbt-core dbt-clickhouse clickhouse-connect

# Keep working dir minimal; volumes will handle your project files
WORKDIR /dbt
ENTRYPOINT ["bash"]