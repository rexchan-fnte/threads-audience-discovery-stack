FROM apache/airflow:2.9.2

# Install Python dependencies once at build time
# (eliminates per-start pip install that caused webserver timeout loops)
RUN pip install --no-cache-dir \
    anthropic gspread google-cloud-bigquery google-auth pyyaml requests db-dtypes

# Copy DAGs, config, and plugins into image
COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/
COPY --chown=airflow:root airflow/config/ /opt/airflow/config/
COPY --chown=airflow:root airflow/plugins/ /opt/airflow/plugins/
