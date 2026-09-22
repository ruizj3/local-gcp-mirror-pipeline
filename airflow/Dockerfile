# Build context must be ./airflow (see render.yaml dockerContext) so this can COPY dags/
# into the image — Render can't bind-mount a repo folder into a running container like
# docker-compose's local `volumes:` does.
FROM apache/airflow:2.7.1-python3.10

COPY dags /opt/airflow/dags
