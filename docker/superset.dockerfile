FROM apache/superset:4.1.2

USER root
RUN pip install psycopg2-binary
USER superset