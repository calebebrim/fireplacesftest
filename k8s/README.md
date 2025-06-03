# Kubernetes Manifests for Fire Incidents KinD Cluster

This directory contains Kubernetes manifests for deploying the core infrastructure and data processing jobs for the Fire Incidents KinD Cluster Project.

## Contents

- **bronze-fireeventsource.yaml**  
  Bronze layer: Data ingestion job for fire event source data.
- **silver-dataquality.yaml**  
  Silver layer: Data quality job to validate and clean ingested data.
- **gold-serving-layer.yaml**  
  Gold layer: Serving job for analytics and reporting.
- **simple-counting-job.yaml**  
  Example analytics job for demonstration or testing.
- **kafka.yaml**  
  Kafka cluster deployment (via Strimzi or raw manifests).
- **kafkastorageclass.yaml**  
  StorageClass definition for Kafka persistent volumes.
- **redis.yaml**  
  Redis cache deployment.
- **postgres.yaml**  
  Postgres database deployment (optional).
- **s3like-minIO.yaml**  
  MinIO S3-compatible object storage (optional).
- **superset.yaml**  
  Apache Superset BI dashboard deployment.
- **superset-helm-values.yaml**  
  Custom Helm values for Superset deployment.

## Environment Variables

The behavior and performance of the pipeline are controlled by several environment variables. These can be set in the Kubernetes manifests using the `env` section of each deployment.

### Key Variables

| Variable                | Default                | Description                                                                                 |
|-------------------------|------------------------|---------------------------------------------------------------------------------------------|
| `RESTART`               | `False`                | If set to `True`, the service will reset its state (delete topics, keys, etc.) and restart. |
| `BATCH_SIZE`            | `100` or `1000`        | Number of records/messages processed per batch or main loop iteration.                       |
| `MAIN_LOOP_INTERVAL`    | `30`                   | Seconds to wait between main loop iterations. Lower values make the pipeline run faster.     |
| `MAIN_LOOP_TIMEOUT`     | `60`                   | Maximum seconds before the main loop times out and restarts.                                 |
| `MAIN_LOOP`             | `True`                 | If `False`, the service will run only one iteration and exit.                               |
| `DATE_FORMAT`           | `%Y/%m/%d` or similar  | Date format for parsing incident dates.                                                      |
| `DATETIME_FORMAT`       | `%Y/%m/%d %H:%M:%S`    | DateTime format for parsing incident dates.                                                  |
| `CSV_FOLDER_PATH`       | `/data/fire_events`    | Path to the folder containing fire event CSV files.                                          |
| `SERVICE_NAME`          | (varies)               | Name of the service, used for Redis keys and logging.                                       |
| `ON_FAILURE`            | `continue`             | Controls error handling: `continue` or `raise`.                                             |

**Performance Tuning:**  
- Lowering `MAIN_LOOP_INTERVAL` and increasing `BATCH_SIZE` will make the pipeline process data faster, but may increase resource usage.
- Setting `RESTART` to `True` will clear state and restart the pipeline from scratch.

### Kafka and Redis

- `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_GROUP_ID`, `KAFKA_AUTO_OFFSET_RESET`, etc. are used to configure Kafka producers and consumers.
- Redis connection and key prefix variables are also set via environment.

See the source code in [`src/services/fire_event_source.py`](../src/services/fire_event_source.py), [`fire_event_data_quality.py`](../src/services/fire_event_data_quality.py), and [`fire_event_data_serving.py`](../src/services/fire_event_data_serving.py) for details on how these variables are used.

## Usage

These manifests are applied automatically by [Tilt](../Tiltfile) as part of the development workflow by `tilt up`. 