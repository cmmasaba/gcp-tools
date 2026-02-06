# GCP Utilities

Simple wrapper for common GCP operations like:

- creating a Google CLoud Storage directory
- checking if a Google Cloud Storage directory exists
- adding a file to Google Cloud Storage
- loading tables from files to Google BigQuery
- logging messages to Google Cloud Logging

## Env Vars

The following environment variables need to be set:

```bash

export GCP_PROJECT_ID=""
export GCP_BUCKET_NAME=""
export GCP_DATASET_ID=""
export GCP_LOGGER_NAME=""
export GOOGLE_APPLICATION_CREDENTIALS=""

```
