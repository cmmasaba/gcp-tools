import atexit
import logging as pylogging
import os
from enum import Enum
from concurrent.futures import TimeoutError
from threading import Lock

from google.cloud import bigquery, logging, storage
from google.cloud.exceptions import GoogleCloudError
from google.cloud.logging.handlers import CloudLoggingHandler


class WriteDisposition(Enum):
    APPEND = "append"
    EMPTY = "empty"
    TRUNCATE = "truncate"


class CreateDisposition(Enum):
    CREATE = "create"
    DO_NOT_CREATE = "do not create"


class SourceFormat(Enum):
    AVRO = "avro"
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    DATASTORE_BACKUP = "datastore_backup"
    ORC = "orc"


class GCP:
    def __init__(self) -> None:
        self.storage_client = storage.Client(project=os.getenv("GCP_PROJECT_ID"))
        self.storage_bucket = self.storage_client.bucket(
            bucket_name=os.getenv("GCP_BUCKET_NAME")
        )
        self.bigquery_client = bigquery.Client(project=os.getenv("GCP_PROJECT_ID"))
        self.gcl_client = logging.Client.from_service_account_json(
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
            project=os.getenv("GCP_PROJECT_ID"),
        )
        self.logger = None
        self._logger_lock = Lock()
        atexit.register(self.cleanup_logger)

    def directory_exists(self, directory_name) -> bool:
        """
        Check if directory_name is in the bucket.

        Args:
            bucket (str): the Google Cloud Storage to check in.
            directory_name (str): the name of the directory to search for.
        Returns:
            True if the directory name is in the bucket, otherwise False
        """
        if not directory_name.endswith("/"):
            directory_name = directory_name + "/"

        blobs = list(
            self.storage_bucket.list_blobs(prefix=directory_name, max_results=1)
        )

        return len(blobs) > 0

    def gcs_add_directory(self, directory_name: str) -> bool:
        """
        Add an empty directory to the cloud storage bucket.

        Args:
            directory_name (str): the name of the directory to add.

        Returns:
            True if added successfully or directory already existed.
        """
        if not self.directory_exists(directory_name) and not directory_name.endswith(
            "/"
        ):
            directory_name = directory_name + "/"

            blob = self.storage_bucket.blob(directory_name)
            blob.upload_from_string(
                "", content_type="application/x-www-form-urlencoded:charset=UTF-8"
            )

        return True

    def gcs_add_file(self, file_path: str, directory_name: str) -> str:
        """
        Add a file to the cloud storage bucket.

        Args:
            file_path (str): the path to the file.
            directory_name (str): the name of the GCS directory to upload the file to.

        Returns:
            a link to the file in Google Cloud Storage.
        """
        if not directory_name.endswith("/"):
            directory_name = directory_name + "/"

        self.gcs_add_directory(directory_name)

        blob = self.storage_bucket.blob(directory_name + os.path.basename(file_path))
        blob.upload_from_filename(file_path)

        return str(blob.self_link)

    def bq_load_table_from_file(
        self,
        table_name: str,
        file_path: str,
        source_format: SourceFormat,
        write_disposition: WriteDisposition,
        create_disposition: CreateDisposition,
        schema: list[bigquery.SchemaField],
    ) -> bool:
        """Add rows to a table in BQ from the data in the given file.

        Args:
            table_name (str): name of table in BigQuery
            file_path (str): path to the file location
            source_format (str): format of the data in the file, json or csv
            write_disposition (str): how writes should be performed if the table contains data;
            create_disposition (str): how creates should be done if the table doesn't exist
            schema (list): schema of the data in the BigQuery table provided above
        Returns:
            True is successful otherwise False.
        """
        table_id = (
            f"{os.getenv("GCP_PROJECT_ID")}.{os.getenv("GCP_DATASET_ID")}.{table_name}"
        )

        match source_format:
            case SourceFormat.AVRO:
                s_format = bigquery.SourceFormat.AVRO
            case SourceFormat.CSV:
                s_format = bigquery.SourceFormat.CSV
            case SourceFormat.JSON:
                s_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            case SourceFormat.ORC:
                s_format = bigquery.SourceFormat.ORC
            case SourceFormat.PARQUET:
                s_format = bigquery.SourceFormat.PARQUET
            case SourceFormat.DATASTORE_BACKUP:
                s_format = bigquery.SourceFormat.DATASTORE_BACKUP

        match write_disposition:
            case WriteDisposition.APPEND:
                w_disposition = bigquery.WriteDisposition.WRITE_APPEND
            case WriteDisposition.TRUNCATE:
                w_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            case WriteDisposition.EMPTY:
                w_disposition = bigquery.WriteDisposition.WRITE_EMPTY

        match create_disposition:
            case CreateDisposition.CREATE:
                c_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
            case CreateDisposition.DO_NOT_CREATE:
                c_disposition = bigquery.CreateDisposition.CREATE_NEVER

        job_config = bigquery.LoadJobConfig(
            autodetect=schema,
            source_format=s_format,
            write_disposition=w_disposition,
            create_disposition=c_disposition,
        )

        with open(file_path, "rb", encoding="utf-8") as fp:
            try:
                job = self.bigquery_client.load_table_from_file(
                    file_obj=fp, destination=table_id, job_config=job_config
                )
                job.result()
            except (
                ValueError,
                TypeError,
                GoogleCloudError,
                TimeoutError,
            ) as e:
                self.logger.error("[bq_load_table_from_file]: error: %s", e)
                raise e

        return True

    def get_logger(self) -> pylogging.Logger:
        """Return a logger that writes to Google Cloud Logging."""
        with self._logger_lock:
            logger_name = os.getenv("GCP_LOGGER_NAME", "main")
            logger = pylogging.getLogger(logger_name)

            if not logger.handlers:
                logger.addHandler(
                    CloudLoggingHandler(self.gcl_client, name=logger_name)
                )
                logger.setLevel(pylogging.INFO)
                logger.propagate = False

            self.logger = logger

            return self.logger

    def cleanup_logger(self) -> None:
        """Flush and close all logging handlers."""
        if not self.logger:
            return

        handlers = self.logger.handlers[:]

        for handler in handlers:
            try:
                handler.flush()
                handler.close()
                self.logger.removeHandler(handler)
            except Exception as e:
                print(f"Error cleaning up logging handler: {e}")

        self.logger = None
