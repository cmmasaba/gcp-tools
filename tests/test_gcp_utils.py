from unittest import TestCase
from unittest.mock import MagicMock, patch, mock_open

import pytest

from gcp_utilities.main import (
    GCP,
    SourceFormat,
    WriteDisposition,
    CreateDisposition,
)

from google.cloud import bigquery

MOCK_ROOT = "gcp_utilities.main."


class GCPTestCase(TestCase):
    """Test GCP class"""

    def setUp(self) -> None:
        super().setUp()

        self.env_patcher = patch.dict(
            "os.environ",
            {
                "GCP_PROJECT_ID": "project-id",
                "GCP_BUCKET_NAME": "bucket-name",
                "GOOGLE_APPLICATION_CREDENTIALS": "creds.json",
                "GCP_LOGGER_NAME": "logger",
                "GCP_DATASET_ID": "dataset",
            },
        )
        self.env_patcher.start()
        self.gcs_patcher = patch(MOCK_ROOT + "storage.Client")
        self.bq_patcher = patch(MOCK_ROOT + "bigquery.Client")
        self.gcl_patcher = patch(MOCK_ROOT + "logging.Client")

        self.mock_gcs = self.gcs_patcher.start()
        self.mock_gcs_bucket = MagicMock()
        self.mock_gcs.return_value.bucket.return_value = self.mock_gcs_bucket

        self.mock_bq = self.bq_patcher.start()
        self.mock_bq_instance = MagicMock()
        self.mock_bq.return_value = self.mock_bq_instance

        self.mock_gcl = self.gcl_patcher.start()
        self.mock_gcl_instance = MagicMock()
        self.mock_gcl.return_value.from_service_account_json.return_value = (
            self.mock_gcl_instance
        )

        self.gcp = GCP()

    def tearDown(self) -> None:
        """Stop patchers after test."""
        self.env_patcher.stop()
        self.gcl_patcher.stop()
        self.bq_patcher.stop()
        self.gcl_patcher.stop()

        super().tearDown()

    def test_directory_exists(self) -> None:
        """Test directory_exists method."""
        self.mock_gcs_bucket.list_blobs.return_value = (1,)

        self.gcp.directory_exists("test_dir")

        self.mock_gcs_bucket.list_blobs.assert_called_once_with(
            prefix="test_dir/", max_results=1
        )

    def test_gcs_add_directory(self) -> None:
        """Test gcs_add_directory method."""
        mock_blob = MagicMock()
        self.mock_gcs_bucket.list_blobs.return_value = ()
        self.mock_gcs_bucket.blob.return_value = mock_blob

        self.gcp.gcs_add_directory("test_dir")
        self.mock_gcs_bucket.list_blobs.assert_called_once_with(
            prefix="test_dir/", max_results=1
        )
        self.mock_gcs_bucket.blob.assert_called_once_with("test_dir/")
        mock_blob.upload_from_string.assert_called_once()

    def test_gcs_add_file(self) -> None:
        """Test gcs_add_file method."""
        self.mock_gcs_bucket.list_blobs.return_value = (1,)
        mock_blob = MagicMock()
        self.mock_gcs_bucket.blob.return_value = mock_blob

        self.gcp.gcs_add_file("/path/to/file.txt", "test_dir/")

        self.mock_gcs_bucket.blob.assert_called_once_with("test_dir/file.txt")
        mock_blob.upload_from_filename.assert_called_once_with("/path/to/file.txt")

        self.mock_gcs_bucket.reset_mock()
        mock_blob.reset_mock()

        self.gcp.gcs_add_file("/path/to/file.txt", "test_dir")

        self.mock_gcs_bucket.blob.assert_called_once_with("test_dir/file.txt")
        mock_blob.upload_from_filename.assert_called_once_with("/path/to/file.txt")

    def test_bq_load_table_from_file_json(self) -> None:
        """Test bq_load_table_from_file method."""
        mock_job = MagicMock()
        self.mock_bq_instance.load_table_from_file.return_value = mock_job
        mock_content = b"some content"

        with patch("builtins.open", mock_open(read_data=mock_content)) as mock_file:

            self.gcp.bq_load_table_from_file(
                "test_table",
                "/path/to/creds.json",
                SourceFormat.JSON,
                WriteDisposition.EMPTY,
                CreateDisposition.DO_NOT_CREATE,
                [bigquery.SchemaField("col", "STRING", "NULLABLE")],
            )

            mock_file.assert_called_once_with("/path/to/creds.json", "rb")
            mock_job.result.assert_called_once()

    def test_bq_load_table_from_file_avro(self) -> None:
        """Test bq_load_table_from_file method."""
        mock_job = MagicMock()
        self.mock_bq_instance.load_table_from_file.return_value = mock_job
        mock_content = b"some content"

        with patch("builtins.open", mock_open(read_data=mock_content)) as mock_file:

            self.gcp.bq_load_table_from_file(
                "test_table",
                "/path/to/creds.avro",
                SourceFormat.AVRO,
                WriteDisposition.TRUNCATE,
                CreateDisposition.CREATE,
                [bigquery.SchemaField("col", "STRING", "NULLABLE")],
            )

            mock_file.assert_called_once_with("/path/to/creds.avro", "rb")
            mock_job.result.assert_called_once()

    def test_bq_load_table_from_file_csv(self) -> None:
        """Test bq_load_table_from_file method."""
        mock_job = MagicMock()
        self.mock_bq_instance.load_table_from_file.return_value = mock_job
        mock_content = b"some content"

        with patch("builtins.open", mock_open(read_data=mock_content)) as mock_file:

            self.gcp.bq_load_table_from_file(
                "test_table",
                "/path/to/creds.csv",
                SourceFormat.CSV,
                WriteDisposition.APPEND,
                CreateDisposition.CREATE,
                [bigquery.SchemaField("col", "STRING", "NULLABLE")],
            )

            mock_file.assert_called_once_with("/path/to/creds.csv", "rb")
            mock_job.result.assert_called_once()

    def test_bq_load_table_from_file_orc(self) -> None:
        """Test bq_load_table_from_file method."""
        mock_job = MagicMock()
        self.mock_bq_instance.load_table_from_file.return_value = mock_job
        mock_content = b"some content"

        with patch("builtins.open", mock_open(read_data=mock_content)) as mock_file:

            self.gcp.bq_load_table_from_file(
                "test_table",
                "/path/to/creds.orc",
                SourceFormat.ORC,
                WriteDisposition.EMPTY,
                CreateDisposition.CREATE,
                [bigquery.SchemaField("col", "STRING", "NULLABLE")],
            )

            mock_file.assert_called_once_with("/path/to/creds.orc", "rb")
            mock_job.result.assert_called_once()

    def test_bq_load_table_from_file_parquet(self) -> None:
        """Test bq_load_table_from_file method."""
        mock_job = MagicMock()
        self.mock_bq_instance.load_table_from_file.return_value = mock_job
        mock_content = b"some content"

        with patch("builtins.open", mock_open(read_data=mock_content)) as mock_file:

            self.gcp.bq_load_table_from_file(
                "test_table",
                "/path/to/creds.parquet",
                SourceFormat.PARQUET,
                WriteDisposition.EMPTY,
                CreateDisposition.CREATE,
                [bigquery.SchemaField("col", "STRING", "NULLABLE")],
            )

            mock_file.assert_called_once_with("/path/to/creds.parquet", "rb")
            mock_job.result.assert_called_once()

    def test_bq_load_table_from_file_db_backup(self) -> None:
        """Test bq_load_table_from_file method."""
        mock_job = MagicMock()
        self.mock_bq_instance.load_table_from_file.return_value = mock_job
        mock_content = b"some content"

        with patch("builtins.open", mock_open(read_data=mock_content)) as mock_file:

            self.gcp.bq_load_table_from_file(
                "test_table",
                "/path/to/creds.bak",
                SourceFormat.DATASTORE_BACKUP,
                WriteDisposition.EMPTY,
                CreateDisposition.CREATE,
                [bigquery.SchemaField("col", "STRING", "NULLABLE")],
            )

            mock_file.assert_called_once_with("/path/to/creds.bak", "rb")
            mock_job.result.assert_called_once()

    def test_bq_load_table_from_file_db_backup_fail(self) -> None:
        """Test bq_load_table_from_file method."""
        mock_job = MagicMock()
        mock_logger = MagicMock()
        self.gcp.logger = mock_logger

        self.mock_bq_instance.load_table_from_file.return_value = mock_job
        mock_content = b"some content"
        mock_job.result.side_effect = ValueError("Invalid value")

        with patch("builtins.open", mock_open(read_data=mock_content)):
            with pytest.raises(ValueError, match="Invalid value"):
                self.gcp.bq_load_table_from_file(
                    "test_table",
                    "/path/to/creds.bak",
                    SourceFormat.DATASTORE_BACKUP,
                    WriteDisposition.EMPTY,
                    CreateDisposition.CREATE,
                    [bigquery.SchemaField("col", "STRING", "NULLABLE")],
                )

            self.gcp.logger.error.assert_called_once()

    @patch(MOCK_ROOT + "CloudLoggingHandler")
    @patch(MOCK_ROOT + "pylogging")
    def test_logger(self, mock_pylogger, mock_cloud_handler) -> None:
        """Test get_logger and cleanup_logger methods."""
        mock_logger = MagicMock()
        mock_logger.handlers = []
        mock_pylogger.getLogger.return_value = mock_logger
        mock_pylogger.INFO = 20

        self.gcp.get_logger()

        mock_logger.addHandler.assert_called_once()
        mock_logger.setLevel.assert_called_once_with(20)

        mock_handler = MagicMock()
        mock_logger.handlers = [mock_handler]

        self.gcp.cleanup_logger()

        mock_handler.flush.assert_called_once()
        mock_handler.close.assert_called_once()
        mock_logger.removeHandler.assert_called_once_with(mock_handler)
