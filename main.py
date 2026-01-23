import os

from google.cloud import storage, bigquery


class GCP:
    def __init__(self) -> None:
        self.storage_client = storage.Client(project=os.getenv("GCP_PROJECT_ID"))
        self.storage_bucket = self.storage_client.bucket(
            bucket_name=os.getenv("GCP_BUCKET_NAME")
        )

    def directory_exists(self, directory_name) -> bool:
        """
        Check if directory_name is in the bucket.

        Args:
            bucket: the Google Cloud Storage to check in.
            directory_name: the name of the directory to search for.
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
            directory_name: the name of the directory to add.

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
            file_path: the path to the file.
            directory_name: the name of the GCS directory to upload the file to.

        Returns:
            a link to the file in Google Cloud Storage.
        """
        if not directory_name.endswith("/"):
            directory_name = directory_name + "/"

        blob = self.storage_bucket.blob(directory_name + os.path.basename(file_path))
        blob.upload_from_filename(file_path)

        return str(blob.self_link)
