import logging
import os
import re
from pathlib import Path

from google.cloud import storage  # type: ignore[attr-defined]
from retry import retry

logger = logging.getLogger(__name__)


class GCSConnector:
    """
    A class for interacting with Google Cloud Storage.
    Uses the default application credentials.
    :param project_name: the name of the project. Cannot be None.
    """

    def __init__(self, project_name: str):
        self.storage_client = storage.Client(project=project_name)
        # The slash pattern is used to replace backslashes with forward slashes
        self.slash_pattern = re.compile(r"[\\|/]+")

    def download(self, input_path: str, output_path: Path) -> None:
        """
        Downloads file or folder from storage
        :param input_path: Cloud path to file or folder. Format: gs://bucket_name/path/to/file
        :param output_path: Local path to file or folder
        :return:
        """
        try:
            if self._is_cloud_path_folder(input_path):
                self._download_folder(input_path, str(output_path))
            else:
                self._download_file(input_path, str(output_path))
        except Exception as exception:
            logger.warning(f"Failed to download {input_path} to {output_path}")
            raise exception

    def _clean_path(self, cloud_file_path: str) -> str:
        """
        Removes redundant slashes and backslashes from the path
        Fixes the difference between Windows and Linux paths
        :param cloud_file_path: Cloud path to file or folder
        :return:
        """
        cloud_file_path = self.slash_pattern.sub("/", cloud_file_path.strip("/\\"))
        cloud_file_path = cloud_file_path.removesuffix("/").removeprefix("/")
        return cloud_file_path

    def get_all_files(self, cloud_folder_path: str) -> list[str]:
        """
        Returns a list of all files in the folder
        :param cloud_folder_path: Cloud path to folder
        :return:
        """
        bucket_name, cloud_folder_path = self._parse_cloud_path(cloud_folder_path)
        bucket = self.storage_client.get_bucket(bucket_name)
        prefix = cloud_folder_path + "/"
        blobs_list = bucket.list_blobs(prefix=prefix)
        return [blob.name.removeprefix(prefix) for blob in blobs_list]

    def _parse_cloud_path(
        self,
        full_path: str,
    ) -> tuple[str, str]:
        """
        Parses cloud path to bucket name and file name.
        Removes redundant slashes and backslashes from the path.
        :param full_path: An absolute path to file or folder starting from gs://
        :return:
        """
        if full_path.startswith("gs:"):
            full_path_clean = self._clean_path(full_path[3:])
            bucket_name, file_name = full_path_clean.split("/", 1)
            return bucket_name, file_name
        raise ValueError(f"Invalid cloud path {full_path}")

    @retry(tries=3)
    def _download_file(self, file_cloud_path: str, local_file_path: str) -> None:
        """
        Downloads file from storage
        :param file_cloud_path: Cloud path to input file
        :param local_file_path: local path to output file
        """
        bucket_name, file_cloud_path = self._parse_cloud_path(file_cloud_path)
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        bucket = self.storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_cloud_path)
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        blob.download_to_filename(local_file_path)

    @retry(tries=3)
    def _download_folder(self, cloud_folder_path: str, local_folder_path: str) -> None:
        """
        Downloads all files from folder in storage
        :param cloud_folder_path: Cloud path to input folder
        :param local_folder_path: local path to output folder
        """
        bucket_name, cloud_folder_path = self._parse_cloud_path(cloud_folder_path)
        local_folder_path = local_folder_path.removesuffix("/").removesuffix("\\")
        downloaded_files = []
        try:
            prefix = cloud_folder_path + "/"
            bucket = self.storage_client.get_bucket(bucket_name)
            blobs_list = bucket.list_blobs(prefix=prefix)
            for blob in blobs_list:
                cloud_file_path = blob.name.removeprefix(prefix)
                # In some cases there is a blob with empty name, that can't be seen with UI.
                if len(cloud_file_path) == 0:
                    continue
                local_file_path = os.path.join(local_folder_path, cloud_file_path)
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                blob.download_to_filename(local_file_path)
                downloaded_files.append(local_file_path)
        except Exception:
            for local_file_path in downloaded_files:
                os.remove(local_file_path)
                logger.debug(f"Removed file {local_file_path} due to exception")
            raise  # reraise same exception

    def _is_cloud_path_folder(self, cloud_path: str) -> bool:
        """
        Checks if cloud path is a folder
        :param cloud_path: absolute or relative path to file or folder
        :return:
        """
        bucket_name, cloud_path = self._parse_cloud_path(cloud_path)
        bucket = self.storage_client.get_bucket(bucket_name)
        blobs_list = bucket.list_blobs(prefix=cloud_path + "/", max_results=1)
        return len(list(blobs_list)) > 0

    def is_exist(self, cloud_path: str) -> bool:
        """
        Checks if file or folder exists in storage
        :param cloud_path: Cloud path to file or folder
        :return:
        """
        bucket_name, file_name = self._parse_cloud_path(cloud_path)
        bucket = self.storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        return blob.exists()

    @retry(tries=3)
    def upload_file(self, local_file_path: Path, cloud_file_path: str) -> None:
        """
        Uploads file to storage
        :param local_file_path: local path to input file
        :param cloud_file_path: Cloud path to output file
        """
        bucket_name, cloud_file_path = self._parse_cloud_path(cloud_file_path)
        bucket = self.storage_client.get_bucket(bucket_name)
        blob = bucket.blob(cloud_file_path)
        blob.upload_from_filename(local_file_path)