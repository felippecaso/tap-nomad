"""Custom client handling, including NomadStream base class."""

from __future__ import annotations

import os
import tempfile
from typing import Iterable, List

import boto3
import numpy as np
import pandas as pd
import tabula
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.streams import Stream


class NomadStream(Stream):
    """Stream class for Nomad streams."""

    file_paths: List[str] = []

    def __init__(self, *args, **kwargs):
        """Init NomadStream.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        # cache file_config so we dont need to go iterating the config list again later
        self.file_config = kwargs.pop("file_config")
        super().__init__(*args, **kwargs)

    def list_s3_files_in_folder(self, s3_bucket: str, s3_folder: str) -> List[str]:
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_folder)
        file_paths = []
        for item in response.get("Contents", []):
            if not item["Key"].endswith("/"):
                file_paths.append(f"s3://{s3_bucket}/{item['Key']}")
        return file_paths

    def get_file_paths(self) -> list:
        """Return a list of file paths to read.

        This tap accepts file names and directories so it will detect
        directories and iterate files inside, both in local files and
        in AWS S3.

        Returns:
            return (List): A list with file paths to read.

        Raises:
            Exception: When file path does not exist.
        """
        # Cache file paths so we dont have to iterate multiple times
        if self.file_paths:
            return self.file_paths

        file_path = self.file_config["path"]
        if not file_path:
            raise Exception("file_path is not provided.")

        if file_path.startswith("s3://"):
            path_split = file_path.split("/")
            s3_bucket, s3_key = path_split[2], "/".join(path_split[3:])
            if s3_key.endswith("/"):
                # If the S3 URL points to a folder, list all files inside
                file_paths = self.list_s3_files_in_folder(s3_bucket, s3_key)
                self.file_paths = file_paths
            else:
                # Treat the S3 URL as a single file
                self.file_paths = [file_path]
                file_paths = [file_path]
        elif os.path.isdir(file_path):
            clean_file_path = os.path.normpath(file_path) + os.sep
            file_paths = []
            for filename in os.listdir(clean_file_path):
                file_paths.append(clean_file_path + filename)
            self.file_paths = file_paths
        elif os.path.exists(file_path):
            self.file_paths = [file_path]
            file_paths = [file_path]
        else:
            raise Exception(f"File path does not exist: {file_path}")

        if not self.file_paths:
            raise Exception(f"No acceptable files found for stream '{self.name}'.")

        return file_paths

    name = "nomad_transactions"
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("date", th.DateTimeType, required=True),
        th.Property("amount", th.NumberType, required=True),
        th.Property("description", th.StringType, required=True),
        th.Property("status", th.StringType, required=True),
    ).to_dict()

    def read_pdf_from_s3(self, s3_bucket: str, s3_key: str):
        # Use boto3 to download the file from S3 and then read it
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        pdf_content = response["Body"].read()

        # Create a temporary file-like object for tabula to read from
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(pdf_content)
            temp_file_path = temp_file.name

        try:
            nomad_raw_list: List[pd.DataFrame] = tabula.read_pdf(
                temp_file_path,
                pages="all",
                multiple_tables=False,
                pandas_options={"header": None},
                area=[160, 32, 520, 570],
            )  # type: ignore
        finally:
            # Clean up the temporary file
            os.remove(temp_file_path)
        return nomad_raw_list

    def read_pdf_from_local(self, file_path: str):
        nomad_raw_list: List[pd.DataFrame] = tabula.read_pdf(
            file_path,
            pages="all",
            multiple_tables=False,
            pandas_options={"header": None},
            area=[160, 32, 520, 570],
        )  # type: ignore
        return nomad_raw_list

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Yield a generator of record-type dictionary objects.

        This function will yield a lot of records based on the defined streams.

        Args:
            context: An optional context as dictionary.

        Yields:
            yields: A yielding record with the stream records.
        """
        for file_path in self.get_file_paths():
            if file_path.endswith(".pdf"):
                if file_path.startswith("s3://"):
                    s3_bucket, s3_key = file_path.split("/", 3)[2:]
                    nomad_raw_list = self.read_pdf_from_s3(s3_bucket, s3_key)
                else:
                    nomad_raw_list = self.read_pdf_from_local(file_path)

                nomad_raw: pd.DataFrame = nomad_raw_list[0]
                i = 0
                df_list = []
                while i < len(nomad_raw):
                    df_list.append(
                        [
                            nomad_raw.iloc[i, 0],
                            nomad_raw.iloc[i + 1, 1],
                            nomad_raw.iloc[i + 1, 2][:1],
                            nomad_raw.iloc[i + 1, 2][4:],
                            nomad_raw.iloc[i + 2, 0],
                        ]
                    )
                    i += 3

                nomad: pd.DataFrame = pd.DataFrame(
                    df_list, columns=["description", "date", "type", "amount", "status"]
                )

                nomad["date"] = pd.to_datetime(nomad["date"], dayfirst=True)
                nomad["amount"] = [
                    x.replace(".", "").replace(",", ".") for x in nomad["amount"]
                ]
                nomad["amount"] = np.where(
                    nomad["type"] == "-",
                    -1 * nomad["amount"].astype(float),
                    nomad["amount"].astype(float),
                )
                nomad = nomad[["date", "amount", "description", "status"]]
                nomad = nomad.convert_dtypes()

                for r in nomad.to_dict("records"):
                    yield r
