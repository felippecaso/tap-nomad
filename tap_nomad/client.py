"""Custom client handling, including NomadStream base class."""

from __future__ import annotations

import os
from typing import Iterable, List

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

    def get_file_paths(self) -> list:
        """Return a list of file paths to read.

        This tap accepts file names and directories so it will detect
        directories and iterate files inside.

        Returns:
            return (List): A list with file paths to read.

        Raises:
            Exception: When file path does not exist.
        """
        # Cache file paths so we dont have to iterate multiple times
        if self.file_paths:
            return self.file_paths

        file_path = self.file_config["path"]
        if not os.path.exists(file_path):
            raise Exception(f"File path does not exist {file_path}")

        file_paths = []
        if os.path.isdir(file_path):
            clean_file_path = os.path.normpath(file_path) + os.sep
            for filename in os.listdir(clean_file_path):
                file_path = clean_file_path + filename
                file_paths.append(file_path)
        else:
            file_paths.append(file_path)

        if not file_paths:
            raise Exception(
                f"Stream '{self.name}' has no acceptable files. \
                    See warning for more detail."
            )
        self.file_paths = file_paths
        return file_paths

    name = "nomad_transactions"
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("date", th.DateTimeType, required=True),
        th.Property("amount", th.NumberType, required=True),
        th.Property("description", th.StringType, required=True),
        th.Property("status", th.StringType, required=True),
    ).to_dict()

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
                nomad_raw_list: List[pd.DataFrame] = tabula.read_pdf(
                    file_path,
                    pages="all",
                    multiple_tables=False,
                    pandas_options={"header": None},
                    area=[160, 32, 520, 570],
                )  # type: ignore
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
