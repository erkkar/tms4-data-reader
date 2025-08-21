"""Module for reading TOMST TMS-4 logger data"""

import datetime
import logging
import os.path
import re
import warnings
from collections.abc import Iterable
from io import StringIO
from pathlib import Path

import dateutil.parser
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

DATA_FILE_PATTERN = "data_*_????_??_??_?.csv"
EMPTY_FILE_STRING = "File is empty"

# File format: https://tomst.com/web/en/systems/tms/software/
DATA_FILE_SCHEMA = {
    "measurement_id": "uint64",
    "timestamp": "str",  # parsed separately, read as string
    "time zone": "int8",
    "T1": "float16",
    "T2": "float16",
    "T3": "float16",
    "soilmoist_count": "uint16",
    "shake": "uint8",
    "errFlag": "uint8",
}


class TMSDataReader:
    """Class for reading data files produced by the TOMST Lolly software

    Attributes:
        data_dir: Directory for reading the data
        filecount (int): Number of data files
        loggers (list): Logger id’s in this dataset
    """

    def __init__(self, data_dir: str | Path):
        """_summary_

        Args:
            data_dir (str | Path): Source folder for reading data
        """
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileExistsError(f"Directory '{data_dir}' not found!")
        self._filepattern = DATA_FILE_PATTERN

    @property
    def filecount(self) -> int:
        """Get number of data files in this dataset"""
        return sum(1 for _ in self.data_dir.glob(self._filepattern))

    @property
    def loggers(self) -> set[np.uint32]:
        """Get logger id’s contained in this dataset"""
        return set(
            map(TMSDataReader._get_logger_id, self.data_dir.glob(self._filepattern))
        )

    def check_missing(self, all_loggers: Iterable[int]) -> set:
        """Check against a list of logger id’s"""
        return set(all_loggers).difference(self.loggers)

    @staticmethod
    def _read_file(filepath: Path) -> pd.DataFrame | None:
        with open(filepath, encoding="utf8") as fp:
            data = fp.read().replace(
                ",",
                ".",  # some files may have comma as decimal separator
            )
            try:
                df = pd.read_csv(
                    StringIO(data),
                    index_col=False,
                    sep=";",
                    decimal=".",
                    header=None,
                    names=list(DATA_FILE_SCHEMA),
                    dtype=DATA_FILE_SCHEMA,  # type: ignore
                ).set_index("measurement_id")
            except (
                pd.errors.ParserError,
                ValueError,
            ) as err:
                fp.seek(0)
                if fp.readline().rstrip("\n") == EMPTY_FILE_STRING:
                    logger.warning("Empty file %s", filepath.name)
                else:
                    logger.warning("Failed reading file %s: %s", filepath.name, err)
                return None

        # Parse timestamps
        with warnings.catch_warnings():
            warnings.filterwarnings(message="Could not infer format", action="error")
            try:
                df["timestamp"] = pd.to_datetime(
                    df["timestamp"],
                    utc=True,
                    yearfirst=True,
                )
            except (
                dateutil.parser.ParserError,
                UserWarning,
            ):
                # Try Lolly default date time format
                df["timestamp"] = pd.to_datetime(
                    df["timestamp"], utc=True, format="%Y/%m/%d %H.%M"
                )

        # Add file modification time rounded to seconds
        df["read_time"] = pd.to_datetime(
            datetime.datetime.fromtimestamp(
                os.path.getmtime(filepath),
            )
        ).round("s")

        # Return data with duplicated rows removed
        return df.drop_duplicates(keep="last")

    @staticmethod
    def _get_logger_id(filepath: Path) -> np.uint32:
        return np.uint32(re.match(r"^data_(\d+)", filepath.stem).group(1))  # type: ignore

    def read(self) -> pd.DataFrame:
        """Read data files into a Data Frame

        Duplicated rows (timestamp and data) are removed automatically
        leaving the last record.
        """
        return pd.concat(
            {
                self._get_logger_id(filepath): self._read_file(filepath)
                for filepath in self.data_dir.glob(self._filepattern)
            },
            names=["logger_id"],
            axis=0,
        )
