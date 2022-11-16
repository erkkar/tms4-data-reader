"""Module for reading TOMST TMS-4 logger data"""

from pathlib import Path
import re
from collections.abc import Iterable
import logging
import datetime
import os.path

import dateutil
import pandas as pd


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
        self._filepattern = "data_*_????_??_??_?.csv"

    @property
    def filecount(self):
        """Get number of data files in this dataset"""
        return sum(1 for _ in self.data_dir.glob(self._filepattern))

    @property
    def loggers(self) -> set[int]:
        """Get logger id’s contained in this dataset"""
        return set(
            map(TMSDataReader._get_logger_id, self.data_dir.glob(self._filepattern))
        )

    def check_missing(self, all_loggers: Iterable[int]) -> set:
        """Check against a list of logger id’s"""
        return set(all_loggers).difference(self.loggers)

    @staticmethod
    def _read_file(filepath: Path) -> pd.DataFrame:

        try:
            df = pd.read_csv(
                filepath,
                index_col=False,
                sep=";",
                header=None,
                names=[  # File format: https://tomst.com/web/en/systems/tms/software/
                    "measurement_id",
                    "timestamp",
                    "time zone",
                    "T1",
                    "T2",
                    "T3",
                    "soilmoist_count",
                    "shake",
                    "errFlag",
                ],
            )
            # Parse timestamps
            df["timestamp"] = pd.to_datetime(
                # Replace dots as time separators with colons,
                # see: https://github.com/dateutil/dateutil/issues/252
                df["timestamp"].str.replace(
                    r"\d{2}.\d{2}$",  # Match only time HH.MM at the end of the string
                    lambda match: match.group().replace(".", ":"),
                    regex=True,
                ),
                utc=True,
                infer_datetime_format=True,
            )
        except (pd.errors.ParserError, dateutil.parser.ParserError, ValueError) as err:
            logging.warning("Failed reading file %s: %s", filepath.name, err)
            return None

        # Parse temperature columns as floats if this failed when reading the file
        try:
            df[["T1", "T2", "T3"]] = df[["T1", "T2", "T3"]].apply(
                lambda ts: ts.str.replace(",", ".").astype(float)
            )
        except AttributeError:
            pass  # values are already floats

        # Parse logger id from the file name
        df["logger_id"] = TMSDataReader._get_logger_id(filepath)

        # Add file modification time
        df["read_time"] = datetime.datetime.fromtimestamp(
            os.path.getmtime(filepath),
        )

        # Find duplicated timestamps
        duplicates = df.duplicated("timestamp", keep="first")

        # Return data with duplicates removed
        return df.loc[~duplicates].set_index(["timestamp", "logger_id"]).sort_index()

    @staticmethod
    def _get_logger_id(filepath):
        return int(re.match(r"^data_(\d+)", filepath.stem).group(1))

    def read(self) -> pd.DataFrame:
        """Read data files into a Data Frame

        Duplicated timestamps are removed automatically leaving the first record.
        """
        return pd.concat(
            (
                self._read_file(filepath)
                for filepath in self.data_dir.glob(self._filepattern)
            ),
            axis=0,
        ).sort_index()
