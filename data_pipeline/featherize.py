#!/usr/bin/env python3
from pathlib import Path

import git
import pandas as pd
import pyreadstat
import requests


REPO_ROOT = Path(git.Repo('.', search_parent_directories=True).working_tree_dir).absolute()
DATA_PATH = REPO_ROOT / 'data'


def featherize_scdb_datasets():
    sav_files_dir = DATA_PATH / 'raw' / 'scdb'
    interim_data_dir = DATA_PATH / 'interim' / 'scdb'
    interim_data_dir.mkdir(parents=True, exist_ok=True)

    for scdb_sav_path in sav_files_dir.glob('*.sav'):
        scdb_feather_path = interim_data_dir / f'{scdb_sav_path.stem}.feather'
        scdb_dataset = scdb_sav_to_dataframe(scdb_sav_path)
        categorical_columns = scdb_dataset.select_dtypes(include='category').columns
        (scdb_dataset
             .pipe(lambda df: df.astype({column: str for column in categorical_columns}))
             .pipe(lambda df: df.fillna({
                 column_name: 'MISSING_VALUE'
                 for column_name in df.select_dtypes(include='object').columns
             }))
             .pipe(lambda df: df.astype({column: 'category' for column in categorical_columns}))
             .to_feather(scdb_feather_path))


def scdb_sav_to_dataframe(scdb_sav_path):
    try:
        dataset = pd.read_spss(str(scdb_sav_path))  # <-- str(…) due to a bug in pandas before v1.1.3
                                                    # see https://github.com/pandas-dev/pandas/pull/36174
    except Exception:
        dataset, _ = pyreadstat.read_sav(
            str(scdb_sav_path),
            apply_value_formats=True,
            encoding='iso-8859-1'
        )
    return dataset


if __name__ == '__main__':
    featherize_scdb_datasets()
