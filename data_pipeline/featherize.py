#!/usr/bin/env python3
from pathlib import Path

import git
import pandas as pd
import pyreadstat
from tqdm.auto import tqdm


_REPO_ROOT = Path(git.Repo('.', search_parent_directories=True).working_tree_dir).absolute()
_DATA_PATH = _REPO_ROOT / 'data'


def featherize_scdb_datasets():
    sav_files_dir = _DATA_PATH / 'raw' / 'scdb'
    interim_data_dir = _DATA_PATH / 'interim' / 'scdb'
    interim_data_dir.mkdir(parents=True, exist_ok=True)

    for scdb_sav_path in tqdm(list(sav_files_dir.glob('*.sav')), disable=None):
        scdb_feather_path = interim_data_dir / f'{scdb_sav_path.stem}.feather'
        scdb_dataset = scdb_sav_to_dataframe(scdb_sav_path)
        categorical_columns = scdb_dataset.select_dtypes(include='category').columns
        object_columns = scdb_dataset.select_dtypes(include='object').columns
        (scdb_dataset
             .astype({column: str for column in categorical_columns})
             .pipe(lambda df: df.fillna({
                 column_name: 'MISSING_VALUE' for column_name in object_columns
             }))
             .astype({column: 'category' for column in categorical_columns}
                     | {column: 'string' for column in object_columns})
             .to_feather(scdb_feather_path))


def scdb_sav_to_dataframe(scdb_sav_path):
    try:
        dataset = pd.read_spss(scdb_sav_path)
    except Exception:
        dataset, _ = pyreadstat.read_sav(
            str(scdb_sav_path),
            apply_value_formats=True,
            encoding='iso-8859-1'
        )
    return dataset


if __name__ == '__main__':
    featherize_scdb_datasets()
