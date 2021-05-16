#!/usr/bin/env python3
from pathlib import Path

import git
import pandas as pd
from tqdm.auto import tqdm

_REPO_ROOT = Path(git.Repo('.', search_parent_directories=True).working_tree_dir).absolute()
_DATA_PATH = _REPO_ROOT / 'data'


def parquet_like_its_hot(feather_dir: Path = _DATA_PATH / 'feather',
                         parquet_dir: Path = _DATA_PATH / 'parquet') -> None:

    for scdb_feather in tqdm(list(feather_dir.glob('**/*.feather')), disable=None, leave=False, unit='Feather File'):
        scdb_parquet_path = (
            parquet_dir / scdb_feather.parent.relative_to(feather_dir)
                        / f'{scdb_feather.stem}.parquet'
        )
        scdb_parquet_path.parent.mkdir(parents=True, exist_ok=True)

        pd.read_feather(scdb_feather).to_parquet(scdb_parquet_path)


if __name__ == '__main__':
    parquet_like_its_hot()
