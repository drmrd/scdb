#!/usr/bin/env python3
import io
import zipfile

from pathlib import Path

import git
import requests

SCDB_DATASET_URL_TEMPLATE = ''.join([
    'http://supremecourtdatabase.org/_brickFiles/{scdb_version}/',
    'SCDB_{scdb_version}_{unit_of_analysis}Centered_{record_granularity}',
    '{file_extension}'
])

_REPO_ROOT = Path(
    git.Repo('.', search_parent_directories=True).working_tree_dir
).absolute()
_DATA_PATH = _REPO_ROOT / 'data'


def acquire_scdb_datasets(destination: Path = _DATA_PATH / 'raw' / 'scdb'):
    for unit_of_analysis in ['case', 'justice']:
        acquire_scdb_dataset(
            scdb_version='Legacy_06',
            dataset_version=f'{unit_of_analysis}Centered_Citation',
            data_dir=destination
        )
        for record_granularity in ['Citation', 'Docket', 'LegalProvision', 'Vote']:
            acquire_scdb_dataset(
                scdb_version='2020_01',
                dataset_version=f'{unit_of_analysis}Centered_{record_granularity}',
                data_dir=destination
            )


def acquire_scdb_dataset(scdb_version, dataset_version, data_dir):
    data_dir.mkdir(parents=True, exist_ok=True)
    file_name = f'SCDB_{scdb_version}_{dataset_version}.sav'
    url = f'http://supremecourtdatabase.org/_brickFiles/{scdb_version}/{file_name}.zip'
    local_file = Path(data_dir) / file_name

    if not local_file.exists():
        download_and_extract_zip(url, data_dir)


def download_and_extract_zip(url, destination_dir):
    zip_file_stream = io.BytesIO()
    zip_file_stream.write(requests.get(url).content)
    zipfile.ZipFile(zip_file_stream).extractall(destination_dir)


if __name__ == '__main__':
    acquire_scdb_datasets()
