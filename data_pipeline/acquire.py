#!/usr/bin/env python3
import io
import zipfile

from pathlib import Path
from typing import List

import git
import requests

from tqdm.auto import tqdm

SCDB_DATASET_URL_TEMPLATE = ''.join([
    'http://supremecourtdatabase.org/_brickFiles/{scdb_version}/',
    'SCDB_{scdb_version}_{unit_of_analysis}Centered_{record_granularity}',
    '{file_extension}'
])

_REPO_ROOT = Path(
    git.Repo('.', search_parent_directories=True).working_tree_dir
).absolute()
_DATA_PATH = _REPO_ROOT / 'data'


def acquire_all_scdb_datasets_by_version(
    scdb_versions: List[str] = ('Legacy_06', '2020_01'),
    destination: Path = _DATA_PATH / 'raw'
) -> None:
    legacy_versions = [version for version in scdb_versions if version.startswith('Legacy')]
    modern_versions = list(set(scdb_versions) - set(legacy_versions))

    for legacy_version in tqdm(legacy_versions, disable=True if len(legacy_versions) < 2 else None, leave=False, unit='Version'):
        acquire_scdb_datasets(
            scdb_version=legacy_version,
            units_of_analysis=['case', 'justice'],
            record_granularities=['Citation'],
            destination=destination / legacy_version
        )

    for modern_version in tqdm(modern_versions, disable=True if len(modern_versions) < 2 else None, leave=False, unit='Version'):
        acquire_scdb_datasets(
            scdb_version=modern_version,
            units_of_analysis=['case', 'justice'],
            record_granularities=['Citation', 'Docket', 'LegalProvision', 'Vote'],
            destination=destination / modern_version
        )


def acquire_scdb_datasets(scdb_version, units_of_analysis, record_granularities, destination):
    for unit_of_analysis in tqdm(units_of_analysis, disable=None, leave=False, unit='Unit of Analysis'):
        for record_granularity in tqdm(record_granularities, disable=None, leave=False, unit='Record Granularity'):
            acquire_scdb_dataset(
                scdb_version=scdb_version,
                dataset_version=f'{unit_of_analysis}Centered_{record_granularity}',
                destination=destination
            )


def acquire_scdb_dataset(scdb_version, dataset_version, destination):
    source_file_name = f'SCDB_{scdb_version}_{dataset_version}.sav'
    url = f'http://supremecourtdatabase.org/_brickFiles/{scdb_version}/{source_file_name}.zip'

    destination_file_path = destination / f'{dataset_version}.sav'

    if not destination_file_path.exists():
        destination_file_path.parent.mkdir(parents=True, exist_ok=True)
        download_and_extract_zip(url, destination_file_path.parent)


def download_and_extract_zip(url, destination_dir):
    zip_file_stream = io.BytesIO()
    zip_file_stream.write(requests.get(url).content)
    zipfile.ZipFile(zip_file_stream).extractall(destination_dir)


if __name__ == '__main__':
    acquire_all_scdb_datasets_by_version()
