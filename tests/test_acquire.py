import io
import zipfile
from unittest.mock import Mock, patch

import pytest

from data_pipeline.acquire import acquire_all_scdb_datasets_by_version, SCDB_DATASET_URL_TEMPLATE


DESIRED_VERSIONS = ['Legacy_999', 'Legacy_3000', 'some_other_version', 'and_another', 'and_one_more_for_good_measure']
DESIRED_DATASET_URLS = [
    SCDB_DATASET_URL_TEMPLATE.format(
        scdb_version=version,
        unit_of_analysis=unit,
        record_granularity=granularity,
        file_extension=extension
    )
    for version in DESIRED_VERSIONS
    for unit in ['case', 'justice']
    for granularity in ['Citation', 'Docket', 'LegalProvision', 'Vote']
    for extension in ['.sav.zip']
    if not version.startswith('Legacy') or granularity == 'Citation'
]

DESIRED_FILE_NAMES = [
    url.rsplit('/', maxsplit=1)[-1]
    for url in DESIRED_DATASET_URLS
]


class ZipFileDouble:
    def __init__(self):
        self.provided_files = []
        self.extraction_paths = []

    def __call__(self, file_object, *_, **__):
        self.provided_files.append(file_object)
        return self

    def extractall(self, path, *_, **__):
        self.extraction_paths.append(path)


@pytest.fixture(autouse=True)
def responses():
    import responses
    with responses.RequestsMock(assert_all_requests_are_fired=True) as resps:
        for url, name in zip(DESIRED_DATASET_URLS, DESIRED_FILE_NAMES):
            resps.add('GET', url, body=name)
        yield resps


def test_requests_all_desired_datasets(responses, monkeypatch, tmp_path):
    monkeypatch.setattr(zipfile, 'ZipFile', Mock())

    acquire_all_scdb_datasets_by_version(scdb_versions=DESIRED_VERSIONS, destination=tmp_path)

    requested_urls = [call.response.url for call in responses.calls]
    assert sorted(requested_urls) == sorted(DESIRED_DATASET_URLS)


def test_attempts_to_create_zip_file_objects_from_all_downloaded_files(monkeypatch, tmp_path):
    zip_file_double = ZipFileDouble()
    monkeypatch.setattr(zipfile, 'ZipFile', zip_file_double)

    acquire_all_scdb_datasets_by_version(scdb_versions=DESIRED_VERSIONS, destination=tmp_path)

    requested_file_names = []
    for provided_file in zip_file_double.provided_files:
        assert isinstance(provided_file, io.BytesIO)
        provided_file.seek(0)
        requested_file_names.append(provided_file.read().decode('utf-8'))
    assert sorted(requested_file_names) == sorted(DESIRED_FILE_NAMES)


def test_attempts_to_unzip_all_downloaded_files(monkeypatch, tmp_path):
    desired_legacy_version_count = sum(version.startswith('Legacy') for version in DESIRED_VERSIONS)
    desired_modern_version_count = len(DESIRED_VERSIONS) - desired_legacy_version_count
    expected_files_per_legacy_version = 2
    expected_files_per_modern_version = 8

    zip_file_double = ZipFileDouble()
    monkeypatch.setattr(zipfile, 'ZipFile', zip_file_double)

    acquire_all_scdb_datasets_by_version(scdb_versions=DESIRED_VERSIONS, destination=tmp_path)

    assert len(zip_file_double.provided_files) == len(zip_file_double.extraction_paths)

    actual_legacy_file_count = sum('Legacy' in str(path) for path in zip_file_double.extraction_paths)
    actual_modern_file_count = len(zip_file_double.extraction_paths) - actual_legacy_file_count

    assert actual_legacy_file_count == desired_legacy_version_count * expected_files_per_legacy_version
    assert actual_modern_file_count == desired_modern_version_count * expected_files_per_modern_version


@patch('pathlib.Path.exists', lambda *_, **__: True)
def test_does_not_request_datasets_that_have_already_been_downloaded(tmp_path, responses):
    responses.reset()

    acquire_all_scdb_datasets_by_version(destination=tmp_path)