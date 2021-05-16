import zipfile
from unittest.mock import Mock, patch

import pytest

from data_pipeline.acquire import acquire_scdb_datasets


SCDB_DATASET_URL_TEMPLATE = ''.join([
    'http://supremecourtdatabase.org/_brickFiles/{scdb_version}/',
    'SCDB_{scdb_version}_{unit_of_analysis}Centered_{record_granularity}',
    '{file_extension}'
])

DESIRED_DATASET_URLS = [
    SCDB_DATASET_URL_TEMPLATE.format(
        scdb_version=version,
        unit_of_analysis=unit,
        record_granularity=granularity,
        file_extension=extension
    )
    for version in ['2020_01', 'Legacy_06']
    for unit in ['case', 'justice']
    for granularity in ['Citation', 'Docket', 'LegalProvision', 'Vote']
    for extension in ['.sav.zip']
    if not version.startswith('Legacy') or granularity == 'Citation'
]

DESIRED_FILE_NAMES = [
    url.rsplit('/', maxsplit=1)[-1]
    for url in DESIRED_DATASET_URLS
]


@pytest.fixture(autouse=True)
def responses():
    import responses
    with responses.RequestsMock(assert_all_requests_are_fired=True) as resps:
        for url, name in zip(DESIRED_DATASET_URLS, DESIRED_FILE_NAMES):
            resps.add('GET', url, body=name)
        yield resps


def test_requests_all_desired_datasets(responses, monkeypatch, tmp_path):
    monkeypatch.setattr(zipfile, 'ZipFile', Mock())

    acquire_scdb_datasets(tmp_path)

    requested_urls = [call.response.url for call in responses.calls]
    assert sorted(requested_urls) == sorted(DESIRED_DATASET_URLS)


def test_attempts_to_unzip_all_downloaded_files(monkeypatch, tmp_path):
    zip_file_mock = Mock()
    monkeypatch.setattr(zipfile, 'ZipFile', zip_file_mock)

    acquire_scdb_datasets(tmp_path)

    requested_file_names = []
    for zip_file_mock_call in zip_file_mock.call_args_list:
        assert len(zip_file_mock_call.args) == 1
        zip_file = zip_file_mock_call.args[0]
        zip_file.seek(0)
        requested_file_names.append(zip_file.read().decode('utf-8'))

    assert sorted(requested_file_names) == sorted(DESIRED_FILE_NAMES)


@patch('pathlib.Path.exists', lambda *_, **__: True)
def test_does_not_request_datasets_that_have_already_been_downloaded(tmp_path, responses):
    responses.reset()

    acquire_scdb_datasets(tmp_path)