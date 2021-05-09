#!/usr/bin/env python3
import datetime as dt
from pathlib import Path

import git
import pandas as pd

REPO_ROOT = Path(git.Repo('.', search_parent_directories=True).working_tree_dir).absolute()
DATA_PATH = REPO_ROOT / 'data'


def wrangle_scdb_data():
    interim_data_dir = DATA_PATH / 'interim' / 'scdb'
    processed_data_dir = DATA_PATH / 'processed' / 'scdb'
    processed_data_dir.mkdir(parents=True, exist_ok=True)
    scdb_datasets = {
        'SCDB_Legacy-and-2020r1_caseCentered_Citation': pd.concat(
            [pd.read_feather(interim_data_dir / f'SCDB_{scdb_version}_caseCentered_Citation.feather')
             for scdb_version in ['Legacy_06', '2020_01']],
            ignore_index=True
        )
    }
    for dataset_name, scdb_dataset in scdb_datasets.items():
        (scdb_dataset.pipe(
                      correct_record,
                      '1992-121',
                      threeJudgeFdc='no mentionof 3-judge ct',
                      caseOrigin='Texas South. U.S. Dist. Ct.',
                      caseSource='U.S. Ct. App., Fifth Cir.',
                      lcDisagreement='no mention of dissent',
                      lcDisposition='affirmed',
                      lcDispositionDirection='unspecifiable',
                      decisionDirection='unspecifiable',
                      caseDispositionUnusual='no unusual disposition',
                      declarationUncon='no unconstitutional',
                      precedentAlteration='precedent unaltered',
                      voteUnclear='vote clearly specified',
                      authorityDecision1='original juris. or supervis.',
                      issue='jud. admin.: review of order',
                      issueArea='Judicial Power',
                      lawType='Infrequent Litigate (Code)',
                      lawSupp='Infrequent litigate (Code)',
                      lawMinor='28 U.S.C. § 2201')
                .pipe(set_dtypes)
                .to_feather(processed_data_dir / f'{dataset_name}.feather')
        )


def correct_record(scdb_df, scdb_case_id, **corrections):
    scdb_df.loc[
        scdb_df.caseId == scdb_case_id,
        list(corrections.keys())
    ] = list(corrections.values())
    return scdb_df


def set_dtypes(scdb_df):
    return scdb_df.astype({
        'term': 'uint16',
        'majVotes': 'uint8',
        'minVotes': 'uint8'
    }).assign(
        **{
            date_column_name: lambda df: df[date_column_name].map(
                gregorian_epoch_time_to_datetime64, na_action='ignore'
            )
            for date_column_name in ['dateArgument', 'dateRearg', 'dateDecision']
        }
    )


def gregorian_epoch_time_to_datetime64(timestamp_seconds):
    return dt.datetime(1582, 10, 14) + dt.timedelta(seconds=timestamp_seconds)


if __name__ == '__main__':
    wrangle_scdb_data()
