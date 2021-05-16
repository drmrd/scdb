# The Supreme Court Database, now in Python-friendly formats!

This repository contains
[Feathers](https://arrow.apache.org/docs/python/feather.html) and
[Parquet](https://parquet.apache.org/) files derived from the most recent
versions of the legacy and modern
[Supreme Court Database](http://supremecourtdatabase.org/) datasets.
[As discussed](http://supremecourtdatabase.org/data.php) on the SCDB website,
the SCDB is released annually in a variety of formats that differ from one
another along several axes (time period, unit of analysis, database record
granularity, and file format).
This repository contains a minimally-altered version of each of these datasets.

## Comparison to Official Datasets
I've made an active effort to ensure that, apart from datasets in the
[`data/preprocessed`](data/preprocessed) directory, the feather and parquet
files in this repository are faithful reproductions of those found in the
official releases.
They should differ from expectations only in that
1. Human-readable strings are used instead of numeric codes for variable
   values. These strings match the ones found in the SPSS release.
2. In string-valued and categorical columns, `np.nan` values are replaced by
   the description `'MISSING_VALUE'`.
3. Variable data types are converted to accurate and more-or-less optimal (in
   terms of storage space) data types. This includes using the experimental
   `pd.StringDtype` from pandas.

## Available Files
- **[`data/raw`](data/raw)** contains the officially-released SPSS files from
  which I've derived datasets.
- **[`data/feather`](data/feather)** contains all of the generated feathers
- **[`data/parquet`](data/parquet)** contains—yep you guessed it—the parquet files
- **[`data/preprocessed`](data/preprocessed)** contains a more refined version
  of the case-centric, citation-level dataset. This is a combination of the
  legacy and modern datasets that also includes some mild error correction and
  imputation work. If you're curious for more details, all changes are
  documented in the repository's [`dvc.yaml`](dvc.yaml) file, the
  `data_pipeline` package and, with more prose, on my blog beginning with
  [this post](https://danielmoore.xyz/2020/12/06/Accessing-SCOTUS-Data-A-First-Look-at-the-SCDB.html).
  If you're interested in getting involved, contributions are welcomed as are
  feature requests and issues!

# Disclaimer
I'm not affiliated with the Supreme Court Database, and this project is not
officially endorsed by members of the Supreme Court Database.