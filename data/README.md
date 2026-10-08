# Data documentation

## Source

All event data comes from the free, public
[GDELT 2.0 Event Database](https://www.gdeltproject.org/data.html), which
publishes one zipped, tab-separated export file every 15 minutes at
`http://data.gdeltproject.org/gdeltv2/YYYYMMDDHHMMSS.export.CSV.zip`.
`python cli.py download` fetches a uniform subsample of those files (default
8 of the 96 published per day), keeps the 11 columns below, and writes one
gzipped CSV per month.

## Directories

| Directory | Contents | Created by |
|---|---|---|
| `sample/` | Six months of filtered real events (2026-02 to 2026-07), bundled with the submission so the pipeline runs with no download | included |
| `raw/` | The full filtered slice (default 2024-01-01 to 2026-07-19, about 87 MB, 9.5M events) | `python cli.py download` |
| `processed/` | Country-week feature table (Parquet + CSV export) | `python cli.py features` |

`raw/manifest.json` records which 15-minute files have been processed so the
download can resume after an interruption.

## Columns in `events_YYYYMM.csv.gz`

These are 11 of the 61 columns of the official
[GDELT 2.0 event schema](http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf),
unchanged except for filtering:

| Column | Meaning |
|---|---|
| `GLOBALEVENTID` | Unique event id (used for deduplication) |
| `SQLDATE` | Date the event happened, `YYYYMMDD` |
| `EventRootCode` | 2-digit CAMEO root code (01 to 20); 10-20 form the escalation ladder from demands to mass violence |
| `QuadClass` | 1 verbal cooperation, 2 material cooperation, 3 verbal conflict, 4 material conflict |
| `GoldsteinScale` | Conflict-cooperation intensity score, -10 (most conflictual) to +10 |
| `NumMentions` | Mentions of the event across all monitored news |
| `NumSources` | Distinct sources reporting the event |
| `NumArticles` | Distinct articles reporting the event |
| `AvgTone` | Average tone of coverage (negative = more negative) |
| `ActionGeo_CountryCode` | FIPS 10-4 code of the country where the event happened (see `src/countries.py`) |
| `DATEADDED` | Timestamp the event entered the GDELT stream, `YYYYMMDDHHMMSS` |

Rows with an empty `ActionGeo_CountryCode` or a malformed `SQLDATE` are
dropped at download time. At feature-build time, events whose `SQLDATE` is
more than 7 days before `DATEADDED` are dropped as retrospective coverage
(anniversaries, historical references), and duplicate `GLOBALEVENTID`s are
removed.

## Sampling note

Downloading 8 of 96 daily files is a uniform 1/12 sample of the stream, so
weekly counts are proportional to full-firehose counts. Every feature and
the spike label are built from relative quantities (ratios to trailing
averages, shares, z-scores), which are invariant to uniform sampling up to
noise. Set `FILES_PER_DAY = 96` in `src/config.py` for the full stream.
