# Forecasting Material Conflict Spikes from GDELT 2.0

CS777 term project. The research question: **can we predict whether a
country-week will see a spike in material conflict 1 to 4 weeks out**, using
only open news-derived event data?

The pipeline aggregates the GDELT 2.0 global event stream into country-week
feature vectors with PySpark, labels weeks whose material conflict event count
jumps to at least 2x their trailing 8-week average, and trains a
Gradient-Boosted Trees classifier (Spark MLlib) to forecast those spikes 1 to
4 weeks ahead. It is evaluated on a strict chronological hold-out against a
logistic regression baseline and a persistence rule.

## Project layout

```
start.sh                One-command launcher: builds missing artifacts, serves the web app
cli.py                  Command-line entry point (download / features / train / predict / all)
src/
  config.py             Every tunable knob: dates, spike definition, model params
  download.py           Downloads and filters the GDELT 2.0 event slice
  features.py           PySpark: events -> country-week features (lags, rolling stats)
  labels.py             Spike definition and 1-4 week forward labels
  train.py              Spark MLlib GBT + logistic regression + persistence baseline
  evaluate.py           PR-AUC, precision/recall/F1, recall at fixed FAR, calibration
  predict.py            Scores the latest week into per-country risk probabilities
  artifacts.py          Shared read access to pipeline outputs (used by CLI and web)
  runstate.py           Records when each stage last ran (drives the daily refresh)
  countries.py          FIPS country code to name mapping
  spark_session.py      Shared local SparkSession factory
webapp/
  app.py                Flask app; calls the same src/ functions, no duplicated logic
  templates/            Dashboard, model evaluation, country pages
  static/               Stylesheet and dependency-free SVG charts
data/
  README.md             Data documentation: source, directories, column dictionary
  sample/               Small bundled sample of raw events (runs out of the box)
  raw/                  Full downloaded slice (created by `python cli.py download`)
  processed/            Country-week feature table (created by `features`)
artifacts/              metrics.json, test predictions, risk scores, saved models
```

## Requirements

* Python 3.10+ (developed on 3.13)
* Java 8, 11, or 17 on the PATH (required by Spark)
* `pip install -r requirements.txt` (PySpark 3.5, pandas, requests, Flask)

No cluster needed; everything runs on a laptop with `local[*]` Spark. For the
full 96-files-per-day firehose see
[Running the full dataset on a cluster](#running-the-full-dataset-on-a-cluster).

## Quick start

The fastest way to see everything working:

```bash
pip install -r requirements.txt
./start.sh
```

`start.sh` builds any missing pipeline artifacts (Spark feature build, model
training, risk scoring) and then serves the web app at
`http://127.0.0.1:5001`. On a fresh checkout this takes several minutes
because the models are trained from scratch.

The individual steps, if you prefer to run them by hand:

```bash
python cli.py features
python cli.py train
python cli.py predict
```

A small sample of real GDELT events ships in `data/sample/` so the pipeline
works immediately: when `data/raw/` is absent, the feature builder
automatically falls back to the bundled sample.

Note: the sample covers a short window, so its metrics are not meaningful.
For real results use the full download below.

## Full run

```bash
# 1. Download the event slice (about 90 MB compressed, resumable).
#    Defaults: 2024-01-01 through 2026-07-19, 8 of the 96 daily files.
python cli.py download

# 2. Aggregate to country-week features with Spark (a few minutes).
python cli.py features

# 3. Train GBT + baselines for lead times 1-4, evaluate, save metrics.
python cli.py train

# 4. Score the latest week into artifacts/risk_scores.csv.
python cli.py predict
```

Or all four in one step: `python cli.py all`.

## Running the full dataset on a cluster

The defaults take 8 of GDELT's 96 daily export files. Going to the full
firehose (`FILES_PER_DAY = 96`) multiplies two things by 12 — the download and
the Spark scan over raw events — and essentially nothing else. That is worth
being explicit about, because it decides how much cluster you actually need:

| Stage | Default (8/96) | Full (96/96) | Needs a cluster? |
|---|---|---|---|
| `download` | 7,448 files, 82 MB gz | ~89,400 files, ~1 GB gz | No — network bound, not compute bound |
| `features` | 82 MB in, 6.6 MB parquet out | ~1 GB in, ~15 MB parquet out | Yes — the only genuinely distributed step |
| `train` / `predict` | 15,222 country-weeks | ~20k (more countries clear the volume floor) | No — MLlib on a single executor |

The feature table is one row per country-week (118 countries x 131 weeks
today), so its size is set by the calendar, not by the sampling rate.
Everything downstream of `features` stays laptop-sized regardless. Size the
cluster for the raw-event scan and nothing else.

### 1. Rescale the count thresholds

Do this in either setup below. `MIN_AVG_WEEKLY_EVENTS` and `SPIKE_MIN_COUNT`
are counts of *sampled* events calibrated to 8-of-96, so they have to scale
with the sampling rate. `SPIKE_RATIO` is a ratio and is scale-invariant —
leave it alone.

```python
# src/config.py
FILES_PER_DAY = 96
MIN_AVG_WEEKLY_EVENTS = 480   # was 40, at 8/96 sampling
SPIKE_MIN_COUNT = 96          # was 8
DOWNLOAD_WORKERS = 32         # was 8
```

Skipping this does not error; it silently changes the experiment. The country
filter admits roughly 12x more marginal countries and the min-count guard on
the spike label stops binding, so the labels no longer mean what the Results
table above measured.

### Option A: one large VM (recommended first)

A 1 GB compressed slice does not require a distributed filesystem. On a single
32-vCPU / 128 GB cloud instance, `local[*]` handles it and every path in the
repo keeps working — including `start.sh` and the web app. One edit:

```python
# src/spark_session.py
.config("spark.driver.memory", "64g")        # was 4g
.config("spark.sql.shuffle.partitions", "200")  # was 16
```

Then:

```bash
python cli.py download --files-per-day 96
python cli.py all --skip-download
```

The download is the long pole (~89k HTTPS requests) but is resumable through
`data/raw/manifest.json`; re-run it after any interruption and it picks up
where it stopped.

### Option B: a real cluster (EMR, Dataproc, Databricks)

Executors do not share the driver's filesystem, so raw data and features have
to live in object storage. The pipeline has a handful of local-filesystem
assumptions that have to be lifted first:

| Location | Change |
|---|---|
| [spark_session.py:8](src/spark_session.py:8) | `.master("local[*]")` is set on the builder, which **overrides** `spark-submit --master`. Remove it or read it from an env var, or the job quietly runs local on the driver. |
| [features.py:120](src/features.py:120) | `load_events` globs the local filesystem. For a URI, hand the wildcard to Spark instead: `spark.read.csv(f"{raw_dir}/events_*.csv.gz", schema=RAW_SCHEMA, header=True)`. |
| [features.py:280](src/features.py:280), [286](src/features.py:286) | `repartition(1)` / `coalesce(1)` force a single writer. Harmless at 6 MB, a bottleneck when the input is 12x larger — raise to match the executor count. |
| [features.py:300](src/features.py:300) | `load_features` guards on `Path.exists()`; skip that check for URIs. |
| [predict.py:51](src/predict.py:51) | Same local `.exists()` check on the saved model directories. Spark ML writes them to `fs.defaultFS`, which is HDFS or the bucket on a cluster, not local disk. |

With those in place, package `src/` and submit:

```bash
zip -r src.zip src
spark-submit --master yarn --deploy-mode client --py-files src.zip \
  --conf spark.executor.instances=10 --conf spark.executor.memory=8g \
  --conf spark.sql.shuffle.partitions=200 \
  cli.py features --raw-dir s3://your-bucket/gdelt/raw
```

Use `--deploy-mode client` so `train` and `predict` write `artifacts/` to the
machine you submitted from — they use plain `open()` and pandas `to_csv`, not
Spark writers, so in cluster deploy mode those files land on whichever node
hosted the driver.

### Getting the full history without 89k downloads

GDELT mirrors the complete 2.0 event history in BigQuery
(`gdelt-bq.gdeltv2.events`). Exporting the eleven columns in `RAW_COLUMNS` —
same names, same order, gzipped CSV with a header — into a bucket and pointing
`--raw-dir` at it produces input the feature builder reads as-is, and skips the
downloader entirely. Verify the column set against `src/config.py:37` before
exporting; the reader applies `RAW_SCHEMA` positionally.

### Serving the results

The web app reads `artifacts/` from the local filesystem, so pull them back
from the cluster before serving:

```bash
aws s3 sync s3://your-bucket/gdelt/artifacts artifacts/
python webapp/app.py
```

Note that the Results table above was produced at 8-of-96 sampling. A
full-firehose run changes the country set and the event counts underneath the
spike definition, so its numbers will not match and should be reported
separately.

## Web app

The web app is a thin Flask layer over the exact same `src/` functions the
CLI uses (it imports them directly; there is no duplicated pipeline code).

```bash
./start.sh
# or, if the artifacts are already built:
python webapp/app.py
# open http://127.0.0.1:5001  (set PORT to change)
```

Pages:

* **Risk Dashboard**: every tracked country with its probability of a
  material conflict spike 1 to 4 weeks ahead, plus the refresh control and
  pipeline console.
* **Model & Evaluation**: PR-AUC and recall at a 10% false alarm rate by lead
  time, GBT vs baselines, calibration curves, and feature importances.
* **Countries**: weekly conflict history per country with spike weeks marked,
  and the model's test-period predictions.

### Refreshing from the browser

**Refresh data and retrain** re-runs the entire pipeline: it downloads any
GDELT files published since the last run, rebuilds the country-week
features, retrains and re-evaluates the models on a fresh chronological
split, and re-scores every country. Progress streams into a read-only
console on the page, one line per pipeline step, so the run can be followed
as it happens.

The rebuild is capped at once per day: if a full run already completed
today, the button reports that and does nothing (the underlying data only
changes as GDELT publishes new weeks). The individual stage buttons below
the table always run, for when only part of the chain needs rebuilding.

Page loads never trigger modeling. Rendering reads the artifacts the last
run wrote (`artifacts/risk_scores.csv`, `metrics.json`, `predictions/`),
which is why pages load instantly; `artifacts/run_state.json` records when
each stage last completed. On a checkout with no prior download, refresh
skips the download step and rebuilds from the bundled sample rather than
pulling years of history unprompted.

## Data

* Source: [GDELT 2.0 Event Database](https://www.gdeltproject.org/data.html)
  (free, updated every 15 minutes; `http://data.gdeltproject.org/gdeltv2/`).
* The downloader fetches a uniform subsample (default 8 of 96 files per day)
  of the 15-minute export files, keeps 11 of the 61 columns, and stores them
  as monthly gzipped CSVs. Uniform sampling preserves week-over-week relative
  volume, which is what the features and labels are built on.
* Events are assigned to countries by `ActionGeo_CountryCode` (where the
  event happened, FIPS codes) and to weeks by their reported date. Events
  whose reported date is more than 7 days before they entered the stream are
  dropped (retrospective and anniversary coverage, not news signal).

## Method

**Features (per country-week, 42 total).** Event counts by CAMEO quad class
(verbal/material cooperation/conflict), counts for each escalation-ladder
root code 10-20 (demand, threaten, protest, coerce, assault, fight, ...),
average Goldstein score, average tone, mention/article/source volume, share
ratios, 1-4 week lags, trailing 8-week means and standard deviations, spike
ratio, z-score, and 4-week trends. All rolling statistics look strictly
backward, so a feature row at week `t` uses nothing after week `t`.

**Label.** Week `t+h` is a spike for country `c` when its material conflict
(quad class 4) event count is at least `2.0x` its trailing 8-week mean and at
least 8 events. `label_h` at week `t` is that indicator for week `t+h`,
`h in {1,2,3,4}`.

**Split.** Chronological: the first 75% of weeks train, the last 25% test,
with an `h`-week gap between them so no training label leaks past the cutoff.

**Models.** Spark MLlib `GBTClassifier` (80 trees, depth 4) versus
`LogisticRegression` on standardized features, versus a persistence rule
("alert if the country is spiking right now"). Operating thresholds are
chosen on training predictions only.

**Metrics.** Spikes are rare, so accuracy is not reported. The headline
metrics are PR-AUC, precision/recall/F1 on the positive class, recall at a
10% false alarm rate, and calibration curves, for each lead time.

## Results

From the run of 2026-07-22 over data from 2024-01-01 to 2026-07-20. Held-out
test weeks begin after the cutoffs below; the test-set spike rate is about
9%, which is the floor a random model would score on PR-AUC.

| Lead | GBT PR-AUC | LR PR-AUC | GBT recall @ 10% FAR | GBT F1 | Persistence F1 |
|---|---|---|---|---|---|
| +1w | 0.172 | 0.192 | 0.249 | 0.242 | 0.179 |
| +2w | 0.144 | 0.133 | 0.203 | 0.200 | 0.134 |
| +3w | 0.144 | 0.143 | 0.183 | 0.175 | 0.088 |
| +4w | 0.131 | 0.133 | 0.162 | 0.188 | 0.100 |

Takeaways: roughly 2x lift over the random-model floor, a clear win over the
persistence rule at every lead with the gap widening as the horizon grows,
signal that decays with lead time as expected, and well-calibrated
probabilities. The GBT's most important features are the country's trailing
material conflict level and volatility (`quad4_mean8`, `quad4_std8`) and its
recent verbal conflict baseline (`quad3_mean8`), which supports the
escalation-ladder hypothesis from the proposal: hostile words carry
information about hostile deeds to come.

These exact numbers move as GDELT publishes new weeks and the pipeline is
refreshed, since the chronological split advances with the data. The current
values always live in `artifacts/metrics.json` and on the web app's Model &
Evaluation page.

## Reproducibility notes

* All randomness is seeded (`RANDOM_SEED` in `src/config.py`).
* `python cli.py download` is resumable and skips already-fetched files.
* Because GDELT continuously publishes new data, re-running with a different
  end date changes the dataset; the dates used for the reported results are
  pinned in `src/config.py`.
