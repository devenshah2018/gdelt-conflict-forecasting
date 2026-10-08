"""Central configuration for the GDELT conflict forecasting pipeline.

Every tunable knob for the pipeline lives here so that the CLI and the
web app always agree on how data is downloaded, aggregated, labeled,
and modeled.
"""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
SAMPLE_DIR = DATA_DIR / "sample"
PROCESSED_DIR = DATA_DIR / "processed"
FEATURES_PATH = PROCESSED_DIR / "country_week_features"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
MODELS_DIR = ARTIFACTS_DIR / "models"
METRICS_PATH = ARTIFACTS_DIR / "metrics.json"
PREDICTIONS_DIR = ARTIFACTS_DIR / "predictions"
RISK_SCORES_PATH = ARTIFACTS_DIR / "risk_scores.csv"

# ---------------------------------------------------------------------------
# Download settings
# ---------------------------------------------------------------------------
GDELT_BASE_URL = "http://data.gdeltproject.org/gdeltv2"
DEFAULT_START_DATE = "2024-01-01"
DEFAULT_END_DATE = "2026-07-19"

# GDELT 2.0 publishes 96 export files per day (one every 15 minutes).
# Downloading a uniform subset of them keeps the project laptop-sized while
# preserving the relative volume signal week over week. Set to 96 for the
# full firehose.
FILES_PER_DAY = 8
DOWNLOAD_WORKERS = 8

# Columns kept from the 61-column GDELT 2.0 export schema, as
# (source index, name) pairs. Everything else is dropped at download time.
RAW_COLUMNS = [
    (0, "GLOBALEVENTID"),
    (1, "SQLDATE"),
    (28, "EventRootCode"),
    (29, "QuadClass"),
    (30, "GoldsteinScale"),
    (31, "NumMentions"),
    (32, "NumSources"),
    (33, "NumArticles"),
    (34, "AvgTone"),
    (53, "ActionGeo_CountryCode"),
    (59, "DATEADDED"),
]

# An event only counts toward a week if its reported date (SQLDATE) is within
# this many days of when it entered the stream (DATEADDED). This drops
# anniversary and retrospective coverage that would smear the weekly signal.
MAX_EVENT_STALENESS_DAYS = 7

# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------
# Drop countries that never generate enough news volume for a weekly signal.
# Threshold is on average sampled events per week across the whole period.
MIN_AVG_WEEKLY_EVENTS = 40

# Number of trailing weeks used as the baseline when deciding whether a week
# is a "spike" in material conflict.
BASELINE_WEEKS = 8

# A week is a spike when its material conflict event count is at least
# SPIKE_RATIO times the trailing baseline mean and also at least
# SPIKE_MIN_COUNT events (guards against tiny denominators).
SPIKE_RATIO = 2.0
SPIKE_MIN_COUNT = 8

# Minimum weeks of history a country-week needs before it gets a valid
# spike flag (otherwise the baseline is too thin to trust).
MIN_HISTORY_WEEKS = 4

# Forecast horizons, in weeks ahead.
LEAD_TIMES = [1, 2, 3, 4]

# ---------------------------------------------------------------------------
# Modeling
# ---------------------------------------------------------------------------
# Fraction of the timeline (by week) reserved for training; the remainder is
# the chronological test set. A gap of `lead` weeks is inserted between train
# and test so no training label overlaps the test window.
TRAIN_FRACTION = 0.75
RANDOM_SEED = 777

GBT_PARAMS = {
    "maxIter": 80,
    "maxDepth": 4,
    "stepSize": 0.1,
    "subsamplingRate": 0.8,
    "featureSubsetStrategy": "sqrt",
    "minInstancesPerNode": 10,
}

LR_PARAMS = {
    "maxIter": 100,
    "regParam": 0.01,
    "elasticNetParam": 0.0,
}

# False alarm rate used for the "recall at fixed false alarm rate" metric.
TARGET_FAR = 0.10

# Calibration curve resolution.
CALIBRATION_BINS = 10
