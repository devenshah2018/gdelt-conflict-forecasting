"""Build country-week feature vectors from raw GDELT events with PySpark.

The pipeline reads the filtered event-level CSVs written by ``download.py``
and produces one row per (country, week) with:

* volume features: total events, counts by CAMEO quad class, counts for the
  escalation-ladder root codes 10 through 20
* intensity features: average Goldstein score, average tone, mention volume
* dynamics features: lags, trailing means and standard deviations, spike
  ratios and z-scores, all computed with window functions that only look
  backward in time (no leakage from the future)
* spike flags and forward-looking labels for each lead time (see labels.py)

The output is a Parquet dataset at ``data/processed/country_week_features``
plus a small CSV export used by the web app.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from . import config, runstate
from .labels import add_spike_flags_and_labels

RAW_SCHEMA = StructType(
    [
        StructField("GLOBALEVENTID", LongType()),
        StructField("SQLDATE", StringType()),
        StructField("EventRootCode", StringType()),
        StructField("QuadClass", IntegerType()),
        StructField("GoldsteinScale", DoubleType()),
        StructField("NumMentions", IntegerType()),
        StructField("NumSources", IntegerType()),
        StructField("NumArticles", IntegerType()),
        StructField("AvgTone", DoubleType()),
        StructField("ActionGeo_CountryCode", StringType()),
        StructField("DATEADDED", StringType()),
    ]
)

# CAMEO root codes 10 through 20 form an escalation ladder from verbal
# pressure (demands, threats, protests) to material conflict (assaults,
# fights, mass violence). Each gets its own weekly count feature.
ROOT_CODE_NAMES = {
    10: "demand",
    11: "disapprove",
    12: "reject",
    13: "threaten",
    14: "protest",
    15: "force_posture",
    16: "reduce_relations",
    17: "coerce",
    18: "assault",
    19: "fight",
    20: "mass_violence",
}

# Feature columns fed to the models, defined once so training, prediction,
# and the web app can never drift apart.
BASE_FEATURES = [
    "events_total",
    "quad1_verbal_coop",
    "quad2_material_coop",
    "quad3_verbal_conflict",
    "quad4_material_conflict",
    "goldstein_avg",
    "tone_avg",
    "mentions_sum",
    "articles_sum",
    "sources_sum",
    "quad3_share",
    "quad4_share",
    "coop_share",
] + [f"cnt_{name}" for name in ROOT_CODE_NAMES.values()]

DYNAMIC_FEATURES = (
    [f"quad4_lag{k}" for k in (1, 2, 3, 4)]
    + [f"quad3_lag{k}" for k in (1, 2)]
    + [
        "protest_lag1",
        "tone_lag1",
        "goldstein_lag1",
        "events_lag1",
        "quad4_mean8",
        "quad4_std8",
        "quad3_mean8",
        "quad4_spike_ratio",
        "quad4_zscore",
        "quad3_trend4",
        "quad4_trend4",
        "log_events_total",
    ]
)

FEATURE_COLS = BASE_FEATURES + DYNAMIC_FEATURES


def load_events(spark: SparkSession, raw_dir: Path | str | None = None) -> DataFrame:
    """Read the filtered raw event CSVs produced by the downloader.

    With no explicit ``raw_dir``, prefers the full download in ``data/raw``
    and falls back to the bundled sample in ``data/sample`` so the pipeline
    works out of the box.
    """
    if raw_dir is None:
        candidates = [config.RAW_DIR, config.SAMPLE_DIR]
    else:
        candidates = [Path(raw_dir)]
    for directory in candidates:
        files = sorted(str(p) for p in Path(directory).glob("events_*.csv.gz"))
        if files:
            return spark.read.csv(files, schema=RAW_SCHEMA, header=True)
    raise FileNotFoundError(
        f"No raw event files in {[str(c) for c in candidates]}. "
        f"Run `python cli.py download` first or point --raw-dir at data/sample."
    )


def clean_events(events: DataFrame) -> DataFrame:
    """Parse dates, drop stale references and duplicates."""
    ev = (
        events.dropDuplicates(["GLOBALEVENTID"])
        .withColumn("event_date", F.to_date("SQLDATE", "yyyyMMdd"))
        .withColumn("added_date", F.to_date(F.substring("DATEADDED", 1, 8), "yyyyMMdd"))
        .withColumn("root_code", F.col("EventRootCode").cast("int"))
    )
    # Keep events that are fresh news, not retrospective references.
    staleness = F.datediff("added_date", "event_date")
    ev = ev.filter(
        F.col("event_date").isNotNull()
        & F.col("added_date").isNotNull()
        & (staleness >= 0)
        & (staleness <= config.MAX_EVENT_STALENESS_DAYS)
    )
    # Week key: the Monday of the event's week.
    return ev.withColumn("week", F.date_trunc("week", "event_date").cast("date"))


def aggregate_country_week(events: DataFrame) -> DataFrame:
    """Aggregate cleaned events into one row per (country, week)."""
    quad = lambda q: F.sum(F.when(F.col("QuadClass") == q, 1).otherwise(0))
    aggs = [
        F.count("*").alias("events_total"),
        quad(1).alias("quad1_verbal_coop"),
        quad(2).alias("quad2_material_coop"),
        quad(3).alias("quad3_verbal_conflict"),
        quad(4).alias("quad4_material_conflict"),
        F.avg("GoldsteinScale").alias("goldstein_avg"),
        F.avg("AvgTone").alias("tone_avg"),
        F.sum("NumMentions").alias("mentions_sum"),
        F.sum("NumArticles").alias("articles_sum"),
        F.sum("NumSources").alias("sources_sum"),
    ]
    for code, name in ROOT_CODE_NAMES.items():
        aggs.append(
            F.sum(F.when(F.col("root_code") == code, 1).otherwise(0)).alias(f"cnt_{name}")
        )
    return events.groupBy(
        F.col("ActionGeo_CountryCode").alias("country"), "week"
    ).agg(*aggs)


def complete_grid(cw: DataFrame) -> DataFrame:
    """Fill in zero rows for country-weeks with no recorded events.

    Without this, quiet weeks would be missing instead of zero and every
    lag or rolling window would silently skip over them.
    """
    spark = cw.sparkSession
    bounds = cw.agg(F.min("week").alias("lo"), F.max("week").alias("hi")).first()
    weeks = spark.sql(
        f"SELECT explode(sequence(to_date('{bounds['lo']}'), to_date('{bounds['hi']}'), "
        f"interval 1 week)) AS week"
    )
    countries = cw.select("country").distinct()
    grid = countries.crossJoin(weeks)
    filled = grid.join(cw, ["country", "week"], "left")
    zero_cols = [c for c in cw.columns if c not in ("country", "week", "goldstein_avg", "tone_avg")]
    for c in zero_cols:
        filled = filled.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
    # Neutral values for averages in silent weeks.
    filled = filled.withColumn("goldstein_avg", F.coalesce("goldstein_avg", F.lit(0.0)))
    filled = filled.withColumn("tone_avg", F.coalesce("tone_avg", F.lit(0.0)))
    return filled


def filter_active_countries(cw: DataFrame) -> DataFrame:
    """Drop countries whose news volume is too thin for a weekly signal."""
    activity = cw.groupBy("country").agg(F.avg("events_total").alias("avg_weekly"))
    keep = activity.filter(F.col("avg_weekly") >= config.MIN_AVG_WEEKLY_EVENTS).select(
        "country"
    )
    return cw.join(keep, "country", "inner")


def add_dynamics(cw: DataFrame) -> DataFrame:
    """Add share, lag, trailing-window, and trend features (backward-looking only)."""
    total = F.col("events_total")
    safe_total = F.when(total > 0, total).otherwise(F.lit(None))
    cw = (
        cw.withColumn("quad3_share", F.coalesce(F.col("quad3_verbal_conflict") / safe_total, F.lit(0.0)))
        .withColumn("quad4_share", F.coalesce(F.col("quad4_material_conflict") / safe_total, F.lit(0.0)))
        .withColumn(
            "coop_share",
            F.coalesce(
                (F.col("quad1_verbal_coop") + F.col("quad2_material_coop")) / safe_total,
                F.lit(0.0),
            ),
        )
        .withColumn("log_events_total", F.log1p("events_total"))
    )

    w = Window.partitionBy("country").orderBy("week")
    trailing8 = w.rowsBetween(-config.BASELINE_WEEKS, -1)

    for k in (1, 2, 3, 4):
        cw = cw.withColumn(f"quad4_lag{k}", F.lag("quad4_material_conflict", k).over(w))
    for k in (1, 2):
        cw = cw.withColumn(f"quad3_lag{k}", F.lag("quad3_verbal_conflict", k).over(w))
    cw = (
        cw.withColumn("protest_lag1", F.lag("cnt_protest", 1).over(w))
        .withColumn("tone_lag1", F.lag("tone_avg", 1).over(w))
        .withColumn("goldstein_lag1", F.lag("goldstein_avg", 1).over(w))
        .withColumn("events_lag1", F.lag("events_total", 1).over(w))
        .withColumn("quad4_mean8", F.avg("quad4_material_conflict").over(trailing8))
        .withColumn("quad4_std8", F.stddev("quad4_material_conflict").over(trailing8))
        .withColumn("quad3_mean8", F.avg("quad3_verbal_conflict").over(trailing8))
        .withColumn("history_weeks", F.count("*").over(trailing8))
    )
    cw = (
        cw.withColumn(
            "quad4_spike_ratio",
            F.col("quad4_material_conflict") / (F.col("quad4_mean8") + F.lit(1.0)),
        )
        .withColumn(
            "quad4_zscore",
            (F.col("quad4_material_conflict") - F.col("quad4_mean8"))
            / (F.coalesce(F.col("quad4_std8"), F.lit(0.0)) + F.lit(1.0)),
        )
        .withColumn("quad4_trend4", F.col("quad4_material_conflict") - F.col("quad4_lag4"))
        .withColumn("quad3_trend4", F.col("quad3_verbal_conflict") - F.col("quad3_lag2"))
    )
    return cw


def build_features(
    spark: SparkSession,
    raw_dir: Path | str | None = None,
    out_path: Path | str | None = None,
    log=print,
) -> str:
    """Run the full feature pipeline and write Parquet + CSV outputs."""
    out_path = Path(out_path or config.FEATURES_PATH)

    raw = load_events(spark, raw_dir)
    log("reading raw events and dropping stale or duplicate records")
    events = clean_events(raw)
    log("aggregating events into country-week rows")
    cw = aggregate_country_week(events)
    cw = complete_grid(cw)
    log(f"filtering to countries averaging >= {config.MIN_AVG_WEEKLY_EVENTS} events per week")
    cw = filter_active_countries(cw)
    log(f"computing lags, trailing {config.BASELINE_WEEKS}-week stats, and trends")
    cw = add_dynamics(cw)
    log(f"labeling spikes and forward targets for leads {config.LEAD_TIMES}")
    cw = add_spike_flags_and_labels(cw)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    log("running the Spark job and writing Parquet (this is the slow step)")
    cw.repartition(1).write.mode("overwrite").parquet(str(out_path))

    # Re-read the written parquet so the CSV export and stats below do not
    # recompute the whole aggregation DAG from raw events.
    cw = spark.read.parquet(str(out_path))
    csv_dir = str(out_path) + "_csv"
    cw.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_dir)

    n_rows = cw.count()
    n_countries = cw.select("country").distinct().count()
    weeks = cw.agg(F.min("week"), F.max("week")).first()
    log(f"weeks covered: {weeks[0]} to {weeks[1]}")
    log(f"features written to {out_path}: {n_rows:,} country-weeks, {n_countries} countries")
    runstate.record_stage("features")
    return str(out_path)


def load_features(spark: SparkSession, path: Path | str | None = None) -> DataFrame:
    """Read the processed feature set."""
    path = Path(path or config.FEATURES_PATH)
    if not path.exists():
        raise FileNotFoundError(
            f"No processed features at {path}. Run `python cli.py features` first."
        )
    return spark.read.parquet(str(path))
