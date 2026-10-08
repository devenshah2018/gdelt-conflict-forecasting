"""Score the most recent week to produce forward-looking risk scores.

Loads the saved GBT pipeline for each lead time and applies it to the latest
country-week feature rows, producing a probability of a material conflict
spike 1 to 4 weeks ahead for every tracked country.
"""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import config, runstate
from .countries import country_name
from .features import FEATURE_COLS, load_features


def score_latest(
    spark: SparkSession,
    leads: list[int] | None = None,
    features_path: Path | str | None = None,
    log=print,
):
    """Write artifacts/risk_scores.csv and return it as a pandas DataFrame."""
    import pandas as pd

    leads = leads or config.LEAD_TIMES
    df = load_features(spark, features_path)
    latest = df.agg(F.max("week")).first()[0]

    cond = F.col("week") == F.lit(latest)
    for c in FEATURE_COLS:
        cond = cond & F.col(c).isNotNull()
    current = df.filter(cond).cache()

    base = current.select(
        "country",
        "week",
        "quad4_material_conflict",
        "quad4_mean8",
        "quad4_spike_ratio",
        "is_spike",
    ).toPandas()
    base = base.rename(columns={"week": "as_of_week"})

    available = [
        lead for lead in leads if (config.MODELS_DIR / f"gbt_lead{lead}").exists()
    ]
    if not available:
        raise FileNotFoundError(
            f"No trained models in {config.MODELS_DIR}. Run `python cli.py train` first."
        )
    if len(available) < len(leads):
        skipped = sorted(set(leads) - set(available))
        log(f"note: no model for lead(s) {skipped}; scoring leads {available} only")
    leads = available

    log(f"scoring week {latest} for {len(base)} countries")
    for lead in leads:
        model_path = config.MODELS_DIR / f"gbt_lead{lead}"
        log(f"applying GBT model for lead +{lead} week(s)")
        model = PipelineModel.load(str(model_path))
        scored = (
            model.transform(current)
            .withColumn("prob", vector_to_array("probability").getItem(1))
            .select("country", "prob")
            .toPandas()
            .rename(columns={"prob": f"prob_lead{lead}"})
        )
        base = base.merge(scored, on="country", how="left")

    base["country_name"] = base["country"].map(country_name)
    base["as_of_week"] = base["as_of_week"].astype(str)
    for lead in leads:
        base[f"target_week_lead{lead}"] = str(latest + timedelta(weeks=lead))

    lead_cols = [f"prob_lead{lead}" for lead in leads]
    base["max_risk"] = base[lead_cols].max(axis=1)
    base = base.sort_values("max_risk", ascending=False).reset_index(drop=True)

    config.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    base.to_csv(config.RISK_SCORES_PATH, index=False)
    current.unpersist()
    top = base.iloc[0]
    log(f"highest risk: {top['country_name']} at {top['max_risk']:.2f}")
    log(f"risk scores for {len(base)} countries as of {latest} -> {config.RISK_SCORES_PATH}")
    runstate.record_stage("predict")
    return base
