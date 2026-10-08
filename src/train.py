"""Train and evaluate the forecasting models with Spark MLlib.

For every lead time the same feature matrix is used to fit:

* a Gradient-Boosted Trees classifier (the proposed model)
* a logistic regression baseline (with feature standardization)
* a persistence baseline: "alert at t+h if week t is already a spike"

The split is chronological: the first ``TRAIN_FRACTION`` of the weeks train
the models, the rest are the test set, and a gap of ``h`` weeks between the
two ensures no training label reaches into the test window.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config, evaluate, runstate
from .features import FEATURE_COLS, load_features


def _prepare_lead(df: DataFrame, lead: int) -> DataFrame:
    """Rows usable for a given lead: complete features, label, spike flag."""
    label_col = f"label_{lead}"
    cond = F.col(label_col).isNotNull() & F.col("is_spike").isNotNull()
    for c in FEATURE_COLS:
        cond = cond & F.col(c).isNotNull()
    return df.filter(cond).withColumn("label", F.col(label_col).cast("double"))


def _split_weeks(df: DataFrame, lead: int) -> tuple[DataFrame, DataFrame, str]:
    """Chronological split with an h-week gap between train and test."""
    weeks = [r["week"] for r in df.select("week").distinct().orderBy("week").collect()]
    cutoff = weeks[int(len(weeks) * config.TRAIN_FRACTION) - 1]
    gap_start = cutoff - timedelta(weeks=lead)
    train = df.filter(F.col("week") <= F.lit(gap_start))
    test = df.filter(F.col("week") > F.lit(cutoff))
    return train, test, cutoff.isoformat()


def _collect_predictions(model, data: DataFrame) -> "pd.DataFrame":
    """Score a dataset and pull (country, week, label, prob, is_spike) locally."""
    import pandas as pd  # local import keeps Spark workers lean

    scored = model.transform(data).withColumn(
        "prob", vector_to_array("probability").getItem(1)
    )
    return scored.select("country", "week", "label", "prob", "is_spike").toPandas()


def _feature_importances(gbt_model) -> dict:
    imp = gbt_model.stages[-1].featureImportances.toArray()
    pairs = sorted(zip(FEATURE_COLS, imp), key=lambda kv: -kv[1])
    return {name: round(float(v), 4) for name, v in pairs if v > 0}


def run_training(
    spark: SparkSession,
    leads: list[int] | None = None,
    features_path: Path | str | None = None,
    log=print,
) -> dict:
    """Train all models for all lead times; write models, predictions, metrics."""
    leads = leads or config.LEAD_TIMES
    df = load_features(spark, features_path).cache()

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features_vec")
    gbt = GBTClassifier(
        featuresCol="features_vec",
        labelCol="label",
        seed=config.RANDOM_SEED,
        **config.GBT_PARAMS,
    )
    scaler = StandardScaler(
        inputCol="features_vec", outputCol="features_std", withMean=True, withStd=True
    )
    lr = LogisticRegression(
        featuresCol="features_std", labelCol="label", **config.LR_PARAMS
    )

    config.MODELS_DIR.mkdir(parents=True, exist_ok=True)
    config.PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)

    all_metrics: dict = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "config": {
            "spike_ratio": config.SPIKE_RATIO,
            "spike_min_count": config.SPIKE_MIN_COUNT,
            "baseline_weeks": config.BASELINE_WEEKS,
            "train_fraction": config.TRAIN_FRACTION,
            "target_far": config.TARGET_FAR,
            "feature_count": len(FEATURE_COLS),
            "gbt_params": config.GBT_PARAMS,
            "lr_params": config.LR_PARAMS,
        },
        "leads": {},
    }

    for lead in leads:
        log(f"=== lead {lead} week(s) ===")
        data = _prepare_lead(df, lead)
        train, test, cutoff = _split_weeks(data, lead)
        n_train, n_test = train.count(), test.count()
        prev_train = train.agg(F.avg("label")).first()[0] or 0.0
        prev_test = test.agg(F.avg("label")).first()[0] or 0.0
        log(
            f"train {n_train:,} rows ({prev_train:.1%} positive), "
            f"test {n_test:,} rows ({prev_test:.1%} positive), cutoff {cutoff}"
        )

        log(f"fitting GBT ({config.GBT_PARAMS['maxIter']} trees, depth "
            f"{config.GBT_PARAMS['maxDepth']}) on {len(FEATURE_COLS)} features")
        gbt_model = Pipeline(stages=[assembler, gbt]).fit(train)
        log("fitting logistic regression baseline on standardized features")
        lr_model = Pipeline(stages=[assembler, scaler, lr]).fit(train)
        log("scoring train and test sets")

        gbt_train = _collect_predictions(gbt_model, train)
        gbt_test = _collect_predictions(gbt_model, test)
        lr_train = _collect_predictions(lr_model, train)
        lr_test = _collect_predictions(lr_model, test)

        # Labels and probabilities always come from the same collected frame,
        # so row order between the two model collections never matters.
        gbt_metrics = evaluate.evaluate_probabilistic(
            gbt_train["label"].to_numpy(),
            gbt_train["prob"].to_numpy(),
            gbt_test["label"].to_numpy(),
            gbt_test["prob"].to_numpy(),
        )
        gbt_metrics["feature_importances"] = _feature_importances(gbt_model)
        lr_metrics = evaluate.evaluate_probabilistic(
            lr_train["label"].to_numpy(),
            lr_train["prob"].to_numpy(),
            lr_test["label"].to_numpy(),
            lr_test["prob"].to_numpy(),
        )
        persistence_metrics = evaluate.precision_recall_f1(
            gbt_test["label"].to_numpy().astype(int),
            gbt_test["is_spike"].to_numpy().astype(int),
        )

        all_metrics["leads"][str(lead)] = {
            "cutoff_week": cutoff,
            "n_train": n_train,
            "n_test": n_test,
            "prevalence_train": round(float(prev_train), 4),
            "prevalence_test": round(float(prev_test), 4),
            "models": {
                "gbt": gbt_metrics,
                "logreg": lr_metrics,
                "persistence": persistence_metrics,
            },
        }

        gbt_model.write().overwrite().save(str(config.MODELS_DIR / f"gbt_lead{lead}"))
        lr_model.write().overwrite().save(str(config.MODELS_DIR / f"lr_lead{lead}"))

        merged = gbt_test.rename(columns={"prob": "prob_gbt"}).merge(
            lr_test[["country", "week", "prob"]].rename(columns={"prob": "prob_lr"}),
            on=["country", "week"],
            how="left",
        )
        merged["week"] = merged["week"].astype(str)
        merged.to_csv(config.PREDICTIONS_DIR / f"lead_{lead}.csv", index=False)
        log(
            f"GBT PR-AUC {gbt_metrics['pr_auc']:.3f} vs LR {lr_metrics['pr_auc']:.3f}; "
            f"GBT recall@{config.TARGET_FAR:.0%}FAR {gbt_metrics['recall_at_far']:.3f}"
        )

    config.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(config.METRICS_PATH, "w") as fh:
        json.dump(all_metrics, fh, indent=2)
    log(f"metrics written to {config.METRICS_PATH}")
    runstate.record_stage("train")
    df.unpersist()
    return all_metrics
