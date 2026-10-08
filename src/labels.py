"""Spike definition and forward-looking labels.

A country-week is a *spike* when its material conflict event count is at
least ``SPIKE_RATIO`` times the trailing ``BASELINE_WEEKS``-week mean and at
least ``SPIKE_MIN_COUNT`` events in absolute terms. The absolute floor stops
tiny denominators from flagging noise (2 events after a baseline of 0.5 is
not a conflict spike).

For each lead time ``h`` the label ``label_h`` on the row for week ``t`` is
the spike flag of week ``t + h``. Features at week ``t`` only use data
through week ``t``, so predicting ``label_h`` is a true forecast.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import config


def add_spike_flags_and_labels(cw: DataFrame) -> DataFrame:
    """Add ``is_spike`` plus one ``label_h`` column per configured lead time.

    Expects the columns produced by ``features.add_dynamics`` (in particular
    ``quad4_mean8`` and ``history_weeks``).
    """
    threshold = F.greatest(
        F.lit(float(config.SPIKE_MIN_COUNT)),
        F.lit(config.SPIKE_RATIO) * F.col("quad4_mean8"),
    )
    has_history = F.col("history_weeks") >= config.MIN_HISTORY_WEEKS
    cw = cw.withColumn(
        "is_spike",
        F.when(~has_history, F.lit(None).cast("int")).otherwise(
            (F.col("quad4_material_conflict") >= threshold).cast("int")
        ),
    )

    w = Window.partitionBy("country").orderBy("week")
    for h in config.LEAD_TIMES:
        cw = cw.withColumn(f"label_{h}", F.lead("is_spike", h).over(w))
    return cw
