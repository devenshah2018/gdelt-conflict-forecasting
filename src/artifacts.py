"""Read access to pipeline outputs, shared by the CLI and the web app.

Everything the UI shows comes through these functions, so there is exactly
one definition of where artifacts live and how they are parsed.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

import pandas as pd

from . import config
from .countries import country_name


def load_metrics() -> dict | None:
    """Parsed metrics.json, or None when training has not run yet.

    Reads return None rather than raising when a file is missing or is being
    rewritten by a pipeline run, so pages degrade to their empty state
    instead of erroring mid-refresh.
    """
    if not config.METRICS_PATH.exists():
        return None
    try:
        return json.loads(config.METRICS_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def load_risk_scores() -> pd.DataFrame | None:
    """Latest-week risk scores, or None when predict has not run yet."""
    if not config.RISK_SCORES_PATH.exists():
        return None
    try:
        return pd.read_csv(config.RISK_SCORES_PATH)
    except (pd.errors.ParserError, pd.errors.EmptyDataError, OSError):
        return None


def load_predictions(lead: int) -> pd.DataFrame | None:
    """Held-out test predictions for one lead time."""
    path = config.PREDICTIONS_DIR / f"lead_{lead}.csv"
    if not path.exists():
        return None
    try:
        return pd.read_csv(path, parse_dates=["week"])
    except (pd.errors.ParserError, pd.errors.EmptyDataError, OSError):
        return None


def _features_csv_path() -> Path | None:
    csv_dir = Path(str(config.FEATURES_PATH) + "_csv")
    if not csv_dir.exists():
        return None
    parts = sorted(csv_dir.glob("part-*.csv"))
    return parts[0] if parts else None


@lru_cache(maxsize=4)
def _load_features_csv(cache_key: float) -> pd.DataFrame:
    path = _features_csv_path()
    df = pd.read_csv(path, parse_dates=["week"])
    return df


def load_features_frame() -> pd.DataFrame | None:
    """The processed country-week table as pandas (cached by file mtime).

    The cache key is the file's modification time, so a pipeline rerun that
    replaces the file is picked up automatically on the next page load.
    """
    path = _features_csv_path()
    if path is None:
        return None
    try:
        return _load_features_csv(path.stat().st_mtime)
    except (pd.errors.ParserError, pd.errors.EmptyDataError, OSError):
        return None


def load_country_history(code: str) -> pd.DataFrame | None:
    """Weekly history for one country, oldest week first."""
    df = load_features_frame()
    if df is None:
        return None
    hist = df[df["country"] == code].sort_values("week").reset_index(drop=True)
    return hist if len(hist) else None


def list_countries() -> list[dict]:
    """Tracked countries as [{code, name}], sorted by name."""
    df = load_features_frame()
    if df is None:
        return []
    codes = sorted(df["country"].unique())
    out = [{"code": c, "name": country_name(c)} for c in codes]
    out.sort(key=lambda d: d["name"])
    return out
