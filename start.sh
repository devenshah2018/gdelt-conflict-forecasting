#!/usr/bin/env bash
# Starts the forecasting engine and the web app.
#
# Any missing pipeline artifacts are built first with the same CLI the
# README documents (Spark feature build, model training, risk scoring),
# then the web app is served on http://127.0.0.1:5001 (override with PORT).
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d data/processed/country_week_features ]; then
  echo "[start] No processed features found. Building with Spark (a few minutes)..."
  python3 cli.py features
fi

if [ ! -f artifacts/metrics.json ]; then
  echo "[start] No trained models found. Training and evaluating (several minutes)..."
  python3 cli.py train
fi

if [ ! -f artifacts/risk_scores.csv ]; then
  echo "[start] No risk scores found. Scoring the latest week..."
  python3 cli.py predict
fi

echo "[start] Launching web app at http://127.0.0.1:${PORT:-5001}"
exec python3 webapp/app.py
