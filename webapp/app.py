"""Flask web app for the GDELT conflict forecasting project.

This app contains no modeling logic of its own. Every number it shows comes
from the same ``src`` package the CLI uses, and the pipeline it runs calls
the exact functions behind ``python cli.py download|features|train|predict``.

Refresh model: the pipeline is rebuilt at most once per day. Clicking
Refresh when the last full run was on an earlier day re-runs the whole
chain (download new events, rebuild features, retrain and re-evaluate the
models, re-score every country) while streaming progress to the console in
the browser. Page loads never trigger modeling; they read the artifacts the
last run produced.

Run with:
    python webapp/app.py
"""
from __future__ import annotations

import itertools
import json
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path

from flask import Flask, abort, jsonify, render_template, request

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src import artifacts, config, runstate  # noqa: E402
from src.countries import country_name  # noqa: E402

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True

# Stages of a full refresh, in order.
STAGES = ["download", "features", "train", "predict"]
MAX_LINES = 2000
CONSOLE_PATH = config.ARTIFACTS_DIR / "last_run_console.json"

# ---------------------------------------------------------------------------
# Background job state and console log
# ---------------------------------------------------------------------------
_job_lock = threading.Lock()
_seq = itertools.count(1)
_job: dict = {
    "running": False,
    "stage": None,
    "kind": None,
    "error": None,
    "started_at": None,
    "finished_at": None,
    "stage_status": {},
    "lines": [],
}


def _emit(text: str, stage: str | None = None, level: str = "info") -> None:
    """Append one line to the console log."""
    with _job_lock:
        _job["lines"].append(
            {
                "seq": next(_seq),
                "time": datetime.now().strftime("%H:%M:%S"),
                "stage": stage or _job.get("stage") or "",
                "text": str(text),
                "level": level,
            }
        )
        if len(_job["lines"]) > MAX_LINES:
            del _job["lines"][: len(_job["lines"]) - MAX_LINES]


def _logger_for(stage: str):
    """A ``log=`` callback that tags every line with its pipeline stage."""
    return lambda message: _emit(message, stage=stage)


def _set_stage(stage: str, status: str) -> None:
    with _job_lock:
        _job["stage_status"][stage] = status
        if status == "running":
            _job["stage"] = stage


def _save_console() -> None:
    """Persist the finished run so the console survives a server restart."""
    try:
        config.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
        with _job_lock:
            payload = {
                "lines": _job["lines"][-400:],
                "stage_status": _job["stage_status"],
                "error": _job["error"],
                "finished_at": _job["finished_at"],
            }
        CONSOLE_PATH.write_text(json.dumps(payload))
    except OSError:
        pass


def _load_console() -> None:
    """Restore the previous run's console on startup."""
    if not CONSOLE_PATH.exists():
        return
    try:
        payload = json.loads(CONSOLE_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return
    lines = payload.get("lines", [])
    for line in lines:
        line["seq"] = next(_seq)
    _job["lines"] = lines
    _job["stage_status"] = payload.get("stage_status", {})
    _job["finished_at"] = payload.get("finished_at")


_load_console()


# ---------------------------------------------------------------------------
# Pipeline execution
# ---------------------------------------------------------------------------
def _run_full_pipeline() -> None:
    """Download new events, rebuild features, retrain, and re-score."""
    from src.download import download_range, has_downloaded_data
    from src.features import build_features
    from src.predict import score_latest
    from src.spark_session import get_spark
    from src.train import run_training

    started = time.time()
    try:
        _emit("Starting full pipeline refresh", level="banner")

        # 1. Download. Skipped when no prior download exists, so a fresh
        #    checkout refreshes against the bundled sample in seconds
        #    instead of pulling years of history unprompted.
        _set_stage("download", "running")
        if has_downloaded_data():
            _emit("fetching any GDELT files published since the last run",
                  stage="download")
            try:
                download_range(
                    end=date.today().isoformat(), log=_logger_for("download")
                )
                _set_stage("download", "done")
            except Exception as exc:
                _set_stage("download", "failed")
                _emit(
                    f"download failed ({type(exc).__name__}: {exc}); "
                    f"continuing with the data already on disk",
                    stage="download",
                    level="warn",
                )
        else:
            _set_stage("download", "skipped")
            _emit(
                "no previous download found; using the bundled sample "
                "(run `python cli.py download` for the full dataset)",
                stage="download",
                level="warn",
            )

        spark = get_spark()

        _set_stage("features", "running")
        _emit("Building country-week features", stage="features", level="banner")
        build_features(spark, log=_logger_for("features"))
        _set_stage("features", "done")

        _set_stage("train", "running")
        _emit(
            "Training and evaluating on a chronological split",
            stage="train",
            level="banner",
        )
        run_training(spark, log=_logger_for("train"))
        _set_stage("train", "done")

        _set_stage("predict", "running")
        _emit("Scoring the latest week", stage="predict", level="banner")
        score_latest(spark, log=_logger_for("predict"))
        _set_stage("predict", "done")

        runstate.record_full_run()
        _job["error"] = None
        _emit(
            f"Pipeline refresh complete in {time.time() - started:.0f}s",
            level="success",
        )
    except Exception as exc:
        if _job.get("stage"):
            _set_stage(_job["stage"], "failed")
        _job["error"] = f"{type(exc).__name__}: {exc}"
        _emit(f"Pipeline failed: {_job['error']}", level="error")
    finally:
        _job["running"] = False
        _job["stage"] = None
        _job["finished_at"] = datetime.now().isoformat(timespec="seconds")
        _save_console()


def _run_single_stage(stage: str) -> None:
    """Run one pipeline stage on its own (the per-stage buttons)."""
    started = time.time()
    try:
        from src.spark_session import get_spark

        _set_stage(stage, "running")
        _emit(f"Running stage: {stage}", stage=stage, level="banner")
        spark = get_spark()
        if stage == "features":
            from src.features import build_features

            build_features(spark, log=_logger_for(stage))
        elif stage == "train":
            from src.train import run_training

            run_training(spark, log=_logger_for(stage))
        elif stage == "predict":
            from src.predict import score_latest

            score_latest(spark, log=_logger_for(stage))
        _set_stage(stage, "done")
        _job["error"] = None
        _emit(f"Stage {stage} finished in {time.time() - started:.0f}s", level="success")
    except Exception as exc:
        _set_stage(stage, "failed")
        _job["error"] = f"{type(exc).__name__}: {exc}"
        _emit(f"Stage {stage} failed: {_job['error']}", level="error")
    finally:
        _job["running"] = False
        _job["stage"] = None
        _job["finished_at"] = datetime.now().isoformat(timespec="seconds")
        _save_console()


def _claim_job(kind: str, initial_status: dict) -> bool:
    """Atomically take the single job slot. False when one is already running."""
    with _job_lock:
        if _job["running"]:
            return False
        _job.update(
            {
                "running": True,
                "kind": kind,
                "error": None,
                "started_at": datetime.now().isoformat(timespec="seconds"),
                "finished_at": None,
                "stage_status": initial_status,
                "lines": [],
            }
        )
    return True


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
@app.post("/api/refresh")
def api_refresh():
    """Re-run the whole pipeline when the last full run was not today."""
    force = request.args.get("force") == "1"
    if not force and not runstate.needs_refresh():
        last = runstate.last_full_run()
        return jsonify(
            {
                "ok": True,
                "skipped": True,
                "reason": f"already refreshed today at {last.strftime('%H:%M')}",
            }
        )
    if not _claim_job("refresh", {s: "pending" for s in STAGES}):
        return jsonify({"ok": False, "reason": "a run is already in progress"}), 409
    threading.Thread(target=_run_full_pipeline, daemon=True).start()
    return jsonify({"ok": True, "skipped": False})


@app.post("/api/run/<stage>")
def api_run(stage: str):
    if stage not in ("features", "train", "predict"):
        abort(404)
    if not _claim_job(f"stage:{stage}", {stage: "pending"}):
        return jsonify({"ok": False, "reason": "a run is already in progress"}), 409
    threading.Thread(target=_run_single_stage, args=(stage,), daemon=True).start()
    return jsonify({"ok": True})


@app.get("/api/status")
def api_status():
    """Job state plus any console lines newer than the client's cursor."""
    try:
        since = int(request.args.get("since", 0))
    except ValueError:
        since = 0
    with _job_lock:
        new_lines = [ln for ln in _job["lines"] if ln["seq"] > since]
        cursor = _job["lines"][-1]["seq"] if _job["lines"] else since
        running, stage = _job["running"], _job["stage"]
        stage_status = dict(_job["stage_status"])
        error, started_at = _job["error"], _job["started_at"]
        finished_at = _job["finished_at"]

    elapsed = None
    if started_at:
        start = datetime.fromisoformat(started_at)
        end = datetime.fromisoformat(finished_at) if finished_at and not running else datetime.now()
        elapsed = int((end - start).total_seconds())

    last_full = runstate.last_full_run()
    return jsonify(
        {
            "running": running,
            "stage": stage,
            "stages": STAGES,
            "stage_status": stage_status,
            "error": error,
            "elapsed": elapsed,
            "lines": new_lines,
            "cursor": cursor,
            "finished_at": finished_at,
            "needs_refresh": runstate.needs_refresh(),
            "last_full_run": last_full.isoformat(sep=" ", timespec="minutes") if last_full else None,
        }
    )


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------
def _refresh_context() -> dict:
    last = runstate.last_full_run()
    return {
        "last_full_run": last.strftime("%Y-%m-%d %H:%M") if last else None,
        "needs_refresh": runstate.needs_refresh(),
        "stages": STAGES,
    }


@app.get("/")
def dashboard():
    scores = artifacts.load_risk_scores()
    metrics = artifacts.load_metrics()
    rows = scores.to_dict("records") if scores is not None else None
    as_of = rows[0]["as_of_week"] if rows else None
    # Only show lead times that were actually scored (a partial
    # `train --leads ...` run produces a subset).
    leads = [
        lead for lead in config.LEAD_TIMES if rows and f"prob_lead{lead}" in rows[0]
    ] or config.LEAD_TIMES
    return render_template(
        "dashboard.html",
        rows=rows,
        as_of=as_of,
        leads=leads,
        metrics=metrics,
        active="dashboard",
        **_refresh_context(),
    )


@app.get("/model")
def model_page():
    metrics = artifacts.load_metrics()
    # Show whatever lead times were actually trained.
    leads = sorted(int(k) for k in metrics["leads"]) if metrics else config.LEAD_TIMES
    return render_template(
        "model.html",
        metrics=metrics,
        leads=leads,
        target_far=config.TARGET_FAR,
        active="model",
    )


@app.get("/country/<code>")
def country_page(code: str):
    code = code.upper()
    hist = artifacts.load_country_history(code)
    if hist is None:
        abort(404)

    weeks = hist["week"].dt.strftime("%Y-%m-%d").tolist()
    series = {
        "weeks": weeks,
        "quad4": hist["quad4_material_conflict"].fillna(0).astype(int).tolist(),
        "quad3": hist["quad3_verbal_conflict"].fillna(0).astype(int).tolist(),
        "is_spike": hist["is_spike"].fillna(-1).astype(int).tolist(),
    }

    # Held-out test predictions for this country, per lead time.
    preds = {}
    for lead in config.LEAD_TIMES:
        p = artifacts.load_predictions(lead)
        if p is None:
            continue
        mine = p[p["country"] == code].sort_values("week")
        if len(mine):
            preds[str(lead)] = {
                "weeks": mine["week"].dt.strftime("%Y-%m-%d").tolist(),
                "prob": mine["prob_gbt"].round(4).tolist(),
                "label": mine["label"].astype(int).tolist(),
            }

    risk = None
    scores = artifacts.load_risk_scores()
    if scores is not None:
        match = scores[scores["country"] == code]
        if len(match):
            risk = match.iloc[0].to_dict()

    risk_leads = [
        lead for lead in config.LEAD_TIMES if risk and f"prob_lead{lead}" in risk
    ]
    pred_leads = [lead for lead in config.LEAD_TIMES if str(lead) in preds]
    return render_template(
        "country.html",
        code=code,
        name=country_name(code),
        series=series,
        preds=preds,
        risk=risk,
        risk_leads=risk_leads,
        pred_leads=pred_leads or config.LEAD_TIMES,
        countries=artifacts.list_countries(),
        active="country",
    )


@app.get("/countries")
def countries_index():
    return render_template(
        "countries.html", countries=artifacts.list_countries(), active="country"
    )


if __name__ == "__main__":
    import os

    # 5001 by default: macOS AirPlay occupies 5000.
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", 5001)), debug=False)
