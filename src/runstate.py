"""Records when each pipeline stage last ran.

The web app uses this to decide whether a refresh is due (the whole pipeline
is rebuilt at most once per day) and to show when the numbers on screen were
produced. The CLI writes the same file, so a terminal run and a web run stay
in sync.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from . import config

RUN_STATE_PATH = config.ARTIFACTS_DIR / "run_state.json"
EMPTY: dict = {"stages": {}, "last_full_run": None}


def load_run_state() -> dict:
    """Current run state; an empty skeleton when nothing has run yet."""
    if not RUN_STATE_PATH.exists():
        return dict(EMPTY)
    try:
        state = json.loads(RUN_STATE_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return dict(EMPTY)
    state.setdefault("stages", {})
    state.setdefault("last_full_run", None)
    return state


def _save(state: dict) -> None:
    config.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    RUN_STATE_PATH.write_text(json.dumps(state, indent=2))


def record_stage(name: str) -> None:
    """Timestamp the completion of a single pipeline stage."""
    state = load_run_state()
    state["stages"][name] = datetime.now().isoformat(timespec="seconds")
    _save(state)


def record_full_run() -> None:
    """Timestamp the completion of a full download-to-predict pipeline."""
    state = load_run_state()
    state["last_full_run"] = datetime.now().isoformat(timespec="seconds")
    _save(state)


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def last_full_run() -> datetime | None:
    return _parse(load_run_state().get("last_full_run"))


def needs_refresh() -> bool:
    """True when no full pipeline run has completed today."""
    last = last_full_run()
    return last is None or last.date() < date.today()
