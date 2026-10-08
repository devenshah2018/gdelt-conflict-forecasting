"""Download a laptop-sized slice of the GDELT 2.0 event stream.

GDELT 2.0 publishes one export file every 15 minutes (96 per day). Each file
is a zipped, tab-separated table with 61 columns. This module downloads a
uniform subset of those files per day, keeps only the columns the pipeline
needs, and appends the filtered rows to one gzipped CSV per month under
``data/raw/``.

A manifest file records which 15-minute files have already been processed,
so the download can be interrupted and resumed safely.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

from . import config, runstate

MANIFEST_NAME = "manifest.json"
COLUMN_NAMES = [name for _, name in config.RAW_COLUMNS]
COLUMN_INDICES = [idx for idx, _ in config.RAW_COLUMNS]


def _slot_timestamps(day: date, files_per_day: int) -> list[str]:
    """Evenly spaced 15-minute slot timestamps (YYYYMMDDHHMMSS) for a day."""
    step = max(1, 96 // files_per_day)
    slots = range(0, 96, step)
    out = []
    for slot in slots:
        minutes = slot * 15
        ts = datetime(day.year, day.month, day.day) + timedelta(minutes=minutes)
        out.append(ts.strftime("%Y%m%d%H%M%S"))
    return out[:files_per_day]


def _fetch_one(timestamp: str, session: requests.Session) -> tuple[str, list[list[str]] | None]:
    """Download and filter a single 15-minute export file.

    Returns (timestamp, rows) where rows is None when the file does not
    exist on the server (GDELT has occasional gaps).
    """
    url = f"{config.GDELT_BASE_URL}/{timestamp}.export.CSV.zip"
    try:
        resp = session.get(url, timeout=60)
    except requests.RequestException:
        return timestamp, None
    if resp.status_code != 200:
        return timestamp, None
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            raw = zf.read(zf.namelist()[0])
    except (zipfile.BadZipFile, IndexError):
        return timestamp, None

    rows = []
    for line in raw.decode("utf-8", errors="replace").splitlines():
        fields = line.split("\t")
        if len(fields) < 61:
            continue
        picked = [fields[i] for i in COLUMN_INDICES]
        # Require an event country and a sane date; everything else can be
        # empty and is handled downstream.
        country = picked[COLUMN_NAMES.index("ActionGeo_CountryCode")]
        sqldate = picked[COLUMN_NAMES.index("SQLDATE")]
        if not country or len(sqldate) != 8 or not sqldate.isdigit():
            continue
        rows.append(picked)
    return timestamp, rows


def _load_manifest(raw_dir: Path) -> set[str]:
    path = raw_dir / MANIFEST_NAME
    if path.exists():
        return set(json.loads(path.read_text()))
    return set()


def _save_manifest(raw_dir: Path, done: set[str]) -> None:
    (raw_dir / MANIFEST_NAME).write_text(json.dumps(sorted(done)))


def _append_rows(raw_dir: Path, month_key: str, rows: list[list[str]]) -> None:
    """Append rows to the month's gzipped CSV, writing a header on creation."""
    path = raw_dir / f"events_{month_key}.csv.gz"
    is_new = not path.exists()
    buf = io.StringIO()
    writer = csv.writer(buf)
    if is_new:
        writer.writerow(COLUMN_NAMES)
    writer.writerows(rows)
    with open(path, "ab") as fh:
        fh.write(gzip.compress(buf.getvalue().encode("utf-8")))


def download_range(
    start: str | None = None,
    end: str | None = None,
    files_per_day: int | None = None,
    raw_dir: Path | None = None,
    workers: int | None = None,
    log=print,
) -> dict:
    """Download the configured GDELT slice. Resumable; returns a summary."""
    start_day = date.fromisoformat(start or config.DEFAULT_START_DATE)
    end_day = date.fromisoformat(end or config.DEFAULT_END_DATE)
    files_per_day = files_per_day or config.FILES_PER_DAY
    raw_dir = Path(raw_dir or config.RAW_DIR)
    workers = workers or config.DOWNLOAD_WORKERS
    raw_dir.mkdir(parents=True, exist_ok=True)

    done = _load_manifest(raw_dir)
    todo = []
    day = start_day
    while day <= end_day:
        for ts in _slot_timestamps(day, files_per_day):
            if ts not in done:
                todo.append(ts)
        day += timedelta(days=1)

    log(f"range {start_day} to {end_day}, {files_per_day} of 96 files per day")
    log(f"{len(todo)} files to fetch ({len(done)} already downloaded)")
    fetched = missing = row_total = 0
    session = requests.Session()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_one, ts, session): ts for ts in todo}
        for i, fut in enumerate(as_completed(futures), 1):
            timestamp, rows = fut.result()
            if rows is None:
                missing += 1
            else:
                fetched += 1
                row_total += len(rows)
                _append_rows(raw_dir, timestamp[:6], rows)
            done.add(timestamp)
            if i % 200 == 0 or i == len(todo):
                _save_manifest(raw_dir, done)
                log(f"  {i}/{len(todo)} files ({row_total:,} events kept)")
    _save_manifest(raw_dir, done)

    summary = {
        "fetched": fetched,
        "missing": missing,
        "events_kept": row_total,
        "raw_dir": str(raw_dir),
    }
    log(
        f"fetched {fetched} files, {missing} unavailable on the server, "
        f"{row_total:,} events kept"
    )
    runstate.record_stage("download")
    return summary


def has_downloaded_data(raw_dir: Path | str | None = None) -> bool:
    """True when a previous download left a manifest to resume from."""
    raw_dir = Path(raw_dir or config.RAW_DIR)
    return (raw_dir / MANIFEST_NAME).exists()
