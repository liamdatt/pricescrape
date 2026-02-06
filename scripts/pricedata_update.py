#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pricedata import DEFAULT_DELAY_SECONDS, scrape_prices, scrape_tickers


INGEST_URL = os.getenv("INGEST_URL", "http://localhost:3000/api/ingest")
UPDATE_WINDOW_DAYS = int(os.getenv("UPDATE_WINDOW_DAYS", "7"))
SCRAPE_DELAY_SECONDS = float(os.getenv("SCRAPE_DELAY_SECONDS", str(DEFAULT_DELAY_SECONDS)))
INGEST_MAX_RETRIES = int(os.getenv("INGEST_MAX_RETRIES", "3"))
INGEST_RETRY_BASE_SECONDS = float(os.getenv("INGEST_RETRY_BASE_SECONDS", "2"))
LOCK_PATH = os.getenv("UPDATE_LOCK_PATH", "/tmp/jse_update.lock")


def log(message: str) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    print(f"[{now}] {message}")


def parse_date_key(date_str: str) -> Optional[datetime]:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None


def acquire_lock(path: str) -> Optional[int]:
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return None

    os.write(fd, f"{os.getpid()}\n".encode("utf-8"))
    return fd


def release_lock(fd: Optional[int], path: str) -> None:
    if fd is not None:
        os.close(fd)
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def build_update_window(
    scraped_data: Dict[str, Dict[str, Dict[str, str]]],
    window_days: int,
) -> Dict[str, Dict[str, Dict[str, str]]]:
    windowed: Dict[str, Dict[str, Dict[str, str]]] = {}

    for ticker, history in scraped_data.items():
        rows: list[Tuple[datetime, str, Dict[str, str]]] = []
        for date_str, row in history.items():
            parsed = parse_date_key(date_str)
            if parsed is None:
                continue
            rows.append((parsed, date_str, row))

        rows.sort(key=lambda item: item[0])
        if window_days > 0:
            rows = rows[-window_days:]

        ticker_payload: Dict[str, Dict[str, str]] = {}
        for _, date_str, row in rows:
            ticker_payload[date_str] = row

        if ticker_payload:
            windowed[ticker] = ticker_payload

    return windowed


def build_ingest_payload(
    windowed_data: Dict[str, Dict[str, Dict[str, str]]]
) -> Dict[str, Dict[str, Dict[str, object]]]:
    payload: Dict[str, Dict[str, Dict[str, object]]] = {}

    for ticker, history in windowed_data.items():
        payload[ticker] = {}
        for date_str, data_row in history.items():
            payload[ticker][date_str] = {
                "1_day_change_pct": None,
                "30_day_change_pct": None,
                "relative_volume": None,
                "data": data_row,
            }

    return payload


def post_with_retries(payload: dict) -> requests.Response:
    last_error: Optional[Exception] = None

    for attempt in range(1, INGEST_MAX_RETRIES + 1):
        try:
            response = requests.post(
                INGEST_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=120,
            )
            return response
        except requests.RequestException as error:
            last_error = error
            if attempt == INGEST_MAX_RETRIES:
                break

            wait_seconds = INGEST_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            log(
                f"Ingest attempt {attempt}/{INGEST_MAX_RETRIES} failed: {error}. "
                f"Retrying in {wait_seconds:.1f}s..."
            )
            time.sleep(wait_seconds)

    assert last_error is not None
    raise last_error


def main() -> int:
    lock_fd = acquire_lock(LOCK_PATH)
    if lock_fd is None:
        log(f"Another update job is already running (lock: {LOCK_PATH}). Exiting.")
        return 1

    try:
        log("Starting incremental JSE update scrape...")
        tickers = scrape_tickers()
        log(f"Discovered {len(tickers)} tickers.")

        if not tickers:
            log("No tickers found. Exiting.")
            return 1

        scraped = scrape_prices(tickers, delay_seconds=SCRAPE_DELAY_SECONDS)
        windowed = build_update_window(scraped, UPDATE_WINDOW_DAYS)
        payload = build_ingest_payload(windowed)

        total_rows = sum(len(days) for days in payload.values())
        log(
            f"Prepared payload with {len(payload)} tickers and {total_rows} rows "
            f"(window={UPDATE_WINDOW_DAYS} days)."
        )

        if total_rows == 0:
            log("No rows found to ingest. Exiting.")
            return 0

        response = post_with_retries(payload)
        log(f"Ingest HTTP status: {response.status_code}")

        body_text = response.text
        try:
            body = response.json()
            summary = json.dumps(body, indent=2)
        except ValueError:
            summary = body_text

        log(f"Ingest response body:\n{summary}")

        if response.status_code >= 400:
            return 1

        return 0
    finally:
        release_lock(lock_fd, LOCK_PATH)


if __name__ == "__main__":
    raise SystemExit(main())
