from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import threading
from queue import Queue
import time
import json
from datetime import datetime
import requests

# -----------------------------
# Configuration
# -----------------------------
URL = "https://www.jamstockex.com/trading/trade-quotes/?market=50"

PROFILE_DIR_TICKERS = "jse_profile_tickers"
PROFILE_DIR_PRICES = "jse_profile_prices"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

DEFAULT_DELAY_SECONDS = 1.0
DEFAULT_INGEST_URL = "http://localhost:3000/api/ingest"


# -----------------------------
# 1) Scrape all Ordinary Share tickers
# -----------------------------
def _scrape_tickers_sync(result_queue: Queue):
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR_TICKERS,
            headless=True,
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 720},
            args=["--disable-blink-features=AutomationControlled"],
        )

        try:
            page = context.new_page()
            page.goto(URL, wait_until="domcontentloaded")

            # Wait for a table (best effort)
            try:
                page.wait_for_selector("table", state="attached", timeout=30000)
                page.wait_for_timeout(2000)
            except Exception:
                pass

            soup = BeautifulSoup(page.content(), "html.parser")
            tables = soup.find_all("table")
            if not tables:
                result_queue.put([])
                return

            # Find the table that has a "Symbol" header
            target_table = None
            symbol_col_idx = None

            for table in tables:
                header_cells = table.find_all("th")
                headers = [th.get_text(strip=True) for th in header_cells]
                if "Symbol" in headers:
                    target_table = table
                    symbol_col_idx = headers.index("Symbol")
                    break

            if not target_table:
                result_queue.put([])
                return

            tickers = []
            for row in target_table.find_all("tr"):
                tds = row.find_all("td")
                if not tds:
                    continue

                # If the number of tds doesn't match headers perfectly, still try safely
                if symbol_col_idx is not None and symbol_col_idx < len(tds):
                    candidate = tds[symbol_col_idx].get_text(strip=True)
                    if candidate:
                        tickers.append(candidate)

            # Remove duplicates while preserving order
            seen = set()
            tickers = [t for t in tickers if not (t in seen or seen.add(t))]

            result_queue.put(tickers)

        finally:
            context.close()


def scrape_tickers():
    """Thread wrapper for sync Playwright so it runs safely in notebook-like environments."""
    q = Queue()
    t = threading.Thread(target=_scrape_tickers_sync, args=(q,), daemon=True)
    t.start()
    t.join()
    return q.get()


# -----------------------------
# 2) Scrape price history for each ticker
# -----------------------------
def _scrape_prices_sync(tickers, result_queue: Queue, delay_seconds=DEFAULT_DELAY_SECONDS):
    all_data = {}

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR_PRICES,
            headless=True,
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 720},
            args=["--disable-blink-features=AutomationControlled"],
        )

        try:
            page = context.new_page()

            for ticker in tickers:
                url = (
                    "https://www.jamstockex.com/trading/instruments/price-history/"
                    f"?instrument={ticker}-jmd"
                )

                ticker_data = {}
                try:
                    page.goto(url, wait_until="domcontentloaded")

                    # Wait for a table (best effort)
                    try:
                        page.wait_for_selector("table", state="attached", timeout=15000)
                        page.wait_for_timeout(1500)
                    except Exception:
                        pass

                    soup = BeautifulSoup(page.content(), "html.parser")
                    tables = soup.find_all("table")
                    if not tables:
                        print(f"  No tables found for {ticker}")
                        all_data[ticker] = {}
                        time.sleep(delay_seconds)
                        continue

                    # Prefer a table that contains "Date" in headers
                    target_table = None
                    for table in tables:
                        headers = [th.get_text(strip=True) for th in table.find_all("th")]
                        if any(h == "Date" for h in headers):
                            target_table = table
                            break
                    if target_table is None:
                        target_table = tables[0]

                    rows = target_table.find_all("tr")

                    for row in rows:
                        cells = row.find_all(["td", "th"])
                        cell_texts = [c.get_text(strip=True) for c in cells]

                        # Skip header rows
                        if any(t == "Date" for t in cell_texts):
                            continue

                        # Expected layout often includes an icon column:
                        # [0 icon], [1 date], [2 volume], [3 last], [4 close], [5 change], [6 bid], [7 ask]
                        if len(cell_texts) < 8:
                            continue

                        date_str = cell_texts[1]
                        if not date_str:
                            continue

                        ticker_data[date_str] = {
                            "Volume": cell_texts[2],
                            "Last Traded Price ($)": cell_texts[3],
                            "Closing Price ($)": cell_texts[4],
                            "Price Change ($)": cell_texts[5],
                            "Closing Bid ($)": cell_texts[6],
                            "Closing Ask ($)": cell_texts[7],
                        }

                    all_data[ticker] = ticker_data

                except Exception as e:
                    print(f"  Error scraping {ticker}: {e}")
                    all_data[ticker] = {}

                time.sleep(delay_seconds)

        finally:
            context.close()

    result_queue.put(all_data)


def scrape_prices(tickers_list, delay_seconds=DEFAULT_DELAY_SECONDS):
    """Thread wrapper for sync Playwright so it runs safely in notebook-like environments."""
    q = Queue()
    t = threading.Thread(
        target=_scrape_prices_sync,
        args=(tickers_list, q, delay_seconds),
        daemon=True,
    )
    t.start()
    t.join()
    return q.get()


# -----------------------------
# 3) Calculate metrics
# -----------------------------
def calculate_metrics(json_data):
    results = {}

    for ticker, history in json_data.items():
        # Convert dict to sorted list of items (tuples of date, data) by date ascending
        sorted_history = sorted(
            history.items(),
            key=lambda x: datetime.strptime(x[0], "%Y-%m-%d"),
        )

        # Prepare lists for vector-like access
        dates = []
        closes = []
        volumes = []

        # Helper to clean number strings
        def clean_num(s):
            if isinstance(s, (int, float)):
                return float(s)
            s = str(s).replace(",", "").strip()
            # Handle cases like "N/A" or empty strings if any
            if not s or s == "-":
                return 0.0
            return float(s)

        # Parse data into lists
        cleaned_history = []
        for date_str, data in sorted_history:
            try:
                close = clean_num(data.get("Closing Price ($)", 0))
                volume = clean_num(data.get("Volume", 0))

                dates.append(date_str)
                closes.append(close)
                volumes.append(volume)
                cleaned_history.append((date_str, data))
            except ValueError:
                # specific handling for bad data rows
                continue

        ticker_results = {}

        for i in range(len(dates)):
            date = dates[i]

            # --- 1 Day Change ---
            change_1d = None
            if i >= 1:
                prev_close = closes[i - 1]
                curr_close = closes[i]
                if prev_close != 0:
                    change_1d = (curr_close - prev_close) / prev_close

            # --- 30 Day Change ---
            change_30d = None
            if i >= 30:
                prev_30_close = closes[i - 30]
                curr_close = closes[i]
                if prev_30_close != 0:
                    change_30d = (curr_close - prev_30_close) / prev_30_close

            # --- Relative Volume (RVOL) ---
            rvol = None
            if i >= 30:
                vol_slice = volumes[i - 30 : i]
                avg_vol = sum(vol_slice) / len(vol_slice)

                current_vol = volumes[i]

                if avg_vol != 0:
                    rvol = current_vol / avg_vol
                else:
                    # If average volume is 0, we can't calculate a meaningful relative volume.
                    rvol = None

            ticker_results[date] = {
                "1_day_change_pct": round(change_1d * 100, 2) if change_1d is not None else None,
                "30_day_change_pct": round(change_30d * 100, 2) if change_30d is not None else None,
                "relative_volume": round(rvol, 2) if rvol is not None else None,
                "data": cleaned_history[i][1],  # Keep original data
            }

        results[ticker] = ticker_results

    return results



def metrics_selftest():
    """Optional self-test from the notebook (not run by default)."""
    mock_json = {"TEST": {}}

    # Generate 40 days of dummy data
    from datetime import timedelta
    current_date = datetime(2025, 1, 1)

    ticker_data = {}
    for i in range(40):
        date_str = current_date.strftime("%Y-%m-%d")
        price = 100 + i  # Linearly increasing price
        volume = 1000  # Constant volume

        if i == 35:
            volume = 5000  # Spike

        ticker_data[date_str] = {
            "Volume": str(volume),
            "Closing Price ($)": str(price),
        }
        current_date += timedelta(days=1)

    mock_json["TEST"] = ticker_data
    processed = calculate_metrics(mock_json)

    print("--- Test Results ---")
    keys = sorted(processed["TEST"].keys())
    for k in keys[-5:]:
        print(k, processed["TEST"][k])


# -----------------------------
# 4) Ingest analyzed data
# -----------------------------
def ingest_data(analyzed_data, api_url=DEFAULT_INGEST_URL):
    """
    Post the analyzed data JSON to the API.

    Args:
        analyzed_data (dict): The dictionary returned by calculate_metrics
        api_url (str): The endpoint to post to
    """
    print(f"Selling payload to {api_url}...")

    try:
        response = requests.post(
            api_url,
            json=analyzed_data,  # requests automatically adds Content-Type: application/json
            headers={"Content-Type": "application/json"},
        )

        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")

        return response
    except requests.exceptions.RequestException as e:
        print(f"Error posting data: {e}")
        return None


# -----------------------------
# Main flow (combined notebook cells)
# -----------------------------
def main():
    # Cell 1 behavior
    tickers = scrape_tickers()
    print(f"Found {len(tickers)} tickers:")
    print(tickers)

    # Cell 2 behavior
    all_data = scrape_prices(tickers, delay_seconds=DEFAULT_DELAY_SECONDS)
    print("\n--- JSON OUTPUT (preview) ---")
    print(json.dumps(all_data, indent=2)[:5000])  # preview first 5000 chars

    # Cell 3+4 behavior
    analyzed_data = calculate_metrics(all_data)
    print(json.dumps(analyzed_data, indent=4))

    # Cell 5+6 behavior
    ingest_data(analyzed_data)


if __name__ == "__main__":
    main()
