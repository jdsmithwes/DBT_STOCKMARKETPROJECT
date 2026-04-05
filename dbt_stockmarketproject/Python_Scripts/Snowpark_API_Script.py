"""
Snowpark_API_Script.py
======================
Self-contained Snowpark Python stored procedure that fetches daily stock
price data from the AlphaVantage API and appends it to a Snowflake table.

This file is designed to be uploaded to a Snowflake internal stage and
registered as a stored procedure via register_sproc.py (see that file for
the one-time registration step).

Handler
-------
  run_as_sproc(session, start_date, end_date)

Date-range behaviour
--------------------
  - Both `start_date` and `end_date` are optional VARCHAR arguments in
    YYYY-MM-DD format.
  - If NEITHER is supplied the script defaults to today as both start and
    end. If today is not a US stock market trading day (weekend or NYSE
    holiday) the procedure returns immediately — no API call is made.
  - If only `end_date` is omitted it defaults to today.
  - If only `start_date` is omitted it defaults to the day after the
    highest date already present in the target Snowflake table (or
    2020-01-01 when the table is empty).

Trading-day detection
---------------------
  Uses `pandas_market_calendars` (NYSE calendar, handles all holidays).
  Falls back to a simple Mon-Fri weekday check if the package is not
  available in the Snowpark runtime.

Credentials / configuration
----------------------------
  The following are read from Snowflake environment variables, which you
  set when creating the stored procedure (see register_sproc.py):

  Required:
    ALPHAVANTAGE_API_KEY   - AlphaVantage REST API key
    TARGET_DATABASE        - Snowflake destination database
    TARGET_SCHEMA          - Snowflake destination schema
    TARGET_TABLE           - Snowflake destination table name

  Optional:
    TICKERS                - Comma-separated ticker list, e.g. "AAPL,MSFT"
                             If omitted, tickers are read from the target
                             table itself (distinct values of the ticker
                             column). Falls back to ["AAPL"] on empty table.

Design notes
------------
  - NO external file imports. All logic is inlined so that only this single
    file needs to be staged in Snowflake.
  - aiohttp/asyncio replaced with `requests` + `concurrent.futures` because
    Snowpark's Python sandbox does not support a running asyncio event loop.
    `requests` is always available in the Snowpark runtime.
  - `tenacity` retry logic is inlined as a simple loop so no extra package
    is required.
"""
from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Optional, TYPE_CHECKING

import pandas as pd
import requests

if TYPE_CHECKING:
    from snowflake.snowpark import Session  # pragma: no cover

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
FALLBACK_START_DATE: date = date(2020, 1, 1)
FALLBACK_TICKER: str = "AAPL"
ALPHAVANTAGE_BASE_URL: str = "https://www.alphavantage.co/query"
MAX_WORKERS: int = 5          # parallel API calls
MAX_RETRIES: int = 3          # per-ticker retry attempts
RETRY_WAIT_SECONDS: int = 2   # wait between retries

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ---------------------------------------------------------------------------
# TRADING-DAY DETECTION
# ---------------------------------------------------------------------------
_NYSE_CALENDAR = None


def _load_market_calendar():
    """Load NYSE calendar from pandas_market_calendars (lazy, cached).

    Returns the calendar object or None when the package is unavailable.
    """
    global _NYSE_CALENDAR
    if _NYSE_CALENDAR is not None:
        return _NYSE_CALENDAR
    try:
        import pandas_market_calendars as mcal  # type: ignore
        _NYSE_CALENDAR = mcal.get_calendar("NYSE")
        logging.info("Using pandas_market_calendars for trading-day detection")
    except ImportError:
        logging.warning(
            "pandas_market_calendars not available — "
            "falling back to Mon-Fri weekday check (holidays excluded)"
        )
        _NYSE_CALENDAR = None
    return _NYSE_CALENDAR


def is_trading_day(check_date: date) -> bool:
    """Return True if *check_date* is a US (NYSE) trading day."""
    cal = _load_market_calendar()
    if cal is not None:
        schedule = cal.schedule(
            start_date=check_date.isoformat(),
            end_date=check_date.isoformat(),
        )
        return not schedule.empty
    return check_date.weekday() < 5  # Mon=0 … Fri=4


# ---------------------------------------------------------------------------
# DATE ARGUMENT PARSING
# ---------------------------------------------------------------------------
def _parse_date_arg(value: Optional[str], name: str) -> Optional[date]:
    """Parse a YYYY-MM-DD string to a date object.

    Returns None if *value* is None, and raises ValueError on bad format.
    """
    if value is None:
        return None
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(
            f"Invalid {name} '{value}' — expected YYYY-MM-DD format"
        )


# ---------------------------------------------------------------------------
# SNOWFLAKE TABLE HELPERS
# ---------------------------------------------------------------------------
def _get_target_table_info() -> tuple[str, str, str]:
    """Read and validate destination table coordinates from env vars."""
    database = os.getenv("TARGET_DATABASE")
    schema = os.getenv("TARGET_SCHEMA")
    table = os.getenv("TARGET_TABLE")

    if not database or not schema or not table:
        raise EnvironmentError(
            "TARGET_DATABASE, TARGET_SCHEMA, and TARGET_TABLE "
            "must all be set as environment variables on the stored procedure."
        )
    return database, schema, table


def _get_last_loaded_date(
    session: "Session", qualified: str
) -> date:
    """Return the highest date in the target table, or FALLBACK_START_DATE."""
    try:
        res = session.sql(
            f'SELECT MAX("date") AS max_date FROM {qualified}'
        ).collect()
        if not res:
            return FALLBACK_START_DATE
        val = res[0][0]
        if val is None:
            return FALLBACK_START_DATE
        if isinstance(val, str):
            return datetime.strptime(val.split()[0], "%Y-%m-%d").date()
        if isinstance(val, datetime):
            return val.date()
        return date.fromisoformat(str(val))
    except Exception as exc:
        logging.warning("Could not read max date from target table: %s", exc)
        return FALLBACK_START_DATE


def _get_tickers_from_table(
    session: "Session", qualified: str
) -> list[str]:
    """Return distinct tickers already in the target table."""
    try:
        rows = session.sql(
            f'SELECT DISTINCT "ticker" FROM {qualified} WHERE "ticker" IS NOT NULL'
        ).collect()
        tickers = sorted(r[0] for r in rows if r[0])
        if tickers:
            logging.info("Discovered %d tickers from Snowflake table", len(tickers))
            return tickers
    except Exception as exc:
        logging.warning("Could not read tickers from table: %s", exc)
    logging.warning("No tickers found in table — using fallback: %s", FALLBACK_TICKER)
    return [FALLBACK_TICKER]


# ---------------------------------------------------------------------------
# ALPHAVANTAGE API
# ---------------------------------------------------------------------------
def _fetch_ticker(
    ticker: str,
    start_date: date,
    end_date: date,
    api_key: str,
) -> pd.DataFrame:
    """Fetch TIME_SERIES_DAILY_ADJUSTED for one ticker with inline retry.

    Returns a filtered DataFrame (start_date..end_date) or empty DataFrame.
    """
    url = (
        f"{ALPHAVANTAGE_BASE_URL}"
        f"?function=TIME_SERIES_DAILY_ADJUSTED"
        f"&symbol={ticker}"
        f"&apikey={api_key}"
        f"&outputsize=full"
    )

    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if "Time Series (Daily)" not in data:
                note = data.get("Note") or data.get("Information") or str(data)
                raise ValueError(f"Unexpected API response for {ticker}: {note}")

            df = (
                pd.DataFrame.from_dict(
                    data["Time Series (Daily)"], orient="index"
                )
                .reset_index()
                .rename(columns={"index": "date"})
            )
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df = df[(df["date"] >= start_date) & (df["date"] <= end_date)].copy()
            df["ticker"] = ticker

            # Normalise column names: strip leading digit+dot added by AV
            df.rename(
                columns=lambda c: c.split(". ", 1)[-1].replace(" ", "_"),
                inplace=True,
            )
            logging.info(
                "Fetched %d rows for %s (%s → %s)",
                len(df),
                ticker,
                start_date,
                end_date,
            )
            return df

        except Exception as exc:
            last_exc = exc
            logging.warning(
                "Attempt %d/%d failed for %s: %s",
                attempt,
                MAX_RETRIES,
                ticker,
                exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_WAIT_SECONDS)

    logging.error("All retries exhausted for %s: %s", ticker, last_exc)
    return pd.DataFrame()


def _fetch_all_tickers(
    tickers: list[str],
    start_date: date,
    end_date: date,
    api_key: str,
) -> pd.DataFrame:
    """Fetch multiple tickers in parallel using a thread pool.

    Returns a concatenated DataFrame of all successful results.
    """
    frames: list[pd.DataFrame] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_ticker, ticker, start_date, end_date, api_key): ticker
            for ticker in tickers
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                df = future.result()
                if not df.empty:
                    frames.append(df)
            except Exception as exc:
                logging.error("Unhandled error fetching %s: %s", ticker, exc)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# STORED PROCEDURE HANDLER
# ---------------------------------------------------------------------------
def run_as_sproc(
    session: "Session",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> str:
    """Snowpark stored-procedure handler.

    Parameters
    ----------
    session : snowflake.snowpark.Session
        Injected automatically by Snowflake.
    start_date : str or None
        Optional. First date to load in YYYY-MM-DD format.
    end_date : str or None
        Optional. Last date to load (inclusive) in YYYY-MM-DD format.

    Returns
    -------
    str
        A summary message logged as the procedure's return value.
    """
    # ---- Configuration ----
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "ALPHAVANTAGE_API_KEY must be set as an environment variable "
            "on the stored procedure."
        )

    database, schema, table = _get_target_table_info()
    qualified_table = f'"{database}"."{schema}"."{table}"'

    today = date.today()
    parsed_start = _parse_date_arg(start_date, "start_date")
    parsed_end = _parse_date_arg(end_date, "end_date")

    # ---- Resolve window ----
    resolved_end: date = parsed_end if parsed_end is not None else today

    if parsed_start is not None:
        resolved_start: date = parsed_start
    else:
        last_loaded = _get_last_loaded_date(session, qualified_table)
        resolved_start = last_loaded + timedelta(days=1)

    # ---- Sanity check ----
    if resolved_start > resolved_end:
        msg = (
            f"Nothing to load: start ({resolved_start}) "
            f"is after end ({resolved_end})"
        )
        logging.info(msg)
        return msg

    # ---- Trading-day guard (default / today-only mode) ----
    if parsed_start is None and parsed_end is None:
        if not is_trading_day(today):
            msg = f"{today} is not a trading day — skipping"
            logging.info(msg)
            return msg
    else:
        # Explicit range — warn about non-trading days but don't block
        non_trading = [
            resolved_start + timedelta(days=i)
            for i in range((resolved_end - resolved_start).days + 1)
            if not is_trading_day(resolved_start + timedelta(days=i))
        ]
        if non_trading:
            sample = ", ".join(str(d) for d in non_trading[:5])
            logging.info(
                "Range includes %d non-trading day(s): %s%s",
                len(non_trading),
                sample,
                " ..." if len(non_trading) > 5 else "",
            )

    logging.info("Fetching %s to %s", resolved_start, resolved_end)

    # ---- Tickers ----
    tickers_env = os.getenv("TICKERS")
    if tickers_env:
        tickers = [t.strip() for t in tickers_env.split(",") if t.strip()]
        logging.info("Using %d tickers from TICKERS env var", len(tickers))
    else:
        tickers = _get_tickers_from_table(session, qualified_table)

    # ---- Fetch from AlphaVantage ----
    final_df = _fetch_all_tickers(tickers, resolved_start, resolved_end, api_key)

    if final_df.empty:
        msg = "API returned no new rows — nothing to insert"
        logging.info(msg)
        return msg

    # ---- Normalise date column type ----
    final_df["date"] = pd.to_datetime(final_df["date"]).dt.date

    # ---- Write to Snowflake ----
    try:
        sdf = session.create_dataframe(final_df)
        sdf.write.save_as_table(qualified_table, mode="append")
        msg = (
            f"Inserted {len(final_df)} rows "
            f"({resolved_start} to {resolved_end}) into {qualified_table}"
        )
        logging.info(msg)
        return msg
    except Exception as exc:
        logging.exception("Failed to write to Snowflake: %s", exc)
        raise


# ---------------------------------------------------------------------------
# LOCAL GUARD
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(
        "Snowpark_API_Script.py is a Snowflake stored procedure handler.\n"
        "Run register_sproc.py to upload and register it in Snowflake."
    )
