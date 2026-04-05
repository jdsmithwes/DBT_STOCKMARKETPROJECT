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
from typing import Optional

import pandas as pd
import requests

# Session is always available inside the Snowflake runtime.
# The try/except lets the file be imported locally for testing without error.
try:
    from snowflake.snowpark import Session
except ImportError:  # pragma: no cover
    Session = object  # type: ignore

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
FALLBACK_START_DATE: date = date(2020, 1, 1)
ALPHAVANTAGE_BASE_URL: str = "https://www.alphavantage.co/query"
MAX_WORKERS: int = 8          # parallel API calls
MAX_RETRIES: int = 3          # per-ticker retry attempts
RETRY_WAIT_SECONDS: int = 15  # wait between retries on 429 / rate-limit note
# AlphaVantage rate limit: 25 req/min on free tier, 75/min on premium.
# We stay under by releasing a token every (60 / RATE_LIMIT_PER_MIN) seconds.
RATE_LIMIT_PER_MIN: int = 25  # increase to 75 if you have a premium key

# ---------------------------------------------------------------------------
# S&P 500 TICKER LIST  (as of early 2026 — update periodically)
# ---------------------------------------------------------------------------
SP500_TICKERS: list[str] = [
    "MMM","AOS","ABT","ABBV","ACN","ADBE","AMD","AES","AFL","A","APD","ABNB",
    "AKAM","ALB","ARE","ALGN","ALLE","LNT","ALL","GOOGL","GOOG","MO","AMZN",
    "AMCR","AEE","AAL","AEP","AXP","AIG","AMT","AWK","AMP","AME","AMGN","APH",
    "ADI","ANSS","AON","APA","AAPL","AMAT","APTV","ACGL","ADM","ANET","AJG",
    "AIZ","T","ATO","ADSK","ADP","AZO","AVB","AVY","AXON","BKR","BALL","BAC",
    "BK","BBWI","BAX","BDX","WRB","BBY","BIO","TECH","BIIB","BLK","BX","BA",
    "BCH","BSX","BMY","AVGO","BR","BRO","BF.B","BLDR","BG","CDNS","CZR","CPT",
    "CPB","COF","CAH","KMX","CCL","CARR","CTLT","CAT","CBOE","CBRE","CDW","CE",
    "COR","CNC","CNX","CDAY","CF","CRL","SCHW","CHTR","CVX","CMG","CB","CHD",
    "CI","CINF","CTAS","CSCO","C","CFG","CLX","CME","CMS","KO","CTSH","CL",
    "CMCSA","CMA","CAG","COP","ED","STZ","CEG","COO","CPRT","GLW","CTVA","CSGP",
    "COST","CTRA","CCI","CSX","CMI","CVS","DHI","DHR","DRI","DVA","DAY","DE",
    "DAL","XRAY","DVN","DXCM","FANG","DLR","DFS","DG","DLTR","D","DPZ","DOV",
    "DOW","DHC","DTE","DUK","DD","EMN","ETN","EBAY","ECL","EIX","EW","EA","ELV",
    "LLY","EMR","ENPH","ETR","EOG","EPAM","EQT","EFX","EQIX","EQR","ESS","EL",
    "ETSY","EG","EVRG","ES","EXC","EXPE","EXPD","EXR","XOM","FFIV","FDS","FICO",
    "FAST","FRT","FDX","FIS","FITB","FSLR","FE","FI","FLT","FMC","F","FTNT",
    "FTV","FOXA","FOX","BEN","FCX","GRMN","IT","GE","GEHC","GEV","GEN","GNRC",
    "GD","GIS","GM","GPC","GILD","GPN","GL","GDDY","GS","HAL","HIG","HAS","HCA",
    "DOC","HSIC","HSY","HES","HPE","HLT","HOLX","HD","HON","HRL","HST","HWM",
    "HPQ","HUBB","HUM","HBAN","HII","IBM","IEX","IDXX","ITW","INCY","IR","PODD",
    "INTC","ICE","IFF","IP","IPG","INTU","ISRG","IVZ","INVH","IQV","IRM","JBAL",
    "JKHY","J","JBL","JNPR","JPM","JNPR","K","KVUE","KDP","KEY","KEYS","KMB",
    "KIM","KMI","KLAC","KHC","KR","LHX","LH","LRCX","LW","LVS","LDOS","LEN",
    "LIN","LYV","LKQ","LMT","L","LOW","LULU","LYB","MTB","MRO","MPC","MKTX",
    "MAR","MMC","MLM","MAS","MA","MTCH","MKC","MCD","MCK","MDT","MRK","META",
    "MET","MTD","MGM","MCHP","MU","MSFT","MAA","MRNA","MHK","MOH","TAP","MDLZ",
    "MPWR","MNST","MCO","MS","MOS","MSI","MSCI","NDAQ","NTAP","NOV","NFLX","NWL",
    "NEM","NWSA","NWS","NEE","NKE","NI","NDSN","NSC","NTRS","NOC","NCLH","NRG",
    "NUE","NVDA","NVR","NXPI","ORLY","OXY","ODFL","OMC","ON","OKE","ORCL","OTIS",
    "PCAR","PKG","PANW","PH","PAYX","PAYC","PYPL","PNR","PEP","PFE","PCG","PM",
    "PSX","PNW","PXD","PNC","POOL","PPG","PPL","PFG","PG","PGR","PLD","PRU","PEG",
    "PTC","PSA","PHM","QRVO","PWR","QCOM","DGX","RL","RJF","RTX","O","REG","REGN",
    "RF","RSG","RMD","RVTY","ROK","ROL","ROP","ROST","RCL","SPGI","CRM","SBAC",
    "SLB","STX","SRE","NOW","SHW","SPG","SWKS","SJM","SW","SNA","SOLV","SO",
    "LUV","SWK","SBUX","STT","STLD","STE","SYK","SMCI","SYF","SNPS","SYY","TMUS",
    "TROW","TTWO","TPR","TRGP","TGT","TEL","TDY","TFX","TER","TSLA","TXN","TMO",
    "TJX","TSCO","TT","TDG","TRV","TRMB","TFC","TYL","TSN","USB","UBER","UDR",
    "ULTA","UNP","UAL","UPS","URI","UNH","UHS","VLO","VTR","VLTO","VRSN","VRSK",
    "VZ","VRTX","VTRS","VICI","V","VMC","WRK","WAB","WBA","WMT","DIS","WBD",
    "WM","WAT","WEC","WFC","WELL","WST","WDC","WHR","WMB","WTW","GWW","WYNN",
    "XEL","XYL","YUM","ZBRA","ZBH","ZTS",
]

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
    """Read destination table coordinates from env vars, with hardcoded fallbacks."""
    database = os.getenv("TARGET_DATABASE", "DBT_STOCKPROJECT")
    schema   = os.getenv("TARGET_SCHEMA",   "DBT_DEV_JDS_STAGING")
    table    = os.getenv("TARGET_TABLE",    "STG_STOCKPRICE")
    return database, schema, table


def _get_last_loaded_date(
    session: Session, qualified: str
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
    session: Session, qualified: str
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
    logging.warning("No tickers in table — falling back to full S&P 500 list (%d tickers)", len(SP500_TICKERS))
    return list(SP500_TICKERS)


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
    """Fetch all tickers in parallel with a rate-limit throttle.

    Dispatches up to MAX_WORKERS threads but gates each call through a
    semaphore that releases at RATE_LIMIT_PER_MIN tokens/minute so we
    never exceed the AlphaVantage API limit.

    Returns a concatenated DataFrame of all successful results.
    """
    import threading

    interval = 60.0 / RATE_LIMIT_PER_MIN          # seconds between permits
    semaphore = threading.Semaphore(0)             # starts locked
    stop_event = threading.Event()
    total = len(tickers)

    def _permit_dispatcher():
        """Release one permit every `interval` seconds until done."""
        released = 0
        while not stop_event.is_set() and released < total:
            semaphore.release()
            released += 1
            if released < total:
                time.sleep(interval)

    dispatcher = threading.Thread(target=_permit_dispatcher, daemon=True)
    dispatcher.start()

    def _throttled_fetch(ticker: str) -> pd.DataFrame:
        semaphore.acquire()
        return _fetch_ticker(ticker, start_date, end_date, api_key)

    frames: list[pd.DataFrame] = []
    failed: list[str] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_throttled_fetch, ticker): ticker
            for ticker in tickers
        }
        done = 0
        for future in as_completed(futures):
            ticker = futures[future]
            done += 1
            try:
                df = future.result()
                if not df.empty:
                    frames.append(df)
            except Exception as exc:
                logging.error("Unhandled error fetching %s: %s", ticker, exc)
                failed.append(ticker)
            if done % 50 == 0 or done == total:
                logging.info("Progress: %d / %d tickers fetched", done, total)

    stop_event.set()

    if failed:
        logging.warning("%d ticker(s) failed entirely: %s", len(failed), failed[:20])

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# STORED PROCEDURE HANDLER
# ---------------------------------------------------------------------------
def run_as_sproc(
    session: Session,
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
    # Read the AlphaVantage key from a Snowflake secret (preferred) and fall
    # back to an environment variable so the script still works locally.
    try:
        import _snowflake  # only available inside the Snowflake runtime
        api_key = _snowflake.get_generic_secret_string('alphavantage_secret')
    except ImportError:
        api_key = os.getenv("ALPHAVANTAGE_API_KEY")

    if not api_key:
        raise EnvironmentError(
            "AlphaVantage API key not found. Inside Snowflake, create a "
            "generic secret named 'alphavantage_api_key' and grant the "
            "procedure access to it. Locally, set ALPHAVANTAGE_API_KEY."
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
    # Priority: TICKERS env var → S&P 500 list (default)
    # The table-lookup path is kept as a last resort but the S&P 500 list
    # is always the baseline so a fresh table still gets all 500 tickers.
    tickers_env = os.getenv("TICKERS")
    if tickers_env:
        tickers = [t.strip() for t in tickers_env.split(",") if t.strip()]
        logging.info("Using %d tickers from TICKERS env var", len(tickers))
    else:
        tickers = list(SP500_TICKERS)
        logging.info("Using full S&P 500 ticker list (%d tickers)", len(tickers))

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
