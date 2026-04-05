"""
Snowpark_CompanyInfo_Script.py
==============================
Self-contained Snowpark Python stored procedure that fetches the AlphaVantage
OVERVIEW endpoint for every S&P 500 company and upserts the results into a
Snowflake table.

Handler
-------
  run_as_sproc(session)

Behaviour
---------
  - Fetches the OVERVIEW endpoint for each S&P 500 ticker.
  - Performs an UPSERT (MERGE) into the target table keyed on `ticker` so
    re-running is safe — existing rows are updated, new rows are inserted.
  - Skips tickers whose API response is empty or missing a Symbol field.
  - Logs progress every 50 tickers.

Credentials / configuration
----------------------------
  The AlphaVantage API key is read from a Snowflake secret named
  'alphavantage_secret' (same secret used by STOCK_SPROC).
  Fallback to ALPHAVANTAGE_API_KEY env var for local testing.

  Target table defaults:
    DBT_STOCKPROJECT.DBT_DEV_STAGING.STG_COMPANYOVERVIEW

Design notes
------------
  - NO external file imports — fully self-contained for Snowflake staging.
  - aiohttp/asyncio replaced with requests + ThreadPoolExecutor (Snowpark
    Python sandbox does not support a running asyncio event loop).
  - Rate-limit throttle: semaphore releases 1 permit every
    (60 / RATE_LIMIT_PER_MIN) seconds to stay under AlphaVantage limits.
  - UPSERT via Snowpark merge() so the table always reflects the latest
    company metadata without duplicates.
"""
from __future__ import annotations

import logging
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pandas as pd
import requests

# Session import — available in Snowflake runtime; fallback for local use.
try:
    from snowflake.snowpark import Session
except ImportError:  # pragma: no cover
    Session = object  # type: ignore

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
ALPHAVANTAGE_BASE_URL: str = "https://www.alphavantage.co/query"
MAX_WORKERS: int = 8
MAX_RETRIES: int = 3
RETRY_WAIT_SECONDS: int = 15  # wait on rate-limit / transient errors
# AlphaVantage: 25 req/min free tier, 75/min premium
RATE_LIMIT_PER_MIN: int = 25  # increase to 75 with a premium key

# Target table defaults (overridden by env vars at runtime if set)
DEFAULT_DATABASE: str = "DBT_STOCKPROJECT"
DEFAULT_SCHEMA: str   = "PUBLIC"
DEFAULT_TABLE: str    = "RAW_COMPANY_OVERVIEW"

# ---------------------------------------------------------------------------
# S&P 500 TICKER LIST  (as of early 2026 — matches Snowpark_API_Script.py)
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

# Columns returned by the AlphaVantage OVERVIEW endpoint that we keep.
# All values are strings from the API — cast to appropriate types in Snowflake
# via the DBT model on top of this staging table.
OVERVIEW_COLUMNS: list[str] = [
    "Symbol", "AssetType", "Name", "Description", "CIK", "Exchange",
    "Currency", "Country", "Sector", "Industry", "Address",
    "OfficialSite", "FiscalYearEnd", "LatestQuarter",
    "MarketCapitalization", "EBITDA", "PERatio", "PEGRatio", "BookValue",
    "DividendPerShare", "DividendYield", "EPS", "RevenuePerShareTTM",
    "ProfitMargin", "OperatingMarginTTM", "ReturnOnAssetsTTM",
    "ReturnOnEquityTTM", "RevenueTTM", "GrossProfitTTM", "DilutedEPSTTM",
    "QuarterlyEarningsGrowthYOY", "QuarterlyRevenueGrowthYOY",
    "AnalystTargetPrice", "AnalystRatingStrongBuy", "AnalystRatingBuy",
    "AnalystRatingHold", "AnalystRatingSell", "AnalystRatingStrongSell",
    "TrailingPE", "ForwardPE", "PriceToSalesRatioTTM", "PriceToBookRatio",
    "EVToRevenue", "EVToEBITDA", "Beta", "52WeekHigh", "52WeekLow",
    "50DayMovingAverage", "200DayMovingAverage", "SharesOutstanding",
    "DividendDate", "ExDividendDate",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ---------------------------------------------------------------------------
# TARGET TABLE HELPER
# ---------------------------------------------------------------------------
def _get_target_table_info() -> tuple[str, str, str]:
    """Return (database, schema, table) with hardcoded fallbacks."""
    database = os.getenv("CI_TARGET_DATABASE", DEFAULT_DATABASE)
    schema   = os.getenv("CI_TARGET_SCHEMA",   DEFAULT_SCHEMA)
    table    = os.getenv("CI_TARGET_TABLE",    DEFAULT_TABLE)
    return database, schema, table


# ---------------------------------------------------------------------------
# ALPHAVANTAGE OVERVIEW FETCH
# ---------------------------------------------------------------------------
def _fetch_overview(ticker: str, api_key: str) -> Optional[dict]:
    """Fetch the OVERVIEW endpoint for one ticker with inline retry.

    Returns a dict of fields or None on permanent failure.
    """
    url = (
        f"{ALPHAVANTAGE_BASE_URL}"
        f"?function=OVERVIEW&symbol={ticker}&apikey={api_key}"
    )

    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            # Empty dict or missing Symbol = no data for this ticker
            if not data or "Symbol" not in data:
                note = data.get("Note") or data.get("Information") or "empty response"
                if "call frequency" in str(note).lower() or "api call" in str(note).lower():
                    # Rate-limit message — treat as retryable
                    raise ValueError(f"Rate limit hit for {ticker}: {note}")
                logging.warning("No overview data for %s: %s", ticker, note)
                return None

            return data

        except Exception as exc:
            last_exc = exc
            logging.warning(
                "Attempt %d/%d failed for %s: %s",
                attempt, MAX_RETRIES, ticker, exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_WAIT_SECONDS)

    logging.error("All retries exhausted for %s: %s", ticker, last_exc)
    return None


# ---------------------------------------------------------------------------
# RATE-THROTTLED BATCH FETCH
# ---------------------------------------------------------------------------
def _fetch_all_overviews(tickers: list[str], api_key: str) -> pd.DataFrame:
    """Fetch OVERVIEW for all tickers with rate-limit throttling.

    Same semaphore-based approach as Snowpark_API_Script.py.
    """
    interval = 60.0 / RATE_LIMIT_PER_MIN
    semaphore = threading.Semaphore(0)
    stop_event = threading.Event()
    total = len(tickers)

    def _permit_dispatcher():
        released = 0
        while not stop_event.is_set() and released < total:
            semaphore.release()
            released += 1
            if released < total:
                time.sleep(interval)

    dispatcher = threading.Thread(target=_permit_dispatcher, daemon=True)
    dispatcher.start()

    def _throttled_fetch(ticker: str) -> Optional[dict]:
        semaphore.acquire()
        return _fetch_overview(ticker, api_key)

    records: list[dict] = []
    failed: list[str] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_throttled_fetch, t): t for t in tickers
        }
        done = 0
        for future in as_completed(futures):
            ticker = futures[future]
            done += 1
            try:
                result = future.result()
                if result:
                    records.append(result)
            except Exception as exc:
                logging.error("Unhandled error fetching %s: %s", ticker, exc)
                failed.append(ticker)
            if done % 50 == 0 or done == total:
                logging.info(
                    "Progress: %d / %d tickers fetched (%d with data)",
                    done, total, len(records),
                )

    stop_event.set()

    if failed:
        logging.warning(
            "%d ticker(s) failed entirely: %s", len(failed), failed[:20]
        )

    if not records:
        return pd.DataFrame()

    # Build DataFrame keeping only the columns we care about;
    # fill any missing fields with None.
    rows = []
    for rec in records:
        row = {col: rec.get(col) for col in OVERVIEW_COLUMNS}
        rows.append(row)

    df = pd.DataFrame(rows, columns=OVERVIEW_COLUMNS)

    # Lowercase column names to match Snowflake convention
    df.columns = [c.lower() for c in df.columns]

    # Add ingest timestamp
    from datetime import datetime
    df["ingest_timestamp"] = datetime.utcnow().isoformat()

    return df


# ---------------------------------------------------------------------------
# UPSERT INTO SNOWFLAKE
# ---------------------------------------------------------------------------
def _upsert_to_snowflake(
    session: Session,
    df: pd.DataFrame,
    qualified_table: str,
) -> int:
    """MERGE df into the target table, keyed on `symbol` (ticker).

    Creates the table on first run if it doesn't exist.
    Returns the number of rows processed.
    """
    # Write to a temp table then MERGE into target
    tmp_table = f"{qualified_table}_TMP_LOAD"

    sdf = session.create_dataframe(df)

    # Overwrite the temp table each run
    sdf.write.save_as_table(tmp_table, mode="overwrite")
    logging.info("Staged %d rows in temp table %s", len(df), tmp_table)

    # Build MERGE statement — update all non-key columns on match
    non_key_cols = [c for c in df.columns if c != "symbol"]
    update_clause = ", ".join(
        f'target."{c}" = source."{c}"' for c in non_key_cols
    )
    insert_cols   = ", ".join(f'"{c}"' for c in df.columns)
    insert_vals   = ", ".join(f'source."{c}"' for c in df.columns)

    merge_sql = f"""
        MERGE INTO {qualified_table} AS target
        USING {tmp_table} AS source
        ON target."symbol" = source."symbol"
        WHEN MATCHED THEN UPDATE SET
            {update_clause}
        WHEN NOT MATCHED THEN INSERT
            ({insert_cols})
        VALUES
            ({insert_vals})
    """
    session.sql(merge_sql).collect()
    logging.info("MERGE into %s complete", qualified_table)

    # Drop temp table
    session.sql(f"DROP TABLE IF EXISTS {tmp_table}").collect()

    return len(df)


# ---------------------------------------------------------------------------
# STORED PROCEDURE HANDLER
# ---------------------------------------------------------------------------
def run_as_sproc(session: Session) -> str:
    """Snowpark stored-procedure handler.

    Parameters
    ----------
    session : snowflake.snowpark.Session
        Injected automatically by Snowflake.

    Returns
    -------
    str
        Summary message (row count + table name).
    """
    # ---- API key ----
    try:
        import _snowflake  # only available inside Snowflake runtime
        api_key = _snowflake.get_generic_secret_string("alphavantage_secret")
    except ImportError:
        api_key = os.getenv("ALPHAVANTAGE_API_KEY")

    if not api_key:
        raise EnvironmentError(
            "AlphaVantage API key not found. Inside Snowflake, ensure the "
            "secret 'alphavantage_secret' is bound via SECRETS = (...) on "
            "this procedure. Locally, set ALPHAVANTAGE_API_KEY."
        )

    # ---- Target table ----
    database, schema, table = _get_target_table_info()
    qualified_table = f'"{database}"."{schema}"."{table}"'

    # ---- Ticker list ----
    tickers_env = os.getenv("CI_TICKERS")
    if tickers_env:
        tickers = [t.strip() for t in tickers_env.split(",") if t.strip()]
        logging.info("Using %d tickers from CI_TICKERS env var", len(tickers))
    else:
        tickers = list(SP500_TICKERS)
        logging.info("Using full S&P 500 ticker list (%d tickers)", len(tickers))

    logging.info(
        "Starting company overview fetch for %d tickers → %s",
        len(tickers), qualified_table,
    )

    # ---- Fetch ----
    df = _fetch_all_overviews(tickers, api_key)

    if df.empty:
        msg = "No company overview data returned — nothing to insert"
        logging.warning(msg)
        return msg

    # ---- Upsert ----
    rows_processed = _upsert_to_snowflake(session, df, qualified_table)

    msg = f"Upserted {rows_processed} company overview rows into {qualified_table}"
    logging.info(msg)
    return msg


# ---------------------------------------------------------------------------
# LOCAL GUARD
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(
        "Snowpark_CompanyInfo_Script.py is a Snowflake stored procedure handler.\n"
        "Run register_company_sproc.py to upload and register it in Snowflake."
    )
