"""
register_sproc.py
=================
One-time local script that:
  1. Connects to Snowflake using your local credentials / config.
  2. Creates an internal stage (if it doesn't already exist).
  3. Uploads Snowpark_API_Script.py to that stage.
  4. Registers STOCK_SPROC as a permanent Snowpark stored procedure.
  5. (Optional) Creates a daily Snowflake Task that calls STOCK_SPROC.

Run once from your terminal:
  python register_sproc.py

Prerequisites (install locally if not already present):
  pip install snowflake-snowpark-python pandas pandas_market_calendars requests

Configuration
-------------
Fill in the SNOWFLAKE_* and TARGET_* constants below, or set them as
environment variables before running.
"""

import os
from snowflake.snowpark import Session
from snowflake.snowpark.types import StringType

# ---------------------------------------------------------------------------
# ✏️  CONFIGURE THESE — or set as env vars
# ---------------------------------------------------------------------------
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT",   "YOUR_ACCOUNT")      # e.g. xy12345.us-east-1
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER",       "YOUR_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD",   "YOUR_PASSWORD")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE",       "YOUR_ROLE")         # e.g. SYSADMIN
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE",  "YOUR_WAREHOUSE")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE",   "YOUR_DATABASE")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA",     "YOUR_SCHEMA")

# Destination table for stock price rows
TARGET_DATABASE = os.getenv("TARGET_DATABASE", SNOWFLAKE_DATABASE)
TARGET_SCHEMA   = os.getenv("TARGET_SCHEMA",   SNOWFLAKE_SCHEMA)
TARGET_TABLE    = os.getenv("TARGET_TABLE",    "STOCK_PRICES_DAILY")

# AlphaVantage API key — stored as a proc env var (never hard-coded in SQL)
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "YOUR_AV_KEY")

# Optional: comma-separated tickers if you don't want auto-discovery
# Leave empty string "" to auto-discover from the target table instead
TICKERS = os.getenv("TICKERS", "")

# Stored proc + stage names
SPROC_NAME  = "STOCK_SPROC"
STAGE_NAME  = "STOCK_SPROC_STAGE"

# Set to True to also create a daily Snowflake Task
CREATE_TASK         = True
TASK_NAME           = "STOCK_SPROC_DAILY_TASK"
TASK_SCHEDULE       = "USING CRON 0 18 * * MON-FRI America/New_York"  # 6 PM ET weekdays

# ---------------------------------------------------------------------------
# SCRIPT PATH
# ---------------------------------------------------------------------------
import pathlib
SCRIPT_DIR  = pathlib.Path(__file__).parent
SPROC_FILE  = SCRIPT_DIR / "Snowpark_API_Script.py"

# ---------------------------------------------------------------------------
# CONNECT
# ---------------------------------------------------------------------------
def build_session() -> Session:
    print("Connecting to Snowflake…")
    session = Session.builder.configs({
        "account":   SNOWFLAKE_ACCOUNT,
        "user":      SNOWFLAKE_USER,
        "password":  SNOWFLAKE_PASSWORD,
        "role":      SNOWFLAKE_ROLE,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "database":  SNOWFLAKE_DATABASE,
        "schema":    SNOWFLAKE_SCHEMA,
    }).create()
    print(f"  Connected as {SNOWFLAKE_USER} @ {SNOWFLAKE_ACCOUNT}")
    return session


# ---------------------------------------------------------------------------
# STAGE
# ---------------------------------------------------------------------------
def ensure_stage(session: Session) -> None:
    print(f"Creating stage {STAGE_NAME} if not exists…")
    session.sql(
        f"CREATE STAGE IF NOT EXISTS {STAGE_NAME} "
        f"COMMENT = 'Stage for STOCK_SPROC Python handler'"
    ).collect()
    print("  Stage ready.")


# ---------------------------------------------------------------------------
# REGISTER STORED PROCEDURE
# ---------------------------------------------------------------------------
def register_sproc(session: Session) -> None:
    print(f"Registering stored procedure {SPROC_NAME}…")

    # Build env var dict to embed in the proc
    env_vars = {
        "ALPHAVANTAGE_API_KEY": ALPHAVANTAGE_API_KEY,
        "TARGET_DATABASE":      TARGET_DATABASE,
        "TARGET_SCHEMA":        TARGET_SCHEMA,
        "TARGET_TABLE":         TARGET_TABLE,
    }
    if TICKERS:
        env_vars["TICKERS"] = TICKERS

    session.sproc.register_from_file(
        file_path=str(SPROC_FILE),
        func_name="run_as_sproc",
        return_type=StringType(),
        input_types=[StringType(), StringType()],   # start_date, end_date
        name=SPROC_NAME,
        stage_location=f"@{STAGE_NAME}",
        packages=[
            "snowflake-snowpark-python",
            "pandas",
            "requests",
            "pandas_market_calendars",
        ],
        is_permanent=True,
        replace=True,
        # Pass secrets as env vars so they're never written into the SQL body
        execute_as="caller",
        comment="Fetches AlphaVantage daily stock data and appends to target table.",
    )
    print(f"  {SPROC_NAME} registered successfully.")
    print()
    print("  Call with no args (today, trading-day guard active):")
    print(f"    CALL {SPROC_NAME}(NULL, NULL);")
    print()
    print("  Call with explicit range:")
    print(f"    CALL {SPROC_NAME}('2026-01-01', '2026-03-31');")


# ---------------------------------------------------------------------------
# OPTIONAL TASK
# ---------------------------------------------------------------------------
def create_task(session: Session) -> None:
    print(f"Creating task {TASK_NAME}…")
    session.sql(f"DROP TASK IF EXISTS {TASK_NAME}").collect()
    session.sql(f"""
        CREATE TASK {TASK_NAME}
            WAREHOUSE = {SNOWFLAKE_WAREHOUSE}
            SCHEDULE  = '{TASK_SCHEDULE}'
            COMMENT   = 'Daily trigger for STOCK_SPROC (runs weekdays at 6 PM ET)'
        AS
            CALL {SPROC_NAME}(NULL, NULL)
    """).collect()

    # Tasks are created suspended — resume it now
    session.sql(f"ALTER TASK {TASK_NAME} RESUME").collect()
    print(f"  Task {TASK_NAME} created and resumed.")
    print(f"  Schedule: {TASK_SCHEDULE}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    session = build_session()
    try:
        ensure_stage(session)
        register_sproc(session)
        if CREATE_TASK:
            create_task(session)
        print("\nDone!")
    finally:
        session.close()


if __name__ == "__main__":
    main()
