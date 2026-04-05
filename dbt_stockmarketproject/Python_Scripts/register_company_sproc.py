"""
register_company_sproc.py
=========================
One-time local script that:
  1. Connects to Snowflake using key-pair auth (same credentials as
     register_sproc.py).
  2. Reuses the existing STOCK_SPROC_STAGE internal stage.
  3. Uploads Snowpark_CompanyInfo_Script.py to that stage.
  4. Registers COMPANY_INFO_SPROC as a permanent Snowpark stored procedure
     with EXTERNAL_ACCESS_INTEGRATIONS + SECRETS (same integration used by
     STOCK_SPROC).
  5. Creates a weekly Snowflake Task (Monday 8 AM ET) that calls
     COMPANY_INFO_SPROC — company fundamentals don't change daily.

Run once from your terminal:
  .venv/bin/python register_company_sproc.py
"""

import os
import pathlib
from cryptography.hazmat.primitives import serialization
from snowflake.snowpark import Session

# Load .env file if present (same directory as this script)
try:
    from dotenv import load_dotenv
    load_dotenv(pathlib.Path(__file__).parent / ".env", override=True)
except ModuleNotFoundError:
    pass

# ---------------------------------------------------------------------------
# CONFIGURATION  (mirrors register_sproc.py — same Snowflake environment)
# ---------------------------------------------------------------------------
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT",   "YOUR_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER",       "YOUR_USER")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE",       "YOUR_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE",  "YOUR_WAREHOUSE")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE",   "YOUR_DATABASE")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA",     "YOUR_SCHEMA")

SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv(
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    str(pathlib.Path.home() / ".snowflake" / "jdsmithwes_rsa_key.p8"),
)

# Target table for company overview rows
TARGET_DATABASE = os.getenv("TARGET_DATABASE", SNOWFLAKE_DATABASE)
TARGET_SCHEMA   = "PUBLIC"
TARGET_TABLE    = "RAW_COMPANY_OVERVIEW"

# Reuse the same stage, secret, and external access integration as STOCK_SPROC
STAGE_NAME                  = "STOCK_SPROC_STAGE"
SECRET_REF                  = f"{TARGET_DATABASE}.DBT_DEV_JDS_STAGING.ALPHAVANTAGE_SECRET"
EXTERNAL_ACCESS_INTEGRATION = "ALPHAVANTAGE_ACCESS_INTEGRATION"

# Stored procedure name
SPROC_NAME = "COMPANY_INFO_SPROC"

# Task: run every Monday at 8 AM ET (company fundamentals refresh weekly)
CREATE_TASK   = True
TASK_NAME     = "COMPANY_INFO_DAILY_TASK"
TASK_SCHEDULE = "USING CRON 0 20 * * * America/New_York"  # 8 PM ET daily

# ---------------------------------------------------------------------------
# SCRIPT PATH
# ---------------------------------------------------------------------------
SCRIPT_DIR = pathlib.Path(__file__).parent
SPROC_FILE = SCRIPT_DIR / "Snowpark_CompanyInfo_Script.py"


# ---------------------------------------------------------------------------
# PRIVATE KEY LOADER
# ---------------------------------------------------------------------------
def _load_private_key() -> bytes:
    """Read the unencrypted PKCS8 private key and return DER bytes."""
    key_path = pathlib.Path(SNOWFLAKE_PRIVATE_KEY_PATH)
    if not key_path.exists():
        raise FileNotFoundError(
            f"Private key not found at {key_path}.\n"
            "Set SNOWFLAKE_PRIVATE_KEY_PATH or ensure the key exists."
        )
    with open(key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


# ---------------------------------------------------------------------------
# CONNECT
# ---------------------------------------------------------------------------
def build_session() -> Session:
    print("Connecting to Snowflake (key-pair auth)…")
    print(f"  account={SNOWFLAKE_ACCOUNT!r}  user={SNOWFLAKE_USER!r}")
    print(f"  role={SNOWFLAKE_ROLE!r}  warehouse={SNOWFLAKE_WAREHOUSE!r}")
    print(f"  database={SNOWFLAKE_DATABASE!r}  schema={SNOWFLAKE_SCHEMA!r}")
    session = Session.builder.configs({
        "account":     SNOWFLAKE_ACCOUNT,
        "user":        SNOWFLAKE_USER,
        "private_key": _load_private_key(),
        "role":        SNOWFLAKE_ROLE,
        "warehouse":   SNOWFLAKE_WAREHOUSE,
        "database":    SNOWFLAKE_DATABASE,
        "schema":      SNOWFLAKE_SCHEMA,
    }).create()
    session.sql(f"USE ROLE {SNOWFLAKE_ROLE}").collect()
    session.sql(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}").collect()
    session.sql(f"USE DATABASE {SNOWFLAKE_DATABASE}").collect()
    session.sql(f"USE SCHEMA {SNOWFLAKE_SCHEMA}").collect()
    print(f"  Connected as {SNOWFLAKE_USER} @ {SNOWFLAKE_ACCOUNT} (role: {SNOWFLAKE_ROLE})")
    return session


# ---------------------------------------------------------------------------
# STAGE  (reuse existing — already created by register_sproc.py)
# ---------------------------------------------------------------------------
def ensure_stage(session: Session) -> None:
    print(f"Ensuring stage {STAGE_NAME} exists…")
    session.sql(
        f"CREATE STAGE IF NOT EXISTS {STAGE_NAME} "
        f"COMMENT = 'Stage for Snowpark stored procedure handlers'"
    ).collect()
    print("  Stage ready.")


# ---------------------------------------------------------------------------
# REGISTER STORED PROCEDURE
# ---------------------------------------------------------------------------
def register_sproc(session: Session) -> None:
    print(f"Uploading {SPROC_FILE.name} to stage…")
    if not SPROC_FILE.exists():
        raise FileNotFoundError(
            f"Handler file not found: {SPROC_FILE}\n"
            "Ensure Snowpark_CompanyInfo_Script.py is in the same directory."
        )
    session.file.put(
        str(SPROC_FILE),
        f"@{STAGE_NAME}/{SPROC_NAME}/",
        overwrite=True,
        auto_compress=False,
    )
    print("  Upload complete.")

    print(f"Registering stored procedure {SPROC_NAME}…")
    ddl = f"""
        CREATE OR REPLACE PROCEDURE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SPROC_NAME}()
        RETURNS VARCHAR
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.11'
        PACKAGES = ('snowflake-snowpark-python', 'pandas', 'requests')
        IMPORTS = ('@{STAGE_NAME}/{SPROC_NAME}/{SPROC_FILE.name}')
        EXTERNAL_ACCESS_INTEGRATIONS = ({EXTERNAL_ACCESS_INTEGRATION})
        SECRETS = ('alphavantage_secret' = {SECRET_REF})
        HANDLER = '{SPROC_FILE.stem}.run_as_sproc'
        EXECUTE AS OWNER
        ;
    """
    session.sql(ddl).collect()
    print(f"  {SPROC_NAME} registered successfully.")
    print()
    print(f"  Call it manually any time:")
    print(f"    CALL {SPROC_NAME}();")


# ---------------------------------------------------------------------------
# TASK
# ---------------------------------------------------------------------------
def create_task(session: Session) -> None:
    print(f"Creating task {TASK_NAME}…")
    session.sql(f"DROP TASK IF EXISTS {TASK_NAME}").collect()
    session.sql(f"""
        CREATE TASK {TASK_NAME}
            WAREHOUSE = {SNOWFLAKE_WAREHOUSE}
            SCHEDULE  = '{TASK_SCHEDULE}'
            COMMENT   = 'Weekly refresh of S&P 500 company overview data (Mon 8 AM ET)'
        AS
            CALL {SPROC_NAME}()
    """).collect()
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
        print()
        print("Next steps:")
        print(f"  1. Verify the proc:  CALL {SPROC_NAME}();")
        print(f"  2. Check the table:  SELECT COUNT(*), COUNT(DISTINCT symbol)")
        print(f"                         FROM {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE};")
        print(f"  3. Task runs every:  {TASK_SCHEDULE}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
