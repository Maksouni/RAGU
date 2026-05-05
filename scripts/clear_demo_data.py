from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = REPO_ROOT / ".env"
GRAPH_DIR = REPO_ROOT / "ragu_working_dir" / "service_graph"

SHEET_HEADERS = {
    "overview": [
        "event_id",
        "user_id",
        "question",
        "answer",
        "timestamp_nsk",
        "response_time_ms",
        "response_mode",
        "answer_mode",
        "cache_hit",
    ],
    "queries": ["event_id", "timestamp", "user_id", "chat_id", "mode", "question", "correlation_id"],
    "answers": ["event_id", "timestamp", "mode", "answer", "correlation_id"],
    "ingest_jobs": ["event_id", "ingest_timestamp", "ingest_status_code", "attempts"],
    "errors_audit": ["event_id", "timestamp", "stage", "error"],
}


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def clear_memgraph() -> None:
    from neo4j import GraphDatabase

    uri = os.getenv("MEMGRAPH_URI", "bolt://127.0.0.1:7687")
    user = os.getenv("MEMGRAPH_USERNAME") or None
    password = os.getenv("MEMGRAPH_PASSWORD") or ""
    database = os.getenv("MEMGRAPH_DATABASE") or None
    auth = (user, password) if user else None

    driver = GraphDatabase.driver(uri, auth=auth)
    try:
        session_kwargs = {"database": database} if database else {}
        with driver.session(**session_kwargs) as session:
            session.run("MATCH (n) DETACH DELETE n").consume()
    except Exception as exc:
        print(f"Memgraph is not reachable, skip graph cleanup: {uri} ({exc})")
        return
    finally:
        driver.close()
    print(f"Memgraph cleared: {uri}")


def clear_local_indexes() -> None:
    if GRAPH_DIR.exists():
        shutil.rmtree(GRAPH_DIR)
    GRAPH_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Local vector/KV indexes cleared: {GRAPH_DIR}")


def clear_outbox() -> None:
    db_path = Path(os.getenv("OUTBOX_DB_PATH", "ragu_working_dir/integration/outbox.sqlite"))
    if not db_path.is_absolute():
        db_path = REPO_ROOT / db_path
    if not db_path.exists():
        print(f"Outbox not found, skip: {db_path}")
        return
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM outbox_events")
        conn.commit()
    print(f"Outbox cleared: {db_path}")


def clear_google_sheets() -> None:
    import gspread
    from google.oauth2.service_account import Credentials

    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    credentials_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "").strip()
    if not spreadsheet_id or not credentials_path:
        print("Google Sheets config is missing, skip.")
        return

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    spreadsheet = gspread.authorize(credentials).open_by_key(spreadsheet_id)

    worksheets_by_title = {worksheet.title: worksheet for worksheet in spreadsheet.worksheets()}
    for title, headers in SHEET_HEADERS.items():
        worksheet = worksheets_by_title.get(title)
        if worksheet is None:
            worksheet = spreadsheet.add_worksheet(title=title, rows=1000, cols=max(26, len(headers)))
        worksheet.clear()
        worksheet.update(values=[headers], range_name="A1")

    overview = spreadsheet.worksheet("overview")
    ordered = [overview] + [ws for ws in spreadsheet.worksheets() if ws.id != overview.id]
    spreadsheet.reorder_worksheets(ordered)
    print(f"Google Sheets cleared: https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit")


def main() -> None:
    parser = argparse.ArgumentParser(description="Clear RAGU demo data from Memgraph, local indexes, outbox and Sheets.")
    parser.add_argument("--skip-memgraph", action="store_true")
    parser.add_argument("--skip-local-indexes", action="store_true")
    parser.add_argument("--skip-outbox", action="store_true")
    parser.add_argument("--skip-sheets", action="store_true")
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    if not args.skip_memgraph:
        clear_memgraph()
    if not args.skip_local_indexes:
        clear_local_indexes()
    if not args.skip_outbox:
        clear_outbox()
    if not args.skip_sheets:
        clear_google_sheets()


if __name__ == "__main__":
    main()
