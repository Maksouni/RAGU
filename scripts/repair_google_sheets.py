from __future__ import annotations

import sys
from pathlib import Path
from argparse import ArgumentParser

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from apps.common.settings import get_settings  # noqa: E402
from apps.orchestrator.intent_guard import is_supported_package_query  # noqa: E402
from apps.sheets_sync.worker import GoogleSheetsBackend  # noqa: E402


def _drop_invalid_rows(backend: GoogleSheetsBackend) -> int:
    backend._connect()
    assert backend._spreadsheet is not None

    overview = backend._spreadsheet.worksheet("overview")
    overview_values = overview.get_all_values()
    if len(overview_values) <= 1:
        return 0

    invalid_event_ids: set[str] = set()
    for row in overview_values[1:]:
        event_id = row[0].strip() if len(row) > 0 else ""
        question = row[2].strip() if len(row) > 2 else ""
        if event_id and question and not is_supported_package_query(question):
            invalid_event_ids.add(event_id)

    if not invalid_event_ids:
        return 0

    deleted = 0
    for worksheet_name in ("overview", "queries", "answers", "ingest_jobs"):
        worksheet = backend._spreadsheet.worksheet(worksheet_name)
        values = worksheet.get_all_values()
        rows_to_delete = []
        for row_index, row in enumerate(values[1:], start=2):
            event_id = row[0].strip() if row else ""
            if event_id in invalid_event_ids:
                rows_to_delete.append(row_index)
        for row_index in sorted(rows_to_delete, reverse=True):
            worksheet.delete_rows(row_index)
            deleted += 1
    return deleted


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("--drop-invalid", action="store_true")
    args = parser.parse_args()

    settings = get_settings()
    backend = GoogleSheetsBackend(settings=settings)
    for worksheet_name in ("overview", "queries", "answers", "ingest_jobs", "errors_audit"):
        backend._ensure_worksheet(worksheet_name)
    deleted = _drop_invalid_rows(backend) if args.drop_invalid else 0
    print("google_sheets_repaired=true")
    print(f"invalid_rows_deleted={deleted}")


if __name__ == "__main__":
    main()
