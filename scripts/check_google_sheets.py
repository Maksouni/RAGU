from __future__ import annotations

import sys
from pathlib import Path
from argparse import ArgumentParser

import gspread
from google.oauth2.service_account import Credentials

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from apps.common.settings import get_settings  # noqa: E402


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("--details", action="store_true")
    args = parser.parse_args()

    settings = get_settings()
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_file(
        settings.google_service_account_json_path,
        scopes=scopes,
    )
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(settings.google_sheets_spreadsheet_id)
    worksheets = spreadsheet.worksheets()
    worksheet_names = ", ".join(worksheet.title for worksheet in worksheets)
    print(f"spreadsheet_title={spreadsheet.title}")
    print(f"worksheets={worksheet_names}")
    for worksheet in worksheets:
        values = worksheet.get_all_values()
        data_rows = max(0, len(values) - 1)
        print(f"{worksheet.title}.data_rows={data_rows}")
        if args.details and worksheet.title == "overview":
            header = values[0] if values else []
            print(f"overview.header={header}")
            for row_index, row in list(enumerate(values, start=1))[-5:]:
                event_id = row[0] if row else ""
                question = row[2] if len(row) > 2 else ""
                print(f"overview.row={row_index} event_id={event_id[:12]} question={question[:80]}")


if __name__ == "__main__":
    main()
