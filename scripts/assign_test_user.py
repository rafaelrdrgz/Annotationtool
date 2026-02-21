#assigns 10 random passages to Test_001 account for testing
import random
import sys
import toml
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path

ROOT = Path(__file__).parent.parent

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def main():
    secrets_path = ROOT / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        print("ERROR: secrets.toml not found")
        sys.exit(1)

    secrets = toml.load(secrets_path)
    creds = Credentials.from_service_account_info(
        dict(secrets["gcp_service_account"]), scopes=SCOPES
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(secrets["spreadsheet_key"])
    print(f"Connected: {spreadsheet.title}")


    passages_sheet = spreadsheet.worksheet("passages")
    all_passages = passages_sheet.get_all_records()
    all_ids = [str(r["id"]) for r in all_passages]
    print(f"Total passages: {len(all_ids)}")

    asgn_sheet = spreadsheet.worksheet("assignments")
    rows = asgn_sheet.get_all_records()
    existing = {r["passage_id"] for r in rows if r["annotator_id"] == "Test_001"}
    print(f"Test_001 already has {len(existing)} assignments")

    available = [pid for pid in all_ids if pid not in existing]
    if len(available) < 10:
        print(f"Only {len(available)} passages available, assigning all")
        chosen = available
    else:
        chosen = random.sample(available, 10)

    new_rows = [["Test_001", pid, "core"] for pid in chosen]
    asgn_sheet.append_rows(new_rows)
    print(f"\nAssigned {len(chosen)} passages to Test_001:")
    for pid in chosen:
        print(f"  - {pid}")

if __name__ == "__main__":
    main()
