#populate google sheets first time with data

import json
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path
import sys

#parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from data import prod_config

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


def load_secrets():
    import toml
    secrets_file = Path(__file__).parent / ".streamlit" / "secrets.toml"
    secrets = toml.load(secrets_file)
    return secrets

#initialise gsheets client
def get_sheets_client(secrets):
    credentials_dict = secrets["gcp_service_account"]
    credentials = Credentials.from_service_account_info(
        credentials_dict,
        scopes=SCOPES
    )
    client = gspread.authorize(credentials)
    spreadsheet_key = secrets["spreadsheet_key"]
    spreadsheet = client.open_by_key(spreadsheet_key)
    return client, spreadsheet

# create new sheet or clear existing one
def create_or_clear_sheet(spreadsheet, title, headers):
    try:
        sheet = spreadsheet.worksheet(title)
        print(f"  Sheet '{title}' exists, clearing it...")
        sheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        print(f"  Creating sheet '{title}'...")
        sheet = spreadsheet.add_worksheet(title=title, rows=1000, cols=len(headers))

    #headers
    sheet.append_row(headers)
    return sheet

#sheet with all passages
def setup_passages_sheet(spreadsheet, passages):
    print("\nsetting up passages sheet...")

    headers = ['id', 'text', 'sentences', 'source', 'article_title', 'date',
               'word_count', 'article_url', 'score', 'priority']

    sheet = create_or_clear_sheet(spreadsheet, "passages", headers)

    print(f"  Uploading {len(passages)} passages...")

    rows = []
    for passage in passages:
        # convert sentences list to JSON string
        sentences_json = json.dumps(passage['sentences'])

        row = [
            passage['id'],
            passage['text'],
            sentences_json,
            passage.get('source', ''),
            passage.get('article_title', ''),
            passage.get('date', 'N/A'),
            passage.get('word_count', ''),
            passage.get('article_url', ''),
            passage.get('score', ''),
            passage.get('priority', '')
        ]
        rows.append(row)

    #batches of 500 bc rate limits
    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        sheet.append_rows(batch)
        print(f"    Uploaded {min(i + batch_size, len(rows))}/{len(rows)} passages")

    print(f"  Uploaded {len(passages)} passages")

#annotators sheet
def setup_annotators_sheet(spreadsheet, annotators):
    print("\n 2 setting up annotors sheet...")

    headers = ['entry_code', 'annotator_id', 'role', 'display_name']
    sheet = create_or_clear_sheet(spreadsheet, "annotators", headers)

    rows = []
    for annotator in annotators:
        row = [
            annotator['entry_code'],
            annotator['annotator_id'],
            annotator['role'],
            annotator['display_name']
        ]
        rows.append(row)

    sheet.append_rows(rows)
    print(f"   Added {len(annotators)} annotators")

#who is assignmed what sheet?
def setup_assignments_sheet(spreadsheet, assignments_dict):

    print("\n3 ffs let this just work killing myself ")

    headers = ['annotator_id', 'passage_id', 'set']
    sheet = create_or_clear_sheet(spreadsheet, "assignments", headers)

    rows = []
    for annotator_id, assignments in assignments_dict.items():
        for assignment in assignments:
            row = [
                annotator_id,
                assignment['passage_id'],
                assignment.get('set', 'core')
            ]
            rows.append(row)

    batch_size = 500
    total = len(rows)
    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        sheet.append_rows(batch)
        print(f"    Uploaded {min(i + batch_size, total)}/{total} assignments")

    print(f"   Added {len(rows)} total assignments")


def main():
    print("=" * 100)
    print("Google Sheets Setup for Annotation Tool")
    secrets = load_secrets()
    print("   credentials loaded")
    print("\nConnecting to Gogglebox")
    try:
        client, spreadsheet = get_sheets_client(secrets)
        print(f"   Connected to spreadsheet: {spreadsheet.title}")
        print(f"  URL: {spreadsheet.url}")
    except Exception as e:
        print(f"  ERROR:could not connect to sheets")
        print ('=' * 100)
        print(f"  {e}")
        sys.exit(1)

    #load passages
    print("\nLoading passages from data/passages.json...")
    passages_file = Path(__file__).parent / "data" / "passages.json"
    with open(passages_file, 'r', encoding='utf-8') as f:
        passages = json.load(f)
    print(f"   loaded {len(passages)} passages")

    # load annotators from config
    annotators = prod_config.PRODUCTION_ANNOTATORS
    print(f"   loaded {len(annotators)} annotators from config")

    #generate assignments
    passage_ids = [p['id'] for p in passages]
    assignments_dict = prod_config.get_production_assignments(passage_ids)
    total_assignments = sum(len(v) for v in assignments_dict.values())
    print(f"   Generated {total_assignments} assignments")

    # confirm before proceeding
    print("\n" + "=" * 50)
    print("good to populate sheet with:")
    print(f"  - {len(passages)} passages")
    print(f"  - {len(annotators)} annotators")
    print(f"  - {total_assignments} assignments")
    response = input("\ngo for it?  ")
    if response.lower() != 'yes':
        print("ok cancel.")
        sys.exit(0)

    #set up sheets
    try:
        setup_passages_sheet(spreadsheet, passages)
        setup_annotators_sheet(spreadsheet, annotators)
        setup_assignments_sheet(spreadsheet, assignments_dict)

        print(f"\nAll Good:")
        print(f"  {spreadsheet.url}")

    except Exception as e:
        print(f"\n ERROR during setup: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
