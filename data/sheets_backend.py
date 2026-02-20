# Google Sheets backend for production
#same interface as local storage but reads/writes to gsheets

import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from pathlib import Path
import streamlit as st

#gsheets setup
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

@st.cache_resource
def get_sheets_client():
    # init and CACHE gsheets client (cached across reruns)
    credentials_dict = dict(st.secrets["gcp_service_account"])
    credentials = Credentials.from_service_account_info(
        credentials_dict,
        scopes=SCOPES
    )
    client = gspread.authorize(credentials)

    spreadsheet_key = st.secrets["spreadsheet_key"]
    spreadsheet = client.open_by_key(spreadsheet_key)

    return client, spreadsheet


def load_passages():
    #load all passages from passages sheet
    _, spreadsheet = get_sheets_client()
    sheet = spreadsheet.worksheet("passages")

    # get all records as list of dicts
    records = sheet.get_all_records()

    #Convert to passage Dict keyed by ID
    passages = {}
    for record in records:
        passage_id = record["id"]
        #parse sentences from JSON string
        sentences = json.loads(record.get("sentences", '[]'))

        passages[passage_id] = {
            'id': passage_id,
            "text": record["text"],
            'sentences': sentences,
            'source': record['source'],
            "article_title": record["article_title"],
            'date': record.get('date', 'N/A'),
            "word_count": record.get("word_count"),
            'article_url': record.get('article_url'),
            'score': record.get('score'),
            "priority": record.get("priority")
        }

    return passages


def lookup_annotator(entry_code):
    # lookup annotator by entry code from annotators sheet
    _, spreadsheet = get_sheets_client()
    sheet = spreadsheet.worksheet("annotators")

    records = sheet.get_all_records()

    for record in records:
        if record["entry_code"].upper() == entry_code.strip().upper():
            return {
                'entry_code': record['entry_code'],
                "annotator_id": record["annotator_id"],
                'role': record['role'],
                "display_name": record["display_name"]
            }

    return None


def get_assignments(annotator_id):
    #Get passage assignments for annotator from assignments sheet
    _, spreadsheet = get_sheets_client()
    sheet = spreadsheet.worksheet("assignments")

    records = sheet.get_all_records()

    # Filter for annotator
    assignments = []
    for record in records:
        if record["annotator_id"] == annotator_id:
            assignments.append({
                "passage_id": record["passage_id"],
                'set': record.get('set', 'core')
            })

    return assignments


def save_annotation(annotator_id: str, annotation: dict) -> bool:
    # SAVE ANNOTATION TO ANNOTATORS DEDICATED SHEET ===========================
    # each annotator has own sheet named by their annotator_id
    try:
        _, spreadsheet = get_sheets_client()

        #get or create annotators sheet
        try:
            sheet = spreadsheet.worksheet(annotator_id)
        except gspread.exceptions.WorksheetNotFound:
            # create new sheet for this annotator
            sheet = spreadsheet.add_worksheet(
                title=annotator_id,
                rows=1000,
                cols=10
            )
            #add header row
            sheet.append_row([
                'timestamp',
                'passage_id',
                'duration_seconds',
                'explicit_philosophy_flag',
                'categories',
                'notes'
            ])

        annotation['timestamp'] = datetime.utcnow().isoformat() + 'Z'

        #convert categories dict to JSON string for storage
        cats_json = json.dumps(annotation.get('categories', {}))

        # append annotation as new row
        sheet.append_row([
            annotation['timestamp'],
            annotation['passage_id'],
            annotation.get('duration_seconds', 0),
            annotation.get('explicit_philosophy_flag', False),
            cats_json,
            annotation.get('notes', '')
        ])

        return True

    except Exception as e:
        print(f"Failed to save annotation to Google Sheets: {e}")
        return False



def load_annotations(annotator_id):
    # load all annotations for annotator from their sheet
    try:
        _, spreadsheet = get_sheets_client()
        sheet = spreadsheet.worksheet(annotator_id)

        records = sheet.get_all_records()

        #convert back to annotation format
        annotations = []
        for record in records:
            categories = json.loads(record.get('categories', '{}'))  #parse categories JSON

            annotations.append({
                'timestamp': record['timestamp'],
                'passage_id': record['passage_id'],
                'annotator_id': annotator_id,
                'duration_seconds': record.get('duration_seconds', 0),
                'explicit_philosophy_flag': record.get('explicit_philosophy_flag', False),
                'categories': categories,
                'notes': record.get('notes', '')
            })

        return annotations

    except gspread.exceptions.WorksheetNotFound:
        #Annotator hasn't saved anything yet
        return []
    except:
        return []


def get_completed_passage_ids(annotator_id):
    #Set of passage id's that have been annotated
    annotations = load_annotations(annotator_id)
    return {a['passage_id'] for a in annotations}

def load_all_annotations():
    # load annotations from ALL annotators returns {annotator_id: [annotations]}
    try:
        _, spreadsheet = get_sheets_client()

        worksheets = spreadsheet.worksheets()

        result = {}
        # skip system sheets
        system_sheets = {'passages', 'annotators', 'assignments'}

        for ws in worksheets:
            if ws.title not in system_sheets:
                result[ws.title] = load_annotations(ws.title)  #annotator sheet

        return result

    except Exception as err:
        print(f"Failed to load all annotations: {err}")
        return {}


def read_iaa_assignments():
    # reads existing IAA assignments from gsheet
    #returns {annotator_id: [{passage_id, set}, ...]} where set starts with 'iaa'
    # USED BY sampler script to detect existing assignments on re-runs
    _, spreadsheet = get_sheets_client()
    sheet = spreadsheet.worksheet("assignments")
    rows = sheet.get_all_records()
    result = {}
    for row in rows:
        if str(row.get("set", "")).startswith("iaa"):
            aid = row["annotator_id"]
            result.setdefault(aid, []).append({
                "passage_id": row["passage_id"],
                "set": row["set"],
            })
    return result


def write_iaa_assignments(assignments: dict, overlap_ids: list) -> bool:
    # APPENDS iaa assignment rows to assignments sheet ===========================
    # assignments = {annotator_id: [passage_id, ...]} only NEW passages
    # overlap ids tagged as iaa_overlap, rest tagged iaa
    #No clear existing rows so safe to call on re-runs
    try:
        _, spreadsheet = get_sheets_client()
        sheet = spreadsheet.worksheet("assignments")
        overlap_set = set(overlap_ids)
        rows = []
        for annotator_id, passage_ids in assignments.items():
            for pid in passage_ids:
                set_type = "iaa_overlap" if pid in overlap_set else "iaa"
                rows.append([annotator_id, pid, set_type])
        if rows:
            sheet.append_rows(rows)
        return True
    except Exception as e:
        print(f"Failed to write IAA assignments to Google Sheets: {e}")
        return False


def add_bonus_passages(annotator_id, all_passage_ids, count=10, pool_ids=None):
    # add bonus passage assignements for annotator who wants more
    # pool_ids restricts to passages primary annotator has completed (for experts)
    try:
        _, spreadsheet = get_sheets_client()
        asgn_sheet = spreadsheet.worksheet("assignments")

        # get current assignments
        current = get_assignments(annotator_id)
        current_ids = {a['passage_id'] for a in current}

        #find available passages, restricted to pool if provided
        pool = pool_ids if pool_ids is not None else set(all_passage_ids)
        available = [pid for pid in all_passage_ids if pid in pool and pid not in current_ids]
        bonus = available[:count]

        #add new assignments to sheet
        for passage_id in bonus:
            asgn_sheet.append_row([
                annotator_id,
                passage_id,
                'bonus'
            ])

        return bonus

    except Exception as e:
        print(f"CANT ADD bonus passages bc {e}")
        return []
