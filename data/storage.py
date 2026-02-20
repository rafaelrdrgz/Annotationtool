# DATA STORAGE BACKEND ===========================
# test mode = local JSON, production = google sheets (set in secrets.toml)
# both backends expose same interface so app doesnt need to change

import json
import os
import time
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent

#determine storage mode
STORAGE_MODE = 'local'  # default
try:
    import streamlit as st
    if hasattr(st, "secrets") and "storage_mode" in st.secrets:
        STORAGE_MODE = st.secrets["storage_mode"]
except(ImportError, Exception):
    pass

# test annotators with entry codes
TEST_ANNOTATORS = [
    {"entry_code": "PRIMARY-001", "annotator_id": "primary_rafuh", "role": "primary", "display_name": "Rafuh (Primary)"},
    {"entry_code": "PHIL-A7X2", "annotator_id": "expert_01", "role": "expert", "display_name": "Expert Annotator 1"},
    {"entry_code": "PHIL-B3K9", "annotator_id": "expert_02", "role": "expert", "display_name": "Expert Annotator 2"},
]

#test assignments - experts get specific passages
TEST_ASSIGNMENTS = {
    "primary_rafuh": [
        {"passage_id": "test_001", "set": "core"},
        {"passage_id": "test_002", "set": "core"},
        {"passage_id": "test_003", "set": "core"},
        {"passage_id": "test_004", "set": "core"},
        {"passage_id": "test_005", "set": "core"},
        {"passage_id": "test_006", "set": "core"},
        {"passage_id": "test_007", "set": "core"},
        {"passage_id": "test_008", "set": "core"},
    ],
    "expert_01": [
        {"passage_id": "test_001", "set": "core"},
        {"passage_id": "test_002", "set": "core"},
        {"passage_id": "test_003", "set": "core"},
        {"passage_id": "test_004", "set": "core"},
    ],
    "expert_02": [
        {"passage_id": "test_001", "set": "core"},
        {"passage_id": "test_005", "set": "core"},
        {"passage_id": "test_006", "set": "core"},
        {"passage_id": "test_007", "set": "core"},
    ],
}


def load_passages():
    # load all passages from local JSON or gsheets depending on mode
    if STORAGE_MODE == 'sheets':
        from . import sheets_backend
        return sheets_backend.load_passages()
    else:
        # local mode - prod passages if available otherwise test
        prod_file = DATA_DIR / "passages.json"
        test_file = DATA_DIR / "test_passages.json"

        file_to_use = prod_file if prod_file.exists() else test_file

        with open(file_to_use, 'r', encoding='utf-8') as f:
            passages = json.load(f)
        return {p["id"]: p for p in passages}


def lookup_annotator(entry_code):
    #look up annotator by entry code, returns dict or None
    if STORAGE_MODE == 'sheets':
        from . import sheets_backend
        return sheets_backend.lookup_annotator(entry_code)
    else:
        # try production config first
        try:
            from . import prod_config
            for a in prod_config.PRODUCTION_ANNOTATORS:
                if a["entry_code"].upper() == entry_code.strip().upper():
                    return a.copy()
        except ImportError:
            pass

        #fall back to test annotators
        for a in TEST_ANNOTATORS:
            if a["entry_code"].upper() == entry_code.strip().upper():
                return a.copy()
        return None


def get_assignments(annotator_id):
    # get passage assignements for an annotator
    if STORAGE_MODE == 'sheets':
        from . import sheets_backend
        return sheets_backend.get_assignments(annotator_id)
    else:
        #local mode - try prod assignments first
        try:
            from . import prod_config
            # generate on the fly from passages
            all_passages = load_passages()
            passage_ids = list(all_passages.keys())
            prod_asgn = prod_config.get_production_assignments(passage_ids)
            if annotator_id in prod_asgn:
                return prod_asgn[annotator_id]
        except (Exception) as e:
            print(e)

        # fallback to test
        return TEST_ASSIGNMENTS.get(annotator_id, [])


def get_annotation_file(annotator_id: str) -> Path:
    #path to annotators local JSON file
    annotations_dir = DATA_DIR / "annotations"
    annotations_dir.mkdir(exist_ok=True)
    return annotations_dir / f"{annotator_id}.json"

def load_annotations(annotator_id):
    # load all annotations for annotator
    if STORAGE_MODE == 'sheets':
        from . import sheets_backend
        return sheets_backend.load_annotations(annotator_id)
    else:
        fpath = get_annotation_file(annotator_id)
        if fpath.exists():
            with open(fpath, 'r', encoding='utf-8') as f:
                return json.load(f)
        return []


def save_annotation(annotator_id: str, annotation: dict) -> bool:
    # APPEND ONLY save - same passage_id kept twice, latest wins at export ===========================
    if STORAGE_MODE == 'sheets':
        from . import sheets_backend
        return sheets_backend.save_annotation(annotator_id, annotation)
    else:
        try:
            existing = load_annotations(annotator_id)
            annotation["timestamp"] = datetime.utcnow().isoformat() + 'Z'
            existing.append(annotation)
            fpath = get_annotation_file(annotator_id)
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2)
            return True
        except Exception as e:
            print(f"Save failed: {e}")
            return False



def get_completed_passage_ids(annotator_id):
    # get set of passage ids that have been annotated
    if STORAGE_MODE == 'sheets':
        from . import sheets_backend
        return sheets_backend.get_completed_passage_ids(annotator_id)
    else:
        annotations = load_annotations(annotator_id)
        return {a["passage_id"] for a in annotations}


def load_all_annotations():
    #Load annotations from ALL annotators, returns {annotator_id: [annotations]}
    if STORAGE_MODE == 'sheets':
        from . import sheets_backend
        return sheets_backend.load_all_annotations()
    else:
        annotations_dir = DATA_DIR / "annotations"
        if not annotations_dir.exists():
            return {}
        result = {}
        for fpath in annotations_dir.glob("*.json"):
            annotator_id = fpath.stem
            with open(fpath, 'r', encoding='utf-8') as f:
                result[annotator_id] = json.load(f)
        return result


def add_bonus_passages(annotator_id, passages, count=10):
    # add bonus passage assignments for annotator who wants more
    if STORAGE_MODE == 'sheets':
        from . import sheets_backend
        return sheets_backend.add_bonus_passages(annotator_id, passages, count)
    else:
        current = get_assignments(annotator_id)
        current_ids = {a["passage_id"] for a in current}

        available = [p_id for p_id in passages if p_id not in current_ids]
        bonus = available[:count]

        for p_id in bonus:
            TEST_ASSIGNMENTS.setdefault(annotator_id, []).append(
                {"passage_id": p_id, "set": "bonus"}
            )
        return bonus
