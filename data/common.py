# shared constants and validation for app.py and sampler_script
#single source of truth so nothing is duplicated

import json
from pathlib import Path

EXCLUSION_IDS = {"insufficient_philosophical_content", "insufficient_context"}


def load_categories():
    #Load categories from config json
    config_path = Path(__file__).parent.parent / "config" / "categories.json"
    with open(config_path) as f:
        return json.load(f)


def build_domain_map(categories=None):
    # map of category_id to domain_name USED FOR SCRIPT STRATIFICATION
    if categories is None:
        categories = load_categories()
    domain_map = {}
    for domain in categories["domains"]:
        #derive short domain key from name
        name = domain["name"].lower()
        if "metaphysical" in name:
            key = "metaphysical"
        elif "epistemological" in name:
            key = "epistemological"
        elif "ethical" in name:
            key = "ethical"
        elif "political" in name:
            key = "political"
        else:
            key = name  # exclusion categories etc
        for cat in domain["categories"]:
            if cat["id"] not in EXCLUSION_IDS:
                domain_map[cat["id"]] = key
    return domain_map


def is_annotation_complete(annotation_data):
    # checks if annotation is complete
    # exclusion cats alone = complete otherwise need evidence + confidence
    if not annotation_data:
        return False
    categories = annotation_data.get("categories", {})
    if not categories:
        return False
    if any(cat_id in EXCLUSION_IDS for cat_id in categories):
        return True
    for cat_id, cat_data in categories.items():
        if cat_id in EXCLUSION_IDS:
            continue
        if not cat_data.get("evidence"):
            return False
        if not cat_data.get("confidence"):
            return False
    return True
