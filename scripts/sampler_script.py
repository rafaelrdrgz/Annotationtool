#Assigns a stratifed sample of passages to annotators for Inter-Annotator Agreement
# reads completed primary annotations from gSHEET and samples a set of 20+5
#python scripts/sampler_script.py --dry-run  (preview) and --seed 99 for custom seed
import argparse
import json
import sys
import toml
import gspread
from collections import defaultdict
from google.oauth2.service_account import Credentials
from pathlib import Path

#project root on path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from data import prod_config
from data.common import EXCLUSION_IDS, build_domain_map, is_annotation_complete

DOMAIN_MAP = build_domain_map()

ALL_DOMAINS = {"metaphysical", "epistemological", "ethical", "political"}
ALL_CATEGORIES = set(DOMAIN_MAP.keys())

MINIMUM_POOL = 250
OVERLAP_COUNT = 5

# target unique slots per expert per bin (ann_type x wc_tertile) totalling 20
TARGET_BINS = {
    ("exclusion",  "short"):  1,
    ("exclusion",  "medium"): 2,
    ("exclusion",  "long"):   1,
    ("single_cat", "short"):  2,
    ("single_cat", "medium"): 3,
    ("single_cat", "long"):   2,
    ("multi_cat",  "short"):  2,
    ("multi_cat",  "medium"): 4,
    ("multi_cat",  "long"):   2,
}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]



def _get_client():
    secrets_path = ROOT / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        print("ERROR: .streamlit/secrets.toml not found.")
        sys.exit(1)
    secrets = toml.load(secrets_path)
    creds = Credentials.from_service_account_info(
        dict(secrets["gcp_service_account"]), scopes=SCOPES
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(secrets["spreadsheet_key"])
    return spreadsheet


def _load_primary_annotations(spreadsheet):
    #load all rows from primary_rafuh annotator sheet
    try:
        sheet = spreadsheet.worksheet("primary_rafuh")
        records = sheet.get_all_records()
        annotations = []
        for r in records:
            cats_raw = r.get("categories", "{}")
            try:
                categories = json.loads(cats_raw) if isinstance(cats_raw, str) else cats_raw
            except (json.JSONDecodeError, TypeError):
                categories = {}
            annotations.append({
                "passage_id":               str(r.get("passage_id", "")),
                "timestamp":                str(r.get("timestamp", "")),
                "explicit_philosophy_flag": r.get("explicit_philosophy_flag", False),
                "categories":               categories,
                "notes":                    r.get("notes", ""),
                "duration_seconds":         r.get("duration_seconds", 0),
            })
        return annotations
    except gspread.exceptions.WorksheetNotFound:
        print("ERROR: 'primary_rafuh' sheet not found. Has the primary annotator saved any annotations?")
        sys.exit(1)

def _load_passages_metadata(spreadsheet):
    # load passage metadata from passages sheet, returns {id: passage_dict}
    sheet = spreadsheet.worksheet("passages")
    records = sheet.get_all_records()
    passages = {}
    for r in records:
        pid = str(r.get("id", ""))
        passages[pid] = {
            "id":            pid,
            "source":        r.get("source", "unknown"),
            "word_count":    int(r.get("word_count") or 0),
            "score":         r.get("score", 0),
            "priority":      r.get("priority", ""),
        }
    return passages

# CORE ALGORITHM FUNCTIONS ===========================

def resolve_latest(raw_records):
    #collapse append-only log to most recent record per passage_id
    latest = {}
    for record in raw_records:
        pid = record.get("passage_id", "")
        ts  = record.get("timestamp", "")
        if pid and (pid not in latest or ts > latest[pid]["timestamp"]):
            latest[pid] = record
    return latest


def extract_features(record: dict, passage_meta: dict) -> dict:
    # derive all stratification dimensions for a single passage
    # this is the MAIN feature extraction used for sampling
    pid   = record["passage_id"]
    cats  = record.get("categories", {})
    meta  = passage_meta or {}

    phil_cats = {k: v for k, v in cats.items() if k not in EXCLUSION_IDS}
    excl_cats = {k: v for k, v in cats.items() if k in EXCLUSION_IDS}

    #annotation type
    if excl_cats:
        ann_type = "exclusion"
    elif len(phil_cats) == 1:
        ann_type = "single_cat"
    else:
        ann_type = "multi_cat"

    #domains present
    domains_present = {DOMAIN_MAP[c] for c in phil_cats if c in DOMAIN_MAP}

    # confidence tier - maps high/med/low to numeric for comparison
    conf_rank = {"high": 3, "medium": 2, "low": 1}
    conf_vals = [
        conf_rank.get(v.get("confidence"), 0)
        for v in phil_cats.values()
        if v.get("confidence") in conf_rank
    ]
    if not conf_vals:
        conf_tier = "exclusion_only"
    elif all(c == 3 for c in conf_vals):
        conf_tier = "high_conf"
    elif any(c == 1 for c in conf_vals):
        conf_tier = "low_conf"
    else:
        conf_tier = "mixed_conf"

    return {
        "passage_id":      pid,
        "ann_type":        ann_type,
        "domains_present": domains_present,
        "category_ids":    frozenset(phil_cats.keys()),
        "conf_tier":       conf_tier,
        "source":          meta.get("source", "unknown"),
        "word_count":      meta.get("word_count", 0),
        "score":           meta.get("score", 0),
        "num_cats":        len(phil_cats),
    }


def assign_wc_tertiles(features_list):
    #mutates features_list in place adding wc_tertile key
    wcs = sorted(f["word_count"] for f in features_list)
    n = len(wcs)
    if n == 0:
        return
    t1 = wcs[n // 3]
    t2 = wcs[(2 * n) // 3]
    for f in features_list:
        wc = f["word_count"]
        if wc <= t1:
            f["wc_tertile"] = "short"
        elif wc <= t2:
            f["wc_tertile"] = "medium"
        else:
            f["wc_tertile"] = "long"


def compute_priority_score(feat):
    # higher = more useful for IAA, breaks ties within strata
    score = 0.0
    score += feat["num_cats"] * 1.5
    conf_bonus = {"low_conf": 3.0, "mixed_conf": 1.5, "high_conf": 0.0, "exclusion_only": 0.5}
    score += conf_bonus.get(feat["conf_tier"], 0.0)
    score += len(feat["domains_present"]) * 1.0
    if feat.get("wc_tertile") == "medium":
        score += 1.0
    return score



def sample_overlap(features_list: list, existing_overlap_ids: set, seed: int) -> list:
    # SELECT OVERLAP_COUNT passages shared across ALL experts ===========================
    # stratified to cover as many domains as possible, favours mixed/low confidence
    candidates = [
        f for f in features_list
        if f["passage_id"] not in existing_overlap_ids
        and f["ann_type"] != "exclusion"  #overlap should be philosophical
    ]

    overlap = []
    covered_domains = set()

    #first try pick one passage per domain (4 passages)
    for domain in sorted(ALL_DOMAINS):
        dom_cands = [
            f for f in candidates
            if domain in f["domains_present"]
            and f["passage_id"] not in {o["passage_id"] for o in overlap}
        ]
        if dom_cands:
            chosen = dom_cands[0]
            overlap.append(chosen)
            covered_domains |= chosen["domains_present"]
            if len(overlap) >= OVERLAP_COUNT:
                break

    # fill remaining slots with highest priority remaining
    rest = [
        f for f in candidates
        if f["passage_id"] not in {o["passage_id"] for o in overlap}
    ]
    for f in rest:
        if len(overlap) >= OVERLAP_COUNT:
            break
        overlap.append(f)

    return [f["passage_id"] for f in overlap]


def sample_for_experts(
    features_list: list,
    expert_ids: list,
    existing_assignments: set,  # set of (annotator_id, passage_id)
    overlap_ids: list,
    per_expert: int = 25,
    seed: int = 42,
) -> dict:
    # returns {expert_id: [passage_id, ...]} with each list length per_expert
    # each experts list = overlap_ids + unique passages
    #no unique passage in more than one experts list
    # passages already assigned to expert are skipped
    unique_per_expert = per_expert - len(overlap_ids)
    feat_by_pid = {f["passage_id"]: f for f in features_list}

    overlap_set = set(overlap_ids)  #global pool not in overlap for unique slots

    claimed_unique = set()  #passages claimed as unique by earlier experts

    result = {}

    for expert_id in expert_ids:
        already_assigned = {pid for (aid, pid) in existing_assignments if aid == expert_id}

        # available for this experts unique slots
        available = [
            f for f in features_list
            if f["passage_id"] not in overlap_set
            and f["passage_id"] not in claimed_unique
            and f["passage_id"] not in already_assigned
        ]

        # PASS 1: stratified bin filling ===========================
        bins = defaultdict(list)
        for f in available:
            key = (f["ann_type"], f["wc_tertile"])
            bins[key].append(f)

        selection = []
        for bin_key, target in sorted(TARGET_BINS.items(), key=lambda x: -x[1]):
            candidates = bins.get(bin_key, [])
            selected_pids = {f["passage_id"] for f in selection}  #exclude already added
            candidates = [f for f in candidates if f["passage_id"] not in selected_pids]
            for f in candidates[:target]:
                selection.append(f)

        # PASS 2: domain coverage enforcement ===========================
        selected_pids = {f["passage_id"] for f in selection}
        covered_domains = set()
        for f in selection:
            covered_domains |= f["domains_present"]
        #include overlap domains too
        for pid in overlap_ids:
            if pid in feat_by_pid:
                covered_domains |= feat_by_pid[pid]["domains_present"]

        missing_domains = ALL_DOMAINS - covered_domains
        for domain in sorted(missing_domains):
            dom_cands = [
                f for f in available
                if domain in f["domains_present"]
                and f["passage_id"] not in selected_pids
            ]
            if dom_cands:
                chosen = dom_cands[0]
                selection.append(chosen)
                selected_pids.add(chosen["passage_id"])
                covered_domains |= chosen["domains_present"]

        #opportunistically fill rare categories if still under quota
        if len(selection) < unique_per_expert:
            covered_cats = set()
            for f in selection:
                covered_cats |= f["category_ids"]
            for pid in overlap_ids:
                if pid in feat_by_pid:
                    covered_cats |= feat_by_pid[pid]["category_ids"]

            cat_counts = {
                cat: sum(1 for f in features_list if cat in f["category_ids"])
                for cat in ALL_CATEGORIES - covered_cats
            }
            for cat in sorted(cat_counts, key=lambda c: cat_counts[c]):
                if len(selection) >= unique_per_expert:
                    break
                cat_candidates = [
                    f for f in available
                    if cat in f["category_ids"]
                    and f["passage_id"] not in selected_pids
                ]
                if cat_candidates:
                    chosen = cat_candidates[0]
                    selection.append(chosen)
                    selected_pids.add(chosen["passage_id"])

        # PASS 3: backfill with source cap ===========================
        source_cap = max(1, int(unique_per_expert * 0.40))
        while len(selection) < unique_per_expert:
            source_counts = defaultdict(int)
            for f in selection:
                source_counts[f["source"]] += 1

            remaining = [
                f for f in available
                if f["passage_id"] not in {sf["passage_id"] for sf in selection}
            ]
            capped = [f for f in remaining if source_counts[f["source"]] < source_cap]
            candidates = capped if capped else remaining
            if not candidates:
                break
            selection.append(candidates[0])

        # trim to unique_per_expert if domain/category passes overshot
        if len(selection) > unique_per_expert:
            #sort by priority, preserve domain coverage
            selection.sort(key=lambda f: -f["_priority"])
            trimmed = selection[:unique_per_expert]
            # check domain coverage maintained
            trimmed_domains = set()
            for f in trimmed:
                trimmed_domains |= f["domains_present"]
            trimmed_domains |= covered_domains  #includes overlap domains
            if trimmed_domains < ALL_DOMAINS:
                #re-add passages needed for missing domains
                for f in selection[unique_per_expert:]:
                    if f["domains_present"] - trimmed_domains:
                        trimmed.append(f)
                        trimmed_domains |= f["domains_present"]
                        if trimmed_domains >= ALL_DOMAINS:
                            break
            selection = trimmed

        unique_pids = [f["passage_id"] for f in selection]

        claimed_unique.update(unique_pids)  #mark as claimed for subsequent experts

        result[expert_id] = overlap_ids + unique_pids  # expert gets overlap + unique

    return result


def print_iaa_report(assignments, overlap_ids, features_list):
    feat_by_pid = {f["passage_id"]: f for f in features_list}
    overlap_set = set(overlap_ids)

    print("\n" + "=" * 60)
    print("IAA SAMPLING REPORT")
    print("=" * 60)

    all_assigned = set()
    for pids in assignments.values():
        all_assigned.update(pids)

    print(f"\nPool size (completed annotations): {len(features_list)}")
    print(f"Total unique passages assigned:    {len(all_assigned)}")
    print(f"Overlap passages ({len(overlap_ids)}):          {', '.join(overlap_ids)}")

    all_cats_covered = set()
    for expert_id, pids in assignments.items():
        feats = [feat_by_pid[p] for p in pids if p in feat_by_pid]

        ann_types   = defaultdict(int)
        conf_tiers  = defaultdict(int)
        wc_tertiles = defaultdict(int)
        sources     = defaultdict(int)
        domains     = set()
        cats        = set()

        for f in feats:
            ann_types[f["ann_type"]] += 1
            conf_tiers[f["conf_tier"]] += 1
            wc_tertiles[f.get("wc_tertile", "?")] += 1
            sources[f["source"]] += 1
            domains |= f["domains_present"]
            cats |= f["category_ids"]

        all_cats_covered |= cats

        overlap_in_set = [p for p in pids if p in overlap_set]
        unique_in_set  = [p for p in pids if p not in overlap_set]

        print(f"\n-- {expert_id} ({len(pids)} passages: {len(unique_in_set)} unique + {len(overlap_in_set)} overlap) --")
        print(f"   Ann types:   {dict(ann_types)}")
        print(f"   Confidence:  {dict(conf_tiers)}")
        print(f"   Word count:  {dict(wc_tertiles)}")
        print(f"   Sources:     {dict(sources)}")
        print(f"   Domains:     {domains}")
        missing = ALL_CATEGORIES - cats
        if missing:
            print(f"   Missing categories: {missing}")
        else:
            print(f"   All 12 categories covered")

    print(f"\n-- Global category coverage across all experts --")
    global_missing = ALL_CATEGORIES - all_cats_covered
    if global_missing:
        print(f"   Missing from entire IAA set: {global_missing}")
    else:
        print(f"   All 12 categories covered globally")
    print("=" * 60)


# gsheets write/read functions

def read_iaa_assignments(spreadsheet):
    # returns set of (annotator_id, passage_id) for existing IAA assignments
    #prevents duplicating on re-runs
    try:
        sheet = spreadsheet.worksheet("assignments")
        rows = sheet.get_all_records()
        existing = set()
        for row in rows:
            if str(row.get("set", "")).startswith("iaa"):
                existing.add((str(row["annotator_id"]), str(row["passage_id"])))
        return existing
    except Exception as e:
        print(f"WARNING: Could not read existing IAA assignments: {e}")
        return set()


def write_iaa_assignments(spreadsheet, assignments, overlap_ids):
    #appends rows to assignments sheet with set=iaa or set=iaa_overlap
    # does NOT clear existing rows, safe to run multple times
    try:
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
        print(f"ERROR: Failed to write IAA assignments: {e}")
        return False


# MAIN ===========================

def main():
    parser = argparse.ArgumentParser(description="Generate IAA stratified sample")
    parser.add_argument("--seed",       type=int,  default=42)
    parser.add_argument("--per-expert", type=int,  default=25)
    parser.add_argument("--dry-run",    action="store_true",
                        help="Print report without writing to Google Sheets")
    args = parser.parse_args()

    print("Connecting to Google Sheets...")
    spreadsheet = _get_client()
    print(f"  Connected: {spreadsheet.title}")

    # 1. load primary annotations
    print("\nLoading primary annotations...")
    raw_annotations = _load_primary_annotations(spreadsheet)
    print(f"  {len(raw_annotations)} raw records loaded")

    #2. resolve latest per passage and filter to complete
    latest = resolve_latest(raw_annotations)
    eligible = {pid: rec for pid, rec in latest.items() if is_annotation_complete(rec)}
    print(f"  {len(eligible)} complete annotations (latest per passage)")

    # 3. check minimum pool
    if len(eligible) < MINIMUM_POOL:
        needed = MINIMUM_POOL - len(eligible)
        print(f"\nERROR: Need at least {MINIMUM_POOL} completed annotations to run.")
        print(f"  Currently have {len(eligible)}. Annotate {needed} more passages first.")
        sys.exit(1)

    #4. load passage metadata
    print("\nLoading passage metadata...")
    passages_meta = _load_passages_metadata(spreadsheet)
    print(f"  {len(passages_meta)} passages loaded")

    # 5. extract features
    features_list = []
    for pid, rec in eligible.items():
        meta = passages_meta.get(pid, {})
        feat = extract_features(rec, meta)
        features_list.append(feat)

    assign_wc_tertiles(features_list)
    for f in features_list:
        f["_priority"] = compute_priority_score(f)

    #sort highest priority first, stable tie-break by passage_id
    features_list.sort(key=lambda f: (-f["_priority"], f["passage_id"]))

    # 6. load existing IAA assignments for re-run detection
    print("\nChecking for existing IAA assignments...")
    existing_assignments = read_iaa_assignments(spreadsheet)
    print(f"  {len(existing_assignments)} existing IAA assignment(s) found")

    #7. get expert IDs excluding Test_001
    expert_ids = [
        a["annotator_id"]
        for a in prod_config.PRODUCTION_ANNOTATORS
        if a["role"] == "expert" and a["annotator_id"] != "Test_001"
    ]
    print(f"\nExperts: {expert_ids}")

    # 8. check capacity
    unique_per_expert = args.per_expert - OVERLAP_COUNT
    total_unique_needed = len(expert_ids) * unique_per_expert

    non_overlap_pool = [
        f for f in features_list
        if f["ann_type"] != "exclusion"  #overlap drawn from non-exclusion
    ]

    if len(features_list) < total_unique_needed + OVERLAP_COUNT:
        actual_unique = (len(features_list) - OVERLAP_COUNT) // len(expert_ids)
        actual_per_expert = actual_unique + OVERLAP_COUNT
        print(f"\nWARNING: Pool has {len(features_list)} passages but {total_unique_needed + OVERLAP_COUNT} needed.")
        print(f"  Reducing to {actual_per_expert} passages per expert ({actual_unique} unique + {OVERLAP_COUNT} overlap).")
        args.per_expert = actual_per_expert

    #9. sample overlap passages
    existing_olap = {
        pid for (aid, pid) in existing_assignments
        if any(
            (aid2, pid) in existing_assignments
            for aid2 in expert_ids
        )
    }
    # overlap passages = those assigned to ALL experts
    expert_assignment_sets = {
        eid: {pid for (aid, pid) in existing_assignments if aid == eid}
        for eid in expert_ids
    }
    existing_olap = set.intersection(*expert_assignment_sets.values()) if expert_assignment_sets else set()

    print(f"\nExisting overlap passages: {len(existing_olap)}")

    if len(existing_olap) >= OVERLAP_COUNT:
        overlap_ids = list(existing_olap)[:OVERLAP_COUNT]
        print(f"  Using existing overlap: {overlap_ids}")
    else:
        new_overlap = sample_overlap(features_list, existing_olap, seed=args.seed)
        overlap_ids = list(existing_olap) + new_overlap
        overlap_ids = overlap_ids[:OVERLAP_COUNT]
        print(f"  Sampled overlap: {overlap_ids}")

    #10. sample unique passages for each expert
    print("\nSampling passages for each expert...")
    assignments = sample_for_experts(
        features_list=features_list,
        expert_ids=expert_ids,
        existing_assignments=existing_assignments,
        overlap_ids=overlap_ids,
        per_expert=args.per_expert,
        seed=args.seed,
    )
    # mirror Christophs assignments to Test user so test user works outside IAA
    assignments["Test_001"] = list(assignments.get("Chris_005", []))

    # 11. report
    print_iaa_report(assignments, overlap_ids, features_list)

    #count new assignments (exclude already in gsheets)
    new_rows = 0
    for expert_id, pids in assignments.items():
        for pid in pids:
            if (expert_id, pid) not in existing_assignments:
                new_rows += 1

    if new_rows == 0:
        print("\nNo new assignments to write — all experts are already fully assigned.")
        print("To assign more, re-run with a higher --per-expert value.")
        return

    print(f"\n{new_rows} new assignment(s) to write.")

    #12. write to gsheets unless dry run
    if args.dry_run:
        print("\n[DRY RUN] Skipping write to Google Sheets.")
        print("Remove --dry-run to commit these assignments.")
    else:
        # only write NEW assignments
        new_assignments = {
            expert_id: [
                pid for pid in pids
                if (expert_id, pid) not in existing_assignments
            ]
            for expert_id, pids in assignments.items()
        }
        print("\nWriting to Google Sheets assignments tab...")
        success = write_iaa_assignments(spreadsheet, new_assignments, overlap_ids)
        if success:
            print(f"  Done. {new_rows} rows written.")
            print("  Experts can now log in and see their passages.")
        else:
            print("  Write failed. Check error above.")
            sys.exit(1)


if __name__ == "__main__":
    main()
