#production config,, defines annotators and their assignments

PRODUCTION_ANNOTATORS = [
    {#me
        "entry_code": "RAFUH-PRIMARY",
        "annotator_id": "primary_rafuh",
        "role": "primary",
        "display_name": "MAIN ANNOTATION"
    },
    # other annotators
    {
        "entry_code": "TOM-B8D4",
        "annotator_id": "TomB_001",
        "role": "expert",
        "display_name": "Tom B"
    },
    {
        "entry_code": "TOM-W7Q0",
        "annotator_id": "TomW_002",
        "role": "expert",
        "display_name": "Tom W"
    },
    {
        "entry_code": "ALICE-H2F3",
        "annotator_id": "Alice_003",
        "role": "expert",
        "display_name": "Alice"
    },
    {
        "entry_code": "DAVID-F8V3",
        "annotator_id": "David_004",
        "role": "expert",
        "display_name": "David"
    },
    {
        "entry_code": "CHRISTOPH-S4E2",
        "annotator_id": "Chris_005",
        "role": "expert",
        "display_name": "Christoph"
    },
    {#TEST
        "entry_code": "TEST-0T0T",
        "annotator_id": "Test_001",
        "role": "expert",
        "display_name": "DEBUG USER"
    },
]


def generate_primary_assignments(passage_ids, num_passages=2000):
    # assigns first num_passages to primary annotator
    assignments = []
    for i, passage_id in enumerate(passage_ids[:num_passages]):
        assignments.append({
            "annotator_id": "primary_rafuh",
            "passage_id": passage_id,
            "set": "core"
        })
    return assignments


def generate_expert_assignments(passage_ids, expert_annotators, overlap_count=100):
    #Generate expert assignments with overlap for inter-rater reliablity
    #each expert gets first overlap_count passages
    assignments = []

    # assign first overlap_count to each expert
    for annotator_id in expert_annotators:
        for passage_id in passage_ids[:overlap_count]:
            assignments.append({
                "annotator_id": annotator_id,
                "passage_id": passage_id,
                "set": "core"
            })

    return assignments

#generate assignmnets for deployment
def get_production_assignments(passage_ids):
    all_assignments = {}

    all_assignments["primary_rafuh"] = generate_primary_assignments(passage_ids, num_passages=2000)

    # expert_assignments = generate_expert_assignments(passage_ids, expert_ids, overlap_count=5)
    # for assignment in expert_assignments:
    #     annotator_id = assignment["annotator_id"]
    #     if annotator_id not in all_assignments:
    #         all_assignments[annotator_id] = []
    #     all_assignments[annotator_id].append(assignment)

    return all_assignments #dict mapping annotator_id to assignments list
