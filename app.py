import streamlit as st
import json
import time
import copy
from datetime import datetime
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent))

from data.storage import (
    load_passages, lookup_annotator, get_assignments,
    save_annotation, load_annotations, get_completed_passage_ids,
    load_all_annotations, add_bonus_passages
)
from data import prod_config
from data.common import EXCLUSION_IDS, is_annotation_complete
st.set_page_config(
    page_title="Philosophical Presupposition Annotator",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="collapsed",
)
#catgeories loading from json in 
@st.cache_data
def load_categories():
    with open(Path(__file__).parent / "config" / "categories.json") as f:
        return json.load(f)

CATEGORIES = load_categories()

CATEGORY_MAP = {}
DOMAIN_MAP = {}
for domain in CATEGORIES["domains"]:
    for cat in domain["categories"]:
        CATEGORY_MAP[cat["id"]] = cat
        DOMAIN_MAP[cat["id"]] = domain

# CSS ==========================================================================
def inject_css():
    st.markdown("""
    <style>
    .block-container { padding-top: 3rem; max-width: 1400px; }

    .entry-container {
        max-width: 480px; margin: 8vh auto; padding: 3rem 2.5rem;
        border: 1px solid #e2e8f0; border-radius: 16px;
        background: #ffffff; box-shadow: 0 15px 55px rgba(0,0,0,0.3);
    }
                


    .entry-title {
        font-size: 1.6rem; font-weight: 700; color: #1e293b;
        margin-bottom: 1.0rem; text-align: center;
    }
    .entry-subtitle {
        font-size: 0.95rem; color: #64748b; text-align: center;
        margin-bottom: -0.5rem; line-height: 1.5;
    }

                

    .passage-box {
        background: #ffffff; border: 1px solid #e2e8f0; border-radius: 12px;
        padding: 1.5rem 1.75rem; line-height: 1.85; font-size: 1.05rem;
        color: #1e293b; margin-bottom: 0.75rem;
    }
    .passage-meta {
        font-size: 0.8rem; color: #94a3b8; margin-bottom: 0.75rem;
        padding: 0.4rem 0.75rem; background: #f8fafc; border-radius: 6px;
        border: 1px solid #f1f5f9;
    }
    .sentence {
        cursor: default; padding: 2px 1px; border-radius: 3px;
        transition: background 0.15s ease;
    }
    .sentence-selectable { cursor: pointer; }
    .sentence-selectable:hover {
        text-decoration: underline; text-decoration-style: dotted;
        text-decoration-color: #94a3b8; text-underline-offset: 3px;
    }

                
                
    .domain-header {
        font-size: 0.8rem; font-weight: 600; color: #64748b;
        text-transform: uppercase; letter-spacing: 0.05em;
        margin: 0.4rem 0 0.3rem 0; padding-bottom: 0.25rem;
        border-bottom: 2px solid #f1f5f9;
    }
                

                

    .cat-info {
        font-size: 0.78rem; color: #64748b; line-height: 1.4;
        padding: 0.5rem 0.75rem; background: #f8fafc; border-radius: 6px;
        margin: 0.25rem 0 0.5rem 0; border-left: 3px solid #e2e8f0;
    }

    .save-success {
        padding: 0.5rem 1rem; background: #f0fdf4; border: 1px solid #bbf7d0;
        border-radius: 8px; color: #166534; font-size: 0.85rem; text-align: center;
    }
    .save-warning {
        padding: 0.5rem 1rem; background: #fffbeb; border: 1px solid #fde68a;
        border-radius: 8px; color: #92400e; font-size: 0.85rem; text-align: center;
    }

    .progress-text { font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; }

    .completion-card {
        max-width: 580px; margin: 4vh auto; padding: 2.5rem;
        border: 1px solid #bbf7d0; border-radius: 16px;
        background: #f0fdf4; text-align: center;
    }
    .completion-card h2 { color: #166534; margin-bottom: 0.5rem; }
    .completion-card p { color: #15803d; line-height: 1.6; }

    .explicit-flag {
        padding: 0.5rem 0.75rem; background: #fefce8; border: 1px solid #fde68a;
        border-radius: 8px; margin-bottom: 0.75rem;
    }

    /* hiding Ctrl+Enter streamlit text  for notes box*/
    .stTextArea [data-testid="InputInstructions"] {
        display: none !important;
    }
    .stTextArea [data-baseweb="textarea"] [data-testid="InputInstructions"] {
        display: none !important;
    }
    .stTextArea textarea::placeholder {
        opacity: 0.6;
    }
    /* alt selector for the instructions */
    [data-testid="stTextArea"] [data-testid="InputInstructions"] {
        display: none !important;
    }
    .stCheckbox label p {
        font-size: 1.1rem !important;
        font-weight: 500;
    }
/* INCOMPLETE BANNERS HERE ------------------------------ */
    .incomplete-banner {
        padding: 0.75rem 1.25rem;
        background: #fffbeb;
        border: 1px solid #fde68a;
        border-left: 4px solid #f59e0b;
        border-radius: 8px;
        margin-bottom: 0.75rem;
    }
    .incomplete-banner-title {
        font-size: 0.95rem;
        font-weight: 600;
        color: #92400e;
        margin-bottom: 0.25rem;
    }
    .incomplete-banner-text {
        font-size: 0.85rem;
        color: #a16207;
        line-height: 1.5;
    }

    .all-complete-banner {
        padding: 0.75rem 1.25rem;
        background: #f0fdf4;
        border: 1px solid #bbf7d0;
        border-left: 4px solid #22c55e;
        border-radius: 8px;
        margin-bottom: 0.75rem;
    }
    .all-complete-banner-title {
        font-size: 0.95rem;
        font-weight: 600;
        color: #166534;
        margin-bottom: 0.25rem;
    }
    .all-complete-banner-text {
        font-size: 0.85rem;
        color: #15803d;
    }
    </style>
    """, unsafe_allow_html=True)

inject_css()

# INTERFACE STATE  ----------------------------------------------------------------INTERFACE STATE  -----------
def init_session():
    defaults = {
        "authenticated": False,
        "annotator": None,
        "passages": None,
        "assignments": None,
        "current_index": 0,
        "annotation_state": {},
        "active_category": None,
        "completed_annotations": [],
        "retry_queue": [],
        "save_status": None,
        "start_time": None,
        "has_unsaved_changes": False,
        "annotation_history": {},
        "bonus_rounds": 0,
        "incomplete_check_active": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session()
#ENTRY SCREEN 
def show_entry_screen():
    st.markdown("""
    <div class="entry-container">
        <div class="entry-title">Philosophical Presupposition Annotator Interface</div>
        <div class="entry-subtitle">
            This site will allow you to classify passages to help with my Final Year Project. 
            <br><strong>Please ensure you have read the guidance document before you begin. </strong> 
            <br>Enter your access code to log in.
            <br>Or email me regarding any issues :)
            <br>Thanks for your help!
        </div>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        code = st.text_input("Access Code", placeholder="e.g. RAFA-A1X1", label_visibility="collapsed")
        if st.button("Enter", use_container_width=True, type="primary"):
            if code:
                annotator = lookup_annotator(code)
                if annotator:
                    st.session_state.authenticated = True
                    st.session_state.annotator = annotator
                    st.session_state.passages = load_passages()
                    st.session_state.assignments = get_assignments(annotator["annotator_id"])
                    st.session_state.current_index = 0
                    _load_or_init_annotation()
                    st.rerun()
                else:
                    st.error("Invalid access code. Please check and try again")
            else:
                st.warning("Please enter your access code")

#annotation state 
def _load_or_init_annotation():
    """Load saved annotation for current passage from history, or start fresh."""
    passage = get_current_passage()
    if passage and passage["id"] in st.session_state.annotation_history:
        st.session_state.annotation_state = copy.deepcopy(
            st.session_state.annotation_history[passage["id"]]
        )
    else:
        #reset to fresh state for new passage
        st.session_state.annotation_state = {
            "categories": {},
            "explicit_flag": False,
            "notes": "",
        }
    st.session_state.active_category = None
    st.session_state.start_time = time.time()
    st.session_state.has_unsaved_changes = False
    st.session_state.save_status = None


def get_current_passage(): 
    assignments = st.session_state.assignments
    if not assignments:
        return None
    idx = st.session_state.current_index
    if idx < 0 or idx >= len(assignments):
        return None
    passage_id = assignments[idx]["passage_id"]
    return st.session_state.passages.get(passage_id)



#then find and return incomplete passages for incomplete banner
#  (assignment_index, passage_id)
def get_incomplete_passages():
    annotator = st.session_state.annotator
    assignments = st.session_state.assignments

    #Loading latest saved annotations from prewrite storage
    saved = load_annotations(annotator["annotator_id"])
    saved_by_passage = {}
    for record in saved:
        saved_by_passage[record["passage_id"]] = record  #latest update bu user is saved 

    incomplete = []
    for idx, assignment in enumerate(assignments):
        pid = assignment["passage_id"]
        #prefer pre-write in-session history 
        ann = st.session_state.annotation_history.get(pid)
        if ann is None:
            ann = saved_by_passage.get(pid)
        if not is_annotation_complete(ann):
            incomplete.append((idx, pid))
    return incomplete


def render_incomplete_banner(incomplete):
    count = len(incomplete)
    passage_nums = [str(idx + 1) for idx, _ in incomplete]
    if count == 1:
        passage_list = f"Passage {passage_nums[0]}"
    elif count == 2:
        passage_list = f"Passages {passage_nums[0]} and {passage_nums[1]}"
    else:
        passage_list = (
            f"Passages {', '.join(passage_nums[:-1])}, and {passage_nums[-1]}"
        )

    st.markdown(
        f"""
        <div class="incomplete-banner">
            <div class="incomplete-banner-title">
                {count} passage{'s' if count != 1 else ''} still
                need{'s' if count == 1 else ''} completion
            </div>
            <div class="incomplete-banner-text">
                {passage_list} {'needs' if count == 1 else 'need'} at least one
                category with a corresponding evidence sentence and confidence
                level (unless marked as an exclusion).
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    ROW_SIZE = 6
    for row_start in range(0, count, ROW_SIZE):
        row_items = incomplete[row_start : row_start + ROW_SIZE]
        cols = st.columns(len(row_items))
        for i, (idx, _pid) in enumerate(row_items):
            with cols[i]:
                is_current = idx == st.session_state.current_index
                label = (
                    f"Passage {idx + 1} (current)"
                    if is_current
                    else f"Passage {idx + 1}"
                )
                if st.button(
                    label,
                    key=f"incomplete_nav_{idx}",
                    use_container_width=True,
                    type="primary" if is_current else "secondary",
                    disabled=is_current,
                ):
                    if st.session_state.has_unsaved_changes:
                        do_save()
                    st.session_state.current_index = idx
                    _load_or_init_annotation()
                    st.rerun()


# Passage rendering
# ----------------------------------------------------------------------
#multi-classified sentences get striped 
def _striped_gradient(colours):
    if len(colours) == 1:
        return colours[0]
    stripe = 6
    parts = []
    for i, c in enumerate(colours):
        parts.append(f"{c} {i*stripe}px, {c} {(i+1)*stripe}px")
    return f"repeating-linear-gradient(135deg, {', '.join(parts)})"
def render_passage(passage):
    """Render passage with color-coded evidence highlighting."""
    ann = st.session_state.annotation_state
    sentences = passage["sentences"]

    # Build highlight map: sentence_idx -> [(bg, border, cat_id), ...]
    highlight_map = {}
    for cat_id, cat_data in ann["categories"].items():
        domain = DOMAIN_MAP.get(cat_id)
        if domain:
            for s_idx in cat_data.get("evidence", []):
                highlight_map.setdefault(s_idx, []).append(
                    (domain["colour"], domain["border_colour"], cat_id)
                )

    # Build HTML spans with highlighting
    spans = []
    for i, sent in enumerate(sentences):
        highlights = highlight_map.get(i, [])
        style_parts = []

        if highlights:
            unique_bg = list(dict.fromkeys(h[0] for h in highlights))
            unique_border = list(dict.fromkeys(h[1] for h in highlights))

            if len(unique_bg) == 1:
                style_parts.append(f"background: {unique_bg[0]}")
                style_parts.append(f"border-left: 3px solid {unique_border[0]}")
                style_parts.append("padding-left: 4px")
            else:
                style_parts.append(f"background: {_striped_gradient(unique_bg)}")
                h = len(unique_border)
                bp = []
                for j, bc in enumerate(unique_border):
                    bp.append(f"{bc} {(j/h)*100}%, {bc} {((j+1)/h)*100}%")
                style_parts.append("border-left: 4px solid transparent")
                style_parts.append(f"border-image: linear-gradient(to bottom, {', '.join(bp)}) 1")
                style_parts.append("padding-left: 4px")

        style = "; ".join(style_parts) if style_parts else ""
        spans.append(f'<span style="{style}">{sent}</span>')

    prose = " ".join(spans)

    # Metadata in header
    meta = (
        f'<div class="passage-meta">'
        f'<strong>{passage["source"]}</strong>  |  {passage["article_title"]}  |  '
        f'{passage["date"]}  |  ID: {passage["id"]}'
        f'</div>'
    )

    # Color legend for assigned categories
    legend = ""
    if highlight_map:
        seen = {}
        for hl_list in highlight_map.values():
            for bg, _, cid in hl_list:
                dname = DOMAIN_MAP[cid]["name"].replace(" Presuppositions", "")
                if dname not in seen:
                    seen[dname] = bg
        if seen:
            items = " ".join(
                f'<span style="display:inline-block;width:12px;height:12px;'
                f'background:{c};border-radius:2px;margin-right:3px;vertical-align:middle;"></span>'
                f'<span style="font-size:0.75rem;color:#64748b;margin-right:12px;">{n}</span>'
                for n, c in seen.items()
            )
            legend = f'<div style="margin-top:0.25rem;">{items}</div>'

    html_content = f'''
{meta}
<div class="passage-box">{prose}</div>
{legend}
'''

    st.markdown(html_content, unsafe_allow_html=True) 


# Sentence selection

def render_sentence_selection(passage, cat_id):
    ann = st.session_state.annotation_state
    cat_data = ann["categories"].get(cat_id, {})
    evidence = set(cat_data.get("evidence", []))
    sentences = passage["sentences"]
    passage_id = passage["id"]

    cols = st.columns(min(len(sentences), 8))
    for i, sent in enumerate(sentences):
        with cols[i % len(cols)]:
            is_sel = i in evidence
            preview = sent[:60] + "..." if len(sent) > 60 else sent
            label = f"S{i+1}" if not is_sel else f"* S{i+1}"

            if st.button(
                label, key=f"sent_{cat_id}_{i}_{passage_id}",
                help=preview, use_container_width=True,
                type="primary" if is_sel else "secondary"
            ):
                if is_sel:
                    evidence.discard(i)
                else:
                    evidence.add(i)
                cat_data["evidence"] = sorted(evidence)
                ann["categories"][cat_id] = cat_data
                st.session_state.has_unsaved_changes = True
                st.rerun()


# CATEGORY SIDE ----------------------------------------------------------------------------
def render_annotation_panel(passage):
    ann = st.session_state.annotation_state
    passage_id = passage["id"]

    has_exclusion = any(c in EXCLUSION_IDS for c in ann["categories"])
    has_philosophical = any(c not in EXCLUSION_IDS for c in ann["categories"])
    st.markdown("**Select at least one category based on the passage:**")

    for domain in CATEGORIES["domains"]:
        st.markdown(
            f'<div class="domain-header" style="border-bottom-color:{domain["border_colour"]}">'
            f'{domain["name"]}</div>',
            unsafe_allow_html=True
        )

        for cat in domain["categories"]:
            cat_id = cat["id"]
            is_excl = cat_id in EXCLUSION_IDS

            disabled = False
            if is_excl and has_philosophical:
                disabled = True
            elif not is_excl and has_exclusion:
                disabled = True

            is_selected = cat_id in ann["categories"]

            checked = st.checkbox(cat["name"], value=is_selected, key=f"cat_{cat_id}_{passage_id}", disabled=disabled)

            if checked and not is_selected:
                ann["categories"][cat_id] = {"confidence": None, "evidence": []}
                st.session_state.active_category = cat_id
                st.session_state.has_unsaved_changes = True
                st.rerun()
            elif not checked and is_selected:
                del ann["categories"][cat_id]
                if st.session_state.active_category == cat_id:
                    st.session_state.active_category = None
                st.session_state.has_unsaved_changes = True
                st.rerun()

            if is_selected and not is_excl:
                cat_data = ann["categories"][cat_id]
                border_color = domain["border_colour"]
                bg_color = domain["colour"]
 
                # Style keyed container
                container_key = f"catbox_{cat_id}_{passage_id}"
                st.markdown(f"""
                <style>
                .st-key-{container_key} {{
                    background: {bg_color} !important;
                    border-left: 4px solid {border_color} !important;
                    border-radius: 8px !important;
                    padding: 0.75rem 1rem !important;
                    margin: -2.0rem 0 0.75rem 0 !important;
                }}
                </style>
                """, unsafe_allow_html=True)

                with st.container(key=container_key):
                    with st.expander("Category Definition and Markers", expanded=False):
                        st.markdown(f'<div class="cat-info">{cat["description"]}</div>', unsafe_allow_html=True)
                        if cat["markers"]:
                            st.markdown(
                                f'<div class="cat-info"><strong>Key markers:</strong> '
                                f'{", ".join(cat["markers"])}</div>',
                                unsafe_allow_html=True
                            )

                    st.markdown("**Which sentence(s) presuppose this category:**")
                    render_sentence_selection(passage, cat_id)

                    conf_options = ["high", "medium", "low"]
                    conf_labels = ["High", "Medium", "Low"]
                    current = cat_data.get("confidence")
                    idx = conf_options.index(current) if current in conf_options else None

                    st.markdown("**Mark your confidence level with this classification:**")
                    new_conf = st.radio(
                        "Confidence", conf_options, index=idx,
                        format_func=lambda x: conf_labels[conf_options.index(x)],
                        key=f"conf_{cat_id}_{passage_id}", horizontal=True, label_visibility="collapsed",
                    )
                    if new_conf != current:
                        cat_data["confidence"] = new_conf
                        st.session_state.has_unsaved_changes = True



# Logic for technics ------------------------------------------------------------------------
# save
def do_save():
    ann = st.session_state.annotation_state
    passage = get_current_passage()
    annotator = st.session_state.annotator

    if not passage or not annotator:
        return False

    duration = int(time.time() - st.session_state.start_time) if st.session_state.start_time else 0

    record = {
        "passage_id": passage["id"],
        "annotator_id": annotator["annotator_id"],
        "duration_seconds": duration,
        "explicit_philosophy_flag": ann.get("explicit_flag", False),
        "categories": ann.get("categories", {}),
        "notes": ann.get("notes", ""),
    }

    # Store in history so [Previous] can restore it
    st.session_state.annotation_history[passage["id"]] = copy.deepcopy(ann)

    # session backup
    st.session_state.completed_annotations.append(record)

    # Layer 1: persistent storage
    success = save_annotation(annotator["annotator_id"], record)

    if success:
        saved_from_queue = 0
        new_queue = []
        for queued in st.session_state.retry_queue:
            if save_annotation(annotator["annotator_id"], queued):
                saved_from_queue += 1
            else:
                new_queue.append(queued)
        st.session_state.retry_queue = new_queue
        st.session_state.save_status = "success"
        if saved_from_queue:
            st.session_state.save_status = f"success_saved_from_queue_{saved_from_queue}"
    else:
        st.session_state.retry_queue.append(record)
        st.session_state.save_status = "warning"

    st.session_state.has_unsaved_changes = False
    return success


def render_sidebar(annotator, done=None, total=None):
    """Shared sidebar: annotator info, progress, backup download, sign out."""
    with st.sidebar:
        st.markdown(f"**{annotator['display_name']}**")
        if done is not None and total is not None:
            st.caption(f"Role: {annotator['role'].title()}")
            st.markdown("---")
            st.markdown(f"**Progress:** {done}/{total} passages")
        else:
            st.markdown(f"**Completed:** {done} passages" if done is not None else "")

        if st.session_state.retry_queue:
            st.warning(f"{len(st.session_state.retry_queue)} annotation(s) pending retry")

        if st.session_state.completed_annotations:
            backup = json.dumps(st.session_state.completed_annotations, indent=2)
            st.download_button(
                "Download backup (JSON)", data=backup,
                file_name=f"backup_{annotator['annotator_id']}_{datetime.now().strftime('%Y%m%d_%H%M')}.json",
                mime="application/json", use_container_width=True,
            )

        st.markdown("---")
        if st.button("Sign out", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()


# Main logic for annotation interface -------------------------------------
def show_annotation_interface():
    annotator = st.session_state.annotator
    assignments = st.session_state.assignments
    completed_ids = get_completed_passage_ids(annotator["annotator_id"])
    total = len(assignments)
    done = sum(1 for a in assignments if a["passage_id"] in completed_ids)

    if done >= total and st.session_state.current_index >= total:
        incomplete = get_incomplete_passages()
        if incomplete:
            st.session_state.incomplete_check_active = True
            st.session_state.current_index = incomplete[0][0]
            _load_or_init_annotation()
            st.rerun()
        else:
            st.session_state.incomplete_check_active = False
            show_completion_screen(annotator, total, done)
            return

    passage = get_current_passage()
    if not passage:
        st.warning("No passage found for this assignment.")
        return

    #    -----  Header 
    h1, h2, h3 = st.columns([2, 1, 2])
    with h1:
        progress = (st.session_state.current_index + 1) / total if total else 0
        st.markdown(
            f'<div class="progress-text">Passage {st.session_state.current_index + 1} of {total}</div>',
            unsafe_allow_html=True
        )
        st.progress(progress)
    with h2:
        st.markdown(
            f'<div style="text-align:center;font-size:0.85rem;color:#64748b;padding-top:0.5rem;">'
            f'{annotator["display_name"]}</div>',
            unsafe_allow_html=True
        )
    with h3:
        bc1, bc2 = st.columns(2)
        with bc1:
            if st.button("Previous", disabled=st.session_state.current_index <= 0, use_container_width=True):
                if st.session_state.has_unsaved_changes:
                    do_save()
                st.session_state.current_index -= 1
                _load_or_init_annotation()
                st.rerun()
        with bc2:
            if st.button("Save and Next", type="primary", use_container_width=True):
                do_save()
                if st.session_state.current_index < total - 1:
                    st.session_state.current_index += 1
                    _load_or_init_annotation()
                else:
                    st.session_state.current_index = total
                st.rerun()

    # save Status
    status = st.session_state.save_status
    if status == "success":
        st.markdown('<div class="save-success">Saved successfully</div>', unsafe_allow_html=True)
    elif status and status.startswith("success_saved_from_queue_"):
        n = status.split("_")[-1]
        st.markdown(f'<div class="save-success">Saved {n} queued annotation(s) also recovered</div>', unsafe_allow_html=True)
    elif status == "warning":
        n = len(st.session_state.retry_queue)
        st.markdown(f'<div class="save-warning">Save to storage failed: queued for retry ({n} pending)</div>', unsafe_allow_html=True)

    st.markdown("")

    # Incomplete passages banner
    if st.session_state.get("incomplete_check_active"):
        incomplete = get_incomplete_passages()
        if incomplete:
            render_incomplete_banner(incomplete)
        else:
            st.markdown(
                """
                <div class="all-complete-banner">
                    <div class="all-complete-banner-title">
                        All passages are now properly completed
                    </div>
                    <div class="all-complete-banner-text">
                        Click below to continue to the completion screen.
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if st.button("Continue to completion", type="primary"):
                st.session_state.incomplete_check_active = False
                st.session_state.current_index = len(st.session_state.assignments)
                st.rerun()

    # Main layout: passage + notes LEFT, categories RIGH T================================================================================
    left_col, right_col = st.columns([3, 2])

    with left_col:
        render_passage(passage)

        # Explicit flag
        ann = st.session_state.annotation_state
        flag = st.checkbox(
            "Explicit Philosophy: Mark that this passage contains explicit philosophical argument.",
            value=ann.get("explicit_flag", False),
            key=f"explicit_flag_cb_{passage['id']}"
        )
        if flag != ann.get("explicit_flag", False):
            ann["explicit_flag"] = flag
            st.session_state.has_unsaved_changes = True

        # Notes under passage
        st.markdown("**Notes**")
        notes = st.text_area(
            "Notes", value=ann.get("notes", ""),
            placeholder="Any observations: ambiguities, edge cases, reasoning...",
            key=f"notes_area_{passage['id']}", label_visibility="collapsed", height=100,
        )
        if notes != ann.get("notes", ""):
            ann["notes"] = notes
            st.session_state.has_unsaved_changes = True

    with right_col:
        render_annotation_panel(passage)

    render_sidebar(annotator, done=done, total=total)




# END screen
def show_completion_screen(annotator, total, done):
    rounds = st.session_state.bonus_rounds

    if rounds == 0:
        title = "All assigned passages complete"
        body = f"You have completed all <strong>{total}</strong> assigned passages.<br>Thank you so much for your help!"
        extra = (
            "If you'd like to continue annotating, you can request 10 additional passages. "
            "These bonus annotations are entirely optional but greatly strengthen "
            "the inter-annotator agreement analysis and would help me out even more."
            "<br>(Only if you have time!)"
        )
    elif rounds == 1:
        title = "Bonus round complete"
        body = (
            f"You have now completed <strong>{done}</strong> passages in total, "
            f"including your bonus round. That is a tremendous help."
        )
        extra = (
            "If you still have time and would like to annotate even more, "
            "you can request another batch. Every additional passage strengthens "
            "my analysis and project much further."
        )
    else:
        title = f"Bonus round {rounds} complete"
        body = (
            f"<strong>{done}</strong> passages annotated in total across "
            f"{rounds} bonus rounds. Your contribution to this project "
            f"has been exceptional!! Thank you."
        )
        extra = (
            "You can request anotha batch if you'd like to continue. :)"
        )

    st.markdown(f"""
    <div class="completion-card">
        <h2>{title}</h2>
        <p>{body}</p>
        <p style="font-size:0.9rem; margin-top:1rem;">{extra}</p>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("Request more passages", use_container_width=True, type="primary"):
            all_ids = list(st.session_state.passages.keys())
            if annotator.get("role") == "expert":
                # Pool = primary's completed passages, minus any already assigned to any expert
                primary_done = get_completed_passage_ids("primary_rafuh")
                all_expert_assignments = []
                for a in prod_config.PRODUCTION_ANNOTATORS:
                    if a["role"] == "expert":
                        all_expert_assignments += get_assignments(a["annotator_id"])
                already_expert = {a["passage_id"] for a in all_expert_assignments}
                pool = primary_done - already_expert
                bonus = add_bonus_passages(annotator["annotator_id"], all_ids, count=5, pool_ids=pool)
            else:
                bonus = add_bonus_passages(annotator["annotator_id"], all_ids, count=10)
            if bonus:
                st.session_state.bonus_rounds += 1
                st.session_state.assignments = get_assignments(annotator["annotator_id"])
                st.session_state.current_index = total
                _load_or_init_annotation()
                st.rerun()
            else:
                st.info("No additional passages available at this time. This may be an error.")

    render_sidebar(annotator, done=done)



#session state main login
if not st.session_state.authenticated:
    show_entry_screen()
else:
    show_annotation_interface()
