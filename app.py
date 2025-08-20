import os
import re
import io
import pandas as pd
import streamlit as st

# Repo path for the mapping file
MAPPING_PATH = os.environ.get("MAPPING_PATH", "DataMapping.xlsx")


# =========================
# Column resolution helpers
# =========================

def build_column_index(df: pd.DataFrame):
    """Case-safe resolver. Exact wins. Also supports lowercase with and without trimmed spaces."""
    exact = set(df.columns)
    lower_map = {}
    lower_strip_map = {}
    collisions = set()
    collisions_strip = set()

    for c in df.columns:
        key = c.lower()
        if key in lower_map and lower_map[key] != c:
            collisions.add(key)
        else:
            lower_map[key] = c

        key2 = c.lower().strip()
        if key2 in lower_strip_map and lower_strip_map[key2] != c:
            collisions_strip.add(key2)
        else:
            lower_strip_map[key2] = c

    for k in collisions:
        lower_map.pop(k, None)
    for k in collisions_strip:
        lower_strip_map.pop(k, None)

    return {"exact": exact, "lower_map": lower_map, "lower_strip_map": lower_strip_map}


def resolve_col(name: str, col_index):
    """Resolve a requested column name to the actual df column."""
    if name in col_index["exact"]:
        return name
    key = name.lower()
    if key in col_index["lower_map"]:
        return col_index["lower_map"][key]
    key2 = name.lower().strip()
    return col_index["lower_strip_map"].get(key2, None)


# =====================
# Mapping file parsing
# =====================

def parse_data_mapping(mapping_file_path):
    df = pd.read_excel(mapping_file_path)
    df = df.dropna(subset=["FINAL"])

    standard_mappings = {}
    pattern_mappings_1 = []
    pattern_mappings_2 = []
    final_column_order = df["FINAL"].dropna().tolist()

    for _, row in df.iterrows():
        final = str(row["FINAL"]).strip()
        if final not in final_column_order:
            final_column_order.append(final)

        # ORIGINAL_1
        original_1 = row.get("ORIGINAL_1")
        if pd.notna(original_1) and not str(original_1).startswith("["):
            original_1 = str(original_1).strip()
            if re.search(r"(LrNr|\{N\})", original_1, flags=re.IGNORECASE):
                pattern_mappings_1.append({"pattern": original_1, "final_column": final})
            else:
                standard_mappings[original_1] = final  # keep case, resolver handles Q2 vs q2

        # ORIGINAL_2
        original_2 = row.get("ORIGINAL_2")
        if pd.notna(original_2) and not str(original_2).startswith("["):
            original_2 = str(original_2).strip()
            if re.search(r"(LrNr|\{N\})", original_2, flags=re.IGNORECASE):
                pattern_mappings_2.append({"pattern": original_2, "final_column": final})
            else:
                standard_mappings[original_2] = final

    return standard_mappings, pattern_mappings_1, pattern_mappings_2, final_column_order


# ===========================
# Sheet sniffing
# ===========================

def _sniff_best_sheet(xls_file_like):
    """
    Choose the sheet named 'original' that also has 'markers' and at least one *_lrN column.
    If none match that, choose the sheet with 'markers' and the most *_lrN columns.
    If none have both, fall back to the first sheet with 'markers'.
    Else use the first sheet.
    """
    from openpyxl import load_workbook

    def norm(s):
        if s is None:
            return ""
        s = str(s).replace("\ufeff", "").replace("\u200b", "").replace("\u200c", "").replace("\u200d", "")
        return s.strip().lower()

    try:
        xls_file_like.seek(0)
    except Exception:
        pass

    wb = load_workbook(xls_file_like, read_only=True, data_only=True)
    dest_re = re.compile(r"_lr(\d+)", re.IGNORECASE)

    first_sheet = wb.worksheets[0].title if wb.worksheets else None
    fallback_markers = None
    best = None  # tuple(score, sheet_name, headers)

    for ws in wb.worksheets:
        row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
        headers = [str(v).strip() if v is not None else "" for v in row]
        headers_norm = [norm(v) for v in headers]
        has_markers = any(h == "markers" for h in headers_norm)
        n_dest = sum(1 for h in headers if dest_re.search(h))
        is_original_name = norm(ws.title) == "original"

        if has_markers and n_dest > 0:
            # score: prefer sheet named 'original', then more dest cols
            score = (1 if is_original_name else 0, n_dest)
            if best is None or score > best[0]:
                best = (score, ws.title, headers)
        elif has_markers and fallback_markers is None:
            fallback_markers = (ws.title, headers)

    wb.close()
    try:
        xls_file_like.seek(0)
    except Exception:
        pass

    if best:
        return best[1], best[2]
    if fallback_markers:
        return fallback_markers
    if first_sheet:
        # read headers for the first sheet again
        wb = load_workbook(xls_file_like, read_only=True, data_only=True)
        ws = wb[first_sheet]
        row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
        headers = [str(v).strip() if v is not None else "" for v in row]
        wb.close()
        try:
            xls_file_like.seek(0)
        except Exception:
            pass
        return first_sheet, headers
    return None, []


# ===========================
# SATS file loading utility
# ===========================

def _load_sats_dataframe(sats_file_like, sheet_name=None):
    """Return (df, chosen_sheet). Prefer the 'original' sheet that has markers and destination columns."""
    if sheet_name:
        df = pd.read_excel(sats_file_like, sheet_name=sheet_name)
        return df, sheet_name

    chosen_sheet, _ = _sniff_best_sheet(sats_file_like)
    if chosen_sheet is None:
        # fallback to pandas' first sheet
        xls = pd.ExcelFile(sats_file_like)
        chosen_sheet = xls.sheet_names[0]
    try:
        sats_file_like.seek(0)
    except Exception:
        pass
    df = pd.read_excel(sats_file_like, sheet_name=chosen_sheet)
    return df, chosen_sheet


# =======================
# Destination utilities
# =======================

def extract_destination_codes(df: pd.DataFrame):
    dest_pattern = re.compile(r"_lr(\d+)", re.IGNORECASE)
    dest_nums = set()
    for col in df.columns:
        m = dest_pattern.search(col)
        if m:
            dest_nums.add(int(m.group(1)))
    return [f"lr{n}" for n in sorted(dest_nums)]  # lr1, lr2, ...


def expand_pattern_columns(pattern_mappings, dest_codes):
    """Support IDs like Q3, Q3A, Q10_b. Pattern examples:
       Q3_Lr{N}r1, Q3A_LrNr, Q10_b_Lr{N}
       Returns dict with LOWERCASE source keys for resolution.
    """
    expanded = {}
    for entry in pattern_mappings:
        pattern = entry["pattern"].strip()
        final_col = entry["final_column"]

        m = re.match(r"([A-Za-z]\d[\w]*)_Lr(?:Nr|\{N\})(?:r(\d+))?$", pattern, flags=re.IGNORECASE)
        if not m:
            continue

        base_q = m.group(1)
        r_tail = m.group(2)  # digits or None

        for dest in dest_codes:
            full_col = f"{base_q}_{dest}"
            if r_tail:
                full_col += f"r{r_tail}"
            expanded[full_col.lower()] = final_col
    return expanded


# ==============================
# Build mapping and data bundle
# ==============================

def build_full_mapping(mapping_file_path, sats_file_like, wave_label="", sheet_name=None):
    """
    wave_label is provided by the UI and used to set Wave and HIDE Help (year).
    """
    standard_mappings, pattern_mappings_1, pattern_mappings_2, final_column_order = parse_data_mapping(mapping_file_path)

    df, chosen_sheet = _load_sats_dataframe(sats_file_like, sheet_name=sheet_name)
    col_index = build_column_index(df)

    dest_codes = extract_destination_codes(df)
    expanded_1 = expand_pattern_columns(pattern_mappings_1, dest_codes)
    expanded_2 = expand_pattern_columns(pattern_mappings_2, dest_codes)

    full_mapping = {}
    full_mapping.update({k: v for k, v in standard_mappings.items()})
    full_mapping.update(expanded_1)
    full_mapping.update(expanded_2)

    # Derive year from wave text like "June 2025"
    m_year = re.search(r"\b(20\d{2})\b", wave_label or "")
    survey_year = m_year.group(1) if m_year else ""

    for extra in ["Wave", "HIDE Help", "CITY_EVAL"]:
        if extra not in final_column_order:
            final_column_order.append(extra)

    original_to_final_groups = (expanded_1, expanded_2)

    return full_mapping, dest_codes, df, list(df.columns), final_column_order, original_to_final_groups, wave_label, survey_year, col_index


# =========================
# Markers parsing helpers
# =========================

def _get_marker_label_from_blob(markers_str: str, label_type: str, n: int):
    """
    Extract the raw substring after the slash.
    Examples:
      "... ,/D Apollo Destination 1/Philadelphia__Pennsylvania, ..." -> "Philadelphia__Pennsylvania"
      "... ,/D Apollo State 1/Illinois, ..."                        -> "Illinois"
    No cleaning.
    """
    if not isinstance(markers_str, str) or not markers_str:
        return pd.NA
    pat = rf"(?:^|,)\s*/?[^,]*\b{label_type}\s*0*{n}/([^,]+)"
    m = re.search(pat, markers_str, flags=re.IGNORECASE)
    return m.group(1).strip() if m else pd.NA


def _get_marker_labels_from_columns(row: pd.Series, marker_cols: list):
    """Fallback for distributed marker columns. Returns raw strings if present."""
    def is_pos(v):
        s = str(v).strip()
        return s not in ("", "-", "nan", "None")

    positives = []
    for c in marker_cols:
        v = row.get(c, None)
        if is_pos(v):
            positives.append(str(v).strip())

    city_label = positives[0] if positives else pd.NA
    state_label = positives[1] if len(positives) > 1 else pd.NA
    return city_label, state_label


# ==================
# Reshape function
# ==================

def reshape_sats_data(df, full_map, dest_codes, final_column_order, original_to_final_groups, wave_label="", survey_year="", col_index=None):
    """Emit CITY rows first, then STATE rows. Fill CITY_EVAL from markers blob. No cleaning."""
    if col_index is None:
        col_index = build_column_index(df)

    mapping1, mapping2 = original_to_final_groups
    lower_to_actual = {c.lower(): c for c in df.columns}

    markers_blob_col = resolve_col("markers", col_index)
    marker_cols = [c for c in df.columns if re.match(r"(conditionsmarker_|marker_)", str(c), flags=re.IGNORECASE)]

    static_keys = [k for k in full_map.keys() if not re.search(r"lr\d+", str(k), flags=re.IGNORECASE)]

    city_records = []
    state_records = []

    for _, row in df.iterrows():
        found_dests_1 = set()
        found_dests_2 = set()

        for original_lower in mapping1.keys():
            actual = lower_to_actual.get(original_lower)
            if actual is not None and pd.notna(row.get(actual, None)):
                m = re.search(r"(lr\d+)", original_lower, flags=re.IGNORECASE)
                if m:
                    found_dests_1.add(m.group(1).lower())

        for original_lower in mapping2.keys():
            actual = lower_to_actual.get(original_lower)
            if actual is not None and pd.notna(row.get(actual, None)):
                m = re.search(r"(lr\d+)", original_lower, flags=re.IGNORECASE)
                if m:
                    found_dests_2.add(m.group(1).lower())

        static_row = {}
        for src in static_keys:
            actual_src = resolve_col(src, col_index)
            if actual_src is not None:
                val = row.get(actual_src, None)
                if pd.notna(val):
                    static_row[full_map[src]] = val

        static_row["Wave"] = wave_label or pd.NA
        static_row["HIDE Help"] = survey_year or pd.NA

        if markers_blob_col:
            markers_str = str(row.get(markers_blob_col, "")) if pd.notna(row.get(markers_blob_col, None)) else ""
            get_city_label = lambda: _get_marker_label_from_blob(markers_str, "Destination", 1)
            get_state_label = lambda: _get_marker_label_from_blob(markers_str, "State", 1)
        else:
            _city_fallback, _state_fallback = _get_marker_labels_from_columns(row, marker_cols)
            get_city_label = lambda: _city_fallback
            get_state_label = lambda: _state_fallback

        for dest in sorted(found_dests_1, key=lambda s: int(re.search(r"\d+", s).group(0))):
            row_out = static_row.copy()
            for original_lower, final in mapping1.items():
                target_lower = re.sub(r"lr\d+", dest, original_lower, flags=re.IGNORECASE)
                actual = lower_to_actual.get(target_lower)
                if actual is not None:
                    val = row.get(actual, None)
                    if pd.notna(val):
                        row_out[final] = val
            row_out["CITY_EVAL"] = get_city_label()
            for col in final_column_order:
                row_out.setdefault(col, pd.NA)
            city_records.append(row_out)

        for dest in sorted(found_dests_2, key=lambda s: int(re.search(r"\d+", s).group(0))):
            row_out = static_row.copy()
            for original_lower, final in mapping2.items():
                target_lower = re.sub(r"lr\d+", dest, original_lower, flags=re.IGNORECASE)
                actual = lower_to_actual.get(target_lower)
                if actual is not None:
                    val = row.get(actual, None)
                    if pd.notna(val):
                        row_out[final] = val
            row_out["CITY_EVAL"] = get_state_label()
            for col in final_column_order:
                row_out.setdefault(col, pd.NA)
            state_records.append(row_out)

    reshaped_df = pd.DataFrame(city_records + state_records)
    reshaped_df = reshaped_df.reindex(columns=list(dict.fromkeys(final_column_order)))
    return reshaped_df


# ===============
# Streamlit UI
# ===============
st.title("SATS+ Reshaper")
st.caption("Upload SATS data, type the Wave, download a clean output.")

with st.form("sats_form"):
    sats_file = st.file_uploader("SATS datafile (xlsx or xls). Must include a 'markers' column", type=["xlsx", "xls"])
    wave_input = st.text_input("Wave (for example 'June 2025')", value="")
    submitted = st.form_submit_button("Process")

if submitted:
    if not sats_file:
        st.error("Please upload the SATS datafile.")
        st.stop()
    if not wave_input.strip():
        st.error("Wave is required.")
        st.stop()
    if not os.path.exists(MAPPING_PATH):
        st.error(f"Mapping file not found at '{MAPPING_PATH}'. Set MAPPING_PATH env var or place DataMapping.xlsx in the app folder.")
        st.stop()

    fm, dest_codes, df_raw, cols_raw, final_order, groups, wave_label, survey_year, col_index = build_full_mapping(
        MAPPING_PATH, sats_file, wave_input.strip()
    )

    out_df = reshape_sats_data(df_raw, fm, dest_codes, final_order, groups, wave_label, survey_year, col_index)

    safe_wave = re.sub(r"[^\w\s\-\.]", "", wave_label).strip()
    out_name = f"SATS+ {safe_wave}.xlsx" if safe_wave else "SATS+ Output.xlsx"

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xw:
        out_df.to_excel(xw, index=False, sheet_name="SATS+")
    st.download_button("Download Output Excel", data=buf.getvalue(), file_name=out_name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    st.write("Preview")
    st.dataframe(out_df.head(10))