import os
import re
import pandas as pd


# ------------------------
# Helpers, column resolver
# ------------------------

def build_column_index(df: pd.DataFrame):
    """Build fast, safe resolution from requested names to actual df columns.
    Exact match wins. If lowercase is unique, allow case-insensitive match.
    If lowercase is ambiguous (e.g., both Q2 and q2 exist), require exact case.
    """
    exact = set(df.columns)
    lower_map = {}
    collisions = set()

    for c in df.columns:
        key = c.lower()
        if key in lower_map and lower_map[key] != c:
            collisions.add(key)
        else:
            lower_map[key] = c

    # Remove ambiguous keys from lower_map
    for k in collisions:
        lower_map.pop(k, None)

    return {"exact": exact, "lower_map": lower_map, "collisions": collisions}


def resolve_col(name: str, col_index):
    """Resolve a requested column name to the actual df column."""
    if name in col_index["exact"]:
        return name
    key = name.lower()
    return col_index["lower_map"].get(key, None)


def get_cell(row: pd.Series, col_name: str, col_index):
    """Safely get a value by resolving the column name."""
    actual = resolve_col(col_name, col_index)
    if actual is None:
        return None
    return row.get(actual, None)


# ------------------------
# Mapping file parsing
# ------------------------

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
                # Keep original case, resolver will handle matching
                standard_mappings[original_1] = final

        # ORIGINAL_2
        original_2 = row.get("ORIGINAL_2")
        if pd.notna(original_2) and not str(original_2).startswith("["):
            original_2 = str(original_2).strip()
            if re.search(r"(LrNr|\{N\})", original_2, flags=re.IGNORECASE):
                pattern_mappings_2.append({"pattern": original_2, "final_column": final})
            else:
                standard_mappings[original_2] = final

    return standard_mappings, pattern_mappings_1, pattern_mappings_2, final_column_order


# ------------------------
# Destination utilities
# ------------------------

def extract_destination_codes(df: pd.DataFrame):
    dest_pattern = re.compile(r"_lr(\d+)", re.IGNORECASE)
    dest_nums = set()
    for col in df.columns:
        m = dest_pattern.search(col)
        if m:
            dest_nums.add(int(m.group(1)))
    return [f"lr{n}" for n in sorted(dest_nums)]  # lr1, lr2, lr3, ...


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


# ------------------------
# Build mapping bundle
# ------------------------

def build_full_mapping(mapping_file_path, sats_file_path, sheet_name=0):
    standard_mappings, pattern_mappings_1, pattern_mappings_2, final_column_order = parse_data_mapping(mapping_file_path)

    df = pd.read_excel(sats_file_path, sheet_name=sheet_name)  # keep original case
    col_index = build_column_index(df)

    dest_codes = extract_destination_codes(df)
    expanded_1 = expand_pattern_columns(pattern_mappings_1, dest_codes)
    expanded_2 = expand_pattern_columns(pattern_mappings_2, dest_codes)

    # Merge for reference only
    full_mapping = {}
    full_mapping.update({k: v for k, v in standard_mappings.items()})
    full_mapping.update(expanded_1)
    full_mapping.update(expanded_2)

    # Extract wave and year from file name
    base_name = os.path.basename(sats_file_path)
    wave_label = ""
    survey_year = ""

    wave_match = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
                           base_name, flags=re.IGNORECASE)
    year_match = re.search(r"\b(20\d{2})\b", base_name)

    if wave_match:
        wave_label = wave_match.group(0).title()
    if year_match:
        survey_year = year_match.group(1)

    # Ensure metadata columns in final order
    for extra in ["Wave", "HIDE Help", "CITY_EVAL"]:
        if extra not in final_column_order:
            final_column_order.append(extra)

    original_to_final_groups = (expanded_1, expanded_2)

    return full_mapping, dest_codes, df, list(df.columns), final_column_order, original_to_final_groups, wave_label, survey_year, col_index


# ------------------------
# Markers parsing
# ------------------------

def _get_marker_label(markers_str, label_type, n):
    # label_type is "Destination" or "State", format is "<type> n/<value>"
    if not isinstance(markers_str, str) or not markers_str:
        return pd.NA
    m = re.search(rf"{label_type}\s*{n}/([^,]+)", markers_str, flags=re.IGNORECASE)
    return m.group(1).strip() if m else pd.NA


# ------------------------
# Reshape
# ------------------------

def reshape_sats_data(df, full_map, dest_codes, final_column_order, original_to_final_groups, wave_label="", survey_year="", col_index=None):
    import re
    import pandas as pd

    if col_index is None:
        # Build the resolver if not provided
        def build_column_index(df: pd.DataFrame):
            exact = set(df.columns)
            lower_map = {}
            collisions = set()
            for c in df.columns:
                key = c.lower()
                if key in lower_map and lower_map[key] != c:
                    collisions.add(key)
                else:
                    lower_map[key] = c
            for k in collisions:
                lower_map.pop(k, None)
            return {"exact": exact, "lower_map": lower_map, "collisions": collisions}

        col_index = build_column_index(df)

    def resolve_col(name: str, col_index):
        if name in col_index["exact"]:
            return name
        return col_index["lower_map"].get(name.lower(), None)

    # Split mapping groups
    mapping1, mapping2 = original_to_final_groups  # mapping1 = CITY group, mapping2 = STATE group

    # Helper to find actual column by lowercase key
    lower_to_actual = {c.lower(): c for c in df.columns}

    # Try to resolve 'markers' column case-insensitively
    markers_col = resolve_col("markers", col_index)

    # Static source keys are those without lr-dest tokens
    static_keys = [k for k in full_map.keys() if not re.search(r"lr\d+", str(k), flags=re.IGNORECASE)]

    def _get_marker_label(markers_str, label_type, n):
        if not isinstance(markers_str, str) or not markers_str:
            return pd.NA
        m = re.search(rf"{label_type}\s*{n}/([^,]+)", markers_str, flags=re.IGNORECASE)
        return m.group(1).strip() if m else pd.NA

    city_records = []
    state_records = []

    # Iterate respondents in file order
    for _, row in df.iterrows():
        # Determine which destinations are present for each group
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

        # Build static fields once
        static_row = {}
        for src in static_keys:
            actual_src = resolve_col(src, col_index)
            if actual_src is not None:
                val = row.get(actual_src, None)
                if pd.notna(val):
                    static_row[full_map[src]] = val

        # Metadata
        static_row["Wave"] = wave_label or pd.NA
        static_row["HIDE Help"] = survey_year or pd.NA

        markers_str = str(row.get(markers_col, "")) if markers_col else ""

        # CITY block first (group 1)
        for dest in sorted(found_dests_1, key=lambda s: int(re.search(r"\d+", s).group(0))):
            n = int(re.search(r"\d+", dest).group(0))
            row_out = static_row.copy()

            for original_lower, final in mapping1.items():
                target_lower = re.sub(r"lr\d+", dest, original_lower, flags=re.IGNORECASE)
                actual = lower_to_actual.get(target_lower)
                if actual is not None:
                    val = row.get(actual, None)
                    if pd.notna(val):
                        row_out[final] = val

            row_out["CITY_EVAL"] = _get_marker_label(markers_str, "Destination", n)
            for col in final_column_order:
                row_out.setdefault(col, pd.NA)
            city_records.append(row_out)

        # STATE block second (group 2)
        for dest in sorted(found_dests_2, key=lambda s: int(re.search(r"\d+", s).group(0))):
            n = int(re.search(r"\d+", dest).group(0))
            row_out = static_row.copy()

            for original_lower, final in mapping2.items():
                target_lower = re.sub(r"lr\d+", dest, original_lower, flags=re.IGNORECASE)
                actual = lower_to_actual.get(target_lower)
                if actual is not None:
                    val = row.get(actual, None)
                    if pd.notna(val):
                        row_out[final] = val

            # Per your instruction, write STATE label into CITY_EVAL as well
            row_out["CITY_EVAL"] = _get_marker_label(markers_str, "State", n)
            for col in final_column_order:
                row_out.setdefault(col, pd.NA)
            state_records.append(row_out)

    # Concatenate CITY section first, then STATE section for the whole file
    records = city_records + state_records
    reshaped_df = pd.DataFrame(records)

    # Keep expected columns, including Wave, HIDE Help, CITY_EVAL
    reshaped_df = reshaped_df.reindex(columns=list(dict.fromkeys(final_column_order)))
    return reshaped_df


# ------------------------
# Main
# ------------------------

if __name__ == "__main__":
    mapping_file = "DataMapping.xlsx"
    sats_file = "SATS JUNE 2025 example.xlsx"

    print("Building full mapping and loading data...")
    full_map, dest_codes, df, all_columns, final_column_order, original_to_final_groups, wave_label, survey_year, col_index = build_full_mapping(
        mapping_file, sats_file
    )

    print(f"Detected {len(dest_codes)} destination codes: {dest_codes[:10]}{'...' if len(dest_codes) > 10 else ''}")

    print("Reshaping data...")
    reshaped_df = reshape_sats_data(
        df, full_map, dest_codes, final_column_order, original_to_final_groups, wave_label, survey_year, col_index
    )

    print(f"Reshaped data: {reshaped_df.shape[0]} rows, {reshaped_df.shape[1]} columns")
    safe_wave_label = wave_label.replace(" ", "_") if wave_label else "output"
    output_file = f"{wave_label} SATS+ Output.xlsx" if wave_label else "SATS_final_output.xlsx"
    reshaped_df.to_excel(output_file, index=False)
    print(f"Output written to: {output_file}")