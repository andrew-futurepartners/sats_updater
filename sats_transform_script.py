import pandas as pd
import re

def parse_data_mapping(mapping_file_path):
    df = pd.read_excel(mapping_file_path)
    df = df.dropna(subset=["FINAL"])  # Keep rows with FINAL populated

    standard_mappings = {}
    pattern_mappings_1 = []
    pattern_mappings_2 = []
    final_column_order = df["FINAL"].dropna().unique().tolist()

    for _, row in df.iterrows():
        final = str(row["FINAL"]).strip()
        if final not in final_column_order:
            final_column_order.append(final)

        # ORIGINAL_1
        original_1 = row.get("ORIGINAL_1")
        if pd.notna(original_1) and not str(original_1).startswith("["):
            original_1 = str(original_1).strip()
            if re.search(r"(LrNr|{N})", original_1, flags=re.IGNORECASE):
                pattern_mappings_1.append({"pattern": original_1, "final_column": final})
            else:
                standard_mappings[original_1.lower()] = final

        # ORIGINAL_2
        original_2 = row.get("ORIGINAL_2")
        if pd.notna(original_2) and not str(original_2).startswith("["):
            original_2 = str(original_2).strip()
            if re.search(r"(LrNr|{N})", original_2, flags=re.IGNORECASE):
                pattern_mappings_2.append({"pattern": original_2, "final_column": final})
            else:
                standard_mappings[original_2.lower()] = final

    return standard_mappings, pattern_mappings_1, pattern_mappings_2, final_column_order


def extract_destination_codes(df):
    dest_pattern = re.compile(r"_lr(\d+)", re.IGNORECASE)
    dest_codes = set()

    for col in df.columns:
        match = dest_pattern.search(col)
        if match:
            dest_codes.add(f"lr{match.group(1)}")  # Always lowercase
    return sorted(dest_codes)


def expand_pattern_columns(pattern_mappings, dest_codes):
    expanded = {}

    for entry in pattern_mappings:
        pattern = entry["pattern"]
        final_col = entry["final_column"]

        match = re.match(r"(Q\d+)_Lr(Nr|\{N\})(r(\d+))?", pattern, re.IGNORECASE)
        if not match:
            continue

        base_q = match.group(1)
        r_full = match.group(3)  # e.g., 'r1'
        r_num = match.group(4)   # e.g., '1'

        for dest in dest_codes:
            full_col = f"{base_q}_{dest}"
            if r_full:
                full_col += r_full
            expanded[full_col.lower()] = final_col

    return expanded

def build_full_mapping(mapping_file_path, sats_file_path, sheet_name=0):
    standard_mappings, pattern_mappings_1, pattern_mappings_2, final_column_order = parse_data_mapping(mapping_file_path)

    df = pd.read_excel(sats_file_path, sheet_name=sheet_name)
    df.columns = [col.strip().lower() for col in df.columns]

    dest_codes = extract_destination_codes(df)
    expanded_1 = expand_pattern_columns(pattern_mappings_1, dest_codes)
    expanded_2 = expand_pattern_columns(pattern_mappings_2, dest_codes)

    # Merge everything into one master mapping for consistency
    full_mapping = {**standard_mappings, **expanded_1, **expanded_2}

    original_to_final_groups = (expanded_1, expanded_2)

    return full_mapping, dest_codes, df, df.columns.tolist(), final_column_order, original_to_final_groups

def reshape_sats_data(df, full_map, dest_codes, final_column_order, original_to_final_groups):
    import re
    records = []

    mapping1, mapping2 = original_to_final_groups
    df.columns = [col.strip().lower() for col in df.columns]

    for _, row in df.iterrows():
        # 🔍 Extract destination names from the markers column
        markers = str(row.get("markers", ""))
        dest_city = None
        dest_state = None

        city_match = re.search(r"Destination 1/([^,]+)", markers)
        state_match = re.search(r"State 1/([^,]+)", markers)

        if city_match:
            dest_city = city_match.group(1).strip()
        if state_match:
            dest_state = state_match.group(1).strip()

        # 🔁 Track found destinations from data columns
        found_dests_1 = set()
        found_dests_2 = set()

        for original, final in mapping1.items():
            match = re.search(r"(lr\d+)", original, re.IGNORECASE)
            if match:
                dest_tag = match.group(1).lower()
                colname = original.lower()
                if colname in df.columns and pd.notna(row[colname]):
                    found_dests_1.add(dest_tag)

        for original, final in mapping2.items():
            match = re.search(r"(lr\d+)", original, re.IGNORECASE)
            if match:
                dest_tag = match.group(1).lower()
                colname = original.lower()
                if colname in df.columns and pd.notna(row[colname]):
                    found_dests_2.add(dest_tag)

        # 🧱 Shared demographic/static fields (no Lr tag)
        static_row_data = {}
        for col in df.columns:
            if col in full_map and not re.search(r"lr\d+", col):
                static_row_data[full_map[col]] = row[col]

        # 🏙 City destination records
        for dest in found_dests_1:
            row_data = static_row_data.copy()
            for original, final in mapping1.items():
                target_col = re.sub(r"lr\d+", dest, original.lower())
                if target_col in df.columns and pd.notna(row[target_col]):
                    row_data[final] = row[target_col]
            row_data["CITY_EVAL"] = dest_city or pd.NA
            for final_col in final_column_order:
                row_data.setdefault(final_col, pd.NA)
            records.append(row_data)

        # 🏛 State destination records
        for dest in found_dests_2:
            row_data = static_row_data.copy()
            for original, final in mapping2.items():
                target_col = re.sub(r"lr\d+", dest, original.lower())
                if target_col in df.columns and pd.notna(row[target_col]):
                    row_data[final] = row[target_col]
            row_data["CITY_EVAL"] = dest_state or pd.NA
            for final_col in final_column_order:
                row_data.setdefault(final_col, pd.NA)
            records.append(row_data)

    reshaped_df = pd.DataFrame(records)
    reshaped_df = reshaped_df.reindex(columns=final_column_order)
    return reshaped_df

if __name__ == "__main__":
    mapping_file = "DataMapping.xlsx"
    sats_file = "SATS original example.xlsx"

    print("🔄 Building full mapping and loading data...")
    full_map, dest_codes, df, all_columns, final_column_order, original_to_final_groups = build_full_mapping(mapping_file, sats_file)

    print(f"\n✅ Detected {len(dest_codes)} destination codes:")
    print(dest_codes)

    print(f"\n✅ Sample of final column mappings:")
    for raw_col, final_col in list(full_map.items())[:15]:
        print(f"  {raw_col} → {final_col}")

    print("\n🔄 Reshaping data...")
    reshaped_df = reshape_sats_data(df, full_map, dest_codes, final_column_order, original_to_final_groups)

    print(f"\n✅ Reshaped data: {reshaped_df.shape[0]} rows, {reshaped_df.shape[1]} columns")
    print(reshaped_df.head(3))

    output_file = "SATS_final_output.xlsx"
    reshaped_df.to_excel(output_file, index=False)
    print(f"\n💾 Output written to: {output_file}")