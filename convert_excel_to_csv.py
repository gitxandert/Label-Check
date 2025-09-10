import logging
import os

import openpyxl
import pandas as pd

# --- Basic Logging Configuration ---
# This sets up logging to print messages to your console.
# The format includes the timestamp, the logging level (e.g., INFO, ERROR), and the message.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- Configuration ---
folder_path = r"C:\Users\thakursp\Documents\Work\Label-Check\sorted"


def clean_column_names(columns):
    """
    Remove all double quotes, single quotes, and leading/trailing whitespace from column names.
    Replace empty or None column names with a placeholder.
    """
    cleaned = []
    logging.debug(f"Original columns for cleaning: {columns}")
    for i, col in enumerate(columns):
        # Ensure we're working with a string
        col_str = str(col).replace('"', "").replace("'", "").strip()
        if not col_str or col_str.lower() == "none":
            col_str = f"unnamed_{i + 1}"
            logging.warning(
                f"Found empty column name at index {i}, replaced with '{col_str}'"
            )
        cleaned.append(col_str)
    logging.debug(f"Cleaned columns: {cleaned}")
    return cleaned


def parse_sadap_row(row):
    """
    For sadap as annotator, split the row using ';' as separator,
    but only for the first 5 semicolons (i.e., split into 6 columns).
    The rest of the row (after the 5th ;) is kept as the last column.
    """
    if row is None:
        return [""] * 6
    row_str = str(row)
    parts = row_str.split(";", 5)
    # Ensure the list always has 6 elements, padding with empty strings if necessary
    while len(parts) < 6:
        parts.append("")
    return parts


def read_sadap_xlsx(filepath):
    """
    Read sadap's xlsx file row by row, splitting each row as per the custom rule.
    Returns a DataFrame.
    """
    logging.info(
        f"--- Starting custom 'sadap' parser for: {os.path.basename(filepath)} ---"
    )
    wb = openpyxl.load_workbook(filepath)
    ws = wb.active
    data = []
    header = None
    # Iterate through all rows in the active worksheet
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        # The first row (index 0) is assumed to be the header
        if i == 0:
            if row and row[0] is not None:
                logging.debug(f"Raw header string from cell A1: '{row[0]}'")
                header = parse_sadap_row(row[0])
                logging.info(f"Parsed header: {header}")
                header = clean_column_names(header)
                logging.info(f"Cleaned header: {header}")
            else:
                # If the header row is empty, we can't proceed.
                logging.error(
                    f"Header row is empty or missing in {filepath}. Cannot create DataFrame."
                )
                raise ValueError(f"No header found in {filepath}")
            continue  # Move to the next row after processing the header

        # Process data rows
        if row and row[0] is not None:
            parsed_data_row = parse_sadap_row(row[0])
            # Log the first few data rows to help debug parsing issues
            if len(data) < 3:
                logging.debug(f"Parsed data row {i}: {parsed_data_row}")
            data.append(parsed_data_row)

    if header is None:
        raise ValueError(f"Header could not be determined for {filepath}")

    df = pd.DataFrame(data, columns=header)
    logging.info(
        f"--- Finished custom 'sadap' parser. Created DataFrame with shape {df.shape}. ---"
    )
    return df


def process_files(folder_path):
    """
    Processes all .xlsx files in a folder, applying the correct parsing logic for each.
    """
    all_dfs = []
    logging.info(f"Starting to process files in: {folder_path}")

    files_to_process = [f for f in os.listdir(folder_path) if f.endswith(".xlsx")]
    if not files_to_process:
        logging.warning("No .xlsx files found in the specified folder.")
        return all_dfs

    for filename in files_to_process:
        excel_path = os.path.join(folder_path, filename)
        logging.info(
            f"==================== Processing file: {filename} ===================="
        )

        try:
            # Determine annotator from filename
            base = os.path.splitext(filename)[0]
            annotator = base.split("_")[0] if "_" in base else base
            logging.info(f"Determined annotator: '{annotator}'")

            # --- CORE LOGIC FIX: Decide parser based on annotator ---
            if annotator.lower() == "sadap":
                logging.info("Annotator is 'sadap'. Using custom XLSX parser.")
                df = read_sadap_xlsx(excel_path)
            else:
                logging.info("Using standard pandas parser for this file.")
                try:
                    df = pd.read_excel(excel_path)
                except Exception as e_xlsx:
                    logging.warning(
                        f"Could not read '{filename}' as a standard Excel file ({e_xlsx}). Attempting to read as CSV with ';' separator."
                    )
                    try:
                        # Some Excel files are actually CSVs with an .xlsx extension
                        df = pd.read_csv(excel_path, sep=";")
                    except Exception as e_csv:
                        logging.error(
                            f"Failed to load '{filename}' as Excel or as CSV. Skipping file. Error: {e_csv}"
                        )
                        continue

            # --- Common processing for all successfully loaded dataframes ---
            logging.info(
                f"Successfully loaded '{filename}'. Shape: {df.shape}. Original columns: {list(df.columns)}"
            )

            # Clean column names and add the annotator column
            df.columns = clean_column_names(df.columns)
            df.insert(0, "annotator", annotator)

            logging.info(f"Final columns for '{filename}': {list(df.columns)}")
            all_dfs.append(df)

        except Exception:
            logging.error(
                f"An unexpected error occurred while processing {filename}. Skipping file.",
                exc_info=True,
            )
            # exc_info=True will print the full error traceback for better debugging

    return all_dfs


def check_and_save(all_dfs, folder_path):
    """
    Checks for column consistency across all DataFrames and saves a combined CSV.
    Attempts to align columns if they do not match.
    """
    if not all_dfs:
        logging.warning("No dataframes were successfully loaded. Nothing to save.")
        return

    logging.info("All files processed. Now checking for column consistency.")

    # Use the columns of the first dataframe as the reference
    first_cols = all_dfs[0].columns
    mismatches = []

    # Compare each subsequent dataframe's columns to the first one
    for i, df in enumerate(all_dfs[1:], start=1):  # Start from the second df
        if not df.columns.equals(first_cols):
            logging.warning(
                f"Column mismatch found for DataFrame {i + 1} (from file index {i})."
            )
            mismatches.append((i, df.columns, first_cols))

    if mismatches:
        logging.error(
            "CRITICAL: Not all files have the same columns and order. Mismatches found:"
        )
        for idx, cols, ref_cols in mismatches:
            print(f"\nFile index {idx} has columns: {list(cols)}")
            print(f"Reference columns (from first file): {list(ref_cols)}")
            missing = set(ref_cols) - set(cols)
            extra = set(cols) - set(ref_cols)
            if missing:
                print(f"  - Missing columns: {list(missing)}")
            if extra:
                print(f"  - Extra columns: {list(extra)}")

        logging.info("Attempting to auto-fix by aligning all columns...")

        # Create a master list of all unique column names from all dataframes
        all_colnames_set = set()
        for df in all_dfs:
            all_colnames_set.update(df.columns)

        # Create a sorted list for consistent ordering, putting 'annotator' first
        all_colnames = sorted(list(all_colnames_set))
        if "annotator" in all_colnames:
            all_colnames.remove("annotator")
            all_colnames.insert(0, "annotator")

        logging.info(f"Master list of columns for alignment: {all_colnames}")

        fixed_dfs = []
        for i, df in enumerate(all_dfs):
            # Reindex the dataframe to match the master column list.
            # This adds missing columns (with NaN values) and removes extra ones.
            df_reindexed = df.reindex(columns=all_colnames, fill_value="")
            fixed_dfs.append(df_reindexed)

        combined_df = pd.concat(fixed_dfs, ignore_index=True)
        output_filename = "combined_auto_aligned.csv"
        logging.info(
            f"Column alignment complete. Concatenated all data. Final shape: {combined_df.shape}"
        )

    else:
        logging.info("Success! All files have identical columns. Concatenating now.")
        combined_df = pd.concat(all_dfs, ignore_index=True)
        output_filename = "combined.csv"
        logging.info(f"Concatenation complete. Final shape: {combined_df.shape}")

    # Final save
    combined_csv_path = os.path.join(folder_path, output_filename)
    try:
        combined_df.to_csv(combined_csv_path, index=False, sep=";")
        logging.info(f"Successfully saved combined data to: {combined_csv_path}")
    except Exception:
        logging.error("Failed to save the final CSV file.", exc_info=True)


if __name__ == "__main__":
    logging.info("Script starting...")
    # The main logic is now consolidated into these two functions
    all_loaded_dfs = process_files(folder_path)
    check_and_save(all_loaded_dfs, folder_path)
    logging.info("Script finished.")
