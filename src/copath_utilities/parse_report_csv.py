import argparse
import csv
import os
from pathlib import Path
from striprtf.striprtf import rtf_to_text

from copath_texttypes import TEXT_TYPES

REPORT_FIELDS = [ tt[1] for tt in TEXT_TYPES ]


def parse_field(field):
    if not isinstance(field, str):
        return field # Returns original value if it's NaN or Not a String
    
    # Optional: Only convert if the RTF header is present
    if field.startswith('{\\rtf1'):
        try:
            return rtf_to_text(field)
        except:
            return field
    
    return field # Return as-is if it's just plain text


def normalize_report_field(field):
    parsed = parse_field(field)
    if parsed is None:
        return ""

    return str(parsed).replace('\r', '\n').replace('\x00', '')


def combine_report_fields(row, report_fields):
    output = ""
    for field in report_fields:
        output += f"{field}:\n\n"
        output += f"{normalize_report_field(row[field])}\n\n########END FIELD########\n\n"

    return output


def parse_row_to_file(row, fieldnames, output_dir):
    # Iterate through every column found in the header
    output_name = row[fieldnames[0]]
    output_path = output_dir / f"{output_name}.txt"

    with open(output_path, "w") as outfile:
        output = ""
        for col in fieldnames:
            field = row[col]
            parsed = parse_field(field)
            output += f"{col}:\n\n"
            output += f"{parsed}\n\n########END FIELD########\n\n"
       
        # clean carriage returns and null bytes
        output = output.replace('\r', '\n').replace('\x00', '')
        outfile.write(output)

    print(f"Wrote content to {output_path}")


def check_empty(row):
    for cell in row.values():
        if cell != "":
            return False
    
    return True


def process_csv_to_combined_csv(infile, output_file):
    output_file.parent.mkdir(parents=True, exist_ok=True)

    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames
    if not fieldnames:
        raise ValueError("Input CSV does not contain a header row")

    report_fields = [field for field in REPORT_FIELDS if field in fieldnames]
    if not report_fields:
        raise ValueError(f"Input CSV has none of the expected report fields")

    output_fields = [field.strip() for field in fieldnames if field not in REPORT_FIELDS]
   
    header = output_fields + ["report"]

    success_count = 0
    row_count = 0

    with open(output_file, mode="w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=header)
        writer.writeheader()

        for row in reader:
            row_count += 1
            if check_empty(row):
                print(f"Row {row_count} is empty; skipping.")
                continue

            row_out = {
                field: row.get(field, "")
                for field in output_fields
            }
            row_out["report"] = combine_report_fields(row, report_fields)
            writer.writerow(row_out)
            success_count += 1

    print(f"Parsed {success_count}/{row_count} rows to {output_file}")


def process_csv(infile, output_dir):
    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        print(f"Error: {e}")
        return

    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames

    success_count = 0
    row_count = 0

    for row in reader:
        row_count += 1
        if check_empty(row):
            print(f"Row {row_count} is empty; skipping.")
            continue

        try:
            parse_row_to_file(row, fieldnames, output_dir)
            success_count += 1
        except Exception as e:
            print(f"Error: {e}")
            continue
    
    print(f"Parsed {success_count}/{row_count} rows to file")


def main():
    parser = argparse.ArgumentParser(description="Parses CSVs into formatted text files. Handles RTF.")
    parser.add_argument("input", help="Path to the CSV")
    parser.add_argument("-o", "--output_dir", help="Optional output dir")
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Output a CSV with report text combined into a single report column instead of TXT files",
    )
    parser.add_argument(
        "--output_csv",
        "--output-csv",
        help="Optional output CSV path for --csv mode",
    )
    args = parser.parse_args()

    input_file = args.input
    inpath = Path(args.input)

    try:
        with open(input_file, mode='r', encoding='utf-8-sig', newline='') as infile:
            if args.csv or args.output_csv is not None:
                if args.output_csv:
                    output_csv = Path(args.output_csv)
                elif args.output_dir:
                    output_csv = Path(args.output_dir) / f"{inpath.stem}_parsed.csv"
                else:
                    output_csv = Path(f"{inpath.stem}_parsed.csv")

                print(f"Processing {input_file} to {output_csv}")
                process_csv_to_combined_csv(infile, output_csv)
            else:
                output_dir = args.output_dir
                if output_dir is None:
                    inpath_name = inpath.stem
                    output_dir = Path(f"{inpath_name}_parsed")
                else:
                    output_dir = Path(output_dir)

                process_csv(infile, output_dir)
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
