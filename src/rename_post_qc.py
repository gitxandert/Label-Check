"""
This script takes an input CSV file produced from the Label-Check pipeline (post-QC), extracts the accession ID, stain, block number, and original file paths, and creates new file paths according to guidelines in IuCompPath/Wiki/Digital_Pathology/Database_construction_and_management/PseudoID_conventions.md.

For the time being, this script assumes that it is working with only two types of tissue: brain and breast. The tissue is labelled as BRAIN if the accession ID begins with "NP"; otherwise, it is labelled as BRST.

This script also creates a PID column that assigns unique patient identifiers starting from the end of the current brain mastersheet. Currently, the brain mastersheet ends at 'AAAHQF'; this value can be edited in the PID.pid variable below.

Finally, this script renames the original files to the newly-generated file paths. Both the original and new file paths are retained in the resulting CSV.
"""

import sys
import re
import argparse
from pathlib import Path

import pandas as pd

class PID:
    pid = 'AAAHZD'
    prev = {}

    def __init__(self, instance):
        self.instance = instance


def assign_pid(x: str) -> str:
    pid = PID.pid

    if x not in PID.prev:
        reverse = list(pid[::-1])
        for i in range(len(reverse)):
            cur = reverse[i]
            if cur == 'Z':
                reverse[i] = 'A'
            else:
                reverse[i] = chr(ord(cur) + 1)
                break
        reverse = "".join(reverse)
        pid = reverse[::-1]
        PID.pid = pid
        PID.prev[x] = pid
        print(f"\tAssigned {pid} to {x}")
    else:
        print(f"\tPID for {x} = {pid}")

    return PID.prev[x]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rename files using CSV produced from the fourth step of the Label-Check pipeline (app.py)"
    )
    parser.add_argument(
        "input_csv",
        type=Path,
        help="CSV produced by step three of Label-Check pipeline and edited by step four (QC)",
    )
    parser.add_argument(
        "--dry",
        required=False,
        help="Conduct a 'dry run', where files are only renamed in the output csv (post_qc.csv)",
        action="store_true"
    )

    args = parser.parse_args()

    dry_run = args.dry
    if dry_run:
        print("\x1b[1mExecuting dry run; no files will be renamed.\x1b[0m\n")

    post_qc = pd.read_csv(args.input_csv)

    fields = ['AccessionID', 'Stain', 'BlockNumber', 'original_slide_path']

    # check that input CSV has the above columns and populate the new CSV with these columns
    final = pd.DataFrame()
    for i in range(len(fields)):
        f = fields[i]
        if f not in post_qc.columns:
            print(f"ERR: input CSV does not have a {f} field")
            print("Make sure that your CSV has been produced by the Label-Check pipeline and edited via the flask app")
            sys.exit(1)

        final.insert(i, fields[i], post_qc.pop(fields[i]))

    # create new paths
    final.insert(final.columns.get_loc('original_slide_path') + 1, 'new_slide_path', None)
    num_changed = 0
    unique_names = {}
    for i in range(len(final)):
        print(f"Renaming {final.loc[i, 'original_slide_path']}:")
        # generate PID and assign organ type;
        # for now, organ type in this script is determined by whether the accession ID
        # has NP or A at the beginning; if yes, organ is BRAIN, if no, organ is unlabelled
        a_id = final.loc[i, 'AccessionID']
        if not isinstance(a_id, str):
            print(f"\tERR: {a_id} is not a str; cannot assign PID")
            final.loc[i, 'PID'] = "XXXXXX"
            continue

        pid = assign_pid(a_id)
        final.loc[i, 'PID'] = pid

        try:
            org_match = re.match(r"[a-zA-Z]+", a_id)
            org_class = org_match.group() if org_match else ""
            if org_class in {"NP", "A"}:
                organ = "BRAIN"
            else:
                organ = "UNKWN"
        except Exception as e:
            print(f"	ERR -- unable to parse accession ID for {pid}: {e}")
            organ = "UNKWN"

        o_path = Path(final.loc[i, 'original_slide_path'])
        o_parent = o_path.parent
        o_ext = o_path.suffix
        
        stain = final.loc[i, 'Stain']
        block = final.loc[i, 'BlockNumber']

        # if no stain or block, replace with "XX"
        if not isinstance(stain, str):
            print(f"	ERR: stain for {pid} is not a string; cannot rename file")
            continue
        elif '$' in stain:
            stain = "XX"

        if not isinstance(block, str):
            print(f"	ERR: block number for {pid} is not a string; cannot rename file")
            continue
        elif '#' in stain:
            block = "XX"
       
        # for now, ImageType and SampleAcqType are assumed;
        # SectionCount is determined by naming conflicts
        cur_name = f"{organ}_{pid}_XXXXXXXX_XXXX_{stain}_WSI_RE{block}"
        if cur_name in unique_names:
            unique_names[cur_name] += 1
        else:
            unique_names[cur_name] = 0
        section_count = unique_names[cur_name]
        new_name = f"{cur_name}{section_count:03d}{o_ext}"
        new_path = o_parent / new_name
       
        # make sure path exists before renaming
        if o_path.exists():
            # only rename if not a dry run
            if not dry_run:
                o_path.rename(new_path)
            num_changed += 1
            final.loc[i, 'new_slide_path'] = new_path
            print(f"\tRenamed {o_path} to {new_path}")
        else:
            # if the original path doesn't exist, record the new name anyway
            print(f"	ERR: {o_path} doesn't exist")
            final.loc[i, 'new_slide_path'] = new_name
            continue

    # write new CSV to post_qc.csv in the QC directory
    parent = args.input_csv.parent
    post_qc = parent / "post_qc.csv"
    final.to_csv(post_qc, index=False)
    print(f"\nChanged {num_changed} file paths")
    print(f"Wrote new CSV to {post_qc}")
