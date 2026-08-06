"""Data preparation and persistence for post-QC slide naming."""

from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from copath_queue import QueueProtocolError, submit_query


ORGANS = ("BRAIN", "BREAST", "TESTIS", "OTHER", "UNKNOWN")
IMAGE_TYPES = ("WSI", "FNA", "XXX")
SAMPLE_TYPES = ("RE", "BP", "XX")
MAPPING_FIELDS = (
    "AccessionID", "Organ", "PID", "AccessionDate", "Timepoint", "Stain",
    "ImageType", "SampAcqType", "BlockNumber", "SectionCount", "OriginalPath",
    "NewName", "Approved",
)
DERIVED_COPATH_FIELDS = ("organ", "PID", "_accdate", "timepoint", "image_type", "_sampacqtype")
REPORT_FIELDS = (
    "final_diagnosis", "addendum_diagnosis", "note", "addendum_comment",
    "clinical_history", "clinical_summary", "microscopic_description", "gross_description",
    "preliminary_comment", "anatomic_preliminary_comment", "preliminary_diagnosis",
    "anatomic_preliminary_diagnosis", "pre_operative_diagnosis", "intraoperative_diagnosis",
    "intraoperative_diagnosis_detail", "post_operative_diagnosis", "results",
    "results_comments", "interpretation", "card_case_comment", "case_discussion",
    "conference_note", "hot_seat_diagnosis", "neuropathology_final_diagnosis",
    "neuropathology_diagnosis_comment", "neuropathology_addendum_diagnosis",
    "neuropathology_addendum_comment", "neuropathology_microscopic_description",
    "neuropathology_gross_description", "neuropathology_other_gross_description",
    "neuropathology_other_text", "neuropathology_preliminary_comment",
    "neuropathology_preliminary_diagnosis", "other_related_clinical_data",
    "other_diagnoses", "other_gross_description", "physician_notification",
    "cytology_review", "report_comments", "slide_block_description", "special_requests",
    "synoptic_worksheet", "abn", "ancillary_procedures", "operative_procedure",
    "postmortem_imaging_studies", "procedure_note",
)
COPATH_FIELDS = (
    "accession_id", "mrn", "specimen_id", "accession_date", "date_of_birth",
    "attending_physician", "specimen_class_id", "specimen_class_name",
    "sample_acquisition_type", "blocks_and_stains", "report", "signout_date",
    "specimen_status", "status_date", "report_status", "latest_report_status_datetime",
    "proc_or_add_status", "latest_procedure_addendum_signout_date",
    *DERIVED_COPATH_FIELDS,
)
IDENTIFIER_FIELDS = ("AccessionID", "Organ", "MRN", "PID")
LONGITUDINAL_LOG_FIELDS = ("AccessionID", "MRN", "Error")

ACCESSION_RE = re.compile(r"^[A-Z]{1,3}[0-9]{2}-[0-9]+$")
PID_RE = re.compile(r"^[A-Z]{6}$")
DATE_RE = re.compile(r"^(?:[0-9]{8}|XXXXXXXX)$")
TIMEPOINT_RE = re.compile(r"^[A-Z0-9]{4}$")
SECTION_RE = re.compile(r"^[0-9]{3}$")


class RenamingError(Exception):
    pass


def read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                raise RenamingError(f"{path.name} has no header row")
            return list(reader.fieldnames), [
                {key: (value or "") for key, value in row.items()} for row in reader
            ]
    except RenamingError:
        raise
    except (OSError, UnicodeError, csv.Error) as exc:
        raise RenamingError(f"could not read {path}: {exc}") from exc


def atomic_write(path: Path, fields: Sequence[str], rows: Iterable[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except OSError as exc:
        raise RenamingError(f"could not write {path}: {exc}") from exc
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def atomic_write_json(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def initialize_csv(path: Path, fields: Sequence[str]) -> None:
    """Atomically create a header-only CSV without replacing an existing file."""
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", newline="", encoding="utf-8") as handle:
            csv.DictWriter(handle, fieldnames=fields).writeheader()
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            pass
    except OSError as exc:
        raise RenamingError(f"could not initialize {path}: {exc}") from exc
    finally:
        temporary.unlink(missing_ok=True)


def initialize_clone(clone_root: Path) -> None:
    """Create CoPath CSVs and migrate legacy indexes/report columns."""
    for organ in ORGANS:
        path = clone_root / organ / "copath_data.csv"
        initialize_csv(path, COPATH_FIELDS)
        headers, rows = read_csv(path)
        collapsed_headers = collapse_report_headers(headers)
        if headers != collapsed_headers:
            atomic_write(
                path,
                collapsed_headers,
                (collapse_report_fields(row) for row in rows),
            )

    index_path = clone_root / "all_iuh_identifiers.csv"
    if index_path.exists():
        headers, rows = read_csv(index_path)
        if headers != list(IDENTIFIER_FIELDS):
            raise RenamingError(
                "all_iuh_identifiers.csv must contain AccessionID, Organ, MRN, PID"
            )
        _validate_identifiers(rows)
        _retire_legacy_identifier_indexes(clone_root)
        return

    migrated: Dict[str, Dict[str, str]] = {}
    legacy_path = clone_root / "all_accessions.csv"
    if legacy_path.exists():
        _, legacy_rows = read_csv(legacy_path)
        for row in legacy_rows:
            accession = row_accession(row)
            organ = row.get("Organ", "").strip().upper()
            if accession:
                migrated[accession] = {
                    "AccessionID": accession, "Organ": organ if organ in ORGANS else "UNKNOWN",
                    "MRN": row.get("MRN", "").strip(), "PID": row.get("PID", "").strip().upper(),
                }
    for organ in ORGANS:
        _, rows = read_csv(clone_root / organ / "copath_data.csv")
        for row in rows:
            accession = row_accession(row)
            if not accession:
                continue
            current = migrated.setdefault(accession, {
                "AccessionID": accession, "Organ": organ, "MRN": "", "PID": "",
            })
            current["Organ"] = current["Organ"] or organ
            current["MRN"] = current["MRN"] or row.get("mrn", "").strip()
            current["PID"] = current["PID"] or row.get("PID", "").strip().upper()
    rows = sorted(migrated.values(), key=lambda row: row["AccessionID"])
    _validate_identifiers(rows)
    atomic_write(index_path, IDENTIFIER_FIELDS, rows)
    _retire_legacy_identifier_indexes(clone_root)


def _retire_legacy_identifier_indexes(clone_root: Path) -> None:
    backup_root = clone_root / "migration_backups" / "identifier_indexes"
    for name in ("all_accessions.csv", "all_mrns.csv"):
        legacy = clone_root / name
        if not legacy.exists():
            continue
        backup_root.mkdir(parents=True, exist_ok=True)
        backup = backup_root / name
        if not backup.exists():
            shutil.copy2(legacy, backup)
        legacy.unlink()


def _validate_identifiers(rows: Sequence[Dict[str, str]]) -> None:
    accessions: set = set()
    mrn_organ: Dict[Tuple[str, str], str] = {}
    organ_pid: Dict[Tuple[str, str], str] = {}
    for row in rows:
        accession = row_accession(row)
        organ = row.get("Organ", "").strip().upper()
        mrn = row.get("MRN", "").strip()
        pid = row.get("PID", "").strip().upper()
        if not ACCESSION_RE.fullmatch(accession):
            raise RenamingError(f"invalid accession in all_iuh_identifiers.csv: {accession or '(blank)'}")
        if organ not in ORGANS:
            raise RenamingError(f"invalid organ in all_iuh_identifiers.csv: {organ or '(blank)'}")
        if pid and not PID_RE.fullmatch(pid):
            raise RenamingError(f"invalid PID in all_iuh_identifiers.csv: {pid}")
        if accession in accessions:
            raise RenamingError(f"duplicate accession in all_iuh_identifiers.csv: {accession}")
        accessions.add(accession)
        if mrn and pid:
            key = (mrn, organ)
            if key in mrn_organ and mrn_organ[key] != pid:
                raise RenamingError(f"MRN {mrn} has conflicting PIDs in {organ}")
            reverse = (organ, pid)
            if reverse in organ_pid and organ_pid[reverse] != mrn:
                raise RenamingError(f"PID {pid} is assigned to multiple MRNs in {organ}")
            mrn_organ[key] = pid
            organ_pid[reverse] = mrn


def _identifier_rows(clone_root: Path) -> List[Dict[str, str]]:
    initialize_clone(clone_root)
    _, rows = read_csv(clone_root / "all_iuh_identifiers.csv")
    _validate_identifiers(rows)
    return rows


def compile_report(row: Dict[str, str]) -> str:
    """Return one readable report value from current or legacy CoPath fields."""
    existing = (row.get("report") or "").strip()
    if existing:
        return existing
    sections = []
    for field in REPORT_FIELDS:
        value = (row.get(field) or "").strip()
        if value:
            sections.append(f"{field.replace('_', ' ').title()}:\n{value}")
    return "\n\n".join(sections)


def collapse_report_fields(row: Dict[str, str]) -> Dict[str, str]:
    """Replace legacy per-section report values with one report value."""
    collapsed = {
        field: value for field, value in row.items() if field not in REPORT_FIELDS
    }
    collapsed["report"] = compile_report(row)
    return collapsed


def collapse_report_headers(fields: Sequence[str]) -> List[str]:
    """Replace legacy report headers with one report header in the same position."""
    collapsed: List[str] = []
    report_added = False
    for field in fields:
        if field == "signout_date" and not report_added:
            collapsed.append("report")
            report_added = True
        if field in REPORT_FIELDS or field == "report":
            if not report_added:
                collapsed.append("report")
                report_added = True
            continue
        collapsed.append(field)
    if not report_added:
        collapsed.append("report")
    return collapsed


def row_accession(row: Dict[str, str]) -> str:
    return (row.get("AccessionID") or row.get("accession_id") or "").strip().upper()


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() == "true"


def clean_date(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return "XXXXXXXX"
    match = re.match(r"^(\d{4})-(\d{2})-(\d{2})", value)
    if match:
        try:
            dt.date(*(int(part) for part in match.groups()))
            return "".join(match.groups())
        except ValueError:
            return "XXXXXXXX"
    digits = re.sub(r"\D", "", value)[:8]
    if len(digits) == 8:
        try:
            dt.datetime.strptime(digits, "%Y%m%d")
            return digits
        except ValueError:
            pass
    return "XXXXXXXX"


def derive_organ(row: Optional[Dict[str, str]]) -> str:
    if row is None:
        return "UNKNOWN"
    text = (row.get("sample_acquisition_type") or "").lower()
    for needle, organ in (("brain", "BRAIN"), ("breast", "BREAST"), ("testis", "TESTIS")):
        if needle in text:
            return organ
    return "OTHER"


def derive_sample_type(row: Optional[Dict[str, str]]) -> str:
    if row is None:
        return "XX"
    text = " ".join(
        ((row.get("sample_acquisition_type") or ""), compile_report(row))
    ).lower()
    if "resection" in text:
        return "RE"
    if "biopsy" in text:
        return "BP"
    return "XX"


def increment_pid(pid: str) -> str:
    if not PID_RE.fullmatch(pid):
        return "AAAAAA"
    chars = list(pid)
    for index in range(5, -1, -1):
        if chars[index] == "Z":
            chars[index] = "A"
        else:
            chars[index] = chr(ord(chars[index]) + 1)
            return "".join(chars)
    raise RenamingError("PID namespace is exhausted")


def mapping_signature(rows: Sequence[Dict[str, str]]) -> str:
    payload = "\n".join("\x1f".join(row.get(field, "") for field in MAPPING_FIELDS) for row in rows)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def safe_component(value: str) -> bool:
    return bool(value) and not re.search(r"[\\/:*?\"<>|\x00-\x1f]", value)


def build_new_name(row: Dict[str, str]) -> str:
    suffix = Path(row["OriginalPath"]).suffix
    return (
        f"{row['Organ']}_{row['PID']}_{row['AccessionDate']}_{row['Timepoint']}_"
        f"{row['Stain']}_{row['ImageType']}_{row['SampAcqType']}"
        f"{row['BlockNumber']}{row['SectionCount']}{suffix}"
    )


def validate_mapping_rows(rows: Sequence[Dict[str, str]]) -> List[str]:
    errors: List[str] = []
    names = []
    for row in rows:
        accession = row.get("AccessionID", "")
        if not ACCESSION_RE.fullmatch(accession):
            errors.append(f"{accession or 'AccessionID'} must match A12-123")
        if row.get("Organ") not in ORGANS:
            errors.append(f"{accession}: invalid Organ")
        if not PID_RE.fullmatch(row.get("PID", "")):
            errors.append(f"{accession}: PID must be six uppercase letters")
        if not DATE_RE.fullmatch(row.get("AccessionDate", "")):
            errors.append(f"{accession}: AccessionDate must be YYYYMMDD or XXXXXXXX")
        elif row.get("AccessionDate") != "XXXXXXXX":
            try:
                dt.datetime.strptime(row["AccessionDate"], "%Y%m%d")
            except ValueError:
                errors.append(f"{accession}: AccessionDate is not a valid calendar date")
        if not TIMEPOINT_RE.fullmatch(row.get("Timepoint", "")):
            errors.append(f"{accession}: Timepoint must contain four uppercase letters/digits")
        if row.get("ImageType") not in IMAGE_TYPES:
            errors.append(f"{accession}: invalid ImageType")
        if row.get("SampAcqType") not in SAMPLE_TYPES:
            errors.append(f"{accession}: invalid SampAcqType")
        for field in ("Stain", "BlockNumber"):
            if not safe_component(row.get(field, "")):
                errors.append(f"{accession}: {field} is required and must be filename-safe")
        if not SECTION_RE.fullmatch(row.get("SectionCount", "")):
            errors.append(f"{accession}: SectionCount must be exactly three digits")
        if not row.get("OriginalPath", "").strip():
            errors.append(f"{accession}: OriginalPath is required")
        row["NewName"] = build_new_name(row)
        names.append(row["NewName"].casefold())
    duplicates = {name for name in names if names.count(name) > 1}
    if duplicates:
        errors.append("Generated NewName values must be unique within the batch")
    return list(dict.fromkeys(errors))


def _clone_rows(clone_root: Path) -> Tuple[Dict[str, str], Dict[str, List[Dict[str, str]]], Dict[str, List[str]]]:
    initialize_clone(clone_root)
    _, rows = read_csv(clone_root / "all_iuh_identifiers.csv")
    accession_org = {row_accession(row): row.get("Organ", "").upper() for row in rows if row_accession(row)}
    by_organ: Dict[str, List[Dict[str, str]]] = {}
    headers: Dict[str, List[str]] = {}
    for organ in ORGANS:
        path = clone_root / organ / "copath_data.csv"
        raw_headers, raw_rows = read_csv(path)
        headers[organ] = collapse_report_headers(raw_headers)
        by_organ[organ] = [collapse_report_fields(row) for row in raw_rows]
    return accession_org, by_organ, headers


def _reserved_pids(batch_base: Path) -> Dict[str, set]:
    result = {organ: set() for organ in ORGANS}
    if not batch_base.exists():
        return result
    for path in batch_base.glob("SS*/*/name_mapping.csv"):
        try:
            _, rows = read_csv(path)
        except RenamingError:
            continue
        for row in rows:
            if row.get("Organ") in result and PID_RE.fullmatch(row.get("PID", "")):
                result[row["Organ"]].add(row["PID"])
    for path in batch_base.glob("SS*/*/pending_CoPath_history.csv"):
        try:
            _, rows = read_csv(path)
        except RenamingError:
            continue
        for row in rows:
            organ = row.get("organ", "").strip().upper()
            pid = row.get("PID", "").strip().upper()
            if organ in result and PID_RE.fullmatch(pid):
                result[organ].add(pid)
    return result


def _pid_for_identifiers(
    row: Optional[Dict[str, str]], organ: str, identifiers: Sequence[Dict[str, str]], used: set
) -> str:
    mrn = (row or {}).get("mrn", "").strip()
    if mrn:
        for existing in identifiers:
            if (
                existing.get("MRN", "").strip() == mrn
                and existing.get("Organ", "").strip().upper() == organ
                and PID_RE.fullmatch(existing.get("PID", "").strip().upper())
            ):
                return existing["PID"].strip().upper()
    candidates = used | {
        item.get("PID", "").strip().upper()
        for item in identifiers
        if item.get("Organ", "").strip().upper() == organ
    }
    valid = sorted(pid for pid in candidates if PID_RE.fullmatch(pid))
    candidate = increment_pid(valid[-1]) if valid else "AAAAAA"
    while candidate in candidates:
        candidate = increment_pid(candidate)
    used.add(candidate)
    return candidate


def direct_query(
    batch_root: Path, accessions: Sequence[str], output_path: Path,
    scope: str = "exact_accession",
) -> None:
    """Run the legacy CoPath CLI in this process environment."""
    if not accessions:
        atomic_write(output_path, ["accession_id"], [])
        return
    script = Path(__file__).parent / "copath_utilities" / "query_copath_db.py"
    with tempfile.NamedTemporaryFile("w", newline="", suffix=".csv", delete=False, encoding="utf-8") as handle:
        input_path = Path(handle.name)
        writer = csv.DictWriter(handle, fieldnames=["AccessionID"])
        writer.writeheader()
        writer.writerows({"AccessionID": accession} for accession in accessions)
    try:
        result = subprocess.run(
            [sys.executable, str(script), str(input_path), "-i",
             "history" if scope == "patient_history" else "accession",
             "-c", "AccessionID", "-o", str(output_path)],
            cwd=batch_root, capture_output=True, text=True, check=False,
        )
        if result.returncode:
            raise RenamingError((result.stdout + result.stderr).strip() or "CoPath query failed")
    finally:
        input_path.unlink(missing_ok=True)


def windows_queue_query(
    _batch_root: Path, accessions: Sequence[str], output_path: Path,
    scope: str = "exact_accession",
) -> None:
    """Submit a constrained request to the manually started Windows worker."""
    if not accessions:
        atomic_write(output_path, ["accession_id"], [])
        return
    queue_root = Path(
        os.environ.get("COPATH_QUERY_QUEUE", "/data/state/copath-query")
    )
    configured_timeout = os.environ.get("COPATH_QUERY_TIMEOUT_SECONDS", "300")
    try:
        timeout_seconds = float(configured_timeout)
    except ValueError as exc:
        raise RenamingError(
            "COPATH_QUERY_TIMEOUT_SECONDS must be a number greater than zero"
        ) from exc
    try:
        submit_query(
            queue_root, list(accessions), output_path, timeout_seconds, scope=scope
        )
    except QueueProtocolError as exc:
        raise RenamingError(str(exc)) from exc
    except OSError as exc:
        raise RenamingError(f"The Windows CoPath queue is unavailable: {exc}") from exc


def default_query(batch_root: Path, accessions: Sequence[str], output_path: Path) -> None:
    """Dispatch CoPath work according to the configured query mode."""
    mode = os.environ.get("COPATH_QUERY_MODE", "direct").strip().lower()
    if mode == "direct":
        direct_query(batch_root, accessions, output_path)
    elif mode == "windows_queue":
        windows_queue_query(batch_root, accessions, output_path)
    else:
        raise RenamingError(
            "COPATH_QUERY_MODE must be either 'windows_queue' or 'direct'"
        )


def default_history_query(
    batch_root: Path, accessions: Sequence[str], output_path: Path
) -> None:
    mode = os.environ.get("COPATH_QUERY_MODE", "direct").strip().lower()
    if mode == "direct":
        direct_query(batch_root, accessions, output_path, "patient_history")
    elif mode == "windows_queue":
        windows_queue_query(batch_root, accessions, output_path, "patient_history")
    else:
        raise RenamingError("COPATH_QUERY_MODE must be either 'windows_queue' or 'direct'")


def read_history_job(batch_root: Path) -> Dict[str, object]:
    path = batch_root / "copath_history_job.json"
    if not path.exists():
        return {"version": 1, "status": "not_needed", "attempts": 0, "seed_accessions": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RenamingError(f"could not read {path.name}: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise RenamingError(f"{path.name} is invalid")
    return payload


def _update_longitudinal_log(batch_root: Path, error: str) -> None:
    path = batch_root / "copath_longitudinal_jobs.csv"
    if not path.exists():
        return
    _, rows = read_csv(path)
    for row in rows:
        row["Error"] = error
    atomic_write(path, LONGITUDINAL_LOG_FIELDS, rows)


def stage_longitudinal_history(
    batch_root: Path,
    clone_root: Path,
    batch_base: Path,
    query: Callable[[Path, Sequence[str], Path], None] = default_history_query,
) -> None:
    job_path = batch_root / "copath_history_job.json"
    job = read_history_job(batch_root)
    seeds = [str(value) for value in job.get("seed_accessions", [])]
    if not seeds:
        job.update({"status": "not_needed", "error": ""})
        atomic_write_json(job_path, job)
        return
    attempts = int(job.get("attempts", 0)) + 1
    job.update({"status": "running", "attempts": attempts, "error": ""})
    atomic_write_json(job_path, job)
    temporary = batch_root / f".pending_CoPath_history.{uuid.uuid4().hex}.csv"
    try:
        query(batch_root, seeds, temporary)
        headers, queried = read_csv(temporary)
        headers = collapse_report_headers(headers)
        identifiers = _identifier_rows(clone_root)
        identifier_by_accession = {row_accession(row): row for row in identifiers}
        _, clone_rows, _ = _clone_rows(clone_root)
        reserved = _reserved_pids(batch_base)
        reports = report_rows(batch_root, clone_root)
        _, mapping = read_csv(batch_root / "name_mapping.csv")
        pair_pids: Dict[Tuple[str, str], str] = {}
        for row in mapping:
            report = reports.get(row_accession(row), {})
            mrn = report.get("mrn", "").strip()
            organ = row.get("Organ", "").strip().upper()
            pid = row.get("PID", "").strip().upper()
            if mrn and organ in ORGANS and PID_RE.fullmatch(pid):
                pair_pids[(mrn, organ)] = pid
        staged: Dict[str, Dict[str, str]] = {}
        for raw in queried:
            row = collapse_report_fields(raw)
            accession = row_accession(row)
            if not accession:
                continue
            organ = derive_organ(row)
            mrn = row.get("mrn", "").strip()
            pid = pair_pids.get((mrn, organ), "")
            row.update({
                "organ": organ,
                "_accdate": clean_date(row.get("accession_date", "")),
                "timepoint": "XXXX",
                "image_type": "FNA" if accession.startswith("FN") else "WSI",
                "_sampacqtype": derive_sample_type(row),
            })
            existing_id = identifier_by_accession.get(accession)
            existing_raw: Optional[Dict[str, str]] = None
            if existing_id:
                old_organ = existing_id.get("Organ", "").strip().upper()
                existing_raw = next(
                    (item for item in clone_rows.get(old_organ, []) if row_accession(item) == accession),
                    None,
                )
            if existing_raw:
                for field in DERIVED_COPATH_FIELDS:
                    row[field] = existing_raw.get(field, "")
            else:
                if not PID_RE.fullmatch(pid):
                    pid = _pid_for_identifiers(row, organ, identifiers, reserved[organ])
                    pair_pids[(mrn, organ)] = pid
                row["PID"] = pid
            staged[accession] = row
        fields = list(dict.fromkeys([*headers, *DERIVED_COPATH_FIELDS]))
        atomic_write(batch_root / "pending_CoPath_history.csv", fields, staged.values())
        job.update({"status": "staged", "error": ""})
        atomic_write_json(job_path, job)
        _update_longitudinal_log(batch_root, "RETRY" if attempts > 1 else "NONE")
    except Exception as exc:
        message = str(exc).strip() or exc.__class__.__name__
        job.update({"status": "failed", "error": message[:400]})
        atomic_write_json(job_path, job)
        _update_longitudinal_log(batch_root, f"ERROR: {message[:390]}")
        raise
    finally:
        temporary.unlink(missing_ok=True)


def prepare_batch(
    batch_root: Path,
    clone_root: Path,
    batch_base: Path,
    query: Callable[[Path, Sequence[str], Path], None] = default_query,
) -> None:
    enriched_path = batch_root / "enriched.csv"
    _, slides = read_csv(enriched_path)
    required = {"AccessionID", "Stain", "BlockNumber", "original_slide_path"}
    if not slides or not required.issubset(slides[0]):
        raise RenamingError(f"enriched.csv must contain {', '.join(sorted(required))}")
    accessions = list(dict.fromkeys(row_accession(row) for row in slides))
    invalid = [value for value in accessions if not ACCESSION_RE.fullmatch(value)]
    if invalid:
        raise RenamingError(f"invalid accession ID(s): {', '.join(invalid)}")

    identifiers = _identifier_rows(clone_root)
    identifier_by_accession = {row_accession(row): row for row in identifiers}
    accession_org, clone_rows, _ = _clone_rows(clone_root)
    existing: Dict[str, Dict[str, str]] = {}
    for accession in accessions:
        organ = accession_org.get(accession)
        if organ in ORGANS:
            existing[accession] = next(
                (row for row in clone_rows[organ] if row_accession(row) == accession), {}
            )
            existing[accession]["organ"] = organ
            identifier = identifier_by_accession.get(accession, {})
            existing[accession]["PID"] = existing[accession].get("PID") or identifier.get("PID", "")
            existing[accession]["mrn"] = existing[accession].get("mrn") or identifier.get("MRN", "")

    pending_path = batch_root / "pending_CoPath_data.csv"
    unknown = [accession for accession in accessions if accession not in existing]
    query(batch_root, unknown, pending_path)
    pending_headers, pending_rows = read_csv(pending_path)
    pending_headers = collapse_report_headers(pending_headers)
    pending_rows = [collapse_report_fields(row) for row in pending_rows]
    atomic_write(pending_path, pending_headers, pending_rows)
    pending_by_accession = {row_accession(row): row for row in pending_rows}
    reserved = _reserved_pids(batch_base)
    accession_values: Dict[str, Dict[str, str]] = {}
    for accession in accessions:
        source = existing.get(accession) or pending_by_accession.get(accession)
        organ = (source or {}).get("organ") or derive_organ(source)
        organ = organ if organ in ORGANS else "UNKNOWN"
        pid = (source or {}).get("PID", "")
        if not PID_RE.fullmatch(pid):
            pid = _pid_for_identifiers(source, organ, identifiers, reserved[organ])
        accession_values[accession] = {
            "Organ": organ,
            "PID": pid,
            "AccessionDate": (source or {}).get("_accdate") or clean_date((source or {}).get("accession_date", "")),
            "Timepoint": (source or {}).get("timepoint") or "XXXX",
            "ImageType": (source or {}).get("image_type") or ("FNA" if accession.startswith("FN") else "WSI"),
            "SampAcqType": (source or {}).get("_sampacqtype") or derive_sample_type(source),
        }

    counters: Dict[Tuple[str, ...], int] = defaultdict(int)
    mapping_rows: List[Dict[str, str]] = []
    for slide in slides:
        accession = row_accession(slide)
        shared = accession_values[accession]
        stain = (slide.get("Stain") or "XX").strip()
        block = (slide.get("BlockNumber") or "XX").strip()
        key = (accession, *shared.values(), stain, block)
        row = {
            "AccessionID": accession, **shared, "Stain": stain, "BlockNumber": block,
            "SectionCount": f"{counters[key]:03d}",
            "OriginalPath": slide.get("original_slide_path", ""), "Approved": "False",
        }
        counters[key] += 1
        row["NewName"] = build_new_name(row)
        mapping_rows.append(row)
    errors = validate_mapping_rows(mapping_rows)
    if errors:
        raise RenamingError("; ".join(errors))
    atomic_write(batch_root / "name_mapping.csv", MAPPING_FIELDS, mapping_rows)
    known_mrns = {row.get("MRN", "").strip() for row in identifiers if row.get("MRN", "").strip()}
    new_rows = [
        row for row in pending_rows
        if row_accession(row) in unknown
        and row.get("mrn", "").strip()
        and row.get("mrn", "").strip() not in known_mrns
    ]
    seeds_by_mrn: Dict[str, str] = {}
    for row in new_rows:
        seeds_by_mrn.setdefault(row.get("mrn", "").strip(), row_accession(row))
    log_rows = [
        {"AccessionID": row_accession(row), "MRN": row.get("mrn", "").strip(), "Error": "NONE"}
        for row in new_rows
    ]
    atomic_write(batch_root / "copath_longitudinal_jobs.csv", LONGITUDINAL_LOG_FIELDS, log_rows)
    atomic_write_json(batch_root / "copath_history_job.json", {
        "version": 1,
        "generation": uuid.uuid4().hex,
        "status": "pending" if seeds_by_mrn else "not_needed",
        "attempts": 0,
        "seed_accessions": list(seeds_by_mrn.values()),
        "error": "",
    })


def report_rows(batch_root: Path, clone_root: Path) -> Dict[str, Dict[str, str]]:
    result: Dict[str, Dict[str, str]] = {}
    pending = batch_root / "pending_CoPath_data.csv"
    if pending.exists():
        _, rows = read_csv(pending)
        result.update({
            row_accession(row): collapse_report_fields(row)
            for row in rows
            if row_accession(row)
        })
    accession_org, clone_rows, _ = _clone_rows(clone_root)
    for accession, organ in accession_org.items():
        if accession not in result and organ in clone_rows:
            row = next((item for item in clone_rows[organ] if row_accession(item) == accession), None)
            if row:
                result[accession] = row
    return result


def validate_pid_assignment(
    clone_root: Path, organ: str, pid: str, mrn: str
) -> Optional[str]:
    for row in _identifier_rows(clone_root):
        if row.get("Organ", "").strip().upper() != organ or row.get("PID", "").strip().upper() != pid:
            continue
        existing_mrn = row.get("MRN", "").strip()
        if not mrn or not existing_mrn or existing_mrn != mrn:
            return f"PID {pid} is already assigned to a different MRN in {organ}"
    return None


def group_mapping(rows: Sequence[Dict[str, str]], reports: Dict[str, Dict[str, str]]) -> List[Dict[str, object]]:
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["AccessionID"]].append(row)
    result = []
    for accession, slides in grouped.items():
        report = reports.get(accession, {})
        result.append({
            "accession": accession, "shared": slides[0], "slides": slides,
            "approved": all(parse_bool(row["Approved"]) for row in slides),
            "report": compile_report(report),
        })
    return result


def update_group(
    mapping_path: Path, old_accession: str, values: Dict[str, str], slide_values: Dict[str, Dict[str, str]],
    expected_signature: str,
) -> Tuple[List[Dict[str, str]], bool]:
    _, rows = read_csv(mapping_path)
    if mapping_signature(rows) != expected_signature:
        raise RenamingError("The mapping changed in another session; reload and try again")
    new_accession = values["AccessionID"]
    target_exists = new_accession != old_accession and any(row["AccessionID"] == new_accession for row in rows)
    target = next((row for row in rows if row["AccessionID"] == new_accession), None)
    for row in rows:
        if row["AccessionID"] != old_accession:
            continue
        row["AccessionID"] = new_accession
        source = target if target_exists and target else values
        for field in ("Organ", "PID", "AccessionDate", "Timepoint", "ImageType", "SampAcqType"):
            row[field] = source[field]
        submitted = slide_values.get(row["OriginalPath"], {})
        for field in ("Stain", "BlockNumber", "SectionCount"):
            row[field] = submitted.get(field, row[field]).strip()
        row["Approved"] = "False" if target_exists else "True"
        row["NewName"] = build_new_name(row)
    if target_exists:
        for row in rows:
            if row["AccessionID"] == new_accession:
                row["Approved"] = "False"
        counters: Dict[Tuple[str, ...], int] = defaultdict(int)
        for row in rows:
            key = (
                row["Organ"], row["PID"], row["AccessionDate"], row["Timepoint"],
                row["Stain"], row["ImageType"], row["SampAcqType"], row["BlockNumber"],
            )
            row["SectionCount"] = f"{counters[key]:03d}"
            counters[key] += 1
            row["NewName"] = build_new_name(row)
    errors = validate_mapping_rows(rows)
    if errors:
        raise RenamingError("; ".join(errors))
    atomic_write(mapping_path, MAPPING_FIELDS, rows)
    if target_exists:
        pending_path = mapping_path.parent / "pending_CoPath_data.csv"
        if pending_path.exists():
            headers, pending = read_csv(pending_path)
            pending = [row for row in pending if row_accession(row) != old_accession]
            atomic_write(pending_path, headers, pending)
    return rows, target_exists


def retry_group(
    batch_root: Path,
    clone_root: Path,
    batch_base: Path,
    old_accession: str,
    new_accession: str,
    query: Callable[[Path, Sequence[str], Path], None] = default_query,
) -> None:
    new_accession = new_accession.strip().upper()
    if not ACCESSION_RE.fullmatch(new_accession):
        raise RenamingError("AccessionID must match A12-123")
    mapping_path = batch_root / "name_mapping.csv"
    _, mapping = read_csv(mapping_path)
    current = next((row for row in mapping if row["AccessionID"] == old_accession), None)
    target = next(
        (
            row for row in mapping
            if new_accession != old_accession and row["AccessionID"] == new_accession
        ),
        None,
    )
    accession_org, clone_rows, _ = _clone_rows(clone_root)
    identifiers = _identifier_rows(clone_root)
    source: Optional[Dict[str, str]] = None
    organ = accession_org.get(new_accession)
    if organ in ORGANS:
        source = next((row for row in clone_rows[organ] if row_accession(row) == new_accession), None)
    if source is None and target is None:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as handle:
            retry_path = Path(handle.name)
        retry_path.unlink(missing_ok=True)
        try:
            query(batch_root, [new_accession], retry_path)
            headers, queried = read_csv(retry_path)
            headers = collapse_report_headers(headers)
            queried = [collapse_report_fields(row) for row in queried]
            source = next((row for row in queried if row_accession(row) == new_accession), None)
            pending_path = batch_root / "pending_CoPath_data.csv"
            if pending_path.exists():
                pending_headers, pending = read_csv(pending_path)
                pending_headers = collapse_report_headers(pending_headers)
                pending = [collapse_report_fields(row) for row in pending]
            else:
                pending_headers, pending = headers, []
            pending = [row for row in pending if row_accession(row) not in {old_accession, new_accession}]
            if source:
                pending.append(source)
                pending_headers = list(dict.fromkeys([*pending_headers, *source.keys()]))
            atomic_write(pending_path, pending_headers or ["accession_id"], pending)
        finally:
            retry_path.unlink(missing_ok=True)
    if target:
        shared = {field: target[field] for field in ("Organ", "PID", "AccessionDate", "Timepoint", "ImageType", "SampAcqType")}
    else:
        organ = organ or derive_organ(source)
        reserved = _reserved_pids(batch_base)
        pid = (source or {}).get("PID", "")
        if (
            not PID_RE.fullmatch(pid)
            and current
            and current.get("Organ") == organ
            and PID_RE.fullmatch(current.get("PID", ""))
        ):
            pid = current["PID"]
        if not PID_RE.fullmatch(pid):
            pid = _pid_for_identifiers(source, organ, identifiers, reserved[organ])
        shared = {
            "Organ": organ, "PID": pid,
            "AccessionDate": clean_date((source or {}).get("accession_date", "")),
            "Timepoint": "XXXX", "ImageType": "FNA" if new_accession.startswith("FN") else "WSI",
            "SampAcqType": derive_sample_type(source),
        }
    for row in mapping:
        if row["AccessionID"] == old_accession:
            row["AccessionID"] = new_accession
            row.update(shared)
            row["Approved"] = "False"
            row["NewName"] = build_new_name(row)
    if target:
        for row in mapping:
            if row["AccessionID"] == new_accession:
                row["Approved"] = "False"
    errors = validate_mapping_rows(mapping)
    if errors:
        raise RenamingError("; ".join(errors))
    atomic_write(mapping_path, MAPPING_FIELDS, mapping)
    known_mrns = {row.get("MRN", "").strip() for row in identifiers if row.get("MRN", "").strip()}
    mrn = (source or {}).get("mrn", "").strip()
    seeds = [new_accession] if source is not None and mrn and mrn not in known_mrns else []
    atomic_write(
        batch_root / "copath_longitudinal_jobs.csv", LONGITUDINAL_LOG_FIELDS,
        [{"AccessionID": new_accession, "MRN": mrn, "Error": "NONE"}] if seeds else [],
    )
    atomic_write_json(batch_root / "copath_history_job.json", {
        "version": 1, "generation": uuid.uuid4().hex,
        "status": "pending" if seeds else "not_needed", "attempts": 0,
        "seed_accessions": seeds, "error": "",
    })
    (batch_root / "pending_CoPath_history.csv").unlink(missing_ok=True)


def finalize_batch(batch_root: Path, clone_root: Path) -> None:
    mapping_path = batch_root / "name_mapping.csv"
    _, mapping = read_csv(mapping_path)
    if not mapping or not all(parse_bool(row["Approved"]) for row in mapping):
        return
    accession_org, clone_rows, clone_headers = _clone_rows(clone_root)
    identifier_rows = _identifier_rows(clone_root)
    identifiers = {row_accession(row): dict(row) for row in identifier_rows}
    reports = report_rows(batch_root, clone_root)
    approved_by_accession = {row["AccessionID"]: row for row in mapping}
    backup_root = batch_root / "renaming_backups" / dt.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    touched: List[Tuple[Path, Optional[Path]]] = []
    try:
        touched_organs = set()
        for accession, approved in approved_by_accession.items():
            old_organ = accession_org.get(accession)
            if old_organ is not None:
                clone_rows[old_organ] = [
                    row for row in clone_rows[old_organ] if row_accession(row) != accession
                ]
                touched_organs.add(old_organ)
            raw = reports.get(accession)
            mrn = (raw or {}).get("mrn", "").strip()
            identifiers[accession] = {
                "AccessionID": accession, "Organ": approved["Organ"],
                "MRN": mrn, "PID": approved["PID"],
            }
            if raw:
                organ = approved["Organ"]
                enriched = collapse_report_fields(raw)
                enriched.update({
                    "organ": organ, "PID": approved["PID"], "_accdate": approved["AccessionDate"],
                    "timepoint": approved["Timepoint"], "image_type": approved["ImageType"],
                    "_sampacqtype": approved["SampAcqType"],
                })
                clone_rows[organ].append(enriched)
                clone_headers[organ] = list(dict.fromkeys([*clone_headers[organ], *enriched.keys()]))
                touched_organs.add(organ)

        history_path = batch_root / "pending_CoPath_history.csv"
        job = read_history_job(batch_root)
        if history_path.exists() and job.get("status") in {"staged", "committed"}:
            history_headers, history_rows = read_csv(history_path)
            history_headers = list(dict.fromkeys([
                *collapse_report_headers(history_headers), *DERIVED_COPATH_FIELDS,
            ]))
            pair_pids: Dict[Tuple[str, str], str] = {}
            for accession, approved in approved_by_accession.items():
                mrn = (reports.get(accession) or {}).get("mrn", "").strip()
                if mrn:
                    pair_pids[(mrn, approved["Organ"])] = approved["PID"]
            commit_used = {
                organ: {
                    row.get("PID", "").strip().upper()
                    for row in identifiers.values()
                    if row.get("Organ", "").strip().upper() == organ
                    and PID_RE.fullmatch(row.get("PID", "").strip().upper())
                }
                for organ in ORGANS
            }
            for raw_value in history_rows:
                raw = collapse_report_fields(raw_value)
                accession = row_accession(raw)
                if not accession:
                    continue
                old = identifiers.get(accession)
                old_organ = (old or {}).get("Organ", "").strip().upper()
                approved = approved_by_accession.get(accession)
                organ = approved["Organ"] if approved else raw.get("organ", "").strip().upper()
                if organ not in ORGANS:
                    organ = old_organ if old_organ in ORGANS else derive_organ(raw)
                if old_organ in ORGANS:
                    clone_rows[old_organ] = [
                        item for item in clone_rows[old_organ] if row_accession(item) != accession
                    ]
                    touched_organs.add(old_organ)
                mrn = raw.get("mrn", "").strip()
                pid = approved["PID"] if approved else raw.get("PID", "").strip().upper()
                if not old and (mrn, organ) in pair_pids:
                    pid = pair_pids[(mrn, organ)]
                    raw["PID"] = pid
                if not old and not approved:
                    pair_match = next((
                        item.get("PID", "").strip().upper()
                        for item in identifiers.values()
                        if item.get("MRN", "").strip() == mrn
                        and item.get("Organ", "").strip().upper() == organ
                        and PID_RE.fullmatch(item.get("PID", "").strip().upper())
                    ), "")
                    collision = any(
                        item.get("Organ", "").strip().upper() == organ
                        and item.get("PID", "").strip().upper() == pid
                        and item.get("MRN", "").strip() != mrn
                        for item in identifiers.values()
                    )
                    if pair_match:
                        pid = pair_match
                    elif not PID_RE.fullmatch(pid) or collision:
                        pid = _pid_for_identifiers(
                            raw, organ, list(identifiers.values()), commit_used[organ]
                        )
                    raw["PID"] = pid
                    commit_used[organ].add(pid)
                if approved:
                    raw.update({
                        "organ": organ, "PID": pid, "_accdate": approved["AccessionDate"],
                        "timepoint": approved["Timepoint"], "image_type": approved["ImageType"],
                        "_sampacqtype": approved["SampAcqType"],
                    })
                identifiers[accession] = {
                    "AccessionID": accession, "Organ": organ, "MRN": mrn, "PID": pid,
                }
                clone_rows[organ].append(raw)
                clone_headers[organ] = list(dict.fromkeys([*clone_headers[organ], *history_headers, *raw.keys()]))
                touched_organs.add(organ)

        index_rows = sorted(identifiers.values(), key=lambda row: row["AccessionID"])
        _validate_identifiers(index_rows)
        index_path = clone_root / "all_iuh_identifiers.csv"
        targets = [(index_path, list(IDENTIFIER_FIELDS), index_rows)]
        for organ in ORGANS:
            if organ in touched_organs:
                targets.append((clone_root / organ / "copath_data.csv", clone_headers[organ], clone_rows[organ]))
        pending_path = batch_root / "pending_CoPath_data.csv"
        if pending_path.exists():
            headers, pending = read_csv(pending_path)
            headers = collapse_report_headers(headers)
            pending = [collapse_report_fields(raw) for raw in pending]
            pending = [raw for raw in pending if row_accession(raw) in approved_by_accession]
            headers = list(dict.fromkeys([*headers, *DERIVED_COPATH_FIELDS]))
            for raw in pending:
                approved = approved_by_accession.get(row_accession(raw))
                if approved:
                    raw.update({
                        "organ": approved["Organ"], "PID": approved["PID"], "_accdate": approved["AccessionDate"],
                        "timepoint": approved["Timepoint"], "image_type": approved["ImageType"],
                        "_sampacqtype": approved["SampAcqType"],
                    })
            targets.append((pending_path, headers, pending))
        for path, _, _ in targets:
            if path.exists():
                backup = backup_root / f"{path.parent.name}_{path.name}"
                backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, backup)
                touched.append((path, backup))
            else:
                touched.append((path, None))
        for path, headers, rows in targets:
            atomic_write(path, headers, rows)
        if history_path.exists() and job.get("status") in {"staged", "committed"}:
            job.update({"status": "committed", "error": ""})
            atomic_write_json(batch_root / "copath_history_job.json", job)
    except Exception:
        for path, backup in reversed(touched):
            if backup is None:
                path.unlink(missing_ok=True)
            else:
                shutil.copy2(backup, path)
        raise
