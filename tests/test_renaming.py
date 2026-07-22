import csv
import sys
import tempfile
import unittest
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import renaming
import app as app_module


def write_csv(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class RenamingDataTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.batch_base = self.root / "batches"
        self.batch = self.batch_base / "SS100" / "batch-1"
        self.clone = self.root / "clone"
        self.batch.mkdir(parents=True)
        write_csv(
            self.batch / "enriched.csv",
            ["AccessionID", "Stain", "BlockNumber", "original_slide_path"],
            [
                {"AccessionID": "NP25-100", "Stain": "HE", "BlockNumber": "B4", "original_slide_path": "one.svs"},
                {"AccessionID": "NP25-100", "Stain": "HE", "BlockNumber": "B4", "original_slide_path": "two.svs"},
            ],
        )
        write_csv(self.clone / "all_accessions.csv", ["AccessionID", "Organ"], [])
        write_csv(
            self.clone / "BRAIN" / "copath_data.csv",
            ["accession_id", "mrn", "PID", "organ"],
            [{"accession_id": "NP24-1", "mrn": "MRN1", "PID": "AAAAAZ", "organ": "BRAIN"}],
        )

    def tearDown(self):
        self.temporary.cleanup()

    def query(self, _batch, accessions, output):
        self.assertEqual(["NP25-100"], list(accessions))
        write_csv(
            output,
            ["accession_id", "mrn", "accession_date", "sample_acquisition_type", "final_diagnosis"],
            [{
                "accession_id": "NP25-100", "mrn": "MRN1",
                "accession_date": "2025-03-04 00:00:00.000",
                "sample_acquisition_type": "Brain resection", "final_diagnosis": "Example",
            }],
        )

    def test_prepare_reuses_mrn_pid_and_builds_unique_names(self):
        renaming.prepare_batch(self.batch, self.clone, self.batch_base, self.query)

        fields, rows = renaming.read_csv(self.batch / "name_mapping.csv")
        self.assertEqual(list(renaming.MAPPING_FIELDS), fields)
        self.assertEqual(["000", "001"], [row["SectionCount"] for row in rows])
        self.assertTrue(all(row["PID"] == "AAAAAZ" for row in rows))
        self.assertEqual("BRAIN_AAAAAZ_20250304_XXXX_HE_WSI_REB4000.svs", rows[0]["NewName"])
        self.assertTrue(all(row["Approved"] == "False" for row in rows))

    def test_prepare_missing_report_uses_unknown_and_index_only_on_finalize(self):
        def no_results(_batch, _accessions, output):
            write_csv(output, ["accession_id"], [])

        renaming.prepare_batch(self.batch, self.clone, self.batch_base, no_results)
        _, rows = renaming.read_csv(self.batch / "name_mapping.csv")
        self.assertEqual("UNKNOWN", rows[0]["Organ"])
        self.assertEqual("AAAAAA", rows[0]["PID"])
        for row in rows:
            row["Approved"] = "True"
        renaming.atomic_write(self.batch / "name_mapping.csv", renaming.MAPPING_FIELDS, rows)

        renaming.finalize_batch(self.batch, self.clone)

        _, index_rows = renaming.read_csv(self.clone / "all_accessions.csv")
        self.assertEqual([{"AccessionID": "NP25-100", "Organ": "UNKNOWN"}], index_rows)
        unknown_path = self.clone / "UNKNOWN" / "copath_data.csv"
        self.assertTrue(unknown_path.exists())
        headers, unknown_rows = renaming.read_csv(unknown_path)
        self.assertEqual(list(renaming.COPATH_FIELDS), headers)
        self.assertEqual([], unknown_rows)

    def test_empty_clone_is_initialized_with_all_required_csvs(self):
        empty_clone = self.root / "empty-clone"

        accession_org, rows_by_organ, headers_by_organ = renaming._clone_rows(empty_clone)

        self.assertEqual({}, accession_org)
        index_headers, index_rows = renaming.read_csv(empty_clone / "all_accessions.csv")
        self.assertEqual(["AccessionID", "Organ"], index_headers)
        self.assertEqual([], index_rows)
        for organ in renaming.ORGANS:
            self.assertEqual([], rows_by_organ[organ])
            self.assertEqual(list(renaming.COPATH_FIELDS), headers_by_organ[organ])
            self.assertTrue((empty_clone / organ / "copath_data.csv").is_file())

    def test_update_group_rejects_stale_signature_and_merges_to_target(self):
        rows = []
        for accession, path, pid in (("NP25-100", "one.svs", "AAAAAA"), ("NP25-200", "two.svs", "AAAAAB")):
            row = {
                "AccessionID": accession, "Organ": "BRAIN", "PID": pid,
                "AccessionDate": "20250101", "Timepoint": "XXXX", "Stain": "HE",
                "ImageType": "WSI", "SampAcqType": "RE", "BlockNumber": "A1",
                "SectionCount": "001", "OriginalPath": path, "Approved": "True",
            }
            row["NewName"] = renaming.build_new_name(row)
            rows.append(row)
        renaming.atomic_write(self.batch / "name_mapping.csv", renaming.MAPPING_FIELDS, rows)
        with self.assertRaisesRegex(renaming.RenamingError, "changed in another session"):
            renaming.update_group(self.batch / "name_mapping.csv", "NP25-100", {}, {}, "bad")

        values = {field: rows[0][field] for field in ("Organ", "PID", "AccessionDate", "Timepoint", "ImageType", "SampAcqType")}
        values["AccessionID"] = "NP25-200"
        updated, merged = renaming.update_group(
            self.batch / "name_mapping.csv", "NP25-100", values,
            {"one.svs": {"Stain": "HE", "BlockNumber": "A2", "SectionCount": "002"}},
            renaming.mapping_signature(rows),
        )
        self.assertTrue(merged)
        self.assertTrue(all(row["AccessionID"] == "NP25-200" for row in updated))
        self.assertTrue(all(row["PID"] == "AAAAAB" for row in updated))
        self.assertTrue(all(row["Approved"] == "False" for row in updated))


class RenamingPageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.batch_base = self.root / "batches"
        self.batch = self.batch_base / "SS100" / "batch-1"
        self.clone = self.root / "clone"
        (self.batch / "label").mkdir(parents=True)
        (self.batch / "macro").mkdir()
        write_csv(
            self.batch / "enriched.csv",
            ["AccessionID", "Stain", "BlockNumber", "ParsingQCPassed", "original_slide_path"],
            [{"AccessionID": "NP25-100", "Stain": "HE", "BlockNumber": "B4", "ParsingQCPassed": "TRUE", "original_slide_path": "one.svs"}],
        )
        write_csv(self.batch / "completed_stages.csv", ["QC", "Renamed"], [{"QC": "True", "Renamed": "False"}])
        write_csv(self.clone / "all_accessions.csv", ["AccessionID", "Organ"], [])
        row = {
            "AccessionID": "NP25-100", "Organ": "BRAIN", "PID": "AAAAAA",
            "AccessionDate": "20250304", "Timepoint": "XXXX", "Stain": "HE",
            "ImageType": "WSI", "SampAcqType": "RE", "BlockNumber": "B4",
            "SectionCount": "001", "OriginalPath": "one.svs", "Approved": "False",
        }
        row["NewName"] = renaming.build_new_name(row)
        renaming.atomic_write(self.batch / "name_mapping.csv", renaming.MAPPING_FIELDS, [row])
        write_csv(
            self.batch / "pending_CoPath_data.csv",
            ["accession_id", "mrn", "final_diagnosis"],
            [{"accession_id": "NP25-100", "mrn": "MRN1", "final_diagnosis": "Diagnosis text"}],
        )
        self.old_batch_base = app_module.Config.LABEL_CHECK_BATCHES
        self.old_clone = app_module.Config.COPATH_CLONE
        self.old_instance = app_module.Config.INSTANCE_DIR
        self.old_users = app_module.user_manager.users.copy()
        app_module.Config.LABEL_CHECK_BATCHES = str(self.batch_base)
        app_module.Config.COPATH_CLONE = str(self.clone)
        app_module.Config.INSTANCE_DIR = str(self.root / "instance")
        app_module.batch_contexts.clear()
        app_module._renaming_jobs.clear()
        self.user = app_module.User("renamer", "", False)
        app_module.user_manager.users[self.user.id] = self.user
        app_module.app.config.update(TESTING=True, SECRET_KEY="renaming-test")
        self.client = app_module.app.test_client()
        with self.client.session_transaction() as session:
            session["_user_id"] = self.user.id
            session["_fresh"] = True

    def tearDown(self):
        app_module.Config.LABEL_CHECK_BATCHES = self.old_batch_base
        app_module.Config.COPATH_CLONE = self.old_clone
        app_module.Config.INSTANCE_DIR = self.old_instance
        app_module.user_manager.users = self.old_users
        app_module.batch_contexts.clear()
        app_module._renaming_jobs.clear()
        self.temporary.cleanup()

    def test_page_lists_batch_and_renders_mapping_and_report(self):
        listing = self.client.get("/renaming")
        batches, _ = app_module._renaming_batches()
        detail = self.client.get(f"/renaming?batch={batches[0].id}")

        self.assertEqual(200, listing.status_code)
        self.assertIn(b"SS100/batch-1", listing.data)
        self.assertEqual(200, detail.status_code)
        self.assertIn(b"NP25-100", detail.data)
        self.assertIn(b"Diagnosis text", detail.data)

    def test_multiple_accessions_render_as_complete_rows_before_expansion(self):
        _, rows = renaming.read_csv(self.batch / "name_mapping.csv")
        second = dict(rows[0])
        second.update({
            "AccessionID": "NP25-200", "PID": "AAAAAB", "OriginalPath": "two.svs",
            "SectionCount": "001",
        })
        second["NewName"] = renaming.build_new_name(second)
        renaming.atomic_write(
            self.batch / "name_mapping.csv", renaming.MAPPING_FIELDS, [rows[0], second]
        )
        batches, _ = app_module._renaming_batches()

        response = self.client.get(f"/renaming?batch={batches[0].id}")

        self.assertEqual(200, response.status_code)
        self.assertEqual(2, response.data.count(b'class="accession-form'))
        self.assertEqual(2, response.data.count(b'name="accession_id"'))
        self.assertEqual(2, response.data.count(b'class="slide-toggle secondary"'))
        self.assertEqual(2, response.data.count(b'class="expanded"'))
        self.assertNotIn(b'<details>\n                        <summary class="cell"', response.data)

    def test_report_view_is_a_row_button_with_its_own_full_width_panel(self):
        batches, _ = app_module._renaming_batches()

        response = self.client.get(f"/renaming?batch={batches[0].id}")

        self.assertEqual(200, response.status_code)
        self.assertIn(b'class="report-toggle secondary"', response.data)
        self.assertIn(b'>View</button>', response.data)
        self.assertIn(b'class="expanded report-expanded"', response.data)
        self.assertIn(b'id="reports-0" hidden', response.data)
        self.assertNotIn(b'<details class="report">', response.data)

    def test_approval_finalizes_clone_and_completed_stage(self):
        batches, _ = app_module._renaming_batches()
        _, rows = renaming.read_csv(self.batch / "name_mapping.csv")
        response = self.client.post(
            f"/renaming/approve/{batches[0].id}",
            data={
                "old_accession": "NP25-100", "mapping_signature": renaming.mapping_signature(rows),
                "accession_id": "NP25-100", "organ": "BRAIN", "pid": "AAAAAA",
                "accession_date": "20250304", "timepoint": "XXXX", "image_type": "WSI",
                "samp_acq_type": "RE", "slide_count": "1", "original_path_0": "one.svs",
                "stain_0": "HE", "block_number_0": "B4", "section_count_0": "001",
            },
        )

        self.assertEqual(302, response.status_code)
        self.assertEqual("QC,Renamed\nTrue,True\n", (self.batch / "completed_stages.csv").read_text())
        _, index_rows = renaming.read_csv(self.clone / "all_accessions.csv")
        self.assertEqual("NP25-100", index_rows[0]["AccessionID"])
        _, clone_rows = renaming.read_csv(self.clone / "BRAIN" / "copath_data.csv")
        self.assertEqual("AAAAAA", clone_rows[0]["PID"])


if __name__ == "__main__":
    unittest.main()
