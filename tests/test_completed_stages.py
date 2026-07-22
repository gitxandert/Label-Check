import csv
import tempfile
import unittest
from pathlib import Path

import app as qc_app


class CompletedStagesTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.batch_base = self.root / "batches"
        self.batch_root = self.batch_base / "SS100" / "batch-1"
        (self.batch_root / "label").mkdir(parents=True)
        (self.batch_root / "macro").mkdir()
        with open(
            self.batch_root / "enriched.csv", "w", newline="", encoding="utf-8"
        ) as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=[
                    "AccessionID",
                    "BlockNumber",
                    "Stain",
                    "ParsingQCPassed",
                    "original_slide_path",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "AccessionID": "A12-123",
                    "BlockNumber": "A1",
                    "Stain": "H&E",
                    "ParsingQCPassed": "TRUE",
                    "original_slide_path": "slide.svs",
                }
            )

        self.old_batch_base = qc_app.Config.LABEL_CHECK_BATCHES
        self.old_instance_dir = qc_app.Config.INSTANCE_DIR
        qc_app.Config.LABEL_CHECK_BATCHES = str(self.batch_base)
        qc_app.Config.INSTANCE_DIR = str(self.root / "instance")
        qc_app.batch_contexts.clear()

    def tearDown(self):
        qc_app.batch_contexts.clear()
        qc_app.Config.LABEL_CHECK_BATCHES = self.old_batch_base
        qc_app.Config.INSTANCE_DIR = self.old_instance_dir
        self.temp_dir.cleanup()

    def test_discovery_creates_default_file_and_ignores_row_completion(self):
        batches, warnings = qc_app.discover_batches()

        self.assertEqual(warnings, [])
        self.assertEqual(len(batches), 1)
        self.assertFalse(batches[0].qc_complete)
        self.assertEqual(
            (self.batch_root / "completed_stages.csv").read_text(encoding="utf-8"),
            "QC,Renamed\nFalse,False\n",
        )

        with qc_app.app.test_request_context("/qc"):
            selected, available, selection_warnings = qc_app._selected_batch()
            self.assertIsNone(selected)
            self.assertEqual(len(available), 1)
            self.assertEqual(selection_warnings, [])

    def test_qc_true_is_not_available_but_can_be_selected_explicitly(self):
        batches, _ = qc_app.discover_batches()
        batches[0].mark_qc_complete()

        with qc_app.app.test_request_context(f"/qc?batch={batches[0].id}"):
            selected, available, _ = qc_app._selected_batch()
            self.assertIsNone(selected)
            self.assertEqual(available, [])

        with qc_app.app.test_request_context(f"/history?batch={batches[0].id}"):
            selected, available, _ = qc_app._selected_batch(allow_completed=True)
            self.assertEqual(selected.id, batches[0].id)
            self.assertEqual(available, [])

    def test_malformed_existing_file_is_skipped_without_replacement(self):
        status_path = self.batch_root / "completed_stages.csv"
        malformed = "QC,Renamed\nmaybe,False\n"
        status_path.write_text(malformed, encoding="utf-8")

        batches, warnings = qc_app.discover_batches()

        self.assertEqual(batches, [])
        self.assertEqual(status_path.read_text(encoding="utf-8"), malformed)
        self.assertTrue(any("invalid QC value" in warning for warning in warnings))

    def test_mark_qc_complete_preserves_renamed(self):
        status_path = self.batch_root / "completed_stages.csv"
        status_path.write_text("QC,Renamed\nFalse,True\n", encoding="utf-8")
        context = qc_app.BatchContext("test-batch", self.batch_root)
        context.load_completed_stages()

        context.mark_qc_complete()
        context.load_completed_stages()

        self.assertEqual(context.completed_stages, {"QC": True, "Renamed": True})

    def test_qc_row_validation_requires_all_fields_and_canonical_accession(self):
        valid_row = {
            "AccessionID": "NP25-1234",
            "BlockNumber": "A1",
            "Stain": "H&E",
        }

        self.assertEqual(qc_app._qc_row_validation_errors(valid_row), [])
        self.assertIn(
            "Accession ID must match A12-123",
            qc_app._qc_row_validation_errors({**valid_row, "AccessionID": "np25-1234"}),
        )
        self.assertIn(
            "Block Number is required",
            qc_app._qc_row_validation_errors({**valid_row, "BlockNumber": "  "}),
        )
        self.assertIn(
            "Stain is required",
            qc_app._qc_row_validation_errors({**valid_row, "Stain": ""}),
        )

    def test_final_validation_requeues_invalid_completed_row(self):
        batches, _ = qc_app.discover_batches()
        context = batches[0]
        row = context.data_manager.data[0]
        row["AccessionID"] = "A12/123"
        item = context.queue_manager.get(0)
        item.completed_by_id = "reviewer"
        item.completed_at = "2026-07-22T12:00:00"

        invalid_indices = qc_app._requeue_invalid_qc_rows(context)

        self.assertEqual(invalid_indices, [0])
        self.assertFalse(row["_is_complete"])
        self.assertEqual(item.status, "pending")
        self.assertIsNone(item.completed_by_id)
        self.assertIsNone(item.completed_at)

    def test_final_validation_leaves_valid_completed_row_unchanged(self):
        batches, _ = qc_app.discover_batches()
        context = batches[0]
        item = context.queue_manager.get(0)

        self.assertEqual(qc_app._requeue_invalid_qc_rows(context), [])
        self.assertTrue(context.data_manager.data[0]["_is_complete"])
        self.assertEqual(item.status, "completed")


if __name__ == "__main__":
    unittest.main()
