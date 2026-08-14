import csv
import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import deidentify_anonymize


class DeidentifyInputListTests(unittest.TestCase):
    def test_input_list_processes_exact_files_and_returns_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = root / "first.svs"
            second = root / "second.svs"
            input_list = root / "input.csv"
            output_log = root / "results.csv"
            with input_list.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=("file_path",))
                writer.writeheader()
                writer.writerows(
                    ({"file_path": str(first)}, {"file_path": str(second)})
                )

            output = io.StringIO()
            arguments = [
                "deidentify_anonymize.py",
                "--input-list",
                str(input_list),
                "--output-log",
                str(output_log),
            ]
            with mock.patch.object(sys, "argv", arguments), mock.patch.object(
                deidentify_anonymize,
                "anonymize_slide",
                side_effect=(0, 1),
            ) as anonymize, redirect_stdout(output):
                return_code = deidentify_anonymize.main()

            self.assertEqual(1, return_code)
            self.assertEqual(
                [mock.call(str(first), archive_root=None), mock.call(str(second), archive_root=None)],
                anonymize.call_args_list,
            )
            with output_log.open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(["SUCCESS", "FAILURE"], [row["status"] for row in rows])
            self.assertLess(output.getvalue().index("first.svs"), output.getvalue().index("second.svs"))


if __name__ == "__main__":
    unittest.main()
