import csv
import io
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from openpyxl import Workbook, load_workbook

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import app as app_module
import renaming


class FakeProcess:
    def __init__(self, output=b"", return_code=0):
        self.stdout = io.BytesIO(output)
        self.return_code = return_code

    def wait(self):
        return self.return_code

    def poll(self):
        return self.return_code


def write_csv(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def mapping_row(original_path, pid, section):
    row = {
        "AccessionID": "NP25-100",
        "Organ": "BRAIN",
        "PID": pid,
        "AccessionDate": "20250101",
        "Timepoint": "XXXX",
        "Stain": "HE",
        "ImageType": "WSI",
        "SampAcqType": "RE",
        "BlockNumber": "B4",
        "SectionCount": section,
        "OriginalPath": original_path,
        "Approved": "True",
    }
    row["NewName"] = renaming.build_new_name(row)
    return row


class TQTransferTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.batch_base = self.root / "batches"
        self.batch = self.batch_base / "SS12797" / "2026-07-20"
        (self.batch / "label").mkdir(parents=True)
        (self.batch / "macro").mkdir()
        write_csv(
            self.batch / "enriched.csv",
            [
                "AccessionID",
                "Stain",
                "BlockNumber",
                "ParsingQCPassed",
                "original_slide_path",
            ],
            [
                {
                    "AccessionID": "NP25-100",
                    "Stain": "HE",
                    "BlockNumber": "B4",
                    "ParsingQCPassed": "TRUE",
                    "original_slide_path": "one.svs",
                },
                {
                    "AccessionID": "NP25-100",
                    "Stain": "HE",
                    "BlockNumber": "B4",
                    "ParsingQCPassed": "TRUE",
                    "original_slide_path": "two.svs",
                },
            ],
        )
        write_csv(
            self.batch / "completed_stages.csv",
            ["QC", "Renamed"],
            [{"QC": "True", "Renamed": "True"}],
        )
        renaming.atomic_write(
            self.batch / "name_mapping.csv",
            renaming.MAPPING_FIELDS,
            [
                mapping_row(
                    r"D:\scanner\misc\one.svs", "AAAAAA", "001"
                ),
                mapping_row(
                    r"D:\scanner\misc\two.svs", "AAAAAA", "002"
                ),
            ],
        )

        self.sdl_path = self.root / "Slide_Digitization_Log.xlsx"
        workbook = Workbook()
        worksheet = workbook.active
        worksheet.title = app_module.Config.SDL_SHEET_NAME
        worksheet.append(app_module.SDL_HEADERS)
        row = [None] * len(app_module.SDL_HEADERS)
        row[app_module.SDL_HEADERS.index("Accession ID")] = "NP25-100"
        row[app_module.SDL_HEADERS.index("Date Loaded")] = app_module.datetime.date(
            2026, 7, 20
        )
        row[app_module.SDL_HEADERS.index("Pushed to SFTP Server")] = False
        worksheet.append(row)
        workbook.save(self.sdl_path)
        workbook.close()

        self.tq_home = self.root / ".tq"
        self.tq_home.mkdir()
        (self.tq_home / "config.toml").write_text(
            'username = "operator"\nftp_addr = "sftp.example"\nftp_dir = "/transfer"\n',
            encoding="utf-8",
        )
        self.original_config = {
            "LABEL_CHECK_BATCHES": app_module.Config.LABEL_CHECK_BATCHES,
            "INSTANCE_DIR": app_module.Config.INSTANCE_DIR,
            "SDL_FILE_PATH": app_module.Config.SDL_FILE_PATH,
            "TQ_HOME_DIR": app_module.Config.TQ_HOME_DIR,
            "TQ_TRANSFER_LOG_DIR": app_module.Config.TQ_TRANSFER_LOG_DIR,
            "TQ_EXECUTABLE": app_module.Config.TQ_EXECUTABLE,
        }
        app_module.Config.LABEL_CHECK_BATCHES = str(self.batch_base)
        app_module.Config.INSTANCE_DIR = str(self.root / "instance")
        app_module.Config.SDL_FILE_PATH = str(self.sdl_path)
        app_module.Config.TQ_HOME_DIR = str(self.tq_home)
        app_module.Config.TQ_TRANSFER_LOG_DIR = str(self.batch_base / "transfer_logs")
        app_module.Config.TQ_EXECUTABLE = "tq"
        app_module.batch_contexts.clear()
        with app_module._tq_state_lock:
            app_module._tq_drafts.clear()
            app_module._tq_jobs.clear()
            app_module._tq_active_job_id = None

        self.old_users = app_module.user_manager.users.copy()
        self.user = app_module.User("tq-user", "", False)
        app_module.user_manager.users[self.user.id] = self.user
        app_module.app.config.update(TESTING=True, SECRET_KEY="tq-test")
        self.client = app_module.app.test_client()
        with self.client.session_transaction() as session:
            session["_user_id"] = self.user.id
            session["_fresh"] = True

    def tearDown(self):
        for name, value in self.original_config.items():
            setattr(app_module.Config, name, value)
        app_module.batch_contexts.clear()
        with app_module._tq_state_lock:
            app_module._tq_drafts.clear()
            app_module._tq_jobs.clear()
            app_module._tq_active_job_id = None
        app_module.user_manager.users = self.old_users
        self.temporary.cleanup()

    def catalog(self):
        slides, warnings = app_module._tq_catalog()
        self.assertEqual([], warnings)
        self.assertEqual(2, len(slides))
        return slides

    def test_catalog_filters_dates_and_builds_destination(self):
        slides = self.catalog()

        self.assertEqual("2026-07-20", slides[0]["digitization_date"])
        self.assertEqual(
            [slides[0]],
            app_module._tq_filtered_slides(
                slides, "SectionCount", "001", "", "", "none"
            ),
        )
        self.assertEqual(
            "destination/BRAIN/AAAAAA",
            app_module._tq_destination_dir("destination", slides[0]),
        )
        with self.assertRaises(app_module.TQError):
            app_module._tq_destination_dir("../outside", slides[0])

    def test_page_filters_pid_and_contains_transfer_navigation(self):
        response = self.client.get("/tq?filter=PID&filter_value=AAAAAA")

        self.assertEqual(200, response.status_code)
        self.assertIn(b"TQ Transfers", response.data)
        self.assertIn(b"Review Transfer", response.data)
        self.assertIn(b"AAAAAA", response.data)

    def test_filter_validation_rejects_bad_typed_values_and_date_ranges(self):
        self.assertIn(
            "six uppercase letters",
            app_module._tq_validate_filter("PID", "ABC", "", ""),
        )
        self.assertIn(
            "three digits",
            app_module._tq_validate_filter("SectionCount", "12", "", ""),
        )
        self.assertIn(
            "cannot precede",
            app_module._tq_validate_filter(
                "AccessionDate", "", "2026-07-20", "2026-07-19"
            ),
        )

    def test_review_and_transfer_route_use_authoritative_mapping_values(self):
        slides = self.catalog()
        response = self.client.post(
            "/tq/review", data={"slide_id": slides[0]["id"]}
        )
        self.assertEqual(302, response.status_code)
        fake_job = SimpleNamespace(id="job-id", status="running")
        with mock.patch.object(
            app_module, "_start_tq_job", return_value=fake_job
        ) as start:
            response = self.client.post(
                "/tq/transfer",
                data={f"prefix_{slides[0]['id']}": "destination"},
            )

        self.assertEqual(302, response.status_code)
        launched = start.call_args.args[1]
        self.assertEqual("BRAIN", launched[0]["organ"])
        self.assertEqual("AAAAAA", launched[0]["pid"])
        self.assertEqual(
            "destination/BRAIN/AAAAAA", launched[0]["destination_dir"]
        )
        with self.client.session_transaction() as session:
            self.assertEqual("job-id", session["tq_job_id"])

    def test_config_requires_all_connection_values(self):
        (self.tq_home / "config.toml").write_text(
            'username = "operator"\nftp_addr = ""\nftp_dir = "/transfer"\n',
            encoding="utf-8",
        )
        with self.assertRaisesRegex(app_module.TQError, "ftp_addr"):
            app_module._tq_config()

    def run_result_job(self, slide, all_slides):
        slide = dict(slide)
        slide["destination_dir"] = app_module._tq_destination_dir(
            "destination", slide
        )
        output = (
            app_module.json.dumps(
                {
                    "original_path": slide["original_path"],
                    "success": True,
                    "error": None,
                }
            )
            + "\n"
        ).encode()
        manifest = self.root / f"{slide['id']}.csv"
        manifest.write_text("manifest\n", encoding="utf-8")
        job = app_module.TQJob(
            slide["id"],
            self.user.id,
            FakeProcess(output),
            [slide],
            all_slides,
            manifest,
        )
        with app_module._tq_state_lock:
            app_module._tq_jobs[job.id] = job
            app_module._tq_active_job_id = job.id
        app_module._read_tq_output(job)
        return job

    def test_logs_pid_and_updates_sdl_only_after_all_slides_succeed(self):
        slides = self.catalog()

        first_job = self.run_result_job(slides[0], slides)
        with first_job.log_path.open("r", newline="", encoding="utf-8") as handle:
            first_log = list(csv.DictReader(handle))
            self.assertEqual(list(app_module.TQ_LOG_FIELDS), list(first_log[0]))
            self.assertEqual("AAAAAA", first_log[0]["pid"])
            self.assertEqual("SUCCESS", first_log[0]["status"])
        workbook = load_workbook(self.sdl_path)
        worksheet = workbook[app_module.Config.SDL_SHEET_NAME]
        columns = app_module._sdl_header_columns(worksheet)
        self.assertFalse(
            worksheet.cell(
                row=2, column=columns["Pushed to SFTP Server"]
            ).value
        )
        workbook.close()

        second_job = self.run_result_job(slides[1], slides)

        self.assertEqual("succeeded", second_job.status)
        workbook = load_workbook(self.sdl_path)
        worksheet = workbook[app_module.Config.SDL_SHEET_NAME]
        columns = app_module._sdl_header_columns(worksheet)
        self.assertTrue(
            worksheet.cell(
                row=2, column=columns["Pushed to SFTP Server"]
            ).value
        )
        workbook.close()

    def test_launcher_uses_manifest_and_never_uses_shell(self):
        slides = self.catalog()
        selected = [dict(slides[0])]
        selected[0]["destination_dir"] = app_module._tq_destination_dir(
            "destination", selected[0]
        )
        reader = mock.Mock()
        with mock.patch.object(
            app_module.subprocess, "Popen", return_value=FakeProcess()
        ) as popen, mock.patch.object(
            app_module.threading, "Thread", return_value=reader
        ):
            job = app_module._start_tq_job(self.user.id, selected, slides)

        command = popen.call_args.args[0]
        self.assertEqual(["tq", "pusher", "--paths"], command[:3])
        self.assertNotIn("shell", popen.call_args.kwargs)
        with Path(command[3]).open("r", newline="", encoding="utf-8") as handle:
            manifest_rows = list(csv.DictReader(handle))
        self.assertEqual(
            ["original_path", "destination_dir", "destination_name"],
            list(manifest_rows[0]),
        )
        self.assertEqual("running", job.status)
        reader.start.assert_called_once_with()

    def test_log_browser_rejects_symlink_escape(self):
        outside = self.root / "outside.log"
        outside.write_text("secret", encoding="utf-8")
        (self.tq_home / "escape.log").symlink_to(outside)

        response = self.client.get("/tq/logs?file=escape.log")

        self.assertEqual(200, response.status_code)
        self.assertIn(b"Symbolic links cannot be opened", response.data)
        self.assertNotIn(b"secret", response.data)


if __name__ == "__main__":
    unittest.main()
