import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import app as app_module


class SecurityConfigurationTests(unittest.TestCase):
    def valid_config(self):
        return {
            "SECRET_KEY": "s" * 32,
            "ADMIN_DEFAULT_PASSWORD": "strong-admin-password",
            "PIPELINE_INPUT_ROOTS": ("/input",),
            "PIPELINE_OUTPUT_ROOTS": ("/output",),
            "PIPELINE_MAX_WORKERS": 8,
            "PIPELINE_MAX_THUMBNAIL_DIMENSION": 4096,
            "LOGIN_PAIR_ATTEMPT_LIMIT": 5,
            "LOGIN_ACCOUNT_ATTEMPT_LIMIT": 10,
            "LOGIN_RATE_WINDOW_SECONDS": 900,
        }

    def test_valid_credentials_are_accepted(self):
        app_module.validate_security_config(self.valid_config())

    def test_compose_exposes_application_only_on_loopback(self):
        compose = (SRC_DIR.parent / "compose.yaml").read_text(encoding="utf-8")
        self.assertIn('"127.0.0.1:${HOST_PORT:-5000}:5000"', compose)
        self.assertNotIn('- "${HOST_PORT:-5000}:5000"', compose)

    def test_missing_and_short_credentials_are_rejected_without_values(self):
        cases = [
            ({}, "SECRET_KEY"),
            (
                {"SECRET_KEY": "short", "ADMIN_DEFAULT_PASSWORD": "short"},
                "SECRET_KEY",
            ),
        ]
        for configuration, expected_name in cases:
            with self.subTest(configuration=configuration):
                with self.assertRaises(app_module.SecurityConfigurationError) as raised:
                    app_module.validate_security_config(configuration)
                message = str(raised.exception)
                self.assertIn(expected_name, message)
                self.assertNotIn("short", message)

    def test_documented_and_legacy_defaults_are_rejected(self):
        for key, value in (
            ("SECRET_KEY", "a-super-secret-key-that-you-should-change"),
            ("SECRET_KEY", "replace-with-a-long-random-value"),
            ("ADMIN_DEFAULT_PASSWORD", "change_this_password"),
            ("ADMIN_DEFAULT_PASSWORD", "replace-before-first-start"),
        ):
            configuration = self.valid_config()
            configuration[key] = value
            with self.subTest(key=key, value=value):
                with self.assertRaisesRegex(
                    app_module.SecurityConfigurationError, key
                ):
                    app_module.validate_security_config(configuration)

    def test_production_request_fails_closed_when_credentials_are_invalid(self):
        original = {
            "TESTING": app_module.app.config.get("TESTING"),
            "SECRET_KEY": app_module.app.config.get("SECRET_KEY"),
            "ADMIN_DEFAULT_PASSWORD": app_module.app.config.get(
                "ADMIN_DEFAULT_PASSWORD"
            ),
        }
        try:
            app_module.app.config.update(
                TESTING=False,
                SECRET_KEY="short",
                ADMIN_DEFAULT_PASSWORD="short",
            )
            response = app_module.app.test_client().get("/login")
            self.assertEqual(500, response.status_code)
            self.assertEqual(
                b"Server security configuration is invalid.", response.data
            )
        finally:
            app_module.app.config.update(original)

    def test_init_db_does_not_create_admin_with_invalid_credentials(self):
        original = {
            "SECRET_KEY": app_module.app.config.get("SECRET_KEY"),
            "ADMIN_DEFAULT_PASSWORD": app_module.app.config.get(
                "ADMIN_DEFAULT_PASSWORD"
            ),
        }
        try:
            app_module.app.config.update(
                SECRET_KEY="short",
                ADMIN_DEFAULT_PASSWORD="short",
            )
            with mock.patch.object(app_module.user_manager, "add") as add_user:
                result = app_module.app.test_cli_runner().invoke(args=["init-db"])
            self.assertNotEqual(0, result.exit_code)
            self.assertIn("SECRET_KEY", result.output)
            add_user.assert_not_called()
        finally:
            app_module.app.config.update(original)

    def test_runtime_permissions_are_repaired_and_symlinks_rejected(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            instance = root / "instance"
            logs = root / "logs"
            output = instance / "pipeline_job_output"
            output.mkdir(parents=True)
            logs.mkdir()
            sensitive = instance / "users.csv"
            sensitive.write_text("password_hash\n", encoding="utf-8")
            app_log = logs / "app.log"
            app_log.write_text("sensitive path\n", encoding="utf-8")
            os.chmod(instance, 0o755)
            os.chmod(output, 0o755)
            os.chmod(sensitive, 0o644)
            os.chmod(logs, 0o755)
            os.chmod(app_log, 0o644)

            app_module.harden_runtime_permissions(instance, logs)

            self.assertEqual(0o700, stat.S_IMODE(instance.stat().st_mode))
            self.assertEqual(0o700, stat.S_IMODE(output.stat().st_mode))
            self.assertEqual(0o600, stat.S_IMODE(sensitive.stat().st_mode))
            self.assertEqual(0o600, stat.S_IMODE(app_log.stat().st_mode))

            linked = instance / "linked-state"
            try:
                linked.symlink_to(root, target_is_directory=True)
            except OSError:
                self.skipTest("Directory symlinks are unavailable")
            with self.assertRaisesRegex(RuntimeError, "symbolic link"):
                app_module.harden_runtime_permissions(instance, logs)

    def test_api_store_creates_private_database_and_job_output(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            store = app_module.APIStore(
                str(root / "state" / "api.sqlite3"),
                str(root / "state" / "pipeline-output"),
            )
            output_path = Path(
                store.reserve_job("job", "owner", {}, ["pipeline"])
            )

            self.assertEqual(
                0o600, stat.S_IMODE((root / "state" / "api.sqlite3").stat().st_mode)
            )
            self.assertEqual(0o600, stat.S_IMODE(output_path.stat().st_mode))
            self.assertEqual(0o700, stat.S_IMODE(output_path.parent.stat().st_mode))

    def test_password_policy_enforces_length_without_trimming(self):
        self.assertIsNotNone(app_module.password_policy_error("x" * 11))
        self.assertIsNone(app_module.password_policy_error(" " + "x" * 11))
        self.assertIsNone(app_module.password_policy_error("x" * 128))
        self.assertIsNotNone(app_module.password_policy_error("x" * 129))
        self.assertIsNotNone(
            app_module.password_policy_error("replace-before-first-start")
        )

    def test_login_throttle_is_durable_and_success_clears_failures(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            original_store = (
                app_module.api_store.db_path,
                app_module.api_store.output_dir,
            )
            original_users = app_module.user_manager.users.copy()
            original_config = {
                key: app_module.app.config.get(key)
                for key in (
                    "TESTING",
                    "SECRET_KEY",
                    "LOGIN_PAIR_ATTEMPT_LIMIT",
                    "LOGIN_ACCOUNT_ATTEMPT_LIMIT",
                    "LOGIN_RATE_WINDOW_SECONDS",
                )
            }
            try:
                database = str(root / "api.sqlite3")
                output = str(root / "output")
                app_module.api_store.configure(database, output)
                user = app_module.User("login-user", "")
                user.set_password("valid-password-123")
                app_module.user_manager.users[user.id] = user
                app_module.app.config.update(
                    TESTING=True,
                    SECRET_KEY="login-test-secret",
                    LOGIN_PAIR_ATTEMPT_LIMIT=2,
                    LOGIN_ACCOUNT_ATTEMPT_LIMIT=3,
                    LOGIN_RATE_WINDOW_SECONDS=900,
                )
                client = app_module.app.test_client()

                first = client.post(
                    "/login",
                    data={"username": user.id, "password": "wrong"},
                    environ_base={"REMOTE_ADDR": "192.0.2.10"},
                )
                second = client.post(
                    "/login",
                    data={"username": user.id, "password": "wrong"},
                    environ_base={"REMOTE_ADDR": "192.0.2.10"},
                )
                app_module.api_store.configure(database, output)
                after_restart = client.post(
                    "/login",
                    data={"username": user.id, "password": "wrong"},
                    environ_base={"REMOTE_ADDR": "192.0.2.10"},
                )

                self.assertEqual(200, first.status_code)
                self.assertEqual(429, second.status_code)
                self.assertIn("Retry-After", second.headers)
                self.assertEqual(429, after_restart.status_code)

                app_module.api_store.clear_login_failures(user.id, "192.0.2.10")
                failed_once = client.post(
                    "/login",
                    data={"username": user.id, "password": "wrong"},
                    environ_base={"REMOTE_ADDR": "192.0.2.10"},
                )
                succeeded = client.post(
                    "/login",
                    data={"username": user.id, "password": "valid-password-123"},
                    environ_base={"REMOTE_ADDR": "192.0.2.10"},
                )
                allowed, _retry = app_module.api_store.login_rate_limit(
                    user.id, "192.0.2.10", 1, 1, 900
                )
                self.assertEqual(200, failed_once.status_code)
                self.assertEqual(302, succeeded.status_code)
                self.assertTrue(allowed)
            finally:
                app_module.app.config.update(original_config)
                app_module.user_manager.users = original_users
                app_module.api_store.configure(*original_store)

    def test_login_account_limit_combines_failures_across_addresses(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            store = app_module.APIStore(
                str(root / "api.sqlite3"), str(root / "output")
            )
            for client_address in ("192.0.2.1", "192.0.2.2", "192.0.2.3"):
                store.record_login_failure("target-user", client_address, 900)

            allowed, retry_after = store.login_rate_limit(
                "target-user", "192.0.2.4", 100, 3, 900
            )

            self.assertFalse(allowed)
            self.assertGreater(retry_after, 0)


if __name__ == "__main__":
    unittest.main()
