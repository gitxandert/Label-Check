import sys
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
        }

    def test_valid_credentials_are_accepted(self):
        app_module.validate_security_config(self.valid_config())

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


if __name__ == "__main__":
    unittest.main()
