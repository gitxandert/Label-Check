"""
This script implements a Flask web application designed for the manual review and correction
of data extracted from whole-slide images (WSIs). It provides a user interface for operators
to verify and amend information like Accession IDs and Stain types that have been processed
by an automated pipeline (e.g., OCR).

The application features:
- User authentication (login/logout) with role-based access (standard user vs. admin).
- An admin panel for user management (adding new users).
- A robust queuing system that "leases" data rows to users for a fixed duration to prevent
  simultaneous edits. Expired leases are automatically returned to the queue.
- Dynamic loading and saving of data from/to a central CSV file.
- Automatic creation of backups before saving any changes.
- A user-friendly interface displaying slide images (macro/label) and form fields for data entry.
- Logic to pre-fill information based on other slides from the same patient/case.
- A command-line interface (CLI) for initializing the database and user accounts.
"""

# ==============================================================================
# 1. IMPORTS
# ==============================================================================
import csv
import datetime
import logging
import os
import shutil
from collections import Counter, defaultdict
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Flask and its extensions for web framework, user management, and database
from flask import (
    Flask,
    flash,
    get_flashed_messages,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)
from flask.cli import with_appcontext
from flask_login import (
    LoginManager,
    UserMixin,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from werkzeug.security import check_password_hash, generate_password_hash


# ==============================================================================
# 2. CONFIGURATION
# ==============================================================================
class Config:
    """Central configuration class for the Flask application."""

    # A secret key is required for session management and security.
    # It should be a long, random string and kept secret in production.
    SECRET_KEY = os.environ.get(
        "SECRET_KEY", "a-super-secret-key-that-you-should-change"
    )

    # Database URI. For this app, we use a simple SQLite database file.
    SQLALCHEMY_DATABASE_URI = "sqlite:///users.db"
    # Disable an SQLAlchemy feature that is not needed and adds overhead.
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # The base directory where all data (images, CSV) is located.
    # The path is relative to the location of this script.
    IMAGE_BASE_DIR = r"..\NP-22-data"
    # The full path to the primary CSV file that the application reads from and writes to.
    CSV_FILE_PATH = os.path.join(IMAGE_BASE_DIR, "ocr_processed_parsed.csv")
    # Directory to store timestamped backups of the CSV before saving.
    BACKUP_DIR = "csv_backups"

    # Default password for the initial 'admin' user.
    # It is highly recommended to use an environment variable for this in production.
    ADMIN_DEFAULT_PASSWORD = os.environ.get(
        "ADMIN_DEFAULT_PASSWORD", "change_this_password"
    )

    # --- Queue Settings ---
    # The duration (in seconds) a user can hold a "lease" on a queue item before it's
    # automatically returned to the pool for others.
    LEASE_DURATION_SECONDS = 300  # 5 minutes


# ==============================================================================
# 3. LOGGING SETUP
# ==============================================================================
def setup_logging(app: Flask) -> None:
    """Configures comprehensive logging for the application.

    This setup creates two log handlers:
    1. A RotatingFileHandler to save detailed logs to a file (`logs/app.log`),
       which automatically rotates when the file size limit is reached.
    2. A StreamHandler to print informative logs to the console.
    """
    if not os.path.exists("logs"):
        os.mkdir("logs")

    # Handler for writing logs to a file.
    file_handler = RotatingFileHandler("logs/app.log", maxBytes=102400, backupCount=10)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]"
        )
    )
    file_handler.setLevel(logging.INFO)

    # Handler for printing logs to the console.
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    console_handler.setLevel(logging.INFO)

    # Add both handlers to the Flask application's logger.
    app.logger.addHandler(file_handler)
    app.logger.addHandler(console_handler)
    app.logger.setLevel(logging.INFO)
    app.logger.info("Application startup")


# ==============================================================================
# 4. APPLICATION & EXTENSIONS INITIALIZATION
# ==============================================================================
# --- Path Setup ---
# Determine absolute paths for the application's root, instance, and template folders.
# This makes the app runnable from any directory.
base_dir = os.path.abspath(os.path.dirname(__file__))
# The 'instance' folder is where Flask stores instance-specific files like the SQLite DB.
instance_path = os.path.join(base_dir, "instance")
template_dir = os.path.join(base_dir, "templates")

# --- Flask App Initialization ---
app = Flask(__name__, template_folder=template_dir, instance_path=instance_path)
app.config.from_object(Config)

# Ensure the instance directory exists; SQLAlchemy needs it to create the database file.
os.makedirs(app.instance_path, exist_ok=True)

# Initialize logging for the application.
setup_logging(app)

# --- Extensions Initialization ---
# Initialize SQLAlchemy for database operations.
db = SQLAlchemy(app)
# Initialize Flask-Login for handling user sessions.
login_manager = LoginManager()
login_manager.init_app(app)
# Redirect users to the 'login' page if they try to access a protected page without being logged in.
login_manager.login_view = "login"
login_manager.login_message = "Please log in to access this page."
login_manager.login_message_category = "warning"


# ==============================================================================
# 5. CUSTOM EXCEPTIONS
# ==============================================================================
class DataLoadError(Exception):
    """Custom exception raised for errors during the CSV data loading process."""
    pass


class DataSaveError(Exception):
    """Custom exception raised for errors during the CSV data saving process."""
    pass


class BackupError(Exception):
    """Custom exception raised for errors during the backup creation process."""
    pass


# ==============================================================================
# 6. DATABASE MODELS (Using Flask-SQLAlchemy)
# ==============================================================================
class User(UserMixin, db.Model):
    """Represents a user account in the database."""
    # The user's unique identifier (e.g., username).
    id = db.Column(db.String(80), primary_key=True, unique=True, nullable=False)
    # The hashed password for security.
    password_hash = db.Column(db.String(128), nullable=False)
    # A simple counter for user activity/stats.
    correction_count = db.Column(db.Integer, default=0, nullable=False)
    # A boolean flag to determine if the user has administrative privileges.
    is_admin = db.Column(db.Boolean, default=False, nullable=False)

    def set_password(self, password: str) -> None:
        """Hashes the provided password and stores it."""
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password: str) -> bool:
        """Checks if the provided password matches the stored hash."""
        return check_password_hash(self.password_hash, password)

    def __repr__(self) -> str:
        return f"<User {self.id}>"


class QueueItem(db.Model):
    """Represents a single row from the CSV in the processing queue.

    This model tracks the state of each item (e.g., slide) to prevent multiple
    users from working on the same item simultaneously.
    """
    id = db.Column(db.Integer, primary_key=True)
    # The original 0-based index of the row in the CSV file. This links the
    # queue item back to the in-memory data.
    original_index = db.Column(db.Integer, nullable=False, index=True, unique=True)
    # The current status of the item:
    # - 'pending': Available for any user to take.
    # - 'leased': Currently assigned to a user.
    # - 'completed': Work is finished for this item.
    status = db.Column(db.String(20), nullable=False, default="pending", index=True)
    # Foreign key to the User who has leased this item.
    leased_by_id = db.Column(db.String(80), db.ForeignKey("user.id"), nullable=True, index=True)
    # Timestamp of when the lease was granted.
    leased_at = db.Column(db.DateTime, nullable=True)
    # Foreign key to the User who completed this item.
    completed_by_id = db.Column(db.String(80), db.ForeignKey("user.id"), nullable=True, index=True)
    # Timestamp of when the item was marked as complete.
    completed_at = db.Column(db.DateTime, nullable=True)

    # SQLAlchemy relationships to easily access the User objects.
    leased_by = db.relationship("User", foreign_keys=[leased_by_id], backref="leases")
    completed_by = db.relationship("User", foreign_keys=[completed_by_id], backref="completed_items")

    def __repr__(self) -> str:
        return f"<QueueItem {self.original_index} - {self.status}>"


@login_manager.user_loader
def load_user(user_id: str) -> Optional[User]:
    """Flask-Login callback function to load a user from the database by their ID."""
    return db.session.get(User, user_id)


# ==============================================================================
# 7. GLOBAL DATA STORE
# ==============================================================================
# In-memory storage for the CSV data. This is simple and fast but has a key limitation:
# it is NOT safe for production environments with multiple workers (like Gunicorn),
# as each worker would have its own separate copy of the data.
# This approach is suitable for single-worker development or small-scale deployments.
data: List[Dict[str, Any]] = []
headers: List[str] = []


# ==============================================================================
# 8. HELPER FUNCTIONS
# ==============================================================================
def _release_expired_leases():
    """Scans for and releases any item leases that have expired.

    This function is a critical part of the queue system. It finds all items with a
    'leased' status whose `leased_at` timestamp is older than the configured lease
    duration and resets their status to 'pending'.
    """
    lease_duration = datetime.timedelta(seconds=app.config["LEASE_DURATION_SECONDS"])
    expired_time = datetime.datetime.utcnow() - lease_duration

    # Find all items that are leased and whose lease time has passed.
    expired_items = db.session.execute(
        db.select(QueueItem).filter(
            QueueItem.status == "leased", QueueItem.leased_at < expired_time
        )
    ).scalars().all()

    if expired_items:
        count = 0
        for item in expired_items:
            # A safety check to prevent errors if the CSV was reloaded and is now shorter.
            if item.original_index < len(data):
                acc_id = data[item.original_index].get("AccessionID", "Unknown")
                app.logger.info(
                    f"Lease expired for item {item.original_index} ({acc_id}), leased by {item.leased_by_id}."
                )
                # Reset the item's state.
                item.status = "pending"
                item.leased_by_id = None
                item.leased_at = None
                count += 1
        db.session.commit()
        if count > 0:
            flash(
                f"{count} item(s) had expired leases and were returned to the queue.",
                "warning",
            )


def _recalculate_accession_counts() -> None:
    """Recalculates and updates the usage count for each AccessionID.

    This function iterates through all rows in the global `data` list, counts the
    occurrences of each AccessionID, and stores the count in a special `_accession_id_count`
    field. This is used in the UI to alert users if an ID is used multiple times.
    """
    global data
    if not data:
        return

    # Use collections.Counter for an efficient way to count hashable objects.
    id_counts = Counter(
        row.get("AccessionID", "").strip()
        for row in data
        if row.get("AccessionID", "").strip()
    )
    # Update each row with the calculated count.
    for row in data:
        current_id = row.get("AccessionID", "").strip()
        row["_accession_id_count"] = id_counts[current_id] if current_id else 0


def _is_row_incomplete(row_dict: Dict[str, Any]) -> bool:
    """Checks if a data row is marked as incomplete based on internal metadata."""
    return not row_dict.get("_is_complete", False)


def get_current_display_list_indices() -> List[int]:
    """Returns a list of original data indices to be displayed.

    This function respects the user's current filter preference, which is stored
    in the session. It returns all indices if the user wants to see all items, or
    only the indices of incomplete items.
    """
    if not data:
        return []
    if session.get("show_only_incomplete"):
        return [i for i, row in enumerate(data) if _is_row_incomplete(row)]
    return list(range(len(data)))


# ==============================================================================
# 9. CORE DATA I/O FUNCTIONS
# ==============================================================================
def load_csv_data(file_path: str = Config.CSV_FILE_PATH) -> None:
    """Loads and processes data from the specified CSV file into global memory.

    This function reads the CSV, enriches each row with internal metadata
    (like `_original_index`, `_identifier`, `_is_complete`), calculates
    patient-level statistics, and populates the global `data` and `headers` lists.
    """
    global data, headers
    app.logger.info(f"Loading CSV data from: {file_path}")

    if not os.path.exists(file_path):
        raise DataLoadError(f"CSV file not found: {file_path}")

    _data: List[Dict[str, Any]] = []
    # These headers are essential for the application to function correctly.
    critical_headers = [
        "AccessionID", "Stain", "ParsingQCPassed", "original_slide_location"
    ]

    try:
        with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile, delimiter=",")
            _headers = reader.fieldnames

            if not _headers:
                raise DataLoadError("CSV file is empty or has no header.")
            
            # Check for missing critical headers and log a warning if any are found.
            missing = [h for h in critical_headers if h not in _headers]
            if missing:
                app.logger.warning(
                    f"CSV is missing expected headers: {missing}. Functionality may be limited."
                )

            for i, row in enumerate(reader):
                # --- Add Internal Metadata (prefixed with '_') ---
                # Store the original row number.
                row["_original_index"] = i
                # Create a unique identifier for the patient/case from the filename stem.
                orig_path = row.get("original_slide_location")
                row["_identifier"] = Path(orig_path).stem if orig_path else f"Unknown_{i}"
                # Map OCR text columns to internal keys for consistent access.
                row["_label_text"] = row.get("label_text", "N/A")
                row["_macro_text"] = row.get("macro_text", "N/A")
                # Map image path columns to internal keys.
                row["_label_path"] = row.get("label_path")
                row["_macro_path"] = row.get("macro_path")
                
                # --- Standardize Editable Fields ---
                # Ensure these fields exist and strip whitespace.
                row["AccessionID"] = row.get("AccessionID", "").strip()
                row["Stain"] = row.get("Stain", "").strip()
                # The pipeline may not produce a BlockNumber, but we add the key to allow user input.
                row["BlockNumber"] = row.get("BlockNumber", "").strip()
                
                # --- Determine Completion Status ---
                # Convert the 'ParsingQCPassed' column (e.g., "TRUE") to a boolean.
                qc_passed_str = row.get("ParsingQCPassed", "").strip()
                row["_is_complete"] = bool(qc_passed_str)
                _data.append(row)

        # --- Post-processing: Calculate per-patient file statistics ---
        # Group rows by the patient identifier.
        patient_slide_ids = defaultdict(list)
        for i, row in enumerate(_data):
            patient_slide_ids[row["_identifier"]].append(i)
        
        # Annotate each row with its position within its patient group (e.g., "File 1 of 3").
        for _, original_indices in patient_slide_ids.items():
            total = len(original_indices)
            for j, original_idx in enumerate(sorted(original_indices)):
                _data[original_idx]["_total_patient_files"] = total
                _data[original_idx]["_patient_file_number"] = j + 1

        # Atomically update the global variables.
        data, headers = _data, _headers
        _recalculate_accession_counts()
        app.logger.info(f"Loaded {len(data)} rows.")

    except Exception as e:
        # If loading fails, clear the global data to prevent using stale/corrupt data.
        data, headers = [], []
        raise DataLoadError(f"Error reading CSV: {e}")


def save_csv_data(target_path: str = Config.CSV_FILE_PATH) -> None:
    """Saves the current in-memory data back to the CSV file.

    This function performs an atomic write by first writing to a temporary file
    and then replacing the original file. This prevents data corruption if the
    application crashes during the save operation.
    """
    global data, headers
    if not data or not headers:
        app.logger.warning("Save aborted: No data in memory.")
        return

    app.logger.info(f"Saving {len(data)} rows to {target_path}")

    # Define the desired order of columns in the output CSV file.
    priority_fields = ["AccessionID", "Stain", "BlockNumber", "ParsingQCPassed"]
    # Get all other original headers.
    pipeline_fields = [h for h in headers if h not in priority_fields]
    # Combine them, ensuring no duplicates and preserving order.
    fieldnames = list(dict.fromkeys(priority_fields + pipeline_fields))
    
    # Use a temporary file for the initial write.
    temp_path = target_path + ".tmp"
    try:
        with open(temp_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=fieldnames,
                delimiter=",",
                extrasaction="ignore",  # Ignore our internal '_' fields.
                quoting=csv.QUOTE_MINIMAL,
            )
            writer.writeheader()

            for row in data:
                write_row = row.copy()
                # Map the internal `_is_complete` boolean back to the CSV string representation.
                write_row["ParsingQCPassed"] = "TRUE" if row.get("_is_complete") else ""
                writer.writerow(write_row)

        # If writing to temp file succeeds, replace the original file. This is an atomic operation.
        os.replace(temp_path, target_path)
        # Update the last modified time in the session to prevent immediate auto-reloading.
        session["last_loaded_csv_mod_time"] = os.path.getmtime(target_path)
        app.logger.info("Save successful.")

    except Exception as e:
        # If an error occurs, clean up the temporary file.
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise DataSaveError(f"Failed to save CSV: {e}")


def _create_backup() -> None:
    """Creates a timestamped backup of the current CSV file."""
    if not os.path.exists(Config.CSV_FILE_PATH):
        return
    try:
        os.makedirs(Config.BACKUP_DIR, exist_ok=True)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.basename(Config.CSV_FILE_PATH)
        backup_path = os.path.join(Config.BACKUP_DIR, f"{filename}_{timestamp}.bak")
        shutil.copy2(Config.CSV_FILE_PATH, backup_path)
    except Exception as e:
        raise BackupError(f"Backup failed: {e}")


# ==============================================================================
# 10. FLASK ROUTES
# ==============================================================================
@app.before_request
def before_request_handler():
    """A function that runs before every request.

    It handles two main tasks:
    1. Checks if the CSV file on disk has been modified by an external process.
       If so, it automatically reloads the data to ensure the app is up-to-date.
    2. Ensures the session variable for the incomplete filter is initialized.
    """
    # Exclude certain routes (like static files and login) from this check.
    if request.endpoint in ["static", "serve_relative_image", "login", "logout"]:
        return

    session.setdefault("show_only_incomplete", False)

    path = Config.CSV_FILE_PATH
    if not os.path.exists(path):
        if data:  # File was deleted while the app was running.
