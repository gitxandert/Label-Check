import csv
import os
import re
import shutil
import datetime
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, flash, session, jsonify
from flask import get_flashed_messages
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from collections import defaultdict
from flask_sqlalchemy import SQLAlchemy # New import

app = Flask(__name__)
app.secret_key = 'even_more_secret_key_for_robust_session' # MUST be set for session

# --- Configuration ---
# SQLAlchemy Database Configuration
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db' # SQLite database file
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False # Disable tracking modifications overhead
db = SQLAlchemy(app) # Initialize SQLAlchemy

# !!! IMPORTANT: VERIFY THESE PATHS ARE CORRECT FOR YOUR SYSTEM !!!
CSV_FILE_PATH = r'C:\Users\sthakur1\Downloads\Work\WSI-Processing\Correct-Me\NP22-assoc\output_processed.csv'
IMAGE_BASE_DIR = r'C:\Users\sthakur1\Downloads\Work\WSI-Processing\Correct-Me\NP22-assoc' # This should be the parent directory of 'label' and 'macro' folders
BACKUP_DIR = 'csv_backups' # Directory to store backups
INTERMEDIATE_BACKUP_FILE = r'C:\Users\sthakur1\Downloads\Work\WSI-Processing\Correct-Me\NP22-assoc\output_processed_backup.csv'
INTERMEDIATE_BACKUP_FREQUENCY = 10 # Save recovery file every 10 updates
# --- End Configuration ---

# --- Flask-Login Setup ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' # Redirects to this endpoint if user is not logged in
login_manager.login_message = "Please log in to access this page."
login_manager.login_message_category = "warning"

# --- User Model for Flask-Login with SQLAlchemy ---
class User(UserMixin, db.Model):
    id = db.Column(db.String(80), primary_key=True, unique=True, nullable=False) # Username as ID
    password_hash = db.Column(db.String(128), nullable=False)
    correction_count = db.Column(db.Integer, default=0, nullable=False)
    is_admin = db.Column(db.Boolean, default=False, nullable=False)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password):
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return f'<User {self.id}>'

# --- User Loader for Flask-Login ---
@login_manager.user_loader
def load_user(user_id):
    return User.query.get(user_id)

# --- Global Data Store & State (OCR data) ---
data = [] # In-memory list of dictionaries representing CSV rows
headers = []
# Session stores:
# session['show_only_incomplete'] = True/False
# session['update_counter'] = integer
# session['last_loaded_csv_mod_time'] = float (timestamp)
# --- End Global Data Store ---

# --- Helper Functions ---

def parse_original_line(line_str):
    """
    Parses the 'OriginalLine' string to extract the patient identifier,
    and the text from 'Label:' and 'Macro:' fields.
    """
    identifier = None
    label_text = "N/A"
    macro_text = "N/A"

    # The 'OriginalLine' is from output_processed.csv, which itself is from output-ocr.csv
    # The format of 'OriginalLine' looks like:
    # "Patient;Label_Path:path/to/patient_label.png;Macro_Path:path/to/patient_macro.png;Label: detected_label_text;Macro: detected_macro_text"
    # The first field (before the first ';') is the patient ID as output by run_dual_ocr.py
    parts = line_str.split(';')
    if parts:
        identifier = parts[0].strip().replace('"', '') # Clean up potential quotes from the Patient field

    # Extract Label and Macro text using regex for robustness
    label_match = re.search(r'Label:\s*(.*?)(?:;Macro:|$)', line_str)
    if label_match:
        label_text = label_match.group(1).strip()
        # Remove trailing quote if present, from how run_dual_ocr.py combines it (e.g., if text ends in quotes)
        if label_text.endswith('"'):
            label_text = label_text[:-1]

    macro_match = re.search(r'Macro:\s*(.*?)(?:;|$)', line_str)
    if macro_match:
        macro_text = macro_match.group(1).strip()
        if macro_text.endswith('"'):
            macro_text = macro_text[:-1]

    return identifier, label_text, macro_text


def _is_row_incomplete(row_dict):
    """Checks the boolean '_is_complete' flag in a row dictionary."""
    return not row_dict.get('_is_complete', False) # Default to False if missing

def get_current_display_list_indices():
    """Returns a list of original_indices based on the current filter."""
    filter_active = session.get('show_only_incomplete', False)
    if not data:
        return []
    if filter_active:
        return [i for i, row in enumerate(data) if _is_row_incomplete(row)]
    else:
        return list(range(len(data))) # All indices

def get_display_info_for_original_index(original_index):
     """Given an original_index, find its position in the current display list."""
     display_indices = get_current_display_list_indices()
     try:
         # Find where original_index appears in the filtered list
         display_index = display_indices.index(original_index)
         return {
             'display_index': display_index, # 0-based position in the filtered list
             'total_display_count': len(display_indices)
         }
     except ValueError:
         # The requested original_index is not part of the current filter view
         return None

def find_navigation_index(current_original_index, direction):
    """
    Finds the next/previous original_index based on direction and filter.
    direction: 'next', 'prev', 'next_incorrect'
    Returns the target original_index or None if not found.
    """
    if not data: return None

    display_indices = get_current_display_list_indices()
    if not display_indices: return None # Nothing to navigate within

    try:
        current_display_pos = display_indices.index(current_original_index)
    except ValueError:
        # Current index isn't in the display list (shouldn't happen if called correctly)
        # Default to the first item in the display list
        return display_indices[0] if display_indices else None


    if direction == 'next':
        next_display_pos = current_display_pos + 1
        if next_display_pos < len(display_indices):
            return display_indices[next_display_pos]
        else:
            flash("You are at the end of the list.", "info")
            return None # At the end
    elif direction == 'prev':
        prev_display_pos = current_display_pos - 1
        if prev_display_pos >= 0:
            return display_indices[prev_display_pos]
        else:
            flash("You are at the beginning of the list.", "info")
            return None # At the beginning
    elif direction == 'next_incorrect':
        # Find the first original_index *after* current_original_index that is incomplete
        # We need to search within the 'display_indices' because the filter might be active
        for i in range(current_display_pos + 1, len(display_indices)):
            next_original_index_in_view = display_indices[i]
            if _is_row_incomplete(data[next_original_index_in_view]):
                return next_original_index_in_view
        # If not found after current position, search from beginning of filtered list
        # This allows "looping" to the next incomplete if none are found ahead
        for i in range(current_display_pos + 1): # loop up to and including current position (if filtered)
             next_original_index_in_view = display_indices[i]
             if _is_row_incomplete(data[next_original_index_in_view]):
                 flash("No further incomplete rows found after current. Restarting search from beginning.", "info")
                 return next_original_index_in_view

        flash("No incomplete rows found.", "info")
        return None # Stay on current index if none found

    return None # Should not be reached

def load_csv_data(file_path=CSV_FILE_PATH):
    """
    Loads data from CSV into global variables `data` and `headers`.
    Includes logic for patient file counting.
    Returns True on success, False on failure. DOES NOT touch session.
    """
    global data, headers
    print(f"INFO: Attempting to load CSV data from: {file_path}")

    _data = []
    _headers = []
    # These are the expected headers from the output_processed.csv (from name-files.py)
    required_headers_for_processed_csv = ['AccessionID', 'Stain', 'Complete', 'OriginalLine']


    try:
        if not os.path.exists(file_path):
            print(f"ERROR: CSV file not found at '{file_path}'")
            return False

        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            if not reader.fieldnames:
                print(f"ERROR: CSV file '{os.path.basename(file_path)}' appears empty or has no headers.")
                return False
            _headers = reader.fieldnames
            if not all(h in _headers for h in required_headers_for_processed_csv):
                print(f"ERROR: CSV '{os.path.basename(file_path)}' missing required headers: {required_headers_for_processed_csv}")
                return False

            for i, row in enumerate(reader):
                row['_original_index'] = i
                # OriginalLine holds the raw combined text, from which we extract patient ID and OCR texts
                identifier, label, macro = parse_original_line(row.get('OriginalLine', ''))
                row['_identifier'] = identifier # This is the base patient ID derived from OriginalLine
                row['_label_text'] = label
                row['_macro_text'] = macro
                complete_val = str(row.get('Complete', '')).strip().lower()
                row['_is_complete'] = complete_val == 'true'
                # Ensure AccessionID and Stain are present, even if empty from CSV
                row['AccessionID'] = row.get('AccessionID', '')
                row['Stain'] = row.get('Stain', '')
                _data.append(row)

        # --- Patient File Counting Logic ---
        patient_slide_identifiers = defaultdict(list) # Stores lists of original_indices for each patient
        for i, row in enumerate(_data):
            if row['_identifier']: # Use the derived identifier
                patient_slide_identifiers[row['_identifier']].append(i) # Add original index

        for identifier, original_indices in patient_slide_identifiers.items():
            total_for_patient = len(original_indices)
            # Sort original_indices to ensure consistent numbering (e.g., if files are NP-001_1, NP-001_2)
            original_indices.sort() # Ensure numerical order for patient files
            for j, original_index in enumerate(original_indices):
                _data[original_index]['_total_patient_files'] = total_for_patient
                _data[original_index]['_patient_file_number'] = j + 1 # 1-based index

        data = _data
        headers = _headers
        print(f"INFO: Successfully loaded {len(data)} rows from {os.path.basename(file_path)}")
        return True

    except FileNotFoundError:
        print(f"ERROR: CSV file not found during open operation at '{file_path}'")
        data = []
        headers = []
        return False
    except Exception as e:
        print(f"ERROR: Exception reading CSV '{os.path.basename(file_path)}': {e}")
        data = []
        headers = []
        return False

def save_csv_data(target_path=CSV_FILE_PATH):
    """Saves in-memory data to the specified path. Returns True on success."""
    global data, headers
    if not headers:
        flash("Error: Cannot save, headers not loaded.", "error")
        return False

    # Define the core headers that should always be written, in a specific order
    core_headers = ['AccessionID', 'Stain', 'Complete', 'OriginalLine']
    # Get any other original headers that are not internal fields and not already in core_headers
    other_original_headers = [
        h for h in headers
        if not h.startswith('_') and h not in core_headers
    ]
    # Combine to form the final write order for the CSV
    write_headers = core_headers + sorted(other_original_headers) # Sort others for consistency

    try:
        temp_path = target_path + ".tmp"
        with open(temp_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=write_headers, delimiter=';', quoting=csv.QUOTE_NONNUMERIC)
            writer.writeheader()
            for row in data:
                write_row = {}
                for h in write_headers:
                    if h == 'Complete':
                        # Ensure 'Complete' is written as "True" or "False" based on our internal boolean flag
                        write_row[h] = str(row.get('_is_complete', False)).capitalize()
                    else:
                        # For all other headers, use the value from the row, default to empty string
                        write_row[h] = row.get(h, '')
                writer.writerow(write_row)

        os.replace(temp_path, target_path) # Atomic replace on most OS
        session['last_loaded_csv_mod_time'] = os.path.getmtime(target_path) # Update mod time
        return True
    except Exception as e:
        flash(f"Error saving data to {target_path}: {e}", "error")
        if os.path.exists(temp_path):
             try: os.remove(temp_path) # Clean up temp file
             except OSError: pass
        return False

def _create_backup():
    """Creates timestamped backup. Returns True on success."""
    if not os.path.exists(CSV_FILE_PATH):
        flash("Warning: Original CSV not found, cannot create backup.", "warning")
        return False
    try:
        if not os.path.exists(BACKUP_DIR): os.makedirs(BACKUP_DIR)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{os.path.basename(CSV_FILE_PATH)}_{timestamp}.bak"
        backup_path = os.path.join(BACKUP_DIR, backup_filename)
        shutil.copy2(CSV_FILE_PATH, backup_path)
        flash(f"Backup created: {backup_path}", "info")
        return True
    except Exception as e:
        flash(f"Error creating backup: {e}", "error")
        return False

def save_intermediate_backup():
    """Saves current state to recovery file. Called periodically."""
    global data
    if not data: return # Nothing to save

    print(f"Attempting intermediate backup to {INTERMEDIATE_BACKUP_FILE}...") # Log attempt
    if save_csv_data(target_path=INTERMEDIATE_BACKUP_FILE):
        print(f"Intermediate backup successful.")
        session['update_counter'] = 0 # Reset counter ONLY on successful save
    else:
        # Error flashed by save_csv_data
        print(f"Intermediate backup FAILED.")

# --- Flask Routes ---

@app.before_request
def ensure_data_loaded():
    """
    Ensures data is loaded before each request (except static/images and login/logout).
    Checks modification time and reloads if necessary. Handles session setup.
    """
    # Skip for static files, image serving endpoints, and specific user/login routes
    if request.endpoint in ['static', 'serve_image', 'login', 'logout', 'add_user', 'users_management']:
        return

    # Initialize session variables if they don't exist
    if 'show_only_incomplete' not in session:
        session['show_only_incomplete'] = False
    if 'update_counter' not in session:
        session['update_counter'] = 0
    if 'last_loaded_csv_mod_time' not in session:
        session['last_loaded_csv_mod_time'] = 0

    # --- Check if reload is needed ---
    reload_needed = False
    if not data: # Always load if data is empty
        reload_needed = True
        print("INFO [Request]: Data is empty, attempting initial load.")
    else:
        try:
            current_mod_time = os.path.getmtime(CSV_FILE_PATH)
            if current_mod_time != session.get('last_loaded_csv_mod_time', 0):
                reload_needed = True
                print(f"INFO [Request]: CSV file '{os.path.basename(CSV_FILE_PATH)}' modified, reloading.")
        except FileNotFoundError:
             flash(f"Error: Main CSV file '{CSV_FILE_PATH}' not found during request.", "error")
             return
        except Exception as e:
             flash(f"Warning: Could not check CSV modification time: {e}", "warning")

    if reload_needed:
        if load_csv_data(file_path=CSV_FILE_PATH):
             try:
                session['last_loaded_csv_mod_time'] = os.path.getmtime(CSV_FILE_PATH)
                session['update_counter'] = 0 # Reset counter on reload
                flash(f"Data reloaded successfully from {os.path.basename(CSV_FILE_PATH)}.", "info")
             except Exception as e:
                 flash(f"Error updating session after reload: {e}", "error")
        else:
            flash(f"CRITICAL: Failed to load/reload CSV data from '{CSV_FILE_PATH}'. Check file.", "critical")


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Handles user login."""
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        user = User.query.get(username) # Get user from DB
        if user and user.verify_password(password):
            login_user(user)
            flash('Logged in successfully.', 'success')
            next_page = request.args.get('next')
            return redirect(next_page or url_for('index'))
        else:
            flash('Invalid username or password.', 'error')
    return render_template('login.html', messages=flash_messages())

@app.route('/logout')
@login_required
def logout():
    """Logs out the current user."""
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))

@app.route('/users')
@login_required
def users_management():
    """Displays user management page (admin only)."""
    if not current_user.is_admin:
        flash("You do not have permission to view user management.", "error")
        return redirect(url_for('index'))
    all_users = User.query.all()
    return render_template('users.html', users=all_users, messages=flash_messages())

@app.route('/add_user', methods=['POST'])
@login_required
def add_user():
    """Handles adding a new user (admin only)."""
    if not current_user.is_admin:
        flash("You do not have permission to add users.", "error")
        return redirect(url_for('index'))

    username = request.form.get('username').strip()
    password = request.form.get('password').strip()
    is_admin = request.form.get('is_admin') == 'on' # Checkbox value

    if not username or not password:
        flash("Username and Password cannot be empty.", "error")
        return redirect(url_for('users_management'))

    if User.query.get(username):
        flash(f"User '{username}' already exists.", "error")
        return redirect(url_for('users_management'))

    try:
        new_user = User(id=username, is_admin=is_admin)
        new_user.set_password(password)
        db.session.add(new_user)
        db.session.commit()
        flash(f"User '{username}' added successfully.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error adding user: {e}", "error")
    return redirect(url_for('users_management'))


@app.route('/', methods=['GET'])
@login_required # Protect this route
def index():
    """Main QC interface."""
    if not data:
        return render_template('index.html', error_message="Failed to load CSV data. Please check file and logs.", data_loaded=False, messages=flash_messages())

    filter_param = request.args.get('filter')
    target_original_index_str = request.args.get('index')

    if filter_param is not None:
        session['show_only_incomplete'] = (filter_param == 'incomplete')
        display_indices = get_current_display_list_indices()
        current_original_index = display_indices[0] if display_indices else 0
    else:
        try:
            current_original_index = int(target_original_index_str) if target_original_index_str is not None else 0
        except ValueError:
            current_original_index = 0

    if not (0 <= current_original_index < len(data)):
         flash(f"Requested index {current_original_index} out of bounds.", "warning")
         current_original_index = 0

    display_indices = get_current_display_list_indices()
    if not display_indices:
         return render_template('index.html',
                                filter_active=session.get('show_only_incomplete'),
                                no_rows_after_filter=True,
                                total_original_rows=len(data),
                                data_loaded=True,
                                messages=flash_messages())

    if current_original_index not in display_indices:
         current_original_index = display_indices[0]
         flash("Requested item is filtered out, showing first available item.", "info")

    current_row = data[current_original_index]
    display_info = get_display_info_for_original_index(current_original_index)
    if not display_info:
         flash("Error determining display position.", "error")
         display_info = {'display_index': 0, 'total_display_count': len(display_indices)}

    label_img_path, macro_img_path = None, None
    label_img_exists, macro_img_exists = False, False
    if current_row.get('_identifier'):
        base_id = current_row['_identifier']
        label_filename = f"{base_id}_label.png"
        macro_filename = f"{base_id}_macro.png"
        
        # Path construction for checking existence
        potential_label_path = os.path.join(IMAGE_BASE_DIR, 'label', label_filename)
        potential_macro_path = os.path.join(IMAGE_BASE_DIR, 'macro', macro_filename)
        
        # URL construction for serving images
        if os.path.exists(potential_label_path):
            label_img_path = url_for('serve_image', subdir='label', filename=label_filename)
            label_img_exists = True
        if os.path.exists(potential_macro_path):
            macro_img_path = url_for('serve_image', subdir='macro', filename=macro_filename)
            macro_img_exists = True

    return render_template(
        'index.html',
        row=current_row,
        original_index=current_original_index,
        display_index=display_info['display_index'],
        total_display_count=display_info['total_display_count'],
        total_original_rows=len(data),
        label_img_path=label_img_path,
        macro_img_path=macro_img_path,
        label_img_exists=label_img_exists,
        macro_img_exists=macro_img_exists,
        filter_active=session.get('show_only_incomplete'),
        messages=flash_messages(),
        data_loaded=True
    )


@app.route('/update', methods=['POST'])
@login_required # Protect this route
def update():
    """Handles updating record data and user correction counts."""
    global data
    if not data:
        flash("Error: Data not loaded, cannot update.", "error")
        return redirect(url_for('index'))

    try:
        original_index_to_update = int(request.form['original_index'])
        action = request.form.get('action')

        submitted_accession_id = request.form.get('accession_id', '').strip()
        submitted_stain = request.form.get('stain', '').strip()
        submitted_complete_checked = request.form.get('complete') == 'on'

        validation_failed = False
        if action in ['next', 'next_incorrect']:
            if not submitted_accession_id:
                flash("Validation Error: Accession ID cannot be empty to move forward.", "error")
                validation_failed = True
            if not submitted_stain:
                flash("Validation Error: Stain cannot be empty to move forward.", "error")
                validation_failed = True

            if validation_failed:
                flash("Please correct errors before moving forward.", "warning")
                return redirect(url_for('index', index=original_index_to_update))

        if 0 <= original_index_to_update < len(data):
            row_to_update = data[original_index_to_update]
            
            # Store original values to check for changes
            original_accession_id = row_to_update.get('AccessionID')
            original_stain = row_to_update.get('Stain')
            original_is_complete = row_to_update.get('_is_complete')

            data_changed = False
            
            # Check and update AccessionID
            if original_accession_id != submitted_accession_id:
                row_to_update['AccessionID'] = submitted_accession_id
                data_changed = True
            
            # Check and update Stain
            if original_stain != submitted_stain:
                row_to_update['Stain'] = submitted_stain
                data_changed = True

            # Determine target 'complete' state based on submission and validation
            is_complete_target_state = False
            can_be_marked_complete = bool(submitted_accession_id and submitted_stain) # True if both fields are filled
            
            if submitted_complete_checked: # User checked the box
                if can_be_marked_complete:
                    is_complete_target_state = True
                else:
                    is_complete_target_state = False # Force False if deps not met
                    flash("Cannot mark as Complete: Accession ID and Stain must be filled.", "warning")
            else: # User did not check or unchecked the box
                is_complete_target_state = False

            # Check and update _is_complete status
            if original_is_complete != is_complete_target_state:
                 row_to_update['_is_complete'] = is_complete_target_state
                 # For writing back to CSV, we need the capitalized string "True" or "False"
                 row_to_update['Complete'] = str(is_complete_target_state).capitalize()
                 data_changed = True

            if data_changed:
                display_info = get_display_info_for_original_index(original_index_to_update)
                display_num = display_info['display_index'] + 1 if display_info else '?'
                flash(f"Item {display_num} (Original Index {original_index_to_update + 1}) updated locally.", "info")

                # Increment correction count for the current user if OCR data was changed
                try:
                    current_user.correction_count += 1
                    db.session.add(current_user) # Mark current_user as modified
                    db.session.commit() # Save changes to the user in the database
                    print(f"INFO: User {current_user.id} correction count incremented to {current_user.correction_count}")
                except Exception as e:
                    db.session.rollback() # Rollback user changes if DB commit fails
                    print(f"ERROR: Could not increment correction count for user {current_user.id}: {e}")
                    flash(f"Error updating correction count: {e}", "error")

                # Intermediate Backup Trigger
                session['update_counter'] = session.get('update_counter', 0) + 1
                if session['update_counter'] >= INTERMEDIATE_BACKUP_FREQUENCY:
                    save_intermediate_backup()

        else:
            flash("Error: Invalid original index received for update.", "error")
            return redirect(url_for('index'))

        target_original_index = original_index_to_update

        if action in ['next', 'prev', 'next_incorrect']:
            nav_index = find_navigation_index(original_index_to_update, action)
            if nav_index is not None:
                target_original_index = nav_index

        return redirect(url_for('index', index=target_original_index))

    except Exception as e:
        flash(f"Critical Error processing update: {e}", "error")
        return redirect(url_for('index'))

@app.route('/jump', methods=['POST'])
@login_required
def jump():
    """Handles jumping to a specific item in the display queue."""
    if not data: return redirect(url_for('index'))

    try:
        jump_target_display_index_1based = int(request.form.get('jump_to_index', '1'))
        jump_target_display_index_0based = jump_target_display_index_1based - 1

        display_indices = get_current_display_list_indices()
        if not display_indices:
             flash("No items to jump to in current view.", "warning")
             return redirect(url_for('index'))

        if 0 <= jump_target_display_index_0based < len(display_indices):
            target_original_index = display_indices[jump_target_display_index_0based]
            flash(f"Jumped to item {jump_target_display_index_1based}.", "info")
            return redirect(url_for('index', index=target_original_index))
        else:
            flash(f"Invalid jump target: {jump_target_display_index_1based}. Please enter a number between 1 and {len(display_indices)}.", "error")
            current_index = request.form.get('original_index', 0)
            return redirect(url_for('index', index=current_index))

    except ValueError:
        flash("Invalid input for jump target. Please enter a number.", "error")
        current_index = request.form.get('original_index', 0)
        return redirect(url_for('index', index=current_index))
    except Exception as e:
         flash(f"Error during jump: {e}", "error")
         return redirect(url_for('index'))


@app.route('/search', methods=['POST'])
@login_required
def search():
    """Handles searching for a specific Accession ID or Patient Identifier."""
    if not data: return redirect(url_for('index'))

    search_term = request.form.get('search_term', '').strip()
    if not search_term:
        flash("Please enter a search term (Accession ID or Filename ID).", "warning")
        current_index = request.form.get('original_index', 0)
        return redirect(url_for('index', index=current_index))

    found_original_index = -1

    for i, row in enumerate(data):
        # Case-insensitive search
        if row.get('AccessionID', '').lower() == search_term.lower():
            found_original_index = i
            break
        if row.get('_identifier', '').lower() == search_term.lower():
             found_original_index = i
             break

    if found_original_index != -1:
         session['show_only_incomplete'] = False # Typically useful to show all when searching
         flash(f"Found matching item for '{search_term}'. Filter set to 'Show All'.", "info")
         return redirect(url_for('index', index=found_original_index))
    else:
         flash(f"No item found matching Accession ID or Filename ID '{search_term}'.", "warning")
         current_index = request.form.get('original_index', 0)
         return redirect(url_for('index', index=current_index))

@app.route('/save', methods=['POST'])
@login_required
def save():
    """Handles saving all in-memory data back to the primary CSV file, with backup."""
    if not _create_backup():
        flash("Save cancelled because backup failed.", "error")
    else:
        if save_csv_data(target_path=CSV_FILE_PATH):
            flash(f"Data successfully saved to {CSV_FILE_PATH}", "success")
            session['update_counter'] = 0
        else:
            flash(f"Data saving FAILED. Check logs. Recovery data might be in {INTERMEDIATE_BACKUP_FILE}", "critical")

    current_index = request.form.get('original_index', 0)
    return redirect(url_for('index', index=current_index))


# --- Image Serving & Flash Helper ---
@app.route('/images/<subdir>/<path:filename>')
def serve_image(subdir, filename):
    """Serves image files from specified subdirectories."""
    if subdir not in ['label', 'macro', 'thumbnail']: return "Invalid image category", 404
    image_dir = os.path.join(IMAGE_BASE_DIR, subdir)
    if not os.path.isdir(image_dir):
        return f"Image directory for {subdir} not found.", 404
    try:
        return send_from_directory(image_dir, filename)
    except FileNotFoundError:
        return "Image file not found.", 404

def flash_messages():
    """Helper to retrieve and format flash messages."""
    messages = []
    flashed = get_flashed_messages(with_categories=True)
    if flashed:
        for category, message in flashed:
            css_class = category
            if category == 'critical': css_class += ' flash-critical'
            messages.append({'category': category, 'message': message, 'css_class': css_class})
    return messages

# --- Startup Logic ---
if __name__ == '__main__':
    print("INFO: Application starting...")

    # Initialize database and create tables if they don't exist
    with app.app_context():
        db.create_all()
        # Create the initial admin user if not exists
        if not User.query.get('admin'):
            admin_user_obj = User(id='admin', is_admin=True)
            admin_user_obj.set_password('<something@great>') # <<< IMPORTANT: CHANGE THIS PASSWORD IMMEDIATELY!
            db.session.add(admin_user_obj)
            db.session.commit()
            print("INFO: Admin user 'admin' created with default password. PLEASE CHANGE IT!")
        else:
            print("INFO: Admin user 'admin' already exists.")
            # Ensure the global admin_user object used by Flask-Login is up-to-date from DB
            # This is important if an admin's password or admin status was changed via DB directly
            # For this simple case, we don't strictly need to reload `admin_user_obj` here as `load_user` will fetch it.
            pass


    # Initial check for main OCR CSV file
    if not os.path.exists(CSV_FILE_PATH):
        print(f"CRITICAL ERROR: Main CSV file '{CSV_FILE_PATH}' not found. Attempting recovery from backup...")
        if os.path.exists(INTERMEDIATE_BACKUP_FILE):
            print(f"INFO: Recovery file '{INTERMEDIATE_BACKUP_FILE}' found. Attempting to copy to main CSV path.")
            try:
                shutil.copy2(INTERMEDIATE_BACKUP_FILE, CSV_FILE_PATH)
                print(f"INFO: Recovery file successfully copied to '{CSV_FILE_PATH}'.")
            except Exception as e:
                print(f"CRITICAL ERROR: Failed to copy recovery file to main CSV path: {e}. Exiting.")
                exit(1)
        else:
            print("CRITICAL ERROR: No main CSV file and no recovery file found. Please ensure 'output_processed.csv' exists. Exiting.")
            exit(1)

    # Attempt initial load of OCR data
    if not load_csv_data():
        print("CRITICAL ERROR: Initial OCR data load failed even after recovery attempt. Exiting.")
        exit(1)
    else:
        print(f"INFO: Application successfully loaded {len(data)} OCR records.")

    # Check for image directory (just a warning, not critical for app start)
    if not os.path.exists(IMAGE_BASE_DIR):
        print(f"WARNING: Image base directory '{IMAGE_BASE_DIR}' not found. Image loading will likely fail.")
        for sub in ['label', 'macro']:
            if not os.path.exists(os.path.join(IMAGE_BASE_DIR, sub)):
                print(f"WARNING: Image subdirectory '{os.path.join(IMAGE_BASE_DIR, sub)}' not found. Image serving will fail.")


    print("INFO: Starting Flask development server...")
    app.run(debug=True, host='0.0.0.0')