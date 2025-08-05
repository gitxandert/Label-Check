import sys
import os
from werkzeug.security import generate_password_hash
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin

# --- Configuration (Must match app.py's configuration) ---
DATABASE_URI = 'sqlite:///users.db'
# --- End Configuration ---

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# --- User Model (Must match app.py's User model) ---
class User(UserMixin, db.Model):
    id = db.Column(db.String(80), primary_key=True, unique=True, nullable=False) # Username as ID
    password_hash = db.Column(db.String(128), nullable=False)
    correction_count = db.Column(db.Integer, default=0, nullable=False)
    is_admin = db.Column(db.Boolean, default=False, nullable=False)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password):
        # This method is not strictly needed for the CLI script but is part of the User model
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return f'<User {self.id}>'
# --- End User Model ---

def add_user_to_db(username, password, is_admin=False):
    """Adds a new user to the database."""
    with app.app_context():
        # Ensure tables are created if they don't exist
        db.create_all()

        existing_user = User.query.get(username)
        if existing_user:
            print(f"Error: User '{username}' already exists.")
            return False

        try:
            new_user = User(id=username, is_admin=is_admin)
            new_user.set_password(password)
            db.session.add(new_user)
            db.session.commit()
            print(f"User '{username}' (Admin: {is_admin}) added successfully!")
            return True
        except Exception as e:
            db.session.rollback()
            print(f"Error adding user '{username}': {e}")
            return False

if __name__ == "__main__":
    print("--- SVS Slide QC: User Addition Script ---")
    print("This script allows you to add users to the Flask app's database.")

    username = input("Enter username: ").strip()
    if not username:
        print("Username cannot be empty. Exiting.")
        sys.exit(1)

    password = input("Enter password: ").strip()
    if not password:
        print("Password cannot be empty. Exiting.")
        sys.exit(1)

    while True:
        admin_input = input("Grant admin privileges? (yes/no): ").strip().lower()
        if admin_input in ['yes', 'y']:
            is_admin = True
            break
        elif admin_input in ['no', 'n']:
            is_admin = False
            break
        else:
            print("Invalid input. Please enter 'yes' or 'no'.")

    add_user_to_db(username, password, is_admin)

    print("\nRemember to change the default admin password set in app.py if you haven't already!")