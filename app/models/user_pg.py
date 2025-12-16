from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from flask import current_app
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

class User:
    """PostgreSQL User model for Supabase"""

    @staticmethod
    def get_connection():
        """Return a PostgreSQL connection"""
        database_url = current_app.config.get('DATABASE_URL')
        if not database_url:
            raise Exception("DATABASE_URL not configured")
        return psycopg2.connect(database_url)

    @classmethod
    def create_tables(cls):
        """Create the users table if it doesn't exist"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor()
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    password VARCHAR(255),
                    name VARCHAR(255),
                    google_id VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    scrape_count INTEGER DEFAULT 0,
                    last_login TIMESTAMP
                )
            """)
            
            conn.commit()
            cur.close()
            conn.close()
            logging.info("Users table created successfully")
            
        except Exception as e:
            logging.error(f"Error creating users table: {e}")
            raise

    @classmethod
    def create(cls, email, password, name=None, google_id=None):
        """Create a new user"""
        try:
            # Check if user already exists
            existing_user = cls.find_by_email(email)
            if existing_user:
                logging.info(f"User already exists: {email}")
                return existing_user

            conn = cls.get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            hashed_password = generate_password_hash(password) if password else None
            user_name = name or email.split('@')[0]
            
            cur.execute("""
                INSERT INTO users (email, password, name, google_id, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id, email, name, google_id, created_at, updated_at, scrape_count, last_login
            """, (email, hashed_password, user_name, google_id, datetime.utcnow(), datetime.utcnow()))
            
            user = dict(cur.fetchone())
            conn.commit()
            cur.close()
            conn.close()
            
            logging.info(f"Created new user: {email} with ID: {user['id']}")
            return user
            
        except Exception as e:
            logging.error(f"Error creating user {email}: {e}")
            raise

    @classmethod
    def find_by_email(cls, email):
        """Find user by email"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("SELECT * FROM users WHERE email = %s", (email,))
            user = cur.fetchone()
            
            cur.close()
            conn.close()
            
            return dict(user) if user else None
            
        except Exception as e:
            logging.error(f"Error finding user by email: {e}")
            return None

    @classmethod
    def find_by_id(cls, user_id):
        """Find user by ID"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            user = cur.fetchone()
            
            cur.close()
            conn.close()
            
            return dict(user) if user else None
            
        except Exception as e:
            logging.error(f"Error finding user by ID: {e}")
            return None

    @classmethod
    def find_by_google_id(cls, google_id):
        """Find user by Google ID"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("SELECT * FROM users WHERE google_id = %s", (google_id,))
            user = cur.fetchone()
            
            cur.close()
            conn.close()
            
            return dict(user) if user else None
            
        except Exception as e:
            logging.error(f"Error finding user by Google ID: {e}")
            return None

    @classmethod
    def verify_password(cls, user, password):
        """Verify user password"""
        return check_password_hash(user['password'], password)

    @classmethod
    def update_google_id(cls, user_id, google_id):
        """Update user's Google ID"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor()
            
            cur.execute("""
                UPDATE users 
                SET google_id = %s, updated_at = %s 
                WHERE id = %s
            """, (google_id, datetime.utcnow(), user_id))
            
            conn.commit()
            cur.close()
            conn.close()
            
        except Exception as e:
            logging.error(f"Error updating Google ID: {e}")

    @classmethod
    def update_last_login(cls, user_id):
        """Update user's last login timestamp"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor()
            
            cur.execute("""
                UPDATE users 
                SET last_login = %s 
                WHERE id = %s
            """, (datetime.utcnow(), user_id))
            
            conn.commit()
            cur.close()
            conn.close()
            
        except Exception as e:
            logging.error(f"Error updating last login: {e}")

    @classmethod
    def increment_scrape_count(cls, user_id):
        """Increment user's scrape count"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor()
            
            cur.execute("""
                UPDATE users 
                SET scrape_count = scrape_count + 1 
                WHERE id = %s
            """, (user_id,))
            
            conn.commit()
            cur.close()
            conn.close()
            
        except Exception as e:
            logging.error(f"Error incrementing scrape count: {e}")