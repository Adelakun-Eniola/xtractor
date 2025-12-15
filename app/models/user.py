from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from flask import current_app
from bson import ObjectId
import logging

class User:
    """MongoDB User model"""

    @staticmethod
    def get_collection():
        """Return the MongoDB users collection"""
        db = current_app.config["MONGO_DB"]
        return db.users

    @classmethod
    def create(cls, email, password, name=None, google_id=None):
        """Create a new user"""
        try:
            # Check if user already exists
            existing_user = cls.find_by_email(email)
            if existing_user:
                logging.info(f"User already exists: {email}")
                return existing_user  # Return existing user instead of None

            user_data = {
                'email': email,
                'password': generate_password_hash(password) if password else None,
                'name': name or email.split('@')[0],
                'google_id': google_id,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow(),
                'scrape_count': 0,
                'last_login': None
            }

            collection = cls.get_collection()
            result = collection.insert_one(user_data)
            user_data['_id'] = str(result.inserted_id)
            
            logging.info(f"Created new user: {email} with ID: {user_data['_id']}")
            return user_data
            
        except Exception as e:
            logging.error(f"Error creating user {email}: {e}")
            raise

    @classmethod
    def find_by_email(cls, email):
        """Find user by email"""
        user = cls.get_collection().find_one({'email': email})
        if user:
            user['_id'] = str(user['_id'])
        return user

    @classmethod
    def find_by_id(cls, user_id):
        """Find user by ID"""
        try:
            user = cls.get_collection().find_one({'_id': ObjectId(user_id)})
            if user:
                user['_id'] = str(user['_id'])
            return user
        except Exception as e:
            logging.error(f"Error finding user by ID: {e}")
            return None

    @classmethod
    def verify_password(cls, user, password):
        return check_password_hash(user['password'], password)

    @classmethod
    def update_last_login(cls, user_id):
        try:
            cls.get_collection().update_one(
                {'_id': ObjectId(user_id)},
                {'$set': {'last_login': datetime.utcnow()}}
            )
        except Exception as e:
            logging.error(f"Error updating last login: {e}")

    @classmethod
    def find_by_google_id(cls, google_id):
        """Find user by Google ID"""
        user = cls.get_collection().find_one({'google_id': google_id})
        if user:
            user['_id'] = str(user['_id'])
        return user

    @classmethod
    def update_google_id(cls, user_id, google_id):
        """Update user's Google ID"""
        try:
            cls.get_collection().update_one(
                {'_id': ObjectId(user_id)},
                {'$set': {'google_id': google_id, 'updated_at': datetime.utcnow()}}
            )
        except Exception as e:
            logging.error(f"Error updating Google ID: {e}")

    @classmethod
    def increment_scrape_count(cls, user_id):
        try:
            cls.get_collection().update_one(
                {'_id': ObjectId(user_id)},
                {'$inc': {'scrape_count': 1}}
            )
        except Exception as e:
            logging.error(f"Error incrementing scrape count: {e}")
