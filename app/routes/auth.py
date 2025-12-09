from flask import Blueprint, request, jsonify
from flask_jwt_extended import create_access_token, get_jwt_identity, jwt_required
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from google.auth.exceptions import InvalidValue, MalformedError
import os
import logging
from datetime import datetime
from app.models.user import User  # MongoDB User model

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

auth_bp = Blueprint('auth', __name__, url_prefix='/api/auth')

@auth_bp.route('/google', methods=['POST'])
def google_auth():
    logger.debug("### /api/auth/google endpoint hit ###")
    if not request.is_json:
        logger.error("Request is not JSON")
        return jsonify({'error': 'Request must be JSON'}), 400

    try:
        data = request.get_json()
        token = data.get('token')
        if not token:
            logger.error("No token provided in request")
            return jsonify({'error': 'No token provided'}), 400

        google_client_id = os.getenv('GOOGLE_CLIENT_ID')
        if not google_client_id:
            logger.error("GOOGLE_CLIENT_ID environment variable not set")
            return jsonify({'error': 'Server configuration error'}), 500

        try:
            allowed_client_ids = [
                google_client_id,
                '1013892849623-lo8vb2leq2ao4ra83fh431gk2kb5dmil.apps.googleusercontent.com'  # Additional client ID
            ]

            idinfo = None
            for client_id in allowed_client_ids:
                try:
                    idinfo = id_token.verify_oauth2_token(
                        token,
                        google_requests.Request(),
                        client_id,
                        clock_skew_in_seconds=300
                    )
                    logger.debug(f"Token verified with client ID: {client_id}")
                    break
                except ValueError:
                    continue

            if not idinfo:
                raise ValueError("Token could not be verified with any allowed client ID")

            logger.debug(f"User info: {idinfo}")
            logger.debug(f"Token expiration: {datetime.utcfromtimestamp(idinfo['exp'])} UTC")

            google_id = idinfo['sub']
            email = idinfo['email']
            name = idinfo.get('name', '')

            # Check if user exists in MongoDB
            user = User.find_by_google_id(google_id)
            if not user:
                # Try by email as fallback
                user = User.find_by_email(email)
                if not user:
                    # Create new user in MongoDB
                    user_data = User.create(
                        email=email,
                        password=None,  # Google auth users don't need password
                        name=name,
                        google_id=google_id
                    )
                    logger.info(f"Created new user in MongoDB: {email}")
                else:
                    # Update existing user with google_id
                    User.update_google_id(user['_id'], google_id)
                    user['google_id'] = google_id
                    logger.info(f"Updated existing user with Google ID: {email}")
            else:
                logger.info(f"Found existing user: {email}")

            # Create access token with MongoDB user ID (string)
            access_token = create_access_token(identity=str(user['_id']))
            logger.debug(f"Generated JWT with identity: {str(user['_id'])}")

            return jsonify({
                'access_token': access_token,
                'user': {
                    'id': user['_id'],
                    'email': user['email'],
                    'name': user['name'],
                    'google_id': user.get('google_id')
                }
            }), 200

        except ValueError as e:
            logger.error(f"Token validation failed: {str(e)}")
            return jsonify({'error': f'Invalid or expired token: {str(e)}'}), 401
        except MalformedError:
            logger.error("Invalid Google token format")
            return jsonify({'error': 'Invalid Google token format'}), 401
        except InvalidValue:
            logger.error("Invalid Google token value")
            return jsonify({'error': 'Invalid Google token value'}), 401

    except Exception as e:
        logger.error(f"Unexpected error in google_auth: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@auth_bp.route('/me', methods=['GET'])
@jwt_required()
def get_user():
    """Get current user info"""
    user_id = get_jwt_identity()  # MongoDB user ID (string)
    
    # Find user in MongoDB
    user = User.find_by_id(user_id)
    
    if not user:
        return jsonify({'error': 'User not found'}), 404
    
    return jsonify({
        'id': user['_id'],
        'email': user['email'],
        'name': user['name'],
        'google_id': user.get('google_id')
    }), 200

@auth_bp.route('/check', methods=['GET'])
@jwt_required()
def check_token():
    """Check if token is valid"""
    user_id = get_jwt_identity()  # Already a string
    
    # Find user in MongoDB
    user = User.find_by_id(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404
    
    return jsonify({
        'message': 'Token is valid',
        'user': {
            'id': user['_id'],
            'email': user['email'],
            'name': user['name']
        }
    }), 200