# from flask import Blueprint, request, jsonify, redirect, url_for
# from flask_jwt_extended import create_access_token, get_jwt_identity, jwt_required
# from google.oauth2 import id_token
# from google.auth.transport import requests as google_requests
# import os
# from app import db
# from app.models.user import User

# auth_bp = Blueprint('auth', __name__, url_prefix='/api/auth')

# @auth_bp.route('/google', methods=['POST'])
# def google_auth():
#     """Handle Google authentication"""
#     try:
#         token = request.json.get('token')
        
#         # Verify the token
#         idinfo = id_token.verify_oauth2_token(
#             token, 
#             google_requests.Request(), 
#             os.getenv('GOOGLE_CLIENT_ID')
#         )
        
#         # Get user info
#         google_id = idinfo['sub']
#         email = idinfo['email']
#         name = idinfo.get('name', '')
        
#         # Check if user exists
#         user = User.query.filter_by(google_id=google_id).first()
        
#         if not user:
#             # Create new user
#             user = User(
#                 email=email,
#                 name=name,
#                 google_id=google_id
#             )
#             db.session.add(user)
#             db.session.commit()
        
#         # Create access token
#         access_token = create_access_token(identity=user.id)
        
#         return jsonify({
#             'access_token': access_token,
#             'user': {
#                 'id': user.id,
#                 'email': user.email,
#                 'name': user.name
#             }
#         }), 200
        
#     except Exception as e:
#         return jsonify({'error': str(e)}), 400

# @auth_bp.route('/me', methods=['GET'])
# @jwt_required()
# def get_user():
#     """Get current user info"""
#     user_id = get_jwt_identity()
#     user = User.query.get(user_id)
    
#     if not user:
#         return jsonify({'error': 'User not found'}), 404
    
#     return jsonify({
#         'id': user.id,
#         'email': user.email,
#         'name': user.name
#     }), 200
from flask import Blueprint, request, jsonify, session
from flask_jwt_extended import create_access_token
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from google.auth.exceptions import InvalidValue, MalformedError
import os
import logging
from datetime import datetime
from app import db
from app.models.user import User

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

            user = User.query.filter_by(google_id=google_id).first()
            if not user:
                user = User(email=email, name=name, google_id=google_id)
                db.session.add(user)
                db.session.commit()

            # Create access token with string identity
            access_token = create_access_token(identity=str(user.id))  # Ensure string
            logger.debug(f"Generated JWT with identity: {str(user.id)}")

            return jsonify({
                'access_token': access_token,
                'user': {'id': user.id, 'email': user.email, 'name': user.name}
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