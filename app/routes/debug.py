from flask import Blueprint, jsonify, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.models.scraped_data import ScrapedData
from app.models.user import User

debug_bp = Blueprint('debug', __name__, url_prefix='/api/debug')

@debug_bp.route('/db-status', methods=['GET'])
def db_status():
    """Check MongoDB status and record counts"""
    try:
        # Test MongoDB connection
        client = current_app.config['MONGO_CLIENT']
        db = current_app.config['MONGO_DB']
        
        if not client or not db:
            return jsonify({
                'status': 'error',
                'error': 'MongoDB not initialized'
            }), 500
        
        # Test connection
        client.admin.command('ping')
        
        # Get database info
        db_stats = db.command('dbStats')
        collections = db.list_collection_names()
        
        # Count records
        total_scraped = db.scraped_data.count_documents({})
        total_users = db.users.count_documents({})
        
        # Get sample data
        recent_scraped = list(db.scraped_data.find().sort('created_at', -1).limit(5))
        
        # Convert ObjectIds to strings
        for item in recent_scraped:
            item['_id'] = str(item['_id'])
        
        return jsonify({
            'status': 'connected',
            'database_name': db.name,
            'database_size': db_stats.get('dataSize', 0),
            'collections': collections,
            'total_scraped_data': total_scraped,
            'total_users': total_users,
            'recent_scraped': [
                {
                    'id': item['_id'],
                    'company_name': item.get('company_name', 'N/A'),
                    'user_id': item.get('user_id', 'N/A'),
                    'created_at': item.get('created_at', '').strftime('%Y-%m-%d %H:%M:%S') if item.get('created_at') else 'N/A'
                } for item in recent_scraped
            ]
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@debug_bp.route('/user-data/<string:user_id>', methods=['GET'])
def user_data(user_id):
    """Check data for a specific user"""
    try:
        user_scraped = ScrapedData.find_by_user_id(user_id)
        
        return jsonify({
            'user_id': user_id,
            'total_records': len(user_scraped),
            'data': [
                {
                    'id': item['_id'],
                    'company_name': item.get('company_name', 'N/A'),
                    'email': item.get('email', 'N/A'),
                    'phone': item.get('phone', 'N/A'),
                    'address': item.get('address', 'N/A'),
                    'created_at': item.get('created_at', '').strftime('%Y-%m-%d %H:%M:%S') if item.get('created_at') else 'N/A'
                } for item in user_scraped
            ]
        }), 200
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

@debug_bp.route('/clear-test-data', methods=['GET'])
def clear_test_data():
    """Clear test data from database"""
    try:
        # Delete test entries from MongoDB
        db = current_app.config['MONGO_DB']
        result = db.scraped_data.delete_many({'company_name': 'Test Hospital'})
        
        return jsonify({
            'status': 'success',
            'message': f'Test data cleared: {result.deleted_count} documents deleted'
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@debug_bp.route('/check-auth', methods=['GET'])
@jwt_required()
def check_auth():
    """Check current user authentication"""
    try:
        user_id = get_jwt_identity()  # MongoDB user IDs are strings
        
        # Check if user exists
        user = User.find_by_id(user_id)
        
        return jsonify({
            'authenticated': True,
            'user_id': user_id,
            'user_exists': user is not None,
            'user_email': user['email'] if user else None
        }), 200
        
    except Exception as e:
        return jsonify({
            'authenticated': False,
            'error': str(e)
        }), 500