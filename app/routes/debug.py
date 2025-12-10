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

@debug_bp.route('/test-scraper-save', methods=['POST'])
@jwt_required()
def test_scraper_save():
    """Test scraper data saving functionality"""
    user_id = get_jwt_identity()
    
    try:
        # Simulate scraped data like the scraper would create
        test_businesses = [
            {
                'company_name': 'Debug Test Business 1',
                'email': 'contact@debugtest1.com',
                'phone': '+1-555-DEBUG-1',
                'address': '123 Debug Street, Test City, TC 12345',
                'website_url': 'https://debugtest1.com',
                'user_id': user_id
            },
            {
                'company_name': 'Debug Test Business 2',
                'email': None,  # Test with None values like scraper might produce
                'phone': '+1-555-DEBUG-2',
                'address': None,
                'website_url': 'https://debugtest2.com',
                'user_id': user_id
            }
        ]
        
        saved_count = 0
        errors = []
        saved_ids = []
        
        for business_data in test_businesses:
            try:
                # Check if business already exists (same logic as scraper)
                existing = ScrapedData.get_collection().find_one({
                    'user_id': user_id,
                    'company_name': business_data['company_name'],
                    'website_url': business_data['website_url']
                })
                
                if not existing:
                    document_id = ScrapedData.create(business_data)
                    saved_count += 1
                    saved_ids.append(str(document_id))
                    print(f"DEBUG: Saved test business {business_data['company_name']} with ID: {document_id}")
                else:
                    print(f"DEBUG: Test business {business_data['company_name']} already exists")
                    
            except Exception as e:
                error_msg = f"Error saving {business_data['company_name']}: {str(e)}"
                print(f"DEBUG ERROR: {error_msg}")
                errors.append(error_msg)
        
        # Get updated stats
        stats = ScrapedData.get_stats(user_id)
        
        return jsonify({
            'status': 'success',
            'message': f'Test scraper save completed',
            'saved_count': saved_count,
            'saved_ids': saved_ids,
            'errors': errors,
            'user_stats': stats
        }), 200
        
    except Exception as e:
        print(f"DEBUG: Test scraper save error: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'Test scraper save failed',
            'error': str(e)
        }), 500

@debug_bp.route('/cleanup-debug-data', methods=['DELETE'])
@jwt_required()
def cleanup_debug_data():
    """Clean up debug test data"""
    user_id = get_jwt_identity()
    
    try:
        # Delete debug test data
        db = current_app.config['MONGO_DB']
        result = db.scraped_data.delete_many({
            'user_id': user_id,
            'company_name': {'$regex': '^Debug Test Business'}
        })
        
        return jsonify({
            'status': 'success',
            'message': f'Debug data cleaned up: {result.deleted_count} documents deleted'
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500