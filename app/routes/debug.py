from flask import Blueprint, jsonify
from app import db
from app.models.scraped_data import ScrapedData
from app.models.user import User

debug_bp = Blueprint('debug', __name__, url_prefix='/api/debug')

@debug_bp.route('/db-status', methods=['GET'])
def db_status():
    """Check database status and record counts"""
    try:
        # Test database connection
        with db.engine.connect() as conn:
            result = conn.execute(db.text("SELECT version();"))
            db_version = result.fetchone()[0]
        
        # Count records
        total_scraped = ScrapedData.query.count()
        total_users = User.query.count()
        
        # Get sample data
        recent_scraped = ScrapedData.query.order_by(ScrapedData.created_at.desc()).limit(5).all()
        
        return jsonify({
            'status': 'connected',
            'database_version': db_version,
            'total_scraped_data': total_scraped,
            'total_users': total_users,
            'recent_scraped': [
                {
                    'id': item.id,
                    'company_name': item.company_name,
                    'user_id': item.user_id,
                    'created_at': item.created_at.strftime('%Y-%m-%d %H:%M:%S')
                } for item in recent_scraped
            ]
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@debug_bp.route('/user-data/<int:user_id>', methods=['GET'])
def user_data(user_id):
    """Check data for a specific user"""
    try:
        user_scraped = ScrapedData.query.filter_by(user_id=user_id).all()
        
        return jsonify({
            'user_id': user_id,
            'total_records': len(user_scraped),
            'data': [
                {
                    'id': item.id,
                    'company_name': item.company_name,
                    'email': item.email,
                    'phone': item.phone,
                    'address': item.address,
                    'created_at': item.created_at.strftime('%Y-%m-%d %H:%M:%S')
                } for item in user_scraped
            ]
        }), 200
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500