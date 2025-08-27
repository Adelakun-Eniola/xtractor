from flask import Blueprint, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.models.scraped_data import ScrapedData

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/api/dashboard')

@dashboard_bp.route('/data', methods=['GET'])
@jwt_required()
def get_user_data():
    """Get all scraped data for the current user"""
    user_id = int(get_jwt_identity())  # Cast string to int
    
    # Retrieve all data for the user
    data = ScrapedData.query.filter_by(user_id=user_id).order_by(ScrapedData.created_at.desc()).all()
    
    return jsonify({
        'count': len(data),
        'data': [item.to_dict() for item in data]
    }), 200

@dashboard_bp.route('/data/<int:data_id>', methods=['GET'])
@jwt_required()
def get_data_detail(data_id):
    """Get details for a specific scraped data entry"""
    user_id = int(get_jwt_identity())  # Cast string to int
    
    # Retrieve specific data entry
    data = ScrapedData.query.filter_by(id=data_id, user_id=user_id).first()
    
    if not data:
        return jsonify({'error': 'Data not found or access denied'}), 404
    
    return jsonify(data.to_dict()), 200

@dashboard_bp.route('/stats', methods=['GET'])
@jwt_required()
def get_stats():
    """Get statistics about user's scraped data"""
    user_id = int(get_jwt_identity())  # Cast string to int
    
    # Count total entries
    total_entries = ScrapedData.query.filter_by(user_id=user_id).count()
    
    # Count entries with email
    with_email = ScrapedData.query.filter_by(user_id=user_id).filter(ScrapedData.email.isnot(None)).count()
    
    # Count entries with phone
    with_phone = ScrapedData.query.filter_by(user_id=user_id).filter(ScrapedData.phone.isnot(None)).count()
    
    # Count entries with address
    with_address = ScrapedData.query.filter_by(user_id=user_id).filter(ScrapedData.address.isnot(None)).count()
    
    return jsonify({
        'total_entries': total_entries,
        'with_email': with_email,
        'with_phone': with_phone,
        'with_address': with_address,
        'email_success_rate': round((with_email / total_entries) * 100, 1) if total_entries > 0 else 0,
        'phone_success_rate': round((with_phone / total_entries) * 100, 1) if total_entries > 0 else 0,
        'address_success_rate': round((with_address / total_entries) * 100, 1) if total_entries > 0 else 0
    }), 200