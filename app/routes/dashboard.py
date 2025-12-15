# from flask import Blueprint, jsonify
# from flask_jwt_extended import jwt_required, get_jwt_identity
# from app.models.scraped_data import ScrapedData
# import logging

# logger = logging.getLogger(__name__)

# dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/api/dashboard')

# @dashboard_bp.route('/data', methods=['GET'])
# @jwt_required()
# def get_user_data():
#     """Get all scraped data for the current user with pagination"""
#     user_id = get_jwt_identity()  # MongoDB user IDs are strings
    
#     try:
#         # Get page and per_page from query parameters
#         page = int(request.args.get('page', 1))
#         per_page = int(request.args.get('per_page', 20))
        
#         # Get data from MongoDB with pagination
#         result = ScrapedData.find_by_user(user_id, page=page, per_page=per_page)
        
#         logger.info(f"Retrieved {len(result['data'])} documents for user {user_id}")
        
#         return jsonify({
#             'count': result['pagination']['total'],
#             'page': result['pagination']['page'],
#             'per_page': result['pagination']['per_page'],
#             'total_pages': result['pagination']['pages'],
#             'data': result['data']
#         }), 200
        
#     except Exception as e:
#         logger.error(f"Error getting user data: {str(e)}")
#         return jsonify({
#             'error': 'Failed to retrieve data',
#             'details': str(e)
#         }), 500

# @dashboard_bp.route('/data/<string:data_id>', methods=['GET'])
# @jwt_required()
# def get_data_detail(data_id):
#     """Get details for a specific scraped data entry"""
#     user_id = get_jwt_identity()  # MongoDB user IDs are strings
    
#     try:
#         # Get data from MongoDB
#         data = ScrapedData.find_by_id(data_id)
        
#         if not data:
#             logger.warning(f"Data not found with ID: {data_id}")
#             return jsonify({'error': 'Data not found'}), 404
        
#         # Check if data belongs to user
#         if data.get('user_id') != user_id:
#             logger.warning(f"User {user_id} tried to access data {data_id} belonging to {data.get('user_id')}")
#             return jsonify({'error': 'Access denied'}), 403
        
#         return jsonify(data), 200
        
#     except Exception as e:
#         logger.error(f"Error getting data detail: {str(e)}")
#         return jsonify({
#             'error': 'Failed to retrieve data',
#             'details': str(e)
#         }), 500

# @dashboard_bp.route('/stats', methods=['GET'])
# @jwt_required()
# def get_stats():
#     """Get statistics about user's scraped data"""
#     user_id = get_jwt_identity()  # MongoDB user IDs are strings
    
#     try:
#         # Get stats from MongoDB
#         stats = ScrapedData.stats(user_id)
        
#         total_entries = stats.get('total', 0)
#         with_email = stats.get('with_email', 0)
#         with_phone = stats.get('with_phone', 0)
#         with_address = stats.get('with_address', 0)
        
#         # Calculate percentages
#         email_rate = round((with_email / total_entries) * 100, 1) if total_entries > 0 else 0
#         phone_rate = round((with_phone / total_entries) * 100, 1) if total_entries > 0 else 0
#         address_rate = round((with_address / total_entries) * 100, 1) if total_entries > 0 else 0
        
#         logger.info(f"Stats for user {user_id}: total={total_entries}, email={with_email}, phone={with_phone}, address={with_address}")
        
#         return jsonify({
#             'total_entries': total_entries,
#             'with_email': with_email,
#             'with_phone': with_phone,
#             'with_address': with_address,
#             'email_success_rate': email_rate,
#             'phone_success_rate': phone_rate,
#             'address_success_rate': address_rate
#         }), 200
        
#     except Exception as e:
#         logger.error(f"Error getting stats: {str(e)}")
#         return jsonify({
#             'error': 'Failed to retrieve statistics',
#             'details': str(e)
#         }), 500

# @dashboard_bp.route('/search', methods=['GET'])
# @jwt_required()
# def search_data():
#     """Search user's scraped data"""
#     user_id = get_jwt_identity()
#     search_term = request.args.get('q', '')
    
#     if not search_term or len(search_term) < 2:
#         return jsonify({'error': 'Search term must be at least 2 characters'}), 400
    
#     try:
#         # Search in MongoDB
#         results = ScrapedData.search(user_id, search_term)
        
#         return jsonify({
#             'count': len(results),
#             'query': search_term,
#             'results': results
#         }), 200
        
#     except Exception as e:
#         logger.error(f"Error searching data: {str(e)}")
#         return jsonify({
#             'error': 'Failed to search data',
#             'details': str(e)
#         }), 500

# @dashboard_bp.route('/export', methods=['GET'])
# @jwt_required()
# def export_data():
#     """Export user's scraped data as CSV"""
#     user_id = get_jwt_identity()
    
#     try:
#         # Get all user data
#         result = ScrapedData.find_by_user(user_id, page=1, per_page=1000)  # Large limit for export
        
#         if not result['data']:
#             return jsonify({'error': 'No data to export'}), 404
        
#         # Format for CSV export
#         import csv
#         import io
        
#         output = io.StringIO()
#         writer = csv.writer(output)
        
#         # Write header
#         writer.writerow(['Company Name', 'Email', 'Phone', 'Address', 'Website', 'Created At'])
        
#         # Write data
#         for item in result['data']:
#             writer.writerow([
#                 item.get('company_name', ''),
#                 item.get('email', ''),
#                 item.get('phone', ''),
#                 item.get('address', ''),
#                 item.get('website_url', ''),
#                 item.get('created_at', '').strftime('%Y-%m-%d %H:%M:%S') if item.get('created_at') else ''
#             ])
        
#         # Return CSV as downloadable file
#         response = make_response(output.getvalue())
#         response.headers['Content-Disposition'] = f'attachment; filename=scraped_data_{user_id}.csv'
#         response.headers['Content-type'] = 'text/csv'
        
#         return response
        
#     except Exception as e:
#         logger.error(f"Error exporting data: {str(e)}")
#         return jsonify({
#             'error': 'Failed to export data',
#             'details': str(e)
#         }), 500

# @dashboard_bp.route('/data/<string:data_id>', methods=['DELETE'])
# @jwt_required()
# def delete_data(data_id):
#     """Delete a specific scraped data entry"""
#     user_id = get_jwt_identity()
    
#     try:
#         # First check if data exists and belongs to user
#         data = ScrapedData.find_by_id(data_id)
        
#         if not data:
#             return jsonify({'error': 'Data not found'}), 404
        
#         if data.get('user_id') != user_id:
#             return jsonify({'error': 'Access denied'}), 403
        
#         # Delete from MongoDB
#         success = ScrapedData.delete(data_id)
        
#         if success:
#             logger.info(f"Deleted data {data_id} for user {user_id}")
#             return jsonify({'message': 'Data deleted successfully'}), 200
#         else:
#             return jsonify({'error': 'Failed to delete data'}), 500
            
#     except Exception as e:
#         logger.error(f"Error deleting data: {str(e)}")
#         return jsonify({
#             'error': 'Failed to delete data',
#             'details': str(e)
#         }), 500

# @dashboard_bp.route('/clear', methods=['DELETE'])
# @jwt_required()
# def clear_all_data():
#     """Delete all scraped data for the current user"""
#     user_id = get_jwt_identity()
    
#     try:
#         # Delete all data for user
#         deleted_count = ScrapedData.delete_by_user(user_id)
        
#         logger.info(f"Cleared {deleted_count} documents for user {user_id}")
        
#         return jsonify({
#             'message': f'Successfully deleted {deleted_count} records',
#             'deleted_count': deleted_count
#         }), 200
        
#     except Exception as e:
#         logger.error(f"Error clearing data: {str(e)}")
#         return jsonify({
#             'error': 'Failed to clear data',
#             'details': str(e)
#         }), 500

from flask import Blueprint, jsonify, request, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.models.scraped_data_pg import ScrapedData
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/api/dashboard')

@dashboard_bp.route('/data', methods=['GET'])
@jwt_required()
def get_user_data():
    """Get all scraped data for the current user with pagination"""
    user_id = int(get_jwt_identity())  # PostgreSQL user IDs are integers
    
    try:
        # Get page and per_page from query parameters
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 20))
        
        # Get data from PostgreSQL with pagination
        offset = (page - 1) * per_page
        data = ScrapedData.find_by_user_id(user_id, limit=per_page, offset=offset)
        total_count = ScrapedData.count_by_user_id(user_id)
        total_pages = (total_count + per_page - 1) // per_page
        
        result = {
            'data': data,
            'pagination': {
                'total': total_count,
                'page': page,
                'per_page': per_page,
                'pages': total_pages
            }
        }
        
        logger.info(f"Retrieved {len(result['data'])} documents for user {user_id}")
        
        return jsonify({
            'count': result['pagination']['total'],
            'page': result['pagination']['page'],
            'per_page': result['pagination']['per_page'],
            'total_pages': result['pagination']['pages'],
            'data': result['data']
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting user data: {str(e)}")
        return jsonify({
            'error': 'Failed to retrieve data',
            'details': str(e)
        }), 500

@dashboard_bp.route('/data/<string:data_id>', methods=['GET'])
@jwt_required()
def get_data_detail(data_id):
    """Get details for a specific scraped data entry"""
    user_id = int(get_jwt_identity())  # PostgreSQL user IDs are integers
    
    try:
        # Get data from PostgreSQL
        data = ScrapedData.find_by_id(int(data_id))
        
        if not data:
            logger.warning(f"Data not found with ID: {data_id}")
            return jsonify({'error': 'Data not found'}), 404
        
        # Check if data belongs to user
        if data.get('user_id') != user_id:
            logger.warning(f"User {user_id} tried to access data {data_id} belonging to {data.get('user_id')}")
            return jsonify({'error': 'Access denied'}), 403
        
        return jsonify(data), 200
        
    except Exception as e:
        logger.error(f"Error getting data detail: {str(e)}")
        return jsonify({
            'error': 'Failed to retrieve data',
            'details': str(e)
        }), 500

@dashboard_bp.route('/stats', methods=['GET'])
@jwt_required()
def get_stats():
    """Get statistics about user's scraped data"""
    user_id = int(get_jwt_identity())  # PostgreSQL user IDs are integers
    
    try:
        # Get stats from PostgreSQL
        stats = ScrapedData.get_stats_by_user_id(user_id)
        
        total_entries = stats.get('total_records', 0)
        with_email = stats.get('with_email', 0)
        with_phone = stats.get('with_phone', 0)
        with_address = stats.get('with_address', 0)
        
        # Calculate percentages
        email_rate = round((with_email / total_entries) * 100, 1) if total_entries > 0 else 0
        phone_rate = round((with_phone / total_entries) * 100, 1) if total_entries > 0 else 0
        address_rate = round((with_address / total_entries) * 100, 1) if total_entries > 0 else 0
        
        logger.info(f"Stats for user {user_id}: total={total_entries}, email={with_email}, phone={with_phone}, address={with_address}")
        
        return jsonify({
            'total_entries': total_entries,
            'with_email': with_email,
            'with_phone': with_phone,
            'with_address': with_address,
            'email_success_rate': email_rate,
            'phone_success_rate': phone_rate,
            'address_success_rate': address_rate
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        return jsonify({
            'error': 'Failed to retrieve statistics',
            'details': str(e)
        }), 500

@dashboard_bp.route('/search', methods=['GET'])
@jwt_required()
def search_data():
    """Search user's scraped data"""
    user_id = int(get_jwt_identity())
    search_term = request.args.get('q', '')
    
    if not search_term or len(search_term) < 2:
        return jsonify({'error': 'Search term must be at least 2 characters'}), 400
    
    try:
        # Search in PostgreSQL
        results = ScrapedData.search_by_user_id(user_id, search_term)
        
        return jsonify({
            'count': len(results),
            'query': search_term,
            'results': results
        }), 200
        
    except Exception as e:
        logger.error(f"Error searching data: {str(e)}")
        return jsonify({
            'error': 'Failed to search data',
            'details': str(e)
        }), 500

@dashboard_bp.route('/export', methods=['GET'])
@jwt_required()
def export_data():
    """Export user's scraped data as CSV"""
    user_id = int(get_jwt_identity())
    
    try:
        # Get all user data
        data = ScrapedData.find_by_user_id(user_id, limit=1000, offset=0)  # Large limit for export
        result = {'data': data}
        
        if not result['data']:
            return jsonify({'error': 'No data to export'}), 404
        
        # Format for CSV export
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(['Company Name', 'Email', 'Phone', 'Address', 'Website', 'Created At'])
        
        # Write data
        for item in result['data']:
            writer.writerow([
                item.get('company_name', ''),
                item.get('email', ''),
                item.get('phone', ''),
                item.get('address', ''),
                item.get('website_url', ''),
                item.get('created_at', '').strftime('%Y-%m-%d %H:%M:%S') if item.get('created_at') else ''
            ])
        
        # Return CSV as downloadable file
        response = make_response(output.getvalue())
        response.headers['Content-Disposition'] = f'attachment; filename=scraped_data_{user_id}_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.csv'
        response.headers['Content-type'] = 'text/csv'
        
        return response
        
    except Exception as e:
        logger.error(f"Error exporting data: {str(e)}")
        return jsonify({
            'error': 'Failed to export data',
            'details': str(e)
        }), 500

@dashboard_bp.route('/data/<string:data_id>', methods=['DELETE'])
@jwt_required()
def delete_data(data_id):
    """Delete a specific scraped data entry"""
    user_id = int(get_jwt_identity())
    
    try:
        # Delete from PostgreSQL (with user verification)
        success = ScrapedData.delete_by_id(int(data_id), user_id)
        
        if success:
            logger.info(f"Deleted data {data_id} for user {user_id}")
            return jsonify({'message': 'Data deleted successfully'}), 200
        else:
            return jsonify({'error': 'Failed to delete data'}), 500
            
    except Exception as e:
        logger.error(f"Error deleting data: {str(e)}")
        return jsonify({
            'error': 'Failed to delete data',
            'details': str(e)
        }), 500

@dashboard_bp.route('/clear', methods=['DELETE'])
@jwt_required()
def clear_all_data():
    """Delete all scraped data for the current user"""
    user_id = int(get_jwt_identity())
    
    try:
        # Delete all data for user
        deleted_count = ScrapedData.delete_all_by_user_id(user_id)
        
        logger.info(f"Cleared {deleted_count} documents for user {user_id}")
        
        return jsonify({
            'message': f'Successfully deleted {deleted_count} records',
            'deleted_count': deleted_count
        }), 200
        
    except Exception as e:
        logger.error(f"Error clearing data: {str(e)}")
        return jsonify({
            'error': 'Failed to clear data',
            'details': str(e)
        }), 500