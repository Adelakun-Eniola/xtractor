from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app import db
from app.models.scraped_data import ScrapedData
from app.models.user import User
from app.services.scraper import WebScraper
import re
import os

scraper_bp = Blueprint('scraper', __name__, url_prefix='/api/scraper')

@scraper_bp.route('/extract', methods=['POST'])
@jwt_required()
def extract_data():
    """Extract data from a website"""
    user_id = int(get_jwt_identity())  # Cast string to int (from JWT sub claim)
    
    # Get URL from request
    data = request.get_json()
    url = data.get('url')
    
    url_pattern = re.compile(r'^https?://[^\s/$.?#].[^\s]*$')
    if not url or not url_pattern.match(url):
        return jsonify({'error': 'Invalid URL provided'}), 400
    
    # Verify user exists
    user = User.query.get(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404
    
    # Scrape
    scraper = WebScraper(url)
    scraped_data = scraper.scrape()
    
    # Save
    new_data = ScrapedData(
        company_name=scraped_data['company_name'],
        email=scraped_data['email'],
        phone=scraped_data['phone'],
        address=scraped_data['address'],
        website_url=url,
        user_id=user_id
    )
    db.session.add(new_data)
    db.session.commit()
    
    return jsonify({
        'message': 'Data extracted successfully',
        'data': new_data.to_dict()
    }), 200

@scraper_bp.route('/batch', methods=['POST'])
@jwt_required()
def batch_extract():
    """Extract data from multiple websites"""
    user_id = int(get_jwt_identity())  # Cast string to int
    
    # Verify user exists
    user = User.query.get(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404
    
    data = request.get_json()
    urls = data.get('urls', [])
    
    if not urls or not isinstance(urls, list):
        return jsonify({'error': 'No URLs provided or invalid format'}), 400
    
    results, errors = [], []
    
    for url in urls:
        if not re.match(r'^https?://[^\s/$.?#].[^\s]*$', url):
            errors.append({'url': url, 'error': 'Invalid URL format'})
            continue
        
        try:
            scraper = WebScraper(url)
            scraped_data = scraper.scrape()
            
            new_data = ScrapedData(
                company_name=scraped_data['company_name'],
                email=scraped_data['email'],
                phone=scraped_data['phone'],
                address=scraped_data['address'],
                website_url=url,
                user_id=user_id
            )
            db.session.add(new_data)
            results.append(new_data.to_dict())
        except Exception as e:
            errors.append({'url': url, 'error': str(e)})
    
    if results:
        db.session.commit()
    
    return jsonify({
        'message': f'Processed {len(results)} URLs successfully with {len(errors)} errors',
        'results': results,
        'errors': errors
    }), 200