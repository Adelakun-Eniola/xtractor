from flask import Blueprint, request, jsonify, Response
from flask_jwt_extended import jwt_required, get_jwt_identity
from flask_cors import CORS
from app.models.scraped_data_pg import ScrapedData
from app.models.user_pg import User
from app.services.scraper import WebScraper, is_google_maps_search_url, GoogleMapsSearchScraper
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import re
import os
import logging
import json
from datetime import datetime

scraper_bp = Blueprint('scraper', __name__, url_prefix='/api/scraper')
CORS(scraper_bp)

def check_existing_business(user_id, company_name, website_url):
    """Helper function to check if business already exists"""
    try:
        existing_records = ScrapedData.find_by_user_id(user_id, limit=1000)
        for record in existing_records:
            if (record.get('company_name') == company_name and 
                record.get('website_url') == website_url):
                return record
        return None
    except Exception as e:
        logging.error(f"Error checking existing business: {e}")
        return None

@scraper_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'message': 'Scraper service is running',
        'chromium_path': os.getenv('CHROMIUM_PATH', 'Not set'),
        'chromedriver_path': os.getenv('CHROMEDRIVER_PATH', 'Not set')
    }), 200

@scraper_bp.route('/extract', methods=['POST'])
@jwt_required()
def extract_data():
    """Extract data from a website or Google Maps search results"""
    try:
        user_id = int(get_jwt_identity())  # PostgreSQL user IDs are integers
        
        # Get URL from request
        data = request.get_json()
        url = data.get('url')
        stream = data.get('stream', False)  # Check if streaming is requested
        
        logging.info(f"Extract endpoint called by user {user_id} with URL: {url}, stream={stream}")
    except Exception as e:
        logging.error(f"Error in extract_data initialization: {str(e)}")
        return jsonify({
            'error': 'Internal server error',
            'details': str(e)
        }), 500
    
    url_pattern = re.compile(r'^https?://[^\s/$.?#].[^\s]*$')$')$')
    if not url or not url_pattern.match(url):
        logging.warning(f"Invalid URL provided: {url}")
        return jsonify({'error': 'Invalid URL provided'}), 400
    
    # Verify user exists
    user = User.find_by_id(user_id)
    if not user:
        logging.warning(f"User {user_id} not found")
        return jsonify({'error': 'User not found'}), 404
    
    logging.info(f"User {user_id} initiated scraping for URL: {url}")
    
    # Check if URL is a Google Maps search URL
    if is_google_maps_search_url(url):
        logging.info(f"Detected Google Maps search URL, using GoogleMapsSearchScraper")
        
        try:
            # Use GoogleMapsSearchScraper for search results
            search_scraper = GoogleMapsSearchScraper(url)
            result = search_scraper.scrape_all_businesses(user_id)
            
            # Now save the results to PostgreSQL
            saved_results = []
            for business_data in result['results']:
                try:
                    # Check if business already exists
                    existing = check_existing_business(
                        user_id, 
                        business_data['company_name'], 
                        business_data['website_url']
                    )
                    
                    if not existing:
                        # Prepare data for PostgreSQL
                        pg_data = {
                            'user_id': user_id,
                            'company_name': business_data['company_name'],
                            'email': business_data['email'] if business_data['email'] != 'N/A' else None,
                            'phone': business_data['phone'] if business_data['phone'] != 'N/A' else None,
                            'address': business_data['address'] if business_data['address'] != 'N/A' else None,
                            'website_url': business_data['website_url'],
                            'source_url': url
                        }
                        
                        document_id = ScrapedData.create(pg_data)
                        
                        # Get the saved document for response
                        saved_document = ScrapedData.find_by_id(document_id)
                        saved_results.append(saved_document)
                        
                        logging.info(f"Saved business to PostgreSQL: {business_data['company_name']} with ID: {document_id}")
                    else:
                        logging.info(f"Business already exists: {business_data['company_name']}")
                        
                except Exception as save_error:
                    logging.error(f"Error saving business {business_data['company_name']}: {save_error}")
                    result['errors'].append({
                        'business_name': business_data['company_name'],
                        'error': f"Save error: {str(save_error)}"
                    })
            
            total_results = len(saved_results)
            total_errors = len(result['errors'])
            
            message = f'Extracted and saved {total_results} business{"es" if total_results != 1 else ""}'
            if total_errors > 0:
                message += f' with {total_errors} error{"s" if total_errors != 1 else ""}'
            
            logging.info(f"Google Maps scraping complete for user {user_id}: {message}")
            
            return jsonify({
                'message': message,
                'data': saved_results,
                'errors': result['errors']
            }), 200
            
        except Exception as e:
            error_msg = f"Unexpected error during Google Maps scraping: {str(e)}"
            logging.error(error_msg)
            return jsonify({
                'error': 'An unexpected error occurred while scraping Google Maps search results',
                'details': str(e)
            }), 500
    else:
        logging.info(f"Detected single website URL, using WebScraper")
        
        try:
            # Use existing WebScraper for single website
            scraper = WebScraper(url)
            scraped_data = scraper.scrape()
            
            # Save to PostgreSQL
            try:
                document_data = {
                    'user_id': user_id,
                    'company_name': scraped_data['company_name'],
                    'email': scraped_data['email'] if scraped_data['email'] != 'N/A' else None,
                    'phone': scraped_data['phone'] if scraped_data['phone'] != 'N/A' else None,
                    'address': scraped_data['address'] if scraped_data['address'] != 'N/A' else None,
                    'website_url': url,
                    'source_url': url
                }
                
                document_id = ScrapedData.create(document_data)
                
                logging.info(f"Successfully scraped and saved data for {scraped_data['company_name']} (user {user_id}) with ID: {document_id}")
                
                # Get the saved document
                saved_document = ScrapedData.find_by_id(document_id)
                
                return jsonify({
                    'message': 'Data extracted successfully',
                    'data': saved_document
                }), 200
                
            except Exception as e:
                error_msg = f"PostgreSQL error while saving scraped data: {str(e)}"
                logging.error(error_msg)
                return jsonify({
                    'error': 'Failed to save scraped data to database',
                    'details': str(e)
                }), 500
                
        except Exception as e:
            error_msg = f"Unexpected error while scraping {url}: {str(e)}"
            logging.error(error_msg)
            return jsonify({
                'error': 'An unexpected error occurred while scraping the website',
                'details': str(e)
            }), 500

@scraper_bp.route('/sync-data', methods=['POST'])
@jwt_required()
def sync_local_data():
    """Sync local data to server PostgreSQL database"""
    try:
        user_id = int(get_jwt_identity())  # PostgreSQL user IDs are integers
        data = request.get_json()
        businesses = data.get('businesses', [])
        
        logging.info(f"Sync endpoint called by user {user_id} with {len(businesses)} businesses")
        
        if not businesses:
            return jsonify({'error': 'No businesses provided'}), 400
        
        # Verify user exists
        user = User.find_by_id(user_id)
        if not user:
            logging.warning(f"User {user_id} not found")
            return jsonify({'error': 'User not found'}), 404
        
        saved_count = 0
        errors = []
        
        for business in businesses:
            try:
                # Check if this business already exists
                existing = check_existing_business(
                    user_id,
                    business.get('company_name'),
                    business.get('website_url', '')
                )
                
                if existing:
                    logging.info(f"Business already exists: {business.get('company_name')}")
                    continue
                
                # Create document for PostgreSQL
                document_data = {
                    'user_id': user_id,
                    'company_name': business.get('company_name'),
                    'email': business.get('email') if business.get('email') not in ['N/A', 'Not found', None] else None,
                    'phone': business.get('phone') if business.get('phone') not in ['N/A', 'Not found', None] else None,
                    'address': business.get('address') if business.get('address') not in ['N/A', 'Not found', None] else None,
                    'website_url': business.get('website_url', ''),
                    'source_url': business.get('source_url', '')
                }
                
                document_id = ScrapedData.create(document_data)
                saved_count += 1
                logging.info(f"Added business to sync: {business.get('company_name')} with ID: {document_id}")
                
            except Exception as e:
                error_msg = f"Error syncing business {business.get('company_name', 'Unknown')}: {str(e)}"
                logging.error(error_msg)
                errors.append(error_msg)
        
        logging.info(f"Successfully synced {saved_count} businesses to PostgreSQL")
        
        return jsonify({
            'message': f'Successfully synced {saved_count} businesses to server',
            'synced_count': saved_count,
            'errors': errors
        }), 200
        
    except Exception as e:
        logging.error(f"Error in sync_local_data: {str(e)}")
        return jsonify({
            'error': 'Failed to sync data',
            'details': str(e)
        }), 500