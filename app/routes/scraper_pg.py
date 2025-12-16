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
    
    # Validate URL format
    url_pattern = re.compile(r'^https?://[^\s/$.?#].[^\s]*$')
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

@scraper_bp.route('/search-addresses', methods=['POST'])
@jwt_required()
def search_addresses():
    """Get addresses for businesses from Google Maps search URL"""
    search_scraper = None
    try:
        user_id = int(get_jwt_identity())  # PostgreSQL user IDs are integers
        data = request.get_json()
        url = data.get('url')
        stream = data.get('stream', False)  # Enable streaming
        
        logging.info(f"Search addresses endpoint called by user {user_id} with URL: {url}, stream: {stream}")
        
        # If streaming is requested, use SSE
        if stream:
            def generate():
                """Generator function for Server-Sent Events"""
                search_scraper = None
                try:
                    # Send initial status
                    yield f"data: {json.dumps({'type': 'status', 'message': 'Starting address extraction...'})}\\n\\n"
                    
                    # Check URL
                    if not url:
                        yield f"data: {json.dumps({'type': 'error', 'error': 'URL is required'})}\\n\\n"
                        return
                    
                    if not is_google_maps_search_url(url):
                        yield f"data: {json.dumps({'type': 'error', 'error': 'URL must be a Google Maps search URL'})}\\n\\n"
                        return
                    
                    # Create scraper
                    search_scraper = GoogleMapsSearchScraper(url)
                    search_scraper.driver = search_scraper.setup_driver()
                    
                    yield f"data: {json.dumps({'type': 'status', 'message': 'Extracting businesses...'})}\\n\\n"
                    
                    # Get all businesses first
                    businesses_data = search_scraper.extract_businesses_with_names()
                    
                    if not businesses_data:
                        yield f"data: {json.dumps({'type': 'complete', 'message': 'No businesses found', 'total': 0})}\\n\\n"
                        return
                    
                    total = len(businesses_data)
                    yield f"data: {json.dumps({'type': 'status', 'message': f'Found {total} businesses. Extracting phone numbers, addresses, websites, and emails...', 'total': total})}\\n\\n"
                    
                    # Collect businesses for database saving
                    extracted_businesses = []
                    
                    # Stream each business with address and phone
                    for i, business in enumerate(businesses_data, 1):
                        try:
                            business_info = {
                                'index': i,
                                'name': business['name'],
                                'url': business['url']
                            }
                            
                            # Extract phone number first
                            logging.info(f"Extracting phone for business {i}/{total}: {business['name']}")
                            try:
                                if hasattr(search_scraper, 'extract_phone_from_business_page'):
                                    phone = search_scraper.extract_phone_from_business_page(business['url'])
                                    business_info['phone'] = phone if phone else 'N/A'
                                    logging.info(f"Business {i}/{total}: {business['name']} - Phone: {business_info['phone']}")
                                else:
                                    logging.error(f"extract_phone_from_business_page method not found on scraper object")
                                    business_info['phone'] = 'N/A'
                            except Exception as extract_error:
                                logging.error(f"Error extracting phone for {business['name']}: {str(extract_error)}")
                                business_info['phone'] = 'N/A'
                            
                            # Restart driver for address extraction (memory optimization)
                            logging.info(f"Restarting driver for address extraction - business {i}/{total}")
                            try:
                                search_scraper.driver.quit()
                                import time
                                time.sleep(1)  # Wait for cleanup
                                search_scraper.driver = search_scraper.setup_driver()
                                logging.info("Driver restarted successfully for address extraction")
                            except Exception as restart_error:
                                logging.error(f"Error restarting driver for address extraction: {str(restart_error)}")
                            
                            # Extract address
                            logging.info(f"Extracting address for business {i}/{total}: {business['name']}")
                            try:
                                if hasattr(search_scraper, 'extract_address_from_business_page'):
                                    address = search_scraper.extract_address_from_business_page(business['url'])
                                    business_info['address'] = address if address else 'N/A'
                                    logging.info(f"Business {i}/{total}: {business['name']} - Address: {business_info['address']}")
                                else:
                                    logging.error(f"extract_address_from_business_page method not found on scraper object")
                                    business_info['address'] = 'N/A'
                            except Exception as extract_error:
                                logging.error(f"Error extracting address for {business['name']}: {str(extract_error)}")
                                business_info['address'] = 'N/A'
                            
                            # Restart driver for website extraction (memory optimization)
                            logging.info(f"Restarting driver for website extraction - business {i}/{total}")
                            try:
                                search_scraper.driver.quit()
                                import time
                                time.sleep(1)  # Wait for cleanup
                                search_scraper.driver = search_scraper.setup_driver()
                                logging.info("Driver restarted successfully for website extraction")
                            except Exception as restart_error:
                                logging.error(f"Error restarting driver for website extraction: {str(restart_error)}")
                            
                            # Extract website
                            logging.info(f"Extracting website for business {i}/{total}: {business['name']}")
                            try:
                                if hasattr(search_scraper, 'extract_website_from_business_page'):
                                    website = search_scraper.extract_website_from_business_page(business['url'])
                                    business_info['website'] = website if website else 'N/A'
                                    logging.info(f"Business {i}/{total}: {business['name']} - Website: {business_info['website']}")
                                else:
                                    logging.error(f"extract_website_from_business_page method not found on scraper object")
                                    business_info['website'] = 'N/A'
                            except Exception as extract_error:
                                logging.error(f"Error extracting website for {business['name']}: {str(extract_error)}")
                                business_info['website'] = 'N/A'
                            
                            # Restart driver for email extraction (memory optimization)
                            logging.info(f"Restarting driver for email extraction - business {i}/{total}")
                            try:
                                search_scraper.driver.quit()
                                import time
                                time.sleep(1)  # Wait for cleanup
                                search_scraper.driver = search_scraper.setup_driver()
                                logging.info("Driver restarted successfully for email extraction")
                            except Exception as restart_error:
                                logging.error(f"Error restarting driver for email extraction: {str(restart_error)}")
                            
                            # Extract email from website
                            logging.info(f"Extracting email for business {i}/{total}: {business['name']}")
                            try:
                                if hasattr(search_scraper, 'extract_email_from_website'):
                                    email = search_scraper.extract_email_from_website(business_info['website'])
                                    business_info['email'] = email if email else 'N/A'
                                    logging.info(f"Business {i}/{total}: {business['name']} - Email: {business_info['email']}")
                                else:
                                    logging.error(f"extract_email_from_website method not found on scraper object")
                                    business_info['email'] = 'N/A'
                            except Exception as extract_error:
                                logging.error(f"Error extracting email for {business['name']}: {str(extract_error)}")
                                business_info['email'] = 'N/A'
                            
                            # Collect for PostgreSQL saving
                            extracted_businesses.append({
                                'company_name': business_info['name'],
                                'email': business_info['email'] if business_info['email'] not in ['N/A', 'Not found'] else None,
                                'phone': business_info['phone'] if business_info['phone'] not in ['N/A', 'Not found'] else None,
                                'address': business_info['address'] if business_info['address'] not in ['N/A', 'Not found'] else None,
                                'website_url': business_info['website'] if business_info['website'] not in ['N/A', 'Not found'] else business_info['url'],
                                'user_id': user_id,
                                'source_url': url
                            })
                            
                            # Send this business with phone, address, website, and email
                            yield f"data: {json.dumps({'type': 'business', 'data': business_info, 'progress': {'current': i, 'total': total}})}\\n\\n"
                            
                            # Memory optimization: Restart driver after EVERY business to free memory (Render 512MB limit)
                            if i < total:
                                logging.info(f"Restarting driver after business {i} to free memory")
                                try:
                                    search_scraper.driver.quit()
                                    import time
                                    time.sleep(1)  # Wait for cleanup
                                    search_scraper.driver = search_scraper.setup_driver()
                                    logging.info("Driver restarted successfully")
                                except Exception as restart_error:
                                    logging.error(f"Error restarting driver: {str(restart_error)}")
                            
                        except Exception as business_error:
                            logging.error(f"Error processing business {i}/{total}: {str(business_error)}")
                            # Send error for this business but continue
                            yield f"data: {json.dumps({'type': 'business', 'data': {'index': i, 'name': 'Error', 'url': '', 'phone': 'N/A', 'address': 'N/A', 'website': 'N/A', 'email': 'N/A'}, 'progress': {'current': i, 'total': total}})}\\n\\n"
                            continue
                    
                    # Save all businesses to PostgreSQL in batch
                    saved_count = 0
                    if extracted_businesses:
                        try:
                            for business_data in extracted_businesses:
                                try:
                                    # Check if business already exists in PostgreSQL
                                    existing = check_existing_business(
                                        user_id, 
                                        business_data['company_name'], 
                                        business_data['website_url']
                                    )
                                    
                                    if not existing:
                                        ScrapedData.create(business_data)
                                        saved_count += 1
                                except Exception as e:
                                    logging.error(f"Error adding business to PostgreSQL: {e}")
                                    continue
                            
                            logging.info(f"Successfully saved {saved_count} businesses to PostgreSQL")
                                
                        except Exception as e:
                            logging.error(f"PostgreSQL batch save failed: {e}")
                    
                    # Send completion
                    yield f"data: {json.dumps({'type': 'complete', 'message': f'Completed! Extracted {total} businesses (saved {saved_count} to database)', 'total': total})}\\n\\n"
                    
                except Exception as e:
                    logging.error(f"Error in address streaming: {str(e)}")
                    yield f"data: {json.dumps({'type': 'error', 'error': str(e)})}\\n\\n"
                finally:
                    if search_scraper and search_scraper.driver:
                        try:
                            search_scraper.driver.quit()
                        except:
                            pass
            
            response = Response(generate(), mimetype='text/event-stream')
            response.headers['Cache-Control'] = 'no-cache'
            response.headers['X-Accel-Buffering'] = 'no'
            return response
        
        # Non-streaming version (if needed)
        return jsonify({'error': 'Non-streaming address extraction not implemented'}), 400
                
    except Exception as e:
        logging.error(f"Unexpected error in search_addresses: {str(e)}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        
        # Try to close driver if it exists
        if search_scraper and hasattr(search_scraper, 'driver') and search_scraper.driver:
            try:
                search_scraper.driver.quit()
            except:
                pass
        
        return jsonify({
            'error': 'Failed to search addresses',
            'details': str(e)
        }), 500