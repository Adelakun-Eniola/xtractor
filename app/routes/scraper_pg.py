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
from app.models.search_job_pg import SearchJob
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

@scraper_bp.route('/init', methods=['POST'])
@jwt_required()
def init_search_job():
    """Initialize a scraping job: Create job, find businesses, return job ID"""
    search_scraper = None
    try:
        user_id = int(get_jwt_identity())
        data = request.get_json()
        url = data.get('url')
        
        logging.info(f"Init Job called by user {user_id} with URL: {url}")
        
        if not url or not is_google_maps_search_url(url):
             return jsonify({'error': 'Valid Google Maps search URL is required'}), 400

        # Stage 1: Get the list of businesses
        search_scraper = GoogleMapsSearchScraper(url)
        search_scraper.driver = search_scraper.setup_driver()
        
        # Scrape business list
        businesses_data = search_scraper.extract_businesses_with_names()
        
        search_scraper.driver.quit() # Close immediately
        
        if not businesses_data:
            return jsonify({'error': 'No businesses found at that location'}), 404
            
        # Create SearchJob entry
        items = []
        for b in businesses_data:
            items.append({
                'name': b['name'],
                'url': b['url'],
                'status': 'pending' 
            })
            
        job_data = {
            'user_id': user_id,
            'search_url': url,
            'items': items,
            'total_items': len(items)
        }
        
        job_id = SearchJob.create(job_data)
        
        return jsonify({
            'message': 'Job initialized',
            'job_id': job_id,
            'total_items': len(items)
        }), 201
        
    except Exception as e:
        logging.error(f"Error initializing job: {e}")
        if search_scraper and getattr(search_scraper, 'driver', None):
            try:
                search_scraper.driver.quit()
            except:
                pass
        return jsonify({'error': str(e)}), 500

@scraper_bp.route('/batch', methods=['POST'])
@jwt_required()
def process_batch():
    """Process a small batch of a SearchJob"""
    search_scraper = None
    try:
        user_id = int(get_jwt_identity())
        data = request.get_json()
        job_id = data.get('job_id')
        limit = data.get('limit', 5) # Default small batch
        
        if not job_id:
             return jsonify({'error': 'Job ID required'}), 400
             
        # Fetch Job
        job = SearchJob.find_by_id(job_id, user_id)
        if not job:
             return jsonify({'error': 'Job not found'}), 404
             
        if job['status'] == 'completed':
            return jsonify({'message': 'Job already completed', 'completed': True, 'results': []}), 200
            
        items = job['items']
        
        # Identify next batch
        batch_indices = []
        target_items = []
        
        for i, item in enumerate(items):
            if item['status'] == 'pending':
                batch_indices.append(i)
                target_items.append(item)
                if len(batch_indices) >= limit:
                    break
        
        if not target_items:
            # Mark job complete if no pending items
            SearchJob.update_progress(job_id, job['processed_items'], items, status='completed')
            return jsonify({'message': 'Job complete', 'completed': True, 'results': []}), 200
            
        # Process Batch
        results = []
        
        # Setup Scraper ONCE for the batch
        # For batch scraping, we can reuse one driver to save startup time
        # Since the batch is small (5 items), memory risk is low
        search_scraper = GoogleMapsSearchScraper("dummy") # URL doesn't matter here
        search_scraper.driver = search_scraper.setup_driver()
        
        processed_count_in_batch = 0
        
        for idx, item in zip(batch_indices, target_items):
            try:
                logging.info(f"Processing batch item {idx}: {item['name']}")
                
                # Scrape details
                # Phone
                phone = search_scraper.extract_phone_from_business_page(item['url'], driver=search_scraper.driver)
                
                # Address
                address = search_scraper.extract_address_from_business_page(item['url'], driver=search_scraper.driver)
                
                # Website
                website = search_scraper.extract_website_from_business_page(item['url'], driver=search_scraper.driver)
                
                # Email (Deep Scraping)
                email = None
                if website and 'http' in website:
                    # STRICTLY avoid deep scraping if it looks like a Google URL (already filtered by scraper, but double check)
                    if 'google.com' not in website:
                        try:
                            logging.info(f"Deep scraping email from: {website}")
                            email = search_scraper.extract_email_from_website(website, driver=None) # Create new driver for deep scrape to avoid state issues
                        except Exception as deep_err:
                            logging.warning(f"Deep scraping error for {website}: {deep_err}")

                # Prepare Data
                business_data = {
                    'company_name': item['name'],
                    'website_url': website if website else None, # Do NOT fallback to item['url']
                    'source_url': job['search_url'],
                    'address': address,
                    'phone': phone,
                    'email': email,
                    'user_id': user_id
                }
                
                # Save to DB (Incremental Persistence)
                existing = check_existing_business(user_id, business_data['company_name'], business_data['website_url'])
                if not existing:
                    ScrapedData.create(business_data)
                    logging.info(f"Saved: {item['name']}")
                
                # Update Item Status in Memory
                items[idx]['status'] = 'completed'
                processed_count_in_batch += 1
                results.append(business_data)
                
            except Exception as e:
                logging.error(f"Error processing item {item['name']}: {e}")
                items[idx]['status'] = 'failed' # Mark failed so we don't retry forever in this simple logic
        
        # Cleanup Driver
        search_scraper.driver.quit()
        
        # Update Job Progress in DB
        new_processed_count = job['processed_items'] + processed_count_in_batch
        status = 'active'
        if new_processed_count >= job['total_items']:
            status = 'completed'
            
        SearchJob.update_progress(job_id, new_processed_count, items, status=status)
        
        return jsonify({
            'results': results,
            'job_id': job_id,
            'processed': new_processed_count,
            'total': job['total_items'],
            'completed': status == 'completed'
        }), 200

    except Exception as e:
        logging.error(f"Batch processing error: {e}")
        if search_scraper and getattr(search_scraper, 'driver', None):
            try:
                search_scraper.driver.quit()
            except:
                pass
        return jsonify({'error': str(e)}), 500