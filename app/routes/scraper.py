from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app import db
from app.models.scraped_data import ScrapedData
from app.models.user import User
from app.services.scraper import WebScraper, is_google_maps_search_url, GoogleMapsSearchScraper
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import re
import os
import logging

from flask_cors import CORS



scraper_bp = Blueprint('scraper', __name__, url_prefix='/api/scraper')
CORS(scraper_bp)
@scraper_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'message': 'Scraper service is running',
        'chromium_path': os.getenv('CHROMIUM_PATH', 'Not set'),
        'chromedriver_path': os.getenv('CHROMEDRIVER_PATH', 'Not set')
    }), 200

@scraper_bp.route('/test-search', methods=['POST'])
def test_search_businesses():
    """Test endpoint for business search without authentication (for debugging)"""
    try:
        data = request.get_json()
        url = data.get('url')
        
        logging.info(f"Test search endpoint called with URL: {url}")
        
        if not url:
            return jsonify({'error': 'URL is required'}), 400
        
        # Check if it's a Google Maps search URL
        if not is_google_maps_search_url(url):
            return jsonify({'error': 'URL must be a Google Maps search URL'}), 400
        
        # Extract businesses with names from search results
        search_scraper = GoogleMapsSearchScraper(url)
        
        try:
            search_scraper.driver = search_scraper.setup_driver()
            businesses_data = search_scraper.extract_businesses_with_names()
            
            if not businesses_data:
                return jsonify({
                    'message': 'No businesses found',
                    'count': 0,
                    'businesses': []
                }), 200
            
            # Add index to each business
            businesses = []
            for i, business in enumerate(businesses_data):
                business_info = {
                    'index': i+1,
                    'name': business['name'],
                    'url': business['url']
                }
                businesses.append(business_info)
            
            logging.info(f"Test search found {len(businesses)} businesses")
            
            return jsonify({
                'message': f'Found {len(businesses)} businesses',
                'count': len(businesses),
                'businesses': businesses
            }), 200
            
        finally:
            if search_scraper and search_scraper.driver:
                search_scraper.driver.quit()
                
    except Exception as e:
        logging.error(f"Error in test search: {str(e)}")
        return jsonify({
            'error': 'Failed to search businesses',
            'details': str(e)
        }), 500

@scraper_bp.route('/extract', methods=['POST'])
@jwt_required()
def extract_data():
    """Extract data from a website or Google Maps search results"""
    try:
        user_id = int(get_jwt_identity())
        
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
    
    url_pattern = re.compile(r'^https?://[^\s/$.?#].[^\s]*$')
    if not url or not url_pattern.match(url):
        logging.warning(f"Invalid URL provided: {url}")
        return jsonify({'error': 'Invalid URL provided'}), 400
    
    # Verify user exists (create if doesn't exist)
    user = User.query.get(user_id)
    if not user:
        logging.warning(f"User {user_id} not found, creating placeholder user")
        user = User(id=user_id, email=f"user{user_id}@placeholder.com", name=f"User {user_id}", google_id=f"placeholder_{user_id}")
        db.session.add(user)
        try:
            db.session.commit()
            logging.info(f"Created placeholder user {user_id}")
        except:
            db.session.rollback()
            logging.error(f"Failed to create user {user_id}, but continuing anyway")
    
    logging.info(f"User {user_id} initiated scraping for URL: {url}")
    
    # Check if URL is a Google Maps search URL
    if is_google_maps_search_url(url):
        logging.info(f"Detected Google Maps search URL, using GoogleMapsSearchScraper")
        
        # If streaming is requested, use streaming endpoint
        if stream:
            from flask import Response
            import json
            
            def generate():
                """Generator function that yields results as they're scraped"""
                try:
                    search_scraper = GoogleMapsSearchScraper(url)
                    
                    # Send initial status
                    yield f"data: {json.dumps({'type': 'status', 'message': 'Starting scraping...'})}\n\n"
                    
                    # Stream results as they come
                    for result in search_scraper.scrape_all_businesses_stream(user_id):
                        yield f"data: {json.dumps(result)}\n\n"
                    
                    # Send completion
                    yield f"data: {json.dumps({'type': 'complete'})}\n\n"
                    
                except Exception as e:
                    error_data = {
                        'type': 'error',
                        'error': str(e)
                    }
                    yield f"data: {json.dumps(error_data)}\n\n"
            
            return Response(generate(), mimetype='text/event-stream')
        
        # Non-streaming (original behavior)
        try:
            # Use GoogleMapsSearchScraper for search results
            search_scraper = GoogleMapsSearchScraper(url)
            result = search_scraper.scrape_all_businesses(user_id)
            
            total_results = len(result['results'])
            total_errors = len(result['errors'])
            
            message = f'Extracted {total_results} business{"es" if total_results != 1 else ""}'
            if total_errors > 0:
                message += f' with {total_errors} error{"s" if total_errors != 1 else ""}'
            
            logging.info(f"Google Maps scraping complete for user {user_id}: {message}")
            
            return jsonify({
                'message': message,
                'data': result['results'],
                'errors': result['errors']
            }), 200
            
        except WebDriverException as e:
            error_msg = f"WebDriver error during Google Maps scraping: {str(e)}"
            logging.error(error_msg)
            return jsonify({
                'error': 'Failed to scrape Google Maps search results due to browser driver error',
                'details': str(e)
            }), 500
            
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
            
            # Save to database
            try:
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
                
                logging.info(f"Successfully scraped and saved data for {scraped_data['company_name']} (user {user_id})")
                
                return jsonify({
                    'message': 'Data extracted successfully',
                    'data': new_data.to_dict()
                }), 200
                
            except Exception as e:
                error_msg = f"Database error while saving scraped data: {str(e)}"
                logging.error(error_msg)
                db.session.rollback()
                return jsonify({
                    'error': 'Failed to save scraped data to database',
                    'details': str(e)
                }), 500
                
        except TimeoutException as e:
            error_msg = f"Timeout while scraping {url}: {str(e)}"
            logging.warning(error_msg)
            return jsonify({
                'error': 'Timeout while loading the website',
                'details': str(e)
            }), 408
            
        except NoSuchElementException as e:
            error_msg = f"Required element not found on {url}: {str(e)}"
            logging.warning(error_msg)
            return jsonify({
                'error': 'Could not find required information on the website',
                'details': str(e)
            }), 422
            
        except WebDriverException as e:
            error_msg = f"WebDriver error while scraping {url}: {str(e)}"
            logging.error(error_msg)
            return jsonify({
                'error': 'Failed to scrape website due to browser driver error',
                'details': str(e)
            }), 500
            
        except Exception as e:
            error_msg = f"Unexpected error while scraping {url}: {str(e)}"
            logging.error(error_msg)
            return jsonify({
                'error': 'An unexpected error occurred while scraping the website',
                'details': str(e)
            }), 500

@scraper_bp.route('/search-businesses', methods=['POST'])
@jwt_required()
def search_businesses():
    """Get list of businesses from Google Maps search URL without scraping details"""
    search_scraper = None
    try:
        user_id = int(get_jwt_identity())
        data = request.get_json()
        url = data.get('url')
        include_phone = data.get('include_phone', False)  # Optional parameter
        phone_limit = data.get('phone_limit', 10)  # Limit phone extractions
        stream = data.get('stream', False)  # Enable streaming
        
        logging.info(f"Search businesses endpoint called by user {user_id} with URL: {url}, include_phone: {include_phone}, stream: {stream}")
        
        # If streaming is requested, use SSE
        if stream and include_phone:
            from flask import Response
            import json
            
            def generate():
                """Generator function for Server-Sent Events"""
                search_scraper = None
                try:
                    # Send initial status
                    yield f"data: {json.dumps({'type': 'status', 'message': 'Starting search...'})}\n\n"
                    
                    # Check URL
                    if not url:
                        yield f"data: {json.dumps({'type': 'error', 'error': 'URL is required'})}\n\n"
                        return
                    
                    if not is_google_maps_search_url(url):
                        yield f"data: {json.dumps({'type': 'error', 'error': 'URL must be a Google Maps search URL'})}\n\n"
                        return
                    
                    # Create scraper
                    search_scraper = GoogleMapsSearchScraper(url)
                    search_scraper.driver = search_scraper.setup_driver()
                    
                    yield f"data: {json.dumps({'type': 'status', 'message': 'Extracting businesses...'})}\n\n"
                    
                    # Get all businesses first
                    businesses_data = search_scraper.extract_businesses_with_names()
                    
                    if not businesses_data:
                        yield f"data: {json.dumps({'type': 'complete', 'message': 'No businesses found', 'total': 0})}\n\n"
                        return
                    
                    total = len(businesses_data)
                    yield f"data: {json.dumps({'type': 'status', 'message': f'Found {total} businesses. Extracting phone numbers...', 'total': total})}\n\n"
                    
                    # Stream each business with phone
                    for i, business in enumerate(businesses_data, 1):
                        try:
                            business_info = {
                                'index': i,
                                'name': business['name'],
                                'url': business['url']
                            }
                            
                            # Extract phone only (address extraction removed for stability)
                            if business.get('phone'):
                                business_info['phone'] = business['phone']
                                logging.info(f"Business {i}/{total}: {business['name']} - Phone from listing: {business['phone']}")
                            else:
                                logging.info(f"Extracting phone for business {i}/{total}: {business['name']}")
                                try:
                                    phone = search_scraper.extract_phone_from_business_page(business['url'])
                                    business_info['phone'] = phone if phone else 'N/A'
                                    logging.info(f"Business {i}/{total}: {business['name']} - Phone: {business_info['phone']}")
                                except Exception as extract_error:
                                    logging.error(f"Error extracting phone for {business['name']}: {str(extract_error)}")
                                    business_info['phone'] = 'N/A'
                            
                            # Send this business immediately
                            yield f"data: {json.dumps({'type': 'business', 'data': business_info, 'progress': {'current': i, 'total': total}})}\n\n"
                            
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
                            yield f"data: {json.dumps({'type': 'business', 'data': {'index': i, 'name': 'Error', 'url': '', 'phone': 'N/A'}, 'progress': {'current': i, 'total': total}})}\n\n"
                            continue
                    
                    # Send completion
                    yield f"data: {json.dumps({'type': 'complete', 'message': f'Completed! Extracted {total} businesses', 'total': total})}\n\n"
                    
                except Exception as e:
                    logging.error(f"Error in streaming: {str(e)}")
                    yield f"data: {json.dumps({'type': 'error', 'error': str(e)})}\n\n"
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
        
        # Non-streaming (original behavior)
        
        if not url:
            logging.error("No URL provided in request")
            return jsonify({'error': 'URL is required'}), 400
        
        # Check if it's a Google Maps search URL
        if not is_google_maps_search_url(url):
            logging.error(f"URL is not a Google Maps search URL: {url}")
            return jsonify({'error': 'URL must be a Google Maps search URL'}), 400
        
        # Extract businesses with names from search results
        logging.info("Creating GoogleMapsSearchScraper instance")
        search_scraper = GoogleMapsSearchScraper(url)
        
        logging.info("Setting up WebDriver")
        try:
            search_scraper.driver = search_scraper.setup_driver()
            logging.info("WebDriver setup successful")
        except Exception as driver_error:
            logging.error(f"Failed to setup WebDriver: {str(driver_error)}")
            return jsonify({
                'error': 'Failed to initialize browser',
                'details': str(driver_error)
            }), 500
        
        try:
            logging.info("Extracting businesses with names")
            businesses_data = search_scraper.extract_businesses_with_names()
            logging.info(f"Extraction complete. Found {len(businesses_data)} businesses")
            
            if not businesses_data:
                logging.warning("No businesses found in search results")
                return jsonify({
                    'message': 'No businesses found',
                    'count': 0,
                    'businesses': []
                }), 200
            
            # Add index to each business and optionally extract phone
            businesses = []
            phones_extracted = 0
            
            for i, business in enumerate(businesses_data):
                business_info = {
                    'index': i+1,
                    'name': business['name'],
                    'url': business['url']
                }
                
                # Handle phone numbers
                if include_phone and phones_extracted < phone_limit:
                    # Check if phone was already extracted from search results
                    if business.get('phone'):
                        business_info['phone'] = business['phone']
                        logging.info(f"Business {i+1}/{len(businesses_data)}: {business['name']} - Phone from listing: {business['phone']}")
                        phones_extracted += 1
                    else:
                        # Visit individual page to extract phone (only up to limit)
                        logging.info(f"Extracting phone {phones_extracted+1}/{phone_limit} for: {business['name']}")
                        phone = search_scraper.extract_phone_from_business_page(business['url'])
                        business_info['phone'] = phone if phone else 'N/A'
                        phones_extracted += 1
                        if phone:
                            logging.info(f"Business {i+1}/{len(businesses_data)}: {business['name']} - Phone: {phone}")
                else:
                    # Even if not requested, include phone if it was found in search results
                    if business.get('phone'):
                        business_info['phone'] = business['phone']
                
                businesses.append(business_info)
            
            logging.info(f"Extracted phones for {phones_extracted} businesses")
            
            logging.info(f"Successfully found {len(businesses)} businesses for user {user_id}")
            
            return jsonify({
                'message': f'Found {len(businesses)} businesses',
                'count': len(businesses),
                'businesses': businesses
            }), 200
            
        except TimeoutException as e:
            logging.error(f"Timeout while extracting businesses: {str(e)}")
            return jsonify({
                'error': 'Timeout while loading Google Maps',
                'details': str(e)
            }), 408
            
        except WebDriverException as e:
            logging.error(f"WebDriver error while extracting businesses: {str(e)}")
            return jsonify({
                'error': 'Browser error occurred',
                'details': str(e)
            }), 500
            
        finally:
            if search_scraper and search_scraper.driver:
                try:
                    search_scraper.driver.quit()
                    logging.info("WebDriver closed successfully")
                except Exception as quit_error:
                    logging.error(f"Error closing WebDriver: {str(quit_error)}")
                
    except Exception as e:
        logging.error(f"Unexpected error in search_businesses: {str(e)}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        
        # Try to close driver if it exists
        if search_scraper and hasattr(search_scraper, 'driver') and search_scraper.driver:
            try:
                search_scraper.driver.quit()
            except:
                pass
        
        return jsonify({
            'error': 'Failed to search businesses',
            'details': str(e)
        }), 500

@scraper_bp.route('/batch', methods=['POST'])
@jwt_required()
def batch_extract():
    """Extract data from multiple websites"""
    user_id = int(get_jwt_identity())  # Cast string to int
    
    # Verify user exists
    user = User.query.get(user_id)
    if not user:
        logging.error(f"User not found: {user_id}")
        return jsonify({'error': 'User not found'}), 404
    
    data = request.get_json()
    urls = data.get('urls', [])
    
    if not urls or not isinstance(urls, list):
        logging.warning(f"Invalid batch request: no URLs or invalid format")
        return jsonify({'error': 'No URLs provided or invalid format'}), 400
    
    logging.info(f"User {user_id} initiated batch scraping for {len(urls)} URLs")
    
    results, errors = [], []
    
    for url in urls:
        if not re.match(r'^https?://[^\s/$.?#].[^\s]*$', url):
            logging.warning(f"Invalid URL format in batch: {url}")
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
            logging.info(f"Successfully scraped {url} in batch")
            
        except TimeoutException as e:
            error_msg = f"Timeout: {str(e)}"
            logging.warning(f"Timeout for {url} in batch: {error_msg}")
            errors.append({'url': url, 'error': error_msg})
            
        except NoSuchElementException as e:
            error_msg = f"Required element not found: {str(e)}"
            logging.warning(f"Element not found for {url} in batch: {error_msg}")
            errors.append({'url': url, 'error': error_msg})
            
        except WebDriverException as e:
            error_msg = f"WebDriver error: {str(e)}"
            logging.error(f"WebDriver error for {url} in batch: {error_msg}")
            errors.append({'url': url, 'error': error_msg})
            
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Unexpected error for {url} in batch: {error_msg}")
            errors.append({'url': url, 'error': error_msg})
    
    if results:
        try:
            db.session.commit()
            logging.info(f"Successfully committed {len(results)} batch results to database")
        except Exception as e:
            error_msg = f"Database commit error: {str(e)}"
            logging.error(error_msg)
            db.session.rollback()
            return jsonify({
                'error': 'Failed to save batch results to database',
                'details': str(e)
            }), 500
    
    logging.info(f"Batch scraping complete for user {user_id}: {len(results)} successful, {len(errors)} errors")
    
    return jsonify({
        'message': f'Processed {len(results)} URLs successfully with {len(errors)} errors',
        'results': results,
        'errors': errors
    }), 200
@scraper_bp.route('/search-addresses', methods=['POST'])
@jwt_required()
def search_addresses():
    """Get addresses for businesses from Google Maps search URL"""
    search_scraper = None
    try:
        user_id = int(get_jwt_identity())
        data = request.get_json()
        url = data.get('url')
        stream = data.get('stream', False)  # Enable streaming
        
        logging.info(f"Search addresses endpoint called by user {user_id} with URL: {url}, stream: {stream}")
        
        # If streaming is requested, use SSE
        if stream:
            from flask import Response
            import json
            
            def generate():
                """Generator function for Server-Sent Events"""
                search_scraper = None
                try:
                    # Send initial status
                    yield f"data: {json.dumps({'type': 'status', 'message': 'Starting address extraction...'})}\n\n"
                    
                    # Check URL
                    if not url:
                        yield f"data: {json.dumps({'type': 'error', 'error': 'URL is required'})}\n\n"
                        return
                    
                    if not is_google_maps_search_url(url):
                        yield f"data: {json.dumps({'type': 'error', 'error': 'URL must be a Google Maps search URL'})}\n\n"
                        return
                    
                    # Create scraper
                    search_scraper = GoogleMapsSearchScraper(url)
                    search_scraper.driver = search_scraper.setup_driver()
                    
                    yield f"data: {json.dumps({'type': 'status', 'message': 'Extracting businesses...'})}\n\n"
                    
                    # Get all businesses first
                    businesses_data = search_scraper.extract_businesses_with_names()
                    
                    if not businesses_data:
                        yield f"data: {json.dumps({'type': 'complete', 'message': 'No businesses found', 'total': 0})}\n\n"
                        return
                    
                    total = len(businesses_data)
                    yield f"data: {json.dumps({'type': 'status', 'message': f'Found {total} businesses. Extracting phone numbers, addresses, websites, and emails...', 'total': total})}\n\n"
                    
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
                                phone = search_scraper.extract_phone_from_business_page(business['url'])
                                business_info['phone'] = phone if phone else 'N/A'
                                logging.info(f"Business {i}/{total}: {business['name']} - Phone: {business_info['phone']}")
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
                                address = search_scraper.extract_address_from_business_page(business['url'])
                                business_info['address'] = address if address else 'N/A'
                                logging.info(f"Business {i}/{total}: {business['name']} - Address: {business_info['address']}")
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
                                website = search_scraper.extract_website_from_business_page(business['url'])
                                business_info['website'] = website if website else 'N/A'
                                logging.info(f"Business {i}/{total}: {business['name']} - Website: {business_info['website']}")
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
                                email = search_scraper.extract_email_from_website(business_info['website'])
                                business_info['email'] = email if email else 'N/A'
                                logging.info(f"Business {i}/{total}: {business['name']} - Email: {business_info['email']}")
                            except Exception as extract_error:
                                logging.error(f"Error extracting email for {business['name']}: {str(extract_error)}")
                                business_info['email'] = 'N/A'
                            
                            # Send this business with phone, address, website, and email
                            yield f"data: {json.dumps({'type': 'business', 'data': business_info, 'progress': {'current': i, 'total': total}})}\n\n"
                            
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
                            yield f"data: {json.dumps({'type': 'business', 'data': {'index': i, 'name': 'Error', 'url': '', 'phone': 'N/A', 'address': 'N/A', 'website': 'N/A', 'email': 'N/A'}, 'progress': {'current': i, 'total': total}})}\n\n"
                            continue
                    
                    # Send completion
                    yield f"data: {json.dumps({'type': 'complete', 'message': f'Completed! Extracted {total} businesses', 'total': total})}\n\n"
                    
                except Exception as e:
                    logging.error(f"Error in address streaming: {str(e)}")
                    yield f"data: {json.dumps({'type': 'error', 'error': str(e)})}\n\n"
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

@scraper_bp.route('/sync-data', methods=['POST'])
@jwt_required()
def sync_local_data():
    """Sync local data to server database"""
    try:
        user_id = int(get_jwt_identity())
        data = request.get_json()
        businesses = data.get('businesses', [])
        
        logging.info(f"Sync endpoint called by user {user_id} with {len(businesses)} businesses")
        
        if not businesses:
            return jsonify({'error': 'No businesses provided'}), 400
        
        # Verify user exists
        user = User.query.get(user_id)
        if not user:
            logging.warning(f"User {user_id} not found, creating placeholder user")
            user = User(id=user_id, email=f"user{user_id}@placeholder.com", name=f"User {user_id}", google_id=f"placeholder_{user_id}")
            db.session.add(user)
            try:
                db.session.commit()
                logging.info(f"Created placeholder user {user_id}")
            except:
                db.session.rollback()
                logging.error(f"Failed to create user {user_id}, but continuing anyway")
        
        saved_count = 0
        errors = []
        
        for business in businesses:
            try:
                # Check if this business already exists (by company name and website)
                existing = ScrapedData.query.filter_by(
                    user_id=user_id,
                    company_name=business.get('company_name'),
                    website_url=business.get('website_url', '')
                ).first()
                
                if existing:
                    logging.info(f"Business already exists: {business.get('company_name')}")
                    continue
                
                # Create new scraped data entry
                new_data = ScrapedData(
                    company_name=business.get('company_name'),
                    email=business.get('email') if business.get('email') not in ['N/A', 'Not found', None] else None,
                    phone=business.get('phone') if business.get('phone') not in ['N/A', 'Not found', None] else None,
                    address=business.get('address') if business.get('address') not in ['N/A', 'Not found', None] else None,
                    website_url=business.get('website_url', ''),
                    user_id=user_id
                )
                
                # Set created_at if provided
                if business.get('created_at'):
                    try:
                        from datetime import datetime
                        new_data.created_at = datetime.fromisoformat(business['created_at'].replace('Z', '+00:00'))
                    except:
                        pass  # Use default timestamp
                
                db.session.add(new_data)
                saved_count += 1
                logging.info(f"Added business to sync: {business.get('company_name')}")
                
            except Exception as e:
                error_msg = f"Error syncing business {business.get('company_name', 'Unknown')}: {str(e)}"
                logging.error(error_msg)
                errors.append(error_msg)
        
        if saved_count > 0:
            try:
                db.session.commit()
                logging.info(f"Successfully synced {saved_count} businesses to database")
            except Exception as e:
                db.session.rollback()
                logging.error(f"Failed to commit synced data: {str(e)}")
                return jsonify({
                    'error': 'Failed to save synced data to database',
                    'details': str(e)
                }), 500
        
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

@scraper_bp.route('/test-addresses', methods=['POST'])
def test_address_extraction():
    """Test endpoint for address extraction without authentication (for debugging)"""
    try:
        data = request.get_json()
        url = data.get('url')
        
        logging.info(f"Test address endpoint called with URL: {url}")
        
        if not url:
            return jsonify({'error': 'URL is required'}), 400
        
        # Check if it's a Google Maps search URL
        if not is_google_maps_search_url(url):
            return jsonify({'error': 'URL must be a Google Maps search URL'}), 400
        
        # Extract businesses with addresses from search results
        search_scraper = GoogleMapsSearchScraper(url)
        
        try:
            search_scraper.driver = search_scraper.setup_driver()
            businesses_data = search_scraper.extract_businesses_with_names()
            
            if not businesses_data:
                return jsonify({
                    'message': 'No businesses found',
                    'count': 0,
                    'businesses': []
                }), 200
            
            # Extract addresses for first 3 businesses (for testing)
            businesses = []
            for i, business in enumerate(businesses_data[:3]):  # Limit to 3 for testing
                business_info = {
                    'index': i+1,
                    'name': business['name'],
                    'url': business['url']
                }
                
                # Extract address
                logging.info(f"Extracting address for business {i+1}: {business['name']}")
                try:
                    address = search_scraper.extract_address_from_business_page(business['url'])
                    business_info['address'] = address if address else 'N/A'
                    logging.info(f"Address extracted: {business_info['address']}")
                except Exception as extract_error:
                    logging.error(f"Error extracting address: {str(extract_error)}")
                    business_info['address'] = 'N/A'
                
                # Restart driver for phone extraction
                logging.info(f"Restarting driver for phone extraction - business {i+1}")
                try:
                    search_scraper.driver.quit()
                    import time
                    time.sleep(1)
                    search_scraper.driver = search_scraper.setup_driver()
                    logging.info("Driver restarted for phone extraction")
                except Exception as restart_error:
                    logging.error(f"Error restarting driver for phone: {str(restart_error)}")
                
                # Extract phone
                logging.info(f"Extracting phone for business {i+1}: {business['name']}")
                try:
                    phone = search_scraper.extract_phone_from_business_page(business['url'])
                    business_info['phone'] = phone if phone else 'N/A'
                    logging.info(f"Phone extracted: {business_info['phone']}")
                except Exception as extract_error:
                    logging.error(f"Error extracting phone: {str(extract_error)}")
                    business_info['phone'] = 'N/A'
                
                # Restart driver for website extraction
                logging.info(f"Restarting driver for website extraction - business {i+1}")
                try:
                    search_scraper.driver.quit()
                    import time
                    time.sleep(1)
                    search_scraper.driver = search_scraper.setup_driver()
                    logging.info("Driver restarted for website extraction")
                except Exception as restart_error:
                    logging.error(f"Error restarting driver for website: {str(restart_error)}")
                
                # Extract website
                logging.info(f"Extracting website for business {i+1}: {business['name']}")
                try:
                    website = search_scraper.extract_website_from_business_page(business['url'])
                    business_info['website'] = website if website else 'N/A'
                    logging.info(f"Website extracted: {business_info['website']}")
                except Exception as extract_error:
                    logging.error(f"Error extracting website: {str(extract_error)}")
                    business_info['website'] = 'N/A'
                
                # Restart driver for email extraction
                logging.info(f"Restarting driver for email extraction - business {i+1}")
                try:
                    search_scraper.driver.quit()
                    import time
                    time.sleep(1)
                    search_scraper.driver = search_scraper.setup_driver()
                    logging.info("Driver restarted for email extraction")
                except Exception as restart_error:
                    logging.error(f"Error restarting driver for email: {str(restart_error)}")
                
                # Extract email from website
                logging.info(f"Extracting email for business {i+1}: {business['name']}")
                try:
                    email = search_scraper.extract_email_from_website(business_info['website'])
                    business_info['email'] = email if email else 'N/A'
                    logging.info(f"Email extracted: {business_info['email']}")
                except Exception as extract_error:
                    logging.error(f"Error extracting email: {str(extract_error)}")
                    business_info['email'] = 'N/A'
                
                businesses.append(business_info)
                
                # Restart driver to free memory
                if i < 2:  # Don't restart after the last one
                    try:
                        search_scraper.driver.quit()
                        import time
                        time.sleep(1)
                        search_scraper.driver = search_scraper.setup_driver()
                    except Exception as restart_error:
                        logging.error(f"Error restarting driver: {str(restart_error)}")
            
            logging.info(f"Test address extraction found {len(businesses)} businesses")
            
            return jsonify({
                'message': f'Found {len(businesses)} businesses with addresses (limited to 3 for testing)',
                'count': len(businesses),
                'businesses': businesses
            }), 200
            
        finally:
            if search_scraper and search_scraper.driver:
                search_scraper.driver.quit()
                
    except Exception as e:
        logging.error(f"Error in test address extraction: {str(e)}")
        return jsonify({
            'error': 'Failed to extract addresses',
            'details': str(e)
        }), 500