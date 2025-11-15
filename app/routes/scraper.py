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

scraper_bp = Blueprint('scraper', __name__, url_prefix='/api/scraper')

@scraper_bp.route('/extract', methods=['POST'])
@jwt_required()
def extract_data():
    """Extract data from a website or Google Maps search results"""
    user_id = int(get_jwt_identity())  # Cast string to int (from JWT sub claim)
    
    # Get URL from request
    data = request.get_json()
    url = data.get('url')
    
    url_pattern = re.compile(r'^https?://[^\s/$.?#].[^\s]*$')
    if not url or not url_pattern.match(url):
        logging.warning(f"Invalid URL provided: {url}")
        return jsonify({'error': 'Invalid URL provided'}), 400
    
    # Verify user exists
    user = User.query.get(user_id)
    if not user:
        logging.error(f"User not found: {user_id}")
        return jsonify({'error': 'User not found'}), 404
    
    logging.info(f"User {user_id} initiated scraping for URL: {url}")
    
    # Check if URL is a Google Maps search URL
    if is_google_maps_search_url(url):
        logging.info(f"Detected Google Maps search URL, using GoogleMapsSearchScraper")
        
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
