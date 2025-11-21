import pandas as pd
import re
from email_validator import validate_email
from webdriver_manager.chrome import ChromeDriverManager
import platform
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import logging
from selenium.webdriver.chrome.service import Service
import os
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def is_google_maps_search_url(url):
    """
    Detect if URL is a Google Maps search results page.
    
    Args:
        url: The URL to check
        
    Returns:
        True if URL is a Google Maps search, False otherwise
    """
    if not url or not isinstance(url, str):
        return False
    
    url_lower = url.lower()
    
    # Check for google.com/maps/search pattern
    if 'google.com/maps/search' in url_lower:
        return True
    
    # Check for google maps URL with search query parameters
    if 'google.com/maps' in url_lower:
        # Check for search indicators in query parameters
        search_indicators = ['query=', 'q=', 'data=']
        for indicator in search_indicators:
            if indicator in url_lower:
                return True
    
    return False


class WebScraper:
    def __init__(self, url):
        self.url = url
        self.driver = None
        self.data = {
            'company_name': 'N/A',
            'email': 'N/A',
            'phone': 'N/A',
            'address': 'N/A',
            'website_url': self.validate_url(url)
        }

    def validate_phone_number(self, phone_number):
        phone_pattern = r'^\+?\d{1,4}?[-.\s]?\(?\d{1,3}?\)?[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}$'
        return phone_number if phone_number != "N/A" and re.match(phone_pattern, phone_number) else "N/A"

    def validate_email_address(self, email_address):
        try:
            validate_email(email_address, check_deliverability=False)
            return email_address
        except Exception:
            return "N/A"

    def validate_url(self, url):
        url_pattern = r'^(https?:\/\/)?([\w\-]+(\.[\w\-]+)+)(\/.*)?$'
        return url if url != "N/A" and re.match(url_pattern, url, re.IGNORECASE) else "N/A"



    def setup_driver(self, headless=True):
        logging.info(f"Setting up Chromium webdriver (headless={headless})")

        options = webdriver.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--remote-debugging-port=9222")
        options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36")

        if headless:
            options.add_argument("--headless=new")

        system = platform.system().lower()

        if system == "darwin":  # macOS dev
            # Use Chrome + webdriver_manager
            driver_path = ChromeDriverManager().install()
            service = Service(driver_path)
            driver = webdriver.Chrome(service=service, options=options)
        else:  # Linux (Render)
            chromium_path = os.getenv("CHROMIUM_PATH", "/usr/bin/chromium")
            chromedriver_path = os.getenv("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")
            options.binary_location = chromium_path
            service = Service(executable_path=chromedriver_path)
            driver = webdriver.Chrome(service=service, options=options)

        return driver

    def extract_info(self):
        logging.info(f"Extracting info from: {self.url}")
        try:
            self.driver.get(self.url)
            WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(3)
        except TimeoutException:
            logging.warning(f"Timeout navigating to {self.url}.")
            return self.data
        except WebDriverException as e:
            logging.error(f"WebDriver error navigating to {self.url}: {e}")
            return self.data

        try:
            business_name_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//h1|//h2"))
            )
            self.data['company_name'] = business_name_element.text.strip() or "N/A"
        except (TimeoutException, NoSuchElementException):
            logging.warning("Business name element not found.")

        try:
            address_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//address|//div[contains(@class, 'address')]"))
            )
            self.data['address'] = address_element.text.strip() or "N/A"
        except (TimeoutException, NoSuchElementException):
            logging.warning("Address element not found.")

        try:
            phone_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(@href, 'tel:')]"))
            )
            phone = phone_element.text.strip() or phone_element.get_attribute("href").replace("tel:", "")
            self.data['phone'] = self.validate_phone_number(phone)
        except (TimeoutException, NoSuchElementException):
            logging.warning("Phone element not found.")

        if "google.com/maps" in self.url:
            try:
                website_element = self.driver.find_element(
                    By.XPATH,
                    "//a[contains(@href, 'http') and (contains(text(), 'Website') or contains(@aria-label, 'Website'))]"
                )
                self.data['website_url'] = self.validate_url(website_element.get_attribute("href"))
            except NoSuchElementException:
                self.data['website_url'] = "N/A"
        else:
            self.data['website_url'] = self.validate_url(self.url)

        if "google.com/maps" in self.url and self.data['website_url'] != "N/A":
            try:
                self.driver.get(self.data['website_url'])
                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(3)
                email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                page_source = self.driver.page_source.lower()
                emails = re.findall(email_pattern, page_source)
                if emails:
                    self.data['email'] = self.validate_email_address(emails[0])
                    logging.info(f"Email found: {self.data['email']}")
                else:
                    logging.warning("No email found on business website.")
            except Exception as e:
                logging.error(f"Error extracting email: {e}")
        else:
            try:
                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(3)
                email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                page_source = self.driver.page_source.lower()
                emails = re.findall(email_pattern, page_source)
                if emails:
                    self.data['email'] = self.validate_email_address(emails[0])
                    logging.info(f"Email found: {self.data['email']}")
                else:
                    logging.warning("No email found. Checking mailto links...")
                    try:
                        email_link = WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, "//a[contains(@href, 'mailto:')]"))
                        )
                        email = email_link.get_attribute("href").replace("mailto:", "").strip()
                        if re.match(email_pattern, email):
                            self.data['email'] = self.validate_email_address(email)
                            logging.info(f"Email found in mailto link: {self.data['email']}")
                    except (TimeoutException, NoSuchElementException):
                        logging.warning("No mailto link found.")
            except Exception as e:
                logging.error(f"Error during email extraction: {e}")

        return self.data

    def scrape(self):
        try:
            self.driver = self.setup_driver()
            return self.extract_info()
        except Exception as e:
            logging.error(f"Scraping error: {e}")
            return self.data
        finally:
            if self.driver:
                self.driver.quit()
                logging.info("WebDriver closed.")

    def __del__(self):
        if hasattr(self, 'driver') and self.driver:
            self.driver.quit()
            logging.info("WebDriver closed in destructor.")


class GoogleMapsSearchScraper:
    """
    Scraper for extracting business information from Google Maps search results.
    Handles multiple business listings from a single search URL.
    """
    
    def __init__(self, search_url):
        """
        Initialize the GoogleMapsSearchScraper with a search URL.
        
        Args:
            search_url: Google Maps search results URL
        """
        self.search_url = search_url
        self.driver = None
        logging.info(f"Initialized GoogleMapsSearchScraper with URL: {search_url}")
    
    def setup_driver(self, headless=True):
        """
        Setup Chrome webdriver with appropriate options.
        Reuses the same logic as WebScraper.setup_driver().
        
        Args:
            headless: Whether to run browser in headless mode (default: True)
            
        Returns:
            Configured Chrome WebDriver instance
        """
        logging.info(f"Setting up Chromium webdriver (headless={headless})")

        options = webdriver.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--remote-debugging-port=9222")
        options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36")

        if headless:
            options.add_argument("--headless=new")

        system = platform.system().lower()

        if system == "darwin":  # macOS dev
            # Use Chrome + webdriver_manager
            driver_path = ChromeDriverManager().install()
            service = Service(driver_path)
            driver = webdriver.Chrome(service=service, options=options)
        else:  # Linux (Render)
            chromium_path = os.getenv("CHROMIUM_PATH", "/usr/bin/chromium")
            chromedriver_path = os.getenv("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")
            options.binary_location = chromium_path
            service = Service(executable_path=chromedriver_path)
            driver = webdriver.Chrome(service=service, options=options)

        return driver
    
    def scroll_results_panel(self, max_scrolls=10):
        """
        Scroll the Google Maps results panel to load more listings via lazy-loading.
        Stops when no new results appear after 3 consecutive scrolls.
        
        Args:
            max_scrolls: Maximum number of scroll attempts (default: 10)
            
        Returns:
            None
        """
        logging.info("Starting to scroll Google Maps results panel")
        
        try:
            # Wait for the page to load and locate the scrollable results panel
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(2)  # Allow initial content to load
            
            # Try multiple selectors to find the scrollable results panel
            panel_element = None
            selectors = [
                (By.CSS_SELECTOR, "div[role='feed']"),
                (By.CSS_SELECTOR, "div.m6QErb"),
                (By.CSS_SELECTOR, "div.DxyBCb"),
                (By.XPATH, "//div[contains(@class, 'results')]"),
                (By.XPATH, "//div[contains(@aria-label, 'Results')]")
            ]
            
            for selector_type, selector_value in selectors:
                try:
                    panel_element = self.driver.find_element(selector_type, selector_value)
                    logging.info(f"Found scrollable panel using selector: {selector_value}")
                    break
                except NoSuchElementException:
                    logging.warning(f"Scrollable panel not found with selector: {selector_value}, trying next selector")
                    continue
            
            if not panel_element:
                logging.warning("Could not locate scrollable results panel, proceeding with available results")
                return
            
            # Track business listing count to detect when no new results appear
            consecutive_no_change = 0
            previous_count = 0
            scroll_attempt = 0
            
            while scroll_attempt < max_scrolls:
                # Count current business listings
                try:
                    business_links = self.driver.find_elements(
                        By.XPATH, 
                        "//a[contains(@href, '/maps/place/')]"
                    )
                    current_count = len(business_links)
                    logging.info(f"Scroll attempt {scroll_attempt + 1}/{max_scrolls}: Found {current_count} business listings")
                except NoSuchElementException as e:
                    logging.warning(f"No business listing elements found during count: {e}")
                    current_count = previous_count
                except Exception as e:
                    logging.warning(f"Error counting business listings: {e}")
                    current_count = previous_count
                
                # Check if new results were loaded
                if current_count == previous_count:
                    consecutive_no_change += 1
                    logging.info(f"No new results after scroll (consecutive: {consecutive_no_change}/3)")
                    
                    # Stop if no new results for 3 consecutive scrolls
                    if consecutive_no_change >= 3:
                        logging.info(f"Stopping scroll: No new results after 3 consecutive attempts. Total listings: {previous_count}")
                        break
                else:
                    consecutive_no_change = 0
                    previous_count = current_count
                    logging.info(f"New results loaded: {current_count - (previous_count if scroll_attempt > 0 else 0)} additional listings")
                
                # Scroll the panel to the bottom
                try:
                    self.driver.execute_script(
                        "arguments[0].scrollTop = arguments[0].scrollHeight", 
                        panel_element
                    )
                    logging.info(f"Executed scroll on results panel (attempt {scroll_attempt + 1})")
                except WebDriverException as e:
                    logging.error(f"WebDriver error executing scroll: {e}")
                    break
                except Exception as e:
                    logging.error(f"Unexpected error executing scroll: {e}")
                    break
                
                # Wait for lazy-loading (2-3 seconds)
                time.sleep(2.5)
                
                scroll_attempt += 1
            
            if scroll_attempt >= max_scrolls:
                logging.info(f"Reached maximum scroll limit of {max_scrolls} attempts. Total listings: {previous_count}")
            
            logging.info(f"Scrolling complete. Total business listings found: {previous_count}")
            
        except TimeoutException as e:
            logging.warning(f"Timeout while waiting for results panel to load (timeout after 15s), proceeding with available results: {e}")
        except NoSuchElementException as e:
            logging.warning(f"Required element not found during scrolling, proceeding with available results: {e}")
        except WebDriverException as e:
            logging.error(f"WebDriver error during scrolling: {e}. Proceeding with available results.")
        except Exception as e:
            logging.error(f"Unexpected error during scrolling: {e}. Proceeding with available results.")
    
    def extract_business_urls(self):
        """
        Extract individual business detail page URLs from Google Maps search results.
        Finds all anchor elements with href containing '/maps/place/', deduplicates them,
        and validates URLs before returning.
        
        Returns:
            List of unique business detail page URLs (strings)
        """
        logging.info(f"Extracting business URLs from search results: {self.search_url}")
        
        try:
            # Navigate to the search URL
            logging.info(f"Navigating to Google Maps search URL")
            self.driver.get(self.search_url)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)  # Allow page to fully load
            logging.info("Search results page loaded successfully")
            
            # Scroll to load more results
            self.scroll_results_panel()
            
            # Find all anchor elements with href containing '/maps/place/'
            try:
                business_link_elements = self.driver.find_elements(
                    By.XPATH,
                    "//a[contains(@href, '/maps/place/')]"
                )
                logging.info(f"Found {len(business_link_elements)} business link elements")
            except NoSuchElementException as e:
                logging.warning(f"No business link elements found on page: {e}")
                return []
            
            # Extract URLs and deduplicate
            business_urls = []
            seen_urls = set()
            
            for element in business_link_elements:
                try:
                    href = element.get_attribute("href")
                    
                    # Validate that href exists and contains '/maps/place/'
                    if href and '/maps/place/' in href:
                        # Clean the URL (remove query parameters that might cause duplicates)
                        # Keep the base URL up to the place identifier
                        base_url = href.split('?')[0] if '?' in href else href
                        
                        # Deduplicate using the base URL
                        if base_url not in seen_urls:
                            seen_urls.add(base_url)
                            business_urls.append(href)  # Keep original URL with parameters
                            
                except NoSuchElementException as e:
                    logging.warning(f"Element disappeared while extracting href: {e}")
                    continue
                except Exception as e:
                    logging.warning(f"Error extracting href from element: {e}")
                    continue
            
            # Log results
            unique_count = len(business_urls)
            logging.info(f"Successfully extracted {unique_count} unique business URLs")
            
            # Handle cases where fewer than 10 results are found
            if unique_count < 10:
                logging.warning(f"Found fewer than 10 business URLs ({unique_count} found). This may be expected if the search has limited results.")
            
            # Validate URLs before returning
            validated_urls = []
            url_pattern = r'^https?:\/\/'
            
            for url in business_urls:
                if re.match(url_pattern, url):
                    validated_urls.append(url)
                else:
                    logging.warning(f"Invalid URL format, skipping: {url}")
            
            logging.info(f"Returning {len(validated_urls)} validated business URLs")
            return validated_urls
            
        except TimeoutException as e:
            logging.error(f"Timeout while loading Google Maps search results (timeout after 15s): {e}")
            return []
        except NoSuchElementException as e:
            logging.warning(f"Required element not found while extracting business URLs: {e}")
            return []
        except WebDriverException as e:
            logging.error(f"WebDriver error while extracting business URLs: {e}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error while extracting business URLs: {e}")
            return []
    
    def scrape_all_businesses(self, user_id):
        """
        Coordinate extraction of all businesses from Google Maps search results.
        Extracts business URLs, scrapes each one individually, and handles errors gracefully.
        
        Args:
            user_id: The ID of the user initiating the scraping request
            
        Returns:
            Dictionary with 'results' (list of scraped data dicts) and 
            'errors' (list of error dicts with url and error message)
        """
        from app.models.scraped_data import ScrapedData
        from app import db
        
        logging.info(f"Starting coordinated multi-business scraping for user {user_id} from URL: {self.search_url}")
        
        results = []
        errors = []
        db_records = []  # Track database records for adding IDs after commit
        
        try:
            # Setup driver for extracting business URLs
            try:
                self.driver = self.setup_driver()
                logging.info("WebDriver setup successful for search URL extraction")
            except WebDriverException as e:
                error_msg = f"Failed to setup WebDriver: {str(e)}"
                logging.error(error_msg)
                return {
                    'results': results,
                    'errors': [{'url': self.search_url, 'business_name': 'N/A', 'error': error_msg}]
                }
            
            # Extract all business URLs from search results
            business_urls = self.extract_business_urls()
            
            if not business_urls:
                logging.warning("No business URLs found in search results")
                return {
                    'results': results,
                    'errors': [{'url': self.search_url, 'business_name': 'N/A', 'error': 'No business listings found in search results'}]
                }
            
            logging.info(f"Successfully found {len(business_urls)} businesses to scrape")
            
            # Close the search driver before scraping individual businesses
            if self.driver:
                self.driver.quit()
                self.driver = None
                logging.info("Search driver closed, beginning individual business scraping")
            
            # Iterate through each business URL and scrape individually
            for index, business_url in enumerate(business_urls, start=1):
                logging.info(f"Processing business {index}/{len(business_urls)}: {business_url}")
                
                business_name = 'Unknown'
                
                try:
                    # Instantiate WebScraper for this business
                    scraper = WebScraper(business_url)
                    
                    # Scrape the business data
                    scraped_data = scraper.scrape()
                    business_name = scraped_data.get('company_name', 'Unknown')
                    
                    # Validate that we got meaningful data
                    if scraped_data and scraped_data.get('company_name') != 'N/A':
                        # Create database record
                        new_data = ScrapedData(
                            company_name=scraped_data.get('company_name', 'N/A'),
                            email=scraped_data.get('email', 'N/A'),
                            phone=scraped_data.get('phone', 'N/A'),
                            address=scraped_data.get('address', 'N/A'),
                            website_url=business_url,
                            user_id=user_id
                        )
                        
                        # Add to database session
                        db.session.add(new_data)
                        db_records.append(new_data)
                        
                        # Add to results list (will update with ID after commit)
                        results.append({
                            'company_name': scraped_data.get('company_name', 'N/A'),
                            'email': scraped_data.get('email', 'N/A'),
                            'phone': scraped_data.get('phone', 'N/A'),
                            'address': scraped_data.get('address', 'N/A'),
                            'website_url': business_url
                        })
                        
                        logging.info(f"Successfully scraped business {index}/{len(business_urls)}: {business_name}")
                    else:
                        # No meaningful data extracted
                        error_msg = "No meaningful data extracted from business page"
                        logging.warning(f"Partial failure for business {index}/{len(business_urls)} ({business_url}): {error_msg}")
                        errors.append({
                            'url': business_url,
                            'business_name': business_name,
                            'error': error_msg
                        })
                    
                except TimeoutException as e:
                    error_msg = f"Timeout extracting data: {str(e)}"
                    logging.warning(f"Timeout for business {index}/{len(business_urls)} ({business_url}): {error_msg}")
                    errors.append({
                        'url': business_url,
                        'business_name': business_name,
                        'error': error_msg
                    })
                    
                except NoSuchElementException as e:
                    error_msg = f"Required element not found: {str(e)}"
                    logging.warning(f"Missing element for business {index}/{len(business_urls)} ({business_url}): {error_msg}")
                    errors.append({
                        'url': business_url,
                        'business_name': business_name,
                        'error': error_msg
                    })
                    
                except WebDriverException as e:
                    error_msg = f"WebDriver error: {str(e)}"
                    logging.error(f"WebDriver error for business {index}/{len(business_urls)} ({business_url}): {error_msg}")
                    errors.append({
                        'url': business_url,
                        'business_name': business_name,
                        'error': error_msg
                    })
                    
                except Exception as e:
                    error_msg = f"Unexpected error: {str(e)}"
                    logging.error(f"Unexpected error for business {index}/{len(business_urls)} ({business_url}): {error_msg}")
                    errors.append({
                        'url': business_url,
                        'business_name': business_name,
                        'error': error_msg
                    })
                
                # Add delay between scrapes to avoid detection (1-2 seconds)
                if index < len(business_urls):  # Don't delay after the last one
                    delay = 1.5  # 1.5 seconds between scrapes
                    time.sleep(delay)
            
            # Commit all successful records to database in a single transaction
            if db_records:
                try:
                    db.session.commit()
                    logging.info(f"Successfully committed {len(db_records)} records to database")
                    
                    # Update results with database IDs and created_at timestamps
                    for i, db_record in enumerate(db_records):
                        if i < len(results):
                            results[i]['id'] = db_record.id
                            results[i]['created_at'] = db_record.created_at.strftime('%Y-%m-%d %H:%M:%S')
                            
                except Exception as e:
                    error_msg = f"Database commit failed: {str(e)}"
                    logging.error(f"Database error: {error_msg}")
                    # Rollback the failed transaction
                    try:
                        db.session.rollback()
                        logging.info("Database transaction rolled back successfully")
                    except Exception as rollback_error:
                        logging.error(f"Failed to rollback database transaction: {rollback_error}")
                    
                    # Add error to errors list
                    errors.append({
                        'url': self.search_url,
                        'business_name': 'N/A',
                        'error': error_msg
                    })
                    # Clear results since commit failed
                    results = []
            else:
                logging.warning("No database records to commit")
            
            logging.info(f"Multi-business scraping complete. Successfully scraped: {len(results)}, Failed: {len(errors)}")
            
            return {
                'results': results,
                'errors': errors
            }
            
        except WebDriverException as e:
            error_msg = f"WebDriver error during multi-business scraping: {str(e)}"
            logging.error(error_msg)
            # Ensure session is rolled back on fatal error
            try:
                db.session.rollback()
            except Exception as rollback_error:
                logging.error(f"Failed to rollback database transaction: {rollback_error}")
            
            return {
                'results': results,
                'errors': errors + [{'url': self.search_url, 'business_name': 'N/A', 'error': error_msg}]
            }
        except Exception as e:
            error_msg = f"Fatal error in scrape_all_businesses: {str(e)}"
            logging.error(error_msg)
            # Ensure session is rolled back on fatal error
            try:
                db.session.rollback()
            except Exception as rollback_error:
                logging.error(f"Failed to rollback database transaction: {rollback_error}")
            
            return {
                'results': results,
                'errors': errors + [{'url': self.search_url, 'business_name': 'N/A', 'error': error_msg}]
            }
        finally:
            # Ensure driver is closed
            if self.driver:
                try:
                    self.driver.quit()
                    self.driver = None
                    logging.info("Search driver closed in finally block")
                except Exception as e:
                    logging.error(f"Error closing search driver: {e}")
    
    def __del__(self):
        """Cleanup driver on object destruction"""
        if hasattr(self, 'driver') and self.driver:
            self.driver.quit()
            logging.info("GoogleMapsSearchScraper driver closed in destructor")