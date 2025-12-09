# scraper.py - OPTIMIZED VERSION WITH ALL DEPENDENCIES

import re
import time
import logging
import os
import tempfile
import shutil
import platform

# Import pandas but with lazy loading optimization
_pandas_imported = False
_pandas = None

# Import webdriver_manager but only when needed
_webdriver_manager_imported = False

# Keep email-validator as it's lightweight
from email_validator import validate_email

# Selenium imports (essential)
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.service import Service

# Optimized logging
logging.basicConfig(
    level=logging.INFO if os.getenv('FLASK_ENV') == 'development' else logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def lazy_import_pandas():
    """Lazy import pandas to save memory if not used immediately."""
    global _pandas, _pandas_imported
    
    if not _pandas_imported:
        try:
            import pandas as pd
            _pandas = pd
            _pandas_imported = True
            logging.debug("Pandas imported lazily")
        except ImportError as e:
            logging.warning(f"Pandas not available: {e}")
            _pandas = None
    return _pandas


def lazy_import_webdriver_manager():
    """Lazy import webdriver_manager for macOS development."""
    global _webdriver_manager_imported
    
    if not _webdriver_manager_imported:
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            _webdriver_manager_imported = True
            return ChromeDriverManager
        except ImportError as e:
            logging.warning(f"webdriver_manager not available: {e}")
            return None
    return None


def is_google_maps_search_url(url):
    """
    Detect if URL is a Google Maps search results page.
    """
    if not url or not isinstance(url, str):
        return False
    
    url_lower = url.lower()
    
    if 'google.com/maps/search' in url_lower:
        return True
    
    if 'google.com/maps' in url_lower:
        search_indicators = ['query=', 'q=', 'data=']
        return any(indicator in url_lower for indicator in search_indicators)
    
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
        self.temp_dirs = []  # Track temp directories for cleanup

    def validate_phone_number(self, phone_number):
        """Validate phone number with comprehensive patterns."""
        if phone_number == "N/A":
            return "N/A"
        
        # Enhanced phone pattern matching
        phone_patterns = [
            r'^\+?\d{1,4}?[-.\s]?\(?\d{1,3}?\)?[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}$',  # Standard
            r'^\(\d{3}\)\s?\d{3}[-.\s]?\d{4}$',  # (123) 456-7890
            r'^\d{3}[-.\s]?\d{3}[-.\s]?\d{4}$',  # 123-456-7890
            r'^\+\d{1,3}\s?\d{3}\s?\d{3}\s?\d{4}$',  # International
            r'^\d{10}$',  # 1234567890
        ]
        
        for pattern in phone_patterns:
            if re.match(pattern, phone_number):
                return phone_number
        
        return "N/A"

    def validate_email_address(self, email_address):
        """Validate email using email-validator."""
        try:
            validate_email(email_address, check_deliverability=False)
            return email_address
        except Exception:
            # Fallback to regex if email-validator fails
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            return email_address if re.match(email_pattern, email_address) else "N/A"

    def validate_url(self, url):
        """Validate URL format."""
        if url == "N/A":
            return "N/A"
        
        url_pattern = r'^(https?:\/\/)?([\w\-]+(\.[\w\-]+)+)(\/.*)?$'
        return url if re.match(url_pattern, url, re.IGNORECASE) else "N/A"

    def setup_driver(self, headless=True):
        """Setup Chrome webdriver with minimal disk usage and all fallbacks."""
        logging.info(f"Setting up webdriver (headless={headless})")

        options = webdriver.ChromeOptions()
        
        # Critical for Render/Linux environments
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        # Disk space optimizations
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-logging")
        options.add_argument("--log-level=3")
        options.add_argument("--output=/dev/null")
        options.add_argument("--disable-crash-reporter")
        
        # Cache optimization for disk space
        options.add_argument("--disk-cache-size=1")
        options.add_argument("--media-cache-size=1")
        
        # Create minimal profile directory in temp
        temp_profile_dir = tempfile.mkdtemp(prefix='chrome_profile_')
        self.temp_dirs.append(temp_profile_dir)
        options.add_argument(f"--user-data-dir={temp_profile_dir}")
        
        if headless:
            options.add_argument("--headless=new")
        
        # System detection with multiple fallbacks
        system = platform.system().lower()
        
        try:
            if system == "darwin":  # macOS development
                # Try system Chrome first
                chrome_paths = [
                    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                    "/usr/local/bin/chromium",
                    "/opt/homebrew/bin/chromium"
                ]
                
                chrome_found = False
                for chrome_path in chrome_paths:
                    if os.path.exists(chrome_path):
                        options.binary_location = chrome_path
                        chrome_found = True
                        logging.info(f"Using Chrome at: {chrome_path}")
                        break
                
                if chrome_found:
                    # Try system ChromeDriver
                    chromedriver_paths = [
                        "/usr/local/bin/chromedriver",
                        "/opt/homebrew/bin/chromedriver"
                    ]
                    
                    for chromedriver_path in chromedriver_paths:
                        if os.path.exists(chromedriver_path):
                            service = Service(executable_path=chromedriver_path)
                            driver = webdriver.Chrome(service=service, options=options)
                            logging.info("Using system ChromeDriver on macOS")
                            return driver
                
                # Fallback to webdriver_manager (lazy import)
                logging.info("Falling back to webdriver_manager on macOS")
                ChromeDriverManager = lazy_import_webdriver_manager()
                if ChromeDriverManager:
                    driver_path = ChromeDriverManager().install()
                    service = Service(driver_path)
                    driver = webdriver.Chrome(service=service, options=options)
                    return driver
                else:
                    raise Exception("webdriver_manager not available on macOS")
                    
            else:  # Linux (Render/Production)
                # Use system Chrome/Chromium
                chrome_paths = [
                    "/usr/bin/google-chrome",
                    "/usr/bin/google-chrome-stable",
                    "/usr/bin/chromium-browser",
                    "/usr/bin/chromium",
                    os.getenv("CHROMIUM_PATH", "/usr/bin/chromium")
                ]
                
                chrome_found = False
                for chrome_path in chrome_paths:
                    if os.path.exists(chrome_path):
                        options.binary_location = chrome_path
                        chrome_found = True
                        logging.info(f"Using Chrome/Chromium at: {chrome_path}")
                        break
                
                if not chrome_found:
                    raise Exception("Chrome/Chromium not found on system")
                
                # Use system ChromeDriver
                chromedriver_paths = [
                    "/usr/bin/chromedriver",
                    "/usr/local/bin/chromedriver",
                    os.getenv("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")
                ]
                
                for chromedriver_path in chromedriver_paths:
                    if os.path.exists(chromedriver_path):
                        service = Service(executable_path=chromedriver_path)
                        driver = webdriver.Chrome(service=service, options=options)
                        logging.info(f"Using ChromeDriver at: {chromedriver_path}")
                        return driver
                
                # Last resort: Try webdriver_manager on Linux too
                logging.warning("System ChromeDriver not found, trying webdriver_manager")
                ChromeDriverManager = lazy_import_webdriver_manager()
                if ChromeDriverManager:
                    driver_path = ChromeDriverManager().install()
                    service = Service(driver_path)
                    driver = webdriver.Chrome(service=service, options=options)
                    return driver
                
                raise Exception("No ChromeDriver found and webdriver_manager not available")
                
        except Exception as e:
            logging.error(f"Failed to setup driver: {e}")
            raise

    def cleanup_temp_files(self):
        """Clean up temporary files created by Chrome."""
        for temp_dir in self.temp_dirs:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logging.debug(f"Error cleaning temp dir {temp_dir}: {e}")
        self.temp_dirs = []

    def extract_info(self):
        """Extract business information with minimal page interactions."""
        logging.info(f"Extracting info from: {self.url}")
        
        try:
            self.driver.get(self.url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(2)
        except TimeoutException:
            logging.warning(f"Timeout navigating to {self.url}")
            return self.data
        except WebDriverException as e:
            logging.error(f"WebDriver error navigating to {self.url}: {e}")
            return self.data

        # Extract business name
        try:
            if "google.com/maps" in self.url:
                name_selectors = [
                    "//h1[contains(@class, 'DUwDvf')]",
                    "//h1[@class='fontHeadlineLarge']",
                    "//h1",
                    "//div[@role='main']//h1"
                ]
                for selector in name_selectors:
                    try:
                        name_element = self.driver.find_element(By.XPATH, selector)
                        name = name_element.text.strip()
                        if name:
                            self.data['company_name'] = name
                            logging.info(f"Found business name: {name}")
                            break
                    except NoSuchElementException:
                        continue
            else:
                name_element = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//h1"))
                )
                self.data['company_name'] = name_element.text.strip() or "N/A"
        except (TimeoutException, NoSuchElementException):
            logging.debug("Business name element not found")

        # Extract address
        try:
            if "google.com/maps" in self.url:
                address_selectors = [
                    "//button[@data-item-id='address']//div[contains(@class, 'fontBodyMedium')]",
                    "//div[@data-tooltip='Copy address']",
                    "//button[contains(@aria-label, 'Address')]//div",
                ]
                for selector in address_selectors:
                    try:
                        address_element = self.driver.find_element(By.XPATH, selector)
                        address = address_element.text.strip()
                        if address and len(address) > 5:
                            self.data['address'] = address
                            logging.info(f"Found address: {address}")
                            break
                    except NoSuchElementException:
                        continue
            else:
                address_selectors = [
                    "//address",
                    "//div[contains(@class, 'address')]",
                    "//span[contains(@class, 'address')]"
                ]
                for selector in address_selectors:
                    try:
                        address_element = self.driver.find_element(By.XPATH, selector)
                        self.data['address'] = address_element.text.strip() or "N/A"
                        break
                    except NoSuchElementException:
                        continue
        except Exception:
            logging.debug("Address element not found")

        # Extract phone
        try:
            if "google.com/maps" in self.url:
                phone_selectors = [
                    "//button[@data-item-id='phone:tel:']//div[contains(@class, 'fontBodyMedium')]",
                    "//a[contains(@href, 'tel:')]",
                    "//button[contains(@data-tooltip, 'Copy phone number')]"
                ]
                for selector in phone_selectors:
                    try:
                        phone_element = self.driver.find_element(By.XPATH, selector)
                        phone = phone_element.text.strip()
                        if not phone:
                            href = phone_element.get_attribute("href", "")
                            phone = href.replace("tel:", "") if href else ""
                        
                        if phone:
                            self.data['phone'] = self.validate_phone_number(phone)
                            logging.info(f"Found phone: {phone}")
                            break
                    except NoSuchElementException:
                        continue
            else:
                phone_selectors = [
                    "//a[contains(@href, 'tel:')]",
                    "//span[contains(@class, 'phone')]",
                    "//div[contains(@class, 'phone')]"
                ]
                for selector in phone_selectors:
                    try:
                        phone_element = self.driver.find_element(By.XPATH, selector)
                        phone = phone_element.text.strip() or phone_element.get_attribute("href", "").replace("tel:", "")
                        if phone:
                            self.data['phone'] = self.validate_phone_number(phone)
                            break
                    except NoSuchElementException:
                        continue
        except Exception:
            logging.debug("Phone element not found")

        # Extract website URL
        if "google.com/maps" in self.url:
            try:
                website_selectors = [
                    "//a[contains(@href, 'http') and contains(@aria-label, 'Website')]",
                    "//a[@data-tooltip='Open website']",
                    "//a[contains(@data-item-id, 'authority') and contains(@href, 'http')]"
                ]
                for selector in website_selectors:
                    try:
                        website_element = self.driver.find_element(By.XPATH, selector)
                        href = website_element.get_attribute("href")
                        if href and 'google.com' not in href and 'goo.gl' not in href:
                            self.data['website_url'] = self.validate_url(href)
                            logging.info(f"Found website URL: {href}")
                            break
                    except NoSuchElementException:
                        continue
            except Exception as e:
                logging.debug(f"Error finding website URL: {e}")
        
        # Extract email
        try:
            # First try from current page
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            page_source = self.driver.page_source
            emails = re.findall(email_pattern, page_source)
            
            if emails:
                # Filter out common non-business emails
                valid_emails = []
                for email in emails:
                    email_lower = email.lower()
                    excluded = ['noreply', 'no-reply', 'example.com', 'test.com']
                    if not any(excl in email_lower for excl in excluded):
                        valid_emails.append(email)
                
                if valid_emails:
                    self.data['email'] = self.validate_email_address(valid_emails[0])
                    logging.info(f"Found email: {self.data['email']}")
            else:
                # Check mailto links
                try:
                    mailto_links = self.driver.find_elements(By.XPATH, "//a[contains(@href, 'mailto:')]")
                    for link in mailto_links:
                        email = link.get_attribute("href").replace("mailto:", "").strip()
                        if re.match(email_pattern, email):
                            self.data['email'] = self.validate_email_address(email)
                            break
                except:
                    pass
                    
        except Exception as e:
            logging.debug(f"Error during email extraction: {e}")

        return self.data

    def scrape(self):
        """Main scraping method with proper cleanup."""
        try:
            self.driver = self.setup_driver()
            return self.extract_info()
        except Exception as e:
            logging.error(f"Scraping error: {e}")
            return self.data
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up all resources."""
        self.cleanup_temp_files()
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

    def __del__(self):
        """Destructor for cleanup."""
        self.cleanup()

    # Optional: Add pandas functionality if needed
    def to_dataframe(self):
        """Convert scraped data to pandas DataFrame if pandas is available."""
        pd = lazy_import_pandas()
        if pd is not None:
            return pd.DataFrame([self.data])
        else:
            logging.warning("Pandas not available for DataFrame conversion")
            return None


class GoogleMapsSearchScraper:
    """Optimized scraper for Google Maps search results."""
    
    def __init__(self, search_url):
        self.search_url = search_url
        self.driver = None
        self.temp_dirs = []
        logging.info(f"Initialized GoogleMapsSearchScraper for: {search_url}")
    
    def setup_driver(self, headless=True):
        """Reuse WebScraper's optimized setup_driver."""
        scraper = WebScraper(self.search_url)
        return scraper.setup_driver(headless)
    
    def cleanup_temp_files(self):
        """Clean up temporary files."""
        for temp_dir in self.temp_dirs:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logging.debug(f"Error cleaning temp dir {temp_dir}: {e}")
        self.temp_dirs = []
    
    def scroll_results_panel(self, max_scrolls=5):
        """Scroll to load more results with minimal delays."""
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1)
            
            # Find scrollable panel
            panel_selectors = [
                (By.CSS_SELECTOR, "div[role='feed']"),
                (By.CSS_SELECTOR, "div.m6QErb"),
                (By.XPATH, "//div[contains(@class, 'm6QErb')]")
            ]
            
            panel_element = None
            for selector_type, selector_value in panel_selectors:
                try:
                    panel_element = self.driver.find_element(selector_type, selector_value)
                    break
                except NoSuchElementException:
                    continue
            
            if not panel_element:
                return
            
            # Scroll with minimal attempts
            consecutive_no_change = 0
            previous_count = 0
            
            for scroll_attempt in range(max_scrolls):
                try:
                    business_links = self.driver.find_elements(
                        By.XPATH, "//a[contains(@href, '/maps/place/')]"
                    )
                    current_count = len(business_links)
                    logging.debug(f"Scroll {scroll_attempt + 1}: {current_count} businesses")
                except:
                    current_count = previous_count
                
                if current_count == previous_count:
                    consecutive_no_change += 1
                    if consecutive_no_change >= 2:
                        break
                else:
                    consecutive_no_change = 0
                    previous_count = current_count
                
                # Scroll
                try:
                    self.driver.execute_script(
                        "arguments[0].scrollTop = arguments[0].scrollHeight", 
                        panel_element
                    )
                    time.sleep(1.5)
                except:
                    break
            
        except Exception as e:
            logging.debug(f"Scrolling error: {e}")
    
    def extract_businesses_with_names(self, limit=20):
        """Extract business info with limit to control memory usage."""
        logging.info(f"Extracting businesses from search results (limit: {limit})")
        
        try:
            self.driver.get(self.search_url)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)
            
            # Scroll to load more results
            self.scroll_results_panel()
            
            # Find business listings
            business_containers = []
            container_selectors = [
                "//div[contains(@class, 'Nv2PK')]",
                "//div[@role='article']",
                "//a[contains(@href, '/maps/place/')]/.."
            ]
            
            for selector in container_selectors:
                try:
                    containers = self.driver.find_elements(By.XPATH, selector)
                    if containers:
                        business_containers = containers
                        break
                except:
                    continue
            
            # Extract businesses
            businesses = []
            seen_urls = set()
            
            for container in business_containers[:limit]:  # Limit upfront
                try:
                    # Find link
                    link_element = None
                    try:
                        link_element = container.find_element(By.XPATH, ".//a[contains(@href, '/maps/place/')]")
                    except:
                        continue
                    
                    href = link_element.get_attribute("href")
                    if not href or '/maps/place/' not in href:
                        continue
                    
                    # Deduplicate
                    base_url = href.split('?')[0] if '?' in href else href
                    if base_url in seen_urls:
                        continue
                    seen_urls.add(base_url)
                    
                    # Extract name
                    business_name = "Unknown Business"
                    try:
                        name_elements = container.find_elements(By.XPATH, ".//div[contains(@class, 'fontHeadlineSmall')]")
                        if name_elements:
                            business_name = name_elements[0].text.strip()
                        else:
                            business_name = link_element.text.strip() or "Unknown Business"
                    except:
                        pass
                    
                    businesses.append({
                        'name': business_name,
                        'url': href
                    })
                    
                except Exception as e:
                    logging.debug(f"Error processing business container: {e}")
                    continue
            
            logging.info(f"Found {len(businesses)} businesses")
            return businesses
            
        except TimeoutException:
            logging.error("Timeout loading search results")
            return []
        except Exception as e:
            logging.error(f"Error extracting businesses: {e}")
            return []
    
    def extract_business_urls(self, limit=20):
        """Extract business URLs with limit."""
        businesses = self.extract_businesses_with_names(limit)
        return [business['url'] for business in businesses]
    
    def scrape_all_businesses(self, user_id, limit=10):
        """
        Scrape businesses with controlled memory usage.
        
        Args:
            user_id: User ID for tracking
            limit: Maximum number of businesses to scrape (default: 10)
        """
        try:
            from app.models.scraped_data import ScrapedData
        except ImportError:
            logging.error("MongoDB model not found")
            return {
                'results': [],
                'errors': [{'url': self.search_url, 'business_name': 'N/A', 'error': 'Database model not available'}]
            }
        
        logging.info(f"Starting multi-business scraping (limit: {limit})")
        
        results = []
        errors = []
        
        try:
            # Setup driver
            self.driver = self.setup_driver()
            
            # Extract business URLs with limit
            business_urls = self.extract_business_urls(limit)
            
            if not business_urls:
                logging.warning("No business URLs found")
                return {
                    'results': results,
                    'errors': [{'url': self.search_url, 'business_name': 'N/A', 'error': 'No business listings found'}]
                }
            
            logging.info(f"Found {len(business_urls)} businesses to scrape")
            
            # Clean up search driver
            self.cleanup()
            
            # Scrape each business
            for index, business_url in enumerate(business_urls, start=1):
                business_name = 'Unknown'
                
                try:
                    # Create and use scraper
                    scraper = WebScraper(business_url)
                    scraped_data = scraper.scrape()
                    business_name = scraped_data.get('company_name', 'Unknown')
                    
                    # Save to MongoDB
                    if scraped_data.get('company_name') != 'N/A':
                        mongo_data = {
                            'company_name': scraped_data.get('company_name', 'N/A'),
                            'email': scraped_data.get('email', 'N/A'),
                            'phone': scraped_data.get('phone', 'N/A'),
                            'address': scraped_data.get('address', 'N/A'),
                            'website_url': business_url,
                            'user_id': user_id
                        }
                        
                        try:
                            document_id = ScrapedData.create(mongo_data)
                            results.append({
                                'id': str(document_id),
                                'company_name': scraped_data.get('company_name', 'N/A'),
                                'email': scraped_data.get('email', 'N/A'),
                                'phone': scraped_data.get('phone', 'N/A'),
                                'address': scraped_data.get('address', 'N/A'),
                                'website_url': business_url,
                                'created_at': 'N/A'  # Will be set by MongoDB
                            })
                            logging.info(f"Successfully saved business {index}/{len(business_urls)}")
                        except Exception as db_error:
                            logging.error(f"MongoDB error for business {index}: {str(db_error)}")
                            errors.append({
                                'url': business_url,
                                'business_name': business_name,
                                'error': f"MongoDB error: {str(db_error)}"
                            })
                    else:
                        errors.append({
                            'url': business_url,
                            'business_name': business_name,
                            'error': 'No meaningful data extracted'
                        })
                    
                except Exception as e:
                    logging.error(f"Error scraping business {index}: {str(e)}")
                    errors.append({
                        'url': business_url,
                        'business_name': business_name,
                        'error': str(e)
                    })
                
                # Small delay between scrapes
                if index < len(business_urls):
                    time.sleep(1)
            
            logging.info(f"Scraping complete: {len(results)} successful, {len(errors)} failed")
            
            return {
                'results': results,
                'errors': errors
            }
            
        except Exception as e:
            logging.error(f"Fatal error in scrape_all_businesses: {e}")
            return {
                'results': results,
                'errors': errors + [{'url': self.search_url, 'business_name': 'N/A', 'error': str(e)}]
            }
        finally:
            self.cleanup()
    
    def scrape_all_businesses_stream(self, user_id, limit=10):
        """Streaming version with controlled memory usage."""
        try:
            from app.models.scraped_data import ScrapedData
        except ImportError:
            yield {'type': 'error', 'error': 'Database model not available'}
            return
        
        logging.info(f"Starting streaming scrape (limit: {limit})")
        
        try:
            # Setup driver
            self.driver = self.setup_driver()
            yield {'type': 'status', 'message': 'Extracting business URLs...'}
            
            # Extract URLs with limit
            business_urls = self.extract_business_urls(limit)
            
            if not business_urls:
                yield {'type': 'error', 'error': 'No business listings found'}
                return
            
            total_count = len(business_urls)
            yield {'type': 'status', 'message': f'Found {total_count} businesses', 'total': total_count}
            
            # Clean up
            self.cleanup()
            
            # Scrape each business
            for index, business_url in enumerate(business_urls, start=1):
                business_name = 'Unknown'
                
                try:
                    scraper = WebScraper(business_url)
                    scraped_data = scraper.scrape()
                    business_name = scraped_data.get('company_name', 'Unknown')
                    
                    if scraped_data.get('company_name') != 'N/A':
                        # Save to MongoDB
                        mongo_data = {
                            'company_name': scraped_data.get('company_name', 'N/A'),
                            'email': scraped_data.get('email', 'N/A'),
                            'phone': scraped_data.get('phone', 'N/A'),
                            'address': scraped_data.get('address', 'N/A'),
                            'website_url': business_url,
                            'user_id': user_id
                        }
                        
                        try:
                            document_id = ScrapedData.create(mongo_data)
                            yield {
                                'type': 'result',
                                'data': {
                                    'id': str(document_id),
                                    'company_name': scraped_data.get('company_name', 'N/A'),
                                    'email': scraped_data.get('email', 'N/A'),
                                    'phone': scraped_data.get('phone', 'N/A'),
                                    'address': scraped_data.get('address', 'N/A'),
                                    'website_url': business_url
                                },
                                'progress': {
                                    'current': index,
                                    'total': total_count
                                }
                            }
                        except Exception as db_error:
                            yield {
                                'type': 'error',
                                'error': f"MongoDB error: {str(db_error)}",
                                'business_name': business_name,
                                'url': business_url
                            }
                    else:
                        yield {
                            'type': 'error',
                            'error': 'No meaningful data extracted',
                            'business_name': business_name,
                            'url': business_url
                        }
                        
                except Exception as e:
                    yield {
                        'type': 'error',
                        'error': str(e),
                        'business_name': business_name,
                        'url': business_url
                    }
                
                if index < total_count:
                    time.sleep(1)
                    
        except Exception as e:
            logging.error(f"Streaming error: {e}")
            yield {'type': 'error', 'error': str(e)}
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up all resources."""
        self.cleanup_temp_files()
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
    
    def __del__(self):
        """Destructor."""
        self.cleanup()
    
    # Optional: Add pandas export functionality
    def export_to_csv(self, data, filename):
        """Export data to CSV using pandas if available."""
        pd = lazy_import_pandas()
        if pd is not None:
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False)
            logging.info(f"Data exported to {filename}")
            return True
        else:
            logging.warning("Pandas not available for CSV export")
            return False