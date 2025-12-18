

import re
import time
import logging
import os
import tempfile
import shutil
import platform
import traceback
from datetime import datetime
from bson import ObjectId

# Import dependencies
from email_validator import validate_email
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.service import Service

# Lazy imports for optional dependencies
_pandas_imported = False
_pandas = None
_webdriver_manager_imported = False

# Enhanced logging
logging.basicConfig(
    level=logging.DEBUG if os.getenv('FLASK_ENV') == 'development' else logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def is_google_maps_search_url(url):
    """Detect if URL is a Google Maps search results page."""
    if not url or not isinstance(url, str):
        return False
    
    url_lower = url.lower()
    
    # Direct search URLs
    if 'google.com/maps/search' in url_lower:
        return True
    
    # General Google Maps URLs with search indicators
    if 'google.com/maps' in url_lower:
        search_indicators = ['query=', 'q=', 'data=', 'search/', '/search']
        return any(indicator in url_lower for indicator in search_indicators)
    
    # Alternative Google Maps domains
    if any(domain in url_lower for domain in ['maps.google.com', 'maps.app.goo.gl']):
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
            'website_url': self.validate_url(url),
            'scraped_at': datetime.utcnow()
        }
        self.temp_dirs = []

    def validate_phone_number(self, phone_number):
        if phone_number == "N/A":
            return "N/A"
        
        phone_pattern = r'^\+?\d{1,4}?[-.\s]?\(?\d{1,3}?\)?[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}$'
        return phone_number if re.match(phone_pattern, phone_number) else "N/A"

    def validate_email_address(self, email_address):
        try:
            validate_email(email_address, check_deliverability=False)
            return email_address
        except Exception:
            return "N/A"

    def validate_url(self, url):
        if url == "N/A":
            return "N/A"
        
        url_pattern = r'^(https?:\/\/)?([\w\-]+(\.[\w\-]+)+)(\/.*)?$'
        return url if re.match(url_pattern, url, re.IGNORECASE) else "N/A"

    def setup_driver(self, headless=True):
        logging.info(f"Setting up webdriver (headless={headless})")

        options = webdriver.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        # options.add_argument("--remote-debugging-port=9222") # Removed to prevent port conflicts
        options.add_argument("--disable-extensions")
        options.add_argument("--window-size=1920,1080")
        
        if headless:
            options.add_argument("--headless=new")

        system = platform.system().lower()

        if system == "darwin":  # macOS dev
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                driver_path = ChromeDriverManager().install()
                service = Service(driver_path)
                driver = webdriver.Chrome(service=service, options=options)
                logging.info("Using webdriver_manager on macOS")
            except ImportError:
                logging.error("webdriver_manager not available on macOS")
                raise
        else:  # Linux (Render)
            chromium_path = os.getenv("CHROMIUM_PATH", "/usr/bin/chromium")
            chromedriver_path = os.getenv("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")
            options.binary_location = chromium_path
            service = Service(executable_path=chromedriver_path)
            driver = webdriver.Chrome(service=service, options=options)
            logging.info(f"Using system Chrome at {chromium_path}")

        return driver

    def extract_info(self):
        logging.info(f"Extracting info from: {self.url}")
        
        try:
            self.driver.get(self.url)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)
        except TimeoutException:
            logging.warning(f"Timeout navigating to {self.url}")
            return self.data
        except WebDriverException as e:
            logging.error(f"WebDriver error: {e}")
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
                        element = self.driver.find_element(By.XPATH, selector)
                        name = element.text.strip()
                        if name:
                            self.data['company_name'] = name
                            break
                    except NoSuchElementException:
                        continue
            else:
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//h1|//h2"))
                )
                self.data['company_name'] = element.text.strip() or "N/A"
        except Exception:
            logging.debug("Business name not found")

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
                        element = self.driver.find_element(By.XPATH, selector)
                        address = element.text.strip()
                        if address and len(address) > 5:
                            self.data['address'] = address
                            break
                    except NoSuchElementException:
                        continue
            else:
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//address|//div[contains(@class, 'address')]"))
                )
                self.data['address'] = element.text.strip() or "N/A"
        except Exception:
            logging.debug("Address not found")

        # Extract phone
        try:
            if "google.com/maps" in self.url:
                phone_selectors = [
                    "//button[@data-item-id='phone:tel:']//div[contains(@class, 'fontBodyMedium')]",
                    "//button[contains(@aria-label, 'Phone')]//div[contains(@class, 'fontBodyMedium')]",
                    "//a[contains(@href, 'tel:')]",
                ]
                for selector in phone_selectors:
                    try:
                        element = self.driver.find_element(By.XPATH, selector)
                        phone = element.text.strip() or element.get_attribute("href", "").replace("tel:", "")
                        if phone:
                            self.data['phone'] = self.validate_phone_number(phone)
                            break
                    except NoSuchElementException:
                        continue
            else:
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//a[contains(@href, 'tel:')]"))
                )
                phone = element.text.strip() or element.get_attribute("href").replace("tel:", "")
                self.data['phone'] = self.validate_phone_number(phone)
        except Exception:
            logging.debug("Phone not found")

        # Extract website
        if "google.com/maps" in self.url:
            try:
                website_selectors = [
                    "//a[contains(@href, 'http') and contains(@aria-label, 'Website')]",
                    "//a[contains(@data-item-id, 'authority') and contains(@href, 'http')]",
                    "//a[@data-tooltip='Open website']",
                ]
                
                for selector in website_selectors:
                    try:
                        element = self.driver.find_element(By.XPATH, selector)
                        href = element.get_attribute("href")
                        if href and 'google.com' not in href and 'goo.gl' not in href:
                            self.data['website_url'] = self.validate_url(href)
                            break
                    except NoSuchElementException:
                        continue
            except Exception as e:
                logging.debug(f"Website not found: {e}")
        else:
            self.data['website_url'] = self.validate_url(self.url)

        # Extract email
        try:
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            page_source = self.driver.page_source.lower()
            emails = re.findall(email_pattern, page_source)
            
            if emails:
                self.data['email'] = self.validate_email_address(emails[0])
            else:
                try:
                    email_link = self.driver.find_element(By.XPATH, "//a[contains(@href, 'mailto:')]")
                    email = email_link.get_attribute("href").replace("mailto:", "").strip()
                    if re.match(email_pattern, email):
                        self.data['email'] = self.validate_email_address(email)
                except NoSuchElementException:
                    pass
        except Exception as e:
            logging.debug(f"Email extraction error: {e}")

        return self.data

    def scrape(self):
        try:
            self.driver = self.setup_driver()
            return self.extract_info()
        except Exception as e:
            logging.error(f"Scraping error: {e}")
            logging.error(traceback.format_exc())
            return self.data
        finally:
            if self.driver:
                self.driver.quit()

    def __del__(self):
        if hasattr(self, 'driver') and self.driver:
            try:
                self.driver.quit()
            except:
                pass


class GoogleMapsSearchScraper:
    def __init__(self, search_url):
        self.search_url = search_url
        self.driver = None
        logging.info(f"Initialized GoogleMapsSearchScraper for: {search_url}")
    
    def setup_driver(self, headless=True):
        scraper = WebScraper(self.search_url)
        return scraper.setup_driver(headless)
    
    def scroll_results_panel(self, max_scrolls=5):
        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(2)
            
            panel_selectors = [
                (By.CSS_SELECTOR, "div[role='feed']"),
                (By.CSS_SELECTOR, "div.m6QErb"),
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
            
            consecutive_no_change = 0
            previous_count = 0
            
            for scroll_attempt in range(max_scrolls):
                try:
                    business_links = self.driver.find_elements(
                        By.XPATH, "//a[contains(@href, '/maps/place/')]"
                    )
                    current_count = len(business_links)
                except:
                    current_count = previous_count
                
                if current_count == previous_count:
                    consecutive_no_change += 1
                    if consecutive_no_change >= 2:
                        break
                else:
                    consecutive_no_change = 0
                    previous_count = current_count
                
                try:
                    self.driver.execute_script(
                        "arguments[0].scrollTop = arguments[0].scrollHeight", 
                        panel_element
                    )
                    time.sleep(2)
                except:
                    break
                    
        except Exception as e:
            logging.debug(f"Scrolling error: {e}")
    
    def extract_businesses_with_names(self, limit=20):
        logging.info(f"Extracting businesses (limit: {limit})")
        
        try:
            self.driver.get(self.search_url)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(5)  # Increased wait time for Google Maps to load
            
            logging.info("Page loaded, starting business extraction...")
            
            # Try to scroll and load more results
            self.scroll_results_panel()
            
            businesses = []
            seen_urls = set()
            
            # Try multiple selectors for business links (Google Maps changes frequently)
            link_selectors = [
                "//a[contains(@href, '/maps/place/')]",
                "//a[contains(@href, 'place/')]",
                "//div[@role='feed']//a[contains(@href, 'maps')]",
                "//div[contains(@class, 'm6QErb')]//a",
                "//div[contains(@class, 'Nv2PK')]//a",
                "//div[contains(@jsaction, 'click')]//a[contains(@href, 'maps')]"
            ]
            
            business_links = []
            for selector in link_selectors:
                try:
                    links = self.driver.find_elements(By.XPATH, selector)
                    logging.info(f"Selector '{selector}' found {len(links)} links")
                    if links:
                        business_links = links[:limit]
                        break
                except Exception as e:
                    logging.debug(f"Selector '{selector}' failed: {e}")
                    continue
            
            if not business_links:
                logging.warning("No business links found with any selector")
                # Try to get page source for debugging
                page_source = self.driver.page_source
                logging.debug(f"Page source length: {len(page_source)}")
                
                # Check for common Google Maps indicators
                if "maps" in page_source.lower():
                    logging.info("Page contains 'maps' - likely on Google Maps")
                if "place/" in page_source:
                    logging.info("Page contains 'place/' - business links may be present")
                
                return []
            
            logging.info(f"Processing {len(business_links)} business links...")
            
            for i, link in enumerate(business_links):
                try:
                    href = link.get_attribute("href")
                    if not href:
                        logging.debug(f"Link {i+1}: No href attribute")
                        continue
                    
                    # More flexible URL checking
                    if not any(pattern in href for pattern in ['/maps/place/', 'place/', 'maps']):
                        logging.debug(f"Link {i+1}: Not a maps place URL: {href[:50]}...")
                        continue
                    
                    base_url = href.split('?')[0] if '?' in href else href
                    if base_url in seen_urls:
                        logging.debug(f"Link {i+1}: Duplicate URL")
                        continue
                    seen_urls.add(base_url)
                    
                    # Try multiple methods to get business name
                    business_name = "Unknown Business"
                    
                    # Method 1: Look for name in nearby elements
                    try:
                        # Try different name selectors
                        name_selectors = [
                            ".//div[contains(@class, 'fontHeadlineSmall')]",
                            ".//div[contains(@class, 'fontHeadlineLarge')]", 
                            ".//span[contains(@class, 'fontHeadlineSmall')]",
                            ".//div[contains(@class, 'qBF1Pd')]",
                            ".//div[contains(@class, 'NrDZNb')]",
                            ".//h3",
                            ".//h2",
                            ".//span[@class='OSrXXb']"
                        ]
                        
                        parent = link.find_element(By.XPATH, "./..")
                        for selector in name_selectors:
                            try:
                                name_elements = parent.find_elements(By.XPATH, selector)
                                if name_elements:
                                    name_text = name_elements[0].text.strip()
                                    if name_text and len(name_text) > 0:
                                        business_name = name_text
                                        break
                            except:
                                continue
                        
                        # Method 2: Try link text itself
                        if business_name == "Unknown Business":
                            link_text = link.text.strip()
                            if link_text and len(link_text) > 0:
                                business_name = link_text
                        
                        # Method 3: Try aria-label
                        if business_name == "Unknown Business":
                            aria_label = link.get_attribute("aria-label")
                            if aria_label and len(aria_label) > 0:
                                business_name = aria_label
                                
                    except Exception as name_error:
                        logging.debug(f"Error extracting name for link {i+1}: {name_error}")
                    
                    logging.info(f"Found business {len(businesses)+1}: {business_name}")
                    
                    businesses.append({
                        'name': business_name,
                        'url': href
                    })
                    
                    if len(businesses) >= limit:
                        break
                    
                except Exception as e:
                    logging.debug(f"Error processing link {i+1}: {e}")
                    continue
            
            logging.info(f"Successfully extracted {len(businesses)} businesses")
            return businesses
            
        except Exception as e:
            logging.error(f"Error extracting businesses: {e}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            return []
    
    def extract_business_urls(self, limit=20):
        businesses = self.extract_businesses_with_names(limit)
        return [business['url'] for business in businesses]
    
    def scrape_all_businesses(self, user_id, limit=10):
        """
        Main scraping function with MongoDB debugging.
        
        Args:
            user_id: User ID for tracking
            limit: Maximum number of businesses to scrape
            
        Returns:
            Dictionary with results and errors
        """
        logging.info(f"Starting scrape_all_businesses for user {user_id}, limit: {limit}")
        
        results = []
        errors = []
        
        try:
            # 1. Setup driver and extract URLs
            self.driver = self.setup_driver()
            business_urls = self.extract_business_urls(limit)
            
            logging.info(f"Found {len(business_urls)} business URLs")
            
            if not business_urls:
                return {
                    'results': results,
                    'errors': [{'url': self.search_url, 'error': 'No businesses found'}]
                }
            
            # 2. Close search driver
            if self.driver:
                self.driver.quit()
                self.driver = None
            
            # 3. Scrape each business
            for index, business_url in enumerate(business_urls, start=1):
                business_name = 'Unknown'
                
                try:
                    logging.info(f"Scraping business {index}/{len(business_urls)}: {business_url}")
                    
                    # Scrape the business
                    scraper = WebScraper(business_url)
                    scraped_data = scraper.scrape()
                    business_name = scraped_data.get('company_name', 'Unknown')
                    
                    # DEBUG: Print scraped data
                    logging.info(f"Scraped data for {business_name}: {scraped_data}")
                    
                    # --- DEEP SCRAPING START ---
                    # If we found a website URL that is NOT the source Google Maps URL, visit it to get the email!
                    website_url = scraped_data.get('website_url')
                    if website_url and website_url != 'N/A' and website_url != business_url:
                        if 'google.com' not in website_url: # Extra safety check
                            logging.info(f"Deep scraping: Visiting {website_url} for email...")
                            try:
                                # We can reuse the existing driver or let extract_email create one
                                # Since we are in a loop, let's reuse to save time if possible, 
                                # but extract_email_from_website handles its own driver if none provided.
                                # Let's provide our current driver if it's alive, or let it make one.
                                # actually self.driver is closed above at step 2. We need to open a new one or let the function do it.
                                # extract_email_from_website creates a new driver if none is passed.
                                email = self.extract_email_from_website(website_url)
                                if email:
                                    scraped_data['email'] = email
                                    logging.info(f"Deep scraping success! Found email: {email}")
                            except Exception as deep_err:
                                logging.warning(f"Deep scraping failed for {website_url}: {deep_err}")
                    # --- DEEP SCRAPING END ---
                    
                    # Only return data if we have meaningful data (don't save here - let route handle saving)
                    if scraped_data.get('company_name') != 'N/A':
                        logging.info(f"Successfully scraped data for {business_name}")
                        
                        results.append({
                            'company_name': scraped_data.get('company_name', 'N/A'),
                            'email': scraped_data.get('email', 'N/A'),
                            'phone': scraped_data.get('phone', 'N/A'),
                            'address': scraped_data.get('address', 'N/A'),
                            'website_url': scraped_data.get('website_url', business_url),
                            'scraped_at': datetime.utcnow().isoformat(),
                            'source_url': self.search_url
                        })
                    else:
                        errors.append({
                            'url': business_url,
                            'business_name': business_name,
                            'error': 'No meaningful data extracted'
                        })
                        
                except Exception as e:
                    logging.error(f"Error scraping {business_url}: {e}")
                    logging.error(traceback.format_exc())
                    errors.append({
                        'url': business_url,
                        'business_name': business_name,
                        'error': str(e)
                    })
                
                # Delay between scrapes
                if index < len(business_urls):
                    time.sleep(1.5)
            
            logging.info(f"Scraping complete. Results: {len(results)}, Errors: {len(errors)}")
            
            return {
                'results': results,
                'errors': errors
            }
            
        except Exception as e:
            logging.error(f"Fatal error in scrape_all_businesses: {e}")
            logging.error(traceback.format_exc())
            return {
                'results': results,
                'errors': errors + [{'url': self.search_url, 'error': str(e)}]
            }
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
    
    def test_scrape_single(self, business_url):
        """
        Test function to scrape a single business and return data without saving to DB.
        Useful for debugging.
        """
        logging.info(f"Test scraping single business: {business_url}")
        
        try:
            scraper = WebScraper(business_url)
            scraped_data = scraper.scrape()
            
            return {
                'status': 'success',
                'url': business_url,
                'data': scraped_data
            }
            
        except Exception as e:
            logging.error(f"Test scrape error: {e}")
            return {
                'status': 'error',
                'url': business_url,
                'error': str(e)
            }
    
    def extract_address_from_business_page(self, business_url, driver=None):
        """
        Extract address from a Google Maps business detail page.
        
        Args:
            business_url: URL of the business detail page
            driver: Optional existing webdriver to reuse
            
        Returns:
            Address string or None if not found
        """
        try:
            # Setup driver (reuse if provided, otherwise create temp)
            if driver:
                temp_driver = driver
            else:
                temp_driver = self.setup_driver()
            
            # Navigate if needed (check if already on page to save time)
            try:
                if temp_driver.current_url != business_url:
                    temp_driver.get(business_url)
            except:
                temp_driver.get(business_url)
            
            WebDriverWait(temp_driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1)
            
            # Extract address using multiple selectors
            address_selectors = [
                "//button[@data-item-id='address']//div[contains(@class, 'fontBodyMedium')]",
                "//button[contains(@aria-label, 'Address')]//div[contains(@class, 'fontBodyMedium')]",
                "//div[@data-tooltip='Copy address']",
                "//button[contains(@data-tooltip, 'Copy address')]//div",
                "//div[contains(@class, 'rogA2c')]",  # Address container
                "//address", 
                "//div[contains(@class, 'Io6YTe') and contains(@class, 'fontBodyMedium')]", # Common text container
            ]
            
            for selector in address_selectors:
                try:
                    address_element = temp_driver.find_element(By.XPATH, selector)
                    address_text = address_element.text.strip()
                    
                    if address_text and len(address_text) > 5:
                        if not driver:
                            temp_driver.quit()
                        return address_text
                        
                except NoSuchElementException:
                    continue
            
            if not driver:
                temp_driver.quit()
            return None
            
        except (TimeoutException, Exception) as e:
            logging.warning(f"Could not extract address from {business_url}: {str(e)}")
            if not driver:
                if 'temp_driver' in locals():
                    try:
                        temp_driver.quit()
                    except:
                        pass
            return None

    def extract_website_from_business_page(self, business_url, driver=None):
        """
        Extract website URL from a Google Maps business detail page.
        Looks for domain extensions (.com, .ca, .org, etc.) and www prefixes.
        
        Args:
            business_url: URL of the business detail page
            driver: Optional existing webdriver to reuse
            
        Returns:
            Website URL string or None if not found
        """
        try:
            # Setup driver (reuse if provided, otherwise create temp)
            if driver:
                temp_driver = driver
            else:
                temp_driver = self.setup_driver()
            
            # Navigate if needed
            try:
                if temp_driver.current_url != business_url:
                    temp_driver.get(business_url)
            except:
                temp_driver.get(business_url)
                
            WebDriverWait(temp_driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(2)  # Increased wait for Google Maps to fully load
            
            # PRIORITY 1: Look for the website button/link in Google Maps (most reliable)
            # These selectors target the actual website link in the business info panel
            priority_selectors = [
                # Website button with data-item-id containing 'authority' (most reliable)
                "//a[@data-item-id='authority']",
                # Website link with aria-label
                "//a[contains(@aria-label, 'Website:')]",
                "//a[contains(@aria-label, 'website')]",
                # Button that opens website
                "//button[@data-item-id='authority']//following::a[1]",
                # Link inside website section
                "//div[contains(@class, 'rogA2c')]//a[contains(@href, 'http')]",
            ]
            
            for selector in priority_selectors:
                try:
                    elements = temp_driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        href = element.get_attribute("href")
                        # Strict filter: Must not be a Google Maps/Search link
                        if href and 'google.com/maps' not in href and 'google.com/search' not in href and 'goo.gl' not in href:
                            logging.info(f"Found website URL (priority): {href}")
                            if not driver:
                                temp_driver.quit()
                            return href
                except:
                    continue
            
            # PRIORITY 2: Try standard selectors
            website_selectors = [
                "//a[contains(@href, 'http') and contains(@aria-label, 'Website')]",
                "//a[contains(@data-item-id, 'authority') and contains(@href, 'http')]",
                "//a[@data-tooltip='Open website']",
                "//div[contains(@class, 'fontBodyMedium')]//a[contains(@href, 'http')]",
            ]
            
            for selector in website_selectors:
                try:
                    website_elements = temp_driver.find_elements(By.XPATH, selector)
                    for element in website_elements:
                        href = element.get_attribute("href")
                        if href:
                            # Make sure it's not a Google URL
                            if 'google.com/maps' not in href and 'google.com/search' not in href and 'goo.gl' not in href:
                                # Check if it contains common domain extensions (including country-code TLDs)
                                domain_extensions = [
                                    '.com', '.ca', '.org', '.net', '.gov', '.edu', '.co', '.io', '.biz', '.info',
                                    '.com.au', '.co.uk', '.co.nz', '.com.sg', '.co.za', '.com.br', '.com.mx',
                                    '.au', '.uk', '.nz', '.de', '.fr', '.jp', '.cn', '.in', '.us'
                                ]
                                for ext in domain_extensions:
                                    if ext in href.lower():
                                        logging.info(f"Found website URL: {href}")
                                        if not driver:
                                            temp_driver.quit()
                                        return href
                        
                        # Also check element text for domain patterns
                        text = element.text.strip()
                        if text:
                            # Look for domain patterns in text (like "ahs.ca" or "example.com.au")
                            import re
                            domain_pattern = r'\b(?:www\.)?[a-zA-Z0-9-]+\.[a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})?\b'
                            matches = re.findall(domain_pattern, text)
                            for match in matches:
                                if not any(skip in match.lower() for skip in ['google', 'maps', 'goo.gl']):
                                    # Add http if not present
                                    if not match.startswith('http'):
                                        website_url = f"https://{match}"
                                    else:
                                        website_url = match
                                    logging.info(f"Found website from text: {website_url}")
                                    if not driver: # Only quit if we created the driver
                                        temp_driver.quit()
                                    return website_url
                                    
                except NoSuchElementException:
                    continue
            
            # Additional search in page source for domain patterns
            try:
                page_source = temp_driver.page_source
                import re
                # Look for domain patterns in the entire page (including country-code TLDs like .com.au)
                domain_pattern = r'\b(?:www\.)?[a-zA-Z0-9-]+\.(?:com|ca|org|net|gov|edu|co|io|biz|info|au|uk|nz|de|fr)(?:\.(?:au|uk|nz|sg|za|br|mx))?\b'
                matches = re.findall(domain_pattern, page_source, re.IGNORECASE)
                
                for match in matches:
                    if not any(skip in match.lower() for skip in ['google', 'maps', 'goo.gl', 'youtube', 'facebook', 'instagram']):
                        # Add https if not present
                        if not match.startswith('http'):
                            website_url = f"https://{match}"
                        else:
                            website_url = match
                        logging.info(f"Found website from page source: {website_url}")
                        if not driver:
                            temp_driver.quit()
                        return website_url
                        
            except Exception as e:
                logging.warning(f"Error searching page source for website: {e}")
            
            if not driver:
                temp_driver.quit()
            return None
            
        except (TimeoutException, Exception) as e:
            logging.warning(f"Could not extract website from {business_url}: {str(e)}")
            if not driver:
                if 'temp_driver' in locals():
                    try:
                        temp_driver.quit()
                    except:
                        pass
            return None

    def extract_email_from_website(self, website_url, driver=None):
        """
        Extract email address from a business website.
        Tries contact/about pages first for faster results.
        
        Args:
            website_url: URL of the business website
            driver: Optional existing webdriver to reuse
            
        Returns:
            Email address string or None if not found
        """
        temp_driver = None
        created_driver = False
        
        try:
            # Skip if no website URL or if it's a Google Maps URL
            if not website_url or website_url == 'N/A':
                return None
            if 'google.com/maps' in website_url or 'goo.gl' in website_url:
                return None
                
            logging.info(f"Extracting email from website: {website_url}")
            
            # Reuse driver if provided, otherwise create new one
            if driver:
                temp_driver = driver
            else:
                temp_driver = self.setup_driver()
                created_driver = True
                
            temp_driver.set_page_load_timeout(10)  # 10 second max
            
            # Email regex pattern
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            
            # Excluded domains
            excluded_domains = [
                'example.com', 'test.com', 'gmail.com', 'yahoo.com', 'hotmail.com',
                'outlook.com', 'facebook.com', 'twitter.com', 'instagram.com',
                'linkedin.com', 'youtube.com', 'google.com', 'microsoft.com',
                'apple.com', 'amazon.com', 'noreply', 'no-reply', 'sentry.io',
                'wixpress.com', 'schema.org', 'w3.org'
            ]
            
            # Get base URL for constructing contact page URLs
            base_url = website_url.rstrip('/')
            if not base_url.startswith('http'):
                base_url = 'https://' + base_url
            
            # Try contact/about pages FIRST (most likely to have emails)
            # Priority order: contact pages first, then about, then home
            contact_paths = [
                '/contact', '/contact-us', '/contactus', '/contact.html',
                '/about', '/about-us', '/aboutus', '/about.html',
                '/get-in-touch', '/reach-us', '/connect',
                ''  # Home page last
            ]
            pages_to_try = [base_url + path for path in contact_paths]
            
            for page_url in pages_to_try[:4]:  # Try up to 4 pages
                try:
                    logging.info(f"Checking page for email: {page_url}")
                    temp_driver.get(page_url)
                    time.sleep(1)
                    
                    # Quick check for mailto links first
                    try:
                        mailto_links = temp_driver.find_elements(By.XPATH, "//a[contains(@href, 'mailto:')]")
                        for link in mailto_links:
                            href = link.get_attribute("href")
                            if href and 'mailto:' in href:
                                email = href.replace("mailto:", "").strip()
                                if '?' in email:
                                    email = email.split('?')[0]
                                if re.match(email_pattern, email):
                                    email = email.lower()
                                    if not any(ex in email for ex in excluded_domains):
                                        logging.info(f"Found email from mailto: {email}")
                                        if created_driver:
                                            temp_driver.quit()
                                        return email
                    except:
                        pass
                    
                    # Search page source
                    page_source = temp_driver.page_source
                    emails = re.findall(email_pattern, page_source)
                    
                    for email in emails:
                        email = email.lower().strip()
                        if not any(ex in email for ex in excluded_domains):
                            logging.info(f"Found email: {email}")
                            if created_driver:
                                temp_driver.quit()
                            return email
                            
                except TimeoutException:
                    logging.warning(f"Timeout loading {page_url}")
                    continue
                except Exception as e:
                    logging.warning(f"Error checking {page_url}: {e}")
                    continue
            
            if created_driver:
                temp_driver.quit()
            return None
            
        except Exception as e:
            logging.warning(f"Could not extract email from {website_url}: {str(e)}")
            if created_driver and temp_driver:
                try:
                    temp_driver.quit()
                except:
                    pass
            return None

    def extract_phone_from_business_page(self, business_url, driver=None):
        """
        Extract phone number from a Google Maps business detail page.
        
        Args:
            business_url: URL of the business detail page
            driver: Optional existing webdriver to reuse
            
        Returns:
            Phone number string or None if not found
        """
        try:
            # Setup driver (reuse if provided, otherwise create temp)
            if driver:
                temp_driver = driver
            else:
                temp_driver = self.setup_driver()
            
            # Navigate if needed
            try:
                if temp_driver.current_url != business_url:
                    temp_driver.get(business_url)
            except:
                temp_driver.get(business_url)
                
            WebDriverWait(temp_driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(2)  # Increased wait for Google Maps to fully load
            
            # PRIORITY 1: Most reliable phone selectors for Google Maps
            phone_selectors = [
                # Phone button with data-item-id (most reliable)
                "//button[starts-with(@data-item-id, 'phone:tel:')]//div[contains(@class, 'fontBodyMedium')]",
                "//button[contains(@data-item-id, 'phone')]//div[contains(@class, 'fontBodyMedium')]",
                # Phone link with aria-label
                "//a[contains(@aria-label, 'Phone:')]",
                "//button[contains(@aria-label, 'Phone:')]//div",
                # Tel links
                "//a[starts-with(@href, 'tel:')]",
                # Copy phone button
                "//button[contains(@data-tooltip, 'Copy phone')]//div",
                "//button[contains(@aria-label, 'Copy phone')]//div",
                # Fallback selectors
                "//div[contains(@class, 'rogA2c')]//span[contains(text(), '(')]",
                "//div[contains(@class, 'Io6YTe') and contains(text(), '(')]", 
                "//div[contains(@class, 'Io6YTe') and contains(text(), '+')]",

            ]
            
            for selector in phone_selectors:
                try:
                    phone_element = temp_driver.find_element(By.XPATH, selector)
                    phone_text = phone_element.text.strip()
                    
                    if not phone_text:
                        href = phone_element.get_attribute("href")
                        if href and 'tel:' in href:
                            phone_text = href.replace("tel:", "").strip()
                    
                    if phone_text and len(phone_text) > 5:
                        if not driver:
                            temp_driver.quit()
                        return phone_text
                        
                except NoSuchElementException:
                    continue
            
            if not driver:
                temp_driver.quit()
            return None
            
        except (TimeoutException, Exception) as e:
            logging.warning(f"Could not extract phone from {business_url}: {str(e)}")
            # Only quit driver if we created it (not passed in)
            if not driver and 'temp_driver' in locals():
                try:
                    temp_driver.quit()
                except:
                    pass
            return None

    def __del__(self):
        if hasattr(self, 'driver') and self.driver:
            try:
                self.driver.quit()
            except:
                pass
