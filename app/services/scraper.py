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

        # Extract business name (works for both Google Maps and regular websites)
        try:
            if "google.com/maps" in self.url:
                # Google Maps specific selectors
                name_selectors = [
                    "//h1[contains(@class, 'DUwDvf')]",  # Main business name
                    "//h1[@class='fontHeadlineLarge']",
                    "//h1",
                    "//div[@role='main']//h1"
                ]
                for selector in name_selectors:
                    try:
                        business_name_element = self.driver.find_element(By.XPATH, selector)
                        name = business_name_element.text.strip()
                        if name:
                            self.data['company_name'] = name
                            logging.info(f"Found business name: {name}")
                            break
                    except NoSuchElementException:
                        continue
            else:
                business_name_element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//h1|//h2"))
                )
                self.data['company_name'] = business_name_element.text.strip() or "N/A"
        except (TimeoutException, NoSuchElementException):
            logging.warning("Business name element not found.")

        # Extract address
        try:
            if "google.com/maps" in self.url:
                # Google Maps specific selectors for address
                address_selectors = [
                    "//button[@data-item-id='address']//div[contains(@class, 'fontBodyMedium')]",
                    "//div[@data-tooltip='Copy address']",
                    "//button[contains(@aria-label, 'Address')]//div",
                    "//div[contains(@class, 'rogA2c')]"  # Address container
                ]
                for selector in address_selectors:
                    try:
                        address_element = self.driver.find_element(By.XPATH, selector)
                        address = address_element.text.strip()
                        if address and len(address) > 5:  # Basic validation
                            self.data['address'] = address
                            logging.info(f"Found address: {address}")
                            break
                    except NoSuchElementException:
                        continue
            else:
                address_element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//address|//div[contains(@class, 'address')]"))
                )
                self.data['address'] = address_element.text.strip() or "N/A"
        except (TimeoutException, NoSuchElementException):
            logging.warning("Address element not found.")

        # Extract phone
        try:
            if "google.com/maps" in self.url:
                # Google Maps specific selectors for phone
                phone_selectors = [
                    "//button[@data-item-id='phone:tel:']//div[contains(@class, 'fontBodyMedium')]",
                    "//button[contains(@aria-label, 'Phone')]//div[contains(@class, 'fontBodyMedium')]",
                    "//a[contains(@href, 'tel:')]",
                    "//button[contains(@data-tooltip, 'Copy phone number')]"
                ]
                for selector in phone_selectors:
                    try:
                        phone_element = self.driver.find_element(By.XPATH, selector)
                        phone = phone_element.text.strip() or phone_element.get_attribute("href", "").replace("tel:", "")
                        if phone:
                            self.data['phone'] = self.validate_phone_number(phone)
                            logging.info(f"Found phone: {phone}")
                            break
                    except NoSuchElementException:
                        continue
            else:
                phone_element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//a[contains(@href, 'tel:')]"))
                )
                phone = phone_element.text.strip() or phone_element.get_attribute("href").replace("tel:", "")
                self.data['phone'] = self.validate_phone_number(phone)
        except (TimeoutException, NoSuchElementException):
            logging.warning("Phone element not found.")

        if "google.com/maps" in self.url:
            try:
                # Try multiple selectors to find the website link
                website_selectors = [
                    "//a[contains(@href, 'http') and contains(@aria-label, 'Website')]",
                    "//a[contains(@data-item-id, 'authority') and contains(@href, 'http')]",
                    "//a[contains(@href, 'http') and contains(text(), 'Website')]",
                    "//a[@data-tooltip='Open website']",
                    "//div[contains(@class, 'fontBodyMedium')]//a[contains(@href, 'http')]"
                ]
                
                website_url = None
                for selector in website_selectors:
                    try:
                        website_element = self.driver.find_element(By.XPATH, selector)
                        href = website_element.get_attribute("href")
                        # Make sure it's not a Google URL
                        if href and 'google.com' not in href and 'goo.gl' not in href:
                            website_url = href
                            logging.info(f"Found website URL: {website_url}")
                            break
                    except NoSuchElementException:
                        continue
                
                if website_url:
                    self.data['website_url'] = self.validate_url(website_url)
                else:
                    logging.warning("No website URL found on Google Maps page")
                    self.data['website_url'] = "N/A"
                    
            except Exception as e:
                logging.error(f"Error finding website URL: {e}")
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

        if system == "darwin":  
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
        businesses = self.extract_businesses_with_names()
        return [business['url'] for business in businesses]
    
    def extract_businesses_with_names(self):
        """
        Extract business information (name and URL) from Google Maps search results.
        Finds all business listings, extracts their names and URLs, and deduplicates.
        
        Returns:
            List of dictionaries with 'name' and 'url' keys
        """
        logging.info(f"Extracting businesses with names from search results: {self.search_url}")
        
        try:
            # Navigate to the search URL
            logging.info(f"Navigating to Google Maps search URL")
            self.driver.get(self.search_url)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(5)  # Allow page to fully load (increased from 3 to 5 seconds)
            
            # Wait for Google Maps to load results
            try:
                WebDriverWait(self.driver, 10).until(
                    lambda driver: len(driver.find_elements(By.XPATH, "//a[contains(@href, '/maps/place/')]")) > 0
                )
                logging.info("Search results page loaded successfully with business links")
            except:
                logging.warning("No business links found after waiting, but continuing anyway")
                logging.info("Search results page loaded (no business links detected yet)")
            
            # Scroll to load more results
            self.scroll_results_panel()
            
            # Find all business listing containers
            # Google Maps uses div elements with specific classes for each listing
            business_containers = []
            
            # Try multiple strategies to find business containers
            container_selectors = [
                "//div[contains(@class, 'Nv2PK')]",  # Common container class
                "//div[contains(@class, 'THOPZb')]",  # Alternative container
                "//div[@role='article']",  # Role-based selector
                "//div[contains(@class, 'lI9IFe')]",  # Another container class
                "//div[contains(@class, 'VkpGBb')]",  # Yet another container
                "//a[contains(@href, '/maps/place/')]/..",  # Parent of link
            ]
            
            for selector in container_selectors:
                try:
                    business_containers = self.driver.find_elements(By.XPATH, selector)
                    if business_containers and len(business_containers) > 0:
                        logging.info(f"Found {len(business_containers)} business containers using selector: {selector}")
                        break
                except Exception as e:
                    logging.warning(f"Selector {selector} failed: {e}")
                    continue
            
            if not business_containers:
                logging.error("No business containers found with any selector")
                # Last resort: try to get just the links directly
                try:
                    links = self.driver.find_elements(By.XPATH, "//a[contains(@href, '/maps/place/')]")
                    logging.info(f"Fallback: Found {len(links)} business links directly")
                    if links:
                        # Create pseudo-containers from links
                        business_containers = links
                    else:
                        # Try even more generic selectors
                        generic_selectors = [
                            "//div[contains(@aria-label, 'Results')]//div",
                            "//div[@role='feed']//div",
                            "//div[contains(@class, 'section-result')]",
                        ]
                        for generic_selector in generic_selectors:
                            try:
                                containers = self.driver.find_elements(By.XPATH, generic_selector)
                                if containers:
                                    logging.info(f"Generic fallback found {len(containers)} containers with: {generic_selector}")
                                    business_containers = containers
                                    break
                            except Exception as e:
                                logging.warning(f"Generic selector {generic_selector} failed: {e}")
                                continue
                except Exception as e:
                    logging.error(f"Even fallback link extraction failed: {e}")
                    return []
            
            # Extract business info
            businesses = []
            seen_urls = set()
            
            for container in business_containers:
                try:
                    # Check if container is already a link element (fallback case)
                    if container.tag_name == 'a':
                        link_element = container
                        href = link_element.get_attribute("href")
                    else:
                        # Try multiple ways to find the link within container
                        link_element = None
                        href = None
                        
                        link_selectors = [
                            ".//a[contains(@href, '/maps/place/')]",
                            ".//a[contains(@href, 'maps/place')]",
                            ".//a[contains(@data-value, 'feature')]",
                            ".//a[@role='button']",
                        ]
                        
                        for link_selector in link_selectors:
                            try:
                                link_element = container.find_element(By.XPATH, link_selector)
                                href = link_element.get_attribute("href")
                                if href and '/maps/place/' in href:
                                    break
                            except NoSuchElementException:
                                continue
                        
                        # If still no link found, try data attributes
                        if not href:
                            try:
                                data_fid = container.get_attribute("data-fid")
                                if data_fid:
                                    href = f"https://www.google.com/maps/place/?ftid={data_fid}"
                            except:
                                pass
                    
                    if not href or '/maps/place/' not in href:
                        continue
                    
                    # Clean the URL for deduplication
                    base_url = href.split('?')[0] if '?' in href else href
                    
                    # Skip if we've already seen this business
                    if base_url in seen_urls:
                        continue
                    
                    # Extract business name - try multiple selectors
                    business_name = "Unknown Business"
                    
                    # If container is a link, try to get aria-label or text
                    if container.tag_name == 'a':
                        aria_label = container.get_attribute("aria-label")
                        if aria_label and len(aria_label) > 0:
                            business_name = aria_label
                        else:
                            text = container.text.strip()
                            if text and len(text) > 0:
                                business_name = text
                    else:
                        # Try multiple selectors within container
                        name_selectors = [
                            ".//div[contains(@class, 'fontHeadlineSmall')]",  # Common name class
                            ".//div[contains(@class, 'qBF1Pd')]",  # Alternative name class
                            ".//span[contains(@class, 'OSrXXb')]",  # Another alternative
                            ".//div[contains(@class, 'fontBodyMedium')]",  # Another class
                            ".//a[contains(@href, '/maps/place/')]",  # Fallback to link text
                        ]
                        
                        for selector in name_selectors:
                            try:
                                name_element = container.find_element(By.XPATH, selector)
                                name_text = name_element.text.strip()
                                if name_text and len(name_text) > 0:
                                    business_name = name_text
                                    break
                            except NoSuchElementException:
                                continue
                        
                        # If still no name, try aria-label on the link
                        if business_name == "Unknown Business":
                            try:
                                aria_label = link_element.get_attribute("aria-label")
                                if aria_label and len(aria_label) > 0:
                                    business_name = aria_label
                            except:
                                pass
                    
                    # Try to extract phone number from the listing (if visible in search results)
                    # Note: Google Maps usually doesn't show phone numbers in search results
                    # They only appear when you click on the business
                    phone_number = None
                    
                    # Add to results
                    seen_urls.add(base_url)
                    business_data = {
                        'name': business_name,
                        'url': href
                    }
                    if phone_number:
                        business_data['phone'] = phone_number
                    businesses.append(business_data)
                    
                except NoSuchElementException as e:
                    logging.warning(f"Could not extract business info from container: {e}")
                    continue
                except Exception as e:
                    logging.warning(f"Error extracting business info: {e}")
                    continue
            
            # Log results
            unique_count = len(businesses)
            logging.info(f"Successfully extracted {unique_count} unique businesses with names")
            
            if unique_count < 10:
                logging.warning(f"Found fewer than 10 businesses ({unique_count} found). This may be expected if the search has limited results.")
            
            # Validate URLs
            validated_businesses = []
            url_pattern = r'^https?:\/\/'
            
            for business in businesses:
                if re.match(url_pattern, business['url']):
                    validated_businesses.append(business)
                else:
                    logging.warning(f"Invalid URL format, skipping: {business['url']}")
            
            logging.info(f"Returning {len(validated_businesses)} validated businesses")
            return validated_businesses
            
        except TimeoutException as e:
            logging.error(f"Timeout while loading Google Maps search results (timeout after 15s): {e}")
            return []
        except NoSuchElementException as e:
            logging.warning(f"Required element not found while extracting businesses: {e}")
            return []
        except WebDriverException as e:
            logging.error(f"WebDriver error while extracting businesses: {e}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error while extracting businesses: {e}")
            return []
    
    def extract_phone_from_business_page(self, business_url):
        """
        Extract phone number from a Google Maps business detail page.
        
        Args:
            business_url: URL of the business detail page
            
        Returns:
            Phone number string or None if not found
        """
        try:
            self.driver.get(business_url)
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1)
            
            phone_selectors = [
                "//button[@data-item-id='phone:tel:']//div[contains(@class, 'fontBodyMedium')]",
                "//button[contains(@aria-label, 'Phone')]//div[contains(@class, 'fontBodyMedium')]",
                "//a[contains(@href, 'tel:')]",
                "//button[contains(@data-tooltip, 'Copy phone number')]//div",
                "//div[contains(@class, 'rogA2c') and contains(., '+')]",
            ]
            
            for selector in phone_selectors:
                try:
                    phone_element = self.driver.find_element(By.XPATH, selector)
                    phone_text = phone_element.text.strip()
                    
                    if not phone_text:
                        href = phone_element.get_attribute("href")
                        if href and 'tel:' in href:
                            phone_text = href.replace("tel:", "").strip()
                    
                    if phone_text and len(phone_text) > 5:
                        return phone_text
                        
                except NoSuchElementException:
                    continue
            
            return None
            
        except (TimeoutException, Exception) as e:
            logging.warning(f"Could not extract phone from {business_url}: {str(e)}")
            return None
    
    def extract_email_from_website(self, website_url):
        """
        Extract email address from a business website.
        
        Args:
            website_url: URL of the business website
            
        Returns:
            Email address string or None if not found
        """
        try:
            # Skip if no website URL
            if not website_url or website_url == 'N/A':
                return None
                
            logging.info(f"Extracting email from website: {website_url}")
            
            self.driver.get(website_url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)  # Allow page to fully load
            
            # Email regex pattern
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            
            # Method 1: Look for mailto links
            try:
                mailto_links = self.driver.find_elements(By.XPATH, "//a[contains(@href, 'mailto:')]")
                for link in mailto_links:
                    href = link.get_attribute("href")
                    if href and 'mailto:' in href:
                        email = href.replace("mailto:", "").strip()
                        # Clean up any additional parameters
                        if '?' in email:
                            email = email.split('?')[0]
                        if re.match(email_pattern, email):
                            logging.info(f"Found email from mailto link: {email}")
                            return email
            except Exception as e:
                logging.warning(f"Error checking mailto links: {e}")
            
            # Method 2: Search page source for email patterns
            try:
                page_source = self.driver.page_source
                emails = re.findall(email_pattern, page_source)
                
                # Filter out common non-business emails
                excluded_domains = [
                    'example.com', 'test.com', 'gmail.com', 'yahoo.com', 'hotmail.com',
                    'outlook.com', 'facebook.com', 'twitter.com', 'instagram.com',
                    'linkedin.com', 'youtube.com', 'google.com', 'microsoft.com',
                    'apple.com', 'amazon.com', 'noreply', 'no-reply'
                ]
                
                for email in emails:
                    email = email.lower().strip()
                    # Skip if it contains excluded domains or patterns
                    if not any(excluded in email for excluded in excluded_domains):
                        # Prefer emails that match the website domain
                        website_domain = website_url.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
                        if website_domain in email:
                            logging.info(f"Found matching domain email: {email}")
                            return email
                
                # If no domain match, return the first valid email
                for email in emails:
                    email = email.lower().strip()
                    if not any(excluded in email for excluded in excluded_domains):
                        logging.info(f"Found email from page source: {email}")
                        return email
                        
            except Exception as e:
                logging.warning(f"Error searching page source for emails: {e}")
            
            # Method 3: Look for common contact page patterns
            try:
                contact_links = self.driver.find_elements(By.XPATH, 
                    "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'contact') or "
                    "contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'about') or "
                    "contains(translate(@href, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'contact')]"
                )
                
                for link in contact_links[:2]:  # Check first 2 contact links
                    try:
                        contact_url = link.get_attribute("href")
                        if contact_url and contact_url.startswith('http'):
                            logging.info(f"Checking contact page: {contact_url}")
                            self.driver.get(contact_url)
                            WebDriverWait(self.driver, 5).until(
                                EC.presence_of_element_located((By.TAG_NAME, "body"))
                            )
                            time.sleep(2)
                            
                            # Search for emails on contact page
                            contact_page_source = self.driver.page_source
                            contact_emails = re.findall(email_pattern, contact_page_source)
                            
                            for email in contact_emails:
                                email = email.lower().strip()
                                if not any(excluded in email for excluded in excluded_domains):
                                    logging.info(f"Found email from contact page: {email}")
                                    return email
                                    
                    except Exception as contact_error:
                        logging.warning(f"Error checking contact page: {contact_error}")
                        continue
                        
            except Exception as e:
                logging.warning(f"Error checking contact pages: {e}")
            
            return None
            
        except (TimeoutException, Exception) as e:
            logging.warning(f"Could not extract email from {website_url}: {str(e)}")
            return None

    def extract_website_from_business_page(self, business_url):
        """
        Extract website URL from a Google Maps business detail page.
        Looks for domain extensions (.com, .ca, .org, etc.) and www prefixes.
        
        Args:
            business_url: URL of the business detail page
            
        Returns:
            Website URL string or None if not found
        """
        try:
            self.driver.get(business_url)
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1)
            
            # Try multiple selectors to find website links
            website_selectors = [
                "//a[contains(@href, 'http') and contains(@aria-label, 'Website')]",
                "//a[contains(@data-item-id, 'authority') and contains(@href, 'http')]",
                "//a[contains(@href, 'http') and contains(text(), 'Website')]",
                "//a[@data-tooltip='Open website']",
                "//div[contains(@class, 'fontBodyMedium')]//a[contains(@href, 'http')]",
                "//button[contains(@aria-label, 'Website')]//a",
                "//a[contains(@href, '.com')]",
                "//a[contains(@href, '.ca')]",
                "//a[contains(@href, '.org')]",
                "//a[contains(@href, '.net')]",
                "//a[contains(@href, '.gov')]",
                "//a[contains(@href, '.edu')]",
            ]
            
            for selector in website_selectors:
                try:
                    website_elements = self.driver.find_elements(By.XPATH, selector)
                    for element in website_elements:
                        href = element.get_attribute("href")
                        if href:
                            # Make sure it's not a Google URL
                            if 'google.com' not in href and 'goo.gl' not in href and 'maps' not in href:
                                # Check if it contains common domain extensions
                                domain_extensions = ['.com', '.ca', '.org', '.net', '.gov', '.edu', '.co', '.io', '.biz', '.info']
                                for ext in domain_extensions:
                                    if ext in href.lower():
                                        logging.info(f"Found website URL: {href}")
                                        return href
                        
                        # Also check element text for domain patterns
                        text = element.text.strip()
                        if text:
                            # Look for domain patterns in text (like "ahs.ca")
                            import re
                            domain_pattern = r'\b(?:www\.)?[a-zA-Z0-9-]+\.[a-zA-Z]{2,}\b'
                            matches = re.findall(domain_pattern, text)
                            for match in matches:
                                if not any(skip in match.lower() for skip in ['google', 'maps', 'goo.gl']):
                                    # Add http if not present
                                    if not match.startswith('http'):
                                        website_url = f"https://{match}"
                                    else:
                                        website_url = match
                                    logging.info(f"Found website from text: {website_url}")
                                    return website_url
                                    
                except NoSuchElementException:
                    continue
            
            # Additional search in page source for domain patterns
            try:
                page_source = self.driver.page_source
                import re
                # Look for domain patterns in the entire page
                domain_pattern = r'\b(?:www\.)?[a-zA-Z0-9-]+\.(?:com|ca|org|net|gov|edu|co|io|biz|info)\b'
                matches = re.findall(domain_pattern, page_source, re.IGNORECASE)
                
                for match in matches:
                    if not any(skip in match.lower() for skip in ['google', 'maps', 'goo.gl', 'youtube', 'facebook', 'instagram']):
                        # Add https if not present
                        if not match.startswith('http'):
                            website_url = f"https://{match}"
                        else:
                            website_url = match
                        logging.info(f"Found website from page source: {website_url}")
                        return website_url
                        
            except Exception as e:
                logging.warning(f"Error searching page source for website: {e}")
            
            return None
            
        except (TimeoutException, Exception) as e:
            logging.warning(f"Could not extract website from {business_url}: {str(e)}")
            return None

    def extract_address_from_business_page(self, business_url):
        """
        Extract address from a Google Maps business detail page.
        
        Args:
            business_url: URL of the business detail page
            
        Returns:
            Address string or None if not found
        """
        try:
            self.driver.get(business_url)
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1)
            
            # Extract address
            address_selectors = [
                "//button[@data-item-id='address']//div[contains(@class, 'fontBodyMedium')]",
                "//button[contains(@aria-label, 'Address')]//div[contains(@class, 'fontBodyMedium')]",
                "//div[@data-tooltip='Copy address']",
                "//button[contains(@data-tooltip, 'Copy address')]//div",
                "//div[contains(@class, 'rogA2c')]",  # Address container
            ]
            
            for selector in address_selectors:
                try:
                    address_element = self.driver.find_element(By.XPATH, selector)
                    address_text = address_element.text.strip()
                    
                    if address_text and len(address_text) > 5:
                        return address_text
                        
                except NoSuchElementException:
                    continue
            
            return None
            
        except (TimeoutException, Exception) as e:
            logging.warning(f"Could not extract address from {business_url}: {str(e)}")
            return None

    def extract_phone_and_address_from_business_page(self, business_url):
        """
        Extract phone number and address from a Google Maps business detail page.
        OPTIMIZED: Extracts both in one page load to save time.
        
        Args:
            business_url: URL of the business detail page
            
        Returns:
            Dictionary with 'phone' and 'address' keys (values are None if not found)
        """
        result = {'phone': None, 'address': None}
        
        try:
            # Navigate to the business page
            self.driver.get(business_url)
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1)
            
            # Extract phone number
            phone_selectors = [
                "//button[@data-item-id='phone:tel:']//div[contains(@class, 'fontBodyMedium')]",
                "//button[contains(@aria-label, 'Phone')]//div[contains(@class, 'fontBodyMedium')]",
                "//a[contains(@href, 'tel:')]",
                "//button[contains(@data-tooltip, 'Copy phone number')]//div",
                "//div[contains(@class, 'rogA2c') and contains(., '+')]",
            ]
            
            for selector in phone_selectors:
                try:
                    phone_element = self.driver.find_element(By.XPATH, selector)
                    phone_text = phone_element.text.strip()
                    
                    if not phone_text:
                        href = phone_element.get_attribute("href")
                        if href and 'tel:' in href:
                            phone_text = href.replace("tel:", "").strip()
                    
                    if phone_text and len(phone_text) > 5:
                        result['phone'] = phone_text
                        break
                        
                except NoSuchElementException:
                    continue
            
            # Extract address
            address_selectors = [
                "//button[@data-item-id='address']//div[contains(@class, 'fontBodyMedium')]",
                "//button[contains(@aria-label, 'Address')]//div[contains(@class, 'fontBodyMedium')]",
                "//div[@data-tooltip='Copy address']",
                "//button[contains(@data-tooltip, 'Copy address')]//div",
                "//div[contains(@class, 'rogA2c')]",  # Address container
            ]
            
            for selector in address_selectors:
                try:
                    address_element = self.driver.find_element(By.XPATH, selector)
                    address_text = address_element.text.strip()
                    
                    if address_text and len(address_text) > 5:
                        result['address'] = address_text
                        break
                        
                except NoSuchElementException:
                    continue
            
            return result
            
        except (TimeoutException, Exception) as e:
            logging.warning(f"Could not extract data from {business_url}: {str(e)}")
            return result
    
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
                        
                        # Commit immediately to database to free memory
                        try:
                            db.session.add(new_data)
                            db.session.commit()
                            logging.info(f"Successfully saved business {index}/{len(business_urls)} to database")
                            
                            # Add to results list with database ID
                            results.append({
                                'id': new_data.id,
                                'company_name': scraped_data.get('company_name', 'N/A'),
                                'email': scraped_data.get('email', 'N/A'),
                                'phone': scraped_data.get('phone', 'N/A'),
                                'address': scraped_data.get('address', 'N/A'),
                                'website_url': business_url,
                                'created_at': new_data.created_at.strftime('%Y-%m-%d %H:%M:%S')
                            })
                            
                            logging.info(f"Successfully scraped business {index}/{len(business_urls)}: {business_name}")
                        except Exception as db_error:
                            logging.error(f"Database error for business {index}: {str(db_error)}")
                            db.session.rollback()
                            errors.append({
                                'url': business_url,
                                'business_name': business_name,
                                'error': f"Database error: {str(db_error)}"
                            })
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
                
                # Add delay between scrapes to avoid detection and free memory
                if index < len(business_urls):  # Don't delay after the last one
                    delay = 1.5  # 1.5 seconds between scrapes
                    time.sleep(delay)
            
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
    
    def scrape_all_businesses_stream(self, user_id):
        """
        Stream businesses as they're scraped (generator function).
        Yields each business result immediately after scraping.
        
        Args:
            user_id: The ID of the user initiating the scraping request
            
        Yields:
            Dictionary with result data for each business as it's scraped
        """
        from app.models.scraped_data import ScrapedData
        from app import db, create_app
        
        # Get app context for database operations
        app = create_app()
        app.app_context().push()
        
        logging.info(f"Starting STREAMING multi-business scraping for user {user_id}")
        
        try:
            # Setup driver for extracting business URLs
            try:
                self.driver = self.setup_driver()
                yield {'type': 'status', 'message': 'Extracting business URLs...'}
            except WebDriverException as e:
                yield {'type': 'error', 'error': f"Failed to setup WebDriver: {str(e)}"}
                return
            
            # Extract all business URLs
            business_urls = self.extract_business_urls()
            
            if not business_urls:
                yield {'type': 'error', 'error': 'No business listings found'}
                return
            
            total_count = len(business_urls)
            yield {'type': 'status', 'message': f'Found {total_count} businesses. Starting scraping...', 'total': total_count}
            
            # Close search driver
            if self.driver:
                self.driver.quit()
                self.driver = None
            
            # Scrape each business and yield immediately
            for index, business_url in enumerate(business_urls, start=1):
                business_name = 'Unknown'
                
                try:
                    # Scrape this business
                    scraper = WebScraper(business_url)
                    scraped_data = scraper.scrape()
                    business_name = scraped_data.get('company_name', 'Unknown')
                    
                    if scraped_data and scraped_data.get('company_name') != 'N/A':
                        # Save to database immediately
                        try:
                            new_data = ScrapedData(
                                company_name=scraped_data.get('company_name', 'N/A'),
                                email=scraped_data.get('email', 'N/A'),
                                phone=scraped_data.get('phone', 'N/A'),
                                address=scraped_data.get('address', 'N/A'),
                                website_url=business_url,
                                user_id=user_id
                            )
                            db.session.add(new_data)
                            db.session.commit()
                            
                            # Yield this result immediately
                            yield {
                                'type': 'result',
                                'data': {
                                    'id': new_data.id,
                                    'company_name': scraped_data.get('company_name', 'N/A'),
                                    'email': scraped_data.get('email', 'N/A'),
                                    'phone': scraped_data.get('phone', 'N/A'),
                                    'address': scraped_data.get('address', 'N/A'),
                                    'website_url': business_url,
                                    'created_at': new_data.created_at.strftime('%Y-%m-%d %H:%M:%S')
                                },
                                'progress': {
                                    'current': index,
                                    'total': total_count
                                }
                            }
                            
                        except Exception as db_error:
                            logging.error(f"Database error: {str(db_error)}")
                            db.session.rollback()
                            yield {
                                'type': 'error',
                                'error': f"Database error for {business_name}: {str(db_error)}",
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
                    logging.error(f"Error scraping {business_url}: {str(e)}")
                    yield {
                        'type': 'error',
                        'error': str(e),
                        'business_name': business_name,
                        'url': business_url
                    }
                
                # Small delay between scrapes
                if index < total_count:
                    time.sleep(1.5)
                    
        except Exception as e:
            logging.error(f"Fatal error in streaming: {str(e)}")
            yield {'type': 'error', 'error': f"Fatal error: {str(e)}"}
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
    
    def __del__(self):
        """Cleanup driver on object destruction"""
        if hasattr(self, 'driver') and self.driver:
            self.driver.quit()
            logging.info("GoogleMapsSearchScraper driver closed in destructor")