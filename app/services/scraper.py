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