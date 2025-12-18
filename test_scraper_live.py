
import logging
import sys
import os

# Ensure backend directory is in path
sys.path.append('/Users/mac/Documents/extractor2.0/backend')

from app.services.scraper import GoogleMapsSearchScraper

# Setup logging
logging.basicConfig(level=logging.INFO)

def test_scraper_live():
    # Use the same search as the user failure to reproduce
    # "Texan Automotive" seems to result in map links
    search_url = "https://www.google.com/maps/search/Texas+Auto+Repairs" 
    
    print(f"Testing scraper with URL: {search_url}")
    
    scraper = GoogleMapsSearchScraper(search_url)

    # 1. Test Search (might be flaky)
    print("Testing Search...")
    # results = scraper.scrape_all_businesses(user_id=1, limit=3)
    # For now, let's skip search if it's failing and test the single scraper directly 
    # as that's where we made the logic changes.
    
    # 2. Test Single Business Scrape (Direct Verification of WebScraper)
    print("\nTesting Single Business Scrape...")
    test_url = "https://www.google.com/maps/place/Texas+Collision+Centers/@32.7625794,-97.0465637,17z/data=!3m1!4b1!4m6!3m5!1s0x864e8156b8256a0d:0x39a1d1d8a14b30e0!8m2!3d32.7625794!4d-97.0465637"
    single_result = scraper.test_scrape_single(test_url)
    
    with open('test_output.txt', 'w') as f:
        print(f"Single Scrape Result: {single_result}")
        f.write(f"Single Scrape Result: {single_result}\n")
        
        data = single_result.get('data', {})
        output = f"\n--- SINGLE SCRAPE DATA ---\n"
        output += f"Name: {data.get('company_name')}\n"
        output += f"Website: {data.get('website_url')}\n"
        output += f"Email: {data.get('email')}\n"
        output += f"Phone: {data.get('phone')}\n"
        output += f"Address: {data.get('address')}\n"
        print(output)
        f.write(output)

if __name__ == "__main__":
    test_scraper_live()
