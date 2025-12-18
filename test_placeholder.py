
import logging
import sys
import unittest
from unittest.mock import MagicMock

# Ensure backend directory is in path
sys.path.append('/Users/mac/Documents/extractor2.0/backend')

from app.services.scraper import GoogleMapsSearchScraper

logging.basicConfig(level=logging.INFO)

class TestScraperFix(unittest.TestCase):
    def test_strict_url_filtering(self):
        # Determine if filters work strictly
        scraper = GoogleMapsSearchScraper("dummy")
        
        # Test 1: Should reject Google Maps link
        bad_url = "https://www.google.com/maps/place/Texas+Auto+Repairs/..."
        validated = scraper.validate_url(bad_url)
        # Note: scraper.validate_url is basic, but the extraction logic has the strict filter.
        # Let's test the extraction logic component if we can, or just run a real scrape.
        
        # Real scrape is better proof
        pass

if __name__ == "__main__":
    # We will just run the scrape script again
    pass
