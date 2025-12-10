import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # MongoDB Atlas Configuration
    MONGO_URI = os.environ.get('MONGO_URI')
    if not MONGO_URI:
        raise ValueError("MONGO_URI environment variable is required")
    
    # Optional: Separate database name
    MONGO_DB_NAME = os.environ.get('MONGO_DB_NAME', 'scraper_db')
    
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'your-secret-key-here'
    
    # MongoDB connection options
    MONGO_CONNECT = False  # Lazy connection
    MONGO_MAX_POOL_SIZE = 100
    MONGO_MIN_POOL_SIZE = 10
    MONGO_SOCKET_TIMEOUT_MS = 30000
    MONGO_CONNECT_TIMEOUT_MS = 20000