from flask import Flask, jsonify
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from datetime import timedelta
import os
from dotenv import load_dotenv
import logging

load_dotenv()

def create_app():
    app = Flask(__name__)
    
    # Configuration
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')
    app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY', 'dev-jwt-secret')
    
    # MongoDB Atlas Configuration
    mongo_uri = os.getenv('MONGO_URI')
    
    if not mongo_uri:
        # Fallback for development
        mongo_uri = 'mongodb://localhost:27017/extractor_db'
        logging.warning("MONGO_URI not set, using local MongoDB")
    
    # CORS Configuration (keep as is)
    allowed_origins = [
        "https://xtract-indol.vercel.app",
        "http://localhost:3000",
        "http://127.0.0.1:3000"
    ]
    CORS(app, resources={r"/api/*": {
        "origins": allowed_origins,
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"],
        "supports_credentials": True
    }})
    
    # Initialize JWT (keep as is)
    jwt = JWTManager(app)
    
    # JWT Error Handlers (keep as is)
    @jwt.invalid_token_loader
    def invalid_token_callback(error):
        return jsonify({'error': 'Invalid token', 'details': str(error)}), 422
    
    @jwt.unauthorized_loader
    def unauthorized_callback(error):
        return jsonify({'error': 'Missing or invalid token', 'details': str(error)}), 401
    
    @jwt.expired_token_loader
    def expired_token_callback(jwt_header, jwt_payload):
        return jsonify({'error': 'Token has expired', 'details': 'Please log in again'}), 401
    
    # Initialize MongoDB DIRECTLY
    try:
        # Create MongoDB client
        client = MongoClient(
            mongo_uri,
            maxPoolSize=100,
            serverSelectionTimeoutMS=30000
        )
        
        # Test connection
        client.admin.command('ping')
        print("✅ MongoDB Atlas connection successful!")
        
        # Store the client and db in app config
        db_name = mongo_uri.split('/')[-1].split('?')[0]
        app.config['MONGO_CLIENT'] = client
        app.config['MONGO_DB'] = client[db_name]
        
        # Create indexes
        create_indexes(client[db_name])
        
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"⚠️ MongoDB connection warning: {e}")
        print("App will start, but MongoDB features may not work")
        app.config['MONGO_CLIENT'] = None
        app.config['MONGO_DB'] = None
    except Exception as e:
        print(f"⚠️ MongoDB initialization error: {e}")
        app.config['MONGO_CLIENT'] = None
        app.config['MONGO_DB'] = None
    
    # Register Blueprints (keep as is)
    from app.routes.auth import auth_bp
    from app.routes.scraper import scraper_bp
    from app.routes.dashboard import dashboard_bp
    from app.routes.debug import debug_bp
    
    app.register_blueprint(auth_bp)
    app.register_blueprint(scraper_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(debug_bp, url_prefix='/api')
    
    # Health check endpoint
    @app.route('/api/health', methods=['GET'])
    def health_check():
        try:
            client = app.config['MONGO_CLIENT']
            if client:
                client.admin.command('ping')
                return jsonify({'status': 'healthy', 'database': 'connected'}), 200
            else:
                return jsonify({'status': 'unhealthy', 'database': 'not initialized'}), 500
        except Exception as e:
            return jsonify({'status': 'unhealthy', 'database': 'error', 'error': str(e)}), 500
    
    return app

def create_indexes(db):
    """Create MongoDB indexes."""
    try:
        # Index for users
        db.users.create_index([('email', 1)], unique=True)
        
        # Indexes for scraped_data
        db.scraped_data.create_index([('user_id', 1)])
        db.scraped_data.create_index([('created_at', -1)])
        db.scraped_data.create_index([('user_id', 1), ('created_at', -1)])
        db.scraped_data.create_index([('company_name', 'text')])
        
        print("✅ MongoDB indexes created successfully")
    except Exception as e:
        print(f"⚠️ Error creating MongoDB indexes: {e}")