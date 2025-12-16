from flask import Flask, jsonify
from flask_cors import CORS
from flask_jwt_extended import JWTManager
import psycopg2
from datetime import timedelta
import os
from dotenv import load_dotenv
import logging

# Load env explicitly from backend directory
basedir = os.path.abspath(os.path.dirname(__file__))
env_path = os.path.join(basedir, '..', '.env')
load_dotenv(env_path)

def create_app():
    app = Flask(__name__)
    
    # Configuration
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')
    app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY', 'dev-jwt-secret')
    
    # Supabase PostgreSQL Configuration
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        # Fallback for development
        database_url = 'postgresql://postgres:password@localhost:5432/extractor_db'
        logging.warning("DATABASE_URL not set, using local PostgreSQL")
    
    # Strip whitespace/newlines from database_url if it exists
    if database_url:
        database_url = database_url.strip()
    
    # Store database URL in app config
    app.config['DATABASE_URL'] = database_url
    
    # CORS Configuration
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
    
    # Initialize JWT
    jwt = JWTManager(app)
    
    # JWT Error Handlers
    @jwt.invalid_token_loader
    def invalid_token_callback(error):
        return jsonify({'error': 'Invalid token', 'details': str(error)}), 422
    
    @jwt.unauthorized_loader
    def unauthorized_callback(error):
        return jsonify({'error': 'Missing or invalid token', 'details': str(error)}), 401
    
    @jwt.expired_token_loader
    def expired_token_callback(jwt_header, jwt_payload):
        return jsonify({'error': 'Token has expired', 'details': 'Please log in again'}), 401
    
    # Initialize PostgreSQL Database
    try:
        # Test connection
        conn = psycopg2.connect(database_url)
        conn.close()
        print("✅ Supabase PostgreSQL connection successful!")
        
        # Create tables
        with app.app_context():
            create_tables()
        
        app.config['DB_CONNECTED'] = True
        
    except Exception as e:
        print(f"⚠️ PostgreSQL connection warning: {e}")
        print("App will start, but database features may not work")
        app.config['DB_CONNECTED'] = False
    
    # Register Blueprints
    from app.routes.auth import auth_bp
    from app.routes.scraper_pg import scraper_bp  # Use PostgreSQL version
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
            if app.config.get('DB_CONNECTED'):
                conn = psycopg2.connect(app.config['DATABASE_URL'])
                conn.close()
                return jsonify({'status': 'healthy', 'database': 'connected'}), 200
            else:
                return jsonify({'status': 'unhealthy', 'database': 'not initialized'}), 500
        except Exception as e:
            return jsonify({'status': 'unhealthy', 'database': 'error', 'error': str(e)}), 500
    
    return app

def create_tables():
    """Create PostgreSQL tables."""
    try:
        from app.models.user_pg import User
        from app.models.scraped_data_pg import ScrapedData
        
        User.create_tables()
        ScrapedData.create_tables()
        
        print("✅ PostgreSQL tables created successfully")
    except Exception as e:
        print(f"⚠️ Error creating PostgreSQL tables: {e}")