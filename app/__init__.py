from flask import Flask, jsonify
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from flask_sqlalchemy import SQLAlchemy
from datetime import timedelta
import os
from dotenv import load_dotenv

load_dotenv()

db = SQLAlchemy()

def create_app():
    app = Flask(__name__)
    
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')
    app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY', 'dev-jwt-secret')
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'sqlite:///extractor.db')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(hours=12)
    
    CORS(app, resources={r"/api/*": {"origins": "https://xtract-indol.vercel.apps"}})  # Allow frontend origin in production
    jwt = JWTManager(app)
    
    @jwt.invalid_token_loader
    def invalid_token_callback(error):
        print(f"JWT Invalid Token Error: {error}")
        return jsonify({'error': 'Invalid token', 'details': str(error)}), 422
    
    @jwt.unauthorized_loader
    def unauthorized_callback(error):
        print(f"JWT Unauthorized Error: {error}")
        return jsonify({'error': 'Missing or invalid token', 'details': str(error)}), 401
    
    @jwt.expired_token_loader
    def expired_token_callback(jwt_header, jwt_payload):
        print(f"JWT Expired Token: {jwt_payload}")
        return jsonify({'error': 'Token has expired', 'details': 'Please log in again'}), 401
    
    db.init_app(app)
    
    with app.app_context():
        db.create_all()  # Create tables if they don't exist
    
    from app.routes.auth import auth_bp
    from app.routes.scraper import scraper_bp
    from app.routes.dashboard import dashboard_bp
    
    app.register_blueprint(auth_bp)
    app.register_blueprint(scraper_bp)
    app.register_blueprint(dashboard_bp)
    
    return app