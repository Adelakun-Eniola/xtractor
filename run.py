# from app import create_app, db
# import os

# app = create_app()

# @app.before_first_request
# def create_tables():
#     db.create_all()

# if __name__ == '__main__':

#     port = int(os.environ.get("PORT", 5000))  # use Render’s PORT if available
#     app.run(host="0.0.0.0", port=port)


import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from app import create_app

app = create_app()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') == 'development'
    
    print(f"🚀 Starting Flask Application on port {port}...")
    
    app.run(
        host='0.0.0.0',
        port=port,
        debug=debug
    )