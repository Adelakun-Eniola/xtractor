import os
from dotenv import load_dotenv  # Add this
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Load environment variables from .env file
load_dotenv()  # Add this

# Get connection string from environment
mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/extractor_db')

if not mongo_uri or mongo_uri == 'mongodb://localhost:27017/extractor_db':
    print("⚠️  Using default MongoDB URI. Check if .env file exists with MONGO_URI")
    print("Current working directory:", os.getcwd())
    print("Files in directory:", os.listdir('.'))

print(f"Connecting to: {mongo_uri[:50]}..." if len(mongo_uri) > 50 else f"Connecting to: {mongo_uri}")

try:
    # Test connection
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    
    # Test the connection
    client.admin.command('ping')
    print("✅ MongoDB connection successful!")
    
    # List databases
    print("📦 Available databases:")
    for db in client.list_databases():
        print(f"  - {db['name']}")
    
    # Create test collection
    db = client.extractor_db
    test_collection = db.test_collection
    
    # Insert test document
    test_doc = {'test': 'Hello MongoDB!', 'timestamp': 'Now'}
    result = test_collection.insert_one(test_doc)
    print(f"✅ Test document inserted with ID: {result.inserted_id}")
    
    # Count documents
    count = test_collection.count_documents({})
    print(f"📊 Documents in test collection: {count}")
    
    # Show all documents
    print("📝 All documents in test collection:")
    for doc in test_collection.find():
        print(f"  - {doc}")
    
    # Clean up
    test_collection.delete_many({})
    print("🧹 Test documents cleaned up")
    
except ConnectionFailure as e:
    print(f"❌ MongoDB connection failed: {e}")
    print("\n💡 Possible solutions:")
    print("1. Check if .env file exists in the same directory")
    print("2. Check if MONGO_URI is set in .env file")
    print("3. Format should be: MONGO_URI=mongodb+srv://username:password@cluster.mongodb.net/dbname")
except Exception as e:
    print(f"⚠️ Error: {e}")
    import traceback
    traceback.print_exc()

