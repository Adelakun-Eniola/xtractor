
import os
import logging
from datetime import datetime
from pymongo import MongoClient, DESCENDING
from bson import ObjectId
from flask import current_app

logger = logging.getLogger(__name__)

class ScrapedData:
    """MongoDB model for scraped business data."""
    
    # Collection name
    COLLECTION_NAME = 'scraped_data'
    
    @classmethod
    def get_db(cls):
        """Use the database stored in Flask app config."""
        try:
            if current_app:
                db = current_app.config.get("MONGO_DB")
                if db is not None:
                    return db

            raise Exception("MongoDB not initialized in current_app.config['MONGO_DB']")
        
        except Exception as e:
            logger.error(f"Failed to get MongoDB instance: {e}")
            raise

    
    @classmethod
    def get_collection(cls):
        """Get the collection for scraped data."""
        db = cls.get_db()
        return db[cls.COLLECTION_NAME]
    
    @classmethod
    def create(cls, data):
        """Create a new scraped data document."""
        try:
            collection = cls.get_collection()
            
            # Add timestamps
            data['created_at'] = datetime.utcnow()
            data['updated_at'] = datetime.utcnow()
            
            # Insert document
            result = collection.insert_one(data)
            
            logger.info(f"Inserted document with ID: {result.inserted_id}")
            return str(result.inserted_id)
            
        except Exception as e:
            logger.error(f"Error creating document: {e}")
            raise
    
    @classmethod
    def find_by_id(cls, document_id):
        """Find a document by its ID."""
        try:
            collection = cls.get_collection()
            document = collection.find_one({'_id': ObjectId(document_id)})
            
            if document:
                # Convert ObjectId to string for JSON serialization
                document['_id'] = str(document['_id'])
            
            return document
            
        except Exception as e:
            logger.error(f"Error finding document {document_id}: {e}")
            return None
    
    @classmethod
    def find_by_user_id(cls, user_id, limit=50, skip=0):
        """Find all documents for a specific user."""
        try:
            collection = cls.get_collection()
            
            cursor = collection.find(
                {'user_id': user_id}
            ).sort('created_at', DESCENDING).skip(skip).limit(limit)
            
            documents = list(cursor)
            
            # Convert ObjectIds to strings
            for doc in documents:
                doc['_id'] = str(doc['_id'])
            
            return documents
            
        except Exception as e:
            logger.error(f"Error finding documents for user {user_id}: {e}")
            return []
    
    @classmethod
    def count_by_user_id(cls, user_id):
        """Count documents for a specific user."""
        try:
            collection = cls.get_collection()
            return collection.count_documents({'user_id': user_id})
        except Exception as e:
            logger.error(f"Error counting documents for user {user_id}: {e}")
            return 0
    
    @classmethod
    def delete_by_id(cls, document_id, user_id):
        """Delete a document if it belongs to the user."""
        try:
            collection = cls.get_collection()
            result = collection.delete_one({
                '_id': ObjectId(document_id),
                'user_id': user_id
            })
            
            return result.deleted_count > 0
            
        except Exception as e:
            logger.error(f"Error deleting document {document_id}: {e}")
            return False
    
    @classmethod
    def update(cls, document_id, user_id, updates):
        """Update a document."""
        try:
            collection = cls.get_collection()
            
            # Don't allow updating protected fields
            if '_id' in updates:
                del updates['_id']
            if 'user_id' in updates:
                del updates['user_id']
            
            updates['updated_at'] = datetime.utcnow()
            
            result = collection.update_one(
                {'_id': ObjectId(document_id), 'user_id': user_id},
                {'$set': updates}
            )
            
            return result.modified_count > 0
            
        except Exception as e:
            logger.error(f"Error updating document {document_id}: {e}")
            return False
    
    @classmethod
    def find_by_user(cls, user_id, page=1, per_page=20):
        """Find all documents for a specific user with pagination."""
        try:
            collection = cls.get_collection()
            
            # Calculate skip value
            skip = (page - 1) * per_page
            
            # Get total count
            total = collection.count_documents({'user_id': user_id})
            
            # Get documents with pagination
            cursor = collection.find(
                {'user_id': user_id}
            ).sort('created_at', DESCENDING).skip(skip).limit(per_page)
            
            documents = list(cursor)
            
            # Convert ObjectIds to strings
            for doc in documents:
                doc['_id'] = str(doc['_id'])
            
            # Calculate pagination info
            total_pages = (total + per_page - 1) // per_page
            
            return {
                'data': documents,
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total': total,
                    'pages': total_pages
                }
            }
            
        except Exception as e:
            logger.error(f"Error finding documents for user {user_id}: {e}")
            return {
                'data': [],
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total': 0,
                    'pages': 0
                }
            }
    
    @classmethod
    def get_stats(cls, user_id):
        """Get statistics for a user's scraped data."""
        try:
            collection = cls.get_collection()
            
            # Use aggregation pipeline for stats
            pipeline = [
                {'$match': {'user_id': user_id}},
                {'$group': {
                    '_id': None,
                    'total': {'$sum': 1},
                    'with_email': {
                        '$sum': {
                            '$cond': [
                                {'$and': [
                                    {'$ne': ['$email', None]},
                                    {'$ne': ['$email', 'N/A']},
                                    {'$ne': ['$email', 'Not found']},
                                    {'$ne': ['$email', '']}
                                ]},
                                1, 0
                            ]
                        }
                    },
                    'with_phone': {
                        '$sum': {
                            '$cond': [
                                {'$and': [
                                    {'$ne': ['$phone', None]},
                                    {'$ne': ['$phone', 'N/A']},
                                    {'$ne': ['$phone', 'Not found']},
                                    {'$ne': ['$phone', '']}
                                ]},
                                1, 0
                            ]
                        }
                    },
                    'with_address': {
                        '$sum': {
                            '$cond': [
                                {'$and': [
                                    {'$ne': ['$address', None]},
                                    {'$ne': ['$address', 'N/A']},
                                    {'$ne': ['$address', 'Not found']},
                                    {'$ne': ['$address', '']}
                                ]},
                                1, 0
                            ]
                        }
                    }
                }}
            ]
            
            result = list(collection.aggregate(pipeline))
            
            if result:
                stats = result[0]
                return {
                    'total': stats.get('total', 0),
                    'with_email': stats.get('with_email', 0),
                    'with_phone': stats.get('with_phone', 0),
                    'with_address': stats.get('with_address', 0)
                }
            else:
                return {
                    'total': 0,
                    'with_email': 0,
                    'with_phone': 0,
                    'with_address': 0
                }
                
        except Exception as e:
            logger.error(f"Error getting stats for user {user_id}: {e}")
            return {
                'total': 0,
                'with_email': 0,
                'with_phone': 0,
                'with_address': 0
            }
    
    @classmethod
    def search(cls, user_id, search_term):
        """Search documents for a user."""
        try:
            collection = cls.get_collection()
            
            # Create text search query
            query = {
                'user_id': user_id,
                '$or': [
                    {'company_name': {'$regex': search_term, '$options': 'i'}},
                    {'email': {'$regex': search_term, '$options': 'i'}},
                    {'phone': {'$regex': search_term, '$options': 'i'}},
                    {'address': {'$regex': search_term, '$options': 'i'}},
                    {'website_url': {'$regex': search_term, '$options': 'i'}}
                ]
            }
            
            cursor = collection.find(query).sort('created_at', DESCENDING).limit(50)
            documents = list(cursor)
            
            # Convert ObjectIds to strings
            for doc in documents:
                doc['_id'] = str(doc['_id'])
            
            return documents
            
        except Exception as e:
            logger.error(f"Error searching for user {user_id}: {e}")
            return []
    
    @classmethod
    def delete(cls, document_id):
        """Delete a document by ID."""
        try:
            collection = cls.get_collection()
            result = collection.delete_one({'_id': ObjectId(document_id)})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error deleting document {document_id}: {e}")
            return False
    
    @classmethod
    def delete_by_user(cls, user_id):
        """Delete all documents for a user."""
        try:
            collection = cls.get_collection()
            result = collection.delete_many({'user_id': user_id})
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error deleting documents for user {user_id}: {e}")
            return 0
    
    @classmethod
    def create_indexes(cls):
        """Create necessary indexes for optimal querying."""
        try:
            collection = cls.get_collection()
            
            # Create indexes
            collection.create_index([('user_id', 1)])
            collection.create_index([('created_at', -1)])
            collection.create_index([('company_name', 1)])
            collection.create_index([('user_id', 1), ('created_at', -1)])
            
            logger.info("Created indexes for scraped_data collection")
            return True
            
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
            return False