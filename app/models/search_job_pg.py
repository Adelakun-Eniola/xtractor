from datetime import datetime
from flask import current_app
import psycopg2
from psycopg2.extras import RealDictCursor, Json
import logging
import json

class SearchJob:
    """PostgreSQL SearchJob model for tracking chunked scraping progress"""

    @staticmethod
    def get_connection():
        """Return a PostgreSQL connection"""
        database_url = current_app.config.get('DATABASE_URL')
        if not database_url:
            raise Exception("DATABASE_URL not configured")
        return psycopg2.connect(database_url)

    @classmethod
    def create_tables(cls):
        """Create the search_jobs table if it doesn't exist"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor()
            
            # Create search_jobs table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS search_jobs (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    search_url TEXT NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending', -- pending, active, completed, failed
                    params JSONB DEFAULT '{}',
                    total_items INTEGER DEFAULT 0,
                    processed_items INTEGER DEFAULT 0,
                    items JSONB DEFAULT '[]', -- List of {name, url, status}
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_search_jobs_user_id 
                ON search_jobs(user_id)
            """)
            
            conn.commit()
            cur.close()
            conn.close()
            logging.info("SearchJob table created successfully")
            
        except Exception as e:
            logging.error(f"Error creating search_jobs table: {e}")
            raise

    @classmethod
    def create(cls, data):
        """Create a new search job"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("""
                INSERT INTO search_jobs 
                (user_id, search_url, status, items, total_items)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (
                data.get('user_id'),
                data.get('search_url'),
                'pending',
                Json(data.get('items', [])),
                data.get('total_items', 0)
            ))
            
            result = cur.fetchone()
            conn.commit()
            cur.close()
            conn.close()
            
            return result['id']
            
        except Exception as e:
            logging.error(f"Error creating search job: {e}")
            raise

    @classmethod
    def find_by_id(cls, job_id, user_id):
        """Find a job by ID and User ID"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("""
                SELECT * FROM search_jobs 
                WHERE id = %s AND user_id = %s
            """, (job_id, user_id))
            
            result = cur.fetchone()
            cur.close()
            conn.close()
            
            return dict(result) if result else None
            
        except Exception as e:
            logging.error(f"Error finding search job: {e}")
            return None

    @classmethod
    def update_progress(cls, job_id, processed_items, items, status=None):
        """Update job progress"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor()
            
            # Prepare update query
            updates = [
                "processed_items = %s",
                "items = %s",
                "updated_at = NOW()"
            ]
            params = [processed_items, Json(items)]
            
            if status:
                updates.append("status = %s")
                params.append(status)
                
            params.append(job_id)
            
            query = f"""
                UPDATE search_jobs 
                SET {', '.join(updates)}
                WHERE id = %s
            """
            
            cur.execute(query, tuple(params))
            conn.commit()
            cur.close()
            conn.close()
            
        except Exception as e:
            logging.error(f"Error updating search job progress: {e}")
            raise
