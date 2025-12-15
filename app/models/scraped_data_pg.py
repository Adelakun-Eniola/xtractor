from datetime import datetime
from flask import current_app
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

class ScrapedData:
    """PostgreSQL ScrapedData model for Supabase"""

    @staticmethod
    def get_connection():
        """Return a PostgreSQL connection"""
        import os
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            raise Exception("DATABASE_URL not configured")
        return psycopg2.connect(database_url)

    @classmethod
    def create_tables(cls):
        """Create the scraped_data table if it doesn't exist"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor()
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scraped_data (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    company_name VARCHAR(255),
                    email VARCHAR(255),
                    phone VARCHAR(50),
                    address TEXT,
                    website_url TEXT,
                    source_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for better performance
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_scraped_data_user_id 
                ON scraped_data(user_id)
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_scraped_data_created_at 
                ON scraped_data(created_at DESC)
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_scraped_data_company_name 
                ON scraped_data(company_name)
            """)
            
            conn.commit()
            cur.close()
            conn.close()
            logging.info("Scraped data table created successfully")
            
        except Exception as e:
            logging.error(f"Error creating scraped_data table: {e}")
            raise

    @classmethod
    def create(cls, data):
        """Create a new scraped data record"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("""
                INSERT INTO scraped_data 
                (user_id, company_name, email, phone, address, website_url, source_url, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, user_id, company_name, email, phone, address, website_url, source_url, created_at, updated_at
            """, (
                data.get('user_id'),
                data.get('company_name'),
                data.get('email'),
                data.get('phone'),
                data.get('address'),
                data.get('website_url'),
                data.get('source_url'),
                datetime.utcnow(),
                datetime.utcnow()
            ))
            
            result = dict(cur.fetchone())
            conn.commit()
            cur.close()
            conn.close()
            
            logging.info(f"Created scraped data record with ID: {result['id']}")
            return result['id']
            
        except Exception as e:
            logging.error(f"Error creating scraped data: {e}")
            raise

    @classmethod
    def find_by_id(cls, record_id):
        """Find scraped data by ID"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("SELECT * FROM scraped_data WHERE id = %s", (record_id,))
            result = cur.fetchone()
            
            cur.close()
            conn.close()
            
            return dict(result) if result else None
            
        except Exception as e:
            logging.error(f"Error finding scraped data by ID: {e}")
            return None

    @classmethod
    def find_by_user_id(cls, user_id, limit=50, offset=0):
        """Find all scraped data for a user"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("""
                SELECT * FROM scraped_data 
                WHERE user_id = %s 
                ORDER BY created_at DESC 
                LIMIT %s OFFSET %s
            """, (user_id, limit, offset))
            
            results = cur.fetchall()
            
            cur.close()
            conn.close()
            
            return [dict(row) for row in results]
            
        except Exception as e:
            logging.error(f"Error finding scraped data by user ID: {e}")
            return []

    @classmethod
    def count_by_user_id(cls, user_id):
        """Count total records for a user"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor()
            
            cur.execute("SELECT COUNT(*) FROM scraped_data WHERE user_id = %s", (user_id,))
            count = cur.fetchone()[0]
            
            cur.close()
            conn.close()
            
            return count
            
        except Exception as e:
            logging.error(f"Error counting scraped data: {e}")
            return 0

    @classmethod
    def search_by_user_id(cls, user_id, search_term):
        """Search scraped data for a user"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            search_pattern = f"%{search_term}%"
            cur.execute("""
                SELECT * FROM scraped_data 
                WHERE user_id = %s AND (
                    company_name ILIKE %s OR 
                    email ILIKE %s OR 
                    phone ILIKE %s OR 
                    address ILIKE %s OR 
                    website_url ILIKE %s
                )
                ORDER BY created_at DESC
            """, (user_id, search_pattern, search_pattern, search_pattern, search_pattern, search_pattern))
            
            results = cur.fetchall()
            
            cur.close()
            conn.close()
            
            return [dict(row) for row in results]
            
        except Exception as e:
            logging.error(f"Error searching scraped data: {e}")
            return []

    @classmethod
    def delete_by_id(cls, record_id, user_id):
        """Delete a scraped data record (with user verification)"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor()
            
            cur.execute("""
                DELETE FROM scraped_data 
                WHERE id = %s AND user_id = %s
            """, (record_id, user_id))
            
            deleted_count = cur.rowcount
            conn.commit()
            cur.close()
            conn.close()
            
            return deleted_count > 0
            
        except Exception as e:
            logging.error(f"Error deleting scraped data: {e}")
            return False

    @classmethod
    def delete_all_by_user_id(cls, user_id):
        """Delete all scraped data for a user"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor()
            
            cur.execute("DELETE FROM scraped_data WHERE user_id = %s", (user_id,))
            
            deleted_count = cur.rowcount
            conn.commit()
            cur.close()
            conn.close()
            
            return deleted_count
            
        except Exception as e:
            logging.error(f"Error deleting all scraped data: {e}")
            return 0

    @classmethod
    def get_stats_by_user_id(cls, user_id):
        """Get statistics for a user's scraped data"""
        try:
            conn = cls.get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN email IS NOT NULL AND email != '' THEN 1 END) as with_email,
                    COUNT(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 END) as with_phone,
                    COUNT(CASE WHEN address IS NOT NULL AND address != '' THEN 1 END) as with_address,
                    COUNT(CASE WHEN website_url IS NOT NULL AND website_url != '' THEN 1 END) as with_website,
                    MIN(created_at) as first_scrape,
                    MAX(created_at) as last_scrape
                FROM scraped_data 
                WHERE user_id = %s
            """, (user_id,))
            
            result = cur.fetchone()
            
            cur.close()
            conn.close()
            
            return dict(result) if result else {}
            
        except Exception as e:
            logging.error(f"Error getting scraped data stats: {e}")
            return {}