# src/database.py
import psycopg2
from psycopg2 import sql, errors
from config import DB_CONFIG
import sys

def create_database():
    """Create the PostgreSQL database and tables with PostGIS extension"""
    conn = None
    try:
        # Connect to default database to create our database
        conn = psycopg2.connect(
            dbname='postgres',
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host']
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", 
                      (DB_CONFIG['dbname'],))
        exists = cursor.fetchone()
        
        if not exists:
            # Create database if it doesn't exist
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(DB_CONFIG['dbname'])
            ))
            print(f"Database {DB_CONFIG['dbname']} created successfully")
        else:
            print(f"Database {DB_CONFIG['dbname']} already exists")
            
        cursor.close()
        conn.close()
        
        # Now connect to our database to create tables
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Enable PostGIS extension
        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        
        # Create providers table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS providers (
            provider_id VARCHAR(255) PRIMARY KEY,
            brand_name VARCHAR(255),
            UNIQUE(provider_id)
        );
        """)
        
        # Create census blocks table with geometry
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS census_blocks (
            geoid VARCHAR(20) PRIMARY KEY,
            geometry GEOMETRY(MULTIPOLYGON, 4326)
        );
        """)
        
        # Create broadband data table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS broadband_data (
            id SERIAL PRIMARY KEY,
            frn BIGINT,
            provider_id BIGINT,
            brand_name VARCHAR(255),
            location_id BIGINT,
            technology INTEGER,
            max_advertised_download_speed INTEGER,
            max_advertised_upload_speed INTEGER,
            low_latency BOOLEAN,
            business_residential_code VARCHAR(255),
            state_usps VARCHAR(2),
            block_geoid BIGINT,
            h3_res8_id VARCHAR(255),
            UNIQUE(provider_id, location_id, block_geoid)
        );
        """)
        
        # Create spatial index for faster queries
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS census_blocks_geometry_idx 
        ON census_blocks USING GIST(geometry);
        """)
        
        conn.commit()
        print("Tables created successfully")
        
    except errors.DuplicateDatabase:
        print("Database already exists - continuing")
    except Exception as e:
        print(f"Error creating database: {e}")
        sys.exit(1)
    finally:
        if conn:
            cursor.close()
            conn.close()

def verify_tables_exist():
    """Verify that all required tables exist"""
    required_tables = ['providers', 'census_blocks', 'broadband_data']
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        for table in required_tables:
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
            """, (table,))
            exists = cursor.fetchone()[0]
            if not exists:
                print(f"Error: Table '{table}' does not exist")
                return False
                
        return True
    except Exception as e:
        print(f"Error verifying tables: {e}")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    create_database()
    if verify_tables_exist():
        print("Database setup complete - all tables exist")
    else:
        print("Database setup encountered issues")
        sys.exit(1)