import psycopg2
from psycopg2 import sql
from config import DB_CONFIG

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
        
        # Create database if it doesn't exist
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier(DB_CONFIG['dbname'])
        ))
        print(f"Database {DB_CONFIG['dbname']} created successfully")
        cursor.close()
        conn.close()
        
        # Now connect to our new database to create tables
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
            provider_id VARCHAR(255) REFERENCES providers(provider_id),
            location_id VARCHAR(255),
            block_geoid VARCHAR(20) REFERENCES census_blocks(geoid),
            technology VARCHAR(100),
            max_advertised_download_speed DECIMAL(10, 2),
            max_advertised_upload_speed DECIMAL(10, 2),
            low_latency BOOLEAN,
            business_residential_code INTEGER,
            state_usps VARCHAR(2),
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
        
    except Exception as e:
        print(f"Error creating database: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    create_database()