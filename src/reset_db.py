import psycopg2
from config import DB_CONFIG

def reset_database():
    conn = psycopg2.connect(
        dbname='postgres',
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host']
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    try:
        # Drop existing database if it exists
        cursor.execute(f"DROP DATABASE IF EXISTS {DB_CONFIG['dbname']}")
        
        # Create fresh database
        cursor.execute(f"CREATE DATABASE {DB_CONFIG['dbname']}")
        print(f"Database {DB_CONFIG['dbname']} recreated successfully")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    reset_database()