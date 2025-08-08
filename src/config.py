import os
from pathlib import Path

# Database configuration
DB_CONFIG = {
    'dbname': 'broadband_db',
    'user': 'postgres',
    'password': 'p3lg3nwork;',
    'host': 'localhost'
}

# Data directory configuration
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
FCC_DATA_DIR = DATA_DIR / 'fcc_data'
CENSUS_DATA_DIR = DATA_DIR / 'census_blocks'

# Map configuration
MAP_CENTER = [41.5801, -71.4774] # Default center
MAP_ZOOM = 11