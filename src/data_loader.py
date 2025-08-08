import pandas as pd
import psycopg2
import pyzipper
import geopandas as gpd
from pathlib import Path
from config import DB_CONFIG, FCC_DATA_DIR, CENSUS_DATA_DIR
from typing import List, Dict, Optional
import csv
import io

class DataLoader:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
    
    def load_all_data(self):
        """Load all data including census blocks and FCC data"""
        self.load_census_blocks()
        self.load_fcc_data()
    
    def load_census_blocks(self):
        """Load census block shapefile into database"""
        try:
            shapefile_path = next(CENSUS_DATA_DIR.glob('*.zip'))
            
            with pyzipper.AESZipFile(shapefile_path) as zf:
                # Find the .shp file in the zip
                shp_file = next(f for f in zf.namelist() if f.endswith('.shp'))
                
                # Extract to memory
                with zf.open(shp_file) as f:
                    # Read into GeoDataFrame
                    gdf = gpd.read_file(f)
                    
                    # Filter and rename columns
                    gdf = gdf[['GEOID20', 'geometry']].rename(columns={'GEOID20': 'geoid'})
                    
                    # Save to database
                    with self.conn.cursor() as cursor:
                        # Clear existing data
                        cursor.execute("TRUNCATE census_blocks")
                        
                        # Insert new data
                        for _, row in gdf.iterrows():
                            cursor.execute("""
                            INSERT INTO census_blocks (geoid, geometry)
                            VALUES (%s, ST_GeomFromText(%s, 4326))
                            ON CONFLICT (geoid) DO NOTHING
                            """, (row['geoid'], row['geometry'].wkt))
                        
                        self.conn.commit()
                        print(f"Loaded {len(gdf)} census blocks")
                        
        except Exception as e:
            print(f"Error loading census blocks: {e}")
            self.conn.rollback()
            raise
    
    def load_fcc_data(self):
        """Load all FCC broadband data files"""
        for file in FCC_DATA_DIR.glob('bdc_*.zip'):
            tech_type = self._extract_tech_type(file.name)
            if tech_type:
                print(f"Loading {tech_type} data from {file.name}")
                self.load_tech_data(file, tech_type)
    
    def _extract_tech_type(self, filename: str) -> Optional[str]:
        """Extract technology type from filename"""
        tech_map = {
            'Cable': 'Cable',
            'Copper': 'Copper',
            'FibertothePremises': 'Fiber',
            'LicensedFixedWireless': 'Fixed Wireless',
            'UnlicensedFixedWireless': 'Fixed Wireless',
            'GSOSatellite': 'Satellite',
            'NGSOSatellite': 'Satellite'
        }
        
        for key in tech_map:
            if key in filename:
                return tech_map[key]
        return None
    
    def load_tech_data(self, file_path: Path, tech_type: str):
        """Load data from a single technology file"""
        try:
            with pyzipper.AESZipFile(file_path) as zf:
                # Find the CSV file in the zip
                csv_file = next(f for f in zf.namelist() if f.endswith('.csv'))
                
                with zf.open(csv_file) as f:
                    # Read CSV into DataFrame
                    csv_text = io.TextIOWrapper(f, encoding='utf-8')
                    df = pd.read_csv(csv_text)
                    
                    # Process data
                    providers = self._process_providers(df)
                    self._save_providers(providers)
                    
                    broadband_data = self._process_broadband_data(df, tech_type)
                    self._save_broadband_data(broadband_data)
                    
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            self.conn.rollback()
            raise
    
    def _process_providers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process provider data"""
        # Get unique providers
        providers = df[['provider_id', 'brand_name']].drop_duplicates()
        return providers
    
    def _process_broadband_data(self, df: pd.DataFrame, tech_type: str) -> pd.DataFrame:
        """Process broadband data"""
        # Select and rename columns
        data = df[[
            'provider_id', 'location_id', 'block_geoid', 
            'max_advertised_download_speed', 'max_advertised_upload_speed',
            'low_latency', 'business_residential_code', 'state_usps', 'h3_res8_id'
        ]].copy()
        
        # Add technology type
        data['technology'] = tech_type
        
        return data
    
    def _save_providers(self, providers_df: pd.DataFrame):
        """Save providers to database"""
        if providers_df.empty:
            return
            
        with self.conn.cursor() as cursor:
            # Create temp table
            cursor.execute("""
            CREATE TEMP TABLE temp_providers (
                provider_id VARCHAR(255),
                brand_name VARCHAR(255)
            ) ON COMMIT DROP
            """)
            
            # Bulk insert to temp table
            args = [tuple(x) for x in providers_df.to_numpy()]
            cursor.executemany("""
            INSERT INTO temp_providers VALUES (%s, %s)
            """, args)
            
            # Merge with main table
            cursor.execute("""
            INSERT INTO providers 
            SELECT * FROM temp_providers
            ON CONFLICT (provider_id) DO UPDATE
            SET brand_name = EXCLUDED.brand_name
            """)
            
            self.conn.commit()
            print(f"Saved {len(providers_df)} providers")
    
    def _save_broadband_data(self, broadband_df: pd.DataFrame):
        """Save broadband data to database"""
        if broadband_df.empty:
            return
            
        with self.conn.cursor() as cursor:
            # Create temp table
            cursor.execute("""
            CREATE TEMP TABLE temp_broadband (
                provider_id VARCHAR(255),
                location_id VARCHAR(255),
                block_geoid VARCHAR(20),
                technology VARCHAR(100),
                max_advertised_download_speed DECIMAL(10, 2),
                max_advertised_upload_speed DECIMAL(10, 2),
                low_latency BOOLEAN,
                business_residential_code INTEGER,
                state_usps VARCHAR(2),
                h3_res8_id VARCHAR(255)
            ) ON COMMIT DROP
            """)
            
            # Bulk insert to temp table
            args = [tuple(x) for x in broadband_df.to_numpy()]
            cursor.executemany("""
            INSERT INTO temp_broadband VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, args)
            
            # Merge with main table
            cursor.execute("""
            INSERT INTO broadband_data (
                provider_id, location_id, block_geoid, technology,
                max_advertised_download_speed, max_advertised_upload_speed,
                low_latency, business_residential_code, state_usps, h3_res8_id
            )
            SELECT * FROM temp_broadband
            ON CONFLICT (provider_id, location_id, block_geoid) DO NOTHING
            """)
            
            self.conn.commit()
            print(f"Saved {len(broadband_df)} broadband records")
    
    def close(self):
        self.conn.close()

if __name__ == "__main__":
    loader = DataLoader()
    loader.load_all_data()
    loader.close()