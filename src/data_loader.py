import pandas as pd
import psycopg2
import pyzipper
import geopandas as gpd
from pathlib import Path
import tempfile
import shutil
import os
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
        """Load census blocks with proper type conversion"""
        try:
            shapefile_path = next(CENSUS_DATA_DIR.glob('*.zip'))
            with pyzipper.AESZipFile(shapefile_path) as zf:
                # Extract to temp directory
                with tempfile.TemporaryDirectory() as temp_dir:
                    zf.extractall(temp_dir)
                    shp_file = next(Path(temp_dir).glob('*.shp'))
                    
                    # Read and convert GEOID20 to BIGINT
                    gdf = gpd.read_file(shp_file)
                    gdf['GEOID20'] = pd.to_numeric(gdf['GEOID20'], errors='coerce').dropna().astype('int64')
                    gdf = gdf[['GEOID20', 'geometry']].rename(columns={'GEOID20': 'geoid'})
                    
                    # Save to database
                    with self.conn.cursor() as cursor:
                        cursor.execute("TRUNCATE broadband_data, census_blocks CASCADE")
                        
                        # Use execute_batch for better performance
                        from psycopg2.extras import execute_batch
                        execute_batch(
                            cursor,
                            """
                            INSERT INTO census_blocks (geoid, geometry)
                            VALUES (%s, ST_GeomFromText(%s, 4326))
                            ON CONFLICT (geoid) DO NOTHING
                            """,
                            [(row['geoid'], row['geometry'].wkt) for _, row in gdf.iterrows()]
                        )
                        self.conn.commit()
                        print(f"Loaded {len(gdf)} census blocks with BIGINT geoids")

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

        providers['provider_id'] = pd.to_numeric(
            providers['provider_id'],
            errors='coerce',
            downcast='integer'
        ).dropna().astype('int64')

        return providers
    
    def _process_broadband_data(self, df: pd.DataFrame, tech_type: str) -> pd.DataFrame:
        """Process broadband data with proper type conversion"""
        # Convert data types
        df = df.astype({
            'frn': 'int64',
            'provider_id': 'int64',
            'brand_name': 'str',
            'location_id': 'int64',
            'technology': 'int64',
            'max_advertised_download_speed': 'int64',
            'max_advertised_upload_speed': 'int64',
            'low_latency': 'int64',  # Will convert to boolean later
            'business_residential_code': 'str',
            'state_usps': 'str',
            'block_geoid': 'int64',
            'h3_res8_id': 'str'
        })
        
        # Convert low_latency to boolean (assuming 1=True, 0=False)
        df['low_latency'] = df['low_latency'].astype(bool)
        
        # Convert business_residential_code (assuming 'R'=Residential, 'B'=Business, 'X'=Mixed Use)
        df['business_residential_code'] = df['business_residential_code'].replace({
            'R': 'Residential',
            'B': 'Business',
            'X': 'Mixed Use (Residential and Business)'
        })
        
        return df[[
            'frn', 'provider_id', 'brand_name', 'location_id', 'technology',
            'max_advertised_download_speed', 'max_advertised_upload_speed',
            'low_latency', 'business_residential_code', 'state_usps',
            'block_geoid', 'h3_res8_id'
        ]]
    
    def _save_providers(self, providers_df: pd.DataFrame):
        """Save with explicit type casting"""
        if providers_df.empty:
            return
            
        with self.conn.cursor() as cursor:
            # Create temp table with explicit types
            cursor.execute("""
            CREATE TEMP TABLE temp_providers (
                provider_id BIGINT,
                brand_name VARCHAR(255)
            ) ON COMMIT DROP
            """)
            
            # Bulk insert with type validation
            records = []
            for _, row in providers_df.iterrows():
                try:
                    records.append((
                        int(row['provider_id']), 
                        str(row['brand_name']) if pd.notna(row['brand_name']) else None
                    ))
                except (ValueError, TypeError) as e:
                    print(f"Skipping invalid provider record: {row} - Error: {e}")
                    continue
            
            # Use execute_values for better performance
            from psycopg2.extras import execute_values
            execute_values(
                cursor,
                "INSERT INTO temp_providers (provider_id, brand_name) VALUES %s",
                records
            )
            
            # Merge with main table
            cursor.execute("""
            INSERT INTO providers
            SELECT provider_id, brand_name FROM temp_providers
            ON CONFLICT (provider_id) DO UPDATE
            SET brand_name = EXCLUDED.brand_name
            """)
            
            self.conn.commit()
            print(f"Saved {len(records)} valid provider records")
    
    def _save_broadband_data(self, broadband_df: pd.DataFrame):
        """Save broadband data with proper type handling"""
        if broadband_df.empty:
            return
            
        with self.conn.cursor() as cursor:
            # Create temp table with matching types
            cursor.execute("""
            CREATE TEMP TABLE temp_broadband (
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
                h3_res8_id VARCHAR(255)
            ) ON COMMIT DROP
            """)
            
            # Bulk insert to temp table
            args = [tuple(x) for x in broadband_df.to_numpy()]
            cursor.executemany("""
            INSERT INTO temp_broadband VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, args)
            
            # Merge with main table
            cursor.execute("""
            INSERT INTO broadband_data (
                frn, provider_id, brand_name, location_id, technology,
                max_advertised_download_speed, max_advertised_upload_speed,
                low_latency, business_residential_code, state_usps,
                block_geoid, h3_res8_id
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