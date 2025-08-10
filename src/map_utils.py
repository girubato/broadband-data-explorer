import folium
from folium.plugins import FastMarkerCluster
import psycopg2
from config import DB_CONFIG
from typing import Optional, Dict

class MapBuilder:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
    
    def create_map(self, filters: Optional[Dict] = None) -> folium.Map:
        """Create a Folium map with broadband data"""
        if filters is None:
            filters = {}
        
        m = folium.Map(
            location=[41.5801, -71.4774],
            zoom_start=11,
            tiles='cartodbpositron'
        )
        
        # Add broadband data layer
        self._add_broadband_data(m, filters)
        
        # Add layer control
        folium.LayerControl().add_to(m)
        
        return m
    
    def _add_broadband_data(self, m: folium.Map, filters: Dict):
        """Add broadband data to the map"""
        query = """
        SELECT 
            b.provider_id, p.brand_name, b.block_geoid, b.technology,
            b.max_advertised_download_speed, b.max_advertised_upload_speed,
            b.low_latency, b.business_residential_code,
            ST_X(ST_Centroid(c.geometry)) as lon,
            ST_Y(ST_Centroid(c.geometry)) as lat
        FROM broadband_data b
        JOIN providers p ON b.provider_id = p.provider_id
        JOIN census_blocks c ON b.block_geoid = c.geoid
        WHERE 1=1
        """
        params = []
        
        # Apply filters
        if filters.get('provider_id'):
            query += " AND b.provider_id = %s"
            params.append(int(filters['provider_id']))
            
        if filters.get('technology'):
            query += " AND b.technology = %s"
            params.append(int(filters['technology']))
            
        if filters.get('min_download_speed'):
            query += " AND b.max_advertised_download_speed >= %s"
            params.append(float(filters['min_download_speed']))
        
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            
            if not data:
                print("No data matching filters")
                return
                
            # Prepare data for FastMarkerCluster
            locations = []
            popups = []
            icons = []
            
            for row in data:
                row_dict = dict(zip(columns, row))
                
                # Create popup content
                popup_text = f"""
                <b>Provider:</b> {row_dict['brand_name']}<br>
                <b>Block GEOID:</b> {row_dict['block_geoid']}<br>
                <b>Technology Code:</b> {row_dict['technology']}<br>
                <b>Download:</b> {row_dict['max_advertised_download_speed']} Mbps<br>
                <b>Upload:</b> {row_dict['max_advertised_upload_speed']} Mbps<br>
                <b>Low Latency:</b> {'Yes' if row_dict['low_latency'] else 'No'}
                """
                
                # Different colors based on technology
                color = self._get_tech_color(row_dict['technology'])
                
                locations.append([row_dict['lat'], row_dict['lon']])
                popups.append(popup_text)
                icons.append(folium.Icon(color=color))
            
            # Create marker cluster with new API
            FastMarkerCluster(
                data=locations,
                name="Broadband Locations",
                overlay=True,
                control=True,
                popups=popups,
                icons=icons
            ).add_to(m)
    
    def _get_tech_color(self, technology: int) -> str:
        """Get color for technology type"""
        colors = {
            50: 'red',    # Cable
            40: 'green',  # Fiber
            30: 'orange', # Copper
            70: 'purple', # Wireless
            60: 'pink'    # Satellite
        }
        return colors.get(technology, 'blue')
    
    def close(self):
        self.conn.close()