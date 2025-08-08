import folium
from folium.plugins import FastMarkerCluster
import geopandas as gpd
import psycopg2
from config import DB_CONFIG, MAP_CENTER, MAP_ZOOM
from typing import Optional, Dict
import json

class MapBuilder:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
    
    def create_map(self, filters: Optional[Dict] = None) -> folium.Map:
        """Create a Folium map with census blocks and broadband data"""
        if filters is None:
            filters = {}
        
        # Create base map
        m = folium.Map(
            location=MAP_CENTER,
            zoom_start=MAP_ZOOM,
            tiles='cartodbpositron'
        )
        
        # Add census blocks layer
        self._add_census_blocks(m, filters.get('block_geoid'))
        
        # Add broadband data layer
        self._add_broadband_data(m, filters)
        
        # Add layer control
        folium.LayerControl().add_to(m)
        
        return m
    
    def _add_census_blocks(self, m: folium.Map, block_geoid: Optional[str] = None):
        """Add census blocks to the map"""
        query = """
        SELECT geoid, ST_AsGeoJSON(geometry) as geometry
        FROM census_blocks
        """
        params = []
        
        if block_geoid:
            query += " WHERE geoid = %s"
            params.append(block_geoid)
        
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            blocks = cursor.fetchall()
            
            if not blocks:
                return
                
            # Create GeoJSON layer
            features = []
            for geoid, geojson in blocks:
                feature = {
                    "type": "Feature",
                    "properties": {"geoid": geoid},
                    "geometry": json.loads(geojson)
                }
                features.append(feature)
                
            geojson_layer = folium.GeoJson(
                {
                    "type": "FeatureCollection",
                    "features": features
                },
                name="Census Blocks",
                style_function=lambda x: {
                    'fillColor': '#3186cc',
                    'color': '#3186cc',
                    'weight': 1,
                    'fillOpacity': 0.2
                },
                highlight_function=lambda x: {
                    'weight': 3,
                    'fillOpacity': 0.5
                },
                tooltip=folium.GeoJsonTooltip(
                    fields=['geoid'],
                    aliases=['Block GEOID:']
                )
            )
            geojson_layer.add_to(m)
    
    def _add_broadband_data(self, m: folium.Map, filters: Dict):
        """Add broadband data to the map"""
        query = """
        SELECT 
            b.provider_id, p.brand_name, b.block_geoid, b.technology,
            b.max_advertised_download_speed, b.max_advertised_upload_speed,
            b.low_latency, b.business_residential_code,
            ST_AsText(ST_Centroid(c.geometry)) as centroid
        FROM broadband_data b
        JOIN providers p ON b.provider_id = p.provider_id
        JOIN census_blocks c ON b.block_geoid = c.geoid
        WHERE 1=1
        """
        params = []
        
        # Apply filters
        if filters.get('provider_id'):
            query += " AND b.provider_id = %s"
            params.append(filters['provider_id'])
            
        if filters.get('technology'):
            query += " AND b.technology = %s"
            params.append(filters['technology'])
            
        if filters.get('min_download_speed'):
            query += " AND b.max_advertised_download_speed >= %s"
            params.append(float(filters['min_download_speed']))
        
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            
            if not data:
                return
                
            # Create marker cluster
            marker_cluster = FastMarkerCluster(
                name="Broadband Locations",
                overlay=True,
                control=True
            )
            
            for row in data:
                row_dict = dict(zip(columns, row))
                
                # Parse centroid coordinates
                point_str = row_dict['centroid'][6:-1]  # Remove 'POINT(' and ')'
                lon, lat = map(float, point_str.split())
                
                # Create popup content
                popup_text = f"""
                <b>Provider:</b> {row_dict['brand_name']}<br>
                <b>Block GEOID:</b> {row_dict['block_geoid']}<br>
                <b>Technology:</b> {row_dict['technology']}<br>
                <b>Download:</b> {row_dict['max_advertised_download_speed']} Mbps<br>
                <b>Upload:</b> {row_dict['max_advertised_upload_speed']} Mbps<br>
                <b>Low Latency:</b> {'Yes' if row_dict['low_latency'] else 'No'}
                """
                
                # Different colors based on technology
                color = self._get_tech_color(row_dict['technology'])
                
                # Create marker
                marker = folium.Marker(
                    location=[lat, lon],
                    popup=popup_text,
                    icon=folium.Icon(color=color)
                )
                marker_cluster.add_child(marker)
            
            marker_cluster.add_to(m)
    
    def _get_tech_color(self, technology: str) -> str:
        """Get color for technology type"""
        colors = {
            'Fiber': 'green',
            'Cable': 'red',
            'Copper': 'orange',
            'Fixed Wireless': 'purple',
            'Satellite': 'pink'
        }
        return colors.get(technology, 'blue')
    
    def close(self):
        self.conn.close()