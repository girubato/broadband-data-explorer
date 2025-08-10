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
    
    # def _add_broadband_data(self, m: folium.Map, filters: Dict):
    #     """Add broadband data to the map"""
    #     query = """
    #     SELECT 
    #         b.provider_id, p.brand_name, b.block_geoid, b.technology,
    #         b.max_advertised_download_speed, b.max_advertised_upload_speed,
    #         b.low_latency, b.business_residential_code,
    #         ST_X(ST_Centroid(c.geometry)) as lon,
    #         ST_Y(ST_Centroid(c.geometry)) as lat
    #     FROM broadband_data b
    #     JOIN providers p ON b.provider_id = p.provider_id
    #     JOIN census_blocks c ON b.block_geoid = c.geoid
    #     WHERE 1=1
    #     """
    #     params = []
        
    #     # Apply filters
    #     if filters.get('provider_id'):
    #         query += " AND b.provider_id = %s"
    #         params.append(int(filters['provider_id']))
            
    #     if filters.get('technology'):
    #         query += " AND b.technology = %s"
    #         params.append(int(filters['technology']))
            
    #     if filters.get('min_download_speed'):
    #         query += " AND b.max_advertised_download_speed >= %s"
    #         params.append(float(filters['min_download_speed']))
        
    #     with self.conn.cursor() as cursor:
    #         cursor.execute(query, params)
    #         columns = [desc[0] for desc in cursor.description]
    #         data = cursor.fetchall()
            
    #         if not data:
    #             print("No data matching filters")
    #             return
                
    #         # Prepare data for FastMarkerCluster
    #         locations = []
    #         popups = []
    #         icons = []
            
    #         for row in data:
    #             row_dict = dict(zip(columns, row))
                
    #             # Create popup content
    #             popup_text = f"""
    #             <b>Provider:</b> {row_dict['brand_name']}<br>
    #             <b>Block GEOID:</b> {row_dict['block_geoid']}<br>
    #             <b>Technology Code:</b> {row_dict['technology']}<br>
    #             <b>Download:</b> {row_dict['max_advertised_download_speed']} Mbps<br>
    #             <b>Upload:</b> {row_dict['max_advertised_upload_speed']} Mbps<br>
    #             <b>Low Latency:</b> {'Yes' if row_dict['low_latency'] else 'No'}
    #             """
                
    #             # Different colors based on technology
    #             color = self._get_tech_color(row_dict['technology'])
                
    #             locations.append([row_dict['lat'], row_dict['lon']])
    #             popups.append(popup_text)
    #             icons.append(folium.Icon(color=color))
            
    #         # Create marker cluster with new API
    #         FastMarkerCluster(
    #             data=locations,
    #             name="Broadband Locations",
    #             overlay=True,
    #             control=True,
    #             popups=popups,
    #             icons=icons
    #         ).add_to(m)

    def _add_broadband_data(self, m: folium.Map, filters: Dict):
        """Add broadband data to the map with working popups"""
        try:
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
                    
                # Create a FeatureGroup for better performance
                fg = folium.FeatureGroup(name="Broadband Locations")
                
                for row in data:
                    row_dict = dict(zip(columns, row))
                    
                    # Create popup content with HTML formatting
                    popup_text = f"""
                    <div style="width: 250px;">
                        <h4 style="margin-bottom: 5px;">{row_dict['brand_name']}</h4>
                        <hr style="margin: 5px 0;">
                        <p><b>Location ID:</b> {row_dict['block_geoid']}</p>
                        <p><b>Technology:</b> {self._get_tech_name(row_dict['technology'])}</p>
                        <p><b>Download:</b> {row_dict['max_advertised_download_speed']} Mbps</p>
                        <p><b>Upload:</b> {row_dict['max_advertised_upload_speed']} Mbps</p>
                        <p><b>Latency:</b> {'Low' if row_dict['low_latency'] else 'Standard'}</p>
                        <p><b>Service Type:</b> {row_dict['business_residential_code']}</p>
                    </div>
                    """
                    
                    # Create popup with max_width to prevent cutoff
                    popup = folium.Popup(
                        popup_text,
                        max_width=300,
                        min_width=200
                    )
                    
                    # Create marker with popup
                    folium.Marker(
                        location=[row_dict['lat'], row_dict['lon']],
                        popup=popup,
                        icon=folium.Icon(
                            color=self._get_tech_color(row_dict['technology']),
                            icon='info-sign'
                        )
                    ).add_to(fg)
                
                # Add the feature group to map
                fg.add_to(m)
                
        except Exception as e:
            print(f"Error in _add_broadband_data: {e}")
            self.conn.rollback()
            raise

    def _get_tech_name(self, tech_code: int) -> str:
        """Convert technology code to human-readable name"""
        tech_names = {
            10: "Asymmetric xDSL",
            11: "ADSL2, ADSL2+",
            12: "VDSL",
            20: "Symmetric xDSL",
            30: "Other Copper Wireline",
            40: "Cable Modem other than DOCSIS 1, 1.1, 2.0, 3.0, or 3.1",
            41: "Cable Modem – DOCSIS 1, 1.1 or 2.0",
            42: "Cable Modem – DOCSIS 3.0",
            43: "Cable Modem – DOCSIS 3.1",
            50: "Optical Carrier / Fiber to the end user",
            60: "Satellite",
            70: "Terrestrial Fixed Wireless",
            90: "Electric Power Line",
            0: "All Other"
        }
        return tech_names.get(tech_code, f"Unknown ({tech_code})")
    
    def _get_tech_color(self, technology: int) -> str:
        """Get color for technology type"""
        colors = {
            10: "red",
            11: "red",
            12: "red",
            20: "red",
            30: "red",
            40: "green",
            41: "green",
            42: "green",
            43: "green",
            50: "blue",
            60: "orange",
            70: "purple",
            90: "pink",
            0: "gray"
        }
        return colors.get(technology, 'blue')
    
    def close(self):
        self.conn.close()