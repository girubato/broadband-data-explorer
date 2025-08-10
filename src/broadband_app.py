import sys
import pandas as pd
import psycopg2
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                             QComboBox, QTableWidget, QTableWidgetItem, 
                             QGroupBox, QMessageBox, QTabWidget, QFileDialog)
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtCore import QUrl, Qt
from PyQt5.QtGui import QDoubleValidator
from config import DB_CONFIG, FCC_DATA_DIR, CENSUS_DATA_DIR
import io
from pathlib import Path
from data_loader import DataLoader
from map_utils import MapBuilder

class BroadbandApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Broadband Data Explorer")
        self.setGeometry(100, 100, 1200, 800)
        
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.init_ui()
        
    def init_ui(self):
        """Initialize the user interface"""
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        
        self.layout = QVBoxLayout()
        self.central_widget.setLayout(self.layout)
        
        # Create tabs
        self.tabs = QTabWidget()
        self.layout.addWidget(self.tabs)
        
        # Data Import Tab
        self.import_tab = QWidget()
        self.setup_import_tab()
        self.tabs.addTab(self.import_tab, "Data Import")
        
        # Map View Tab
        self.map_tab = QWidget()
        self.setup_map_tab()
        self.tabs.addTab(self.map_tab, "Map View")
        
        # Data Table Tab
        self.table_tab = QWidget()
        self.setup_table_tab()
        self.tabs.addTab(self.table_tab, "Data Table")
        
    def setup_import_tab(self):
        """Set up the data import tab"""
        layout = QVBoxLayout()
        self.import_tab.setLayout(layout)
        
        # Data directory selection
        dir_group = QGroupBox("Data Directories")
        dir_layout = QVBoxLayout()
        
        # FCC Data Directory
        fcc_layout = QHBoxLayout()
        self.fcc_dir_input = QLineEdit()
        self.fcc_dir_input.setPlaceholderText("Select FCC data directory")
        fcc_layout.addWidget(self.fcc_dir_input)
        
        self.fcc_browse_button = QPushButton("Browse...")
        self.fcc_browse_button.clicked.connect(lambda: self.browse_directory(self.fcc_dir_input))
        fcc_layout.addWidget(self.fcc_browse_button)
        dir_layout.addLayout(fcc_layout)
        
        # Census Data Directory
        census_layout = QHBoxLayout()
        self.census_dir_input = QLineEdit()
        self.census_dir_input.setPlaceholderText("Select census blocks directory")
        census_layout.addWidget(self.census_dir_input)
        
        self.census_browse_button = QPushButton("Browse...")
        self.census_browse_button.clicked.connect(lambda: self.browse_directory(self.census_dir_input))
        census_layout.addWidget(self.census_browse_button)
        dir_layout.addLayout(census_layout)
        
        dir_group.setLayout(dir_layout)
        layout.addWidget(dir_group)
        
        # Import button
        self.import_button = QPushButton("Import Data")
        self.import_button.clicked.connect(self.import_data)
        layout.addWidget(self.import_button)
        
        # Status label
        self.status_label = QLabel("Ready")
        layout.addWidget(self.status_label)
        
    def setup_map_tab(self):
        """Set up the map visualization tab"""
        layout = QVBoxLayout()
        self.map_tab.setLayout(layout)
        
        # Filter controls
        filter_layout = QHBoxLayout()
        
        # Provider filter
        filter_layout.addWidget(QLabel("Provider:"))
        self.provider_combo = QComboBox()
        self.provider_combo.addItem("All Providers", None)
        self.load_providers()
        filter_layout.addWidget(self.provider_combo)
        
        # Technology filter
        filter_layout.addWidget(QLabel("Technology:"))
        self.tech_combo = QComboBox()
        self.tech_combo.addItem("All", None)
        self.tech_combo.addItem("Cable", 50)
        self.tech_combo.addItem("Fiber", 40)
        self.tech_combo.addItem("Copper", 30)
        self.tech_combo.addItem("Wireless", 70)
        self.tech_combo.addItem("Satellite", 60)
        filter_layout.addWidget(self.tech_combo)
        
        # Speed filter
        filter_layout.addWidget(QLabel("Min Download (Mbps):"))
        self.speed_filter = QLineEdit()
        self.speed_filter.setValidator(QDoubleValidator())
        filter_layout.addWidget(self.speed_filter)
        
        # Filter button
        self.filter_button = QPushButton("Filter")
        self.filter_button.clicked.connect(self.update_map)
        filter_layout.addWidget(self.filter_button)
        
        layout.addLayout(filter_layout)
        
        # Map view
        self.map_view = QWebEngineView()
        layout.addWidget(self.map_view)
        
        # Create initial map
        self.create_map()
        
    def setup_table_tab(self):
        """Set up the data table tab"""
        layout = QVBoxLayout()
        self.table_tab.setLayout(layout)
        
        # Table view
        self.data_table = QTableWidget()
        self.data_table.setColumnCount(8)
        self.data_table.setHorizontalHeaderLabels([
            "Provider", "Block GEOID", "Technology", 
            "Download (Mbps)", "Upload (Mbps)", 
            "Low Latency", "Business/Residential", "State"
        ])
        self.data_table.setSortingEnabled(True)
        layout.addWidget(self.data_table)
        
        # Load initial data
        self.load_table_data()
        
    def browse_directory(self, target_input):
        """Open directory selection dialog"""
        dir_path = QFileDialog.getExistingDirectory(
            self, 
            "Select Directory",
            str(Path.home())
        )
        if dir_path:
            target_input.setText(dir_path)
    
    def load_providers(self):
        """Load providers from database into combo box"""
        try:
            self.provider_combo.clear()
            self.provider_combo.addItem("All Providers", None)
            
            with self.conn.cursor() as cursor:
                cursor.execute("""
                SELECT provider_id, brand_name 
                FROM providers 
                ORDER BY brand_name
                """)
                providers = cursor.fetchall()
                for provider_id, brand_name in providers:
                    display_name = brand_name if brand_name else provider_id
                    self.provider_combo.addItem(display_name, provider_id)
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to load providers: {str(e)}")
    
    def import_data(self):
        """Import data from local files"""
        fcc_dir = self.fcc_dir_input.text()
        census_dir = self.census_dir_input.text()
        
        if not fcc_dir or not census_dir:
            QMessageBox.warning(
                self, 
                "Error", 
                "Please select both FCC data and census blocks directories"
            )
            return
        
        try:
            self.status_label.setText("Importing data...")
            QApplication.processEvents()  # Update UI
            
            # Update config paths
            from config import FCC_DATA_DIR, CENSUS_DATA_DIR
            FCC_DATA_DIR = Path(fcc_dir)
            CENSUS_DATA_DIR = Path(census_dir)
            
            loader = DataLoader()
            loader.load_all_data()
            
            # Verify import
            with loader.conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM broadband_data")
                count = cursor.fetchone()[0]
                
            if count == 0:
                QMessageBox.warning(
                    self, 
                    "Import Issue",
                    "Data files were processed but no records were imported.\n"
                    "Please check:\n"
                    "1. The ZIP files contain valid CSV data\n"
                    "2. The census blocks shapefile is present\n"
                    "3. There are no errors in the console output"
                )
            else:
                self.status_label.setText(f"Imported {count} records successfully!")
                
            loader.close()
            self.load_providers()
            self.load_table_data()
            self.create_map()
            
        except Exception as e:
            QMessageBox.warning(
                self, 
                "Import Error", 
                f"Failed to import data:\n{str(e)}\n\n"
                f"Check console for more details"
            )
            self.status_label.setText("Import failed")
            print(f"Detailed error: {e}")  # This will show in console
    
    def create_map(self, filters=None):
        """Create or update the map"""
        if filters is None:
            filters = {}
            
        try:
            map_builder = MapBuilder()
            m = map_builder.create_map(filters)
            map_builder.close()
            
            # Save map to a temporary file and load into QWebEngineView
            data = io.BytesIO()
            m.save(data, close_file=False)
            self.map_view.setHtml(data.getvalue().decode('utf-8'))
            
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to create map: {str(e)}")
    
    def update_map(self):
        """Update the map with filtered data"""
        filters = {}
        
        # Get provider filter
        provider_id = self.provider_combo.currentData()
        if provider_id:
            filters['provider_id'] = provider_id
            
        # Get technology filter
        technology = self.tech_combo.currentData()
        if technology:
            filters['technology'] = technology
            
        # Get speed filter
        min_speed = self.speed_filter.text()
        if min_speed:
            filters['min_download_speed'] = min_speed
            
        self.create_map(filters)
        self.load_table_data(filters)
    
    def load_table_data(self, filters=None):
        """Load data into the table view"""
        if filters is None:
            filters = {}
            
        try:
            self.conn.rollback()
            query = """
            SELECT 
                p.brand_name, b.block_geoid, b.technology,
                b.max_advertised_download_speed, b.max_advertised_upload_speed,
                b.low_latency, b.business_residential_code, b.state_usps
            FROM broadband_data b
            JOIN providers p ON b.provider_id = p.provider_id
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
                
                self.data_table.setRowCount(0)
                
                if not data:
                    return
                    
                self.data_table.setRowCount(len(data))
                
                for row_idx, row in enumerate(data):
                    for col_idx, value in enumerate(row):
                        if isinstance(value, bool):
                            display_value = "Yes" if value else "No"
                        else:
                            display_value = str(value)
                            
                        self.data_table.setItem(
                            row_idx, 
                            col_idx, 
                            QTableWidgetItem(display_value)
                        )
                
                self.data_table.resizeColumnsToContents()
                
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to load table data: {str(e)}")
    
    def closeEvent(self, event):
        """Clean up when closing the application"""
        if self.conn:
            self.conn.close()
        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = BroadbandApp()
    window.show()
    sys.exit(app.exec_())