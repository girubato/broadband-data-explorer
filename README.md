# Broadband Data Explorer

A Python application for exploring FCC broadband data with a GUI interface.

## Features

- Load FCC broadband data from local ZIP files
- Store data in PostgreSQL database
- Interactive map visualization
- Filterable data table and map view

## Screenshots
<img width="1200" src="https://github.com/user-attachments/assets/890cfde0-5a17-4634-be86-2452a7e8f434" />
<img width="1200" src="https://github.com/user-attachments/assets/3c3c4c01-69eb-4001-8a04-5b35253f0ddd" />
<img width="918" src="https://github.com/user-attachments/assets/b3aab8e5-04ad-4c6f-ae79-65024e747173" />

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/girubato/broadband-data-explorer.git
   cd broadband-data-explorer
   ```

## Usage
1. Move downloaded FCC broadband data ZIP files into the '/data/fcc_data' folder.
2. Move corresponding census block shapefile ZIP files into the '/data/census_blocks' folder.
3. Navigate to the broadband-data-explorer root directory using the command line.
4. Run ```python src/reset_db.py``` to drop existing databases for broadband-data-explorer.
5. Run ```python src/database.py``` to instantiate necessary databases for broadband-data-explorer.
6. Run ```python src/data_loader.py``` to load FCC and census block data into the databases.
7. Run ```python src/broadband_app.py``` to start the broadband data explorer executable.
