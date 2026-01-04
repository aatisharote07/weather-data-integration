import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Setup Paths
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import Project Modules
try:
    from scripts.api_client import fetch_weather_data
    from scripts.data_loader import create_tables, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
except ImportError as e:
    logger.error(f"Error importing modules: {e}")
    sys.exit(1)

# Database Connection URL for SQLAlchemy
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def transform_data_pandas(raw_data):
    """Transforms raw weather data using Pandas instead of Spark"""
    df = pd.DataFrame(raw_data)
    
    # Convert datetime to proper format
    # API client returns ISO format string, pandas can parse it
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['date'] = df['datetime'].dt.date
    df['time'] = df['datetime'].dt.time
    
    # Rounding
    df['temperature'] = df['temperature'].round(1)
    df['wind_speed'] = (df['wind_speed'] * 3.6).round(1) # Convert m/s to km/h
    
    # Create City ID (Dense Rank equivalent)
    # Sort by city name and country to ensure consistent IDs
    unique_cities = df[['city_name', 'country', 'latitude', 'longitude']].drop_duplicates().sort_values(['city_name', 'country'])
    unique_cities['city_id'] = range(1, len(unique_cities) + 1)
    
    # Merge city_id back to main df
    df = df.merge(unique_cities[['city_name', 'country', 'city_id']], on=['city_name', 'country'])
    
    # Split into Fact and Dimension
    fact_columns = ['date', 'time', 'city_id', 'temperature', 'humidity', 'pressure', 'wind_speed']
    fact_df = df[fact_columns]
    
    dim_columns = ['city_id', 'city_name', 'country', 'latitude', 'longitude']
    dim_df = unique_cities[dim_columns]
    
    return fact_df, dim_df

def load_data_pandas(fact_df, dim_df):
    """Loads Pandas DataFrames to PostgreSQL"""
    engine = create_engine(DATABASE_URL)
    with engine.connect() as connection:
        # Load Dimensions (Cities) - overwrite or append? 
        # Original logic was 'overwrite' for cities.
        logger.info("Loading Cities data...")
        dim_df.to_sql('cities', engine, if_exists='replace', index=False)
        
        # Load Facts - append
        logger.info("Loading Weather Measurements...")
        fact_df.to_sql('weather_measurements', engine, if_exists='append', index=False)
        
        logger.info("Data loaded successfully via Pandas!")

def run_pipeline():
    logger.info("Starting Manual ETL Pipeline (Pandas Version)...")
    
    # 1. Create Tables (Using original script logic for safety, though pandas can create too)
    logger.info("Step 1: Verifying/Creating Database Tables...")
    try:
        create_tables() 
    except Exception as e:
        logger.warning(f"Create tables script had issues (possibly pyspark import), but Pandas to_sql will handle table creation. Error: {e}")

    # 2. Fetch Data
    logger.info("Step 2: Fetching Weather Data from API...")
    raw_data = fetch_weather_data()
    if not raw_data:
        logger.error("No data fetched! Check API Key or Limits.")
        return
    logger.info(f"Fetched {len(raw_data)} records.")

    # 3. Transform Data
    logger.info("Step 3: Transforming Data with Pandas...")
    fact_df, dim_df = transform_data_pandas(raw_data)
    
    # 4. Load Data
    logger.info("Step 4: Loading Data to PostgreSQL...")
    try:
        load_data_pandas(fact_df, dim_df)
        logger.info("Pipeline Completed Successfully!")
    except Exception as e:
        logger.error(f"Error loading to DB: {e}")

if __name__ == "__main__":
    run_pipeline()
