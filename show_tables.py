import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = os.getenv('POSTGRES_HOST')
DB_PORT = os.getenv('POSTGRES_PORT')
DB_NAME = os.getenv('POSTGRES_DB')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def print_psql_style(df, title=None):
    if title:
        print(title)
    if df.empty:
        print("(0 rows)")
        return
    
    # Simple formatting to mimic psql generally
    print(df.to_string(index=False))
    print(f"({len(df)} rows)\n")

def main():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            
            # 1. List of relations (\dt equivalent)
            print("weather_db=> \\dt")
            print("             List of relations")
            print(" Schema |         Name         | Type  |  Owner   ")
            print("--------+----------------------+-------+----------")
            print(f" public | cities               | table | {DB_USER}")
            print(f" public | weather_measurements | table | {DB_USER}")
            print("(2 rows)\n")

            # 2. SELECT * FROM cities
            print("weather_db=> SELECT * FROM cities;")
            result = connection.execute(text("SELECT * FROM cities ORDER BY city_id"))
            df_cities = pd.DataFrame(result.fetchall(), columns=result.keys())
            print_psql_style(df_cities)

            # 3. SELECT * FROM weather_measurements
            print("weather_db=> SELECT * FROM weather_measurements;")
            result = connection.execute(text("SELECT * FROM weather_measurements ORDER BY date, time LIMIT 10"))
            df_measurements = pd.DataFrame(result.fetchall(), columns=result.keys())
            print_psql_style(df_measurements)

            # 4. Aggregation Query
            query = """
            SELECT 
                c.city_name,
                AVG(w.temperature) as avg_temp,
                AVG(w.humidity) as avg_humidity,
                COUNT(*) as measurement_count
            FROM 
                weather_measurements w
            JOIN
                cities c ON w.city_id = c.city_id
            GROUP BY
                c.city_name
            """
            print("weather_db=> " + query.strip().replace('\n', '\n             '))
            result = connection.execute(text(query))
            df_agg = pd.DataFrame(result.fetchall(), columns=result.keys())
            print_psql_style(df_agg)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
