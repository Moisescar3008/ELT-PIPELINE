from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
import logging

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'earthquake_elt_pipeline',
    default_args=default_args,
    description='ELT pipeline for earthquake data from USGS',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    tags=['elt', 'earthquake', 'usgs'],
)

def extract_earthquake_data(**context):
    """
    EXTRACT: Fetch earthquake data from USGS API
    Gets earthquakes from the last 24 hours
    """
    try:
        # USGS Earthquake API - Last 24 hours, all magnitudes
        end_time = datetime.now()
        start_time = end_time - timedelta(days=1)
        
        url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
        params = {
            'format': 'geojson',
            'starttime': start_time.strftime('%Y-%m-%d'),
            'endtime': end_time.strftime('%Y-%m-%d'),
            'minmagnitude': 2.5,  # Significant earthquakes only
        }
        
        logging.info(f"Fetching earthquake data from {start_time} to {end_time}")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        earthquake_count = len(data.get('features', []))
        
        logging.info(f"Successfully extracted {earthquake_count} earthquakes")
        
        # Push to XCom for next task
        context['ti'].xcom_push(key='earthquake_data', value=data)
        context['ti'].xcom_push(key='record_count', value=earthquake_count)
        
        return earthquake_count
        
    except Exception as e:
        logging.error(f"Extract failed: {str(e)}")
        raise

def load_raw_data(**context):
    """
    LOAD: Store raw JSON data exactly as received
    NO transformations, NO cleaning - preserve original data
    """
    try:
        # Get data from previous task
        ti = context['ti']
        data = ti.xcom_pull(task_ids='extract_data', key='earthquake_data')
        
        if not data or 'features' not in data:
            logging.warning("No earthquake data to load")
            return 0
        
        # Connect to PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        loaded_count = 0
        
        for feature in data['features']:
            earthquake_id = feature['id']
            raw_json = json.dumps(feature)
            
            # Insert raw data - ON CONFLICT DO NOTHING to handle duplicates
            cursor.execute("""
                INSERT INTO raw_earthquakes (earthquake_id, raw_json, api_source)
                VALUES (%s, %s, %s)
                ON CONFLICT (earthquake_id) DO NOTHING
            """, (earthquake_id, raw_json, 'USGS API'))
            
            loaded_count += cursor.rowcount
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Loaded {loaded_count} new raw earthquake records")
        
        # Log to load history
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        pg_hook.run("""
            INSERT INTO load_history (load_date, records_loaded, status)
            VALUES (NOW(), %s, 'SUCCESS')
        """, parameters=(loaded_count,))
        
        return loaded_count
        
    except Exception as e:
        logging.error(f"Load failed: {str(e)}")
        # Log failure
        try:
            pg_hook = PostgresHook(postgres_conn_id='postgres_default')
            pg_hook.run("""
                INSERT INTO load_history (load_date, records_loaded, status, error_message)
                VALUES (NOW(), 0, 'FAILED', %s)
            """, parameters=(str(e),))
        except:
            pass
        raise

# TRANSFORM SQL - Executed in database after load
transform_sql = """
-- TRANSFORM: Clean and enrich data in the database
-- This runs AFTER load, working with raw data to create analytics table

INSERT INTO analytics_earthquakes (
    earthquake_id,
    occurred_at,
    latitude,
    longitude,
    depth_km,
    magnitude,
    magnitude_type,
    place,
    country,
    region,
    magnitude_category,
    depth_category,
    risk_level,
    day_of_week,
    hour_of_day
)
SELECT DISTINCT
    raw_json->>'id' as earthquake_id,
    to_timestamp((raw_json->'properties'->>'time')::bigint / 1000) as occurred_at,
    (raw_json->'geometry'->'coordinates'->>1)::decimal as latitude,
    (raw_json->'geometry'->'coordinates'->>0)::decimal as longitude,
    (raw_json->'geometry'->'coordinates'->>2)::decimal as depth_km,
    (raw_json->'properties'->>'mag')::decimal as magnitude,
    raw_json->'properties'->>'magType' as magnitude_type,
    raw_json->'properties'->>'place' as place,
    
    -- Extract country (simplified)
    CASE 
        WHEN raw_json->'properties'->>'place' LIKE '%Mexico%' THEN 'Mexico'
        WHEN raw_json->'properties'->>'place' LIKE '%California%' THEN 'United States'
        WHEN raw_json->'properties'->>'place' LIKE '%Japan%' THEN 'Japan'
        WHEN raw_json->'properties'->>'place' LIKE '%Chile%' THEN 'Chile'
        ELSE 'Other'
    END as country,
    
    -- Extract region
    SPLIT_PART(raw_json->'properties'->>'place', ',', -1) as region,
    
    -- Magnitude category
    CASE 
        WHEN (raw_json->'properties'->>'mag')::decimal < 3.0 THEN 'Minor'
        WHEN (raw_json->'properties'->>'mag')::decimal < 5.0 THEN 'Light'
        WHEN (raw_json->'properties'->>'mag')::decimal < 6.0 THEN 'Moderate'
        WHEN (raw_json->'properties'->>'mag')::decimal < 7.0 THEN 'Strong'
        ELSE 'Major'
    END as magnitude_category,
    
    -- Depth category
    CASE 
        WHEN (raw_json->'geometry'->'coordinates'->>2)::decimal < 70 THEN 'Shallow'
        WHEN (raw_json->'geometry'->'coordinates'->>2)::decimal < 300 THEN 'Intermediate'
        ELSE 'Deep'
    END as depth_category,
    
    -- Risk level (combination of magnitude and depth)
    CASE 
        WHEN (raw_json->'properties'->>'mag')::decimal >= 6.0 
             AND (raw_json->'geometry'->'coordinates'->>2)::decimal < 70 THEN 'High'
        WHEN (raw_json->'properties'->>'mag')::decimal >= 5.0 THEN 'Medium'
        ELSE 'Low'
    END as risk_level,
    
    -- Time analysis
    TO_CHAR(to_timestamp((raw_json->'properties'->>'time')::bigint / 1000), 'Day') as day_of_week,
    EXTRACT(HOUR FROM to_timestamp((raw_json->'properties'->>'time')::bigint / 1000)) as hour_of_day

FROM raw_earthquakes
WHERE extracted_at >= NOW() - INTERVAL '2 days'
ON CONFLICT (earthquake_id) DO UPDATE SET
    transformed_at = NOW();
"""

# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_earthquake_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_raw_data',
    python_callable=load_raw_data,
    dag=dag,
)

transform_task = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_default',
    sql=transform_sql,
    dag=dag,
)

# Set task dependencies
extract_task >> load_task >> transform_task