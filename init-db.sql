-- PostgreSQL initialization script
-- The database 'airflow' is already created by docker-compose

-- Create raw data table
CREATE TABLE IF NOT EXISTS raw_earthquakes (
    id SERIAL PRIMARY KEY,
    earthquake_id VARCHAR(50) UNIQUE,
    raw_json JSONB NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    api_source VARCHAR(100)
);

-- Create analytics table
CREATE TABLE IF NOT EXISTS analytics_earthquakes (
    id SERIAL PRIMARY KEY,
    earthquake_id VARCHAR(50) UNIQUE,
    occurred_at TIMESTAMP,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    depth_km DECIMAL(10,2),
    magnitude DECIMAL(4,2),
    magnitude_type VARCHAR(10),
    place VARCHAR(255),
    country VARCHAR(100),
    region VARCHAR(100),
    
    -- Derived columns
    magnitude_category VARCHAR(20),
    depth_category VARCHAR(20),
    risk_level VARCHAR(20),
    day_of_week VARCHAR(10),
    hour_of_day INTEGER,
    
    -- Metadata
    transformed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes for performance
    CONSTRAINT unique_earthquake_id UNIQUE (earthquake_id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_raw_extracted_at ON raw_earthquakes(extracted_at);
CREATE INDEX IF NOT EXISTS idx_analytics_occurred_at ON analytics_earthquakes(occurred_at);
CREATE INDEX IF NOT EXISTS idx_analytics_magnitude ON analytics_earthquakes(magnitude);
CREATE INDEX IF NOT EXISTS idx_analytics_region ON analytics_earthquakes(region);

-- Create load history table for incremental loads
CREATE TABLE IF NOT EXISTS load_history (
    id SERIAL PRIMARY KEY,
    load_date TIMESTAMP,
    records_loaded INTEGER,
    status VARCHAR(20),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);