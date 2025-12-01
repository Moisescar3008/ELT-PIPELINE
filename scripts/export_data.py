import pandas as pd
import psycopg2
import os

# Crear carpeta data si no existe
os.makedirs('data', exist_ok=True)

print("Connecting to PostgreSQL...")
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="airflow",
    user="airflow",
    password="airflow"
)

print("Exporting raw_earthquakes...")
df_raw = pd.read_sql("SELECT * FROM raw_earthquakes", conn)
df_raw.to_csv('data/raw_earthquakes.csv', index=False)
print(f"✅ Exported {len(df_raw)} rows to data/raw_earthquakes.csv")

print("Exporting analytics_earthquakes...")
df_analytics = pd.read_sql("SELECT * FROM analytics_earthquakes", conn)
df_analytics.to_csv('data/analytics_earthquakes.csv', index=False)
print(f"✅ Exported {len(df_analytics)} rows to data/analytics_earthquakes.csv")

print("Exporting load_history...")
df_history = pd.read_sql("SELECT * FROM load_history", conn)
df_history.to_csv('data/load_history.csv', index=False)
print(f"✅ Exported {len(df_history)} rows to data/load_history.csv")

conn.close()
print("\n🎉 All data exported successfully!")