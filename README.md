# 🌍 Earthquake Data ELT Pipeline with Apache Airflow

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.3-blue)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-blue)](https://www.postgresql.org/)
[![Python](https://img.shields.io/badge/Python-3.8+-green)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://www.docker.com/)

A complete ELT (Extract, Load, Transform) pipeline implementation using Apache Airflow to analyze global earthquake data from the USGS (United States Geological Survey) API.

---

## 📋 Table of Contents

- [Phase 1: Dataset Selection & Social Justification](#phase-1-dataset-selection--social-justification)
- [Phase 2: ELT Pipeline Implementation](#phase-2-elt-pipeline-implementation)
- [Phase 3: Analytics Dashboard](#phase-3-analytics-dashboard)
- [Installation & Setup](#installation--setup)
- [Project Structure](#project-structure)
- [Pipeline Execution](#pipeline-execution)
- [Results & Evidence](#results--evidence)
- [Why ELT Over ETL?](#why-elt-over-etl)
- [Technologies Used](#technologies-used)

---

## 🎯 Phase 1: Dataset Selection & Social Justification

### Dataset: USGS Earthquake Data

**Source:** [USGS Earthquake API](https://earthquake.usgs.gov/fdsnws/event/1/)  
**Update Frequency:** Real-time (updated continuously)  
**Coverage:** Global seismic activity (magnitude ≥ 2.5)

### What Real-World Problem Does This Address?

Earthquakes pose significant risks to:
- **Human Life:** Thousands of casualties annually worldwide
- **Infrastructure:** Billions in property damage and economic losses
- **Social Stability:** Displacement of communities and disruption of services

This pipeline enables:
- ✅ **Early Warning Systems:** Pattern analysis for predictive modeling
- ✅ **Urban Planning:** Evidence-based building code regulations
- ✅ **Emergency Response:** Optimized resource allocation
- ✅ **Risk Assessment:** Insurance and policy decision support
- ✅ **Scientific Research:** Long-term seismological trend analysis

### Who Benefits?

| Stakeholder | Benefit |
|------------|---------|
| 🏗️ **Urban Planners** | Design earthquake-resistant infrastructure in high-risk zones |
| 🚨 **Emergency Services** | Optimize resource allocation and response protocols |
| 📊 **Policy Makers** | Create evidence-based disaster preparedness regulations |
| 👥 **Citizens** | Increased risk awareness and safety education |
| 💼 **Insurance Companies** | Accurate risk modeling and premium calculations |
| 🔬 **Scientists** | Advance seismological research and prediction models |

### Why ELT Architecture?

1. **Growing Dataset:** USGS records ~150 earthquakes daily worldwide
2. **Data Preservation:** Original seismic readings must remain intact for scientific validation
3. **Flexible Transformations:** Magnitude scales and risk algorithms can evolve without re-extraction
4. **Multiple Analysis Needs:** Same raw data serves different analytical purposes
5. **Performance:** SQL transformations in PostgreSQL are faster than Python preprocessing

---

## ⚙️ Phase 2: ELT Pipeline Implementation

### Architecture Overview

```
┌─────────────────┐
│  USGS API       │
│  (Data Source)  │
└────────┬────────┘
         │ Extract (Python)
         ▼
┌─────────────────┐
│  raw_earthquakes│  ◄── Raw JSON (No Transformations)
│  (PostgreSQL)   │
└────────┬────────┘
         │ Transform (SQL)
         ▼
┌─────────────────┐
│ analytics_      │  ◄── Cleaned & Enriched Data
│ earthquakes     │
└─────────────────┘
         │
         ▼
┌─────────────────┐
│   Dashboard     │
│  (Matplotlib)   │
└─────────────────┘
```

### Pipeline Components

#### 1. **Extract (E)** - Python Task
- **Function:** Fetch data from USGS API
- **Frequency:** Daily (`@daily` schedule)
- **Data:** Last 24 hours of earthquakes (magnitude ≥ 2.5)
- **Output:** Raw JSON passed via XCom to Load task

```python
# Extract from USGS API
url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
params = {
    'format': 'geojson',
    'starttime': start_time,
    'endtime': end_time,
    'minmagnitude': 2.5
}
```

#### 2. **Load (L)** - Python Task
- **Function:** Store raw JSON exactly as received
- **Destination:** `raw_earthquakes` table in PostgreSQL
- **Critical:** NO transformations, NO cleaning at this stage
- **Duplicate Handling:** `ON CONFLICT DO NOTHING`

```sql
-- Raw data table structure
CREATE TABLE raw_earthquakes (
    id SERIAL PRIMARY KEY,
    earthquake_id VARCHAR(50) UNIQUE,
    raw_json JSONB NOT NULL,  -- Stores original API response
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    api_source VARCHAR(100)
);
```

#### 3. **Transform (T)** - SQL Task
- **Function:** Clean, enrich, and categorize data in PostgreSQL
- **Source:** `raw_earthquakes` table
- **Destination:** `analytics_earthquakes` table
- **Transformations:**
  - Parse JSONB to structured columns
  - Categorize magnitude (Minor/Light/Moderate/Strong/Major)
  - Categorize depth (Shallow/Intermediate/Deep)
  - Calculate risk level (Low/Medium/High)
  - Extract temporal features (day of week, hour)
  - Parse geographic regions

```sql
-- Example transformation (magnitude categorization)
CASE 
    WHEN magnitude < 3.0 THEN 'Minor'
    WHEN magnitude < 5.0 THEN 'Light'
    WHEN magnitude < 6.0 THEN 'Moderate'
    WHEN magnitude < 7.0 THEN 'Strong'
    ELSE 'Major'
END as magnitude_category
```

### Mandatory Features Implemented

#### ✅ Scheduling
- **Schedule:** `@daily` (runs every day at midnight UTC)
- **Catchup:** Disabled to prevent backfilling historical data

#### ✅ Error Handling
- **Retries:** 3 automatic retries with 5-minute delays
- **SQL Error Catching:** Wrapped in try-except blocks
- **Failure Logging:** Errors recorded in `load_history` table
- **Database Connection Handling:** Proper cleanup of connections

#### ✅ Scalability Features
- **Incremental Loads:** Only new earthquakes inserted (duplicate handling)
- **Indexed Columns:** Fast queries on `occurred_at`, `magnitude`, `region`
- **SQL Push-down:** Transformations executed in database, not Python
- **JSONB Storage:** Efficient semi-structured data handling in PostgreSQL

---

## 📊 Phase 3: Analytics Dashboard

### Dashboard Overview

Built with **Python + Matplotlib**, querying **ONLY** the `analytics_earthquakes` table (not raw data).

### Key Performance Indicators (KPIs)

| KPI | Description | Sample Value |
|-----|-------------|--------------|
| 📊 **Total Earthquakes** | Last 30 days | 34 |
| ⚠️ **High-Risk Events** | Magnitude ≥ 6.0 & Depth < 70km | 0 |
| 📈 **Average Magnitude** | Mean of all events | 4.04 |
| 🌍 **Most Active Region** | Region with most events | Alaska |

### Visualizations

#### 1. **Earthquakes by Magnitude Category**
- Bar chart showing distribution across Minor/Light/Moderate/Strong/Major
- Insight: Most events are "Light" (magnitude 3-5)

#### 2. **Risk Level Distribution**
- Pie chart: Low (82.4%) / Medium (17.6%) / High (0%)
- Insight: Majority pose low risk to infrastructure

#### 3. **Top 10 Countries by Earthquake Count**
- Horizontal bar chart showing geographic distribution
- Insight: "Other" category dominates, indicating global distribution

#### 4. **Magnitude vs Depth Correlation**
- Scatter plot with color-coded magnitude
- Insight: Shallow earthquakes (< 70km) can have high magnitudes

#### 5. **Earthquakes by Depth Category**
- Bar chart: Shallow / Intermediate / Deep
- Insight: 70.6% are shallow, posing greater surface damage risk

#### 6. **Earthquake Frequency by Hour of Day**
- Line plot showing temporal patterns
- Insight: Seismic activity distributed throughout the day (no clear pattern)

### Social Impact Insights Generated

```
1. URBAN PLANNING INSIGHTS:
   - Alaska shows highest activity → needs infrastructure reinforcement
   
2. EMERGENCY PREPAREDNESS:
   - Average magnitude of 4.04 → public awareness campaigns needed
   
3. POLICY RECOMMENDATIONS:
   - 70.6% shallow earthquakes → enhanced monitoring required
   
4. RISK MITIGATION:
   - High-risk zones identified → prioritize early warning systems
```

---

## 🚀 Installation & Setup

### Prerequisites

- Docker Desktop for Windows/Mac/Linux
- At least 4GB RAM allocated to Docker
- Git (for cloning repository)

### Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/earthquake-elt-pipeline.git
cd earthquake-elt-pipeline

# 2. Start all services
docker-compose up -d

# 3. Wait 2-3 minutes for initialization
docker ps  # Verify all containers are running

# 4. Access Airflow UI
# Browser: http://localhost:8080
# Username: admin
# Password: admin
```

### Configure PostgreSQL Connection

1. Go to **Admin** → **Connections** in Airflow UI
2. Edit `postgres_default`:
   ```
   Host: postgres
   Schema: airflow
   Login: airflow
   Password: airflow
   Port: 5432
   ```
3. Click **Test** → Should show "Connection successfully tested"
4. Click **Save**

### Enable & Run the DAG

1. Go to **DAGs** → Find `earthquake_elt_pipeline`
2. Toggle it **ON** (left side)
3. Click **Play button** (▶️) to trigger manually
4. Monitor execution in **Graph View**

### Generate Dashboard

```bash
# Install Python dependencies
pip install pandas matplotlib seaborn psycopg2-binary

# Run dashboard script
python scripts/earthquake_dashboard.py
```

**Output:** 3 PNG files with visualizations

---

## 📁 Project Structure

```
earthquake-elt-pipeline/
├── dags/
│   └── earthquake_etl_dag.py       # Airflow DAG definition
├── scripts/
│   └── earthquake_dashboard.py     # Dashboard generation
├── logs/                            # Airflow logs
├── plugins/                         # Custom Airflow plugins
├── docker-compose.yml               # Docker services configuration
├── init-db.sql                      # PostgreSQL initialization
├── requirements.txt                 # Python dependencies
└── README.md                        # This file
```

---

## 📸 Results & Evidence

### 1. Airflow DAG Execution

**Graph View - All Tasks Successful:**

![DAG Success](path/to/dag_success_screenshot.png)

- ✅ **extract_data:** Fetched 34 earthquakes from USGS API
- ✅ **load_raw_data:** Loaded raw JSON to `raw_earthquakes` table
- ✅ **transform_data:** Created analytics table with enriched data

### 2. Raw Data Table (Preserved)

```sql
SELECT earthquake_id, api_source, extracted_at, 
       LEFT(raw_json::text, 100) as json_preview
FROM raw_earthquakes
LIMIT 5;
```

**Output:**
```
earthquake_id       | api_source | extracted_at              | json_preview
--------------------|------------|---------------------------|------------------
ak024dxr6u49        | USGS API   | 2025-11-30 15:15:42       | {"id":"ak024dxr6u49","type":"Feature","properties":{"mag":2.9...
```

**Evidence:** Raw JSON is stored unchanged, exactly as received from API.

### 3. Analytics Data Table (Transformed)

```sql
SELECT earthquake_id, magnitude, magnitude_category, 
       depth_km, depth_category, risk_level, place
FROM analytics_earthquakes
ORDER BY magnitude DESC
LIMIT 5;
```

**Output:**
```
earthquake_id | magnitude | magnitude_category | depth_km | depth_category | risk_level | place
--------------|-----------|-------------------|----------|----------------|------------|------------------
us7000nxyz    | 5.8       | Moderate          | 10.5     | Shallow        | Medium     | 45 km S of Tokyo
```

**Evidence:** Transformed data with categorizations and calculated fields.

### 4. Dashboard Visualizations

**Main Dashboard (6 Charts):**

![Dashboard](path/to/earthquake_dashboard.png)

**Time Series Analysis:**

![Time Series](path/to/time_series.png)

### 5. Load History

```sql
SELECT load_date, records_loaded, status 
FROM load_history 
ORDER BY created_at DESC 
LIMIT 5;
```

**Output:**
```
load_date           | records_loaded | status
--------------------|----------------|--------
2025-11-30 15:15:42 | 34             | SUCCESS
2025-11-29 00:00:00 | 28             | SUCCESS
```

---

## 🤔 Why ELT Over ETL?

### ELT Advantages for This Project

| Aspect | ETL Approach | ELT Approach (Used) |
|--------|-------------|---------------------|
| **Data Preservation** | ❌ Raw data lost after transformation | ✅ Original JSON preserved in `raw_earthquakes` |
| **Transformation Flexibility** | ❌ Must re-extract if logic changes | ✅ Re-run SQL transforms on existing raw data |
| **Performance** | ❌ Python preprocessing bottleneck | ✅ PostgreSQL SQL is faster for aggregations |
| **Multiple Views** | ❌ Need separate ETL pipelines | ✅ Create multiple analytics tables from same raw |
| **Scalability** | ❌ Python memory limits | ✅ Database handles large datasets efficiently |
| **Audit Trail** | ❌ Can't verify original data | ✅ Always have source of truth |

### Real-World ELT Scenarios

1. **Scientific Research:** Seismologists can reanalyze raw data with updated algorithms
2. **Regulatory Compliance:** Original data preserved for audit requirements
3. **Machine Learning:** Data scientists can extract different features without re-ingestion
4. **Multi-Team Analysis:** Different departments analyze same raw data differently

---

## 🛠️ Technologies Used

| Technology | Version | Purpose |
|------------|---------|---------|
| **Apache Airflow** | 2.7.3 | Workflow orchestration |
| **PostgreSQL** | 14 | Data warehouse (raw + analytics) |
| **Docker & Docker Compose** | Latest | Containerization |
| **Python** | 3.8+ | Scripting & dashboard |
| **Pandas** | 2.1.3 | Data manipulation |
| **Matplotlib** | 3.8+ | Data visualization |
| **psycopg2** | 2.9.9 | PostgreSQL connector |
| **USGS API** | v1 | Earthquake data source |

---

## 📝 Troubleshooting

### Issue: Airflow webserver won't start

**Solution:**
```bash
docker-compose down -v
docker-compose up -d
docker logs elt-airflow-webserver-1
```

### Issue: No data in analytics table

**Solution:**
```bash
# Check if DAG ran successfully
docker logs elt-airflow-scheduler-1

# Manually trigger DAG
# Go to Airflow UI → earthquake_elt_pipeline → Trigger DAG
```

### Issue: PostgreSQL connection failed

**Solution:**
- Verify connection in Airflow UI (Admin → Connections)
- Ensure `postgres_default` uses:
  - Host: `postgres` (not `localhost`)
  - Login: `airflow`
  - Password: `airflow`

---

## 📚 Additional Resources

- [USGS Earthquake API Documentation](https://earthquake.usgs.gov/fdsnws/event/1/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [PostgreSQL JSONB Functions](https://www.postgresql.org/docs/current/functions-json.html)
