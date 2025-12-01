# Earthquake Analytics Dashboard - CORRECTED VERSION
# Phase 3: Visualization using analytics_earthquakes table

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import psycopg2
from datetime import datetime
import os

# Create output directory
os.makedirs('output', exist_ok=True)

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

# Database connection
def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )

# Load data from analytics table (NOT from raw table)
def load_analytics_data():
    conn = get_connection()
    query = """
    SELECT 
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
    FROM analytics_earthquakes
    ORDER BY occurred_at DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Export data to CSV
def export_to_csv(df):
    df.to_csv('output/analytics_earthquakes.csv', index=False)
    print("✅ Data exported to 'output/analytics_earthquakes.csv'")
    
    # Also export raw data
    conn = get_connection()
    df_raw = pd.read_sql("SELECT * FROM raw_earthquakes", conn)
    df_raw.to_csv('output/raw_earthquakes.csv', index=False)
    conn.close()
    print("✅ Raw data exported to 'output/raw_earthquakes.csv'")

# Load data
print("Loading earthquake data from analytics table...")
df = load_analytics_data()
print(f"Total earthquakes loaded: {len(df)}")
print(f"Date range: {df['occurred_at'].min()} to {df['occurred_at'].max()}")

# Export to CSV
export_to_csv(df)

# ============================================
# KEY PERFORMANCE INDICATORS (KPIs)
# ============================================
print("\n" + "="*60)
print("KEY PERFORMANCE INDICATORS")
print("="*60)

kpi_total = len(df)
kpi_high_risk = len(df[df['risk_level'] == 'High'])
kpi_avg_magnitude = df['magnitude'].mean()
kpi_most_active_region = df['region'].value_counts().index[0] if len(df) > 0 else "N/A"

print(f"📊 Total Earthquakes: {kpi_total:,}")
print(f"⚠️  High-Risk Events: {kpi_high_risk}")
print(f"📈 Average Magnitude: {kpi_avg_magnitude:.2f}")
print(f"🌍 Most Active Region: {kpi_most_active_region}")

# ============================================
# DASHBOARD VISUALIZATIONS
# ============================================

# Create figure with subplots
fig = plt.figure(figsize=(20, 12))

# Chart 1: Earthquakes by Magnitude Category
ax1 = plt.subplot(2, 3, 1)
magnitude_counts = df['magnitude_category'].value_counts()
colors = ['#2ecc71', '#f39c12', '#e74c3c', '#c0392b', '#8e44ad']
magnitude_counts.plot(kind='bar', color=colors, ax=ax1)
ax1.set_title('Earthquakes by Magnitude Category', fontsize=14, fontweight='bold')
ax1.set_xlabel('Magnitude Category')
ax1.set_ylabel('Count')
ax1.tick_params(axis='x', rotation=45)
for i, v in enumerate(magnitude_counts.values):
    ax1.text(i, v + 0.5, str(v), ha='center', fontweight='bold')

# Chart 2: Risk Level Distribution (Pie Chart)
ax2 = plt.subplot(2, 3, 2)
risk_counts = df['risk_level'].value_counts()
colors_risk = ['#2ecc71', '#f39c12', '#e74c3c']
ax2.pie(risk_counts.values, labels=risk_counts.index, autopct='%1.1f%%',
        colors=colors_risk, startangle=90)
ax2.set_title('Risk Level Distribution', fontsize=14, fontweight='bold')

# Chart 3: Top 10 Countries by Earthquake Count
ax3 = plt.subplot(2, 3, 3)
top_countries = df['country'].value_counts().head(10)
top_countries.plot(kind='barh', color='skyblue', ax=ax3)
ax3.set_title('Top 10 Countries by Earthquake Count', fontsize=14, fontweight='bold')
ax3.set_xlabel('Count')
ax3.set_ylabel('Country')
for i, v in enumerate(top_countries.values):
    ax3.text(v + 0.5, i, str(v), va='center', fontweight='bold')

# Chart 4: Magnitude vs Depth (Scatter Plot)
ax4 = plt.subplot(2, 3, 4)
scatter = ax4.scatter(df['depth_km'], df['magnitude'], 
                     c=df['magnitude'], cmap='YlOrRd', 
                     alpha=0.6, s=50)
ax4.set_title('Magnitude vs Depth Correlation', fontsize=14, fontweight='bold')
ax4.set_xlabel('Depth (km)')
ax4.set_ylabel('Magnitude')
ax4.grid(True, alpha=0.3)
plt.colorbar(scatter, ax=ax4, label='Magnitude')

# Chart 5: Earthquakes by Depth Category
ax5 = plt.subplot(2, 3, 5)
depth_counts = df['depth_category'].value_counts()
colors_depth = ['#3498db', '#9b59b6', '#e67e22']
depth_counts.plot(kind='bar', color=colors_depth, ax=ax5)
ax5.set_title('Earthquakes by Depth Category', fontsize=14, fontweight='bold')
ax5.set_xlabel('Depth Category')
ax5.set_ylabel('Count')
ax5.tick_params(axis='x', rotation=45)
for i, v in enumerate(depth_counts.values):
    ax5.text(i, v + 0.5, str(v), ha='center', fontweight='bold')

# Chart 6: Earthquakes by Hour of Day
ax6 = plt.subplot(2, 3, 6)
hourly_counts = df.groupby('hour_of_day').size()
ax6.plot(hourly_counts.index, hourly_counts.values, 
         marker='o', linewidth=2, markersize=8, color='#e74c3c')
ax6.set_title('Earthquake Frequency by Hour of Day', fontsize=14, fontweight='bold')
ax6.set_xlabel('Hour of Day (UTC)')
ax6.set_ylabel('Count')
ax6.set_xticks(range(0, 24, 3))
ax6.grid(True, alpha=0.3)
ax6.fill_between(hourly_counts.index, hourly_counts.values, alpha=0.3, color='#e74c3c')

plt.tight_layout()
plt.savefig('output/earthquake_dashboard.png', dpi=300, bbox_inches='tight')
print("\n✅ Dashboard saved as 'output/earthquake_dashboard.png'")
plt.show()

# ============================================
# ADDITIONAL ANALYSIS: HIGH-RISK EVENTS
# ============================================

print("\n" + "="*60)
print("HIGH-RISK EARTHQUAKE ANALYSIS")
print("="*60)

high_risk = df[df['risk_level'] == 'High'].sort_values('magnitude', ascending=False)

if len(high_risk) > 0:
    print(f"\n⚠️  Total High-Risk Events: {len(high_risk)}")
    print("\nTop 5 High-Risk Earthquakes:")
    print(high_risk[['occurred_at', 'magnitude', 'depth_km', 'place']].head(10).to_string(index=False))
    
    # Plot high-risk events
    fig2, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # High-risk by country
    high_risk_countries = high_risk['country'].value_counts().head(10)
    high_risk_countries.plot(kind='barh', color='#e74c3c', ax=ax1)
    ax1.set_title('High-Risk Earthquakes by Country', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Count')
    
    # High-risk magnitude distribution
    ax2.hist(high_risk['magnitude'], bins=15, color='#e74c3c', edgecolor='black', alpha=0.7)
    ax2.set_title('High-Risk Events - Magnitude Distribution', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Magnitude')
    ax2.set_ylabel('Frequency')
    ax2.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig('output/high_risk_analysis.png', dpi=300, bbox_inches='tight')
    print("\n✅ High-risk analysis saved as 'output/high_risk_analysis.png'")
    plt.show()
else:
    print("\n✅ No high-risk earthquakes in this period (magnitude < 6.0 or depth > 70km)")

# ============================================
# TIME SERIES ANALYSIS - CORRECTED
# ============================================

print("\n" + "="*60)
print("TIME SERIES ANALYSIS")
print("="*60)

# Convert to datetime
df['datetime'] = pd.to_datetime(df['occurred_at'])
df['date'] = df['datetime'].dt.date

# Check how many unique dates we have
unique_dates = df['date'].nunique()
print(f"Data spans {unique_dates} unique day(s)")

if unique_dates == 1:
    # Single day: show hourly activity
    print("Showing HOURLY activity (single day of data)")
    
    hourly_data = df.set_index('datetime').resample('H').size()
    
    fig3, ax = plt.subplots(figsize=(16, 6))
    
    ax.plot(hourly_data.index, hourly_data.values, 
            marker='o', linewidth=2, markersize=8, color='#3498db')
    ax.fill_between(hourly_data.index, hourly_data.values, 
                    alpha=0.3, color='#3498db')
    
    ax.set_title(f'Hourly Earthquake Activity - {df["date"].iloc[0]}', 
                 fontsize=14, fontweight='bold')
    ax.set_xlabel('Time (UTC)')
    ax.set_ylabel('Number of Earthquakes')
    ax.grid(True, alpha=0.3)
    
    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('output/hourly_activity.png', dpi=300, bbox_inches='tight')
    print("✅ Hourly activity plot saved as 'output/hourly_activity.png'")
    plt.show()
    
else:
    # Multiple days: show daily activity
    print("Showing DAILY activity (multiple days of data)")
    
    daily_counts = df.groupby('date').size().sort_index()
    dates = pd.to_datetime(daily_counts.index)
    
    fig3, ax = plt.subplots(figsize=(16, 6))
    
    ax.plot(dates, daily_counts.values, 
            marker='o', linewidth=2, markersize=8, color='#3498db')
    ax.fill_between(dates, daily_counts.values, alpha=0.3, color='#3498db')
    
    ax.set_title('Daily Earthquake Activity', fontsize=14, fontweight='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel('Number of Earthquakes')
    ax.grid(True, alpha=0.3)
    
    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    if unique_dates <= 7:
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    else:
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, unique_dates//10)))
    
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('output/daily_activity.png', dpi=300, bbox_inches='tight')
    print("✅ Daily activity plot saved as 'output/daily_activity.png'")
    plt.show()

# ============================================
# SOCIAL/ENVIRONMENTAL IMPACT INSIGHTS
# ============================================

print("\n" + "="*60)
print("SOCIAL & ENVIRONMENTAL IMPACT INSIGHTS")
print("="*60)

shallow_pct = (len(df[df['depth_category'] == 'Shallow']) / len(df) * 100) if len(df) > 0 else 0

print(f"""
🌍 KEY FINDINGS & SOCIAL IMPACT:

1. URBAN PLANNING INSIGHTS:
   - Regions with frequent seismic activity require stricter building codes
   - {kpi_most_active_region} shows highest activity and needs infrastructure reinforcement
   
2. EMERGENCY PREPAREDNESS:
   - {kpi_high_risk} high-risk events require immediate response protocols
   - Average magnitude of {kpi_avg_magnitude:.2f} indicates need for public awareness campaigns
   
3. POLICY RECOMMENDATIONS:
   - Shallow earthquakes (< 70km) pose greater surface damage risk
   - {shallow_pct:.1f}% of events are shallow, requiring enhanced monitoring
   
4. RISK MITIGATION:
   - High-risk zones identified: prioritize early warning systems
   - Insurance companies can use this data for accurate risk assessment
   
5. SCIENTIFIC VALUE:
   - Pattern analysis helps predict future seismic activity
   - Data preservation enables long-term trend analysis
   
BENEFICIARIES:
✓ Urban Planners: Infrastructure design decisions
✓ Emergency Services: Resource allocation optimization
✓ Policy Makers: Evidence-based disaster preparedness regulations
✓ Citizens: Risk awareness and safety education
✓ Insurance Industry: Accurate risk modeling
✓ Scientific Community: Seismological research advancement
""")

print("\n" + "="*60)
print("WHY ELT WAS IDEAL FOR THIS PROJECT")
print("="*60)

print("""
✅ RAW DATA PRESERVATION:
   - Original seismic readings remain intact for scientific validation
   - Multiple research teams can analyze the same source data differently
   
✅ FLEXIBLE TRANSFORMATIONS:
   - Magnitude categorization can evolve (new scales, thresholds)
   - Risk algorithms can be refined without re-extracting data
   
✅ SCALABILITY:
   - USGS adds ~150 earthquakes daily worldwide
   - Database handles incremental loads efficiently
   - SQL transformations are faster than Python preprocessing
   
✅ MULTIPLE ANALYTICAL VIEWS:
   - Same raw data serves different analytical purposes
   - Easy to create new derived tables for specific research questions
   
✅ DATA WAREHOUSE APPROACH:
   - PostgreSQL's JSONB handles semi-structured API responses
   - Indexing accelerates queries on transformed data
   - Supports both operational and analytical workloads
""")

print("\n" + "="*60)
print("Dashboard generation complete! ✅")
print("="*60)
print("\nGenerated files in 'output/' directory:")
print("  1. earthquake_dashboard.png - Main dashboard with 6 charts")
if len(high_risk) > 0:
    print("  2. high_risk_analysis.png - High-risk event analysis")
if unique_dates == 1:
    print("  3. hourly_activity.png - Hourly earthquake activity")
else:
    print("  3. daily_activity.png - Daily earthquake activity trends")
print("  4. analytics_earthquakes.csv - Transformed data export")
print("  5. raw_earthquakes.csv - Raw data export")
print("\nThese visualizations use ONLY the analytics_earthquakes table,")
print("demonstrating proper ELT architecture with separated concerns.")
print("="*60)