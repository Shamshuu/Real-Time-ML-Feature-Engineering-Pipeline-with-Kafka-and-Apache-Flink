import streamlit as st
import json
import threading
import time
import os
import requests
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
FEATURE_STORE_TOPIC = os.getenv('FEATURE_STORE_TOPIC', 'feature-store')
FLINK_JOBMANAGER_URL = os.getenv('FLINK_JOBMANAGER_URL', 'http://flink-jobmanager:8081')

st.set_page_config(page_title="Real-Time Feature Dashboard", layout="wide")

st.title("🚀 Real-Time Feature engineering Pipeline")
st.markdown("---")

# Session state for features
if 'features' not in st.session_state:
    st.session_state.features = {} # key: entity_id:feature_name -> {value, computed_at}
if 'metrics' not in st.session_state:
    st.session_state.metrics = {"late_dropped": 0, "watermark_lag": "N/A"}

# Kafka Consumer Thread
def consume_features():
    consumer = KafkaConsumer(
        FEATURE_STORE_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.loads(v.decode('utf-8')),
        group_id='dashboard_consumer'
    )
    for message in consumer:
        val = message.value
        entity_id = val['entity_id']
        feature_name = val['feature_name']
        key = f"{entity_id}:{feature_name}"
        st.session_state.features[key] = {
            "entity_id": entity_id,
            "feature_name": feature_name,
            "value": val['feature_value'],
            "computed_at": val['computed_at']
        }

# Metrics Fetcher Thread (Polling Flink API)
def fetch_flink_metrics():
    while True:
        try:
            # Get list of jobs
            jobs_resp = requests.get(f"{FLINK_JOBMANAGER_URL}/jobs").json()
            running_jobs = [j['id'] for j in jobs_resp['jobs'] if j['status'] == 'RUNNING']
            
            if running_jobs:
                job_id = running_jobs[0]
                # Example: Fetching specific metrics if configured.
                # For now, we simulate/placeholder or fetch what's available
                # In a real setup, we might hit /jobs/:jid/metrics?get=numLateRecordsDropped
                m_resp = requests.get(f"{FLINK_JOBMANAGER_URL}/jobs/{job_id}/metrics?get=numLateRecordsDropped").json()
                if m_resp:
                    st.session_state.metrics["late_dropped"] = m_resp[0].get('value', 0)
        except Exception as e:
            pass
        time.sleep(5)

if 'threads_started' not in st.session_state:
    threading.Thread(target=consume_features, daemon=True).start()
    threading.Thread(target=fetch_flink_metrics, daemon=True).start()
    st.session_state.threads_started = True

# Layout
col1, col2 = st.columns([1, 2])

with col1:
    st.header("🛠 Pipeline Health")
    
    # Metrics
    st.metric("Late Events Dropped", st.session_state.metrics["late_dropped"])
    
    # Freshness calculation
    if st.session_state.features:
        latest_ts = max(datetime.fromisoformat(f['computed_at'].replace('Z', '')) for f in st.session_state.features.values())
        lag = (datetime.now() - latest_ts).total_seconds()
        st.metric("Watermark Lag (Simulated)", f"{int(lag)}s")
    else:
        st.metric("Watermark Lag", "N/A")

    st.markdown("---")
    st.header("🔍 Entity Viewer")
    search_id = st.text_input("Enter User ID or Content ID (e.g., user_1, content_1)", value="user_1")

with col2:
    st.header("📊 Latest Features")
    
    # Filter features for search_id
    filtered = [f for f in st.session_state.features.values() if f['entity_id'] == search_id or f['entity_id'].startswith(search_id + ":")]
    
    if filtered:
        df = pd.DataFrame(filtered)
        # Calculate freshness for each
        df['computed_at_dt'] = pd.to_datetime(df['computed_at'].str.replace('Z', ''))
        df['freshness_sec'] = (datetime.now() - df['computed_at_dt']).dt.total_seconds().astype(int)
        
        st.table(df[['feature_name', 'value', 'computed_at', 'freshness_sec']])
        
        # Affinity Plot
        affinity_df = df[df['feature_name'] == 'category_affinity_count']
        if not affinity_df.empty:
            st.subheader("Category Affinity")
            # Extract category from entity_id (format user_id:category)
            affinity_df['category'] = affinity_df['entity_id'].apply(lambda x: x.split(':')[-1])
            affinity_df['value'] = affinity_df['value'].astype(int)
            st.bar_chart(affinity_df.set_index('category')['value'])
    else:
        st.info(f"No features found for {search_id} yet. Wait for window to tumble...")

# Auto-refresh
time.sleep(2)
st.rerun()
