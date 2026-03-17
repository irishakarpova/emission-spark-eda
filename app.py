import os
import streamlit as st
import pandas as pd
from pyspark.sql import functions as F
from engine import DataEngine

st.set_page_config(page_title="VIN Data Pipeline", layout="wide")
st.title("VIN Processing Pipeline")

# PATHS 
CSV_PATH = "/app/data/data.csv"
PARQUET_PATH = "/app/data/cleaned_emissions.parquet"
MAPPING_PATH = "/app/data/nhtsa_mapping.parquet"
FINAL_EXPORT_PATH = "/app/data/labeled_vehicle_data.parquet"

# Initialize Engine
if 'engine_instance' not in st.session_state:
    st.session_state['engine_instance'] = DataEngine()
data_engine = st.session_state['engine_instance']

# Get file size in GB
def get_dir_size_gb(path):
    if not os.path.exists(path): return 0
    if os.path.isfile(path):
        return os.path.getsize(path) / (1024**3)
# For Parquet folders
    total = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total += os.path.getsize(fp)
    return total / (1024**3)

# --- STEP 1: DATA LOADING ---
st.header("Step 1: Data Loading...")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Optimizer")
    if st.button("Convert CSV to Parquet"):
        with st.spinner("Converting CSV... "):
            success = data_engine.convert_csv_to_parquet(CSV_PATH, PARQUET_PATH)
            if success:
                st.success("Conversion Complete")
                st.info(f"Parquet Size: {get_dir_size_gb(PARQUET_PATH):.2f}")
            else:
                st.error("Conversion failed. Check memory/logs.")

with col2:
    st.subheader("Loader")
    if os.path.exists(FINAL_EXPORT_PATH):
        load_mode = st.radio("Source:", ["Labeled Results", "Optimized Parquet", "Raw CSV Sample"], horizontal=True)
    elif os.path.exists(PARQUET_PATH):
        load_mode = st.radio("Source:", ["Optimized Parquet", "Raw CSV Sample"], horizontal=True)
    else:
        load_mode = "Raw CSV Sample"

    if st.button("Connect & Load"):
        with st.spinner("Loading into Spark..."):
            if load_mode == "Labeled Results":
                df = data_engine.spark.read.parquet(FINAL_EXPORT_PATH)
            elif load_mode == "Optimized Parquet":
                df = data_engine.spark.read.parquet(PARQUET_PATH)
            else:
                df = data_engine.load_dataset(CSV_PATH, PARQUET_PATH)
            
            if df:
                st.session_state['df'] = df
                st.session_state['load_mode'] = load_mode
                st.success(f"Connected to {load_mode}!")

# --- DATA PREVIEW ---
if 'df' in st.session_state:
    df = st.session_state['df']
    st.divider()
    
    st.subheader("Dataset Preview")
    m_col1, m_col2, m_col3 = st.columns(3)
    
    with st.spinner("Calculating dataset metadata..."):
        row_count = df.count()
        current_columns = df.columns
    
    m_col1.metric("Total Rows", f"{row_count:,}")
    m_col2.metric("Features (Columns)", len(current_columns))
    
    if st.session_state.get('load_mode') == "Optimized Parquet":
        size_gb = get_dir_size_gb(PARQUET_PATH)
        m_col3.metric("Disk Usage", f"{size_gb:.2f} GB", delta="-85% vs CSV")

    # --- NEW: Feature List Section ---
    with st.expander("View All Available Features"):
        st.write(f"The following **{len(current_columns)}** columns were retained after cleaning:")
        
        # Displaying columns in a clean 4-column grid
        cols = st.columns(4)
        for i, column_name in enumerate(sorted(current_columns)):
            cols[i % 4].code(column_name)
            
    # PREVIEW
    st.subheader("Data Preview")
    has_engine_data = "cylinders" in df.columns
    if has_engine_data:
        matched_df = df.filter(F.col("cylinders").isNotNull()).limit(10).toPandas()
        st.dataframe(matched_df if not matched_df.empty else df.limit(10).toPandas(), use_container_width=True)
    else:
        st.dataframe(df.limit(10).toPandas(), use_container_width=True)

# --- STEP 2: MAPPING GENERATION ---
st.divider()
st.header("Step 2: Engine Mapping")
map_limit = st.number_input("Patterns to map", value=1000, min_value=1)

if st.button("Run Mapping Process"):
    if 'df' in st.session_state:
        working_df = st.session_state['df']

        if st.session_state.get('load_mode') == "Optimized Parquet":
            working_df = working_df.sample(False, 0.01, seed=42)
            st.warning("Using 1% sample of Parquet for API mapping to prevent network timeout.")

        progress_bar = st.progress(0)
        mapping_df = data_engine.create_mapping_file(working_df, MAPPING_PATH, progress_bar, limit=map_limit)
        
        if mapping_df is not None:
            final_df = data_engine.label_dataset(st.session_state['df'], MAPPING_PATH)
            st.session_state['final_df'] = final_df 
            
            with st.spinner("Calculating match success..."):
                match_count = final_df.filter(F.col("cylinders").isNotNull()).count()
                st.metric("Successful Matches Found", f"{match_count:,}")
                st.dataframe(final_df.filter(F.col("cylinders").isNotNull()).limit(10).toPandas())
    else:
        st.error("Load data in Step 1.")

# --- STEP 3: FINAL EXPORT ---
st.divider()
st.header("Step 3: Save Processed Data")

if st.button("Export Labeled Dataset"):
    if 'final_df' in st.session_state:
        with st.spinner("Saving results..."):
            success = data_engine.save_final_dataset(st.session_state['final_df'], FINAL_EXPORT_PATH)
            if success:
                st.success(f"Saved to {FINAL_EXPORT_PATH}")
                st.balloons()