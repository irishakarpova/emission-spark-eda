import streamlit as st
from engine import DataEngine
import pandas as pd
from pyspark.sql import functions as F
import os

st.set_page_config(page_title="VIN Data Pipeline", layout="wide")
st.title("VIN Processing Pipeline")

# Paths
CSV_PATH = "/app/data/data.csv"
PARQUET_PATH = "/app/data/cleaned_emissions.parquet"
MAPPING_PATH = "/app/data/nhtsa_mapping.parquet"
FINAL_EXPORT_PATH = "/app/data/labeled_vehicle_data.parquet"

if 'engine_instance' not in st.session_state:
    st.session_state['engine_instance'] = DataEngine()
data_engine = st.session_state['engine_instance']

# --- STEP 1: LOAD DATA ---
st.header("Step 1: Load & Prune")
if st.button("Connect Data"):
    with st.spinner("Loading and stripping unnecessary data..."):
        df = data_engine.load_dataset(CSV_PATH, PARQUET_PATH)
        if df:
            df = data_engine.remove_specific_column(df)
            st.session_state['df'] = df
            st.success("Connected! Pruned heavy columns.")

if 'df' in st.session_state:
    st.divider()
    if st.button("Calculate Row Count"):
        with st.spinner("Scanning..."):
            st.metric("Total Rows", f"{st.session_state['df'].count():,}")

    st.write("### Current Data Preview (Raw)")
    st.dataframe(st.session_state['df'].limit(10).toPandas())

# --- STEP 2: MAPPING GENERATION ---
st.divider()
st.header("Step 2: Engine Mapping")
map_limit = st.number_input("Patterns to map", value=50, min_value=1)

if st.button("Run Mapping Process"):
    if 'df' in st.session_state:
        progress_bar = st.progress(0)
        
        # 1. Create mapping dictionary
        mapping_df = data_engine.create_mapping_file(
            st.session_state['df'], MAPPING_PATH, progress_bar, limit=map_limit
        )
        
        if mapping_df is not None:
            with st.expander("🔍 Inspect Mapping Dictionary"):
                st.write("Columns in Mapping:", mapping_df.columns)
                st.dataframe(mapping_df.limit(5).toPandas())

            # 2. Perform the Join
            final_df = data_engine.label_dataset(st.session_state['df'], MAPPING_PATH)
            
            # --- THE FIX: Match logic to your actual mapping columns ---
            cols_after_join = final_df.columns
            
            # We look for lowercase 'cylinders' because that's what your debug showed
            if "cylinders" in cols_after_join:
                st.session_state['final_df'] = final_df 
                st.success("Join Successful! Engine data attached.")
                
                with st.spinner("Hunting 13GB for matches..."):
                    # Dynamically select columns that exist to avoid crash
                    select_cols = ["VIN"]
                    if "displacement_l" in cols_after_join: select_cols.append(F.round("displacement_l", 1).alias("Disp_L"))
                    if "cylinders" in cols_after_join: select_cols.append("cylinders")
                    if "horsepower" in cols_after_join: select_cols.append(F.round("horsepower", 0).alias("HP"))
                    
                    success_preview = final_df.filter(F.col("cylinders").isNotNull())\
                                            .select(*select_cols)\
                                            .limit(10)\
                                            .toPandas()
                    
                    if not success_preview.empty:
                        st.subheader("Results: 10-Row Truth Check")
                        st.dataframe(success_preview, use_container_width=True)
                    else:
                        st.warning("Join worked, but no matches found. Check if your VIN patterns match.")
            else:
                st.error(f"Join failed: Engine data not attached. Available: {cols_after_join}")
                st.info("The mapping file is missing 'vin_pattern' or 'cylinders'. Please delete nhtsa_mapping.parquet and try again.")
    else:
        st.error("Load data in Step 1 first.")

# --- STEP 3: FINAL EXPORT ---
st.divider()
st.header("Step 3: Permanent Save")
if st.button("Export Labeled Dataset"):
    if 'final_df' in st.session_state:
        with st.spinner("Processing all 13GB... this will take time."):
            data_engine.save_final_dataset(st.session_state['final_df'], FINAL_EXPORT_PATH)
            st.success("Exported successfully!")
            st.balloons()