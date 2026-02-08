import streamlit as st
import engine
import time
import shutil
import os
import plotly.express as px

st.title("Data Pipeline")

CSV_PATH = "/app/data/data.csv"
PARQUET_PATH = "/app/data/cleaned_emissions.parquet"


# --- STEP 1: LOAD ---
if st.button("Connect to Dataset"):
    df = engine.load_dataset(CSV_PATH, PARQUET_PATH)
    st.session_state['df'] = df
    
    source = "Parquet" if "parquet" in str(df).lower() else "CSV"
    st.success(f"Connected via {source}")
    st.dataframe(df.limit(10).toPandas())

# --- STEP 2: REMOVE COLUMNS ---
st.divider()
st.header("Step 2: Data Cleaning")

if 'df' in st.session_state:

    if st.button("Execute Column Removal"):
        with st.spinner("Cleaning..."):
            df_cleaned = engine.remove_specific_column(st.session_state['df'])
            st.session_state['df_cleaned'] = df_cleaned # Save to session state
            st.success("Columns removed successfully!")

    # Display results if the cleaned data exists in the session
    if 'df_cleaned' in st.session_state:
        df_c = st.session_state['df_cleaned']
        col1, col2 = st.columns(2)
        col1.metric("Original Columns", len(st.session_state['df'].columns))
        col2.metric("New Columns", len(df_c.columns))
        st.dataframe(df_c.limit(5).toPandas())
else:
    st.info("Waiting for Data")

# --- STEP 3: SAVE TO PARQUET ---
st.divider()
st.header("Step 3: Compress & Save")

if 'df_cleaned' in st.session_state:
    if st.button("Start Compression"):
        start_time = time.time()
        with st.spinner("Writing Parquet"):
            try:
                engine.save_as_parquet(st.session_state['df_cleaned'], PARQUET_PATH)
                duration = time.time() - start_time
                st.success(f"File saved in {duration:.1f} seconds!")
                st.balloons()
            except Exception as e:
                st.error(f"Error: {e}")
else:
    st.warning("")  
    
    
    
    
# --- STEP 3.5: EXPLORE SAVED DATA ---
st.divider()
st.header("Step 3.5: Open & Inspect Parquet")

if st.button("Open Saved Parquet"):
    try:
        # Point Spark at the folder we saw in your terminal
        saved_df = engine.get_spark().read.parquet(PARQUET_PATH)
        
        # Store it so Step 4 can use it
        st.session_state['df_cleaned'] = saved_df
        
        # Show stats
        row_count = saved_df.count()
        st.success(f"Successfully opened Parquet! Total rows in sample: {row_count}")
        
        st.write("### Data Preview (First 10 rows)")
        st.dataframe(saved_df.limit(10).toPandas())
        
        st.write("### Schema (Columns & Types)")
        st.json(saved_df.schema.jsonValue())
        
    except Exception as e:
        st.error(f"Could not open Parquet: {e}")
        st.info("Tip: Make sure the _SUCCESS file exists in the folder.")    


# --- STEP 4: VISUALIZATION ---
st.divider()
st.header("Step 4: Data Insights")

if 'df' in st.session_state:
    df = st.session_state['df']
    
    category_col = st.selectbox("Select category to visualize:", df.columns)
    
    if st.button("Generate Chart"):
        with st.spinner("Calculating distributions..."):
            # Spark handles the heavy counting on the Parquet data
            counts = df.groupBy(category_col).count().orderBy("count", ascending=False).limit(10).toPandas()
            
            # Create a nice Plotly chart
            fig = px.bar(counts, x=category_col, y="count", 
                         title=f"Top 10 {category_col} Distribution",
                         color="count", 
                         color_continuous_scale='Viridis')
            
            st.plotly_chart(fig, use_container_width=True)