import streamlit as st
import engine
import time
import plotly.express as px

st.title("Data Transformation Pipeline")

CSV_PATH = "/app/data/data.csv"
PARQUET_PATH = "/app/data/cleaned_emissions.parquet"

# STEP 1: LOAD 
if st.button("Step 1: Connect to Dataset"):
    df = engine.load_dataset(CSV_PATH, PARQUET_PATH)
    st.session_state['df'] = df
    
    # Identify which source we used
    source = "Parquet (Fast)" if "parquet" in str(df) else "CSV (Slow)"
    st.success(f"Connected via {source}")
    st.dataframe(df.limit(10).toPandas())
    
    st.write("Current Columns:", st.session_state['df'].columns)
    st.dataframe(st.session_state['df'].limit(5).toPandas())

# STEP 2: REMOVE COLUMNS 
st.divider()
st.header("Step 2: Data cleaning")

if 'df' in st.session_state:
    if st.button("Load Data"):
        df_cleaned = engine.remove_specific_column(st.session_state['df'])
        st.session_state['df_cleaned'] = df_cleaned
        st.success("Column 'Model Year Change' has been removed!")
        
        # Visual Verification
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Original Column Count:**")
            st.write(len(st.session_state['df'].columns))
        with col2:
            st.write("**New Column Count:**")
            st.write(len(df_cleaned.columns))
            
        st.write("### New Data Preview:")
        st.dataframe(df_cleaned.limit(10).toPandas())
else:
    st.warning("Run Step 1.")
    
    
# --- STEP 3: SAVE TO PARQUET ---
st.divider()
st.header("Step 3: Compress & Save to Parquet")

if 'df_cleaned' in st.session_state:

    PARQUET_PATH = "/app/data/cleaned_emissions.parquet"

    if st.button("Start Compression"):
        start_time = time.time()
        
        with st.spinner("Spark is processing the 13GB file..."):
            try:
                engine.save_as_parquet(st.session_state['df_cleaned'], PARQUET_PATH)
                
                duration = time.time() - start_time
                st.success(f"File saved in {duration:.1f} seconds.")
                st.balloons()
                
                st.info(f"Check your folder for: **cleaned_emissions.parquet**")
            except Exception as e:
                st.error(f"Error during save: {e}")
else:
    st.warning("Cmplete Step 2 before saving.")    


# --- STEP 4: VISUALIZATION ---
st.divider()
st.header("Step 4: Data Insights")

if 'df' in st.session_state:
    df = st.session_state['df']
    
    # 1. Choose a column to analyze (e.g., 'Make' or 'Vehicle Class')
    # Let's assume you have a column named 'Make'
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