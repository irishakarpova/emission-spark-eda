import streamlit as st
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

st.set_page_config(layout="wide", page_title="Emission EDA")

@st.cache_resource
def get_spark():
    return SparkSession.builder \
        .appName("EmissionAnalysis") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

spark = get_spark()

st.title("Emission Data")


path = "/app/data/data.csv" 

if st.sidebar.button("Step 1: Connect to Data"):
    df = spark.read.csv(path, header=True, inferSchema=False)
    
    st.write("### Data Overview")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Column Count", len(df.columns))
    with col2:

        st.info("Click below to count total rows (slow)")
        if st.button("Count Rows"):
            st.write(f"Total Rows: {df.count():,}")
            
    st.write("### First 10 Rows (Preview)")
    st.dataframe(df.limit(10).toPandas())

    st.write("### Data Sampling for Visualization")
    st.write("Taking a 0.1% random sample to keep the chart fast...")
    sample_pd = df.sample(fraction=0.001).toPandas()
    st.line_chart(sample_pd.iloc[:, 1]) 