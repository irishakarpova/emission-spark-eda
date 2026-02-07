import streamlit as st
import engine

st.title("Data Transformation Pipeline")

CSV_PATH = "/app/data/data.csv"

# STEP 1: LOAD (From previous step) ---
if st.button("Step 1: Show Raw Data"):
    st.session_state['df'] = engine.load_dataset(CSV_PATH)
    st.write("Current Columns:", st.session_state['df'].columns)
    st.dataframe(st.session_state['df'].limit(5).toPandas())

# STEP 2: REMOVE COLUMNS ---
st.divider()
st.header("Step 2: Data cleaning")

if 'df' in st.session_state:
    if st.button("Remove irrelevant columns"):
        # Call the engine logic
        df_cleaned = engine.remove_specific_column(st.session_state['df'])
        
        # Update session state with the new cleaned dataframe
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

