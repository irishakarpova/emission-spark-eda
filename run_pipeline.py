import downloader
import engine
import os

# The specific Kaggle dataset you want
KAGGLE_SLUG = "cisautomotiveapi/large-car-dataset" 

def run_ingestion():
    print("--- STEP 1: KAGGLE INGESTION ---")
    try:
        # 1. Download the data
        csv_path = downloader.download_dataset(KAGGLE_SLUG)
        print(f"✅ Data downloaded to: {csv_path}")

        # 2. Initialize Spark and Load
        print("--- STEP 2: SPARK PROCESSING ---")
        spark_df = engine.load_dataset(csv_path)
        
        # 3. Show a preview to confirm
        spark_df.show(10)
        
        # 4. Save to Parquet for the Streamlit App to use
        output_parquet = "/app/data/processed_data.parquet"
        spark_df.write.mode("overwrite").parquet(output_parquet)
        print(f"✅ Success! Processed data saved to {output_parquet}")

    except Exception as e:
        print(f"Pipeline failed: {e}")

if __name__ == "__main__":
    run_ingestion()