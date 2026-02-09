from pyspark.sql import SparkSession
import os
import shutil
from kaggle.api.kaggle_api_extended import KaggleApi


def get_spark():
    return SparkSession.builder \
        .appName("EmissionCleaner") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "2g") \
        .getOrCreate()
        
        
def load_dataset(csv_path, parquet_path):
    spark = get_spark()

    if os.path.exists(parquet_path):
        try:
            print("Testing Parquet health...")
            df = spark.read \
                .option("mergeSchema", "false") \
                .parquet(parquet_path)
            df.limit(1).collect()

            print("Parquet is healthy. Loading Parquet.")
            return df

        except Exception as e:
            print("Parquet is corrupted. Deleting folder.")
            print(f"Reason: {e}")
            shutil.rmtree(parquet_path, ignore_errors=True)

    print("Loading raw CSV...")
    return spark.read.csv(csv_path, header=True, inferSchema=False)
       
    
def remove_specific_column(df):
    col_names = [
        "Model Year Change", "Registration Class", "No Registration Reason",
        "DMV Facility Number", "Fuel Type", "Fuel Type Change",
        "Vehicle Transmission Type", "NYMA", "NYMA Change",
        "NYVIP Unit Number", "Certified Inspector Number",
        "Inspection Certificate Number", "Data Entry Method",
        "Inspection Test Type", "Gas Cap Replacement",
        "Safety Inspection Result", "Advisory Message",
        "Warning Message", "Wheel Removed 4", "Wheel Removed 3",
        "Wheel Removed 2", "Wheel Removed 1", "Ten-Day Extension Issued",
        "Emissions Waiver Issued", "On-Board Diagnostic Check Result"
    ]
    return df.drop(*col_names)

def save_as_parquet(df, output_path):
    print("Saving 10% sample to avoid disk errors...")
    df.sample(False, 0.01, seed=42) \
      .repartition(1) \
      .write \
      .mode("overwrite") \
      .option("compression", "snappy") \
      .parquet(output_path)
      
      
def download_from_kaggle(dataset_slug, download_path):
    """
    dataset_slug: "user/dataset-name" (e.g., "mohansacharya/graduate-admissions")
    download_path: Local folder to save the CSV
    """
    api = KaggleApi()
    api.authenticate()
    
    # Download and unzip
    api.dataset_download_files(dataset_slug, path=download_path, unzip=True)
    return os.listdir(download_path)