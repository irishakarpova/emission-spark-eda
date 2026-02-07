from pyspark.sql import SparkSession
import os

def get_spark():
    return SparkSession.builder \
        .appName("EmissionCleaner") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

def load_dataset(csv_path, parquet_path):
    spark = get_spark()
    
    # Check if the compressed Parquet exists first
    if os.path.exists(parquet_path):
        print("Loading compressed Parquet data")
        return spark.read.parquet(parquet_path)
    else:
        print("Parquet not found. Loading raw CSV")
        return spark.read.csv(csv_path, header=True, inferSchema=False)

def remove_specific_column(df, col_names=[
    "Model Year Change", 
    "Registration Class", 
    "No Registration Reason",
    "DMV Facility Number",
    "Fuel Type",
    "Fuel Type Change",
    "Vehicle Transmission Type",
    "NYMA",
    "NYMA Change",
    "NYVIP Unit Number",
    "Certified Inspector Number",
    "Inspection Certificate Number",
    "Data Entry Method",
    "Inspection Test Type",
    "Gas Cap Replacement",
    "Safety Inspection Result",
    "Advisory Message",
    "Warning Message",
    "Wheel Removed 4",
    "Wheel Removed 3",
    "Wheel Removed 2",
    "Wheel Removed 1",
    "Ten-Day Extension Issued",
    "Emissions Waiver Issued",
    "On-Board Diagnostic Check Result"
    ]):
    return df.drop(*col_names)


def save_as_parquet(df, output_path):
    df.write.mode("overwrite").parquet(output_path)