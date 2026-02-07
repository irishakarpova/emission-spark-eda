from pyspark.sql import SparkSession

def get_spark():
    return SparkSession.builder \
        .appName("EmissionCleaner") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

def load_dataset(path):
    spark = get_spark()
    # inferSchema=False makes this nearly instant even for 13GB
    return spark.read.csv(path, header=True, inferSchema=False)

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