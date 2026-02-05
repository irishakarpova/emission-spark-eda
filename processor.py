from pyspark.sql import SparkSession

def get_spark_session():
    """Starts or retrieves the Spark engine."""
    return SparkSession.builder \
        .appName("EmissionEDA") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

def simplify_data(input_path, output_path):
    """
    Reads the 13GB CSV, drops unnecessary columns, 
    and saves it to the fast Parquet format.
    """
    spark = get_spark_session()
    
    # Read CSV
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # DROP LOGIC: Add any columns you want to remove here
    cols_to_drop = ["Model Year Change"]
    df = df.drop(*cols_to_drop)
    
    # Save as Parquet
    df.write.mode("overwrite").parquet(output_path)
    return df