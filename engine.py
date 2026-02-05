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

def remove_specific_column(df, col_name="Model Year Change"):
    return df.drop(col_name)