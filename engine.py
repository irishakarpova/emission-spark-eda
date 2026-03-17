import os
import requests
import logging
import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class DataEngine:
    def __init__(self):
        self.LOG_DIR = "data/logs"
        self.PIPELINE_LOG = os.path.join(self.LOG_DIR, "pipeline.log")
        self.ERROR_LOG = os.path.join(self.LOG_DIR, "errors.log")
        
        if not os.path.exists(self.LOG_DIR):
            os.makedirs(self.LOG_DIR, exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.FileHandler(self.PIPELINE_LOG), logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger("DataEngine")
        
        self.error_logger = logging.getLogger("ErrorLogger")
        error_handler = logging.FileHandler(self.ERROR_LOG)
        error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.error_logger.addHandler(error_handler)

        self.spark = self._get_spark()

    def _get_spark(self):
        return SparkSession.builder \
            .appName("VIN_Processor") \
            .master("local[*]") \
            .config("spark.driver.memory", "8g") \
            .config("spark.memory.fraction", "0.7") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "2g") \
            .config("spark.network.timeout", "1200s") \
            .config("spark.sql.shuffle.partitions", "20") \
            .config("spark.sql.debug.maxToStringFields", "100") \
            .getOrCreate()

    def log_error(self, context):
        exc_type, exc_value, exc_tb = sys.exc_info()
        stack_trace = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        self.error_logger.error(f"Context: {context}\n{stack_trace}")

    def clean_columns(self, df):
        cols_to_remove = [
                "DMV Facility Number", "Registration Class", 
                "Certified Inspector Number", "Data Entry Method", "NYMA Change","NYMA", 
                "Fuel Type Change", "Inspection Type",
                "No Registration Reason", 
                "Inspection Expiration", "Advisory Message", "Warning Message",
                "Emissions Waiver Issued", "Gas Cap Result", "Gas Cap Replacement",
                "Ten-Day Extension Issued", "Initial Emission Result", 
                "Overall Readiness Result", "On-Board Diagnostic Check Result",
                "Wheel Removed 1", "Wheel Removed 2", "Wheel Removed 3", "Wheel Removed 4", "Vehicle Transmission Type", "Inspection Test Type", "Model Year Change"
            ]
        
        # Normalized check: Spark is case-sensitive, so we handle both cases
        existing_cols = df.columns
        to_drop = [c for c in existing_cols if c.lower() in [rc.lower() for rc in cols_to_remove]]
        
        if to_drop:
            self.logger.info(f"Cleaning phase: Dropping {len(to_drop)} columns: {to_drop}")
            return df.drop(*to_drop)
        return df

    def convert_csv_to_parquet(self, csv_path, parquet_path):
        try:
            self.logger.info("Starting optimized Parquet write...")
            
            # Read CSV - explicitly set low-memory options
            df = self.spark.read \
                .option("header", "true") \
                .option("lowFilesizeOptimization", "true") \
                .csv(csv_path)
            
            # Apply the consolidated cleaning step
            df_lean = self.clean_columns(df)
            
            self.logger.info(f"Writing optimized data to {parquet_path}...")
            df_lean.coalesce(10).write \
                .mode("overwrite") \
                .parquet(parquet_path)
            
            self.logger.info("Success! Cleaned Parquet created.")
            return True
        except Exception as e:
            self.log_error(f"Parquet write failed: {e}")
            return False
        
    def load_dataset(self, csv_path, parquet_path):
        try:
            if os.path.exists(parquet_path):
                self.logger.info("Loading Optimized Parquet...")
                return self.spark.read.parquet(parquet_path)
            
            self.logger.info("Parquet not found. Loading 1% CSV sample...")
            return self.spark.read.csv(csv_path, header=True, inferSchema=False) \
                .sample(withReplacement=False, fraction=0.01, seed=42)
        except Exception as e:
            self.log_error(f"Loading dataset failed: {str(e)}")
            return None

    def create_mapping_file(self, df, output_path, streamlit_progress, limit):
        try:
            vin_col = "VIN" if "VIN" in df.columns else "vin"
            # Extract distinct patterns for API efficiency
            pattern_df = df.select(F.col(vin_col)).distinct()
            
            pattern_df = pattern_df.withColumn(
                "vin_pattern", 
                F.upper(F.concat(F.substring(F.col(vin_col), 1, 8), F.substring(F.col(vin_col), 10, 1)))
            ).select("vin_pattern").distinct()
            
            if limit:
                pattern_df = pattern_df.limit(limit)
            
            patterns = [row.vin_pattern for row in pattern_df.collect()]
            results = []

            for i in range(0, len(patterns), 50):
                batch = patterns[i:i + 50]
                batch_data = ";".join([f"{p[:8]}*{p[8:]},2024" for p in batch])
                
                try:
                    response = requests.post(
                        "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/", 
                        data={'format': 'json', 'data': batch_data}, timeout=20
                    ).json()
                    
                    for res in response.get('Results', []):
                        rv = res.get("VIN", "")
                        if len(rv) >= 10:
                            results.append((
                                f"{rv[:8]}{rv[9]}".upper(),
                                str(res.get("DisplacementL") or ""),
                                str(res.get("EngineCylinders") or ""),
                                str(res.get("HorsePower") or ""),
                                str(res.get("FuelTypePrimary") or "")
                            ))
                except:
                    continue
                
                if streamlit_progress:
                    streamlit_progress.progress(min((i + 50) / len(patterns), 1.0))

            if results:
                schema = ["vin_pattern", "displacement_l", "cylinders", "horsepower", "fuel_type"]
                mapping_df = self.spark.createDataFrame(results, schema)
                mapping_df.write.mode("overwrite").parquet(output_path)
                return mapping_df
            return None
        except Exception as e:
            self.log_error(f"Mapping creation failure: {e}")
            return None

    def label_dataset(self, df, mapping_path):
        if not os.path.exists(mapping_path): return df
        mapping_df = self.spark.read.parquet(mapping_path)
        vin_col = "VIN" if "VIN" in df.columns else "vin"
        
        df_with_key = df.withColumn("vin_pattern", F.upper(F.concat(
            F.substring(F.col(vin_col), 1, 8), F.substring(F.col(vin_col), 10, 1)
        )))
        return df_with_key.join(mapping_df, on="vin_pattern", how="left")

    def save_final_dataset(self, df, output_path):
        try:
            self.logger.info("Exporting results...")
            # Filter for mapped rows and collapse to 1 file for final output
            df.filter(F.col("cylinders").isNotNull()) \
              .repartition(1) \
              .write.mode("overwrite").parquet(output_path)
            return True
        except Exception as e:
            self.log_error(f"Save failed: {e}")
            return False