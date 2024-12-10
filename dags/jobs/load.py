from pyspark.sql import SparkSession
import logging

def load_data():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("Load Data")
    
    logger.info("Starting Spark Session...")
    spark = SparkSession.builder \
        .appName("Load Tennis Data") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.2.24.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.2.24.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.2.24.jar") \
        .getOrCreate()
    
    data_path = "/opt/airflow/dags/transformed_data/"
    logger.info(f"Reading data from {data_path}...")
    try:
        df = spark.read.parquet(data_path)
        logger.info(f"Data schema: {df.schema}")
        
        logger.info("Writing data to PostgreSQL...")
        df.write \
            .mode("overwrite") \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/airflow") \
            .option("dbtable", "match_data") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .save()
        logger.info("Data successfully written to PostgreSQL.")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise

if __name__ == "__main__":
    load_data()