from pyspark.sql import SparkSession
from delta import *
import dotenv
import os

if __name__ == "__main__":

    dotenv.load_dotenv()

    builder = (
         SparkSession
        .builder
        .appName("check-delta-table")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    transformed_messages_root = os.getenv('TRANSFORMED_MESSAGE_FOLDER')

    df = (spark.read.format("delta").load(transformed_messages_root+r"/messages"))

    df.show(truncate=False)

    print(f"Messages count: {df.count()}")

    spark.stop()
