import sys
import os

import pyspark
from delta import configure_spark_with_delta_pip
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.catalog import Column
from pyspark.sql.functions import col

from utils.dtos import get_raw_schema_definition
import dotenv

def _handle_batches(spark_session: SparkSession,
                    data_frame: DataFrame,
                    batch_number: int,
                    table_path: str):

    print(f'Processing batch with the number {batch_number}...')

    if not DeltaTable.isDeltaTable(identifier=table_path, sparkSession=spark_session):
        print(f"Delta table at {table_path} does not exist. Creating it.")
        data_frame.write.format("delta").mode("overwrite").save(table_path)
        return

    delta_table = DeltaTable.forPath(spark_session, table_path)

    updates_df = data_frame.dropDuplicates(["message_date", "message_text"])

    merge_conditions: Column = (
        (col("target.message_date") == col("source.message_date")) &
        (col("target.message_text") == col("source.message_text"))
    )

    (
      delta_table
     .alias("target")
     .merge(source=updates_df.alias("source"),  condition=merge_conditions)
     .whenNotMatchedInsertAll()
     .execute()
     )

if __name__ == "__main__":

    dotenv.load_dotenv()

    folder_with_tg_messages = os.getenv('MESSAGE_FOLDER')
    transformed_messages_root = os.getenv('TRANSFORMED_MESSAGE_FOLDER')
    checkpoint_path = transformed_messages_root + r"/checkpoints"
    transformed_messages_output_path = transformed_messages_root + r"/messages"

    if not os.path.isdir(folder_with_tg_messages):
        print("There are no messages for creating the table. Please, park the messages first under the `dialogs` folder!")
        sys.exit(1)

    builder = (pyspark.sql.SparkSession.builder.appName("Telegram message transformation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    query = ( spark.readStream
        .option("multiLine", "true")
        .option("maxFilesPerTrigger", 1000)
        .schema(get_raw_schema_definition())
        .option("recursiveFileLookup", "true")
        .option("includeMetadataField", "true")
        .json(folder_with_tg_messages + "/**/*.json")
        .dropDuplicates()
        .writeStream
        .option("checkpointLocation", checkpoint_path)
        .format("json")
        .foreachBatch(lambda df,number: _handle_batches(spark,df,number,transformed_messages_output_path))
        .trigger(availableNow=True)
        .start(transformed_messages_output_path))

    query.awaitTermination()