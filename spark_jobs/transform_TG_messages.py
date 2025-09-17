import sys
import os
from pyspark.sql import SparkSession
from utils.dtos import get_raw_schema_definition
import dotenv

if __name__ == "__main__":

    dotenv.load_dotenv()

    folder_with_tg_messages = os.getenv('MESSAGE_FOLDER')
    transformed_messages_root = os.getenv('TRANSFORMED_MESSAGE_FOLDER')
    checkpoint_path = transformed_messages_root + r"/checkpoints"
    transformed_messages_output_path = transformed_messages_root + r"/messages"

    if not os.path.isdir(folder_with_tg_messages):
        print("There are no messages for creating the table. Please, park the messages first under the `dialogs` folder!")
        sys.exit(1)

    spark = (SparkSession.builder.appName("transform_TG_messages_in_a_batch").getOrCreate())


    query = ( spark.readStream
        .option("multiLine", "true")
        .schema(get_raw_schema_definition())
        .option("recursiveFileLookup", "true")
        .option("includeMetadataField", "true")
        .json(folder_with_tg_messages + "/**/*.json")
        .dropDuplicates()
        .writeStream
        .option("checkpointLocation", checkpoint_path)
        .format("json")

        .outputMode("complete")

        .trigger(availableNow=True)
        .start(transformed_messages_output_path)
    )

    query.awaitTermination()