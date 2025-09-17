import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, input_file_name, to_date, explode
from utils.dtos import get_raw_schema_definition
import dotenv

if __name__ == "__main__":

    dotenv.load_dotenv()

    folder_with_tg_messages = os.getenv('MESSAGE_FOLDER')
    transformed_message_folder = os.getenv('TRANSFORMED_MESSAGE_FOLDER')


    if not os.path.isdir(folder_with_tg_messages):
        print("There are no messages for creating the table. Please, park the messages first under the `dialogs` folder!")
        sys.exit(1)

    spark = (SparkSession.builder.appName("transform_TG_messages_in_a_batch").getOrCreate())


    df = (spark
          .read
          .option("multiline","true")
          .option("recursiveFileLookup", "true")
          .schema(get_raw_schema_definition())
          .json(folder_with_tg_messages)
          .withColumn("path",input_file_name())
          .withColumn("message_date",to_date(col("message_date")))
          .orderBy(col("message_date").desc_nulls_last())
          )

    df.explain(extended=True)

    (df
     .write
     .mode("overwrite")
     .option("overwriteSchema","True")
     .json(transformed_message_folder))


    spark.stop()
