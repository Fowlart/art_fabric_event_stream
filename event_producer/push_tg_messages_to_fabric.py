import os
from azure.eventhub import EventHubProducerClient, EventData
from delta import configure_spark_with_delta_pip
from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
import dotenv
dotenv.load_dotenv()
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")
CONNECTION_STR = os.getenv("CONNECTION_STR")
producer = (EventHubProducerClient.from_connection_string
            (conn_str=CONNECTION_STR,
             eventhub_name=EVENTHUB_NAME))

PATH_TO_MESSAGES = os.getenv("TRANSFORMED_MESSAGE_FOLDER")

if __name__=='__main__':

    builder = (SparkSession
               .builder
               .appName("read-messages-and-push-to-fabric")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    list_of_rows: list[Row] = (spark
          .read
          .format("delta")
          .load(PATH_TO_MESSAGES+"/messages")
          .orderBy(col("message_date").desc())
          .limit(10)
          .select(
            col("crawling_date"),
            col("message_date"),
            col("message_text"),
            col("dialog"),
            col("post_author"),
            col("is_channel"),
            col("is_group"))
          .collect())

    list_of_jsons: list[dict[str,str]] = [el.asDict() for el in list_of_rows]

    try:
        print("Producing messages to Fabric: ")
        event_data_batch = producer.create_batch()
        for m in list_of_jsons:
            print(f"Sending message {m}")
            message = json.dumps(m)
            event_data_batch.add(EventData(message))
            producer.send_batch(event_data_batch)

    except KeyboardInterrupt:
        print("Stopped by the user")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the producer
        producer.close()