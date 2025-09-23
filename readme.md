# Fabric event stream message producer

- The application can parse the Telegram messages, reprocess and store them in JSON format for further populating in Fabric Eventstream 

## Warning
- Under any circumstances, do not commit `init_session.session` in public repos. Your telegram will be attempted to hack.

- In case of `NoClassDefFoundError`, please copy the necessary package/packages into: 
`.venv\Lib\site-packages\pyspark\jars\`, or make sure the jar is accessible for the Python interpreter you are using


## Goals

**Topics to include and study:**
- Telethon
- OOP style of code
- Processing data with Spark
- Spark Structured Streaming
- Fabric Eventstream
- Fabric Eventstream transformations
- Fabric Eventstream window functions

## Tasks

1. [x] ~~Ingest the telegram messages from the specific channel~~

2. [x] ~~Configure Spark Structured Streaming to get 
the resulting messages table that contains no duplicates 
and a full list of messages. Use the Delta format `merge into` 
capabilities for each batch stream function. Check and optimize.~~

3. [x] Implement change data capture(CDC) from the Delta table to the EventStream
