from pyspark.sql.types import StructType, StructField, StringType


def get_raw_schema_definition() -> StructType:
    """
    Creates a PySpark DataFrame schema from the provided JSON structure.
    """
    schema = StructType([
        StructField("message_date", StringType(), True),
        StructField("message_text", StringType(), True),
        StructField("first_name", StringType(), True)
    ])

    return schema