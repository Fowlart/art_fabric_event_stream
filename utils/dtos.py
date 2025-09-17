from pyspark.sql.types import StructType, StructField, StringType


def get_raw_schema_definition() -> StructType:
    """
    Creates a PySpark DataFrame schema from the provided JSON structure.
    """

    user_schema = StructType([
        StructField("_", StringType(), True),
        StructField("id", StringType(), True),  # Changed LongType to StringType because the id seems too large for a LongType, and it's represented as string in JSON
        StructField("is_self", StringType(), True),
        StructField("contact", StringType(), True),
        StructField("mutual_contact", StringType(), True),
        StructField("deleted", StringType(), True),
        StructField("bot", StringType(), True),
        StructField("bot_chat_history", StringType(), True),
        StructField("bot_nochats", StringType(), True),
        StructField("verified", StringType(), True),
        StructField("restricted", StringType(), True),
        StructField("min", StringType(), True),
        StructField("bot_inline_geo", StringType(), True),
        StructField("support", StringType(), True),
        StructField("scam", StringType(), True),
        StructField("apply_min_photo", StringType(), True),
        StructField("fake", StringType(), True),
        StructField("bot_attach_menu", StringType(), True),
        StructField("premium", StringType(), True),
        StructField("attach_menu_enabled", StringType(), True),
        StructField("bot_can_edit", StringType(), True),
        StructField("close_friend", StringType(), True),
        StructField("stories_hidden", StringType(), True),
        StructField("stories_unavailable", StringType(), True),
        StructField("contact_require_premium", StringType(), True),
        StructField("bot_business", StringType(), True),
        StructField("bot_has_main_app", StringType(), True),
        StructField("access_hash", StringType(), True), # access_hash is a large number, so StringType is more appropriate.
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("username", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("photo", StringType(), True),
        StructField("status", StringType(), True), # Complex structure, storing as string for simplicity. You can create a nested schema if needed.
        StructField("bot_info_version", StringType(), True),
        StructField("restriction_reason", StringType(), True), # changed to string because it contains "[]"
        StructField("bot_inline_placeholder", StringType(), True),
        StructField("lang_code", StringType(), True),
        StructField("emoji_status", StringType(), True),
        StructField("usernames", StringType(), True), # changed to string because it contains "[]"
        StructField("stories_max_id", StringType(), True),
        StructField("color", StringType(), True),
        StructField("profile_color", StringType(), True),
        StructField("bot_active_users", StringType(), True)
    ])

    schema = StructType([
        StructField("crawling_date", StringType(), True),
        StructField("message_date", StringType(), True),
        StructField("message_text", StringType(), True),
        StructField("dialog", StringType(), True),
        StructField("post_author", StringType(), True),
        StructField("is_channel", StringType(), True),
        StructField("is_group", StringType(), True),
        StructField("user", user_schema, True)
    ])

    return schema