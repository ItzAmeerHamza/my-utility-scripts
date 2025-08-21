import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
import boto3
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StringType, TimestampType, IntegerType, FloatType, BooleanType
import uuid
from pyspark.sql.functions import md5, concat_ws, current_timestamp
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, TimestampType, DateType, MapType
import json
from pyspark.sql.functions import col, from_json, to_date, to_json, struct
from pyspark.sql import DataFrame
from datetime import datetime, timedelta
from pyspark.sql.functions import udf
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark Session with Iceberg configurations
catalog_nm = "glue_catalog"
s3_bucket = "s3://datalake-unified-221490242148-us-west-2/uct_events"
spark = SparkSession.builder \
    .config(f"spark.sql.catalog.{catalog_nm}",
        "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_nm}.warehouse", s3_bucket) \
    .config(f"spark.sql.catalog.{catalog_nm}.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_nm}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get current date and subtract one day
yesterday = datetime.now() - timedelta(days=1)

# Extract year, month, and day from the previous date
year = yesterday.strftime('%Y')
month = yesterday.strftime('%m')
day = yesterday.strftime('%d')

# Configuration parameters
flow_type = 'hist'
current_date = datetime.now().strftime("%Y%m%d")
current_time = datetime.now()
profile_workspace_id = '679'  # Update as needed for OneSignal
user_id = '18'  # Update as needed for OneSignal
account_id = '27'  # Update as needed for OneSignal
source = 'onesignal'
connector_id = '25'  # Update with OneSignal connector ID

def batch_id_generator(stage):
    """
    Generate batch ID based on existing data in raw or optimized layer
    """
    try:
        if stage == 'raw':
            s3_path = f"s3://datalake-raw-221490242148-us-west-2/onesignal_raw/"
            data_frame = spark.read.option("recursiveFileLookup", "true") \
                    .parquet(s3_path)
        else:
            s3_path = f"s3://datalake-optimized-221490242148-us-west-2/onesignal_opt/"
            data_frame = spark.read.option("recursiveFileLookup", "true") \
                    .parquet(s3_path)
        
        if data_frame is None or data_frame.head(1) == []:
            batch_id = 1
        elif "batch_id" not in data_frame.columns:
            batch_id = 1
        else:
            max_batch_id = data_frame.agg({"batch_id": "max"}).collect()[0][0]
            batch_id = max_batch_id + 1 if max_batch_id is not None else 1
    except Exception as e:
        # If directory doesn't exist or any other error, start with batch_id = 1
        batch_id = 1
    return batch_id

# ================== LANDING TO RAW STAGE ==================

# Define OneSignal data schema (adjust based on actual OneSignal data structure)
onesignal_schema = StructType([
    StructField("id", StringType(), True),
    StructField("external_id", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("app_id", StringType(), True),
    StructField("notification_id", StringType(), True),
    StructField("sent_at", StringType(), True),
    StructField("received_at", StringType(), True),
    StructField("opened_at", StringType(), True),
    StructField("converted", BooleanType(), True),
    StructField("conversion_time", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("device_type", IntegerType(), True),
    StructField("language", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("city", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("game_version", StringType(), True),
    StructField("device_model", StringType(), True),
    StructField("device_os", StringType(), True),
    StructField("sdk_version", StringType(), True),
    StructField("session_count", IntegerType(), True),
    StructField("session_time", FloatType(), True),
    StructField("first_session", StringType(), True),
    StructField("last_active", StringType(), True),
    StructField("amount_spent", FloatType(), True),
    StructField("bought_sku", StringType(), True),
    StructField("tags", MapType(StringType(), StringType()), True),
    StructField("badge_count", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("test_type", StringType(), True),
    StructField("notification_types", IntegerType(), True),
    StructField("ip", StringType(), True),
    StructField("outcomes", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])
    ), True),
    StructField("heading", StringType(), True),
    StructField("contents", StringType(), True),
    StructField("url", StringType(), True),
    StructField("web_url", StringType(), True),
    StructField("app_url", StringType(), True),
    StructField("chrome_web_icon", StringType(), True),
    StructField("chrome_web_image", StringType(), True),
    StructField("firefox_icon", StringType(), True),
    StructField("priority", IntegerType(), True),
    StructField("ttl", IntegerType(), True),
    StructField("include_player_ids", ArrayType(StringType()), True),
    StructField("include_external_user_ids", ArrayType(StringType()), True),
    StructField("included_segments", ArrayType(StringType()), True),
    StructField("excluded_segments", ArrayType(StringType()), True),
    StructField("data", MapType(StringType(), StringType()), True),
    StructField("buttons", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("text", StringType(), True),
            StructField("icon", StringType(), True),
            StructField("url", StringType(), True)
        ])
    ), True),
    StructField("android_sound", StringType(), True),
    StructField("ios_sound", StringType(), True),
    StructField("android_led_color", StringType(), True),
    StructField("android_accent_color", StringType(), True),
    StructField("android_visibility", IntegerType(), True),
    StructField("ios_badge_type", StringType(), True),
    StructField("ios_badge_count", IntegerType(), True),
    StructField("collapse_id", StringType(), True),
    StructField("send_after", StringType(), True),
    StructField("delayed_option", StringType(), True),
    StructField("delivery_time_of_day", StringType(), True),
    StructField("throttle_rate_per_minute", IntegerType(), True),
    StructField("android_group", StringType(), True),
    StructField("android_group_message", StringType(), True),
    StructField("adm_sound", StringType(), True),
    StructField("thread_id", StringType(), True),
    StructField("summary_arg", StringType(), True),
    StructField("summary_arg_count", IntegerType(), True),
    StructField("email_subject", StringType(), True),
    StructField("email_body", StringType(), True),
    StructField("email_from_name", StringType(), True),
    StructField("email_from_address", StringType(), True),
    StructField("sms_from", StringType(), True),
    StructField("sms_media_urls", ArrayType(StringType()), True),
    StructField("successful", IntegerType(), True),
    StructField("failed", IntegerType(), True),
    StructField("errored", IntegerType(), True),
    StructField("converted", IntegerType(), True),
    StructField("queued_at", StringType(), True),
    StructField("completed_at", StringType(), True),
    StructField("canceled", BooleanType(), True),
    StructField("remaining", IntegerType(), True),
    StructField("status", StringType(), True)
])

# Read data from landing zone
s3_path_landing = "s3://datalake-landingzone-221490242148-us-west-2/onesignal_raw_messages_data/"

df_land = spark.read.option("multiline", "true") \
    .option("recursiveFileLookup", "true") \
    .schema(onesignal_schema) \
    .json(s3_path_landing)

# Generate batch ID for raw stage
batch_id_raw = batch_id_generator(stage='raw')

# Add metadata columns for raw layer
df_land = (
    df_land
    .withColumn("id", expr("uuid()"))
    .withColumn("r_created_at", current_timestamp())
    .withColumn("r_updated_at", current_timestamp())
    .withColumn("event_date", 
        when(col("sent_at").isNotNull(), to_date(col("sent_at")))
        .when(col("received_at").isNotNull(), to_date(col("received_at")))
        .when(col("opened_at").isNotNull(), to_date(col("opened_at")))
        .otherwise(to_date(lit(current_date), "yyyyMMdd"))
    )
    .withColumn("batch_id", lit(batch_id_raw))
    .withColumn("profile_workspace_id", lit(profile_workspace_id))
    .withColumn("user_id", lit(user_id))
    .withColumn("account_id", lit(account_id))
    .withColumn("source", lit(source))
    .withColumn("connector_id", lit(connector_id))
    .withColumn("eventtype", 
        when(col("email").isNotNull(), "EMAIL")
        .when(col("sms_from").isNotNull(), "SMS")
        .otherwise("PUSH_NOTIFICATION")
    )
)

# Write to raw S3 location
output_directory_raw = f"s3://datalake-raw-221490242148-us-west-2/onesignal_raw/{current_date}/"

try:
    df_land.write.mode("append").parquet(output_directory_raw)
    print(f"Data written successfully to raw S3 path: {output_directory_raw}")
except Exception as e:
    print(f"Error writing data to raw S3: {e}")

# ================== RAW TO OPTIMIZED STAGE ==================

# Read from raw layer
s3_path_raw = f"s3://datalake-raw-221490242148-us-west-2/onesignal_raw/{current_date}/"
df_raw = spark.read.parquet(s3_path_raw)

# Generate batch ID for optimized stage
batch_id_opt = batch_id_generator(stage='optimized')

# Process and clean data for optimized layer
df_raw = (
    df_raw.dropDuplicates()
          .drop('r_created_at', 'r_updated_at', 'batch_id')
          .withColumnRenamed("id", "raw_id")
          .withColumn("id", expr("uuid()"))
          .withColumn("r_created_at", current_timestamp())
          .withColumn("r_updated_at", current_timestamp())
          .withColumn("batch_id", lit(batch_id_opt))
)

# Add any OneSignal-specific data cleaning/transformation
# For example, clean phone numbers if SMS
df_raw = df_raw.withColumn("cleaned_phone", 
    when(col("sms_from").isNotNull(), regexp_replace("sms_from", r'^\+1', ''))
    .otherwise(lit(None))
)

# Standardize device information
df_raw = df_raw.withColumn("device_type_name",
    when(col("device_type") == 0, "iOS")
    .when(col("device_type") == 1, "Android")
    .when(col("device_type") == 2, "Amazon")
    .when(col("device_type") == 3, "WindowsPhone")
    .when(col("device_type") == 4, "Chrome Apps")
    .when(col("device_type") == 5, "Chrome Web Push")
    .when(col("device_type") == 6, "Windows")
    .when(col("device_type") == 7, "Safari")
    .when(col("device_type") == 8, "Firefox")
    .when(col("device_type") == 9, "macOS")
    .when(col("device_type") == 10, "Alexa")
    .when(col("device_type") == 11, "Email")
    .when(col("device_type") == 12, "Huawei")
    .when(col("device_type") == 13, "SMS")
    .otherwise("Unknown")
)

# Calculate engagement metrics
df_raw = df_raw.withColumn("is_opened",
    when(col("opened_at").isNotNull(), lit(1)).otherwise(lit(0))
)

df_raw = df_raw.withColumn("is_converted",
    when(col("converted") == True, lit(1)).otherwise(lit(0))
)

# Write to optimized S3 location
optimized_s3_path = f"s3://datalake-optimized-221490242148-us-west-2/onesignal_opt/{current_date}/"

try:
    df_raw.write.mode("append").parquet(optimized_s3_path)
    print(f"Data written successfully to the optimized S3 path: {optimized_s3_path}")
except Exception as e:
    print(f"Error writing data to optimized S3: {e}")

# ================== OPTIMIZED TO UCT STAGE ==================

# Read from optimized layer
s3_path_opt = f"s3://datalake-optimized-221490242148-us-west-2/onesignal_opt/{current_date}/"
df_opt = spark.read.parquet(s3_path_opt)

# Prepare for UCT transformation
df_opt = (
    df_opt.drop('r_created_at', 'r_updated_at', 'batch_id', 'raw_id')
          .withColumnRenamed("id", "optimized_id")
          .withColumn("id", expr("uuid()"))
          .withColumn("r_created_at", current_timestamp())
          .withColumn("r_updated_at", current_timestamp())
)

def generate_uuid():
    return str(uuid.uuid4())

generate_uuid_udf = udf(generate_uuid, StringType())

def fields_selection(columns_to_keep, df: DataFrame) -> DataFrame:
    """
    Select specific columns and put the rest in unmapped field
    """
    all_columns = df.columns
    excluded_columns = list(set(all_columns) - set(columns_to_keep))
    if excluded_columns:
        df = df.withColumn('unmapped', to_json(struct(*excluded_columns)))
    else:
        df = df.withColumn('unmapped', lit(json.dumps({'no unmapped fields': None})))
    return df

# UCT Schema definition
UCT = {
    "id": StringType(),
    "batch_id": IntegerType(),
    "r_created_at": TimestampType(),
    "r_updated_at": TimestampType(),
    "connector_id": IntegerType(),
    "source_id": StringType(),
    "timestamp": TimestampType(),
    "source": StringType(),
    "search_id": StringType(),
    "persona": StringType(),
    "profile_workspace_id": StringType(),
    "user_id": StringType(),
    "account_id": StringType(),
    "provider_id": StringType(),
    "session_id": StringType(),
    "event_type": StringType(),
    "campaign_id": StringType(),
    "ticket_id": StringType(),
    "child_event_id": StringType(),
    "referrer": StringType(),
    "ip_address": StringType(),
    "user_agent": StringType(),
    "name": StringType(),
    "email": StringType(),
    "phone": StringType(),
    "address": StringType(),
    "status": StringType(),
    "channel": StringType(),
    "utm_content": StringType(),
    "utm_term": StringType(),
    "utm_campaign": StringType(),
    "utm_source": StringType(),
    "utm_medium": StringType(),
    "page_referrer": StringType(),
    "page_title": StringType(),
    "page_url": StringType(),
    "unmapped": StringType(),
    "device_brand": StringType(),
    "device_model": StringType(),
    "device_os": StringType(),
    "device_os_version": StringType(),
    "device_id": StringType(),
    "android_id": StringType(),
    "mac_address": StringType(),
    "ios_ifa": StringType(),
    "event_source": StringType(),
    "event_date": DateType()
}

columns_to_keep = list(UCT.keys())

# Add required columns
df_opt = (
    df_opt
    .withColumn("r_created_at", lit(current_time))
    .withColumn("r_updated_at", lit(current_time))
    .withColumn("search_id", lit(None).cast(StringType()))
    .withColumn("persona", lit(None).cast(StringType()))
)

# Map OneSignal fields to UCT fields
manual_mappings = {
    "optimized_id": "source_id",
    "source": "source",
    "sent_at": "timestamp",  # Primary timestamp field
    "eventtype": "event_type",
    "batch_id": "batch_id",
    "status": "status",
    "r_created_at": "r_created_at",
    "r_updated_at": "r_updated_at",
    "connector_id": "connector_id",
    "profile_workspace_id": "profile_workspace_id",
    "account_id": "account_id",
    "user_id": "user_id",
    "event_date": "event_date",
    "email": "email",
    "cleaned_phone": "phone",
    "ip": "ip_address",
    "external_id": "provider_id",
    "player_id": "session_id",
    "notification_id": "campaign_id",
    "url": "page_url",
    "web_url": "referrer",
    "device_model": "device_model",
    "device_os": "device_os",
    "device_type_name": "channel"
}

# Apply column mappings
for old_col, new_col in manual_mappings.items():
    if old_col in df_opt.columns:
        df_opt = df_opt.withColumnRenamed(old_col, new_col)

# Fill missing UCT columns with nulls
for col_name, col_type in UCT.items():
    if col_name not in df_opt.columns:
        df_opt = df_opt.withColumn(col_name, lit(None).cast(col_type))

# Select and prepare final columns
df_opt = fields_selection(columns_to_keep, df_opt)
df_opt = df_opt.withColumn("connector_id", col("connector_id").cast(IntegerType()))

# Get batch ID for UCT table
table = f"{catalog_nm}.unified.uct_events"

try:
    data_frame_uct = spark.sql(f"SELECT max(batch_id) as max_batch_id FROM {table}")
    max_batch_id = data_frame_uct.collect()[0]["max_batch_id"]
    batch_id_uct = max_batch_id + 1 if max_batch_id is not None else 1
except Exception as e:
    if 'Table or view not found' in str(e):
        batch_id_uct = 1
    else:
        raise e

print(f"UCT Batch ID: {batch_id_uct}")

# Set batch ID and prepare final dataframe
df_opt = df_opt.withColumn("batch_id", lit(batch_id_uct))
df_opt = df_opt.select(*columns_to_keep)

# Ensure timestamp is properly cast
df_opt = df_opt.withColumn("timestamp", 
    when(col("timestamp").isNotNull(), col("timestamp").cast(TimestampType()))
    .otherwise(lit(current_time))
)

# Define partition columns
partition_columns = ["connector_id", "event_date"]

# Write to Iceberg table
try:
    df_opt.writeTo(table) \
        .tableProperty("format-version", "2") \
        .partitionedBy(*partition_columns) \
        .append()
    print(f"Data successfully written to UCT table: {table}")
except Exception as e:
    print(f"Error writing DataFrame to UCT table: {e}")

# Commit the job
job.commit()
