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
logger = glueContext.get_logger()

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
profile_workspace_id = None
user_id = '85'
account_id = None
source = 'onesignal'
connector_id = '45'

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
            s3_path = f"s3://datalake-optimized-221490242148-us-west-2/oneSignalNotifications_opt/"
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

def fix_void_columns(df):
    """
    Fix void/null data type columns by casting them to StringType
    """
    for field in df.schema.fields:
        if str(field.dataType) == 'void' or 'void' in str(field.dataType).lower():
            print(f"Fixing void column: {field.name}")
            df = df.withColumn(field.name, col(field.name).cast(StringType()))
    return df

def ensure_column_types(df):
    """
    Ensure all columns have proper data types for Parquet compatibility
    """
    for field in df.schema.fields:
        col_name = field.name
        col_type = field.dataType
        
        # Handle problematic data types
        if str(col_type) == 'void':
            df = df.withColumn(col_name, lit(None).cast(StringType()))
        elif 'null' in str(col_type).lower():
            df = df.withColumn(col_name, col(col_name).cast(StringType()))
            
    return df

# ================== LANDING TO RAW STAGE ==================

onesignal_schema = StructType([
    # Core notification fields
    StructField("id", StringType(), True),
    StructField("app_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("canceled", BooleanType(), True),
    
    # Content and message fields
    StructField("contents", MapType(StringType(), StringType()), True),
    StructField("headings", MapType(StringType(), StringType()), True),
    StructField("subtitle", StringType(), True),
    StructField("data", MapType(StringType(), StringType()), True),
    
    # Timing fields
    StructField("queued_at", LongType(), True),
    StructField("send_after", LongType(), True),
    StructField("completed_at", LongType(), True),
    StructField("delivery_time_of_day", StringType(), True),
    StructField("delayed_option", StringType(), True),
    
    # Statistics fields
    StructField("successful", IntegerType(), True),
    StructField("failed", IntegerType(), True),
    StructField("errored", IntegerType(), True),
    StructField("converted", IntegerType(), True),
    StructField("remaining", IntegerType(), True),
    StructField("received", IntegerType(), True),
    
    # Platform delivery stats
    StructField("platform_delivery_stats", MapType(
        StringType(), 
        StructType([
            StructField("successful", IntegerType(), True),
            StructField("failed", IntegerType(), True),
            StructField("errored", IntegerType(), True),
            StructField("converted", IntegerType(), True),
            StructField("received", IntegerType(), True),
            StructField("suppressed", IntegerType(), True),
            StructField("frequency_capped", IntegerType(), True),
            StructField("provider_successful", IntegerType(), True),
            StructField("provider_failed", IntegerType(), True),
            StructField("provider_errored", IntegerType(), True)
        ])
    ), True),
    
    # Platform type indicators
    StructField("isAdm", BooleanType(), True),
    StructField("isAndroid", BooleanType(), True),
    StructField("isChrome", BooleanType(), True),
    StructField("isChromeWeb", BooleanType(), True),
    StructField("isAlexa", BooleanType(), True),
    StructField("isFirefox", BooleanType(), True),
    StructField("isIos", BooleanType(), True),
    StructField("isSafari", BooleanType(), True),
    StructField("isWP", BooleanType(), True),
    StructField("isWP_WNS", BooleanType(), True),
    StructField("isEdge", BooleanType(), True),
    StructField("isHuawei", BooleanType(), True),
    StructField("isSMS", BooleanType(), True),
    StructField("isEmail", BooleanType(), True),
    
    # SMS specific fields
    StructField("sms_from", StringType(), True),
    StructField("sms_media_urls", ArrayType(StringType()), True),
    
    # Email specific fields
    StructField("email_subject", StringType(), True),
    StructField("email_from_name", StringType(), True),
    StructField("email_from_address", StringType(), True),
    StructField("email_preheader", StringType(), True),
    StructField("email_reply_to_address", StringType(), True),
    StructField("email_click_tracking_disabled", BooleanType(), True),
    StructField("include_unsubscribed", BooleanType(), True),
    
    # Targeting fields
    StructField("include_player_ids", ArrayType(StringType()), True),
    StructField("include_external_user_ids", ArrayType(StringType()), True),
    StructField("include_aliases", StringType(), True),
    StructField("included_segments", ArrayType(StringType()), True),
    StructField("excluded_segments", ArrayType(StringType()), True),
    StructField("filters", ArrayType(StringType()), True),
    
    # Android specific fields
    StructField("android_accent_color", StringType(), True),
    StructField("android_group", StringType(), True),
    StructField("android_group_message", StringType(), True),
    StructField("android_led_color", StringType(), True),
    StructField("android_sound", StringType(), True),
    StructField("android_visibility", StringType(), True),
    
    # iOS specific fields
    StructField("ios_badgeCount", IntegerType(), True),
    StructField("ios_badgeType", StringType(), True),
    StructField("ios_category", StringType(), True),
    StructField("ios_interruption_level", StringType(), True),
    StructField("ios_relevance_score", FloatType(), True),
    StructField("ios_sound", StringType(), True),
    StructField("ios_attachments", ArrayType(StringType()), True),
    StructField("apns_alert", MapType(StringType(), StringType()), True),
    StructField("target_content_identifier", StringType(), True),
    
    # Chrome/Web specific fields
    StructField("chrome_big_picture", StringType(), True),
    StructField("chrome_icon", StringType(), True),
    StructField("chrome_web_icon", StringType(), True),
    StructField("chrome_web_image", StringType(), True),
    StructField("chrome_web_badge", StringType(), True),
    StructField("firefox_icon", StringType(), True),
    
    # Huawei specific fields
    StructField("huawei_sound", StringType(), True),
    StructField("huawei_led_color", StringType(), True),
    StructField("huawei_accent_color", StringType(), True),
    StructField("huawei_visibility", StringType(), True),
    StructField("huawei_group", StringType(), True),
    StructField("huawei_group_message", StringType(), True),
    StructField("huawei_channel_id", StringType(), True),
    StructField("huawei_existing_channel_id", StringType(), True),
    StructField("huawei_small_icon", StringType(), True),
    StructField("huawei_large_icon", StringType(), True),
    StructField("huawei_big_picture", StringType(), True),
    StructField("huawei_msg_type", StringType(), True),
    StructField("huawei_category", StringType(), True),
    StructField("huawei_bi_tag", StringType(), True),
    
    # ADM specific fields
    StructField("adm_big_picture", StringType(), True),
    StructField("adm_group", StringType(), True),
    StructField("adm_large_icon", StringType(), True),
    StructField("adm_small_icon", StringType(), True),
    StructField("adm_sound", StringType(), True),
    
    # Alexa specific fields
    StructField("spoken_text", StringType(), True),
    StructField("alexa_ssml", StringType(), True),
    StructField("alexa_display_title", StringType(), True),
    StructField("amazon_background_data", StringType(), True),
    
    # Other fields
    StructField("big_picture", StringType(), True),
    StructField("large_icon", StringType(), True),
    StructField("small_icon", StringType(), True),
    StructField("global_image", StringType(), True),
    StructField("buttons", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("text", StringType(), True),
            StructField("icon", StringType(), True),
            StructField("url", StringType(), True)
        ])
    ), True),
    StructField("web_buttons", ArrayType(StringType()), True),
    StructField("content_available", BooleanType(), True),
    StructField("priority", IntegerType(), True),
    StructField("ttl", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("web_url", StringType(), True),
    StructField("app_url", StringType(), True),
    StructField("web_push_topic", StringType(), True),
    StructField("wp_sound", StringType(), True),
    StructField("wp_wns_sound", StringType(), True),
    StructField("tags", MapType(StringType(), StringType()), True),
    StructField("template_id", StringType(), True),
    StructField("thread_id", StringType(), True),
    StructField("throttle_rate_per_minute", IntegerType(), True),
    StructField("fcap_group_ids", ArrayType(StringType()), True),
    StructField("fcap_status", StringType(), True)
])

# Read data from landing zone - all JSON files in the folder
s3_path_landing = "s3://datalake-landingzone-221490242148-us-west-2/onesignal_raw_messages_data/"

try:
    df_land = spark.read.option("multiline", "true") \
        .option("recursiveFileLookup", "true") \
        .schema(onesignal_schema) \
        .json(s3_path_landing)
    
    print(f"Successfully read data from: {s3_path_landing}")
    
    # Check if dataframe is empty
    record_count_check = df_land.count()
    if record_count_check == 0:
        print("No data found in landing zone")
    else:
        print(f"Found {record_count_check} records in landing zone")
        
except Exception as e:
    print(f"Error reading data from landing zone: {e}")
    raise e

# Generate batch ID for raw stage
batch_id_raw = batch_id_generator(stage='raw')

# Add metadata columns for raw layer (Fixed version)
df_transformed = (
    df_land
    .withColumn("record_id", expr("uuid()"))  # record_id to avoid conflict with API's id field
    .withColumn("r_created_at", current_timestamp())
    .withColumn("r_updated_at", current_timestamp())
    .withColumn("event_date", 
        when(col("queued_at").isNotNull(), 
             to_date(from_unixtime(col("queued_at"))))
        .when(col("completed_at").isNotNull(), 
             to_date(from_unixtime(col("completed_at"))))
        .otherwise(to_date(lit(current_date), "yyyyMMdd"))
    )
    .withColumn("batch_id", lit(batch_id_raw))
    .withColumn("profile_workspace_id", 
        when(lit(profile_workspace_id).isNotNull(), lit(profile_workspace_id))
        .otherwise(lit(None).cast(StringType()))
    )
    .withColumn("user_id", lit(user_id))
    .withColumn("account_id", 
        when(lit(account_id).isNotNull(), lit(account_id))
        .otherwise(lit(None).cast(StringType()))
    )
    .withColumn("source", lit(source))
    .withColumn("connector_id", lit(connector_id))
    .withColumn("eventtype", 
        when(col("isEmail") == True, "EMAIL")
        .when(col("isSMS") == True, "SMS")
        .otherwise("PUSH_NOTIFICATION")
    )
    # Add processing date for tracking which batch this data came from
    .withColumn("processing_date", lit(current_date))
    .withColumn("data_source_path", lit(s3_path_landing))
    # Add file processing timestamp to track which files were processed
    .withColumn("file_processed_at", current_timestamp())
    # Extract contents text for easier querying
    .withColumn("content_text", 
        when(col("contents").isNotNull(), 
             col("contents").getItem("en"))
        .otherwise(lit(None).cast(StringType()))
    )
    # Convert Unix timestamps to readable format
    .withColumn("queued_at_timestamp", 
        when(col("queued_at").isNotNull(), 
             from_unixtime(col("queued_at")))
        .otherwise(lit(None).cast(TimestampType()))
    )
    .withColumn("send_after_timestamp", 
        when(col("send_after").isNotNull(), 
             from_unixtime(col("send_after")))
        .otherwise(lit(None).cast(TimestampType()))
    )
    .withColumn("completed_at_timestamp", 
        when(col("completed_at").isNotNull(), 
             from_unixtime(col("completed_at")))
        .otherwise(lit(None).cast(TimestampType()))
    )
)

# Fix void columns before writing
df_transformed = fix_void_columns(df_transformed)
df_transformed = ensure_column_types(df_transformed)

# Debug: Print schema to identify problematic columns
print("=== RAW LAYER SCHEMA ===")
df_transformed.printSchema()

# Write to raw S3 location - FIXED PATH
output_directory_raw = f"s3://datalake-raw-221490242148-us-west-2/onesignal_raw/{current_date}/"

try:
    # Check if we have any data to process
    record_count = df_transformed.count()
    if record_count == 0:
        print("No records to process. Exiting.")
    else:
        df_transformed.write.mode("append").parquet(output_directory_raw)
        print(f"Data written successfully to raw S3 path: {output_directory_raw}")
        print(f"Processed {record_count} records from {s3_path_landing}")
        
        # Show some basic statistics
        print("Event type distribution:")
        df_transformed.groupBy("eventtype").count().show()
        
        # Show sample of the data
        print("Sample of processed data:")
        df_transformed.select("record_id", "name", "eventtype", "successful", "failed", "content_text", "processing_date").show(5, truncate=False)
    
except Exception as e:
    print(f"Error writing data to raw S3: {e}")
    raise e

# ================== RAW TO OPTIMIZED STAGE ==================

# Read from raw layer
s3_path_raw = f"s3://datalake-raw-221490242148-us-west-2/onesignal_raw/{current_date}/"
try:
    df_raw = spark.read.parquet(s3_path_raw)
    print(f"Reading raw data from: {s3_path_raw}")
except Exception as e:
    print(f"Error reading from raw layer: {e}")
    # Try yesterday's data if current date doesn't exist
    yesterday_date = yesterday.strftime("%Y%m%d")
    s3_path_raw = f"s3://datalake-raw-221490242148-us-west-2/onesignal_raw/{yesterday_date}/"
    df_raw = spark.read.parquet(s3_path_raw)
    print(f"Reading raw data from previous day: {s3_path_raw}")

# Generate batch ID for optimized stage
batch_id_opt = batch_id_generator(stage='optimized')

# Process and clean data for optimized layer - Focus on business requirements
df_optimized = (
    df_raw.dropDuplicates(["id"])  # Use OneSignal's notification ID for deduplication
    .drop('r_created_at', 'r_updated_at', 'batch_id')
    .withColumnRenamed("record_id", "raw_record_id")  # Keep reference to raw record
    .withColumn("opt_id", expr("uuid()"))  # New ID for optimized layer
    .withColumn("r_created_at", current_timestamp())
    .withColumn("r_updated_at", current_timestamp())
    .withColumn("batch_id", lit(batch_id_opt))
    
    # BUSINESS REQUIREMENT FIELDS MAPPING:
    
    # 1. SENDS - Map from OneSignal's successful + failed + errored
    .withColumn("sends", 
        coalesce(col("successful"), lit(0)) + 
        coalesce(col("failed"), lit(0)) + 
        coalesce(col("errored"), lit(0))
    )
    
    # 2. DELIVERED - Map from OneSignal's successful
    .withColumn("delivered", coalesce(col("successful"), lit(0)))
    
    # 3. FAILED - Map from OneSignal's failed
    .withColumn("failed_count", coalesce(col("failed"), lit(0)))
    
    # 4. REJECTED - Map from OneSignal's errored
    .withColumn("rejected", coalesce(col("errored"), lit(0)))
    
    # 5. ALL FAILED STATUSES - Combine failed + errored
    .withColumn("total_failed", 
        coalesce(col("failed"), lit(0)) + coalesce(col("errored"), lit(0))
    )
    
    # 6. CLICKS - Map from OneSignal's converted
    .withColumn("clicks", coalesce(col("converted"), lit(0)))
    
    # 7. CAMPAIGN ID - Use OneSignal's notification ID as campaign identifier
    .withColumn("campaign_id", col("id"))
    .withColumn("campaign_name", coalesce(col("name"), lit("Unknown Campaign")))
    
    # 8. AUDIENCE/SEGMENT - Extract from targeting info
    .withColumn("audience_segments", 
        when(col("included_segments").isNotNull(), 
             concat_ws(",", col("included_segments")))
        .otherwise(lit("ALL_USERS"))
    )
    
    # 9. DATE - Use event_date or queued_at
    .withColumn("send_date", col("event_date"))
    .withColumn("send_datetime", col("queued_at_timestamp"))
    
    # 10. RECIPIENT NUMBERS - For SMS only
    .withColumn("recipient_count", col("sends"))  # Total recipients
    
    # 11. SENDING NUMBER - For SMS campaigns
    .withColumn("sending_number", 
        when(col("eventtype") == "SMS", col("sms_from"))
        .otherwise(lit(None).cast(StringType()))
    )
    
    # 12. CREATIVE - Extract message content
    .withColumn("creative_content", col("content_text"))
    .withColumn("creative_title", 
        when(col("headings").isNotNull(), 
             col("headings").getItem("en"))
        .otherwise(lit(None).cast(StringType()))
    )
    
    # 13. REPLIES - OneSignal doesn't provide reply data directly
    .withColumn("replies", lit(None).cast(IntegerType()))
    
    # 14. LINKS - Extract from content or URL fields
    .withColumn("has_links", 
        when(col("url").isNotNull() | 
             col("web_url").isNotNull() | 
             col("app_url").isNotNull() |
             col("content_text").rlike("http"), 
             lit(1))
        .otherwise(lit(0))
    )
    .withColumn("primary_link", 
        coalesce(col("url"), col("web_url"), col("app_url"))
    )
    
    # 15. SHORTENED YES/NO - Check if links appear to be shortened
    .withColumn("links_shortened", 
        when(col("primary_link").rlike("(bit\\.ly|tinyurl|short|go\\.|/aff_ad)"), 
             lit("YES"))
        .when(col("primary_link").isNotNull(), 
             lit("NO"))
        .otherwise(lit(None).cast(StringType()))
    )
    
    # ADDITIONAL USEFUL FIELDS
    .withColumn("message_type", col("eventtype"))
    .withColumn("platform_stats", 
        when(col("platform_delivery_stats").isNotNull(), 
             to_json(col("platform_delivery_stats")))
        .otherwise(lit(None).cast(StringType()))
    )
    .withColumn("delivery_status", 
        when(col("canceled") == True, "CANCELED")
        .when(col("completed_at_timestamp").isNotNull(), "COMPLETED")
        .otherwise("IN_PROGRESS")
    )
    .withColumn("success_rate", 
        when(col("sends") > 0, 
             round(col("delivered") / col("sends") * 100, 2))
        .otherwise(lit(0.0))
    )
    .withColumn("click_through_rate", 
        when(col("delivered") > 0, 
             round(col("clicks") / col("delivered") * 100, 2))
        .otherwise(lit(0.0))
    )
    
    # Clean and standardize data
    .withColumn("content_text_clean", 
        regexp_replace(coalesce(col("content_text"), lit("")), "\\{\\{[^}]+\\}\\}", "")  # Remove template variables
    )
)

# Add platform-specific metrics with proper null handling
df_optimized = df_optimized.withColumn("sms_metrics",
    when(col("eventtype") == "SMS",
        to_json(struct(
            col("delivered").alias("sms_delivered"),
            col("failed_count").alias("sms_failed"), 
            col("sending_number").alias("sms_from_number")
        )))
    .otherwise(lit(None).cast(StringType()))
)

df_optimized = df_optimized.withColumn("email_metrics", 
    when(col("eventtype") == "EMAIL",
        to_json(struct(
            col("delivered").alias("email_delivered"),
            col("clicks").alias("email_clicks"),
            col("email_subject")
        )))
    .otherwise(lit(None).cast(StringType()))
)

df_optimized = df_optimized.withColumn("push_metrics",
    when(col("eventtype") == "PUSH_NOTIFICATION",
        to_json(struct(
            col("delivered").alias("push_delivered"),
            col("clicks").alias("push_clicks"),
            col("priority")
        )))
    .otherwise(lit(None).cast(StringType()))
)

# Final selection of business-critical fields
df_final = df_optimized.select(
    # IDs and metadata
    "opt_id",
    "campaign_id", 
    "campaign_name",
    "raw_record_id",
    "batch_id",
    
    # Core business metrics
    "sends",
    "delivered", 
    "failed_count",
    "rejected",
    "total_failed",
    "clicks",
    "success_rate",
    "click_through_rate",
    
    # Campaign details
    "message_type",
    "audience_segments",
    "send_date",
    "send_datetime", 
    "delivery_status",
    
    # Content and creative
    "creative_content",
    "creative_title",
    "content_text_clean",
    
    # Links and engagement
    "has_links",
    "primary_link", 
    "links_shortened",
    "replies",
    
    # Channel-specific
    "recipient_count",
    "sending_number",
    "sms_metrics",
    "email_metrics", 
    "push_metrics",
    
    # Technical fields
    "platform_stats",
    "processing_date",
    "r_created_at",
    "r_updated_at",
    
    # Original fields for reference
    "profile_workspace_id",
    "user_id", 
    "account_id",
    "source",
    "connector_id"
)

# Fix void columns before writing optimized layer
df_final = fix_void_columns(df_final)
df_final = ensure_column_types(df_final)

# Debug: Print schema to identify problematic columns
print("=== OPTIMIZED LAYER SCHEMA ===")
df_final.printSchema()

# Write to optimized S3 location
optimized_s3_path = f"s3://datalake-optimized-221490242148-us-west-2/oneSignalNotifications_opt/{current_date}/"

try:
    # Check record count before writing
    record_count = df_final.count()
    if record_count == 0:
        print("No records to write to optimized layer. Exiting.")
    else:
        df_final.write.mode("append").parquet(optimized_s3_path)
        print(f"Data written successfully to optimized S3 path: {optimized_s3_path}")
        print(f"Optimized {record_count} records")
        
        # Show business metrics summary
        print("\n=== BUSINESS METRICS SUMMARY ===")
        df_final.select("message_type", "sends", "delivered", "clicks", "success_rate").groupBy("message_type").agg(
            sum("sends").alias("total_sends"),
            sum("delivered").alias("total_delivered"), 
            sum("clicks").alias("total_clicks"),
            avg("success_rate").alias("avg_success_rate")
        ).show()
        
        print("\n=== SAMPLE OPTIMIZED DATA ===")
        df_final.select("campaign_name", "message_type", "sends", "delivered", "clicks", "has_links", "links_shortened").show(5, truncate=False)
        
except Exception as e:
    print(f"Error writing data to optimized S3: {e}")
    raise e

# ================== OPTIMIZED TO UCT STAGE ==================

# Read from optimized layer
s3_path_opt = f"s3://datalake-optimized-221490242148-us-west-2/oneSignalNotifications_opt/{current_date}/"
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
    "sent_at": "timestamp",
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
