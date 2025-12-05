import sys
import boto3
import json
from urllib.parse import urlparse
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import uuid
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, to_json, broadcast, coalesce, when, udf, struct, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType, NullType, FloatType, TimestampType
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# Initialize Glue Context
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
s3_client = boto3.client('s3')

# ==========================================
# 1. CONFIGURATION
# ==========================================

profile_workspace_id = None
user_id = '85'
account_id = None
source = 'klaviyo'
connector_id = '46'

# Paths
s3_base_path_landing = "s3://datalake-landingzone-221490242148-us-west-2/klaviyo-events-data/events-data/"
s3_base_path_raw = "s3://datalake-raw-221490242148-us-west-2/klaviyo-events-data-raw-test/"
s3_base_path_opt = "s3://datalake-optimized-221490242148-us-west-2/klaviyo-events-data-opt-test/"
s3_path_unified_test = "s3://datalake-unified-221490242148-us-west-2/klaviyo-events-data-test-v2/"

# Date Range
start_date = datetime(2025, 11, 1)
end_date = datetime(2025, 12, 5)

# ==========================================
# 2. SCHEMAS
# ==========================================

event_schema = StructType([
    StructField("type", StringType(), True),
    StructField("id", StringType(), True),
    StructField("attributes", StructType([
        StructField("timestamp", IntegerType(), True),
        StructField("datetime", StringType(), True),
        StructField("uuid", StringType(), True),
        StructField("profile_id", StringType(), True),
        StructField("event_properties", StructType([
            StructField("To Number", StringType(), True),
            StructField("From Number", StringType(), True),
            StructField("Message Body", StringType(), True),
            StructField("Message Format", StringType(), True),
            StructField("Message Name", StringType(), True),
            StructField("Message Type", StringType(), True),
            StructField("Campaign Name", StringType(), True),
            StructField("Intent 1", StringType(), True),
            StructField("Intent 1 Confidence", StringType(), True),
            StructField("is_spam", BooleanType(), True),
            
            # --- CRITICAL FIELD FOR BOTS ---
            StructField("Bot Click", BooleanType(), True),
            # -------------------------------
            
            StructField("spam_type", StringType(), True),
            StructField("channel_type", StringType(), True),
            StructField("$event_id", StringType(), True),
            
            StructField("Image URL", StringType(), True), 
            StructField("$campaign", StringType(), True), 
            StructField("$message", StringType(), True),  
            StructField("Credits", IntegerType(), True),  
            StructField("$usage_amount", IntegerType(), True), 
            StructField("Failure Type", StringType(), True),

            StructField("$extra", StructType([
                StructField("From City", StringType(), True),
                StructField("From State", StringType(), True),
                StructField("From Country", StringType(), True),
                StructField("IP Address", StringType(), True),
                StructField("Message Body", StringType(), True)
            ]), True),
        ]), True)
    ]), True),
    
    StructField("relationships", StructType([
        StructField("metric", StructType([
            StructField("data", StructType([
                StructField("id", StringType(), True)
            ]), True)
        ]), True),
        StructField("profile", StructType([
            StructField("data", StructType([
                StructField("id", StringType(), True)
            ]), True)
        ]), True)
    ]), True)
])

# B. Unified Schema Keys
UCT_SCHEMA_KEYS = [
    "id", "batch_id", "r_created_at", "r_updated_at", "connector_id", "source_id", 
    "timestamp", "source", "search_id", "persona", "event_source", "profile_workspace_id", 
    "user_id", "account_id", "provider_id", "session_id", "event_type", "campaign_id", 
    "ticket_id", "child_event_id", "referrer", "ip_address", "user_agent", "name", 
    "email", "phone", "address", "status", "channel", "utm_content", "utm_term", 
    "utm_campaign", "utm_source", "utm_medium", "page_referrer", "page_title", 
    "page_url", "unmapped", "device_brand", "device_model", "device_os", 
    "device_os_version", "device_id", "android_id", "mac_address", "ios_ifa", "event_date"
]

# ==========================================
# 3. HELPER FUNCTIONS
# ==========================================

def generate_uuid():
    return str(uuid.uuid4())

generate_uuid_udf = udf(generate_uuid, StringType())

def fields_selection(columns_to_keep, df):
    all_columns = df.columns
    excluded_columns = list(set(all_columns) - set(columns_to_keep))
    
    if excluded_columns:
        df = df.withColumn('unmapped', to_json(struct(*excluded_columns)))
    else:
        df = df.withColumn('unmapped', lit(json.dumps({'no unmapped fields': None})))
    return df

def check_s3_path_exists(s3_path):
    try:
        parsed_url = urlparse(s3_path)
        bucket_name = parsed_url.netloc
        prefix = parsed_url.path.lstrip('/')
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
        return 'Contents' in response
    except Exception as e:
        print(f"Warning: S3 check failed for {s3_path}. Error: {e}")
        return False

batch_id_counter = 1

# ==========================================
# 5. MAIN LOOP
# ==========================================

ALLOWED_METRIC_IDS = [
    "Y5SMsU", # SMS Failed (MT)
    "XsRJQh", # SMS Sent (MO)
    "Uv32rW", # SMS Clicked (MT)
    "SGB8Vt", # SMS Received (MT)
    "S5A6iQ", # SMS Unsubscribed (MO)
    "SBXSA5"  # SMS Relayed (MT)
]

current_date_loop = start_date
print(f"Starting Iterative Backfill from {start_date} to {end_date}")

while current_date_loop <= end_date:
    year_str = current_date_loop.strftime('%Y')
    month_str = current_date_loop.strftime('%m')
    day_str = current_date_loop.strftime('%d')
    
    landing_folder = f"{s3_base_path_landing}year={year_str}/month={month_str}/day={day_str}/"
    
    print(f"\n--- Processing: {year_str}-{month_str}-{day_str} ---")
    
    if not check_s3_path_exists(landing_folder):
        print(f"Skipping: No data found at {landing_folder}")
        current_date_loop += timedelta(days=1)
        continue

    try:
        # ------------------------------------
        # A. LANDING -> RAW
        # ------------------------------------
        df_land = spark.read.schema(event_schema).json(landing_folder)
        
        # 1. FILTER: Only allow your 6 specific IDs
        df_land = df_land.filter(
            col("relationships.metric.data.id").isin(ALLOWED_METRIC_IDS)
        )
        
        # 2. SELECT & CALCULATE RAW FIELDS
        df_processed = df_land.select(
            col("id").cast(StringType()).alias("source_event_id"),
            col("type").cast(StringType()).alias("record_type"),
            col("attributes.timestamp").cast(IntegerType()).alias("unix_timestamp"),
            col("attributes.datetime").cast(StringType()).alias("event_timestamp"),
            
            col("attributes.uuid").cast(StringType()).alias("klaviyo_uuid"),
            col("relationships.metric.data.id").cast(StringType()).alias("metric_id"),
            
            # PROFILE ID is the most reliable way to identify a "User" or "Number" in Klaviyo
            F.coalesce(col("relationships.profile.data.id"), col("attributes.profile_id")).cast(StringType()).alias("profile_id"),
            
            col("attributes.event_properties.`$campaign`").cast(StringType()).alias("campaign_id"),
            col("attributes.event_properties.`$message`").cast(StringType()).alias("message_id"),
            
            # Phone Extraction (Note: Often Null for Clicks)
            when(col("relationships.metric.data.id").isin(["XsRJQh", "S5A6iQ"]), col("attributes.event_properties.`From Number`"))
            .otherwise(col("attributes.event_properties.`To Number`"))
            .cast(StringType()).alias("phone"),
            
            col("attributes.event_properties.`To Number`").cast(StringType()).alias("destination_address"),
            col("attributes.event_properties.`From Number`").cast(StringType()).alias("source_address"),
            col("attributes.event_properties.`Message Body`").cast(StringType()).alias("content_body"),
            col("attributes.event_properties.`$extra`.`Message Body`").cast(StringType()).alias("extra_message_body"),
            col("attributes.event_properties.`Message Name`").cast(StringType()).alias("message_name"),
            col("attributes.event_properties.`Image URL`").cast(StringType()).alias("media_url"), 
            col("attributes.event_properties.`Message Format`").cast(StringType()).alias("format"),
            col("attributes.event_properties.`Message Type`").cast(StringType()).alias("message_type"),
            col("attributes.event_properties.`Campaign Name`").cast(StringType()).alias("campaign_name"),
            col("attributes.event_properties.`Failure Type`").cast(StringType()).alias("failure_type"),
            
            # Cost Logic
            when(col("relationships.metric.data.id") == "SBXSA5", col("attributes.event_properties.`$usage_amount`"))
            .otherwise(col("attributes.event_properties.Credits"))
            .cast(IntegerType()).alias("credits_used"),
            
            (when(col("relationships.metric.data.id") == "SBXSA5", col("attributes.event_properties.`$usage_amount`"))
             .otherwise(col("attributes.event_properties.Credits")) * 0.09)
            .cast(FloatType()).alias("cost_usd"),

            col("attributes.event_properties.`is_spam`").cast(BooleanType()).alias("is_spam"),
            
            # --- EXTRACT BOT CLICK ---
            col("attributes.event_properties.`Bot Click`").cast(BooleanType()).alias("bot_click"),
            
            col("attributes.event_properties.`$extra`.`IP Address`").cast(StringType()).alias("ip_address"),
            to_json(col("attributes.event_properties")).alias("full_properties_json")
        )

        # 3. ADD ENRICHMENT FIELDS
        df_processed = df_processed \
            .withColumn("raw_id", lit(F.expr("uuid()")).cast(StringType())) \
            .withColumn("r_created_at", lit(F.current_timestamp())) \
            .withColumn("profile_workspace_id", lit(profile_workspace_id).cast(StringType())) \
            .withColumn("user_id", lit(user_id).cast(StringType())) \
            .withColumn("account_id", lit(account_id).cast(StringType())) \
            .withColumn("source", lit(source).cast(StringType())) \
            .withColumn("connector_id", lit(connector_id).cast(StringType())) \
            .withColumn("year", lit(int(year_str))) \
            .withColumn("month", lit(month_str)) \
            .withColumn("day", lit(day_str))

        # Fill Nulls
        for field in df_processed.schema.fields:
            if isinstance(field.dataType, NullType):
                df_processed = df_processed.withColumn(field.name, col(field.name).cast(StringType()))

        # Write RAW
        df_processed.write.partitionBy("year", "month", "day").mode("overwrite").parquet(s3_base_path_raw)

        # ------------------------------------
        # B. RAW -> OPTIMIZED (WITH DEDUPLICATION)
        # ------------------------------------
        # Deduplicate tech-level duplicates first
        df_raw_dedup = df_processed.dropDuplicates(['source_event_id'])

        # 1. ASSIGN EVENT NAMES
        df_opt = df_raw_dedup.withColumn("event_name", 
            when(col("metric_id") == "XsRJQh", "SMS Sent")
            .when(col("metric_id") == "Y5SMsU", "SMS Failed")
            .when(col("metric_id") == "Uv32rW", "SMS Clicked")
            .when(col("metric_id") == "SGB8Vt", "SMS Received")
            .when(col("metric_id") == "S5A6iQ", "SMS Unsubscribed")
            .when(col("metric_id") == "SBXSA5", "SMS Relayed")
            .otherwise("Unknown")
        )

        # =========================================================
        # 2. FILTER BOTS & DEDUPLICATE UNIQUE CLICKS PER USER
        # =========================================================
        
        # Split DataFrames: Clicks vs Everything Else
        df_clicks = df_opt.filter(col("event_name") == "SMS Clicked")
        df_others = df_opt.filter(col("event_name") != "SMS Clicked")

        # A. FILTER BOTS
        # If 'bot_click' is TRUE, drop it. Keep FALSE or NULL.
        df_clicks = df_clicks.filter(
            (col("bot_click") == False) | (col("bot_click").isNull())
        )

        # B. DEDUPLICATE PER USER (PROFILE_ID) + MESSAGE
        # We group by 'profile_id' (The User) and 'message_id' (The Campaign Message)
        # We order by time to keep the very first click they made
        w_click_dedup = Window.partitionBy("profile_id", "message_id").orderBy(col("unix_timestamp").asc())

        df_clicks_unique = df_clicks.withColumn("rn", F.row_number().over(w_click_dedup)) \
            .filter(col("rn") == 1) \
            .drop("rn")

        # C. Recombine
        df_opt = df_others.unionByName(df_clicks_unique)
        
        # =========================================================

        # 3. Calculate Channel
        df_opt = df_opt.withColumn("channel_calc", lit("SMS"))

        # 4. Calculate Status
        df_opt = df_opt.withColumn("status_calc", 
            when(col("event_name") == "SMS Sent", "SMS SEND")
            .when(col("event_name") == "SMS Received", "SMS DELIVERED")
            .when(col("event_name") == "SMS Relayed", "SMS RELAYED")
            .when(col("event_name") == "SMS Clicked", "SMS CLICKED")
            .when(col("event_name") == "SMS Failed", "SMS BOUNCE")
            .when(col("event_name") == "SMS Unsubscribed", "SMS UNSUBSCRIBE")
            .otherwise("OTHER")
        )
        
        # 5. Calculate Direction
        df_opt = df_opt.withColumn("direction",
            when(col("event_name").isin("SMS Sent", "SMS Unsubscribed"), "MO")
            .otherwise("MT")
        )

        # 6. Content Logic
        df_opt = df_opt.withColumn("content_calc",
            when(col("metric_id").isin(["Y5SMsU", "SGB8Vt"]), col("extra_message_body"))
            .when(col("metric_id").isin(["Uv32rW", "SBXSA5"]), lit(None))
            .otherwise(F.coalesce(col("content_body"), col("message_name")))
        )

        # 7. Add Optimized Metadata
        df_opt = df_opt.withColumn("optimized_id", lit(F.expr("uuid()"))) \
                       .withColumn("r_created_at_opt", lit(F.current_timestamp()))

        # Write OPTIMIZED
        df_opt.write.partitionBy("year", "month", "day").mode("overwrite").parquet(s3_base_path_opt)

        # ------------------------------------
        # C. OPTIMIZED -> UNIFIED (TEST LAYER)
        # ------------------------------------
        print(f"Transforming to UNIFIED schema (Test Mode)...")
        df_uct = df_opt

        # FILTER: Uppercase statuses only
        allowed_statuses = ["SMS SEND", "SMS DELIVERED", "SMS CLICKED", "SMS BOUNCE", "SMS UNSUBSCRIBE", "SMS RELAYED"]
        df_uct = df_uct.filter(col("status_calc").isin(allowed_statuses))

        # Derive Event Date
        df_uct = df_uct.withColumn("event_date", F.substring(col("event_timestamp"), 1, 10))

        # --- MAPPING ---
        manual_mappings = {
            "source_event_id": "source_id",
            "event_timestamp": "timestamp",
            "ip_address": "ip_address",
            "campaign_id": "campaign_id", 
            "phone": "phone",
            "status_calc": "status",
            "campaign_name": "utm_term",
            "content_calc": "utm_content"
        }

        for opt_col, unified_col in manual_mappings.items():
            if opt_col in df_uct.columns:
                df_uct = df_uct.withColumnRenamed(opt_col, unified_col)
        
        # --- HARDCODED COLUMNS ---
        df_uct = df_uct.withColumn("utm_source", lit("aff-cc"))
        df_uct = df_uct.withColumn("utm_campaign", lit("R-CPL-001"))
        df_uct = df_uct.withColumn("utm_medium", lit("affiliate"))

        df_uct = df_uct.withColumn("event_type", col("channel_calc"))
        df_uct = df_uct.withColumn("channel", col("channel_calc"))

        # IDs & Timestamps for Unified
        df_uct = df_uct.withColumn("id", generate_uuid_udf())
        df_uct = df_uct.withColumn("r_created_at", current_timestamp())
        df_uct = df_uct.withColumn("r_updated_at", current_timestamp())
        df_uct = df_uct.withColumn("connector_id", col("connector_id").cast(IntegerType()))

        # Fill Missing
        for col_name in UCT_SCHEMA_KEYS:
            if col_name not in df_uct.columns:
                df_uct = df_uct.withColumn(col_name, lit(None).cast(StringType()))

        # Pack Unmapped 
        df_uct = fields_selection(UCT_SCHEMA_KEYS, df_uct)

        # Final Select
        df_uct = df_uct.withColumn("batch_id", lit(batch_id_counter))
        df_uct_final = df_uct.select(*UCT_SCHEMA_KEYS)

        print(f"Writing to UNIFIED TEST path: {s3_path_unified_test}")
        df_uct_final.write.mode("append").parquet(s3_path_unified_test)
        
        batch_id_counter += 1

    except Exception as e:
        print(f"ERROR processing date {year_str}-{month_str}-{day_str}: {e}")

    current_date_loop += timedelta(days=1)

print("Backfill Complete.")
job.commit()
