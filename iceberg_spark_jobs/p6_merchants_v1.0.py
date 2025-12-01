"""
Glue 5.0 Job: merchants Processing
Processes merchants from staging to reporting with incremental MERGE
"""

import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job

ssm = boto3.client('ssm')

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'config', 'tables'])

def read_ssm_json(name: str) -> dict:
    return json.loads(ssm.get_parameter(Name=name)['Parameter']['Value'])

# load env config
env_cfg = read_ssm_json(args['config'])
ENV_PREFIX = env_cfg['rawS3BucketPrefix']
WAREHOUSE_S3 = env_cfg['warehouseS3Path']
REPORT_BUCKET = env_cfg['reportS3BucketName']
CATALOG = "glue_catalog"
TABLE = "p6_merchants"
STAGE_DB = f"{ENV_PREFIX}_stage"
REPORT_DB = f"{ENV_PREFIX}_report"
TABLE_FQDN = f"{CATALOG}.{REPORT_DB}.{TABLE}"
TABLE_LOCATION = f"s3://{REPORT_BUCKET}/{ENV_PREFIX}_gold/{TABLE}/"

# Spark / Iceberg init
spark = (
    SparkSession.builder
    .appName(f"Gold_{TABLE}_{REPORT_DB}_FULL_REBUILD")
    .config("spark.sql.warehouse.dir", WAREHOUSE_S3)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE_S3)
    .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{CATALOG}.glue.locking-mode", "optimistic")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()

# Read Source Data
logger.info(f'***** Job parameters: {ENV_PREFIX}, {CATALOG}, {STAGE_DB}, {REPORT_DB}, {TABLE_FQDN}, {TABLE_LOCATION}')

# Query with explicit catalog
sql_query = f"""
    SELECT id, created_at, updated_at, acquirer_id, name, state
    FROM {CATALOG}.{STAGE_DB}.merchants;
"""
logger.info(f'*** SQL Query: {sql_query}')

result_df = spark.sql(sql_query)

# Write to Iceberg (create or append)
result_df.writeTo(TABLE_FQDN) \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

job.commit()
