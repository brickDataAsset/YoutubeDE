import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1668972925156 = glueContext.create_dynamic_frame.from_catalog(
    database="db_yt_cleaned",
    table_name="cleaned_json_data",
    transformation_ctx="AWSGlueDataCatalog_node1668972925156",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1668972954526 = glueContext.create_dynamic_frame.from_catalog(
    database="db_yt_cleaned",
    table_name="yt_csv_data",
    transformation_ctx="AWSGlueDataCatalog_node1668972954526",
)

# Script generated for node Join
Join_node1668973043689 = Join.apply(
    frame1=AWSGlueDataCatalog_node1668972954526,
    frame2=AWSGlueDataCatalog_node1668972925156,
    keys1=["category_id"],
    keys2=["id"],
    transformation_ctx="Join_node1668973043689",
)

# Script generated for node Amazon S3
AmazonS3_node1668973372778 = glueContext.getSink(
    path="s3://yt-analytics-bucket/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region", "category_id"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1668973372778",
)
AmazonS3_node1668973372778.setCatalogInfo(
    catalogDatabase="db_yt_analytics", catalogTableName="final_analytics"
)
AmazonS3_node1668973372778.setFormat("glueparquet")
AmazonS3_node1668973372778.writeFrame(Join_node1668973043689)
job.commit()
