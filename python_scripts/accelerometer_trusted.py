import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-spark-andres/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1689683876994 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_udacity_database",
    table_name="accelerometer_landing",
    transformation_ctx="AmazonS3_node1689683876994",
)

# Script generated for node Join
Join_node1689683954547 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1689683876994,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1689683954547",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1689817052555 = DynamicFrame.fromDF(
    Join_node1689683954547.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1689817052555",
)

# Script generated for node Drop Fields
DropFields_node1689684017929 = DropFields.apply(
    frame=DropDuplicates_node1689817052555,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1689684017929",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1689684017929,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-spark-andres/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
