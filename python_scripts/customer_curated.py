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

# Script generated for node Amazon S3
AmazonS3_node1689815742628 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-spark-andres/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1689815742628",
)

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

# Script generated for node Join
Join_node1689816013649 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1689815742628,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1689816013649",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1689817692798 = DynamicFrame.fromDF(
    Join_node1689816013649.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1689817692798",
)

# Script generated for node Drop Fields
DropFields_node1689816048194 = DropFields.apply(
    frame=DropDuplicates_node1689817692798,
    paths=["z", "timeStamp", "user", "y", "x"],
    transformation_ctx="DropFields_node1689816048194",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1689816048194,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-spark-andres/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
