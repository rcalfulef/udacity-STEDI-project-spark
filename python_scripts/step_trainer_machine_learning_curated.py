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

# Script generated for node Amazon S3
AmazonS3_node1689819469628 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-spark-andres/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1689819469628",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-spark-andres/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Join
Join_node1689819489291 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1689819469628,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1689819489291",
)

# Script generated for node Drop Fields
DropFields_node1689819810802 = DropFields.apply(
    frame=Join_node1689819489291,
    paths=[
        "`.serialNumber`",
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1689819810802",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1689819810802,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-spark-andres/step_trainer/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
