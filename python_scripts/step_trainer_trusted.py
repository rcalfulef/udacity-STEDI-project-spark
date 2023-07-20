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

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-spark-andres/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1689818500764 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-spark-andres/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1689818500764",
)

# Script generated for node Join
Join_node1689818523236 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1689818500764,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1689818523236",
)

# Script generated for node Drop Fields
DropFields_node1689818590032 = DropFields.apply(
    frame=Join_node1689818523236,
    paths=[
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
        "phone",
        "lastUpdateDate",
        "customerName",
        "email",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "birthDay",
        "serialNumber",
    ],
    transformation_ctx="DropFields_node1689818590032",
)

# Script generated for node Rename Field
RenameField_node1689818652885 = RenameField.apply(
    frame=DropFields_node1689818590032,
    old_name="`.serialNumber`",
    new_name="serialNumber",
    transformation_ctx="RenameField_node1689818652885",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=RenameField_node1689818652885,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-spark-andres/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
