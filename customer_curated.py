import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1716380825432 = glueContext.create_dynamic_frame.from_catalog(database="dev", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1716380825432")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1716380811945 = glueContext.create_dynamic_frame.from_catalog(database="dev", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1716380811945")

# Script generated for node Join
Join_node1716380854313 = Join.apply(frame1=CustomerTrusted_node1716380825432, frame2=AccelerometerLanding_node1716380811945, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1716380854313")

# Script generated for node Drop Fields
DropFields_node1716380924368 = DropFields.apply(frame=Join_node1716380854313, paths=["z", "y", "x", "timestamp", "user"], transformation_ctx="DropFields_node1716380924368")

# Script generated for node Drop Duplicates
DropDuplicates_node1716382748993 =  DynamicFrame.fromDF(DropFields_node1716380924368.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1716382748993")

# Script generated for node Customer Curated
CustomerCurated_node1716381122521 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1716382748993, connection_type="s3", format="json", connection_options={"path": "s3://alphatest1111/customer/curated/", "partitionKeys": []}, transformation_ctx="CustomerCurated_node1716381122521")

job.commit()