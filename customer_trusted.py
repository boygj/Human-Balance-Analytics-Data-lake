import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Landing
CustomerLanding_node1716379663932 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://alphatest1111/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1716379663932")

# Script generated for node Filter
Filter_node1716375002661 = Filter.apply(frame=CustomerLanding_node1716379663932, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="Filter_node1716375002661")

# Script generated for node Customer Trusted
CustomerTrusted_node1716301422926 = glueContext.write_dynamic_frame.from_options(frame=Filter_node1716375002661, connection_type="s3", format="json", connection_options={"path": "s3://alphatest1111/customer/trusted/", "partitionKeys": []}, transformation_ctx="CustomerTrusted_node1716301422926")

job.commit()