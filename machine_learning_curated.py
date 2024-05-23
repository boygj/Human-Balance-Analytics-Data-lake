import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1716413792606 = glueContext.create_dynamic_frame.from_catalog(database="dev", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1716413792606")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1716413762226 = glueContext.create_dynamic_frame.from_catalog(database="dev", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1716413762226")

# Script generated for node Join
Join_node1716413823561 = Join.apply(frame1=step_trainer_trusted_node1716413792606, frame2=AccelerometerTrusted_node1716413762226, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1716413823561")

# Script generated for node Drop Fields
DropFields_node1716413864307 = DropFields.apply(frame=Join_node1716413823561, paths=["user"], transformation_ctx="DropFields_node1716413864307")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1716413879186 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1716413864307, connection_type="s3", format="json", connection_options={"path": "s3://alphatest1111/machine_learning_curated/", "partitionKeys": []}, transformation_ctx="MachineLearningCurated_node1716413879186")

job.commit()