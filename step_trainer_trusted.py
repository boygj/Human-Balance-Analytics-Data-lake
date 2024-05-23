import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Curated
CustomerCurated_node1716387467210 = glueContext.create_dynamic_frame.from_catalog(database="dev", table_name="customer_curated", transformation_ctx="CustomerCurated_node1716387467210")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1716387748784 = glueContext.create_dynamic_frame.from_catalog(database="dev", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1716387748784")

# Script generated for node Join
CustomerCurated_node1716387467210DF = CustomerCurated_node1716387467210.toDF()
StepTrainerLanding_node1716387748784DF = StepTrainerLanding_node1716387748784.toDF()
Join_node1716387813205 = DynamicFrame.fromDF(CustomerCurated_node1716387467210DF.join(StepTrainerLanding_node1716387748784DF, (CustomerCurated_node1716387467210DF['serialnumber'] == StepTrainerLanding_node1716387748784DF['serialnumber']), "left"), glueContext, "Join_node1716387813205")

# Script generated for node Change Schema
ChangeSchema_node1716405959716 = ApplyMapping.apply(frame=Join_node1716387813205, mappings=[("serialnumber", "string", "serialnumber", "string"), ("sensorreadingtime", "long", "sensorreadingtime", "long"), ("distancefromobject", "int", "distancefromobject", "int")], transformation_ctx="ChangeSchema_node1716405959716")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1716388077020 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1716405959716, connection_type="s3", format="json", connection_options={"path": "s3://alphatest1111/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="StepTrainerTrusted_node1716388077020")

job.commit()