import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'db', 'table', 'target_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
db = args['db']
table = args['table']
target_path = args['target_path']
# @type: DataSource
# @args: [database = "cloudtrail_db", table_name = "awslogs", transformation_ctx = "datasource0"]
# @return: datasource0
# @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database=db, table_name=table, transformation_ctx="datasource0")
newpart = datasource0
# @type: DataSink
# @args: [connection_type = "s3", connection_options = {"path": target_path, format = "parquet", transformation_ctx = "datasink4"]
# @return: datasink4
# @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_options(frame=newpart, connection_type="s3", connection_options={
                                                         "path": target_path, "partitionKeys": ["account", "region", "year", "month", "day"]}, format="parquet", transformation_ctx="datasink4")
job.commit()
