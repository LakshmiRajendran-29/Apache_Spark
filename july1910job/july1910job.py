import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="july1910db",
    table_name="july1910test"
)

df = dynamic_frame.toDF()

df.write.format("parquet").option("header", True).option("path", "s3://ecommerce-orders-5000-aug13/ecommerceaug13").save()
  
job.commit()