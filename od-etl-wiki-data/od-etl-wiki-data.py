import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import trim
from pyspark.sql.functions import regexp_replace


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    # trim irregular characters from title column
    #result = result.withColumn("title", trim(result["title"]))
    #result = re.sub("[^A-Z]", "", result["title"], 0, re.IGNORECASE)
    result = result.withColumn("title", trim(regexp_replace(result["title"], "[^A-Za-z0-9]", " ")))
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node od-s3-source
ods3source_node1683847442323 = glueContext.create_dynamic_frame.from_catalog(
    database="od-metadata",
    table_name="wiki_pages_csv",
    transformation_ctx="ods3source_node1683847442323",
)

# Script generated for node od-s3-transform-rds
SqlQuery1380 = """
select * from myDataSource

"""
ods3transformrds_node1683847450503 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1380,
    mapping={"myDataSource": ods3source_node1683847442323},
    transformation_ctx="ods3transformrds_node1683847450503",
)

# Script generated for node od-rds-target
odrdstarget_node1683847455723 = glueContext.write_dynamic_frame.from_options(
    frame=ods3transformrds_node1683847450503,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-315577544850-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.wiki_pages",
        "connectionName": "od-rds-connect",
        "preactions": "CREATE TABLE IF NOT EXISTS public.wiki_pages (pageid BIGINT, ns BIGINT, title VARCHAR); TRUNCATE TABLE public.wiki_pages;",
        "aws_iam_user": "arn:aws:iam::315577544850:role/s3-glue-aws",
    },
    transformation_ctx="odrdstarget_node1683847455723",
)

job.commit()
