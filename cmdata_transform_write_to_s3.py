import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql import Window
from awsglue.dynamicframe import DynamicFrame

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "firstgluedb", table_name = "cmdata1", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "samar", table_name = "cmpart", transformation_ctx = "datasource0")

datasource_c = datasource0.resolveChoice(specs=[("close", "cast:long")])
cmdf = datasource_c.toDF()

# create a function to replace month names with numbers
def mnameToNo(dt):
    mname = dt[3:6].upper()
    calendar = {"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
                "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10",
                "NOV": "11", "DEC": "12"}
    return dt.upper().replace(mname, calendar[mname])

# create a udf from the month name to no function
# to apply to timestamp, expiry date colukns
udf_mname_to_no = udf(mnameToNo)

print('creating cmdf vw with timestamp column proper named tsp')
cmdfvw = cmdf.withColumn(
    "tsp",
    to_timestamp(udf_mname_to_no("TIMESTAMP"), "dd-MM-yyyy"))

# using dataframe api to calculate moving average
mvngAvgSpec = avg("close").over(Window.partitionBy("symbol").orderBy("tsp").rowsBetween(-5, 0))

cmdfvw5 = cmdfvw.select("symbol", "timestamp", "tsp", "close", mvngAvgSpec.alias("mvng_avg_5"))

cmdfvwDynamicFrame = DynamicFrame.fromDF(cmdfvw5,glueContext,'cmdfvwDynamicFrame')

# Script generated for node Amazon S3
cmDFDyanmicFrameWriteS3Sink = glueContext.getSink(
    path="s3://samarslife/cmjob/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="cmDFDyanmicFrameWriteS3",
)
cmDFDyanmicFrameWriteS3Sink.setCatalogInfo(
    catalogDatabase="firstgluedb", catalogTableName="cmdatamvngavg"
)
cmDFDyanmicFrameWriteS3Sink.setFormat("glueparquet")
cmDFDyanmicFrameWriteS3Sink.writeFrame(cmdfvwDynamicFrame)
job.commit()
