import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1794507862336 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://healthcare-project-sampollack/bronze/NH_StateUSAverages_Oct2024.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1794507862336")

# Script generated for node Change Schema
ChangeSchema_node1794509249730 = ApplyMapping.apply(frame=AmazonS3_node1794507862336, mappings=[("state or nation", "string", "state or nation", "string"), ("average number of residents per day", "string", "average number of residents per day", "decimal"), ("reported nurse aide staffing hours per resident per day", "string", "reported nurse aide staffing hours per resident per day", "float"), ("reported lpn staffing hours per resident per day", "string", "reported lpn staffing hours per resident per day", "float"), ("reported rn staffing hours per resident per day", "string", "reported rn staffing hours per resident per day", "float"), ("reported licensed staffing hours per resident per day", "string", "reported licensed staffing hours per resident per day", "float"), ("reported total nurse staffing hours per resident per day", "string", "reported total nurse staffing hours per resident per day", "float"), ("total number of nurse staff hours per resident per day on the weekend", "string", "total number of nurse staff hours per resident per day on the weekend", "float"), ("registered nurse hours per resident per day on the weekend", "string", "registered nurse hours per resident per day on the weekend", "float"), ("reported physical therapist staffing hours per resident per day", "string", "reported physical therapist staffing hours per resident per day", "float"), ("nursing case-mix index", "string", "nursing case-mix index", "float"), ("case-mix rn staffing hours per resident per day", "string", "case-mix rn staffing hours per resident per day", "float"), ("case-mix total nurse staffing hours per resident per day", "string", "case-mix total nurse staffing hours per resident per day", "float"), ("case-mix weekend total nurse staffing hours per resident per day", "string", "case-mix weekend total nurse staffing hours per resident per day", "float"), ("processing date", "string", "processing date", "date")], transformation_ctx="ChangeSchema_node1794509249730")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1794509249730, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794499055702", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1794510077877 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1794509249730, connection_type="s3", format="glueparquet", connection_options={"path": "s3://healthcare-project-sampollack/silver/states/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1794510077877")

job.commit()