import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

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

# Script generated for node Provider Info
ProviderInfo_node1794847069102 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://healthcare-project-sampollack/silver/providers/run-1794516245498-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="ProviderInfo_node1794847069102")

# Script generated for node Daily Nursing Staffing
DailyNursingStaffing_node1794847872771 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://healthcare-project-sampollack/silver/staffing/part-00000-d22974d2-0c2e-4e77-854f-23b3ca15689e-c000.snappy.parquet"], "recurse": True}, transformation_ctx="DailyNursingStaffing_node1794847872771")

# Script generated for node Join Provider Types
DailyNursingStaffing_node1794847872771DF = DailyNursingStaffing_node1794847872771.toDF()
ProviderInfo_node1794847069102DF = ProviderInfo_node1794847069102.toDF()
JoinProviderTypes_node1794847913368 = DynamicFrame.fromDF(DailyNursingStaffing_node1794847872771DF.join(ProviderInfo_node1794847069102DF, (DailyNursingStaffing_node1794847872771DF['PROVNUM'] == ProviderInfo_node1794847069102DF['cms_certification_number__ccn_#0']), "left"), glueContext, "JoinProviderTypes_node1794847913368")

# Script generated for node Change Schema
ChangeSchema_node1794848072153 = ApplyMapping.apply(frame=JoinProviderTypes_node1794847913368, mappings=[("PROVNUM", "string", "CCN", "string"), ("PROVNAME", "string", "provider_name", "string"), ("CITY", "string", "city", "string"), ("STATE", "string", "state", "string"), ("WorkDate", "date", "WorkDate", "date"), ("Hrs_RNDON", "double", "Hrs_RNDON", "double"), ("Hrs_RNadmin", "double", "Hrs_RNadmin", "double"), ("Hrs_RN", "double", "Hrs_RN", "double"), ("Hrs_LPNadmin", "double", "Hrs_LPNadmin", "double"), ("Hrs_LPN", "double", "Hrs_LPN", "double"), ("Hrs_CNA", "double", "Hrs_CNA", "double"), ("Hrs_NAtrn", "double", "Hrs_NAtrn", "double"), ("Hrs_MedAide", "double", "Hrs_MedAide", "double"), ("provider_type#5", "string", "provider_type", "string")], transformation_ctx="ChangeSchema_node1794848072153")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1794848072153, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794847850055", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (ChangeSchema_node1794848072153.count() >= 1):
   ChangeSchema_node1794848072153 = ChangeSchema_node1794848072153.coalesce(1)
AmazonS3_node1794848323058 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1794848072153, connection_type="s3", format="glueparquet", connection_options={"path": "s3://healthcare-project-sampollack/gold/average-staffing/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1794848323058")

job.commit()