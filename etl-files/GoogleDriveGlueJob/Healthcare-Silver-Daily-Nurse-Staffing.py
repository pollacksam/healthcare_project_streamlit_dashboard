import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

## Replace the generated AmazonS3_node1794417859786 block with this:
raw_df = spark.read.option("header", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("multiLine", "true") \
    .option("encoding", "ISO-8859-1") \
    .csv("s3://healthcare-project-sampollack/bronze/PBJ_Daily_Nurse_Staffing_Q2_2024.csv")

AmazonS3_node1794417859786 = DynamicFrame.fromDF(raw_df, glueContext, "AmazonS3_node1794417859786")

# Script generated for node Amazon S3
# AmazonS3_node1794417859786 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://healthcare-project-sampollack/bronze/PBJ_Daily_Nurse_Staffing_Q2_2024.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1794417859786")

# Script generated for node SQL Query
SqlQuery362 = '''
WITH transform AS(
SELECT
    PROVNUM,
    PROVNAME,
    CITY,
    STATE,
    COUNTY_NAME,
    COUNTY_FIPS,
    CY_Qtr,
    CONCAT(
        SUBSTRING(WorkDate, 1, 4), '-',
        SUBSTRING(WorkDate, 5, 2), '-',
        SUBSTRING(WorkDate, 7, 2)
    ) AS WorkDate,
    MDScensus,
    Hrs_RNDON,
    Hrs_RNDON_emp,
    Hrs_RNDON_ctr,
    Hrs_RNadmin,
    Hrs_RNadmin_emp,
    Hrs_RNadmin_ctr,
    Hrs_RN,
    Hrs_RN_emp,
    Hrs_RN_ctr,
    Hrs_LPNadmin,
    Hrs_LPNadmin_emp,
    Hrs_LPNadmin_ctr,
    Hrs_LPN,
    Hrs_LPN_emp,
    Hrs_LPN_ctr,
    Hrs_CNA,
    Hrs_CNA_emp,
    Hrs_CNA_ctr,
    Hrs_NAtrn,
    Hrs_NAtrn_emp,
    Hrs_NAtrn_ctr,
    Hrs_MedAide,
    Hrs_MedAide_emp,
    Hrs_MedAide_ctr
    
FROM myDataSource
)

SELECT * FROM transform
'''
SQLQuery_node1794418420267 = sparkSqlQuery(glueContext, query = SqlQuery362, mapping = {"myDataSource":AmazonS3_node1794417859786}, transformation_ctx = "SQLQuery_node1794418420267")

# Script generated for node Change Schema
ChangeSchema_node1794418013309 = ApplyMapping.apply(frame=SQLQuery_node1794418420267, mappings=[("PROVNUM", "string", "PROVNUM", "string"), ("PROVNAME", "string", "PROVNAME", "string"), ("CITY", "string", "CITY", "string"), ("STATE", "string", "STATE", "string"), ("WorkDate", "string", "WorkDate", "date"), ("MDScensus", "string", "MDScensus", "int"), ("Hrs_RNDON", "string", "Hrs_RNDON", "double"), ("Hrs_RNDON_emp", "string", "Hrs_RNDON_emp", "double"), ("Hrs_RNDON_ctr", "string", "Hrs_RNDON_ctr", "double"), ("Hrs_RNadmin", "string", "Hrs_RNadmin", "double"), ("Hrs_RNadmin_emp", "string", "Hrs_RNadmin_emp", "double"), ("Hrs_RNadmin_ctr", "string", "Hrs_RNadmin_ctr", "double"), ("Hrs_RN", "string", "Hrs_RN", "double"), ("Hrs_RN_emp", "string", "Hrs_RN_emp", "double"), ("Hrs_RN_ctr", "string", "Hrs_RN_ctr", "double"), ("Hrs_LPNadmin", "string", "Hrs_LPNadmin", "double"), ("Hrs_LPNadmin_emp", "string", "Hrs_LPNadmin_emp", "double"), ("Hrs_LPNadmin_ctr", "string", "Hrs_LPNadmin_ctr", "double"), ("Hrs_LPN", "string", "Hrs_LPN", "double"), ("Hrs_LPN_emp", "string", "Hrs_LPN_emp", "double"), ("Hrs_LPN_ctr", "string", "Hrs_LPN_ctr", "double"), ("Hrs_CNA", "string", "Hrs_CNA", "double"), ("Hrs_CNA_emp", "string", "Hrs_CNA_emp", "double"), ("Hrs_CNA_ctr", "string", "Hrs_CNA_ctr", "double"), ("Hrs_NAtrn", "string", "Hrs_NAtrn", "double"), ("Hrs_NAtrn_emp", "string", "Hrs_NAtrn_emp", "double"), ("Hrs_NAtrn_ctr", "string", "Hrs_NAtrn_ctr", "double"), ("Hrs_MedAide", "string", "Hrs_MedAide", "double"), ("Hrs_MedAide_emp", "string", "Hrs_MedAide_emp", "double"), ("Hrs_MedAide_ctr", "string", "Hrs_MedAide_ctr", "double")], transformation_ctx="ChangeSchema_node1794418013309")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1794418013309, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794417786843", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1794421984342 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1794418013309, connection_type="s3", format="parquet", connection_options={"path": "s3://healthcare-project-sampollack/silver/staffing/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1794421984342")

job.commit()