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

# Script generated for node Daily Nursing Staffing
DailyNursingStaffing_node1794848961778 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://healthcare-project-sampollack/silver/staffing/part-00000-d22974d2-0c2e-4e77-854f-23b3ca15689e-c000.snappy.parquet"], "recurse": True}, transformation_ctx="DailyNursingStaffing_node1794848961778")

# Script generated for node Change Schema
ChangeSchema_node1794849017155 = ApplyMapping.apply(frame=DailyNursingStaffing_node1794848961778, mappings=[("PROVNUM", "string", "CCN", "string"), ("PROVNAME", "string", "provider_name", "string"), ("CITY", "string", "city", "string"), ("STATE", "string", "state", "string"), ("WorkDate", "date", "WorkDate", "date"), ("Hrs_RNDON", "double", "Hrs_RNDON", "double"), ("Hrs_RNadmin", "double", "Hrs_RNadmin", "double"), ("Hrs_RN", "double", "Hrs_RN", "double"), ("Hrs_LPNadmin", "double", "Hrs_LPNadmin", "double"), ("Hrs_LPN", "double", "Hrs_LPN", "double"), ("Hrs_CNA", "double", "Hrs_CNA", "double"), ("Hrs_NAtrn", "double", "Hrs_NAtrn", "double"), ("Hrs_MedAide", "double", "Hrs_MedAide", "double")], transformation_ctx="ChangeSchema_node1794849017155")

# Script generated for node SQL Query
SqlQuery0 = '''
WITH overtime AS(
	SELECT
		CCN,
		provider_name,
		city,
		state,
		WorkDate,
		Hrs_RNDON,
		CASE
			WHEN Hrs_RNDON > 8
			THEN Hrs_RNDON - 8
			ELSE 0
		END AS OT_Hrs_RNDON,
		Hrs_RNadmin,
		CASE
			WHEN Hrs_RNadmin > 8
			THEN Hrs_RNadmin - 8
			ELSE 0
		END AS OT_Hrs_RNadmin,
		Hrs_RN,
		CASE
			WHEN Hrs_RN > 8
			THEN Hrs_RN - 8
			ELSE 0
		END AS OT_Hrs_RN,
		Hrs_LPNadmin,
		CASE
			WHEN Hrs_LPNadmin > 8
			THEN Hrs_LPNadmin - 8
			ELSE 0
		END AS OT_Hrs_LPNadmin,
		Hrs_LPN,
		CASE
			WHEN Hrs_LPN > 8
			THEN Hrs_LPN - 8
			ELSE 0
		END AS OT_Hrs_LPN,
		Hrs_CNA,
		CASE
			WHEN Hrs_CNA > 8
			THEN Hrs_CNA - 8
			ELSE 0
		END AS OT_Hrs_CNA,
		Hrs_NAtrn,
		CASE
			WHEN Hrs_NAtrn > 8
			THEN Hrs_NAtrn - 8
			ELSE 0
		END AS OT_Hrs_NAtrn,
		Hrs_MedAide,
		CASE
			WHEN Hrs_MedAide > 8
			THEN Hrs_MedAide - 8
			ELSE 0
		END AS OT_Hrs_MedAide
	FROM
		myDataSource
)

SELECT
	CCN,
	provider_name,
	state,
	SUM(OT_Hrs_RNDON) AS Total_OT_Hrs_RNDON,
	SUM(OT_Hrs_RNadmin) AS Total_OT_Hrs_RNadmin,
	SUM(OT_Hrs_RN) AS Total_OT_Hrs_RN,
	SUM(OT_Hrs_LPNadmin) AS Total_OT_Hrs_LPNadmin,
	SUM(OT_Hrs_LPN) AS Total_OT_Hrs_LPN,
	SUM(OT_Hrs_CNA) AS Total_OT_Hrs_CNA,
	SUM(OT_Hrs_NAtrn) AS Total_OT_Hrs_NAtrn,
	SUM(OT_Hrs_MedAide) AS Total_OT_Hrs_MedAide
FROM
	overtime
GROUP BY
	CCN, provider_name, state

'''
SQLQuery_node1794849437988 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":ChangeSchema_node1794849017155}, transformation_ctx = "SQLQuery_node1794849437988")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1794849437988, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794847850055", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SQLQuery_node1794849437988.count() >= 1):
   SQLQuery_node1794849437988 = SQLQuery_node1794849437988.coalesce(1)
AmazonS3_node1794849141441 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1794849437988, connection_type="s3", format="glueparquet", connection_options={"path": "s3://healthcare-project-sampollack/gold/overtime-hrs/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1794849141441")

job.commit()