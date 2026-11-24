import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
import gs_derived

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

# Script generated for node Silver_Providers
Silver_Providers_node1794511125140 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://healthcare-project-sampollack/silver/providers/run-1794516245498-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="Silver_Providers_node1794511125140")

# Script generated for node Silver_Staffing
Silver_Staffing_node1794588384556 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://healthcare-project-sampollack/silver/staffing/part-00000-d22974d2-0c2e-4e77-854f-23b3ca15689e-c000.snappy.parquet"], "recurse": True}, transformation_ctx="Silver_Staffing_node1794588384556")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1794588951400 = ApplyMapping.apply(frame=Silver_Staffing_node1794588384556, mappings=[("provnum", "string", "right_provnum", "string"), ("provname", "string", "right_provname", "string"), ("city", "string", "right_city", "string"), ("state", "string", "right_state", "string"), ("workdate", "date", "right_workdate", "date"), ("mdscensus", "int", "right_mdscensus", "int"), ("hrs_rndon", "double", "right_hrs_rndon", "double"), ("hrs_rndon_emp", "double", "right_hrs_rndon_emp", "double"), ("hrs_rndon_ctr", "double", "right_hrs_rndon_ctr", "double"), ("hrs_rnadmin", "double", "right_hrs_rnadmin", "double"), ("hrs_rnadmin_emp", "double", "right_hrs_rnadmin_emp", "double"), ("hrs_rnadmin_ctr", "double", "right_hrs_rnadmin_ctr", "double"), ("hrs_rn", "double", "right_hrs_rn", "double"), ("hrs_rn_emp", "double", "right_hrs_rn_emp", "double"), ("hrs_rn_ctr", "double", "right_hrs_rn_ctr", "double"), ("hrs_lpnadmin", "double", "right_hrs_lpnadmin", "double"), ("hrs_lpnadmin_emp", "double", "right_hrs_lpnadmin_emp", "double"), ("hrs_lpnadmin_ctr", "double", "right_hrs_lpnadmin_ctr", "double"), ("hrs_lpn", "double", "right_hrs_lpn", "double"), ("hrs_lpn_emp", "double", "right_hrs_lpn_emp", "double"), ("hrs_lpn_ctr", "double", "right_hrs_lpn_ctr", "double"), ("hrs_cna", "double", "right_hrs_cna", "double"), ("hrs_cna_emp", "double", "right_hrs_cna_emp", "double"), ("hrs_cna_ctr", "double", "right_hrs_cna_ctr", "double"), ("hrs_natrn", "double", "right_hrs_natrn", "double"), ("hrs_natrn_emp", "double", "right_hrs_natrn_emp", "double"), ("hrs_natrn_ctr", "double", "right_hrs_natrn_ctr", "double"), ("hrs_medaide", "double", "right_hrs_medaide", "double"), ("hrs_medaide_emp", "double", "right_hrs_medaide_emp", "double"), ("hrs_medaide_ctr", "double", "right_hrs_medaide_ctr", "double")], transformation_ctx="RenamedkeysforJoin_node1794588951400")

# Script generated for node Join
Silver_Providers_node1794511125140DF = Silver_Providers_node1794511125140.toDF()
RenamedkeysforJoin_node1794588951400DF = RenamedkeysforJoin_node1794588951400.toDF()
Join_node1794588438766 = DynamicFrame.fromDF(Silver_Providers_node1794511125140DF.join(RenamedkeysforJoin_node1794588951400DF, (Silver_Providers_node1794511125140DF['cms_certification_number__ccn_#0'] == RenamedkeysforJoin_node1794588951400DF['right_provnum']), "right"), glueContext, "Join_node1794588438766")

# Script generated for node Renamed keys for Derived Column
RenamedkeysforDerivedColumn_node1794590645995 = ApplyMapping.apply(frame=Join_node1794588438766, mappings=[("cms_certification_number__ccn_#0", "string", "CCN", "string"), ("provider_name#1", "string", "provider_name", "string"), ("state", "string", "state", "string"), ("number_of_certified_beds#2", "int", "certified_beds", "int"), ("average_number_of_residents_per_day#3", "decimal", "avg_residents_per_day", "decimal"), ("average_number_of_residents_per_day_footnote#4", "null", "avg_residents_footnote", "null"), ("provider_type#5", "string", "provider_type", "string"), ("reported_nurse_aide_staffing_hours_per_resident_per_day#6", "float", "rep_nurse_aide_hrs_per_res_per_day", "float"), ("reported_lpn_staffing_hours_per_resident_per_day#7", "float", "rep_lpn_hrs_per_res_per_day", "float"), ("reported_rn_staffing_hours_per_resident_per_day#8", "float", "rep_rn_hrs_per_res_per_day", "float"), ("reported_licensed_staffing_hours_per_resident_per_day#9", "float", "rep_licensed_staff_hrs_per_res_per_day", "float"), ("reported_total_nurse_staffing_hours_per_resident_per_day#10", "float", "rep_total_nurse_staff_hours_per_res_per_day", "float"), ("total_number_of_nurse_staff_hours_per_resident_per_day_on_the_weekend#11", "float", "total_nurse_staff_weekend_hrs_per_res_per_day", "float"), ("registered_nurse_hours_per_resident_per_day_on_the_weekend#12", "float", "rn_weekend_hrs_per_res_per_day", "float"), ("reported_physical_therapist_staffing_hours_per_resident_per_day#13", "float", "rep_physical_therapist_hours_per_res_per_day", "float"), ("nursing_case-mix_index#14", "float", "nursing_case-mix_index", "float"), ("nursing_case-mix_index_ratio#15", "float", "nursing_case-mix_index_ratio", "float"), ("case-mix_nurse_aide_staffing_hours_per_resident_per_day#16", "float", "case-mix_nurse_aide_hrs_per_res_per_day", "float"), ("case-mix_lpn_staffing_hours_per_resident_per_day#17", "float", "case-mix_lpn_hrs_per_res_per_day", "float"), ("case-mix_rn_staffing_hours_per_resident_per_day#18", "float", "case-mix_rn_hrs_per_res_per_day", "float"), ("case-mix_total_nurse_staffing_hours_per_resident_per_day#19", "float", "case-mix_total_nurse_staff_hrs_per_res_per_day", "float"), ("case-mix_weekend_total_nurse_staffing_hours_per_resident_per_day#20", "float", "case-mix_weekend_total_nurse_staff_hours_per_res_per_day", "float"), ("adjusted_nurse_aide_staffing_hours_per_resident_per_day#21", "float", "adj_nurse_aide_hrs_per_res_per_day", "float"), ("adjusted_lpn_staffing_hours_per_resident_per_day#22", "float", "adj_lpn_hrs_per_res_per_day", "float"), ("adjusted_rn_staffing_hours_per_resident_per_day#23", "float", "adj_rn_hrs_per_res_per_day", "float"), ("adjusted_total_nurse_staffing_hours_per_resident_per_day#24", "float", "adj_total_nurse_staff_hrs_per_res_per_day", "float"), ("adjusted_weekend_total_nurse_staffing_hours_per_resident_per_day#25", "float", "adj_weekend_total_nurse_staff_hrs_per_res_per_day", "float"), ("processing_date#26", "date", "processing_date", "date"), ("right_workdate", "date", "daily_workdate", "date"), ("right_mdscensus", "int", "daily_mdscensus", "int"), ("right_hrs_rndon", "double", "daily_hrs_rndon", "double"), ("right_hrs_rnadmin", "double", "daily_hrs_rnadmin", "double"), ("right_hrs_rn", "double", "daily_hrs_rn", "double"), ("right_hrs_lpnadmin", "double", "daily_hrs_lpnadmin", "double"), ("right_hrs_lpn", "double", "daily_hrs_lpn", "double"), ("right_hrs_cna", "double", "daily_hrs_cna", "double"), ("right_hrs_natrn", "double", "daily_hrs_natrn", "double"), ("right_hrs_medaide", "double", "daily_hrs_medaide", "double")], transformation_ctx="RenamedkeysforDerivedColumn_node1794590645995")

# Script generated for node occupancy_rate
occupancy_rate_node1794593438308 = RenamedkeysforDerivedColumn_node1794590645995.gs_derived(colName="occupancy_rate", expr="avg_residents_per_day / certified_beds")

# Script generated for node avg daily staffing
SqlQuery0 = '''
WITH daily_averages AS(
	SELECT
		CCN,
		provider_name,
		state,
		occupancy_rate,
		avg_residents_per_day,
		AVG(daily_hrs_rndon) AS avg_daily_hrs_rndon,
		AVG(daily_hrs_rnadmin) AS avg_daily_hrs_rnadmin,
		AVG(daily_hrs_rn) AS avg_daily_hrs_rn,
		AVG(daily_hrs_lpnadmin) AS avg_daily_hrs_lpnadmin,
		AVG(daily_hrs_lpn) AS avg_daily_hrs_lpn,
		AVG(daily_hrs_cna) AS avg_daily_hrs_cna,
		AVG(daily_hrs_natrn) AS avg_daily_hrs_natrn,
		AVG(daily_hrs_medaide) AS avg_daily_hrs_medaide
	FROM
		myDataSource
	GROUP BY
		CCN, provider_name, state, occupancy_rate, avg_residents_per_day
)

SELECT
	*
FROM
	daily_averages
ORDER BY
	occupancy_rate DESC
'''
avgdailystaffing_node1794595921727 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":occupancy_rate_node1794593438308}, transformation_ctx = "avgdailystaffing_node1794595921727")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=avgdailystaffing_node1794595921727, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794847850055", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1794852565726 = glueContext.write_dynamic_frame.from_options(frame=avgdailystaffing_node1794595921727, connection_type="s3", format="glueparquet", connection_options={"path": "s3://healthcare-project-sampollack/gold/staffing-vs-occupancy/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1794852565726")

job.commit()