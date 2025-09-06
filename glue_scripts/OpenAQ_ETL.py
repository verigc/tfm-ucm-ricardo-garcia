import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_derived

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node locations
locations_node1756254701447 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://tfm-ucm/staging/OpenAQ/locations/locations.parquet"], "recurse": True}, transformation_ctx="locations_node1756254701447")

# Script generated for node parameters
parameters_node1756255147815 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://tfm-ucm/staging/OpenAQ/parameters/parameters.parquet"], "recurse": True}, transformation_ctx="parameters_node1756255147815")

# Script generated for node measurements
measurements_node1756172542186 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://tfm-ucm/staging/OpenAQ/measurements/"], "recurse": True}, transformation_ctx="measurements_node1756172542186")

# Script generated for node Change Schema Locations
ChangeSchemaLocations_node1756255337579 = ApplyMapping.apply(frame=locations_node1756254701447, mappings=[("id", "long", "id", "long"), ("name", "string", "locations_name", "string"), ("locality", "string", "locality", "string"), ("timezone", "string", "timezone", "string"), ("country", "string", "country", "string"), ("ismonitor", "boolean", "ismonitor", "boolean"), ("latitud", "double", "latitud", "double"), ("longitud", "double", "longitud", "double"), ("sensor_id", "long", "sensor_id", "long"), ("parameter_id", "long", "parameter_id", "long")], transformation_ctx="ChangeSchemaLocations_node1756255337579")

# Script generated for node Change Schema Parameters
ChangeSchemaParameters_node1756255241507 = ApplyMapping.apply(frame=parameters_node1756255147815, mappings=[("id", "long", "id", "long"), ("name", "string", "parameter_name", "string"), ("units", "string", "parameter_units", "string"), ("displayname", "string", "parameter_displayname", "string"), ("description", "string", "parameter_description", "string")], transformation_ctx="ChangeSchemaParameters_node1756255241507")

# Script generated for node Derived Column year
DerivedColumnyear_node1756428719394 = measurements_node1756172542186.gs_derived(colName="year", expr="year(datetimeto)")

# Script generated for node Join Loca-Param
JoinLocaParam_node1756255502469 = Join.apply(frame1=ChangeSchemaLocations_node1756255337579, frame2=ChangeSchemaParameters_node1756255241507, keys1=["parameter_id"], keys2=["id"], transformation_ctx="JoinLocaParam_node1756255502469")

# Script generated for node Derived Column month
DerivedColumnmonth_node1756428788482 = DerivedColumnyear_node1756428719394.gs_derived(colName="month", expr="month(datetimeto)")

# Script generated for node Final Join
FinalJoin_node1756257361750 = Join.apply(frame1=DerivedColumnmonth_node1756428788482, frame2=JoinLocaParam_node1756255502469, keys1=["sensor_id"], keys2=["sensor_id"], transformation_ctx="FinalJoin_node1756257361750")

# Script generated for node Datalake
if (FinalJoin_node1756257361750.count() >= 1):
   FinalJoin_node1756257361750 = FinalJoin_node1756257361750.coalesce(1)
Datalake_node1756257717126 = glueContext.write_dynamic_frame.from_options(frame=FinalJoin_node1756257361750, connection_type="s3", format="glueparquet", connection_options={"path": "s3://tfm-ucm/processed/openaq/measurements/", "partitionKeys": ["locality", "year", "month"]}, format_options={"compression": "snappy"}, transformation_ctx="Datalake_node1756257717126")

job.commit()