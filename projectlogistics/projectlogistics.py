import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

s3_input_path_trackingdata = "s3://logistic-visibility/tracking-data/Tracking_data.csv"

df_trackingdata = spark.read.csv(s3_input_path_trackingdata, header = True, inferSchema = True)

df_trackingdata.show()

df_trackingdata.printSchema()
print("Tracking data pulled successfully and store in df_trackingdata")

#1 validate trackingnumber, event, eventdate, eventtime, currentaddress is not null
#Checking one by one so businees can directly pull the required data and address it to carrier
df_td_nullTrackingNumber_validate1 = df_trackingdata.filter(col("Tracking_number").isNull())
df_td_nullEvent_validate1 = df_trackingdata.filter(col("Event").isNull())
df_td_nullEventDate_validate1 = df_trackingdata.filter(col("Event Date").isNull())
df_td_nullEventTime_validate1 = df_trackingdata.filter(col("Event Time").isNull())
df_td_nullCurrentAddress_validate1 = df_trackingdata.filter(col("Current Address").isNull())

#pull all the valid data (not null) ------Valid data validate 1(td = tracking data)
df_td_valid_validate1 = df_trackingdata.dropna(how="any")
print("validate 1 successfully ran")


#2 validate the date and time format in the csv file
#checked in the schema, if there is even one different format than date, then string is expected to be the data type
df_td_date_schema = df_td_valid_validate1.select("Event Date").printSchema()
df_td_time_schema = df_td_valid_validate1.select("Event Time").printSchema()

#pull all the valid data (fromat check) ------Valid data validate 2
from pyspark.sql import functions as F

DATE_COLUMN = "Event Date"
EXPECTED_DATE_FORMAT = "MM/dd/yyyy"

TIME_COLUMN = "Event Time"
EXPECTED_TIME_FORMAT = "HH:mm:ss"

df_td_valid_validate2 = df_td_valid_validate1.filter(F.to_date(F.col(DATE_COLUMN), EXPECTED_DATE_FORMAT).isNotNull() & F.to_timestamp(F.col(TIME_COLUMN), EXPECTED_TIME_FORMAT).isNotNull())

print("validate 2 successfully ran")

#3 validate if the mode falls under Air / Ocean / Road
#pull invalid data for business and support team to highlight the issue
valid_modes = ["Air", "Ocean", "Road"]
df_td_invlaid_validate3 = df_td_valid_validate2.filter(~col("Mode").isin(valid_modes))

#pull all the valid data (Mode check) ------Valid data validate 3
df_td_valid_validate3 = df_td_valid_validate2.filter(col("Mode").isin(valid_modes))

print("validate 3 successfully ran")

#4 validate if (1)event date is less than EDD 
#pull data for business and support team to HIGHLIGHT THE DELAYED SHIPMENTS
df_td_invalid_validate4_1 = df_td_valid_validate3.filter(col("Event Date") > col("Estimated Delivery Date"))

print("validate 4 successfully ran")

#shipments table
s3_input_path_shipmentdata = "s3://shipment-confirmation-data/EDI945_29112025/29_11_2025_EDI945.csv"

df_shipmentdata = spark.read.csv(s3_input_path_shipmentdata, header = True, inferSchema = True)

df_shipmentdata.show()

df_shipmentdata.printSchema()

df_shipmentdata1 = df_shipmentdata.withColumnRenamed("From_Address", "Shipment_From_Address").withColumnRenamed("To_Address", "Shipment_To_Address").withColumnRenamed("Mode", "Shipment_Mode")

df_joined = df_shipmentdata1.join(df_td_valid_validate3, on="Tracking_number", how="inner")
df_joined.show()

#metric 1 - check if shipment delayed / near delivery / ok
from pyspark.sql.functions import lit, when, datediff
df_valid_metric1 = df_joined.withColumn("metric1", when((col("Event Date") > col("Estimated Delivery Date")) & (col("Event") != lit("Delivered")), lit("Delayed")).when((datediff(col("Estimated Delivery Date"), col("Event Date")) < 2) & (col("Event").isin("Pickedup", "In transit 1", "In transit 2")),lit("Near Delivery")).otherwise(lit("OK"))
)

df_delayed_shipment = df_valid_metric1.filter(col("metric1") == "Delayed")
print("priniting delayed shipment")
df_delayed_shipment.show()

df_neardelivery_shipment = df_valid_metric1.filter(col("metric1") == "Near Delivery")
print("priniting Near Delivery shipment")
df_neardelivery_shipment.show()

#metric 2 - check if its priority shipment based on quantity
df_valid_metric2 = df_valid_metric1.withColumn("priority", when(col("Quantity") > 250, lit("High priority")).when((col("Quantity") > 50) & (col("Quantity") < 250), lit("Medium priority")).otherwise(lit("Low priority")))
df_valid_metric2.show()

print(f"-------------Rows after df_valid_metric2: {df_valid_metric2.count()} -----------")

S3_OUTPUT_PATH = "s3://project-logistics-output/output/"
GLUE_DATABASE = "logistics_analytics"
GLUE_TABLE_NAME = "shipment_data_metrics_full"

# 1. Write the DataFrame to S3 in Parquet format
df_valid_metric2.write.mode("overwrite").format("parquet").save(S3_OUTPUT_PATH)

print(f"All data successfully written to S3 path: {S3_OUTPUT_PATH}")

# 2. Create/Replace the External Table in the Glue Data Catalog for Athena

# a) Create a temporary view of the DataFrame
df_valid_metric2.createOrReplaceTempView("temp_metrics_view")

# b) Use Spark SQL to create the external table in the Glue Catalog.
# This registers the table schema and location with Athena.
# The table is defined using the schema from the DataFrame.
spark.sql(f"""CREATE EXTERNAL TABLE IF NOT EXISTS {GLUE_DATABASE}.{GLUE_TABLE_NAME} USING PARQUET LOCATION '{S3_OUTPUT_PATH}' AS SELECT * FROM temp_metrics_view WHERE 1=0 """)

print(f"External table '{GLUE_TABLE_NAME}' created/updated in Glue Catalog.")