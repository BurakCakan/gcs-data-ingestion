from google.cloud import storage
import json
from sparkUtils import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import os  
import datetime 
from dotenv import load_dotenv

load_dotenv()

allFilePaths = [
    "webshop.public.category",
#    "webshop.public.customer",
#    "webshop.public.customeraddress",
#    "webshop.public.customerpaymentprovider"
#    "webshop.public.event"
#    "webshop.public.orderitem",
#    "webshop.public.orders",
#    "webshop.public.paymentprovider",
#    "webshop.public.product",
#    "webshop.public.productbrand",
#    "webshop.public.productcategory"
]

def get_gcs_client():
    """
    Create and return a client object for Google Cloud Storage.
    """
    return storage.Client.from_service_account_json(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

#os.environ['GOOGLE_APPLICATION_CREDENTIALS']=SERVICE_ACC_PATH
#getenv

def setup_spark():
    """
    Set up and return a SparkSession object.
    """
    spark = sessionBuilder("GCSDataMerger", os.environ["SPARK_JAR"])

    spark.sparkContext.setLogLevel('WARN')

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", True)
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize",10000)

    spark._jsc.hadoopConfiguration().set('spak.hadoop.fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS')
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')

    spark._jsc.hadoopConfiguration().set('spak.hadoop.fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

    return spark
 
 #TODO: make refactoring for below

def main():
    
    storage_client = get_gcs_client()
    spark = setup_spark()
    processTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for schema in allFilePaths:
    
        gcs_path = os.environ["GCS_PATH"]
        bucket_name = os.environ["SOURCE_BUCKET_NAME"]
        subfolder = os.environ["SINK_BUCKET_NAME"]

    # Read raw data from GCS
        rawData = read_json_from_gcs(spark, schema, gcs_path)

#    rawData = spark. \
#     read. \
#     option('inferTimestamp','false'). \
#     format("json"). \
#     option("header", True).  \
#     option("inferSchema", True). \
#     load(os.environ["GCS_PATH"]+schema+"/*.json")
     
     # Write raw data to GCS in Parquet format
        write_data_to_gcs(rawData, bucket_name, subfolder, schema, processTime, "parquet", "bronze", 20)

#        rawData. \
#        coalesce(20). \
#        write. \
#        mode("overwrite"). \
#        format("parquet"). \
#        save("gs://{}/{}/{}/{}/{}".format(os.environ["SOURCE_BUCKET_NAME"],
#                                       os.environ["SINK_BUCKET_NAME"],
#                                       'bronze',
#                                       schema,
#                                       processTime
#                                       ))

    # selecting only related data without metadata
        afterData = rawData.select("value.after.*")

#        enrichedData = afterData.withColumn("created_ts", 
#                        F.to_utc_timestamp(
#                            F.from_unixtime(
#                                F.col("created_at")/1_000_000,'yyyy-MM-dd HH:mm:ss'
#                                            ),'GMT+1'
#                                        )) \
#            .withColumn("modified_ts", 
#                        F.to_utc_timestamp(
#                            F.from_unixtime(
#                                F.col("modified_at")/1_000_000,'yyyy-MM-dd HH:mm:ss'
#                                            ),'GMT+1'
#                                        ))

        enrichedData = afterData.withColumn("created_ts", convert_millis_to_datetime("created_at", "GMT+1")) \
                                .withColumn("modified_ts", convert_millis_to_datetime("modified_at", "GMT+1"))

        write_data_to_gcs(enrichedData, bucket_name, subfolder, schema, processTime, "json", "silver", 20)

#        enrichedData. \
#        coalesce(20). \
#        write. \
#        mode("overwrite"). \
#        format("json"). \
#        save("gs://{}/{}/{}/{}/{}".format(os.environ["SOURCE_BUCKET_NAME"],
#                                       os.environ["SINK_BUCKET_NAME"],
#                                       'silver',
#                                       schema,
#                                       processTime
#                                       ))

main()

print('Successfull completion')

