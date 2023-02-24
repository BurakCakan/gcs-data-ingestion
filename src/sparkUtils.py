from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, explode, split, from_unixtime, to_utc_timestamp
from pyspark.sql.types import TimestampType

def sessionBuilder(name: str, sparkJarPath: str) -> SparkSession:

    """"
    Start a spark session making the local computer as master
    """
    spark = SparkSession \
        .builder \
        .master('local[*]') \
        .config("spark.driver.memory", "15g") \
        .config("spark.executor.memory", "10g") \
        .config("spark.executor.memoryOverhead", "10g") \
        .appName(name) \
        .config("spark.jars", sparkJarPath) \
        .getOrCreate()

    return spark

def convert_millis_to_datetime(col_name: str, from_tz: str) -> str:

    """
    Convert a column with epoch milliseconds to datetime with timezone conversion
    :param col_name: the name of the column to be converted
    :param from_tz: the timezone of the original timestamp
    :return: the datetime as timestamp format 
    """

    return F.to_utc_timestamp(
        F.from_unixtime(F.col(col_name) / 1_000_000, 'yyyy-MM-dd HH:mm:ss'),
        from_tz
    ).cast(TimestampType()) 
    
def read_json_from_gcs(spark, schema, gcs_path):

    """
    Reads multi json files from a gcs bucket
    - spark: it is the related spark session
    - schema: it is the schema of the data, there are lots of schema inside the main bucket folder and this parameter specifies that
    - gcs_path: path of the related gcs bucket
    """

    rawData = spark. \
        read. \
        option('inferTimestamp','false'). \
        format("json"). \
        option("header", True).  \
        option("inferSchema", True). \
        load(gcs_path + schema + "/*.json")
    return rawData

def write_data_to_gcs(df, bucket_name, subfolder, schema, process_time, file_format, layer, npartition):

    """
    Writes the data to a new bucket inside gcs with specified format, layer and partition
    - df: related data which is intended to be written
    - bucket_name: name of the main bucket
    - subfolder: name of the sink folder inside the main bucket
    - schema: schema of the related data like category, product etc.
    - process_time: datatime when the job started
    - file_format: specifies the type of data like json, parquet, avro etc.
    - npartition: number of the partitions specified
    """

    df.coalesce(npartition). \
        write. \
        mode("overwrite"). \
        format(file_format). \
        save("gs://{}/{}/{}/{}/{}".format(bucket_name,
                                           subfolder,
                                           layer,
                                           schema,
                                           process_time))
