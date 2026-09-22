from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession

load_dotenv("/app/.env")


spark = SparkSession.builder \
    .appName("ReadDataFromS3") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1") \
    .config("spark.jars.ivy", "/tmp/.ivy2") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"]) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"]) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com") \
    .master("spark://spark-master:7077") \
    .getOrCreate()




import boto3

s3_client = boto3.client("s3")
parquet_path = "s3a://sparkanalysisauth/rba-dataset.parquet"

try:
    s3_client.head_object(
        Bucket="sparkanalysisauth",
        Key="rba-dataset.parquet/_SUCCESS"
    )
    df = spark.read.parquet(parquet_path)
    print("parquet already dowload")
except:
    df = spark.read.csv(
        "s3a://sparkanalysisauth/rba-dataset.csv",
        header=True,
        inferSchema=True
    )
    df.write.parquet(parquet_path)
    print("parquet is not exist, dowload file...")