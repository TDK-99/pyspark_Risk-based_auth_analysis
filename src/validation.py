from pyspark.sql import SparkSession
import boto3
from pyspark.sql.functions import col, count, when, sum as _sum



def data_validation(df):

    # CHECK EXPECTED DATA VALUES


    # convert true false column in boolean

    df = df.withColumn("Login Successful", col("Login Successful").cast("boolean"))

    df = df.withColumn("Is Attack IP", col("Is Attack IP").cast("boolean"))

    df = df.withColumn("Is Account Takeover", col("Is Account Takeover").cast("boolean"))


    expected_schema = {
        "index": "int",
        "Login Timestamp": "timestamp",
        "User ID": "bigint",
        "Round-Trip Time [ms]": "double",
        "IP Address": "string",
        "Country": "string",
        "Region": "string",
        "City": "string",
        "ASN": "int",
        "User Agent String": "string",
        "Browser Name and Version": "string",
        "OS Name and Version": "string",
        "Device Type": "string",
        "Login Successful": "boolean",
        "Is Attack IP": "boolean",
        "Is Account Takeover": "boolean",
    }


    data_types=dict(df.dtypes) 

    shared_items = {k: data_types[k] for k in data_types if k in expected_schema and data_types[k] == expected_schema[k]}


    if len(shared_items) != 16:
        raise ValueError(f"DATA VALUE: match data type is {len(shared_items)} instead of 16") #raise error if  the  exp and data type is not full match


    # CHECK NOT NULL COLUMN

    not_null_col = ["index", "User ID", "IP Address", "Login Timestamp"]

    for c in not_null_col:
    # Conta quante righe hanno valori nulli nella colonna
        null_count = df.filter(col(c).isNull()).count()
    
    # Se ci sono nulli, ma la colonna non è completamente vuota
    if 0 < null_count < df.count():
        raise ValueError(f"COLUMN_NOT_NULL:{c} column shouldn't have {null_count} null values") # raise error if a column of list have a single 1 null value






