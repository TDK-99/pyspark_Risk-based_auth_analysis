from pyspark.sql import SparkSession
import boto3
from pyspark.sql.functions import col, count, when, sum as _sum, countDistinct, length
import pandas as pd


from src.config_import import validate_data



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
    # value null row count
        null_count = df.filter(col(c).isNull()).count()
    
    # check if col have dont have null value if raise error
    if 0 < null_count < df.count():
        raise ValueError(f"COLUMN_NOT_NULL:{c} column shouldn't have {null_count} null values") # raise error if a column of list have a single 1 null value


    # CHECK MAX VALUE % OF NULL VALUE IN COL

    # % null x col
    max_null_perc = {
        "Region": 0.15,       
        "City": 0.10,         
        "Device Type": 0.01,  
    }

    # extract all col null row

    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) 
        for c in df.columns
    ]).collect()[0].asDict()


    # trasform in dict and  after in dataframe

    df_null = pd.DataFrame(null_counts.items(), columns=["col", "cont_null"])

    df_limit_null = pd.DataFrame(max_null_perc.items(), columns=["col", "perc_null"])


    df_count = df.count()

    # count all row
    
    df_null["row"] = df_count


    df_null = df_null.merge(df_limit_null,on="col", how="inner")

    # calcultate % of null and check if exceed the value set

    df_null["perc_null_df"] = ((df_null["cont_null"]/df_null["row"])*100).round(2)

    df_null["perc_null_df"] = df_null["perc_null_df"].astype(float)
    df_null["perc_null"] = df_null["perc_null"].astype(float)


    failed = df_null[df_null["perc_null_df"] > df_null["perc_null"]]
    failed["col"].tolist()

    # raise error if col exceed mx value %
    
    if not failed.empty:
        raise ValueError(f"COLUMN_NULL_VALUES: column {failed['col'].tolist()} exceed the max of null value")


    # CHECK INDEX COL HAVE ALL DISTINCT VALUE

    distinct_index = df.select([
        countDistinct(col("index"))]).collect()[0][0]

    # confront with  count row

    if df_count != distinct_index:
        raise ValueError("Col index dont have all distinct value")


    # CHECK ALL COUNTRY ARE ONLY 2 LETTER

    from pyspark.sql.functions import col,

    invalid_countries = df.filter(
        (length(col("Country")) > 2) | (length(col("Country")) < 2)
    ).groupBy("Country") \
    .agg(count("*").alias("cnt")) \
    .orderBy("cnt", ascending=False) \
    .limit(50) \
    .collect()

    result_dict = {row["Country"]: row["cnt"] for row in invalid_countries}

    country_df = pd.DataFrame(result_dict.items(), columns=["country", "cnt"])

    if not country_df.empty:
        raise ValueError(f"There is a Country with more than 2 letters or less: {result_dict}")


