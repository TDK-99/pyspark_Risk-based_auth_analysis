from pyspark.sql import SparkSession


def analysis(spark,df):

    df.createOrReplaceTempView("logins")

    results = {}

    # ============================================================
    # 1. GENERAL OVERVIEW KPI
    # ============================================================


    results["overview"] = spark.sql("""
        SELECT COUNT(*) as total_logins,
               SUM(CAST(`Login Successful` AS INT)) as successful,
               COUNT(*) - SUM(CAST(`Login Successful` AS INT)) as failed,
               SUM(CAST(`Is Attack IP` AS INT)) as attacks,
               SUM(CAST(`Is Account Takeover` AS INT)) as takeovers,
               COUNT(DISTINCT `User ID`) as distinct_users,
               COUNT(DISTINCT `IP Address`) as distinct_ips,
               ROUND(SUM(CAST(`Is Attack IP` AS INT)) / COUNT(*) * 100, 2) as attack_rate,
               ROUND(SUM(CAST(`Is Account Takeover` AS INT)) / SUM(CAST(`Is Attack IP` AS INT)) * 100, 3) as takeover_rate
        FROM logins
    """).toPandas()
