from pyspark.sql import SparkSession


def analysis(spark,df):

    df.createOrReplaceTempView("loggins")

    

    # ============================================================
    # 1. GENERAL OVERVIEW KPI
    # ============================================================


    kpi_overview = spark.sql("""
        SELECT 
            COUNT(*) as total,
            SUM(CAST(`Login Successful` AS INT)) as success,
            SUM(CAST(`Is Attack IP` AS INT)) as attacks,
            SUM(CAST(`Is Account Takeover` AS INT)) as takeover
        FROM loggins
    """).collect()[0]

    login_tot = kpi_overview["total"]
    login_succ = kpi_overview["success"]
    login_fail = kpi_overview["total"]-kpi_overview["success"]
    ip_attack = kpi_overview["attacks"]
    acc_take = kpi_overview["takeover"]

    perc_attack = round(((ip_attack/login_tot)*100),1)

    perc_take = round(((acc_take/ip_attack)*100),3)
