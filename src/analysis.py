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


    # ============================================================
    # 2. ATTACKS BY HOUR
    # ============================================================

    
    results["attacks_by_hour"] = spark.sql("""
        SELECT HOUR(`Login Timestamp`) as hour,
               COUNT(*) as total_logins,
               SUM(CAST(`Is Attack IP` AS INT)) as attacks,
               SUM(CASE WHEN `Is Attack IP` = false THEN 1 ELSE 0 END) as legit_logins,
               ROUND(SUM(CAST(`Is Attack IP` AS INT)) / COUNT(*) * 100, 2) as attack_rate
        FROM logins
        GROUP BY HOUR(`Login Timestamp`)
        ORDER BY hour
    """).toPandas()

 
    # ============================================================
    # 3. TOP COUNTRIES
    # ============================================================


    results["top_countries"] = spark.sql("""
        SELECT Country,
               COUNT(*) as total_logins,
               SUM(CAST(`Is Attack IP` AS INT)) as attacks,
               SUM(CAST(`Is Account Takeover` AS INT)) as takeovers,
               ROUND(SUM(CAST(`Is Attack IP` AS INT)) / COUNT(*) * 100, 2) as attack_rate,
               ROUND(SUM(CAST(`Is Account Takeover` AS INT)) / COUNT(*) * 100, 4) as takeover_rate
        FROM logins
        GROUP BY Country
        HAVING COUNT(*) >= 1000
        ORDER BY attacks DESC
        LIMIT 20
    """).toPandas()

 
    # ============================================================
    # 4. DEVICE PROFILING
    # ============================================================


    results["device_profiling"] = spark.sql("""
        SELECT `Device Type`,
               COUNT(*) as total,
               SUM(CAST(`Is Attack IP` AS INT)) as attacks,
               SUM(CASE WHEN `Is Attack IP` = false THEN 1 ELSE 0 END) as legit,
               ROUND(SUM(CAST(`Is Attack IP` AS INT)) / COUNT(*) * 100, 2) as attack_rate
        FROM logins
        GROUP BY `Device Type`
        ORDER BY total DESC
    """).toPandas()

 
    # ============================================================
    # 5. MOST TARGETED USERS
    # ============================================================


    results["targeted_users"] = spark.sql("""
        SELECT `User ID`,
               COUNT(*) as total_logins,
               SUM(CAST(`Is Attack IP` AS INT)) as attacks,
               SUM(CAST(`Is Account Takeover` AS INT)) as takeovers,
               COUNT(DISTINCT Country) as countries,
               COUNT(DISTINCT `IP Address`) as distinct_ips
        FROM logins
        GROUP BY `User ID`
        ORDER BY attacks DESC
        LIMIT 20
    """).toPandas()

 
    # ============================================================
    # 6. RTT: ATTACKS VS LEGITIMATE
    # ============================================================


    results["rtt_comparison"] = spark.sql("""
        SELECT `Is Attack IP`,
               COUNT(*) as total,
               ROUND(AVG(`Round-Trip Time [ms]`), 2) as avg_rtt,
               PERCENTILE_APPROX(`Round-Trip Time [ms]`, 0.5) as median_rtt,
               PERCENTILE_APPROX(`Round-Trip Time [ms]`, 0.25) as p25_rtt,
               PERCENTILE_APPROX(`Round-Trip Time [ms]`, 0.75) as p75_rtt,
               PERCENTILE_APPROX(`Round-Trip Time [ms]`, 0.95) as p95_rtt
        FROM logins
        GROUP BY `Is Attack IP`
    """).toPandas()
 
    return results
