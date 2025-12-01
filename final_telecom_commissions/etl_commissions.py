from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, round, current_date, coalesce
 
# ---------- CONFIG ----------
PROJECT_ID     = "telecom-cdr"
BQ_DATASET     = "commissions_ds"
FACT_TABLE     = f"{PROJECT_ID}.{BQ_DATASET}.commissions_fact"
DIM_TABLE      = f"{PROJECT_ID}.{BQ_DATASET}.agent_dim"
INPUT_FILE     = sys.argv[1]   # Passed by Airflow (e.g. gs://telecom-raw-bucket/raw_commissions/sales_xxx.csv)
 
# ---------- SPARK ----------
spark = SparkSession.builder.appName("CommissionsETL").getOrCreate()
 
# ---------- 1. READ RAW CSV ----------
raw = spark.read.option("header","true").option("inferSchema","true").csv(INPUT_FILE)
 
sales = raw \
    .withColumn("sale_amount", col("sale_amount").cast("double")) \
    .withColumn("commission_rate", col("commission_rate").cast("double")) \
    .withColumn("sale_date", to_timestamp("sale_date"))
 
# ---------- 2. CURRENT AGENT DIM (SCD2) ----------
agent_dim = spark.read.format("bigquery") \
    .option("table", DIM_TABLE) \
    .load() \
    .filter("is_current = true") \
    .select("agent_id", "commission_rate")
 
# ---------- 3. ENRICH ----------
joined = sales.join(agent_dim, "agent_id", "left")
 
final = joined \
    .withColumn("final_rate", coalesce(agent_dim.commission_rate, sales.commission_rate)) \
    .withColumn("commission_amount", round(col("sale_amount") * col("final_rate"), 2)) \
    .withColumn("processed_date", current_date()) \
    .select(
        "agent_id","pos_id","product_type","sale_amount",
        "commission_amount","final_rate","sale_date","processed_date"
    )
 
# ---------- 4. WRITE TO BIGQUERY ----------
final.write \
    .format("bigquery") \
    .option("table", FACT_TABLE) \
    .mode("append") \
    .save()
 
print(f"Successfully processed {final.count()} rows → {FACT_TABLE}")
spark.stop()
