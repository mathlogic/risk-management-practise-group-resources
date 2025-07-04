#!/usr/bin/env python
# coding: utf-8
# %%

# %%


import warnings
warnings.filterwarnings('ignore')
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import regexp_replace
from datetime import datetime
import time
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col, concat_ws
import os


# %%


spark = SparkSession.builder \
    .config("spark.sql.debug.maxToStringFields", "1000") \
    .config("spark.executor.memory", "90g") \
    .config("spark.driver.memory", "90g") \
    .config("spark.sql.shuffle.partitions", "24") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")\
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# %%


#dummy function to see the counts (will delete once the code is finalize)
def show_basic(df, c= 'user_id'):
    print(f"No. of columns: {len(df.columns)}")
    print(f"No. of trades: {df.count()}")
    print(f"No. of users: {df.select(c).distinct().count()}")


# %%


base_dir = "/home/ubuntu/data/"
out_dir = "/home/ubuntu/data/data_sid/"
temp_dir = "/home/ubuntu/data/data_sid/temp_dir_inq/"
data_dir  = "/home/ubuntu/new_data/"


# %%


if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)


# __App data__

# %%


app= spark.read.parquet(base_dir + "alt_data/alt_data.parquet")\
.select("user_id","pl_opened_date")


# %%


show_basic(app)


# __Enquiry data__

# %%


df_inq0 = spark.read.parquet(data_dir + "retro_scrub/exp_enq000.parquet", header = True, inferSchema = True)
show_basic(df_inq0)


# %%


df_inq = df_inq0.join(app,'user_id', 'inner')\
.withColumn("amount", F.regexp_replace(F.col("amount"), ",", "").cast("float"))\
.withColumn('inq_date', F.to_date('inq_date', 'dd/MM/yyyy'))\
.withColumnRenamed('amount', 'inq_amt')\
.withColumnRenamed('inq_purp_cd', 'account_type')\
.withColumn("accno", F.col("user_id"))\
.withColumn("run_dt", F.col("pl_opened_date"))


# %%


show_basic(df_inq)


# %%


df_inq2 = df_inq\
.withColumn("inq_date", F.expr("case when inq_date <= to_date('1960-01-01', 'yyyy-MM-dd') or inq_date > run_dt then null else inq_date end"))\
.filter("inq_date is not null")


# %%


show_basic(df_inq2)


# %%


w_dedup = Window.partitionBy([ 'accno','run_dt', 'inq_date', 'account_type']).orderBy(F.desc('inq_amt'))
# 'credtrptid'


# %%


df_inq3 = df_inq2\
.withColumn('rn' , F.row_number().over(w_dedup))\
.filter("rn = 1")\
.drop("rn")


# %%


show_basic(df_inq3)


# %%


#removed the filter of day_diff <= 365
df_inq4 = df_inq3\
.withColumn("day_diff", F.datediff("run_dt","inq_date"))\
.filter("day_diff >0")\
.drop("day_diff")


# %%


show_basic(df_inq4)


# %%


inq_exp0= ["*"]\
+ ["case when account_type in \
    (2,3,4,5,9,10,14,16) \
then 1 else 0 end as SECURED"]\
+ ["case when account_type in \
    (7,12,15) \
then 1 else 0 end as Revolving"] \
+ ["case when account_type in (2) then 'AL' \
when account_type in (11,13) then 'PL' \
when account_type in (7) then 'CC' \
when account_type in \
    (1,3,12) \
then 'BL' \
when account_type in (6,18) then 'CD'\
when account_type in (8) then 'EL'\
when account_type in (5) then 'CE'\
when account_type in (14) then 'LAP' \
when account_type in (10) then 'LAS' \
when account_type in (16) then 'TW' \
when account_type in (4) then 'CV'\
when account_type in (9,15,99) then 'Other'\
else 'Other' end as PRODUCT_DESC"]


# %%


prods=['PL','BL','CC','CD']


# %%


inq_base_exp = ["*"]\
+ ["case when Inq_Month_Since_Inquiry <= 30 then 1 else 0 end as Inq_Num_inq_1m"]\
+ ["case when Inq_Month_Since_Inquiry <= 90 then 1 else 0 end as Inq_Num_inq_3m"]\
+ ["case when Inq_Month_Since_Inquiry <= 180 then 1 else 0 end as Inq_Num_inq_6m"]\
+ ["case when Inq_Month_Since_Inquiry <= 365 then 1 else 0 end as Inq_Num_inq_12m"]\
+ ["case when Inq_Month_Since_Inquiry <= 30 and PRODUCT_DESC='{prod}' then 1 else 0 end as {prod}_Num_inq_1m".format(prod=prod) for prod in prods]\
+ ["case when Inq_Month_Since_Inquiry <= 90 and PRODUCT_DESC='{prod}' then 1 else 0 end as {prod}_Num_inq_3m".format(prod=prod) for prod in prods]\
+ ["case when Inq_Month_Since_Inquiry <= 180 and PRODUCT_DESC='{prod}' then 1 else 0 end as {prod}_Num_inq_6m".format(prod=prod) for prod in prods]\
+ ["case when Inq_Month_Since_Inquiry <= 365 and PRODUCT_DESC='{prod}' then 1 else 0 end as {prod}_Num_inq_12m".format(prod=prod) for prod in prods]\
+ ["case when PRODUCT_DESC='{prod}' then Inq_Month_Since_Inquiry else null end as {prod}_inq".format(prod=prod) for prod in prods]\
+ ["case when secured = 1 then  Inq_Month_Since_Inquiry else null end as sec_inq"]\
+ ["case when secured = 0 then  Inq_Month_Since_Inquiry else null end as unsec_inq"]\
+ ["case when PRODUCT_DESC='{prod}' then 1 else 0 end as {prod}_inq_flag".format(prod=prod) for prod in prods]\
+ ["case when Inq_Month_Since_Inquiry <= 30 and SECURED=0 then 1 else 0 end as unsec_Inq_Num_inq_1m"]\
+ ["case when Inq_Month_Since_Inquiry <= 90 and SECURED=0 then 1 else 0 end as unsec_Inq_Num_inq_3m"]\
+ ["case when Inq_Month_Since_Inquiry <= 180 and SECURED=0 then 1 else 0 end as unsec_Inq_Num_inq_6m"]\
+ ["case when Inq_Month_Since_Inquiry <= 365 and SECURED=0 then 1 else 0 end as unsec_Inq_Num_inq_12m"]\
+ ["case when Inq_Month_Since_Inquiry <= 30 and SECURED=1 then 1 else 0 end as sec_Inq_Num_inq_1m"]\
+ ["case when Inq_Month_Since_Inquiry <= 90 and SECURED=1 then 1 else 0 end as sec_Inq_Num_inq_3m"]\
+ ["case when Inq_Month_Since_Inquiry <= 180 and SECURED=1 then 1 else 0 end as sec_Inq_Num_inq_6m"]\
+ ["case when Inq_Month_Since_Inquiry <= 365 and SECURED=1 then 1 else 0 end as sec_Inq_Num_inq_12m"]\
+ ["case when PRODUCT_DESC='{prod}' and Inq_Month_Since_Inquiry<={j} then num_days_btw_prod else null end as {prod}_num_days_btw_inq_{i}m".format(i=i,prod=prod, j=j) for prod in ['CC','PL'] for i,j in [(3,90),(6,180),(12,365)]]\
+ ["case when SECURED=0 and Inq_Month_Since_Inquiry<={j} then num_days_btw_sec else null end as unsec_num_days_btw_inq_{i}m".format(i=i,j = j) for i,j in [(3,90),(6,180),(12,365)]]\
+ ["case when Inq_Month_Since_Inquiry<={j} then num_days_btw_inq else null end as num_days_btw_inq_{i}m".format(i=i,j = j) for i,j in [(3,90),(6,180),(12,365)]]


# %%


w1 = Window.partitionBy('accno').orderBy('INQ_DATE',F.desc('INQ_Amt'),'PRODUCT_DESC')
w2 = Window.partitionBy('accno').orderBy(F.desc('INQ_DATE'),F.desc('INQ_Amt'),'PRODUCT_DESC')
w3 = Window.partitionBy('accno').orderBy('INQ_DATE').rowsBetween(-1,-1)
w4 = Window.partitionBy('accno','PRODUCT_DESC').orderBy('INQ_DATE').rowsBetween(-1,-1)
w5 = Window.partitionBy('accno','SECURED').orderBy('INQ_DATE').rowsBetween(-1,-1)


# %%


df_inq5 = df_inq4\
.withColumnRenamed("run_dt", "retro_dt")\
.withColumn("loan_month", F.trunc("retro_dt","month"))\
.withColumn("Inq_Month_Since_Inquiry",F.datediff(F.col("retro_dt"),F.col("INQ_DATE")))\
.selectExpr(inq_exp0)\
.withColumn('Inq_First_prod', F.first(F.col('PRODUCT_DESC')).over(w1))\
.withColumn('Inq_Latest_prod', F.first(F.col('PRODUCT_DESC')).over(w2))\
.withColumn('prev_inq_date_all',F.max('INQ_DATE').over(w3))\
.withColumn('prev_inq_date_prod',F.max('INQ_DATE').over(w4))\
.withColumn('prev_inq_date_sec',F.max('INQ_DATE').over(w5))\
.withColumn('num_days_btw_inq',F.datediff('INQ_DATE','prev_inq_date_all'))\
.withColumn('num_days_btw_prod',F.datediff('INQ_DATE','prev_inq_date_prod'))\
.withColumn('num_days_btw_sec',F.datediff('INQ_DATE','prev_inq_date_sec'))\
.selectExpr(inq_base_exp)


# %%


show_basic(df_inq5)


# %%


df_inq5.write.mode('overwrite').parquet(temp_dir + 'df_inq5.parquet')


# %%


df_inq5 = spark.read.parquet(temp_dir + 'df_inq5.parquet')


# %%


del df_inq4 
del df_inq3
del df_inq2


# %%


#calculating features values at CRN level by aggregating
inq_agg_exp = []\
+ [F.max(F.col('Inq_Month_Since_Inquiry')).alias('Inq_days_since_oldest_inq')]\
+ [F.min(F.col('Inq_Month_Since_Inquiry')).alias('Inq_days_since_latest_inq')]\
+ [F.max(F.col('sec_inq')).alias('Inq_days_since_oldest_secured_inq')]\
+ [F.min(F.col('sec_inq')).alias('Inq_days_since_latest_unsecured_inq')]\
+ [F.max(F.col('unsec_inq')).alias('Inq_days_since_oldest_unsecured_inq')]\
+ [F.min(F.col('unsec_inq')).alias('Inq_days_since_latest_secured_inq')]\
+ [F.sum(F.col('Inq_Num_inq_1m')).alias('Inq_Num_inq_1m')]\
+ [F.sum(F.col('Inq_Num_inq_3m')).alias('Inq_Num_inq_3m')]\
+ [F.sum(F.col('Inq_Num_inq_6m')).alias('Inq_Num_inq_6m')]\
+ [F.sum(F.col('Inq_Num_inq_12m')).alias('Inq_Num_inq_12m')]\
+ [F.sum(prod+"_Num_inq_1m").alias(prod+"_Num_inq_1m") for prod in prods]\
+ [F.sum(prod+"_Num_inq_3m").alias(prod+"_Num_inq_3m") for prod in prods]\
+ [F.sum(prod+"_Num_inq_6m").alias(prod+"_Num_inq_6m") for prod in prods]\
+ [F.sum(prod+"_Num_inq_12m").alias(prod+"_Num_inq_12m") for prod in prods]\
+ [F.first(F.col('Inq_Latest_prod')).alias('Inq_Latest_prod')]\
+ [F.countDistinct(F.col('account_type')).alias('Inq_distinct_products')]\
+ [F.min(prod+"_inq").alias(prod+"_inq_latest") for prod in prods]\
+ [F.max(prod+"_inq").alias(prod+"_inq_oldest") for prod in prods]\
+ [F.sum(F.col('unsec_Inq_Num_inq_1m')).alias('unsec_Inq_Num_inq_1m')]\
+ [F.sum(F.col('unsec_Inq_Num_inq_3m')).alias('unsec_Inq_Num_inq_3m')]\
+ [F.sum(F.col('unsec_Inq_Num_inq_6m')).alias('unsec_Inq_Num_inq_6m')]\
+ [F.sum(F.col('unsec_Inq_Num_inq_12m')).alias('unsec_Inq_Num_inq_12m')]\
+ [F.sum(F.col('sec_Inq_Num_inq_1m')).alias('sec_Inq_Num_inq_1m')]\
+ [F.sum(F.col('sec_Inq_Num_inq_3m')).alias('sec_Inq_Num_inq_3m')]\
+ [F.sum(F.col('sec_Inq_Num_inq_6m')).alias('sec_Inq_Num_inq_6m')]\
+ [F.sum(F.col('sec_Inq_Num_inq_12m')).alias('sec_Inq_Num_inq_12m')]\
+ [F.expr(f"round({agg_func}(num_days_btw_inq_{i}m),4) as {agg_func}_num_days_btw_inq_{i}m") for i in [3,6,12] for agg_func in ['avg','max','min','stddev']]\
+ [F.expr(f"round({agg_func}({prod}_num_days_btw_inq_{i}m),4) as {agg_func}_{prod}_num_days_btw_inq_{i}m") for prod in ['PL','CC','unsec'] for i in [3,6,12] for agg_func in ['avg','max','min','stddev']]


# %%


#expression for ratio variables
inq_ratio_exp = ["*"]\
+ ["case when Inq_Num_inq_6m is null then -1 when Inq_Num_inq_12m is null then -2  when Inq_Num_inq_12m = 0 then -3 else (Inq_Num_inq_6m/Inq_Num_inq_12m) end as Inq_Perc_Inquiry_6m"]\
+ ["case when {prod}_Num_inq_6m is null then -1 when {prod}_Num_inq_12m is null then -2  when {prod}_Num_inq_12m = 0 then -3 else ({prod}_Num_inq_6m/{prod}_Num_inq_12m) end as {prod}_Inq_Perc_Inquiry_6m"\
   .format(prod=prod) for prod in prods]


# %%


df_inq6=df_inq5.\
groupby('accno','loan_month')\
.agg(*inq_agg_exp).selectExpr(inq_ratio_exp)


# %%


show_basic(df_inq6,"accno")


# %%


df_inq6.write.mode('overwrite').parquet(out_dir + 'inq_features_2.7lac_users.parquet')


# %%


df_inq6 = spark.read.parquet(out_dir + 'inq_features_2.7lac_users.parquet')


# %%


del df_inq5


# %%





# %%




