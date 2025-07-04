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
import os
from pyspark.sql.functions import col, concat_ws
import pickle
import shutil

# %%


spark = SparkSession.builder \
    .config("spark.sql.debug.maxToStringFields", "1000") \
    .config("spark.executor.memory", "50g") \
    .config("spark.driver.memory", "50g") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")\
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")\
    .getOrCreate()


# %%


#dummy function to check counts (will be removed after finalizing)
def show_basic(df):
    c1 = 'user_id'
    print(f"No. of columns: {len(df.columns)}")
    print(f"No. of trades: {df.count()}")
    print(f"No. of users: {df.select('user_id').distinct().count()}")


# %%


base_dir = "/home/ubuntu/data/"
out_dir = "/home/ubuntu/data/data_sid/"
temp_dir = "/home/ubuntu/data/data_sid/temp_dir_tl_2105/"
data_dir  = "/home/ubuntu/data/"


# %%


if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)


# __Alt data__

# %%


app= spark.read.parquet(data_dir + "alt_data/alt_data.parquet")\
.select("user_id","pl_opened_date")


# %%


# show_basic(app)


# %%


# app= spark.read.parquet(base_dir + "alt_data/alt_data.parquet")\
# .select("user_id","pl_opened_date", "is_pl_indmoney")


# %%


# app.select(F.max('pl_opened_date'), F.min('pl_opened_date')).show()


# __Experian score data__

# %%


exp_score = spark.read.parquet(base_dir + "retro_scrub/summary000.parquet")
# show_basic(exp_score)


# %%


# exp_score.columns


# __Experian TL data__

# %%


df0 = spark.read.parquet(base_dir + "retro_scrub/exp_account000.parquet")


# %%


# show_basic(df0)


# __Merge bureau Score__

# %%


df0 = df0.join(exp_score.select("user_id", "score_v3", "scrub_date"), on = 'user_id', how = 'left')


# %%


# df0.select("user_id").distinct().count()


# __check the reporting of trades is unique__

# %%


# df0.groupBy(['user_id', 'acct_key']).count().groupBy('count').count().show()


# %%


#check is all present in retro scrub
df_dum = df0.select("user_id").distinct()
# app.select("user_id").join(df_dum,"user_id","inner").count()


# __Getting PL Opened date for the user and other date column operations__

# %%


df0 = df0.join(app.select("user_id", "pl_opened_date"), 'user_id','inner')\
.withColumn("bal_dt",F.to_date("balance_dt","dd/MM/yyyy"))\
.withColumn("op_dt",F.to_date("open_dt","dd/MM/yyyy"))\
.withColumn("cl_dt",F.to_date("closed_dt","dd/MM/yyyy"))


# %%


df1 = df0.filter("op_dt <= pl_opened_date")


# %%


# df1.select('user_id').distinct().count()


# %%


df1 = df1.withColumn("orig_loan_am",F.col('orig_loan_am').cast('int'))\
.withColumn("orig_loan_am_int",F.when(F.col("orig_loan_am") == -1, None)
            .when(F.col("orig_loan_am") <0,0)
            .otherwise(F.col("orig_loan_am")))\
.withColumn("credit_limit_am",F.col('credit_limit_am').cast('int'))\
.withColumn("credit_limit_am_int",F.when(F.col("credit_limit_am") == -1, None)
            .when(F.col("credit_limit_am") <0,0)
            .otherwise(F.col("credit_limit_am")))\
.withColumn("past_due_am",F.when(F.col("past_due_am") <0,0)
            .otherwise(F.col("past_due_am")))\
.withColumn("score_v3", F.col("score_v3").cast("int"))\
.withColumn("op_mn", F.trunc("op_dt", "month"))\
.withColumn("bal_mn", F.trunc("bal_dt", "month"))\
.withColumn("pl_mn", F.trunc("pl_opened_date", "month"))\
.withColumn("cl_mn", F.trunc("cl_dt", "month"))\
.withColumn("mn_rpt", F.months_between("pl_mn", "bal_mn").cast("int"))\
.withColumn("mn_opn", F.months_between("pl_mn", "op_mn").cast("int"))\
.withColumn("mn_cls", F.months_between( "pl_mn", "cl_mn").cast("int"))\
.filter(" pl_opened_date > bal_dt" )
#mn_rpt flag removed for now
# .filter("mn_rpt > 0 and mn_rpt < 13")


# %%


# show_basic(df1)


# %%


df1.repartition(100).write.mode('overwrite').parquet(temp_dir + 'df1.parquet')


# %%


df1 = spark.read.parquet(temp_dir + 'df1.parquet')


# %%


# show_basic(df1)


# %%


del df0


# %%


expr1 = ["*"] \
+ ["1 as Trade"]\
+ ["case when acct_type_cd in \
    (47, 58, 195, 185, 191, 219, 173, 184, 172, 248, \
    168, 246, 176, 175, 220, 221, 223, 240, 241, 243)\
    then 1 else 0 end as secured"]\
+ ["case when acct_type_cd in \
    (5, 121, 181, 214, 197, 198, 199, 200, 220, 213, 224, 225, 226, 244 ) then 1 else 0 end as revolving"]\
+ ["case when acct_type_cd in (47, 221, 223) then 'AL'\
    when acct_type_cd in (248, 249, 167, 175,176, 177, 178, 179, 197, 198, 199, 200, 227, 228, 241) then 'BL'\
    when acct_type_cd in (5, 214, 220, 213, 224) then 'CC'\
    when acct_type_cd in (189) then 'CD'\
    when acct_type_cd in (222) then 'CE'\
    when acct_type_cd in (172) then 'CV'\
    when acct_type_cd in (130) then 'EL'\
    when acct_type_cd in (191, 243) then 'GL'\
    when acct_type_cd in (58, 168, 240) then 'HL'\
    when acct_type_cd in (195) then 'LAP'\
    when acct_type_cd in (185) then 'LAS'\
    when acct_type_cd in (121, 226, 244) then 'OD'\
    when acct_type_cd in (123, 187, 169, 170, 245, 246, 247, 196, 225, 242) then 'PL'\
    when acct_type_cd in (173) then 'TW'\
    when acct_type_cd in (219, 181, 217, 215, 216, 999, 184) then 'Other'\
    ELSE 'Other' END AS product_desc"]\
+ ["case when product_desc = 'CC' then credit_limit_am_int else null end as creditlimit"]\
+ ["orig_loan_am_int as sanctionamount"]\
+ ["case when (revolving=1 and mn_cls is not null and mn_cls > 0) then 0 \
         when (revolving=0 and mn_cls is not null and mn_cls > 0) then 0 \
         when (revolving=0 and mn_cls is null and BALANCE_AM <= 0) then 0 else 1 end as open"]\
+ ["case when open = 1 then pl_mn else bal_mn end as last_mn"]


# %%


expr2 = ["*"] + [
    "case when PAYMENT_RATING_CD_{x} in ('?', '-1', 'U') then -1 "
    "when ((PAYMENT_RATING_CD_{x} ='0') or (PAYMENT_RATING_CD_{x} ='S')) then 0 "
    "when PAYMENT_RATING_CD_{x} = '1' then 2 "
    "when PAYMENT_RATING_CD_{x} = '2' then 3 "
    "when ((PAYMENT_RATING_CD_{x} in ('3','4')) OR (PAYMENT_RATING_CD_{x} in ('B','D','L','M'))) then 4 "
    "when PAYMENT_RATING_CD_{x} = '5' then 5 "
    "when PAYMENT_RATING_CD_{x} = '6' then 6 "
    "else -1 end as dpd_t_bkt_{x}".format(x=str(i).zfill(2))
    for i in range(1, 37)]


# %%


expr_bal_col_rename = ["*"]\
+ [f"case when balance_am_{str(i).zfill(2)} < 0 then 0 else balance_am_{str(i).zfill(2)} end as balance_am1_{str(i).zfill(2)}" for i in range(1, 25)]\
+ [f"case when past_due_am_{str(i).zfill(2)} < 0 then 0 else past_due_am_{str(i).zfill(2)} end as past_due_am1_{str(i).zfill(2)}" for i in range(1, 37)]


# %%


df2 = df1.selectExpr(expr_bal_col_rename)\
.selectExpr(expr1)\
.selectExpr(expr2)


# %%


dpd_cols_all = [f"dpd_t_bkt_{str(i).zfill(2)}" for i in range(1,37)]
act_cols_all = [f"actual_payment_am_{str(i).zfill(2)}" for i in range(1,25)]
bal_cols_all = [f"balance_am1_{str(i).zfill(2)}" for i in range(1,25)]
cl_cols_all = [f"credit_limit_am_{str(i).zfill(2)}" for i in range(1,25)]
rat_cols_all = [f"payment_rating_cd_{str(i).zfill(2)}" for i in range(1,25)]
due_cols_all = [f"past_due_am1_{str(i).zfill(2)}" for i in range(1,37)]


# %%


df2.repartition(100).write.mode('overwrite').parquet(temp_dir + 'df2.parquet')


# %%


df2 = spark.read.parquet(temp_dir + 'df2.parquet')


# %%


# show_basic(df2)


# %%


del df1


# %%


df3 = df2\
.withColumn("lp_dt",F.to_date("last_payment_dt","dd/MM/yyyy"))\
.withColumn("dflt_dt",F.to_date("dflt_status_dt","dd/MM/yyyy"))\
.withColumn("wo_dt",F.to_date("write_off_status_dt","dd/MM/yyyy"))\
.withColumn("chargeoff_am_int", F.col("charge_off_am").cast("float"))\
.withColumn("retro_date", F.col('pl_opened_date'))\
.withColumn("vintage_days", F.datediff(F.col('retro_date'), F.col('op_dt')))\
.withColumn("vintage", F.months_between(F.col('retro_date'), F.col('op_dt')))\
.withColumn("days_since_closed", F.datediff(F.col('retro_date'), F.col('cl_dt')))\
.withColumn("days_since_phist", F.datediff(F.col('retro_date'), F.col('bal_dt')))\
.withColumn("days_since_rpt", F.datediff(F.col('retro_date'), F.col('bal_dt')))\
.withColumn("mn",F.months_between('pl_mn','bal_mn'))\
.withColumn("currentbal_rpt", F.col("BALANCE_AM").cast("float"))\
.withColumn("currentbal_rpt", F.expr("case when currentbal_rpt < 0 then 0 else currentbal_rpt end "))\
.withColumn("dpd_array", F.array([F.col(c).cast('int')  for c in dpd_cols_all]))\
.withColumn("bal_array", F.array([F.col(c).cast('float')  for c in bal_cols_all]))\
.withColumn("act_am_array", F.array([F.col(c).cast('float')  for c in act_cols_all]))\
.withColumn("cl_array", F.array([F.col(c).cast('float')  for c in cl_cols_all]))\
.withColumn("due_array", F.array([F.col(c).cast('float')  for c in due_cols_all]))\
.withColumn("dpd_final", F.concat(F.array_repeat(F.lit(-2), F.col('mn').cast('int')-1), F.col("dpd_array")))\
.withColumn("bal_final", F.concat(F.array_repeat(F.lit(None), F.col('mn').cast('int')-1), F.col("bal_array")))\
.withColumn("act_am_final", F.concat(F.array_repeat(F.lit(None), F.col('mn').cast('int')-1), F.col("act_am_array")))\
.withColumn("cl_final", F.concat(F.array_repeat(F.lit(None), F.col('mn').cast('int')-1), F.col("cl_array")))\
.withColumn("due_final", F.concat(F.array_repeat(F.lit(None), F.col('mn').cast('int')-1), F.col("due_array")))\
.withColumn("overdueamt_rpt", F.col("PAST_DUE_AM").cast("float"))\
.withColumn("max_index", F.lit(36).cast(IntegerType()))\
.withColumn("full_sequence", F.sequence(F.lit(1), F.col("max_index")))\
.withColumn("cl_mn_new", F.coalesce(F.col('cl_mn'), F.col('last_mn')))\
.withColumn("status_array",F.expr("""
transform(full_sequence, pos -> 
                    CASE WHEN add_months(pl_mn,  -1*pos) BETWEEN op_mn AND cl_mn_new-1 THEN 1
                    ELSE 0
                END
            )
        """))\
.withColumn("loan_month", F.col("pl_mn"))\
.drop("tenure")


# %%


# show_basic(df3)


# %%


df3.repartition(100).write.mode('overwrite').parquet(temp_dir + 'df3.parquet')


# %%


df3 = spark.read.parquet(temp_dir + 'df3.parquet')

# %%


del df2


# %%


#### Array Structure ####
# so actual_payment_Am_01/actual_payment_Am is the month of balance date 
#actual payment amount of any month should be equal balance of the previous month for ideal customer 
#act_am_final array starts from [pl_mn -1, pl_mn-2, ....... ]
# if the loan is opened on 2024-10-17 then for 10th month of 2024 its considered open cuz we taking trucated date 
# if its closed on 2025-02-13 then for 2025 feb its considered as closed 
# for the status array [pl_mn-1, pl_mn-2,...........]   if its 1 then its open 


# %%


expr3 = ["*"]\
+ ["dpd_final[{}] as dpd_bkt_{}".format(i-1,i) for i in range(1,25)]\
+ ["bal_final[{}] as curbal_{}".format(i-1,i) for i in range(1,37)] \
+ ["act_am_final[{}] as act_am_{}".format(i-1,i) for i in range(1,25)] \
+ ["due_final[{}] as amtoverdue_{}".format(i-1,i) for i in range(1,37)] \
+ ["status_array[{}] as is_open_m{}".format(i-1,i) for i in range(1,37)]


# %%


expr4 = ["*"]\
+ [" + ".join('curbal_' + str(i) for i in range(1,4)) + " as total_bal_3m"]\
+ [" + ".join('curbal_' + str(i) for i in range(1,7)) + " as total_bal_6m"]\
+ [" + ".join('curbal_' + str(i) for i in range(1,10)) + " as total_bal_9m"]\
+ [" + ".join('curbal_' + str(i) for i in range(1,13)) + " as total_bal_12m"]\
+ [" + ".join('amtoverdue_'+ str(i) for i in range(1,4)) + " as total_amtoverdue_3m"]\
+ [" + ".join('amtoverdue_'+ str(i) for i in range(1,7)) + " as total_amtoverdue_6m"]\
+ [" + ".join('amtoverdue_'+ str(i) for i in range(1,10)) + " as total_amtoverdue_9m"]\
+ [" + ".join('amtoverdue_'+ str(i) for i in range(1,13)) + " as total_amtoverdue_12m"]\
+ ["greatest(" + ", ".join('act_am_' +  str(i) for i in range(1, 13)) + ") as highcredit"]


# %%


#This is equal to crif expr7
expr5 = ["*"]\
+["case when open = 1 then past_due_am else 0 end as open_overdue"]\
+["case when product_desc='CC' then past_due_am else 0 end as cc_overdue"]\
+["case when product_desc='PL' then past_due_am else 0 end as pl_overdue"]\
+["case when product_desc!='CC' then past_due_am else 0 end as non_cc_overdue"]\
+["case when mn_rpt<=3 then past_due_am else 0 end as overdue_rpt_3mn"]\
+["case when mn_rpt<=6 then past_due_am else 0 end as overdue_rpt_6mn"]\
+ ["case when product_desc != 'CC' then sanctionamount else creditlimit end as sanctionamt_der"]


# %%


df4 = df3.selectExpr(expr3)\
.selectExpr(expr4)\
.selectExpr(expr5)


# %%


# show_basic(df4)


# %%


df4.repartition(100).write.mode('overwrite').parquet(temp_dir + 'df4.parquet')


# %%


df4 = spark.read.parquet(temp_dir + 'df4.parquet')


# %%


del df3


# %%


w1 = Window.partitionBy('user_id').orderBy(F.desc('mn_opn'),F.desc('Sanctionamt_der'), F.asc('product_desc'))
w2 = Window.partitionBy('user_id').orderBy(F.asc('mn_opn'),F.desc('Sanctionamt_der'), F.asc('product_desc'))
w3 = Window.partitionBy('user_id')


# %%


df5  = df4.withColumn("first_product", F.first(F.col('product_desc')).over(w1))\
.withColumn("latest_product", F.first(F.col('product_desc')).over(w2))


# %%


prods = ['PL','CC','CD','HL','LAP','BL']
inst_type =  ["PVT","HFC","NBF","PUB","FOR"]


# %%


#EMI cis there in our data still we made  some 
expr6 = ["*"]\
+ ["mn_opn as tenure"]\
+ ["case when days_since_rpt > 365 then 1 else 0 end as hanging_trade"]\
+ ["case when PRODUCT_DESC = '{i}' and days_since_rpt > 365 then 1 else 0 end as hanging_{i}_trade".format(i = i) for i in prods]\
+ ["case when days_since_rpt <= 365 then 1 else 0 end as rcnt_trd_flag"]\
+ ["case when Revolving = 0 then 1 else 0 end as installment"]\
+ ["case when secured = 0 then 1 else 0 end as unsecured"]\
+ ["case when PRODUCT_DESC = 'BL' and currentbal_rpt > 0 and sanctionamount > 0 and open = 1 then 0.03*sanctionamount \
         when PRODUCT_DESC = 'CC' and currentbal_rpt > 0 and creditlimit > 0 and open = 1 then 0.05*currentbal_rpt \
         when PRODUCT_DESC = 'PL' and currentbal_rpt > 0 and sanctionamount > 0 and open = 1 then 0.03*sanctionamount \
         when PRODUCT_DESC = 'HL' and currentbal_rpt > 0 and sanctionamount > 0 and open = 1 then 0.01*sanctionamount \
         when PRODUCT_DESC = 'AL' and currentbal_rpt > 0 and sanctionamount > 0 and open = 1 then 0.03*sanctionamount \
         when PRODUCT_DESC not in ('BL','CC','PL','HL','AL') and currentbal_rpt > 0 and sanctionamount > 0 and open = 1 then 0.03*sanctionamount else 0 end as EMI"]\
+[f"case when (cl_dt is not null and days_since_closed > {j}) or ((currentbal_rpt=0 and REVOLVING=0) and (days_since_phist>{j})) then 0 else 1 end as is_live_q{int(i/3)}".format(i=i,j=j) for i,j in [(3,90),(6,180)]]\
+[f"case when ((cl_dt is not null and days_since_closed > {j}) or ((currentbal_rpt=0 and REVOLVING=0) and (days_since_phist>{j}))) and PRODUCT_DESC='{prod}' \
then 0 else 1 end as {prod}_is_live_q{int(i/3)}".format(i=i, prod = prod, j=j) for i,j in [(3,90),(6,180)] for prod in ['CC', 'PL']]\
+["case when PRODUCT_DESC!='HL' and rcnt_trd_flag=1 then coalesce(CREDITLIMIT,greatest(SANCTIONAMOUNT,HIGHCREDIT)) else 0 end as non_mortage_sanc_amt"]\
+ [f"case when PRODUCT_DESC = 'CC' and open = 1 and M_SUB_ID = '{ty}' then 1 else 0 end  as open_cc_given_{ty}" for ty in inst_type]\
+ [f"case when PRODUCT_DESC = 'PL' and open = 1 and M_SUB_ID = '{ty}' then 1 else 0 end  as open_pl_given_{ty}" for ty in inst_type]\
+ [f"case when PRODUCT_DESC = 'CC' and open = 0 and M_SUB_ID = '{ty}' then 1 else 0 end  as closed_cc_given_{ty}" for ty in inst_type]\
+ [f"case when PRODUCT_DESC = 'PL' and open = 0 and M_SUB_ID = '{ty}' then 1 else 0 end  as closed_pl_given_{ty}" for ty in inst_type]\
+["case when PRODUCT_DESC = 'PL' and open = 1 and secured = 1 then 1 else 0 end  as open_pl_secured"]\
+["case when PRODUCT_DESC = 'PL' and open = 1 and unsecured = 1 then 1 else 0 end  as open_pl_unsecured"]\
+["case when PRODUCT_DESC = 'PL' and open = 0 and secured = 1 then 1 else 0 end  as close_pl_secured"]\
+["case when PRODUCT_DESC = 'PL' and open = 0 and unsecured = 1 then 1 else 0 end  as close_pl_unsecured"]


# %%


df6 = df5.selectExpr(expr6)


# %%


df6.repartition(100).write.mode('overwrite').parquet(temp_dir + 'df6.parquet')


# %%


df6 = spark.read.parquet(temp_dir + 'df6.parquet')


# %%


del df4
del df5


# %%


# show_basic(df6)


# %%


w_reopen = Window.partitionBy('user_id','PRODUCT_DESC').orderBy('op_dt','bal_dt','balance_am','cl_dt')
df7 = df6.withColumn('prev_closed_date',F.lag('cl_dt').over(w_reopen))\
.withColumn("reopened_loan",F.expr("CASE WHEN prev_closed_date IS NOT NULL AND op_dt > prev_closed_date THEN 1 ELSE 0 END"))


# %%


expr_reopen = ["*"]\
+["CASE WHEN reopened_loan = 1 AND months_between(op_dt, prev_closed_date) < {i} THEN 1 ELSE 0 END AS reopen_{i}m".format(i=i)
    for i in ['1','3','6','12']]\
+["CASE WHEN PRODUCT_DESC = '{prod}' and reopened_loan = 1 AND months_between(op_dt, prev_closed_date) < {i} THEN 1 ELSE 0 END AS reopen_{prod}_{i}m".format(i=i, prod=prod)
    for i in ['1','3','6','12'] for prod in prods]


# %%


expr8 = ["*"]\
+["case when PRODUCT_DESC = '{i}' then sanctionamount else null end as {i}_sanc_amt".format(i = prod) for prod in [ 'AL', 'HL']]\
+ ["case when PRODUCT_DESC = '{prod}' then overdueamt_rpt else 0 end as {prod}_cur_overdue".format(prod = prod) for prod in prods]\
+ ["case when days_since_rpt <= {j} then overdueamt_rpt else 0 end as cur_overdue_rpt_{i}m".format(i = i, j = j) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')]]\
+ ["case when PRODUCT_DESC = '{prod}' then total_amtoverdue_{i}m/{i} else null end as avg_{prod}_overdue_rpt_{i}m".format(i = i, prod = prod) for i in ['3','6','9','12'] for prod in prods]\
+ ["case when days_since_rpt <= {j} then total_amtoverdue_{i}m/{i} else null end as avg_overdue_rpt_{i}m".format(i = i, j = j) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')]]\
+ ["case when PRODUCT_DESC = '{prod}' and days_since_rpt <= 365 then currentbal_rpt else 0 end as {prod}_cur_bal".format(prod = prod) for prod in prods]\
+ ["case when PRODUCT_DESC = '{prod}' then total_bal_{i}m/{i} else null end as avg_{prod}_bal_rpt_{i}m".format(i = i, prod = prod) for i in ['3','6','9','12'] for prod in prods]\
+ ["case when days_since_rpt <= {j} then total_bal_{i}m/{i} else null end as avg_bal_rpt_{i}m".format(i = i, j = j) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')]]\
+ ["case when PRODUCT_DESC = 'CC' and days_since_rpt <= 365 then creditlimit else null end as cc_cr_lim"]\
+ ["case when PRODUCT_DESC = '{prod}' and days_since_rpt <= 365 then vintage_days else null end as vintage_days_{prod}".format(prod = prod) for prod in ['CC','PL','HL']]\
+ ["case when vintage_days <= {j} then 1 else null end as num_trades_open_last{i}m".format(i = i, j = j) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')]]\
+ ["case when vintage_days <= {j} and PRODUCT_DESC = '{prod}' then 1 else null end as {prod}_num_open_last{i}m".format(i = i, j = j, prod = prod) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')] for prod in prods]\
+ ["case when vintage_days <= {j} and PRODUCT_DESC = 'CC' and creditlimit <= {amt} then 1 else null end as CC_num_open_last{i}m_{amt1}".format(i = i, j = j, amt = k, amt1 = l) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')] for k,l in [('50000','50k'),('100000','1L')]]\
+ ["case when open = 1 and secured = 1 then 1 else 0 end as sec_ind"]\
+ ["case when open = 1 and secured = 0 then 1 else 0 end as unsec_ind"]\
+ ["case when open = 1 then currentbal_rpt else 0 end as cur_bal_live_trades"]\
+ ["case when open = 1 then total_bal_{i}m/{i} else null end as avg_bal_live_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when open = 1 then EMI else 0 end as emi_live_trades"]\
+ ["case when open = 1 and PRODUCT_DESC = '{prod}' then currentbal_rpt else null end as live_{prod}_trades_cur_bal".format(prod = prod) for prod in prods]\
+ ["case when open = 1 and PRODUCT_DESC = '{prod}' then EMI else null end as {prod}_emi".format(prod = prod) for prod in prods]\
+ ["case when PRODUCT_DESC = '{prod}' then tenure else null end as {prod}_tenure".format(prod = prod) for prod in prods]\
+ ["case when open = 1 and PRODUCT_DESC = '{prod}' and vintage_days <= {j} then currentbal_rpt else null end as live_{prod}_trades_cur_bal_open_last{i}m".format(i = i, j = j, prod = prod) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')] for prod in prods]\
+ ["case when installment = 1 and open = 1 then 1 else 0 end as live_install_trades"]\
+ ["case when installment = 1 then currentbal_rpt else 0 end as cur_bal_install_trades"]\
+ ["case when installment = 1 then total_bal_{i}m/{i} else null end as avg_bal_install_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when installment = 1 and open = 1 then currentbal_rpt else 0 end as cur_bal_live_install_trades"]\
+ ["case when installment = 1 and open = 1 then total_bal_{i}m/{i} else null end as avg_bal_live_install_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when installment = 1 then EMI else 0 end as emi_install_trades"]\
+ ["case when installment = 1 and open = 1 then EMI else 0 end as emi_live_install_trades"]\
+ ["case when installment = 1 then Sanctionamt_der else 0 end as loan_amt_install_trades"]\
+ ["case when installment = 1 and open = 1 then Sanctionamt_der else 0 end as loan_amt_live_install_trades"]\
+ ["case when revolving = 1 and open = 1 then 1 else 0 end as live_revolving_trades"]\
+ ["case when revolving = 1 then currentbal_rpt else 0 end as cur_bal_revolving_trades"]\
+ ["case when revolving = 1 then total_bal_{i}m/{i} else null end as avg_bal_revolving_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when revolving = 1 and open = 1 then currentbal_rpt else 0 end as cur_bal_live_revolving_trades"]\
+ ["case when revolving = 1 and open = 1 then total_bal_{i}m/{i} else null end as avg_bal_live_revolving_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when revolving = 1 then EMI else 0 end as emi_revolving_trades"]\
+ ["case when revolving = 1 and open = 1 then EMI else 0 end as emi_live_revolving_trades"]\
+ ["case when revolving = 1 then coalesce(creditlimit, sanctionamount) else 0 end as sanc_amt_revolving_trades"]\
+ ["case when revolving = 1 and open = 1 then coalesce(creditlimit, sanctionamount) else 0 end as sanc_amt_live_revolving_trades"]\
+ ["case when secured = 1 then EMI else 0 end as emi_secured_trades"]\
+ ["case when secured = 1 and open = 1 then EMI else 0 end as emi_live_secured_trades"]\
+ ["case when secured = 1 then currentbal_rpt else 0 end as cur_bal_secured_trades"]\
+ ["case when secured = 1 then total_bal_{i}m/{i} else null end as avg_bal_secured_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when secured = 1 and open = 1 then currentbal_rpt else 0 end as cur_bal_live_secured_trades"]\
+ ["case when secured = 1 and open = 1 then total_bal_{i}m/{i} else null end as avg_bal_live_secured_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when secured = 0 then EMI else 0 end as emi_unsecured_trades"]\
+ ["case when secured = 0 and open = 1 then EMI else 0 end as emi_live_unsecured_trades"]\
+ ["case when secured = 0 then currentbal_rpt else 0 end as cur_bal_unsecured_trades"]\
+ ["case when secured = 0 then total_bal_{i}m/{i} else null end as avg_bal_unsecured_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when secured = 0 and open = 1 then currentbal_rpt else 0 end as cur_bal_live_unsecured_trades"]\
+ ["case when secured = 0 and open = 1 then total_bal_{i}m/{i} else null end as avg_bal_live_unsecured_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when revolving = 1 then tenure else null end as tenure_all_revol"]\
+ ["case when installment = 1 then tenure else null end as tenure_all_install"]\
+["case when unsecured=1 then sanctionamount end as unsecured_sanamt"]\
+["case when secured=1 then sanctionamount end as secured_sanamt"]\
+ ["case when " + " or ".join(["dpd_t_bkt_" + (str(i).zfill(2)) + "=0" for i in range(1,13)]) + " then 1 else 0 end as dpd0"]\
+ ["case when " + " or ".join(["dpd_t_bkt_" + str(i).zfill(2) + ">=0" for i in range(1,13)]) + " then 1 else 0 end as dpdvalid"]\
+ ["case when " + " or ".join(["dpd_t_bkt_" + str(i).zfill(2) + ">=1" for i in range(1,13)]) + " then 1 else 0 end as dpdx"]\
+ ["case when " + " or ".join(["dpd_t_bkt_" + str(i).zfill(2) + ">=2" for i in range(1,13)]) + " then 1 else 0 end as dpd30"]\
+ ["case when " + " or ".join(["dpd_t_bkt_" + str(i).zfill(2) + ">=3" for i in range(1,13)]) + " then 1 else 0 end as dpd60"]\
+ ["case when " + " or ".join(["dpd_t_bkt_" + str(i).zfill(2) + ">=4" for i in range(1,13)]) + " then 1 else 0 end as dpd90"]\
+ ["case when " + " or ".join(["dpd_t_bkt_" + str(i).zfill(2) + ">=5" for i in range(1,13)]) + " then 1 else 0 end as dpd150"]\
+ ["case when " + " or ".join(["dpd_t_bkt_" + str(i).zfill(2) + ">=6" for i in range(1,13)]) + " then 1 else 0 end as dpd180"]\
+ ["case when " + " or ".join(["dpd_t_bkt_" + str(i).zfill(2) + ">=7" for i in range(1,13)]) + " then 1 else 0 end as dpd360"]\
+ [" + ".join(["int(dpd_t_bkt_" + str(i).zfill(2) + " ==0)" for i in range(1,13)]) + " as dpd0_counter_last12m"]\
+ [" + ".join(["int(dpd_t_bkt_" + str(i).zfill(2) + " >=0)" for i in range(1,13)]) + " as dpdvalid_counter_last12m"]\
+ [" + ".join(["int(dpd_t_bkt_" + str(i).zfill(2) + " >=1)" for i in range(1,13)]) + " as dpdx_counter_last12m"]\
+ [" + ".join(["int(dpd_t_bkt_" + str(i).zfill(2) + " >=2)" for i in range(1,13)]) + " as dpd30_counter_last12m"]\
+ [" + ".join(["int(dpd_t_bkt_" + str(i).zfill(2) + " >=3)" for i in range(1,13)]) + " as dpd60_counter_last12m"]\
+ [" + ".join(["int(dpd_t_bkt_" + str(i).zfill(2) + " >=4)" for i in range(1,13)]) + " as dpd90_counter_last12m"]\
+ [" + ".join(["int(dpd_t_bkt_" + str(i).zfill(2) + " >=3)" for i in range(1,7)]) + " as dpd60_counter_last6m"]\
+ [" + ".join(["int(dpd_t_bkt_" + str(i).zfill(2) + " >=4)" for i in range(1,7)]) + " as dpd90_counter_last6m"]\
+ ["case " + " ".join(["when dpd_t_bkt_" + str(i).zfill(2) + ">0 then dpd_t_bkt_" + str(i).zfill(2) for i in range(1,13)]) + " else null end as min_dpd_month"]\
+ ["case " + " ".join(["when dpd_t_bkt_" + str(i).zfill(2) + ">0 then dpd_t_bkt_" + str(i).zfill(2) for i in range(12,0,-1)]) + " else null end as max_dpd_month"]\
+ ["case " + " ".join(["when dpd_t_bkt_" + str(i).zfill(2) + ">=0 then dpd_t_bkt_" + str(i).zfill(2) for i in range(1,13)]) + " else null end as min_valid_dpd_month"]\
+ ["case " + " ".join(["when dpd_t_bkt_" + str(i).zfill(2) + ">1 then dpd_t_bkt_" + str(i).zfill(2) for i in range(1,13)]) + " else null end as min_dpd_month_30plus"]\
+ ["case " + " ".join(["when dpd_t_bkt_" + str(i).zfill(2) + ">2 then dpd_t_bkt_" + str(i).zfill(2) for i in range(1,13)]) + " else null end as min_dpd_month_60plus"]\
+ ["case " + " ".join(["when dpd_t_bkt_" + str(i).zfill(2) + ">3 then dpd_t_bkt_" + str(i).zfill(2) for i in range(1,13)]) + " else null end as min_dpd_month_90plus"]\
+ ["greatest(" + ", ".join(["dpd_t_bkt_" + str(i).zfill(2) for i in range(1,4)]) + ") as max_dpd_3m"]\
+ ["greatest(" + ", ".join(["dpd_t_bkt_" + str(i).zfill(2) for i in range(1,7)]) + ") as max_dpd_6m"]\
+ ["greatest(" + ", ".join(["dpd_t_bkt_" + str(i).zfill(2) for i in range(1,13)]) + ") as max_dpd_12m"]\
+ ["case when secured = 0 then highcredit else 0 end as  highcr_unsecured_trades"]\
+ ["case when secured = 0 and open = 1 then highcredit else 0 end as  highcr_live_unsecured_trades"]\
+ ["case when secured = 1 then highcredit else 0 end as  highcr_secured_trades"]\
+ ["case when secured = 1 and open = 1 then highcredit else 0 end as  highcr_live_secured_trades"]\
+ ["case when vintage_days <= {j} and PRODUCT_DESC = '{prod}' and greatest(sanctionamount,highcredit) <= {amt} then 1 else null end as {prod}_num_open_last{i}m_{amt1}".format(i = i, j = j, prod = prod, amt = k, amt1 = l) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')] for prod in [i for i in prods if 'CC' not in i] for k,l in [('50000','50k'),('100000','1L'),('500000','5L')]]\
+ ["case when PRODUCT_DESC = '{prod}' and days_since_rpt <= 365 then greatest(sanctionamount,highcredit) else null end as {prod}_highcr".format(prod = prod) for prod in ['PL','HL']]\
+ ["case when PRODUCT_DESC = 'CC' then greatest(" + ", ".join(["dpd_t_bkt_" + str(i).zfill(2) for i in range(1,13)]) + ") else null end as cc_max_dpd_ever"]\
+["case when min_dpd_month_{i}plus is not null and min_dpd_month_{i}plus=min_valid_dpd_month then currentbal_rpt else null end as {i}plus_dlnq_bal".format(i = i) for i in ['30','60','90']]\
+["case when open=1 and min_dpd_month_{i}plus is not null and min_dpd_month_{i}plus=min_valid_dpd_month and PRODUCT_DESC='{prod}' then currentbal_rpt else null end as {prod}_live_{i}plus_dlnq_bal".format(prod =prod, i=i) for prod in ['CC','PL'] for i in ['30','60','90']]\
+["case when min_dpd_month_{i}plus is not null and min_dpd_month_{i}plus=min_valid_dpd_month and vintage_days<={k} then 1 else 0 end as {i}plus_dlnq_opn_last_{j}mn_flag".format(i = i, j = j, k=k) for i in ['30','60','90'] for j,k in [('3',90),('6',180),('9',270),('12',365)]]\
+["case when min_dpd_month_{i}plus is not null and min_dpd_month_{i}plus=min_valid_dpd_month and vintage_days<={k} and PRODUCT_DESC='{prod}' then 1 else 0 end as {prod}_{i}plus_dlnq_opn_last_{j}mn_flag".format(i = i, j = j, prod = prod, k=k) for i in ['30','60','90'] for j,k in [('3',90),('6',180),('9',270),('12',365)] for prod in ['CC','PL']]


# %%


df8  = df7.selectExpr(expr_reopen).selectExpr(expr8)


# %%


# show_basic(df8)


# %%


df8.repartition(100).write.mode('overwrite').option("compression", "snappy").parquet(temp_dir + 'df8.parquet.snappy')


# %%


df8 = spark.read.parquet(temp_dir + 'df8.parquet.snappy')


# %%


del df7
del df6


# %%


w1 = Window.partitionBy('user_id','RETRO_DATE').orderBy(F.desc('rcnt_trd_flag'),F.asc('op_dt'),F.desc('Sanctionamt_der'), F.asc('PRODUCT_DESC'))
w2 = Window.partitionBy('user_id','RETRO_DATE').orderBy(F.desc('rcnt_trd_flag'),F.desc('op_dt'),F.desc('Sanctionamt_der'), F.asc('PRODUCT_DESC'))
w3 = Window.partitionBy('user_id','RETRO_DATE')


# %%


df9 = df8\
.withColumn('max_hl_highcr', F.max('hl_highcr').over(w3))\
.withColumn('max_hl_cur_bal', F.max('hl_cur_bal').over(w3))\
.withColumn('oldest_hl', F.max('vintage_days_hl').over(w3))\
.withColumn('latest_hl', F.min('vintage_days_hl').over(w3))\
.withColumn('max_pl_highcr', F.max('pl_highcr').over(w3))\
.withColumn('max_pl_cur_bal', F.max('pl_cur_bal').over(w3))\
.withColumn('oldest_pl', F.max('vintage_days_pl').over(w3))\
.withColumn('latest_pl', F.min('vintage_days_pl').over(w3))\
.withColumn('max_cc_cr_lim', F.max('cc_cr_lim').over(w3))\
.withColumn('max_cc_cur_bal', F.max('cc_cur_bal').over(w3))\
.withColumn('oldest_cc', F.max('vintage_days_cc').over(w3))\
.withColumn('latest_cc', F.min('vintage_days_cc').over(w3))\
.withColumn('maximum_dlnq_m1', F.max('dpd_t_bkt_02').over(w3))


# %%


expr9 = ["*"]\
+ ["case when max_{prod}_highcr is not null and {prod}_highcr = max_{prod}_highcr then {prod}_highcr else null end as max_highcr_{prod}_highcr".format(prod = prod) for prod in ['PL','HL']]\
+ ["case when max_{prod}_highcr is not null and {prod}_highcr = max_{prod}_highcr then {prod}_cur_bal else null end as max_highcr_{prod}_cur_bal".format(prod = prod) for prod in ['PL','HL']]\
+ ["case when max_cc_cr_lim is not null and cc_cr_lim = max_cc_cr_lim then cc_cr_lim else null end as max_lim_cc_cr_lim"]\
+ ["case when max_cc_cr_lim is not null and cc_cr_lim = max_cc_cr_lim then cc_cur_bal else null end as max_lim_cc_cur_bal"]\
+ ["case when max_{prod}_cur_bal is not null and {prod}_cur_bal = max_{prod}_cur_bal then {prod}_highcr else null end as max_bal_{prod}_highcr".format(prod = prod) for prod in ['PL','HL']]\
+ ["case when max_cc_cur_bal is not null and cc_cur_bal = max_cc_cur_bal then cc_cr_lim else null end as max_bal_cc_cr_lim"]\
+ ["case when max_{prod}_cur_bal is not null and {prod}_cur_bal = max_{prod}_cur_bal then {prod}_cur_bal else null end as max_bal_{prod}_cur_bal".format(prod = prod) for prod in ['CC','PL','HL']]\
+ ["case when oldest_{prod} is not null and vintage_days_{prod} = oldest_{prod} then {prod}_highcr else null end as oldest_{prod}_highcr".format(prod = prod) for prod in ['PL','HL']]\
+ ["case when oldest_cc is not null and vintage_days_cc = oldest_cc then cc_cr_lim else null end as oldest_cc_cr_lim"]\
+ ["case when oldest_{prod} is not null and vintage_days_{prod} = oldest_{prod} then {prod}_cur_bal else null end as oldest_{prod}_cur_bal".format(prod = prod) for prod in ['CC','PL','HL']]\
+ ["case when latest_{prod} is not null and vintage_days_{prod} = latest_{prod} then {prod}_highcr else null end as latest_{prod}_highcr".format(prod = prod) for prod in ['PL','HL']]\
+ ["case when latest_cc is not null and vintage_days_cc = latest_cc then cc_cr_lim else null end as latest_cc_cr_lim"]\
+ ["case when latest_{prod} is not null and vintage_days_{prod} = latest_{prod} then {prod}_cur_bal else null end as latest_{prod}_cur_bal".format(prod = prod) for prod in ['CC','PL','HL']]


# %%


expr10 = ["*"]\
+["case when rcnt_trd_flag = 1 and PRODUCT_DESC='PL' and currentbal_rpt/SANCTIONAMOUNT>"+str(i)+ " then 1 else null end as pl_util_gt_"+str(int(i*100))+"_flag".format(i = i) for i in [0.25,0.5,0.75]]\
+["case when rcnt_trd_flag = 1 and PRODUCT_DESC='CC' and currentbal_rpt/coalesce(CREDITLIMIT,greatest(SANCTIONAMOUNT,HIGHCREDIT))>"+str(i)+ " then 1 else null end as cred_card_util_gt_"+str(int(i*100))+"_flag".format(i = i) for i in [0.25,0.5,0.75]]\
+[f"case when rcnt_trd_flag = 1 and PRODUCT_DESC = '{i}' then 1 else null end as {i}_flag" for i in ['PL','HL','CC', 'BL']] \
+["case when Revolving=1 and Open=1 then currentbal_rpt else 0 end as Open_Revolving_currentbal_rpt_amt"]\
+["case when Revolving=1 and open=1 then coalesce(CREDITLIMIT,greatest(SANCTIONAMOUNT,HIGHCREDIT)) else 0 end as Revolving_open_credit_limit"]\
+["case when Installment=1 and Open=1 then currentbal_rpt else 0 end as Open_install_currentbal_rpt"]\
+["case when open=1 and dpdx=1 then emi else null end as open_dpdx_emi"]\
+["case when open=1 and dpdx=1 then currentbal_rpt else null end as open_dpdx_bal"]\
+["case when SECURED=1 then currentbal_rpt end as sum_secured_currentbal_rpt"]\
+["case when UNSECURED=1 then currentbal_rpt end as sum_unsecured_currentbal_rpt"]\
+["case when open=1 and PRODUCT_DESC='CC' then coalesce(CREDITLIMIT,greatest(SANCTIONAMOUNT,HIGHCREDIT)) else null end as CC_sanc_amt"]\
+["case when open=1 and PRODUCT_DESC='"+prod+"' then currentbal_rpt else null end as live_"+prod+"_bal" for prod in ['CC','PL']]\
+["case when Installment=1 and Open=1 then Sanctionamt_der else 0 end as Loan_amt_open_install"]\
+["case when open=1 and PRODUCT_DESC='PL' then SANCTIONAMOUNT else null end as PL_sanc_amt"]\
+["case when Revolving=1 then EMI else 0 end as Monthly_payment_revolving"]\
+["case when maximum_dlnq_m1>=1 then currentbal_rpt else null end as highest_dlnq_bal"]\
+["case when maximum_dlnq_m1>=1 then tenure else null end as highest_dlnq_ten"]\
+["case when latest_cc is not null and vintage_days_cc=latest_cc then dpd_t_bkt_02 else null end as latest_cc_dlnq_m1"]\
+["case when score_v3 < 600 then 1 else 0 end as scr_less_600"]


# %%


df99 = df9.selectExpr(expr9)\
.selectExpr(expr10)


# %%


df99.write.mode('overwrite').option("compression", "snappy").parquet(temp_dir + 'df99.parquet.snappy')


# %%


df99 = spark.read.parquet(temp_dir + 'df99.parquet.snappy')


# %%


# del df8
# del df9


# %%


cc_expr6 = F.expr("filter(arrays_zip(dpd_arr_6mn, due_arr_6mn), x -> x['dpd_arr_6mn'] >= 2 AND x['due_arr_6mn'] >= 5000)")
cc_expr24 = F.expr("filter(arrays_zip(dpd_arr_24mn, due_arr_24mn), x -> x['dpd_arr_24mn'] >= 3 AND x['due_arr_24mn'] >= 5000)")
cc_expr36= F.expr("filter(arrays_zip(dpd_arr_36mn, due_arr_36mn), x -> x['dpd_arr_36mn'] >= 4 AND x['due_arr_36mn'] >= 5000)")


# %%


window_user = Window.partitionBy("user_id")


# %%


df10 = df99.withColumn("dpd_arr_6mn", F.expr("slice(dpd_final, 1, 6)"))\
.withColumn("dpd_arr_24mn", F.expr("slice(dpd_final, 1, 24)"))\
.withColumn("dpd_arr_36mn", F.expr("slice(dpd_final, 1, 36)"))\
.withColumn("due_arr_6mn", F.expr("slice(due_final, 1, 6)"))\
.withColumn("due_arr_24mn", F.expr("slice(due_final, 1, 24)"))\
.withColumn("due_arr_36mn", F.expr("slice(due_final, 1, 36)"))\
.withColumn("dpd_30_6mn_rpt",F.when(
          ((F.col("product_Desc") != "CC") & (F.expr("exists(dpd_arr_6mn, x -> x >= 2)"))) |
          ((F.col("product_Desc") == "CC") & (F.size(cc_expr6) >0)),
    1).otherwise(0))\
.withColumn("dpd_60_24mn_rpt",F.when(
          ((F.col("product_Desc") != "CC") & (F.expr("exists(dpd_arr_24mn, x -> x >= 3)"))) |
          ((F.col("product_Desc") == "CC") & (F.size(cc_expr24) >0)),
    1).otherwise(0))\
.withColumn("dpd_90_36mn_rpt",F.when(
          ((F.col("product_Desc") != "CC") & (F.expr("exists(dpd_arr_36mn, x -> x >= 4)"))) |
          ((F.col("product_Desc") == "CC") & (F.size(cc_expr36) >0)),
    1).otherwise(0))\
# .drop("dpd_arr_6mn", "dpd_arr_24mn", "dpd_arr_36mn", "due_arr_6mn", "due_arr_24mn", "due_arr_36mn")


# %%


# show_basic(df10)


# %%


df10.repartition(100).write.mode('overwrite').option("compression", "snappy").parquet(temp_dir + 'df10.parquet.snappy')


# %%


df10 = spark.read.parquet(temp_dir + 'df10.parquet.snappy')


# %%


df10_1 = df10.withColumn("writen_am", F.coalesce("write_off_amt","settlement_amt","chargeoff_am_int"))\
.withColumn("written_off_flag", F.when(
    (F.col('written_off_and_settled_status').isin(['00',"01","02", "03","04", "06", "08", "09", "10","11","12","13","14","15","16"])) &
    (F.col('writen_am') > 10000),
    1).otherwise(0))\
.withColumn("write_off_date", F.when(F.col("written_off_flag")==1,F.col("bal_dt")).otherwise(None))\
.withColumn("latest_write_off_date", F.max("write_off_date").over(window_user))\
.withColumn("wo_mn", F.trunc("latest_write_off_date", "month"))\
.withColumn("mn_wo", F.months_between("pl_mn", "wo_mn").cast("int"))\
.withColumn("written_off_in_12mn", F.when(
    (F.col("written_off_flag") ==1 ) & (F.col('mn_wo')<=12),
    1).otherwise(0))\
.withColumn("written_off_aft_12mn", F.when(
    (F.col("written_off_flag") ==1 ) & (F.col('mn_wo')>12),
    1).otherwise(0))\
.withColumn("trd_opn_aft_wo",F.when((F.col('op_dt')> F.col('latest_write_off_date')) &
                                    (F.col("vintage")>=12),1).otherwise(0))\
.withColumn("dpd_wo",F.when(
                        F.col("trd_opn_aft_wo") == 1,
                        F.slice("dpd_final", 1,F.col("mn_opn"))).otherwise(F.array()))\
.withColumn("dpd_wo_f", F.slice(F.reverse(F.col("dpd_wo")),2,13))\
.withColumn("dft_aftr_woff", F.when((F.col("trd_opn_aft_wo")==1) & (F.expr("exists(dpd_wo_f, x -> not x in (0,-1,-2))")),  1).otherwise(0))\
.withColumn("inclu_if_wo_aft_12", F.when(F.col("dft_aftr_woff") == 0,1).otherwise(0))
# .drop("dpd_wo", "dpd_wo_f", "dft_after_woff","trd_opn_aft_wo","mn_wo", "wo_mn")


# %%


df10_1.repartition(100).write.mode('overwrite').option("compression", "snappy").parquet(temp_dir + 'df10_1.parquet.snappy')


# %%


df10_1 = spark.read.parquet(temp_dir + 'df10_1.parquet.snappy')


# %%


df10_2 = df10_1.withColumn("cl_satisfactory_unsec_trd_flag", F.when(
    F.expr("open == 0 and written_off_flag == 0 and unsecured == 1  and dpd_t_bkt_01 in (0, NULL,-1)"), 
    1).otherwise(0))\
.withColumn("cl_satisfactory_sec_trd_flag", F.when(
    F.expr("open == 0 and written_off_flag == 0 and secured == 1 and dpd_t_bkt_01 in (0, NULL,-1)"), 
    1).otherwise(0))\
.withColumn("op_satisfactory_unsec_trd_flag", F.when(
    F.expr("open == 1 and written_off_flag == 0 and unsecured == 1 and dpd_t_bkt_01 in (0, NULL,-1)"), 
    1).otherwise(0))\
.withColumn("op_satisfactory_sec_trd_flag", F.when(
    F.expr("open == 1 and written_off_flag == 0 and secured == 1 and dpd_t_bkt_01 in (0, NULL,-1)"), 
    1).otherwise(0))


# %%


# show_basic(df10_2)


# %%


# df10_2.repartition(100).write.mode('overwrite').option("compression", "snappy").parquet(temp_dir + 'df10_2.parquet.snappy')


# %%


# df10_2 = spark.read.parquet(temp_dir + 'df10_2.parquet.snappy')


# %%


# del df99
# del df10_1
# del df10


# %%


agg_expr = []\
+[F.max('score_v3').alias('score_v3')]\
+[F.max('dpd_30_6mn_rpt').alias('exclu_b')]\
+[F.max('dpd_60_24mn_rpt').alias('exclu_c')]\
+[F.max('dpd_90_36mn_rpt').alias('exclu_d')]\
+[F.max(F.col('vintage_days')).alias('bureau_vintage')]\
+[F.max('written_off_in_12mn').alias('exclu_ea')]\
+[F.max('written_off_aft_12mn').alias('exclu_eb')]\
+[F.max('inclu_if_wo_aft_12').alias('inclu_eb')]\
+ [F.sum(F.col("Trade") * F.col("rcnt_trd_flag")).alias('num_trades')]\
+[F.sum('open').alias('num_open_trades')]\
+ [F.sum("Trade").alias('total_num_trades')]\
+[F.max(F.col(f"{i}_sanc_amt")).alias(f"max_{i}_sanc_amt") for i in ["AL","HL"] ]\
+[F.sum("open_pl_secured").alias('total_open_pl_secured')]\
+[F.sum("close_pl_secured").alias('total_close_pl_secured')]\
+[F.sum("open_pl_unsecured").alias('total_open_pl_unsecured')]\
+[F.sum("close_pl_unsecured").alias('total_close_pl_unsecured')]\
+[F.sum("op_satisfactory_sec_trd_flag").alias('total_op_satisfactory_sec_trd')]\
+[F.sum("op_satisfactory_unsec_trd_flag").alias('total_op_satisfactory_unsec_trd')]\
+[F.sum("cl_satisfactory_sec_trd_flag").alias('total_cl_satisfactory_sec_trd')]\
+[F.sum("cl_satisfactory_unsec_trd_flag").alias('total_cl_satisfactory_unsec_trd')]\
+ [F.sum(F.col(f"open_cc_given_{ty}")).alias(f"total_open_cc_given_{ty}") for ty in inst_type]\
+ [F.sum(F.col(f"open_pl_given_{ty}")).alias(f"total_open_pl_given_{ty}") for ty in inst_type]\
+ [F.sum(F.col(f"closed_cc_given_{ty}")).alias(f"total_closed_cc_given_{ty}") for ty in inst_type]\
+ [F.sum(F.col(f"closed_pl_given_{ty}")).alias(f"total_closed_pl_given_{ty}") for ty in inst_type]\
+[F.sum('non_mortage_sanc_amt').alias('non_mortage_sanc_amt')]\
+[F.sum(F.col('open_overdue')*F.col('rcnt_trd_flag')).alias('sum_live_trade_overdue')]\
+[F.sum(F.col('cc_overdue')*F.col('rcnt_trd_flag')).alias('sum_cc_overdue')]\
+[F.sum(F.col('non_cc_overdue')*F.col('rcnt_trd_flag')).alias('sum_non_cc_overdue')]\
+[F.sum(F.col('pl_overdue')*F.col('rcnt_trd_flag')).alias('sum_pl_overdue')]\
+ [F.sum(F.col(prod+"_cur_overdue") * F.col("rcnt_trd_flag")).alias("sum_"+prod+"_cur_overdue") for prod in prods]\
+ [F.sum(F.col("cur_overdue_rpt_"+i+"m")).alias("sum_cur_overdue_rpt"+i+"m") for i in ['3','6','9','12']]\
+ [F.sum(F.col("avg_"+prod+"_overdue_rpt_"+i+"m")).alias("avg_"+prod+"_overdue_rpt_"+i+"m") for i in ['3','6','9','12'] for prod in prods]\
+ [F.sum(F.col("avg_overdue_rpt_"+i+"m")).alias("avg_overdue_rpt_"+i+"m") for i in ['3','6','9','12']]\
+ [F.sum(F.col(prod+"_cur_bal") * F.col("rcnt_trd_flag")).alias("sum_"+prod+"_cur_bal") for prod in prods]\
+ [F.sum(F.col("avg_"+prod+"_bal_rpt_"+i+"m")).alias("avg_"+prod+"_bal_rpt_"+i+"m") for i in ['3','6','9','12'] for prod in prods]\
+ [F.sum(F.col("avg_bal_rpt_"+i+"m")).alias("avg_bal_rpt_"+i+"m") for i in ['3','6','9','12']]\
+ [F.sum(F.col("max_highcr_"+prod+"_highcr")).alias("max_highcr_"+prod+"_highcr") for prod in ['PL','HL']]\
+ [F.sum(F.col("max_highcr_"+prod+"_cur_bal")).alias("max_highcr_"+prod+"_cur_bal") for prod in ['PL','HL']]\
+ [F.sum(F.col("max_lim_cc_cr_lim")).alias("max_lim_cc_cr_lim")]\
+ [F.sum(F.col("max_lim_cc_cur_bal")).alias("max_lim_cc_cur_bal")]\
+ [F.sum(F.col("max_bal_"+prod+"_highcr")).alias("max_bal_"+prod+"_highcr") for prod in ['PL','HL']]\
+ [F.sum(F.col("max_bal_cc_cr_lim")).alias("max_bal_cc_cr_lim")]\
+ [F.sum(F.col("max_bal_"+prod+"_cur_bal")).alias("max_bal_"+prod+"_cur_bal") for prod in ['CC','PL','HL']]\
+ [F.sum(F.col("oldest_"+prod+"_highcr")).alias("oldest_"+prod+"_highcr") for prod in ['PL','HL']]\
+ [F.sum(F.col("oldest_cc_cr_lim")).alias("oldest_cc_cr_lim")]\
+ [F.sum(F.col("oldest_"+prod+"_cur_bal")).alias("oldest_"+prod+"_cur_bal") for prod in ['CC','PL']]\
+ [F.sum(F.col("latest_"+prod+"_highcr")).alias("latest_"+prod+"_highcr") for prod in ['PL','HL']]\
+ [F.sum(F.col("latest_cc_cr_lim")).alias("latest_cc_cr_lim")]\
+ [F.sum(F.col("latest_"+prod+"_cur_bal")).alias("latest_"+prod+"_cur_bal") for prod in ['CC','PL']]\
+ [F.min(F.col("hanging_trade")).alias('hanging_trade')]\
+ [F.min(F.col("hanging_"+prod+"_trade")).alias("hanging_"+prod+"_trade") for prod in prods]\
+ [F.sum(F.col("num_trades_open_last"+i+"m")).alias("num_trades_open_last"+i+"m") for i in ['3','6','9','12']]\
+ [F.sum(F.col(prod+"_num_open_last"+i+"m")).alias("num_"+prod+"trades_open_last"+i+"m") for i in ['3','6','9','12'] for prod in prods]\
+ [F.sum(F.col(prod+"_num_open_last"+i+"m_"+amt)).alias("num_"+prod+"_trades_open_last"+i+"m_"+amt) for i in ['3','6','9','12'] for prod in prods for amt in ['50k','1L']]\
+ [F.sum(F.col("sec_ind") * F.col("rcnt_trd_flag")).alias('num_live_sec_trades')]\
+ [F.sum(F.col("unsec_ind") * F.col("rcnt_trd_flag")).alias('num_live_unsec_trades')]\
+ [F.sum(F.col("cur_bal_live_trades") * F.col("rcnt_trd_flag")).alias('total_cur_bal_live_trades')]\
+ [F.sum(F.col("avg_bal_live_trades_rpt_"+i+"m")).alias("avg_bal_live_trades_rpt_"+i+"m") for i in ['3','6','9','12']]\
+ [F.sum(F.col("emi_live_trades") * F.col("rcnt_trd_flag")).alias("total_emi_live_trades")]\
+ [F.sum(F.col("live_"+prod+"_trades_cur_bal") * F.col("rcnt_trd_flag")).alias('total_cur_bal_live_'+prod+'_trades') for prod in prods]\
+ [F.sum(F.col("live_"+prod+"_trades_cur_bal_open_last"+i+"m")).alias("total_cur_bal_live"+prod+"_trades_open_last"+i+"m") for i in ['3','6','9','12'] for prod in prods]\
+ [F.sum(F.col("installment") * F.col("rcnt_trd_flag")).alias('num_install_trades')]\
+ [F.sum(F.col("live_install_trades") * F.col("rcnt_trd_flag")).alias('num_live_install_trades')]\
+ [F.sum(F.col("cur_bal_install_trades") * F.col("rcnt_trd_flag")).alias('total_cur_bal_install_trades')]\
+ [F.sum(F.col("avg_bal_install_trades_rpt_"+i+"m")).alias("avg_bal_install_trades_rpt_"+i+"m") for i in ['3','6','9','12']]\
+ [F.sum(F.col("cur_bal_live_install_trades") * F.col("rcnt_trd_flag")).alias('total_cur_bal_live_installtrades')]\
+ [F.sum(F.col("avg_bal_live_install_trades_rpt_"+i+"m")).alias("avg_bal_live_install_trades_rpt_"+i+"m") for i in ['3','6','9','12']]\
+ [F.sum(F.col("emi_install_trades") * F.col("rcnt_trd_flag")).alias("total_emi_install_trades")]\
+ [F.sum(F.col("emi_live_install_trades") * F.col("rcnt_trd_flag")).alias("total_emi_live_install_trades")]\
+ [F.sum(F.col("loan_amt_live_install_trades") * F.col("rcnt_trd_flag")).alias("total_sanc_amt_live_install_trades")]\
+ [F.sum(F.col("loan_amt_install_trades") * F.col("rcnt_trd_flag")).alias("total_sanc_amt_install_trades")]\
+ [F.sum(F.col("live_revolving_trades") * F.col("rcnt_trd_flag")).alias('num_live_revolving_trades')]\
+ [F.sum(F.col("cur_bal_revolving_trades") * F.col("rcnt_trd_flag")).alias('total_cur_bal_revolving_trades')]\
+ [F.sum(F.col("avg_bal_revolving_trades_rpt_"+i+"m")).alias("avg_bal_revolving_trades_rpt_"+i+"m") for i in ['3','6','9','12']]\
+ [F.sum(F.col("cur_bal_live_revolving_trades") * F.col("rcnt_trd_flag")).alias('total_cur_bal_live_revolving_trades')]\
+ [F.sum(F.col("avg_bal_live_revolving_trades_rpt_"+i+"m")).alias("avg_bal_live_revolving_trades_rpt_"+i+"m") for i in ['3','6','9','12']]\
+ [F.sum(F.col("emi_revolving_trades") * F.col("rcnt_trd_flag")).alias("total_emi_revolving_trades")]\
+ [F.sum(F.col("emi_live_revolving_trades") * F.col("rcnt_trd_flag")).alias("total_emi_live_revolving_trades")]\
+ [F.sum(F.col("sanc_amt_live_revolving_trades") * F.col("rcnt_trd_flag")).alias("total_sanc_amt_live_revolving_trades")]\
+ [F.sum(F.col("sanc_amt_revolving_trades") * F.col("rcnt_trd_flag")).alias("total_sanc_amt_revolving_trades")]\
+ [F.sum(F.col("SECURED") * F.col("rcnt_trd_flag")).alias('num_secured_trades')]\
+ [F.sum(F.col("emi_secured_trades") * F.col("rcnt_trd_flag")).alias("total_emi_secured_trades")]\
+ [F.sum(F.col("emi_live_secured_trades") * F.col("rcnt_trd_flag")).alias("total_emi_live_secured_trades")]\
+ [F.sum(F.col("cur_bal_secured_trades") * F.col("rcnt_trd_flag")).alias('total_cur_bal_secured_trades')]\
+ [F.sum(F.col("avg_bal_secured_trades_rpt_"+i+"m")).alias("avg_bal_secured_trades_rpt_"+i+"m") for i in ['3','6','9','12']]\
+ [F.sum(F.col("cur_bal_live_secured_trades") * F.col("rcnt_trd_flag")).alias('total_cur_bal_live_secured_trades')]\
+ [F.sum(F.col("avg_bal_live_secured_trades_rpt_"+i+"m")).alias("avg_bal_live_secured_trades_rpt_"+i+"m") for i in ['3','6','9','12']]\
+ [F.sum(F.col("emi_unsecured_trades") * F.col("rcnt_trd_flag")).alias("total_emi_unsecured_trades")]\
+ [F.sum(F.col("emi_live_unsecured_trades") * F.col("rcnt_trd_flag")).alias("total_emi_live_unsecured_trades")]\
+ [F.sum(F.col("highcr_secured_trades") * F.col("rcnt_trd_flag")).alias("total_highcr_secured_trades")]\
+ [F.sum(F.col("highcr_live_secured_trades") * F.col("rcnt_trd_flag")).alias("total_highcr_live_secured_trades")]\
+ [F.sum(F.col("cur_bal_unsecured_trades") * F.col("rcnt_trd_flag")).alias('total_cur_bal_unsecured_trades')]\
+ [F.sum(F.col("avg_bal_unsecured_trades_rpt_"+i+"m")).alias("avg_bal_unsecured_trades_rpt_"+i+"m") for i in ['3','6','9','12']]\
+ [F.sum(F.col("cur_bal_live_unsecured_trades") * F.col("rcnt_trd_flag")).alias('total_cur_bal_live_unsecured_trades')]\
+ [F.sum(F.col("avg_bal_live_unsecured_trades_rpt_"+i+"m")).alias("avg_bal_live_unsecured_trades_rpt_"+i+"m") for i in ['3','6','9','12']]\
+ [F.avg(F.col("tenure_all_revol") * F.col("rcnt_trd_flag")).alias('avg_vintage_revol_trades')]\
+ [F.max(F.col("tenure_all_revol") * F.col("rcnt_trd_flag")).alias('revol_trade_vintage')]\
+ [F.avg(F.col("tenure_all_install") * F.col("rcnt_trd_flag")).alias('avg_vintage_install_trades')]\
+ [F.max(F.col("tenure_all_install") * F.col("rcnt_trd_flag")).alias('install_trade_vintage')]\
+ [F.max(F.col("dpd0") * F.col("rcnt_trd_flag")).alias('num_trades_dpd0')]\
+ [F.max(F.col("dpdx") * F.col("rcnt_trd_flag")).alias('num_trades_dpdx_plus')]\
+ [F.max(F.col("dpd30") * F.col("rcnt_trd_flag")).alias('num_trades_dpd30_plus')]\
+ [F.max(F.col("dpd60") * F.col("rcnt_trd_flag")).alias('num_trades_dpd60_plus')]\
+ [F.max(F.col("dpd90") * F.col("rcnt_trd_flag")).alias('num_trades_dpd90_plus')]\
+ [F.max(F.col("dpd150") * F.col("rcnt_trd_flag")).alias('num_trades_dpd150_plus')]\
+ [F.max(F.col("dpd180") * F.col("rcnt_trd_flag")).alias('num_trades_dpd180_plus')]\
+ [F.max(F.col("dpd360") * F.col("rcnt_trd_flag")).alias('num_trades_dpd360_plus')]\
+[F.sum(F.col("dpdvalid")*F.col('rcnt_trd_flag')).alias("num_trades_dpdvalid")]\
+ [F.max(F.col('max_dpd_3m')).alias('max_dpd_3m')]\
+ [F.max(F.col('max_dpd_6m')).alias('max_dpd_6m')]\
+ [F.max(F.col('max_dpd_12m')).alias('max_dpd_12m')]\
+[F.sum('pl_util_gt_'+str(i)+'_flag').alias('num_pl_util_gt_'+str(i)) for i in [25,50,75]]\
+[F.sum('pl_flag').alias('num_pl')]\
+[F.sum('cred_card_util_gt_'+str(i)+'_flag').alias('num_cred_card_util_gt_'+str(i)) for i in [25,50,75]]\
+[F.sum('cc_flag').alias('num_cred_card')]\
+[F.sum(F.col('Open_Revolving_currentbal_rpt_amt')*F.col('rcnt_trd_flag')).alias('total_bal_live_revol_trades')]\
+[F.sum(F.col('Revolving_open_credit_limit')*F.col('rcnt_trd_flag')).alias('total_cr_lim_live_revol_trades')]\
+[F.sum(F.col('Open_install_currentbal_rpt')*F.col('rcnt_trd_flag')).alias('live_install_trades_bal')]\
+[F.sum(F.col("EMI")*F.col('rcnt_trd_flag')).alias("sum_emi")]\
+[F.sum(F.col("currentbal_rpt")*F.col('rcnt_trd_flag')).alias("total_outstanding_bal")]\
+[F.sum(F.col("open_dpdx_emi")*F.col('rcnt_trd_flag')).alias("live_dpdx_emi")]\
+[F.sum(F.col("open_dpdx_bal")*F.col('rcnt_trd_flag')).alias("live_dpdx_bal")]\
+[F.sum(F.col("sum_secured_currentbal_rpt")*F.col('rcnt_trd_flag')).alias("sum_secured_currentbal_rpt")]\
+[F.sum(F.col("sum_unsecured_currentbal_rpt")*F.col('rcnt_trd_flag')).alias("sum_unsecured_currentbal_rpt")]\
+[F.sum(F.col('CC_sanc_amt')*F.col('rcnt_trd_flag')).alias('live_CC_sanc_amt')]\
+[F.sum(F.col('live_cc_bal')*F.col('rcnt_trd_flag')).alias('total_bal_live_cc_trades')]\
+[F.sum(F.col('live_pl_bal')*F.col('rcnt_trd_flag')).alias('total_bal_live_pl_trades')]\
+[F.sum(F.col(prod+i+'plus_dlnq_bal')*F.col('rcnt_trd_flag')).alias(prod+i+'plus_dlnq_bal') for i in ['30','60','90'] for prod in ['','cc_live_','pl_live_']]\
+[F.sum(F.col(prod+i+'plus_dlnq_opn_last_'+j+'mn_flag')*F.col('rcnt_trd_flag')).alias('num_'+prod+i+'plus_dlnq_opn_last_'+j+'mn') for i in ['30','60','90'] for j in ['3','6','9','12'] for prod in ['CC_','PL_','']]\
+[F.sum(F.col('Loan_amt_open_install')*F.col('rcnt_trd_flag')).alias('live_install_trades_sanc_amt')]\
+ [F.sum(F.col("reopen_"+i+"m") * F.col("rcnt_trd_flag")).alias("sum_"+"reopen"+i+"m") for i in ['1','3','6','12']]\
+ [F.sum(F.col("reopen_"+prod+"_"+i+"m")).alias("sum_"+"reopen_"+prod+"_"+i+"m") for i in ['1','3','6','12'] for prod in prods]\
+[F.sum(F.col('PL_sanc_amt')*F.col('rcnt_trd_flag')).alias('live_PL_sanc_amt')]\
+[F.sum('overdue_rpt_3mn').alias('sum_overdue_rpt_3mn')]\
+[F.sum('overdue_rpt_6mn').alias('sum_overdue_rpt_6mn')]\
+[F.sum(F.col(prod+'_num_open_last'+str(i)+'m_'+j)).alias('num_'+prod+'_opened_in_last'+str(i)+'m_'+j) for prod in ['cc','pl'] for i in [3,6,12] for j in ['50k','1l','5l'] \
if (prod=='pl' and (j=='50k' or j=='1l' or j=='5l')) or (prod=='cc' and (j=='50k' or j=='1l'))]\
+[F.sum(F.col('Monthly_payment_revolving')*F.col('rcnt_trd_flag')).alias('sum_min_due_revol_trades')]\
+[F.first(F.col("latest_product")).alias("latest_product")]\
+[F.max('dpdx_counter_last12m').alias('num_missed_pmt')]\
+[F.sum('highest_dlnq_bal').alias('highest_dlnq_trd_bal')]\
+[F.min('highest_dlnq_ten').alias('highest_dlnq_trd_vintage')]\
+[F.max('latest_cc_dlnq_m1').alias('latest_cc_dlnq_m1')]\
+ [F.sum(F.col(f"curbal_{i}") * F.col("secured")).alias(f"secured_curbal_{i}") for i in range(1, 37)] \
+ [F.sum(F.col(f"curbal_{i}") * (1 - F.col("secured"))).alias(f"unsecured_curbal_{i}") for i in range(1, 37)] \
+ [F.sum(F.col(f"curbal_{i}") * F.col("bl_flag")).alias(f"bl_curbal_{i}") for i in range(1, 37)] \
+ [F.sum(F.col(f"curbal_{i}") * (1 - F.col("bl_flag"))).alias(f"non_bl_curbal_{i}") for i in range(1, 37)] \
+ [F.max(F.col("vintage")).alias(f"mn_since_oldest_trd")]\
+ [F.max(F.col("vintage") * F.col(f"{i}_flag")).alias(f"mn_since_oldest_{i}_trd") for i in ['hl', 'pl']] \
+ [F.min(F.col("vintage") * F.col(f"{i}_flag")).alias(f"mn_since_latest_{i}_trd") for i in ['hl', 'pl']] \
+ [F.max(F.col("cc_flag") * F.col("curbal_1")).alias("latest_bal_top_wallet_card")] \
+ [F.sum(F.col("cc_flag") * F.col("curbal_1")).alias("latest_cc_bal")] \
+ [F.sum(F.col("cc_flag") * F.col("highcredit")).alias("latest_cc_cr_lmt")] \
+ [F.sum(F.col("cc_flag") * F.col(f"curbal_{i}")).alias(f"cc_bal{i}") for i in range(1,13)] \
+ [F.max(f"{i}_flag").alias(f"{i}_flag") for i in ['hl', 'pl', 'cc']]\
+[F.expr(f"sum(is_open_m{i}) as num_concurrent_live_loans_m{i}") for i in [1,4]]\
+[F.expr(f"sum(is_open_m{i}*{flag}) as num_{flag}_concurrent_live_loans_m{i}") for i in [1,4] for flag in ['secured','unsecured','installment','revolving']]


# %%


df11 = df10_2.groupby("user_id", "loan_month").agg(*agg_expr)


# %%


df11.write.mode('overwrite').option("compression", "snappy").parquet(temp_dir + 'df11.parquet.snappy')


# %%


df11 = spark.read.parquet(temp_dir + 'df11.parquet.snappy')


# %%


# del df10_2


# %%


# show_basic(df11)


# %%


expr11 = ["*"]\
+ ["case when num_pl is null or num_pl=0 then -1 else num_pl_util_gt_{i}/num_pl end as perc_pl_util_gt_{i}".format(i = i) for i in ['25','50','75']]\
+ ["case when num_cred_card is null or num_cred_card=0 then -1 else num_cred_card_util_gt_{i}/num_cred_card end as perc_cr_card_util_gt_{i}".format(i = i) for i in ['25','50','75']]\
+ ["case when total_bal_live_revol_trades is null then -1 when total_cr_lim_live_revol_trades is null then -2 when total_cr_lim_live_revol_trades = 0 then -3 else total_bal_live_revol_trades/total_cr_lim_live_revol_trades end as util_live_revol_trades"]\
+ ["case when live_install_trades_bal is null then -1 when live_install_trades_sanc_amt is null then -2 when live_install_trades_sanc_amt = 0 then -3 else live_install_trades_bal/live_install_trades_sanc_amt end as frac_unpaid_live_install_trades"]\
+ ["case when sum_emi is null then -1 \
    when total_outstanding_bal is null then -2 \
    when total_outstanding_bal = 0 then -3 \
    else sum_emi/total_outstanding_bal end as emi_bal_ratio"]\
+ ["case when live_dpdx_emi is null then -1 \
    when live_dpdx_bal is null then -2 \
    when live_dpdx_bal=0 then -3 \
    else live_dpdx_emi/live_dpdx_bal end as emi_bal_ratio_live_dpdx_trades"]\
+ ["case when total_outstanding_bal is null or total_outstanding_bal=0 then -1 when sum_secured_currentbal_rpt is null then 0 else sum_secured_currentbal_rpt/total_outstanding_bal end as prop_sec_bal"]\
+ ["case when total_outstanding_bal is null or total_outstanding_bal=0 then -1 when sum_unsecured_currentbal_rpt is null then 0 else sum_unsecured_currentbal_rpt/total_outstanding_bal end as prop_unsec_bal"]\
+ ["case when sum_secured_currentbal_rpt is null then -1 when sum_unsecured_currentbal_rpt is null then -2 when sum_unsecured_currentbal_rpt = 0 then -3 else sum_secured_currentbal_rpt/sum_unsecured_currentbal_rpt end as ratio_secured_unsum_secured_currentbal_rpts"]\
+ ["case when num_trades_dpdvalid>0 then num_trades_dpd{i}_plus/num_trades else -1 end as frac_trades_{i}plus".format(i = i) for i in ['30','60','90']]\
+ ["case when live_CC_sanc_amt=0 then -2 when live_CC_sanc_amt is null then -1 else total_bal_live_cc_trades/live_CC_sanc_amt end as live_cc_util"]\
+ ["case when live_PL_sanc_amt=0 then -3 when live_PL_sanc_amt is null then -2 when live_PL_sanc_amt<1000 then -1 else (live_PL_sanc_amt-total_bal_live_pl_trades)/live_PL_sanc_amt end as frac_pl_paid"]\
+ ["case when total_outstanding_bal is null or total_outstanding_bal = 0 then -1  else {i}plus_dlnq_bal/total_outstanding_bal end as frac_{i}plus_dlnq_bal".format(i=  i) for i in ['30','60','90']]\
+["case when total_bal_live_{prod}_trades is null or total_bal_live_{prod}_trades = 0 then -1  else {prod}_live_{i}plus_dlnq_bal/total_bal_live_{prod}_trades end as frac_{prod}_live_{i}plus_dlnq_bal".format(i=i, prod=prod) for i in ['30','60','90'] for prod in ['cc','pl']]\
+ [f"case when num_{flag}_{product_type}concurrent_live_loans_m1 is null and num_{flag}_{product_type}concurrent_live_loans_m4 is null then 0 \
when num_{flag}_{product_type}concurrent_live_loans_m1 is null then -1*num_{flag}_{product_type}concurrent_live_loans_m4 \
when num_{flag}_{product_type}concurrent_live_loans_m4 is null then num_{flag}_{product_type}concurrent_live_loans_m1 \
else num_{flag}_{product_type}concurrent_live_loans_m1-num_{flag}_{product_type}concurrent_live_loans_m4 \
end as diff_num_{flag}_{product_type}concurrent_live_m1_m4" for flag in ['secured','unsecured'] for product_type in ['']]\
+ ["case when num_concurrent_live_loans_m1 is null then 0 when num_concurrent_live_loans_m4 is null or num_concurrent_live_loans_m4=0 then num_concurrent_live_loans_m1 else num_concurrent_live_loans_m1/num_concurrent_live_loans_m4 end as ratio_num_concurrent_live_loans_m1_m4"]\
+ [f"case when num_{flag}_{product_type}concurrent_live_loans_m1 is null then 0 when num_{flag}_{product_type}concurrent_live_loans_m4 is null or num_{flag}_{product_type}concurrent_live_loans_m4=0 then num_{flag}_{product_type}concurrent_live_loans_m1 else num_{flag}_{product_type}concurrent_live_loans_m1/num_{flag}_{product_type}concurrent_live_loans_m4 end as ratio_num_{flag}_{product_type}concurrent_live_loans_m1_m4" for flag in ['secured','unsecured'] for product_type in ['']]\
+ [f"CASE WHEN secured_curbal_{i+1} IS NOT NULL \
AND (secured_curbal_{i} > secured_curbal_{i+1} + 500) \
THEN 1 ELSE 0 END as sec_bal_inc_ind_{i}" for i in range(1,36)] \
+ [f"CASE WHEN secured_curbal_{i} IS NOT NULL \
AND (secured_curbal_{i} < secured_curbal_{i+1} + 500) \
THEN 1 ELSE 0 END as sec_bal_dec_ind_{i}" for i in range(1,36)] \
+ [f"CASE WHEN unsecured_curbal_{i+1} IS NOT NULL \
AND (unsecured_curbal_{i} > unsecured_curbal_{i+1} + 500) \
THEN 1 ELSE 0 END as unsec_bal_inc_ind_{i}" for i in range(1,36)] \
+ [f"CASE WHEN unsecured_curbal_{i} IS NOT NULL \
AND (unsecured_curbal_{i} < unsecured_curbal_{i+1} + 500) \
THEN 1 ELSE 0 END as unsec_bal_dec_ind_{i}" for i in range(1,36)] \
+ [f"CASE WHEN bl_curbal_{i+1} IS NOT NULL \
AND (bl_curbal_{i} > bl_curbal_{i+1} + 500) \
THEN 1 ELSE 0 END as bl_bal_inc_ind_{i}" for i in range(1,36)] \
+ [f"CASE WHEN bl_curbal_{i} IS NOT NULL \
AND (bl_curbal_{i} < bl_curbal_{i+1} + 500) \
THEN 1 ELSE 0 END as bl_bal_dec_ind_{i}" for i in range(1,36)] \
+ [f"CASE WHEN non_bl_curbal_{i+1} IS NOT NULL \
AND (non_bl_curbal_{i} > non_bl_curbal_{i+1} + 500) \
THEN 1 ELSE 0 END as non_bl_bal_inc_ind_{i}" for i in range(1,36)] \
+ [f"CASE WHEN non_bl_curbal_{i} IS NOT NULL \
AND (non_bl_curbal_{i} < non_bl_curbal_{i+1} + 500) \
THEN 1 ELSE 0 END as non_bl_bal_dec_ind_{i}" for i in range(1,36)]


# %%


expr12 = ["*"] \
+ ["case when mn_since_oldest_trd >=12 and (live_CC_sanc_amt >= 75000 or live_PL_sanc_amt >= 200000 or max_AL_sanc_amt >= 200000 or max_AL_sanc_amt >= 1000000) then 'thick' when (mn_since_oldest_trd >= 6 and mn_since_oldest_trd < 12) or (live_CC_sanc_amt < 75000 and live_PL_sanc_amt < 200000 and max_AL_sanc_amt < 200000 and max_AL_sanc_amt < 1000000) then 'thin' else 'less_than_6' end as thickness_indicator"]\
+ ["case when score_v3 < 600 then 1 else 0 end as exclu_a"]\
+ ["case when bureau_vintage < 180 then 1 else 0 end as exclu_f"]\
+ [" + ".join([f"sec_bal_inc_ind_{i}" for i in range(1,12)]) + " as num_times_sec_bal_inc_12m"] \
+ [" + ".join([f"sec_bal_dec_ind_{i}" for i in range(1,12)]) + " as num_times_sec_bal_dec_12m"] \
+ [" + ".join([f"unsec_bal_inc_ind_{i}" for i in range(1,12)]) + " as num_times_unsec_bal_inc_12m"] \
+ [" + ".join([f"unsec_bal_dec_ind_{i}" for i in range(1,12)]) + " as num_times_unsec_bal_dec_12m"] \
+ [" + ".join([f"sec_bal_inc_ind_{i}" for i in range(1,24)]) + " as num_times_sec_bal_inc_24m"] \
+ [" + ".join([f"sec_bal_dec_ind_{i}" for i in range(1,24)]) + " as num_times_sec_bal_dec_24m"] \
+ [" + ".join([f"unsec_bal_inc_ind_{i}" for i in range(1,24)]) + " as num_times_unsec_bal_inc_24m"] \
+ [" + ".join([f"unsec_bal_dec_ind_{i}" for i in range(1,24)]) + " as num_times_unsec_bal_dec_24m"] \
+ [" + ".join([f"sec_bal_inc_ind_{i}" for i in range(1,36)]) + " as num_times_sec_bal_inc_36m"] \
+ [" + ".join([f"sec_bal_dec_ind_{i}" for i in range(1,36)]) + " as num_times_sec_bal_dec_36m"] \
+ [" + ".join([f"unsec_bal_inc_ind_{i}" for i in range(1,36)]) + " as num_times_unsec_bal_inc_36m"] \
+ [" + ".join([f"unsec_bal_dec_ind_{i}" for i in range(1,36)]) + " as num_times_unsec_bal_dec_36m"] \
+ [" + ".join([f"bl_bal_inc_ind_{i}" for i in range(1,12)]) + " as num_times_bl_bal_inc_12m"] \
+ [" + ".join([f"bl_bal_dec_ind_{i}" for i in range(1,12)]) + " as num_times_bl_bal_dec_12m"] \
+ [" + ".join([f"non_bl_bal_inc_ind_{i}" for i in range(1,12)]) + " as num_times_non_bl_bal_inc_12m"] \
+ [" + ".join([f"non_bl_bal_dec_ind_{i}" for i in range(1,12)]) + " as num_times_non_bl_bal_dec_12m"] \
+ [" + ".join([f"bl_bal_inc_ind_{i}" for i in range(1,24)]) + " as num_times_bl_bal_inc_24m"] \
+ [" + ".join([f"bl_bal_dec_ind_{i}" for i in range(1,24)]) + " as num_times_bl_bal_dec_24m"] \
+ [" + ".join([f"non_bl_bal_inc_ind_{i}" for i in range(1,24)]) + " as num_times_non_bl_bal_inc_24m"] \
+ [" + ".join([f"non_bl_bal_dec_ind_{i}" for i in range(1,24)]) + " as num_times_non_bl_bal_dec_24m"] \
+ [" + ".join([f"bl_bal_inc_ind_{i}" for i in range(1,36)]) + " as num_times_bl_bal_inc_36m"] \
+ [" + ".join([f"bl_bal_dec_ind_{i}" for i in range(1,36)]) + " as num_times_bl_bal_dec_36m"] \
+ [" + ".join([f"non_bl_bal_inc_ind_{i}" for i in range(1,36)]) + " as num_times_non_bl_bal_inc_36m"] \
+ [" + ".join([f"non_bl_bal_dec_ind_{i}" for i in range(1,36)]) + " as num_times_non_bl_bal_dec_36m"] \
+ ["case when secured_curbal_12 > 0 then secured_curbal_6 / secured_curbal_12 else -1 end as secured_curbal_6_12_ratio"] \
+ ["case when unsecured_curbal_12 > 0 then unsecured_curbal_6 / unsecured_curbal_12 else -1 end as unsecured_curbal_6_12_ratio"] \
+ ["case when latest_cc_cr_lmt is null then -2 \
    when latest_cc_cr_lmt <= 0 then -1 \
    else latest_cc_bal / latest_cc_cr_lmt end as latest_cc_util"]


# %%


rm_cols = [f"secured_curbal_{i}" for i in range(1,37)] \
+ [f"unsecured_curbal_{i}" for i in range(1,37)] \
+ [f"bl_curbal_{i}" for i in range(1,37)] \
+ [f"non_bl_curbal_{i}" for i in range(1,37)] \
+ [f"sec_bal_inc_ind_{i}" for i in range(1,37)] \
+ [f"unsec_bal_inc_ind_{i}" for i in range(1,37)] \
+ [f"sec_bal_dec_ind_{i}" for i in range(1,37)] \
+ [f"unsec_bal_dec_ind_{i}" for i in range(1,37)] \
+ [f"cc_bal{i}" for i in range(1,37)] \
+ [f"bl_bal_inc_ind_{i}" for i in range(1,36)] \
+ [f"bl_bal_dec_ind_{i}" for i in range(1,36)] \
+ [f"non_bl_bal_inc_ind_{i}" for i in range(1,36)] \
+ [f"non_bl_bal_dec_ind_{i}" for i in range(1,36)]\
+["cc_bal_array"]


# %%


df12 = df11.selectExpr(expr11).selectExpr(expr12)


# %%


# show_basic(df12)


# %%


df12.write.mode('overwrite').option("compression", "snappy").parquet(temp_dir + 'df12.parquet.snappy')


# %%


df12 = spark.read.parquet(temp_dir + 'df12.parquet.snappy')
# %%


del df11


# %%


expr13 = ["*"]\
+ ["case when total_open_pl_secured > 0 then total_op_satisfactory_sec_trd/total_open_pl_secured else null end as ratio_stas_op_pl_sec"]\
+ ["case when total_open_pl_unsecured > 0 then total_op_satisfactory_unsec_trd/total_open_pl_unsecured else null end as ratio_stas_op_pl_unsec"]\
+ ["case when total_close_pl_secured > 0 then total_cl_satisfactory_sec_trd/total_close_pl_secured else null end as ratio_stas_cl_pl_sec"]\
+ ["case when total_close_pl_unsecured > 0 then total_cl_satisfactory_unsec_trd/total_close_pl_unsecured else null end as ratio_stas_cl_pl_unsec"]


# %%


df13 = df12.withColumn("max_cc_bal_12mn", F.greatest(*[F.col(c) for c in [f'cc_bal{i}' for i in range(1,13)]]))\
.withColumn("cc_bal_array", F.array([F.col(c) for c in [f'cc_bal{i}' for i in range(1, 13)]]))\
.withColumn("mn_since_max_cc_bal", F.expr("array_position(cc_bal_array, max_cc_bal_12mn)"))\
.selectExpr(expr13)\
.drop(*rm_cols)


# %%


# show_basic(df13)


# %%


start_time = time.time()
df13.write.mode('overwrite').parquet(out_dir + 'TL_features_2.9lac_users_2105.parquet')
end_time = time.time()
print("time required in saving the file ", (end_time-start_time)/60, "minutes")


# %%




