#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
# coding: utf-8
 


# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import types as T

import pandas as pd
import sys
import os


# In[3]:


spark = SparkSession.builder.appName('abc')\
.config('spark.driver.memory','10g')\
.config('spark.sql.legacy.timeParserPolicy','LEGACY')\
.config('spark.sql.codegen.wholeStage', 'false')\
.getOrCreate()


# In[4]:


## change this date according to the month for which data is required
data_month = datetime.strptime('2024-06-01', "%Y-%m-%d").date()
 
tl_raw_data_path = '/home/data/manu/sample/crif/'
 
 


# In[5]:


run_dt = '2025-05-01'


# In[6]:


data_month_str = "_".join(str(data_month).split("-")[:2])


# In[7]:


data_dir = "/home/jupyter-monica.marmit/feature_enginerring/crif/output/"
# base_data_with_waterfall_and_target_path = f"{data_dir}/base_data_with_waterfall_and_target_{data_month_str}.parquet"
base_raw_data_with_waterfall_path = f"{data_dir}/base_raw_data_with_waterfall_{data_month_str}.parquet"
tl_pre_prc_data_path = f"{data_dir}/tl_pre_prc_data_{data_month_str}.parquet"
tl_feat_path = f"{data_dir}/tl_feat_{data_month_str}.parquet"
 
 


# In[ ]:


# df_base = spark.read.parquet(base_raw_data_with_waterfall_path)\
# .select("accno", "loan_month")\
# .withColumn("run_dt", F.add_months(F.col("loan_month"), 1))
 
 
 


# In[64]:


df_trd_pre = spark.read.csv(
    tl_raw_data_path + 'CRIF_sample.csv' ,header=True,inferSchema=True
) 


# In[10]:


expr0 = [f"`{i}` as {i.replace(' ', '').replace('-', '').replace('/', '').replace(':', '')}" for i in df_trd_pre.columns]


# In[11]:


#Replacing acc_num to lossappid
df_trd_pre = df_trd_pre.selectExpr(expr0)


# In[12]:


df_trd_pre.count()


# In[13]:


df_trd_pre.filter(F.col('losappid').startswith('LAN')).count()


# In[14]:


df_trd=df_trd_pre.withColumnRenamed("losappid","accno")\
.withColumn("run_dt",F.to_date(F.lit(run_dt)))\
.withColumn("loan_month", F.add_months(F.col("run_dt"), -1))


# In[15]:


df_trd.count()


# In[20]:


#df = df_base.join(df_trd, on = 'accno', how = 'inner')
 
 


# In[ ]:


#print(df_base.count(), df_trd.count(), df.count())
 
 


# In[ ]:


#df.groupby('accno').count().count()
 
 


# In[ ]:


# df.columns
 
 


# In[16]:


df1 = df_trd\
.withColumn("currentbal", F.regexp_replace(F.col("currentbal"), ",", "").cast("float"))\
.withColumn("creditlimitsancamt", F.regexp_replace(F.col("creditlimitsancamt"), ",", "").cast("float"))\
.withColumn("disbursedamthighcredit", F.regexp_replace(F.col("disbursedamthighcredit"), ",", "").cast("float"))\
.withColumn("overdueamt", F.regexp_replace(F.col("overdueamt"), ",", "").cast("float"))\
.withColumn("REPORTEDDATEHIST", F.split(F.col("REPORTEDDATEHIST"), ","))\
.withColumn("REPORTEDDATEHIST", F.slice("REPORTEDDATEHIST", 1, F.size("REPORTEDDATEHIST") - 1))\
.withColumn("HIGHCRDHIST", F.split(F.col("HIGHCRDHIST"), ","))\
.withColumn("HIGHCRDHIST", F.slice("HIGHCRDHIST", 1, F.size("HIGHCRDHIST") - 1))\
.withColumn("CURBALHIST", F.split(F.col("CURBALHIST"), ","))\
.withColumn("CURBALHIST", F.slice("CURBALHIST", 1, F.size("CURBALHIST") - 1))\
.withColumn("AMTOVERDUEHIST", F.split(F.col("AMTOVERDUEHIST"), ","))\
.withColumn("AMTOVERDUEHIST", F.slice("AMTOVERDUEHIST", 1, F.size("AMTOVERDUEHIST") - 1))\
.withColumn("AMTPAIDHIST", F.split(F.col("AMTPAIDHIST"), ","))\
.withColumn("AMTPAIDHIST", F.slice("AMTPAIDHIST", 1, F.size("AMTPAIDHIST") - 1))\
.withColumn("num_mn_hist", F.size("REPORTEDDATEHIST"))\
.filter("num_mn_hist > 0")\
.withColumn("HIGHCRDHIST", F.expr("transform(HIGHCRDHIST, x -> cast(x as float))"))\
.withColumn("CURBALHIST", F.expr("transform(CURBALHIST, x -> cast(x as float))"))\
.withColumn("AMTOVERDUEHIST", F.expr("transform(AMTOVERDUEHIST, x -> cast(x as float))"))\
.withColumn("AMTPAIDHIST", F.expr("transform(AMTPAIDHIST, x -> cast(x as float))"))\
.withColumn("DPDHIST", F.expr("case when length(DPDHIST) < num_mn_hist*3 then rpad(DPDHIST, num_mn_hist*3, 0) else DPDHIST end"))\
.withColumn("dpd_array", F.expr("""
    transform(
    sequence(1, num_mn_hist*3, 3), 
    pos -> substring(DPDHIST, pos, 3))
"""))\
.withColumn("asset_class_array", F.expr("""
    transform(
    sequence(1, num_mn_hist*3, 3), 
    pos -> substring(ASSETCLASSHIST, pos, 3))
"""))\
.withColumn("das_array", F.expr("""
    transform(
    sequence(1, num_mn_hist*3, 3), 
    pos -> substring(DASHIST, pos, 3))
"""))\
.withColumn("REPORTEDDATEHIST",F.transform('REPORTEDDATEHIST', lambda x: F.trunc(F.to_date(x,format='yyyyMMdd'),"month")))\
.withColumn("comparison_array", F.expr(f"""transform(REPORTEDDATEHIST, x -> IF(x < run_dt, 1, 0))"""))\
.withColumn("start_idx", F.when(F.array_max("comparison_array") == 1, F.array_position(F.col("comparison_array"), 1)).otherwise(100))\
.withColumn("REPORTEDDATEHIST", F.slice(F.col("REPORTEDDATEHIST"), F.col("start_idx"), 100))\
.withColumn("num_mn_hist1", F.size("REPORTEDDATEHIST"))\
.filter("num_mn_hist1 > 0")\
.withColumn("HIGHCRDHIST", F.slice(F.col("HIGHCRDHIST"), F.col("start_idx"), 100))\
.withColumn("CURBALHIST", F.slice(F.col("CURBALHIST"), F.col("start_idx"), 100))\
.withColumn("AMTOVERDUEHIST", F.slice(F.col("AMTOVERDUEHIST"), F.col("start_idx"), 100))\
.withColumn("AMTPAIDHIST", F.slice(F.col("AMTPAIDHIST"), F.col("start_idx"), 100))\
.withColumn("dpd_array", F.slice(F.col("dpd_array"), F.col("start_idx"), 100))\
.withColumn("asset_class_array", F.slice(F.col("asset_class_array"), F.col("start_idx"), 100))\
.withColumn("das_array", F.slice(F.col("das_array"), F.col("start_idx"), 100))\
.withColumn("xarray",F.transform('REPORTEDDATEHIST', lambda x: F.months_between(F.col('run_dt'),F.trunc(F.to_date(x,format='yyyyMMdd'),"month"))))\
.drop('num_mn_hist', 'num_mn_hist1', 'start_idx')
 
 

 


# In[17]:


df2 = df1.withColumn("phist_start_date", F.expr("element_at(REPORTEDDATEHIST, 1)").cast("string"))\
.withColumn("phist_start_date", F.to_date(F.col('phist_start_date'), 'yyyyMMdd'))\
.withColumn("overdueamt", F.expr("element_at(amtoverduehist, 1)"))\
.withColumn("currentbal", F.expr("element_at(curbalhist, 1)"))\
.withColumn("currentbal", F.expr("case when currentbal < 0 then 0 else currentbal end "))\
.withColumn("disbursedamthighcredit", F.expr("element_at(highcrdhist, 1)"))\
.withColumn("reported_date", F.expr("element_at(reporteddatehist, 1)"))\
.withColumn(
    'closed_date',
    F.when(F.col('CLOSEDT').rlike(r'^\d{2}-\d{2}-\d{4}$'), F.to_date(F.col('CLOSEDT'), 'dd-MM-yyyy'))\
    .when(F.col('CLOSEDT').rlike(r'^\d{1,2}/\d{1,2}/\d{4}$'), F.to_date(F.unix_timestamp(F.col('CLOSEDT'), 'd/M/yyyy').cast('timestamp')))\
    .otherwise(None)
)\
.withColumn("closed_date", F.expr("case when closed_date >= run_dt then null else closed_date end"))\
.withColumn(
    'disbursement_date',
    F.when(F.col('DISBURSEDDT').rlike(r'^\d{2}-\d{2}-\d{4}$'), F.to_date(F.col('DISBURSEDDT'), 'dd-MM-yyyy'))\
    .when(F.col('DISBURSEDDT').rlike(r'^\d{1,2}/\d{1,2}/\d{4}$'), F.to_date(F.unix_timestamp(F.col('DISBURSEDDT'), 'd/M/yyyy').cast('timestamp')))\
    .otherwise(None)
)\
.withColumn("disbursement_date", F.expr("case when disbursement_date >= run_dt then null else disbursement_date end"))\
.filter("disbursement_date is not null")


# In[18]:


df3 = df2.withColumn("max_index",F.lit(36).cast(IntegerType()))\
.withColumn("full_sequence", F.sequence(F.lit(1), F.col("max_index")))\
.withColumn(
    "final_dpd",
    F.expr("""
        transform(full_sequence, pos -> 
            case
                when array_contains(xarray, pos) 
                then
                    element_at(dpd_array, aggregate(sequence(1, pos), 0, (acc, x) -> if(array_contains(xarray, x), acc + 1, acc)))
                else 
                    'XXX'
            end
        )
    """)
)\
.withColumn(
    "final_asset_class",
    F.expr("""
        transform(full_sequence, pos -> 
            case
                when array_contains(xarray, pos) 
                then
                    element_at(asset_class_array, aggregate(sequence(1, pos), 0, (acc, x) -> if(array_contains(xarray, x), acc + 1, acc)))
                else 
                    'XXX'
            end
        )
    """)
)\
.withColumn(
    "final_das",
    F.expr("""
        transform(full_sequence, pos -> 
            case
                when array_contains(xarray, pos) 
                then
                    element_at(das_array, aggregate(sequence(1, pos), 0, (acc, x) -> if(array_contains(xarray, x), acc + 1, acc)))
                else 
                    'XXX'
            end
        )
    """)
)\
.withColumn(
    "final_highcredit",
    F.expr("""
        transform(full_sequence, pos -> 
            case
                when array_contains(xarray, pos) 
                then
                    element_at(HIGHCRDHIST, aggregate(sequence(1, pos), 0, (acc, x) -> if(array_contains(xarray, x), acc + 1, acc)))
                else 
                    null
            end
        )
    """)
)\
.withColumn(
    "final_curbal",
    F.expr("""
        transform(full_sequence, pos -> 
            case
                when array_contains(xarray, pos) 
                then
                    element_at(CURBALHIST, aggregate(sequence(1, pos), 0, (acc, x) -> if(array_contains(xarray, x), acc + 1, acc)))
                else 
                    null
            end
        )
    """)
)\
.withColumn(
    "final_amtoverdue",
    F.expr("""
        transform(full_sequence, pos -> 
            case
                when array_contains(xarray, pos) 
                then
                    element_at(AMTOVERDUEHIST, aggregate(sequence(1, pos), 0, (acc, x) -> if(array_contains(xarray, x), acc + 1, acc)))
                else 
                    null
            end
        )
    """)
)\
.withColumn(
    "final_amtpaid",
    F.expr("""
        transform(full_sequence, pos -> 
            case
                when array_contains(xarray, pos) 
                then
                    element_at(AMTPAIDHIST, aggregate(sequence(1, pos), 0, (acc, x) -> if(array_contains(xarray, x), acc + 1, acc)))
                else 
                    null
            end
        )
    """)
)


# In[19]:


expr1 = ["*"] \
+ ["final_dpd[{}] as dpd_{}".format(i-1,i) for i in range(1,37)] \
+ ["final_asset_class[{}] as asset_class_{}".format(i-1,i) for i in range(1,37)] \
+ ["final_das[{}] as das_{}".format(i-1,i) for i in range(1,37)] \
+ ["final_highcredit[{}] as highcredit_{}".format(i-1,i) for i in range(1,37)] \
+ ["final_curbal[{}] as curbal_{}".format(i-1,i) for i in range(1,37)] \
+ ["final_amtoverdue[{}] as amtoverdue_{}".format(i-1,i) for i in range(1,37)] \
+ ["final_amtpaid[{}] as amtpaid_{}".format(i-1,i) for i in range(1,37)] 


# In[20]:


expr2 = ["*"]+["case when dpd_{i}='XXX' then 9 \
when (dpd_{i} is null) or (dpd_{i} < 0) or (dpd_{i} = '') or (dpd_{i} = '   ') then 8 \
when (dpd_{i} =0) or (dpd_{i} ='STD')  then 0 \
when dpd_{i} >=1 and dpd_{i} <30 then 1 \
when dpd_{i} >=30 and dpd_{i} <60 then 2 \
when (dpd_{i} >=60 and dpd_{i} <90)  then 3 \
when (dpd_{i} >=90 and dpd_{i} < 150)  then 4 \
when (dpd_{i} >=150 and dpd_{i} < 180)  then 5 \
when (dpd_{i} >=180 and dpd_{i} <360)  then 6 \
when (dpd_{i} >=360 and dpd_{i} <=999) then 7 \
else 8 end as dpd_bkt_int_{i}".format(i=i) for i in range(1,37)]


# In[21]:


expr3 = ["*"] \
+ ["1 as Trade"]\
+ ["case when ACCTTYPE in \
    ('Auto Loan (Personal)', \
    'Business Loan - Secured', \
    'Business Loan Against Bank Deposits', \
    'Business Loan General', \
    'Commercial Equipment Loan', \
    'Commercial Vehicle Loan', \
    'GECL Loan secured', \
    'Gold Loan', \
    'Housing Loan', \
    'Leasing', \
    'Loan Against Bank Deposits', \
    'Loan Against Shares / Securities', \
    'Microfinance Housing Loan', \
    'Pradhan Mantri Awas Yojana - CLSS', \
    'Property Loan', \
    'Secured Credit Card', \
    'Tractor Loan', \
    'Two-Wheeler Loan', \
    'Used Car Loan') \
then 1 else 0 end as SECURED"]\
+ ["case when ACCTTYPE in \
    ('Business Non-Funded Credit Facility General', \
    'Business Non-Funded Credit Facility-Priority Sector- Small Business', \
    'Business Non-Funded Credit Facility-Priority Sector-Agriculture', \
    'Business Non-Funded Credit Facility-Priority Sector-Others', \
    'Corporate Credit Card', \
    'Credit Card', \
    'Kisan Credit Card', \
    'Secured Credit Card', \
    'Overdraft', \
    'Prime Minister Jaan Dhan Yojana - Overdraft', \
    'Non-Funded Credit Facility') \
then 1 else 0 end as Revolving"] \
+ ["case when ACCTTYPE in ('Auto Lease','Auto Loan (Personal)','Tractor Loan','Used Car Loan') then 'AL' \
when ACCTTYPE in ('Loan on Credit Card','Loan to Professional','Microfinance Personal Loan','P2P Personal Loan','Personal Loan','Staff Loan','Microfinance Others') then 'PL' \
when ACCTTYPE in ('Gold Loan') then 'GL' \
when ACCTTYPE in ('Corporate Credit Card','Credit Card','Fleet Card','Kisan Credit Card','Loan Against Bank Deposits','Secured Credit Card') then 'CC' \
when ACCTTYPE in \
    ('Business Loan - Secured', \
    'Business Loan Against Bank Deposits', \
    'Business Loan General', \
    'Business Loan Priority Sector Agriculture', \
    'Business Loan Priority Sector Others', \
    'Business Loan Priority Sector Small Business', \
    'Business Loan Unsecured', \
    'Business Non-Funded Credit Facility General', \
    'Business Non-Funded Credit Facility-Priority Sector- Small Business', \
    'Business Non-Funded Credit Facility-Priority Sector-Agriculture', \
    'Business Non-Funded Credit Facility-Priority Sector-Others', \
    'GECL Loan secured', \
    'GECL Loan unsecured', \
    'Microfinance Business Loan', \
    'Mudra Loans - Shishu / Kishor / Tarun') \
then 'BL' \
when ACCTTYPE in ('Overdraft','Prime Minister Jaan Dhan Yojana - Overdraft') then 'OD'\
when ACCTTYPE in ('Consumer Loan') then 'CD'\
when ACCTTYPE in ('Housing Loan','Microfinance Housing Loan','Pradhan Mantri Awas Yojana - CLSS') then 'HL' \
when ACCTTYPE in ('Education Loan') then 'EL'\
when ACCTTYPE in ('Commercial Equipment Loan', 'Construction Equipment Loan') then 'CE'\
when ACCTTYPE in ('Property Loan') then 'LAP' \
when ACCTTYPE in ('Loan Against Shares / Securities') then 'LAS' \
when ACCTTYPE in ('Two-Wheeler Loan') then 'TW' \
when ACCTTYPE in ('Commercial Vehicle Loan') then 'CV'\
when ACCTTYPE in ('Leasing','Non-Funded Credit Facility','Other','Telco Wireless') then 'Other'\
else 'Other' end as PRODUCT_DESC"]


# In[22]:


expr4 = ["*"]\
+ ["case when dpd_bkt_int_{i} = 9 then -2 when dpd_bkt_int_{i} = 8 then -1 else dpd_bkt_int_{i} end as dpd_bkt_{i}".format(i=i) for i in range(1,37)] 


# In[23]:


df4 = df3\
.selectExpr(expr1)\
.selectExpr(expr2)\
.selectExpr(expr3)\
.selectExpr(expr4) 


# In[24]:


df4.count()


# In[25]:


df4.columns 


# In[26]:


start_time = time.time()
df4.write.mode('overwrite').parquet(tl_pre_prc_data_path)
end_time = time.time()
print("File was saved in ", (end_time - start_time) / 60, " minutes!")


# In[27]:


tl_pre_prc_data_path


# In[79]:


# ### Feature Creation


# In[28]:


df4 = spark.read.parquet(tl_pre_prc_data_path)


# In[29]:


df5 = df4\
.withColumnRenamed('creditlimitsancamt', 'CREDITLIMIT_SANCAMT')\
.withColumnRenamed('disbursedamthighcredit', 'DISBURSEDAMT_HIGHCREDIT')\
.withColumnRenamed('tenure', 'tenure_1')\
.withColumn("vintage", F.months_between("run_dt", "disbursement_date"))\
.withColumn("ext_hl_flag", F.expr("case when PRODUCT_DESC = 'HL' and lower(CREDITGRANTOR) not like '%pnb housing%' then 1 else 0 end"))


# In[30]:


expr5 = ["*"]\
+ [" + ".join('curbal_' + str(i) for i in range(1,4)) + " as total_bal_3m"]\
+ [" + ".join('curbal_' + str(i) for i in range(1,7)) + " as total_bal_6m"]\
+ [" + ".join('curbal_' + str(i) for i in range(1,10)) + " as total_bal_9m"]\
+ [" + ".join('curbal_' + str(i) for i in range(1,13)) + " as total_bal_12m"]\
+ [" + ".join('amtoverdue_'+ str(i) for i in range(1,4)) + " as total_amtoverdue_3m"]\
+ [" + ".join('amtoverdue_'+ str(i) for i in range(1,7)) + " as total_amtoverdue_6m"]\
+ [" + ".join('amtoverdue_'+ str(i) for i in range(1,10)) + " as total_amtoverdue_9m"]\
+ [" + ".join('amtoverdue_'+ str(i) for i in range(1,13)) + " as total_amtoverdue_12m"]\
+ ["case when das_{i} = 'S04' then 1 else 0 end as is_open_m{i}".format(i = i) for i in range(1,37)]\
+ ["case when PRODUCT_DESC = 'CC' and CREDITLIMIT_SANCAMT = 0 and DISBURSEDAMT_HIGHCREDIT > 0 then DISBURSEDAMT_HIGHCREDIT \
         when PRODUCT_DESC = 'CC' then coalesce(CREDITLIMIT_SANCAMT,DISBURSEDAMT_HIGHCREDIT) else null end as creditlimit"]\
+ ["case when PRODUCT_DESC != 'CC' then coalesce(DISBURSEDAMT_HIGHCREDIT,CREDITLIMIT_SANCAMT) else null end as sanctionamount"]\
+ ["DISBURSEDAMT_HIGHCREDIT as highcredit"]


# In[31]:


df6 = df5\
.selectExpr(expr5)\
.withColumn("retro_date", F.col('run_dt'))\
.withColumn("vintage_days", F.datediff(F.col('retro_date'), F.col('disbursement_date')))\
.withColumn("days_since_closed", F.datediff(F.col('retro_date'), F.col('closed_date')))\
.withColumn("days_since_phist", F.datediff(F.col('retro_date'), F.col('phist_start_date')))\
.withColumn("days_since_rpt", F.datediff(F.col('retro_date'), F.col('reported_date')))\
.withColumn("mn", F.months_between(F.trunc(F.col('retro_date'), 'month'), F.trunc(F.col('phist_start_date'), 'month')))\
.withColumn("mn_opn", F.months_between(F.trunc(F.col('retro_date'), 'month'), F.trunc(F.col('disbursement_date'), 'month')))\
.withColumn("mn_cls", F.months_between(F.trunc(F.col('retro_date'), 'month'), F.trunc(F.col('closed_date'), 'month')))\
.withColumn("mn_rpt", F.months_between(F.trunc(F.col('retro_date'), 'month'), F.trunc(F.col('reported_date'), 'month')))


# In[32]:


prods = ['PL','CC','CD','HL','LAP','BL']


# In[33]:


#EMI
expr6 = ["*"]\
+ ["mn_opn as tenure"]\
+ ["is_open_m1 as open"]\
+ ["case when days_since_rpt > 365 then 1 else 0 end as hanging_trade"]\
+ ["case when PRODUCT_DESC = '{i}' and days_since_rpt > 365 then 1 else 0 end as hanging_{i}_trade".format(i = i) for i in prods]\
+ ["case when days_since_rpt <= 365 then 1 else null end as rcnt_trd_flag"]\
+ ["case when PRODUCT_DESC != 'CC' then sanctionamount else creditlimit end as Sanctionamt_der"]\
+ ["case when Revolving = 0 then 1 else 0 end as installment"]\
+ ["case when secured = 0 then 1 else 0 end as unsecured"]\
+ ["case when PRODUCT_DESC = 'BL' and CURRENTBAL > 0 and sanctionamount > 0 and open = 1 then 0.03*sanctionamount \
         when PRODUCT_DESC = 'CC' and CURRENTBAL > 0 and creditlimit > 0 and open = 1 then 0.05*CURRENTBAL \
         when PRODUCT_DESC = 'PL' and CURRENTBAL > 0 and sanctionamount > 0 and open = 1 then 0.03*sanctionamount \
         when PRODUCT_DESC = 'HL' and CURRENTBAL > 0 and sanctionamount > 0 and open = 1 then 0.01*sanctionamount \
         when PRODUCT_DESC = 'AL' and CURRENTBAL > 0 and sanctionamount > 0 and open = 1 then 0.03*sanctionamount \
         when PRODUCT_DESC not in ('BL','CC','PL','HL','AL') and CURRENTBAL > 0 and sanctionamount > 0 and open = 1 then 0.03*sanctionamount else 0 end as EMI"]\
+[f"case when (CLOSEDT is not null and days_since_closed > {j}) or ((CURRENTBAL=0 and REVOLVING=0) and (days_since_phist>{j})) then 0 else 1 end as is_live_q{int(i/3)}".format(i=i,j=j) for i,j in [(3,90),(6,180)]]\
+[f"case when ((CLOSEDT is not null and days_since_closed > {j}) or ((CURRENTBAL=0 and REVOLVING=0) and (days_since_phist>{j}))) and PRODUCT_DESC='{prod}' \
then 0 else 1 end as {prod}_is_live_q{int(i/3)}".format(i=i, prod = prod, j=j) for i,j in [(3,90),(6,180)] for prod in ['CC', 'PL']]


# In[34]:


expr7 = ["*"]\
+["case when PRODUCT_DESC!='HL' and rcnt_trd_flag=1 then coalesce(CREDITLIMIT,greatest(SANCTIONAMOUNT,HIGHCREDIT)) else 0 end as non_mortage_sanc_amt"]\
+["case when open = 1 then OVERDUEAMT else 0 end as open_overdue"]\
+["case when PRODUCT_DESC='CC' then OVERDUEAMT else 0 end as cc_overdue"]\
+["case when PRODUCT_DESC='PL' then OVERDUEAMT else 0 end as pl_overdue"]\
+["case when PRODUCT_DESC!='CC' then OVERDUEAMT else 0 end as non_cc_overdue"]\
+["case when days_since_rpt<=90 then OVERDUEAMT else 0 end as overdue_rpt_3mn"]\
+["case when days_since_rpt<=180 then OVERDUEAMT else 0 end as overdue_rpt_6mn"]


# In[35]:


w1 = Window.partitionBy('accno','RETRO_DATE').orderBy(F.desc('rcnt_trd_flag'),F.asc('disbursement_date'),F.desc('Sanctionamt_der'), F.asc('PRODUCT_DESC'))
w2 = Window.partitionBy('accno','RETRO_DATE').orderBy(F.desc('rcnt_trd_flag'),F.desc('disbursement_date'),F.desc('Sanctionamt_der'), F.asc('PRODUCT_DESC'))
w3 = Window.partitionBy('accno','RETRO_DATE')


# In[36]:


df7 = df6\
.selectExpr(expr6)\
.withColumn("first_product", F.first(F.col('PRODUCT_DESC')).over(w1))\
.withColumn("latest_product", F.first(F.col('PRODUCT_DESC')).over(w2))\
.selectExpr(expr7)


# In[37]:


w_reopen=Window.partitionBy('accno','PRODUCT_DESC').orderBy('disbursement_date')


# In[38]:


df7=df7.withColumn('prev_closed_date',F.lag('closed_date').over(w_reopen))


# In[39]:


df7 = df7.withColumn(
    "reopened_loan",
    F.expr("CASE WHEN prev_closed_date IS NOT NULL AND disbursement_date > prev_closed_date THEN 1 ELSE 0 END")
)


# In[40]:


df7.columns


# In[93]:


# df_71.filter('PRODUCT_DESC="PL"').select('accno','reopened_loan','prev_closed_date','disbursement_date','closed_date','PRODUCT_DESC').show()


# In[41]:


expr_reopen=["*"]\
+["CASE WHEN reopened_loan = 1 AND months_between(disbursement_date, prev_closed_date) < {i} THEN 1 ELSE 0 END AS reopen_{i}m".format(i=i)
    for i in ['1','3','6','12']]\
+["CASE WHEN PRODUCT_DESC = '{prod}' and reopened_loan = 1 AND months_between(disbursement_date, prev_closed_date) < {i} THEN 1 ELSE 0 END AS reopen_{prod}_{i}m".format(i=i, prod=prod)
    for i in ['1','3','6','12'] for prod in prods]


# In[42]:


expr8 = ["*"]\
+ ["case when PRODUCT_DESC = '{prod}' then OVERDUEAMT else 0 end as {prod}_cur_overdue".format(prod = prod) for prod in prods]\
+ ["case when days_since_rpt <= {j} then OVERDUEAMT else 0 end as cur_overdue_rpt_{i}m".format(i = i, j = j) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')]]\
+ ["case when PRODUCT_DESC = '{prod}' then total_amtoverdue_{i}m/{i} else null end as avg_{prod}_overdue_rpt_{i}m".format(i = i, prod = prod) for i in ['3','6','9','12'] for prod in prods]\
+ ["case when days_since_rpt <= {j} then total_amtoverdue_{i}m/{i} else null end as avg_overdue_rpt_{i}m".format(i = i, j = j) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')]]\
+ ["case when PRODUCT_DESC = '{prod}' and days_since_rpt <= 365 then CURRENTBAL else 0 end as {prod}_cur_bal".format(prod = prod) for prod in prods]\
+ ["case when PRODUCT_DESC = '{prod}' then total_bal_{i}m/{i} else null end as avg_{prod}_bal_rpt_{i}m".format(i = i, prod = prod) for i in ['3','6','9','12'] for prod in prods]\
+ ["case when days_since_rpt <= {j} then total_bal_{i}m/{i} else null end as avg_bal_rpt_{i}m".format(i = i, j = j) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')]]\
+ ["case when PRODUCT_DESC = 'CC' and days_since_rpt <= 365 then creditlimit else null end as cc_cr_lim"]\
+ ["case when PRODUCT_DESC = '{prod}' and days_since_rpt <= 365 then greatest(sanctionamount,highcredit) else null end as {prod}_highcr".format(prod = prod) for prod in ['PL','HL']]\
+ ["case when PRODUCT_DESC = '{prod}' and days_since_rpt <= 365 then vintage_days else null end as vintage_days_{prod}".format(prod = prod) for prod in ['CC','PL','HL']]\
+ ["case when vintage_days <= {j} then 1 else null end as num_trades_open_last{i}m".format(i = i, j = j) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')]]\
+ ["case when vintage_days <= {j} and PRODUCT_DESC = '{prod}' then 1 else null end as {prod}_num_open_last{i}m".format(i = i, j = j, prod = prod) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')] for prod in prods]\
+ ["case when vintage_days <= {j} and PRODUCT_DESC = '{prod}' and greatest(sanctionamount,highcredit) <= {amt} then 1 else null end as {prod}_num_open_last{i}m_{amt1}".format(i = i, j = j, prod = prod, amt = k, amt1 = l) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')] for prod in [i for i in prods if 'CC' not in i] for k,l in [('50000','50k'),('100000','1L'),('500000','5L')]]\
+ ["case when vintage_days <= {j} and PRODUCT_DESC = 'CC' and creditlimit <= {amt} then 1 else null end as CC_num_open_last{i}m_{amt1}".format(i = i, j = j, amt = k, amt1 = l) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')] for k,l in [('50000','50k'),('100000','1L')]]\
+ ["case when open = 1 and secured = 1 then 1 else 0 end as sec_ind"]\
+ ["case when open = 1 and secured = 0 then 1 else 0 end as unsec_ind"]\
+ ["case when open = 1 then CURRENTBAL else 0 end as cur_bal_live_trades"]\
+ ["case when open = 1 then total_bal_{i}m/{i} else null end as avg_bal_live_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when open = 1 then EMI else 0 end as emi_live_trades"]\
+ ["case when open = 1 and PRODUCT_DESC = '{prod}' then CURRENTBAL else null end as live_{prod}_trades_cur_bal".format(prod = prod) for prod in prods]\
+ ["case when open = 1 and PRODUCT_DESC = '{prod}' then EMI else null end as {prod}_emi".format(prod = prod) for prod in prods]\
+ ["case when PRODUCT_DESC = '{prod}' then tenure else null end as {prod}_tenure".format(prod = prod) for prod in prods]\
+ ["case when open = 1 and PRODUCT_DESC = '{prod}' and vintage_days <= {j} then CURRENTBAL else null end as live_{prod}_trades_cur_bal_open_last{i}m".format(i = i, j = j, prod = prod) for i,j in [('3','90'),('6','180'),('9','270'),('12','365')] for prod in prods]\
+ ["case when installment = 1 and open = 1 then 1 else 0 end as live_install_trades"]\
+ ["case when installment = 1 then CURRENTBAL else 0 end as cur_bal_install_trades"]\
+ ["case when installment = 1 then total_bal_{i}m/{i} else null end as avg_bal_install_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when installment = 1 and open = 1 then CURRENTBAL else 0 end as cur_bal_live_install_trades"]\
+ ["case when installment = 1 and open = 1 then total_bal_{i}m/{i} else null end as avg_bal_live_install_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when installment = 1 then EMI else 0 end as emi_install_trades"]\
+ ["case when installment = 1 and open = 1 then EMI else 0 end as emi_live_install_trades"]\
+ ["case when installment = 1 then Sanctionamt_der else 0 end as loan_amt_install_trades"]\
+ ["case when installment = 1 and open = 1 then Sanctionamt_der else 0 end as loan_amt_live_install_trades"]\
+ ["case when revolving = 1 and open = 1 then 1 else 0 end as live_revolving_trades"]\
+ ["case when revolving = 1 then CURRENTBAL else 0 end as cur_bal_revolving_trades"]\
+ ["case when revolving = 1 then total_bal_{i}m/{i} else null end as avg_bal_revolving_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when revolving = 1 and open = 1 then CURRENTBAL else 0 end as cur_bal_live_revolving_trades"]\
+ ["case when revolving = 1 and open = 1 then total_bal_{i}m/{i} else null end as avg_bal_live_revolving_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when revolving = 1 then EMI else 0 end as emi_revolving_trades"]\
+ ["case when revolving = 1 and open = 1 then EMI else 0 end as emi_live_revolving_trades"]\
+ ["case when revolving = 1 then coalesce(creditlimit, sanctionamount) else 0 end as sanc_amt_revolving_trades"]\
+ ["case when revolving = 1 and open = 1 then coalesce(creditlimit, sanctionamount) else 0 end as sanc_amt_live_revolving_trades"]\
+ ["case when secured = 1 then EMI else 0 end as emi_secured_trades"]\
+ ["case when secured = 1 and open = 1 then EMI else 0 end as emi_live_secured_trades"]\
+ ["case when secured = 1 then highcredit else 0 end as  highcr_secured_trades"]\
+ ["case when secured = 1 and open = 1 then highcredit else 0 end as  highcr_live_secured_trades"]\
+ ["case when secured = 1 then CURRENTBAL else 0 end as cur_bal_secured_trades"]\
+ ["case when secured = 1 then total_bal_{i}m/{i} else null end as avg_bal_secured_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when secured = 1 and open = 1 then CURRENTBAL else 0 end as cur_bal_live_secured_trades"]\
+ ["case when secured = 1 and open = 1 then total_bal_{i}m/{i} else null end as avg_bal_live_secured_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when secured = 0 then EMI else 0 end as emi_unsecured_trades"]\
+ ["case when secured = 0 and open = 1 then EMI else 0 end as emi_live_unsecured_trades"]\
+ ["case when secured = 0 then highcredit else 0 end as  highcr_unsecured_trades"]\
+ ["case when secured = 0 and open = 1 then highcredit else 0 end as  highcr_live_unsecured_trades"]\
+ ["case when secured = 0 then CURRENTBAL else 0 end as cur_bal_unsecured_trades"]\
+ ["case when secured = 0 then total_bal_{i}m/{i} else null end as avg_bal_unsecured_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when secured = 0 and open = 1 then CURRENTBAL else 0 end as cur_bal_live_unsecured_trades"]\
+ ["case when secured = 0 and open = 1 then total_bal_{i}m/{i} else null end as avg_bal_live_unsecured_trades_rpt_{i}m".format(i = i) for i in ['3','6','9','12']]\
+ ["case when revolving = 1 then tenure else null end as tenure_all_revol"]\
+ ["case when installment = 1 then tenure else null end as tenure_all_install"]\
+ ["case when " + " or ".join(["dpd_bkt_" + str(i) + "=0" for i in range(1,13)]) + " then 1 else 0 end as dpd0"]\
+ ["case when " + " or ".join(["dpd_bkt_" + str(i) + ">=0" for i in range(1,13)]) + " then 1 else 0 end as dpdvalid"]\
+ ["case when " + " or ".join(["dpd_bkt_" + str(i) + ">=1" for i in range(1,13)]) + " then 1 else 0 end as dpdx"]\
+ ["case when " + " or ".join(["dpd_bkt_" + str(i) + ">=2" for i in range(1,13)]) + " then 1 else 0 end as dpd30"]\
+ ["case when " + " or ".join(["dpd_bkt_" + str(i) + ">=3" for i in range(1,13)]) + " then 1 else 0 end as dpd60"]\
+ ["case when " + " or ".join(["dpd_bkt_" + str(i) + ">=4" for i in range(1,13)]) + " then 1 else 0 end as dpd90"]\
+ ["case when " + " or ".join(["dpd_bkt_" + str(i) + ">=5" for i in range(1,13)]) + " then 1 else 0 end as dpd150"]\
+ ["case when " + " or ".join(["dpd_bkt_" + str(i) + ">=6" for i in range(1,13)]) + " then 1 else 0 end as dpd180"]\
+ ["case when " + " or ".join(["dpd_bkt_" + str(i) + ">=7" for i in range(1,13)]) + " then 1 else 0 end as dpd360"]\
+ [" + ".join(["int(dpd_bkt_" + str(i) + " ==0)" for i in range(1,13)]) + " as dpd0_counter_last12m"]\
+ [" + ".join(["int(dpd_bkt_" + str(i) + " >=0)" for i in range(1,13)]) + " as dpdvalid_counter_last12m"]\
+ [" + ".join(["int(dpd_bkt_" + str(i) + " >=1)" for i in range(1,13)]) + " as dpdx_counter_last12m"]\
+ [" + ".join(["int(dpd_bkt_" + str(i) + " >=2)" for i in range(1,13)]) + " as dpd30_counter_last12m"]\
+ [" + ".join(["int(dpd_bkt_" + str(i) + " >=3)" for i in range(1,13)]) + " as dpd60_counter_last12m"]\
+ [" + ".join(["int(dpd_bkt_" + str(i) + " >=4)" for i in range(1,13)]) + " as dpd90_counter_last12m"]\
+ [" + ".join(["int(dpd_bkt_" + str(i) + " >=3)" for i in range(1,7)]) + " as dpd60_counter_last6m"]\
+ [" + ".join(["int(dpd_bkt_" + str(i) + " >=4)" for i in range(1,7)]) + " as dpd90_counter_last6m"]\
+ ["case " + " ".join(["when dpd_bkt_" + str(i) + ">0 then dpd_bkt_" + str(i) for i in range(1,13)]) + " else null end as min_dpd_month"]\
+ ["case " + " ".join(["when dpd_bkt_" + str(i) + ">0 then dpd_bkt_" + str(i) for i in range(12,0,-1)]) + " else null end as max_dpd_month"]\
+ ["case " + " ".join(["when dpd_bkt_" + str(i) + ">=0 then dpd_bkt_" + str(i) for i in range(1,13)]) + " else null end as min_valid_dpd_month"]\
+ ["case " + " ".join(["when dpd_bkt_" + str(i) + ">1 then dpd_bkt_" + str(i) for i in range(1,13)]) + " else null end as min_dpd_month_30plus"]\
+ ["case " + " ".join(["when dpd_bkt_" + str(i) + ">2 then dpd_bkt_" + str(i) for i in range(1,13)]) + " else null end as min_dpd_month_60plus"]\
+ ["case " + " ".join(["when dpd_bkt_" + str(i) + ">3 then dpd_bkt_" + str(i) for i in range(1,13)]) + " else null end as min_dpd_month_90plus"]\
+ ["greatest(" + ", ".join(["dpd_bkt_" + str(i) for i in range(1,4)]) + ") as max_dpd_3m"]\
+ ["greatest(" + ", ".join(["dpd_bkt_" + str(i) for i in range(1,7)]) + ") as max_dpd_6m"]\
+ ["greatest(" + ", ".join(["dpd_bkt_" + str(i) for i in range(1,13)]) + ") as max_dpd_12m"]\
+["case when min_dpd_month_{i}plus is not null and min_dpd_month_{i}plus=min_valid_dpd_month then CURRENTBAL else null end as {i}plus_dlnq_bal".format(i = i) for i in ['30','60','90']]\
+["case when open=1 and min_dpd_month_{i}plus is not null and min_dpd_month_{i}plus=min_valid_dpd_month and PRODUCT_DESC='{prod}' then CURRENTBAL else null end as {prod}_live_{i}plus_dlnq_bal".format(prod =prod, i=i) for prod in ['CC','PL'] for i in ['30','60','90']]\
+["case when min_dpd_month_{i}plus is not null and min_dpd_month_{i}plus=min_valid_dpd_month and vintage_days<={k} then 1 else 0 end as {i}plus_dlnq_opn_last_{j}mn_flag".format(i = i, j = j, k=k) for i in ['30','60','90'] for j,k in [('3',90),('6',180),('9',270),('12',365)]]\
+["case when min_dpd_month_{i}plus is not null and min_dpd_month_{i}plus=min_valid_dpd_month and vintage_days<={k} and PRODUCT_DESC='{prod}' then 1 else 0 end as {prod}_{i}plus_dlnq_opn_last_{j}mn_flag".format(i = i, j = j, prod = prod, k=k) for i in ['30','60','90'] for j,k in [('3',90),('6',180),('9',270),('12',365)] for prod in ['CC','PL']]\
+ ["case when PRODUCT_DESC = 'CC' then greatest(" + ", ".join(["dpd_bkt_" + str(i) for i in range(1,13)]) + ") else null end as cc_max_dpd_ever"]\
+["case when unsecured=1 then sanctionamount end as unsecured_sanamt"]\
+["case when secured=1 then sanctionamount end as secured_sanamt"]


# In[43]:


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


# In[44]:


expr10 = ["*"]\
+["case when rcnt_trd_flag = 1 and PRODUCT_DESC='PL' and CURRENTBAL/SANCTIONAMOUNT>"+str(i)+ " then 1 else null end as pl_util_gt_"+str(int(i*100))+"_flag".format(i = i) for i in [0.25,0.5,0.75]]\
+["case when rcnt_trd_flag = 1 and PRODUCT_DESC='CC' and CURRENTBAL/coalesce(CREDITLIMIT,greatest(SANCTIONAMOUNT,HIGHCREDIT))>"+str(i)+ " then 1 else null end as cred_card_util_gt_"+str(int(i*100))+"_flag".format(i = i) for i in [0.25,0.5,0.75]]\
+ [f"case when rcnt_trd_flag = 1 and PRODUCT_DESC = '{i}' then 1 else null end as {i}_flag" for i in ['PL','HL','CC', 'BL']] \
+["case when Revolving=1 and Open=1 then CURRENTBAL else 0 end as Open_Revolving_CURRENTBAL_amt"]\
+["case when Revolving=1 and open=1 then coalesce(CREDITLIMIT,greatest(SANCTIONAMOUNT,HIGHCREDIT)) else 0 end as Revolving_open_credit_limit"]\
+["case when Installment=1 and Open=1 then CURRENTBAL else 0 end as Open_install_CURRENTBAL"]\
+["case when open=1 and dpdx=1 then emi else null end as open_dpdx_emi"]\
+["case when open=1 and dpdx=1 then CURRENTBAL else null end as open_dpdx_bal"]\
+["case when SECURED=1 then CURRENTBAL end as secured_CURRENTBAL"]\
+["case when UNSECURED=1 then CURRENTBAL end as unsecured_CURRENTBAL"]\
+["case when open=1 and PRODUCT_DESC='CC' then coalesce(CREDITLIMIT,greatest(SANCTIONAMOUNT,HIGHCREDIT)) else null end as CC_sanc_amt"]\
+["case when open=1 and PRODUCT_DESC='"+prod+"' then CURRENTBAL else null end as live_"+prod+"_bal" for prod in ['CC','PL']]\
+["case when Installment=1 and Open=1 then Sanctionamt_der else 0 end as Loan_amt_open_install"]\
+["case when open=1 and PRODUCT_DESC='PL' then SANCTIONAMOUNT else null end as PL_sanc_amt"]\
+["case when Revolving=1 then EMI else 0 end as Monthly_payment_revolving"]\
+["case when maximum_dlnq_m1>=1 then CURRENTBAL else null end as highest_dlnq_bal"]\
+["case when maximum_dlnq_m1>=1 then tenure else null end as highest_dlnq_ten"]\
+["case when latest_cc is not null and vintage_days_cc=latest_cc then dpd_bkt_2 else null end as latest_cc_dlnq_m1"]


# In[45]:


df7.columns


# In[46]:


df8 = df7\
.selectExpr(expr_reopen)\
.selectExpr(expr8)\
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
.withColumn('maximum_dlnq_m1', F.max('dpd_bkt_2').over(w3))\
.selectExpr(expr9)\
.selectExpr(expr10)


# In[47]:


agg_expr = []\
+[F.sum('non_mortage_sanc_amt').alias('non_mortage_sanc_amt')]\
+[F.sum(F.col('open_overdue')*F.col('rcnt_trd_flag')).alias('sum_live_trade_overdue')]\
+[F.sum(F.col('cc_overdue')*F.col('rcnt_trd_flag')).alias('sum_cc_overdue')]\
+[F.sum(F.col('non_cc_overdue')*F.col('rcnt_trd_flag')).alias('sum_non_cc_overdue')]\
+[F.sum(F.col('pl_overdue')*F.col('rcnt_trd_flag')).alias('sum_pl_overdue')]\
+ [F.sum(F.col(prod+"_cur_overdue") * F.col("rcnt_trd_flag")).alias("sum_"+prod+"_cur_overdue") for prod in prods]\
+ [F.sum(F.col("reopen_"+i+"m") * F.col("rcnt_trd_flag")).alias("sum_"+"reopen"+i+"m") for i in ['1','3','6','12']]\
+ [F.sum(F.col("reopen_"+prod+"_"+i+"m")).alias("sum_"+"reopen_"+prod+"_"+i+"m") for i in ['1','3','6','12'] for prod in prods]\
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
+ [F.sum(F.col("oldest_"+prod+"_cur_bal")).alias("oldest_"+prod+"_cur_bal") for prod in ['CC','PL','HL']]\
+ [F.sum(F.col("latest_"+prod+"_highcr")).alias("latest_"+prod+"_highcr") for prod in ['PL','HL']]\
+ [F.sum(F.col("latest_cc_cr_lim")).alias("latest_cc_cr_lim")]\
+ [F.sum(F.col("latest_"+prod+"_cur_bal")).alias("latest_"+prod+"_cur_bal") for prod in ['CC','PL','HL']]\
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
+ [F.sum(F.col("Trade") * F.col("rcnt_trd_flag")).alias('num_trades')]\
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
+ [F.sum(F.col("highcr_secured_trades") * F.col("rcnt_trd_flag")).alias("total_highcr_secured_trades")]\
+ [F.sum(F.col("highcr_live_secured_trades") * F.col("rcnt_trd_flag")).alias("total_highcr_live_secured_trades")]\
+ [F.sum(F.col("cur_bal_secured_trades") * F.col("rcnt_trd_flag")).alias('total_cur_bal_secured_trades')]\
+ [F.sum(F.col("avg_bal_secured_trades_rpt_"+i+"m")).alias("avg_bal_secured_trades_rpt_"+i+"m") for i in ['3','6','9','12']]\
+ [F.sum(F.col("cur_bal_live_secured_trades") * F.col("rcnt_trd_flag")).alias('total_cur_bal_live_secured_trades')]\
+ [F.sum(F.col("avg_bal_live_secured_trades_rpt_"+i+"m")).alias("avg_bal_live_secured_trades_rpt_"+i+"m") for i in ['3','6','9','12']]\
+ [F.sum(F.col("emi_unsecured_trades") * F.col("rcnt_trd_flag")).alias("total_emi_unsecured_trades")]\
+ [F.sum(F.col("emi_live_unsecured_trades") * F.col("rcnt_trd_flag")).alias("total_emi_live_unsecured_trades")]\
+ [F.sum(F.col("highcr_unsecured_trades") * F.col("rcnt_trd_flag")).alias("total_highcr_unsecured_trades")]\
+ [F.sum(F.col("highcr_live_unsecured_trades") * F.col("rcnt_trd_flag")).alias("total_highcr_live_unsecured_trades")]\
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
+[F.sum(F.col('Open_Revolving_CURRENTBAL_amt')*F.col('rcnt_trd_flag')).alias('total_bal_live_revol_trades')]\
+[F.sum(F.col('Revolving_open_credit_limit')*F.col('rcnt_trd_flag')).alias('total_cr_lim_live_revol_trades')]\
+[F.sum(F.col('Open_install_CURRENTBAL')*F.col('rcnt_trd_flag')).alias('live_install_trades_bal')]\
+[F.sum(F.col("EMI")*F.col('rcnt_trd_flag')).alias("sum_emi")]\
+[F.sum(F.col("CURRENTBAL")*F.col('rcnt_trd_flag')).alias("total_outstanding_bal")]\
+[F.sum(F.col("open_dpdx_emi")*F.col('rcnt_trd_flag')).alias("live_dpdx_emi")]\
+[F.sum(F.col("open_dpdx_bal")*F.col('rcnt_trd_flag')).alias("live_dpdx_bal")]\
+[F.sum(F.col("secured_CURRENTBAL")*F.col('rcnt_trd_flag')).alias("sum_secured_CURRENTBAL")]\
+[F.sum(F.col("unsecured_CURRENTBAL")*F.col('rcnt_trd_flag')).alias("sum_unsecured_CURRENTBAL")]\
+[F.sum(F.col('CC_sanc_amt')*F.col('rcnt_trd_flag')).alias('live_CC_sanc_amt')]\
+[F.sum(F.col('live_cc_bal')*F.col('rcnt_trd_flag')).alias('total_bal_live_cc_trades')]\
+[F.sum(F.col('live_pl_bal')*F.col('rcnt_trd_flag')).alias('total_bal_live_pl_trades')]\
+[F.sum(F.col(prod+i+'plus_dlnq_bal')*F.col('rcnt_trd_flag')).alias(prod+i+'plus_dlnq_bal') for i in ['30','60','90'] for prod in ['','cc_live_','pl_live_']]\
+[F.sum(F.col(prod+i+'plus_dlnq_opn_last_'+j+'mn_flag')*F.col('rcnt_trd_flag')).alias('num_'+prod+i+'plus_dlnq_opn_last_'+j+'mn') for i in ['30','60','90'] for j in ['3','6','9','12'] for prod in ['CC_','PL_','']]\
+[F.sum(F.col('Loan_amt_open_install')*F.col('rcnt_trd_flag')).alias('live_install_trades_sanc_amt')]\
+[F.sum(F.col('PL_sanc_amt')*F.col('rcnt_trd_flag')).alias('live_PL_sanc_amt')]\
+[F.sum('overdue_rpt_3mn').alias('sum_overdue_rpt_3mn')]\
+[F.sum('overdue_rpt_6mn').alias('sum_overdue_rpt_6mn')]\
+[F.expr(f"sum(is_open_m{i}) as num_concurrent_live_loans_m{i}") for i in [1,4]]\
+[F.expr(f"sum(is_open_m{i}*{flag}) as num_{flag}_concurrent_live_loans_m{i}") for i in [1,4] for flag in ['secured','unsecured','installment','revolving']]\
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
+ [F.max(F.col("vintage") * F.col(f"{i}_flag")).alias(f"mn_since_oldest_{i}_trd") for i in ['hl', 'pl']] \
+ [F.min(F.col("vintage") * F.col(f"{i}_flag")).alias(f"mn_since_latest_{i}_trd") for i in ['hl', 'pl']] \
+ [F.max(F.col("cc_flag") * F.col("curbal_1")).alias("latest_bal_top_wallet_card")] \
+ [F.sum(F.col("cc_flag") * F.col("curbal_1")).alias("latest_cc_bal")] \
+ [F.sum(F.col("cc_flag") * F.col("highcredit_1")).alias("latest_cc_cr_lmt")] \
+ [F.max(F.col("ext_hl_flag")).alias("ext_hl_flag")] \
+ [F.sum(F.col("cc_flag") * F.col(f"curbal_{i}")).alias(f"cc_bal{i}") for i in range(1,13)] \
+ [F.max(f"{i}_flag").alias(f"{i}_flag") for i in ['hl', 'pl', 'cc']]


# In[48]:


df8.columns


# In[49]:


df9 = df8.groupby("accno", "loan_month").agg(*agg_expr)


# In[50]:


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
+ ["case when total_outstanding_bal is null or total_outstanding_bal=0 then -1 when sum_secured_CURRENTBAL is null then 0 else sum_secured_CURRENTBAL/total_outstanding_bal end as prop_sec_bal"]\
+ ["case when total_outstanding_bal is null or total_outstanding_bal=0 then -1 when sum_unsecured_CURRENTBAL is null then 0 else sum_unsecured_CURRENTBAL/total_outstanding_bal end as prop_unsec_bal"]\
+ ["case when sum_secured_CURRENTBAL is null then -1 when sum_unsecured_CURRENTBAL is null then -2 when sum_unsecured_CURRENTBAL = 0 then -3 else sum_secured_CURRENTBAL/sum_unsecured_CURRENTBAL end as ratio_secured_unsecured_CURRENTBALs"]\
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


# In[51]:


expr12 = ["*"] \
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


# In[52]:


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
+ [f"non_bl_bal_dec_ind_{i}" for i in range(1,36)]


# In[53]:


df10 = df9\
.selectExpr(expr11)\
.selectExpr(expr12)\
.withColumn("max_cc_bal_12mn", F.greatest(*[F.col(c) for c in [f'cc_bal{i}' for i in range(1,13)]]))\
.withColumn("cc_bal_array", F.array([F.col(c) for c in [f'cc_bal{i}' for i in range(1, 13)]]))\
.withColumn("mn_since_max_cc_bal", F.expr("array_position(cc_bal_array, max_cc_bal_12mn)"))\
.drop(*rm_cols)


# In[54]:


df10.columns


# In[55]:


df10=df10.select([F.col(col).alias(col.lower()) for col in df10.columns])


# In[56]:


tl_feat_path


# In[57]:


start_time = time.time()
df10.write.mode('overwrite').parquet(tl_feat_path)
end_time = time.time()
print("File was saved in ", (end_time - start_time) / 60, " minutes!")


# In[58]:


df10.count(),len(df10.columns)


# In[60]:


# df10.columns
df_trade=spark.read.parquet(tl_feat_path)


# In[62]:


df_trade.columns


# In[67]:


df_trd_pre.columns


# In[68]:


df_trd_pre.groupBy("LOS-APP-ID").count().show()

