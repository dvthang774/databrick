# Databricks notebook source
# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables/

# COMMAND ----------

# DBTITLE 1,Load dataset: containing information about orders on Lazada, where each row represents a specific order. 
# MAGIC %python
# MAGIC path_data_table="dbfs:/FileStore/tables/fake_dataset_miley_lazada_sheet.csv"
# MAGIC df = spark.read.format("csv").option("header", "true").option("inferSchema", "true") .load(path_data_table)
# MAGIC df.cache()
# MAGIC df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("lazada")
sqlContext.sql('select * from lazada')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lazada
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from lazada where createTime is null

# COMMAND ----------

# DBTITLE 1,Selecting the necessary columns from the dataset.
df_ = df.select(['deliveryType','createTime','updateTime','orderNumber','deliveredDate','customerName','customerEmail','shippingName','shippingAddress','shippingAddress3','shippingAddress4','shippingAddress5','billingPhone','payMethod','unitPrice','sellerDiscountTotal','shippingFee','status','buyerFailedDeliveryReturnInitiator','buyerFailedDeliveryReason','sellerNote'])

# COMMAND ----------

display(df_)
df_.count()
df_.createOrReplaceTempView("lazada2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select deliveryType, count(*) as num from lazada2 group by deliveryType
# MAGIC

# COMMAND ----------

# DBTITLE 1,Check and Fill na
# MAGIC %sql
# MAGIC select * from lazada2 where deliveryType is null

# COMMAND ----------

# get all columns
columns_to_check = df_.columns
# Replace "nan" and "NaN" values with None
df_nan = df_.na.replace(["nan", "NaN", "NaT"], [None, None, None], columns_to_check)
display(df_nan)

# COMMAND ----------

df_ = df_.filter(df_['deliveryType'].isNotNull())
df_.show()
df_.createOrReplaceTempView("lazada2")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select deliveryType, count(*) as num from lazada2 group by deliveryType
# MAGIC select * from lazada2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lazada2 where createTime is null

# COMMAND ----------

df_ = df_.filter(df_['createTime'].isNotNull())
df_.show()
df_.createOrReplaceTempView("lazada2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lazada2 where createTime is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lazada2 where orderNumber is null

# COMMAND ----------

df_ = df_.filter(df_['orderNumber'].isNotNull())
df_.show()
df_.createOrReplaceTempView("lazada2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lazada2 where buyerFailedDeliveryReason is null

# COMMAND ----------

df_.summary().display()

# COMMAND ----------

df_ = df_.na.fill("",['customerName'])\
        .na.fill("",["customerEmail"])\
        .na.fill("01 Jan 2000 00:00",["deliveredDate"])\
        .na.fill("",["sellerNote"])\
        .na.fill("",["buyerFailedDeliveryReason"])\
        .na.fill("",["buyerFailedDeliveryReturnInitiator"])\
        .na.fill("",["payMethod"])\
        .na.fill("",["status"])\
        .na.fill(3500,["shippingFee"])\
        .na.fill(155000,["unitPrice"])\
        .na.fill(-28000,["sellerDiscountTotal"])
df_.createOrReplaceTempView("lazada2")


# COMMAND ----------

df_.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select payMethod,count(*) from lazada2 group by payMethod

# COMMAND ----------

# DBTITLE 1,Rename columns, GroupBy, Max and Sum for dataframe
import pyspark.sql.functions as F
from pyspark.sql.functions import sum, max

df2 = df_.select(['deliveryType','createTime','updateTime','orderNumber','deliveredDate','customerName','customerEmail','shippingName','shippingAddress','shippingAddress3','shippingAddress4','shippingAddress5','billingPhone','payMethod','unitPrice','sellerDiscountTotal','shippingFee','status','buyerFailedDeliveryReturnInitiator','buyerFailedDeliveryReason','sellerNote'])\
    .groupBy('orderNumber')\
        .agg(
            max("status").alias("status"),\
            max("buyerFailedDeliveryReturnInitiator").alias("Refund_status"),\
            max("deliveryType").alias("Shipping_method"),\
            max('buyerFailedDeliveryReason').alias('Cancellation_reason'),\
            max("deliveredDate").alias("delivered_date"),\
            sum('unitPrice').alias('total_unit_price'),\
            max("payMethod").alias("Payment_method"),\
            sum('sellerDiscountTotal').alias('Total_discount'),\
            sum('shippingFee').alias('shipping_fee'),\
            max("shippingAddress").alias("Street"),\
            max('shippingAddress3').alias('city'),\
            max('shippingAddress4').alias('District'),\
            max('shippingAddress5').alias('Ward'),\
            max('customerName').alias('source_customer_id'),\
            max('customerEmail').alias('email'),\
            max('billingPhone').alias('phone'),\
            max('shippingName').alias('customer_name'),\
            max('createTime').alias('created_at'),\
            max('updateTime').alias('updated_at'),\
            max('sellerNote').alias('note'),\
            max('billingPhone').alias('customer_unified_key'),\
        )
# df2.display()
# df2.createOrReplaceTempView("lazada")


# COMMAND ----------

display(df2)
df2.count()

# COMMAND ----------

from pyspark.sql.functions import expr

# a = df2['total_unit_price']
# b = df2['shipping_fee']
# df2['total_unit_price']
df2 = df2.withColumnRenamed('orderNumber','source_order_id')
df2 = df2.withColumn("Total_payment", expr("total_unit_price + shipping_fee"))
df2.printSchema()
df2.display()
df2.summary().display()

# COMMAND ----------

# DBTITLE 1, Fix phone columns: add 0 number
from pyspark.sql.functions import col, to_date, lit, lpad

df3 = df2.withColumn("phone",lpad(col("phone").cast("string"), 10, "0"))
        
df3.display()

# COMMAND ----------

# DBTITLE 1, Re-format timestamp column
from datetime import datetime
from pyspark.sql.functions import to_date, to_timestamp, lit, col, udf
from pyspark.sql.types import TimestampType
import pandas as pd

# udf_format = udf( lambda x: datetime.strptime(x,"%d %B %Y"), DataType())
# df3_ = df3.withColumn('delivered_date', udf_format(df3['delivered_date']))

df3_=df3.withColumn("delivered_date",to_timestamp(df3['delivered_date'], 'dd MMM yyyy HH:mm'))\
        .withColumn("created_at",to_timestamp(df3['created_at'], 'dd MMM yyyy HH:mm'))\
        .withColumn("updated_at",to_timestamp(df3['updated_at'], 'dd MMM yyyy HH:mm'))
df3_.display()

# COMMAND ----------

# DBTITLE 1,Create purchase_order_id column using uuid()
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())
df_with_uuid = df3_.withColumn("purchase_order_id",uuidUdf())
df_with_uuid.display()

# COMMAND ----------

from pyspark.sql.functions import lit,date_format

df_with_uuid = df_with_uuid.withColumn("date_key",to_date(df_with_uuid['created_at'], 'ddMMMyyyyHHmm'))
df_with_uuid.display()

# COMMAND ----------

from pyspark.sql.functions import lit
purchase_order = df_with_uuid.withColumn("state", lit(None))\
                             .withColumn("shipping_description", lit(None))\
                             .withColumn("shipping_code", lit(None))\
                             .withColumn("shipping_provider", lit(None))\
                             .withColumn("Feedback", lit(None))\
                             .withColumn("remote_ip", lit(None))\
                             .withColumn("customer_note", lit(None))\
                             .withColumn("year", lit(None))\
                             .withColumn("month", lit(None))\
                             .withColumn("day", lit(None))\
                             .withColumn("source_name", lit(None))\
                            
purchase_order.display()

# COMMAND ----------

purchase_order = purchase_order.select('purchase_order_id','source_order_id','state','status','Refund_status','shipping_description','shipping_code','shipping_provider','Shipping_method','Cancellation_reason','Feedback','delivered_date','total_unit_price','Payment_method','shipping_fee','Total_payment','Total_discount','Street','city', 'District','Ward','source_customer_id','email','phone','customer_name','remote_ip','customer_note','created_at','updated_at','note','year','month','day','source_name','customer_unified_key','date_key')
purchase_order.display()

# COMMAND ----------

from pyspark.sql.types import StringType
# purchase_order.createOrReplaceTempView("lazada3")
purchase_order.withColumn('state', lit(None).cast('string'))
purchase_order.write.format("csv").option("header", "true").save("lazada_clean2.csv", quote='', escape='\"', sep='|', header='True', nullValue=None)

# COMMAND ----------

# DBTITLE 1,#Task1: the number of orders canceled by each reason.
# MAGIC
# MAGIC %sql 
# MAGIC select Cancellation_reason, count(*) as number from lazada3 group by Cancellation_reason

# COMMAND ----------

# DBTITLE 1,# -- Task2: the 20 most ordered customers.
# MAGIC
# MAGIC %sql 
# MAGIC select customer_name,  count(*)  as number from lazada3 group by customer_name  order by number desc limit 20

# COMMAND ----------

# DBTITLE 1,# --Task 3: The number of order and total purchase value by province
# MAGIC
# MAGIC %sql 
# MAGIC select city, count(*) as Summarize, sum(Total_payment) as total from lazada3 group by city order by total desc;

# COMMAND ----------

# DBTITLE 1,# -- Task 4: The number of order by years.
# MAGIC
# MAGIC %sql
# MAGIC select year(date_key) as Order_by_year, count(*) as total from lazada3 group by Order_by_year order by Order_by_year desc

# COMMAND ----------

# DBTITLE 1,# -- Task 5: the number of orders at the hours of the day.
# MAGIC
# MAGIC %sql
# MAGIC select hour(created_at) as Hour_order, count(*) as number_order from lazada3 group by Hour_order order by Hour_order asc

# COMMAND ----------

# DBTITLE 1,# -- Task 6: The top 10 most chosen payment methods
# MAGIC
# MAGIC %sql
# MAGIC select Payment_method, count(*) as total from lazada3 group by Payment_method order by total desc limit 10

# COMMAND ----------

# DBTITLE 1,# -- Task 7: scatter plot of total price vs total discount in purchase orders.
# MAGIC
# MAGIC %sql
# MAGIC select Total_payment, Total_discount from lazada3

# COMMAND ----------

# DBTITLE 1,# -- Task 8. Display the order cancellation rate by refunding obiects.
# MAGIC
# MAGIC %sql 
# MAGIC select Refund_status, count(*) as number from lazada3 group by Refund_status

# COMMAND ----------

# MAGIC %sql
# MAGIC select concat(month(created_at)+year(created_at) ) as Month_order, count(*) as number_order from lazada3 group by Month_order order by Month_order asc

# COMMAND ----------

# DBTITLE 1,# -- Task 9. Generate a line chart showing the number of orders per month (2020-2022).
# MAGIC
# MAGIC %sql
# MAGIC select month(delivered_date) as month, sum(case when year(delivered_date) = '2020' then 1 else 0 end) as sum_order_2020, sum(case when year(delivered_date) = '2021' then 1 else 0 end) as sum_order_2021,sum(case when year(delivered_date) = '2022' then 1 else 0 end) as sum_order_2022 from lazada3 group by month(delivered_date) 
# MAGIC -- select date_format(created_at,'yyyy-MM' ) as orders_per_month, count(*) as number from lazada3 group by orders_per_month order by orders_per_month asc

# COMMAND ----------

# DBTITLE 1, Convert the clean data into a delta table by path.
delta_table_path="/tmp/delta-table"
purchase_order.write.format("delta").save(delta_table_path)

# COMMAND ----------

# DBTITLE 1,#Task 1
delta_df = spark.read.format("delta").load(delta_table_path)
delta_df.createOrReplaceTempView('sourceTable')
delta_df.write.format("csv").mode('overwrite').save("/tmp/sample")
# delta_df.display()
# delta_df.display()
# delta_df.write.format("csv").option("header", "true").save("lazada_clean.csv")

# COMMAND ----------

# Import the necessary libraries
from delta.tables import DeltaTable

delta_table_path="/tmp/delta-table"
# Load the Delta table
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Specify the path where you want to save the CSV file
csv_output_path = "/tmp/sample2.csv"

# Use the .toDF() method to convert the Delta table to a DataFrame
df = delta_table.toDF()

# Write the DataFrame as a CSV file
df.write.format("csv").option("header", "true").save(csv_output_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sourceTable;
# MAGIC

# COMMAND ----------

# DBTITLE 1,#Task 2
from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")

deltaTable.update(
  condition = "status = 'delivered'",
  set = { "note": "'Giao hàng thành công'" }
)
deltaTable.toDF().display()

# COMMAND ----------

new_delta_df = sqlContext.sql('select * from sourceTable order by rand() limit 2')
# new_delta_table.write.format("delta").save("/tmp/new-delta-table")
# new_delta_df.createOrReplaceTempView('targetTable')
new_delta_df.show()

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *

deltaTable = DeltaTable.forPath(spark, "dbfs:/tmp/new-delta-table")
deltaTable.toDF().show()
# condition_update = expr(new_delta_df["purchase_order_id"] == deltaTable['purchase_order_id']) & (new_delta_df["status"] =='delivered'))
deltaTable.alias("oldData") \
.merge(
    new_delta_df.alias("newData"),
    'oldData.purchase_order_id=newData.purchase_order_id')\
.whenMatchedUpdateAll()\
.whenNotMatchedInsertAll()\
.whenNotMatchedBySourceDelete()\
.execute()

new_delta_table.toDF().show()

# COMMAND ----------

new_delta_table = spark.read.format("delta").load("/tmp/new-delta-table")
new_delta_table.createOrReplaceTempView('targetTable')
new_delta_table.display()   

# COMMAND ----------

delta_df.alias("oldData") \
  .merge(
    new_delta_table.alias("newData"),
    "oldData.purchase_order_id = newData.purchase_order_id") \
  .whenMatchedUpdate(set = { "Refund_status": 'No refund' }) \
  .whenNotMatchedInsert(values = { "Refund_status": 'Loading...' }) \
  .execute()

new_delta_table.toDF().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO targetTable target
# MAGIC USING sourceTable source
# MAGIC ON source.purchase_order_id = target.purchase_order_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   DELETE

# COMMAND ----------


from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, 'dbfs:/tmp/new-delta-table')
deltaTable.delete("date-key < '2022-01-01' or status != 'delivered'")
deltaTable.toDF().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY 'dbfs:/tmp/delta-table'
# MAGIC

# COMMAND ----------

from delta.tables import *

pathToTable = 'dbfs:/tmp/delta-table'
deltaTable = DeltaTable.forPath(spark, pathToTable)

fullHistoryDF = deltaTable.history() 
fullHistoryDF.show()

# COMMAND ----------

# DBTITLE 1,Task 7+8: Read data from a specific version and timestamp version
df_timestamp = spark.read.format('delta').option("timestampAsOf", "2023-09-14").load(pathToTable)
df_timestamp.show()

# COMMAND ----------

df_specific_version = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table")
df_specific_version.show()

# COMMAND ----------

# DBTITLE 1,#Task 9: Perform SCD type 2
from delta.tables import *
from pyspark.sql.functions import *

df_specific_version = df_specific_version.withColumn('is_current', lit(1))
init_load_ts = datetime.strptime(
    '2022-01-01 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f')
high_ts = datetime.strptime(
    '9999-12-31 23:59:59.999', '%Y-%m-%d %H:%M:%S.%f')
df_specific_version = df_specific_version.withColumn('effective_ts', lit(init_load_ts))
df_specific_version = df_specific_version.withColumn('expiry_ts', lit(high_ts))
df_specific_version.show(truncate=False)


df_specific_version.write.format('delta').save('/tmp/delta-table-scd2')


# COMMAND ----------

df_scd2 = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table-scd2")
df_scd2.createOrReplaceTempView('scd2')
# spark.sql('SELECT * FROM scd2')


# COMMAND ----------

# df_scd2 = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table-scd2")
# df_scd2.createOrReplaceTempView('scd2')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2

# COMMAND ----------

current_table = sqlContext.sql('select * from scd2')
current_table.write.format('delta').save('/tmp/current-table')
history_table.current_table.write.format('delta').save('/tmp/history-table')

# COMMAND ----------

`# df3 = df3.replace( {'null': None } )

# # from pyspark.sql.functions import col,when
# # df3=df2.select([when(col(c)=="null",None).otherwise(col(c)).alias(c) for c in df2.columns])

# df3 = df3.fillna(0, subset=['status'])
# # df3.summary().display()
# df3.show()

# COMMAND ----------

# dbutils.fs.rm("dbfs:/FileStore/tables/fake_dataset_miley_lazada_sheet-1.csv") delete data

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables/

# COMMAND ----------


