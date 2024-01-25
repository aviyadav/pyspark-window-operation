from pyspark.sql import SparkSession
from pyspark.sql import Window, functions as F

spark = SparkSession.builder.getOrCreate()

data = spark.read.csv("datasets/sample_sales_pyspark.csv", header=True)

# data.show(15)

# example-1
window1 = Window.partitionBy("store_code", "product_code").orderBy("sales_date")

data1 = data.withColumn("total_sales", F.sum("sales_qty").over(window1))

data1 \
    .filter((F.col("store_code")=="B1")) \
    .select("store_code", "product_code", "sales_date", "sales_qty", "total_sales") \
    .show(30)

# example-2

data2 = data.withColumn("max_price", F.max("price").over(window1))

data2 \
    .filter((F.col("store_code") == "B1")) \
    .select("store_code", "product_code", "sales_date", "price", "max_price") \
    .show(15)


# example-3

# previous day sales qty
data3 = data.withColumn("prev_day_sales_lag", F.lag("sales_qty", 1).over(window1))
data3 = data3.withColumn("prev_day_sales_lead", F.lead("sales_qty", -1).over(window1))



data3 \
    .filter((F.col("store_code") == "A1") & (F.col("product_code") == "95955")) \
    .select("sales_date", "sales_qty", "prev_day_sales_lag", "prev_day_sales_lead") \
    .show(15)

# example-4

window2 = Window \
    .partitionBy("store_code", "product_code") \
    .orderBy("sales_date") \
    .rowsBetween(-3, -1)

data4 = data.withColumn("last_3_day_avg", F.mean("sales_qty").over(window2))

data4 \
    .filter((F.col("store_code") == "A1") & (F.col("product_code") == "95955")) \
    .select("sales_date", "sales_qty", "last_3_day_avg") \
    .show()

# example-5

window3 = Window \
    .partitionBy("store_code", "product_code") \
    .orderBy("sales_date") \
    .rowsBetween(Window.unboundedPreceding, -1)
    # .rowsBetween(Window.unboundedPreceding, Window.currentRow)

data5 = data.withColumn("cumulative_mean", F.mean("sales_qty").over(window3))

data5 \
    .filter((F.col("store_code")=="A1") & (F.col("product_code")=="95955")) \
    .select("sales_date", "sales_qty", "cumulative_mean") \
    .show()