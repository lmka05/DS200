from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("DS200_Lab4").getOrCreate()

# Đường dẫn dữ liệu
data_path = "data"

order_items = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Order_Items.csv")
orders = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Orders.csv")
products = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Products.csv")

order_items = order_items.withColumn("Total_Price", F.col("Price") + F.col("Freight_Value"))

result_1 = order_items.groupBy("Seller_ID").agg(
    F.sum("Total_Price").alias("Total_Sales")
).orderBy(F.col("Total_Sales").desc())

print("Xep hang cac Seller theo doanh thu:")
result_1.show(truncate=False)

result_2 = order_items.groupBy("Seller_ID").agg(
    F.count("Order_ID").alias("Number_of_Orders")
).orderBy(F.col("Number_of_Orders").desc())

print("So luong don hang cua moi Seller:")
result_2.show(truncate=False)
raise()
