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


order_items_date = order_items.join(
    orders.select("Order_purchase_timestamp", "Order_ID"),
    on = "Order_ID",
    how = "inner"
)   

order_items_date = order_items_date.filter(F.year(F.col("Order_purchase_timestamp")) == 2024)
order_items_date.show(truncate=False)
order_items_result = order_items_date.join(
    products.select("Product_ID", "Product_Category_Name"),
    on = "Product_ID",
    how = "inner"
)

order_items_result = order_items_result.groupBy("Product_Category_Name").agg(
    F.sum("Total_Price").alias("Total_Sales")
).orderBy(F.col("Total_Sales").desc())

order_items_result.show(truncate=False)     