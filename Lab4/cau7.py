from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("DS200_Lab4").getOrCreate()

# Đường dẫn dữ liệu
data_path = "data"

order_items = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Order_Items.csv")
orders = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Orders.csv")
products = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Products.csv")
order_reviews = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Order_Reviews.csv")


orders_with_score = orders.join(
    order_reviews.select("Order_ID", "Review_Score"),
    on = "Order_ID",
    how = "inner"
)

order_items_with_name = order_items.join(
    products.select("Product_Category_Name", "Product_ID"),
    on = "Product_ID",
    how = "inner"
)

result_1 = order_items_with_name.groupBy("Product_Category_Name").agg(
    F.count("Order_ID").alias("Number_of_Orders")
).orderBy(F.col("Number_of_Orders").desc())


print("San pham duoc mua nhieu nhat la:")
result_1.show(1,truncate=False)

order_items_with_name_and_score = order_items_with_name.join(
    orders_with_score.select("Review_Score", "Order_ID"),
    on = "Order_ID",
    how = "inner"
)

result_2 = order_items_with_name_and_score.groupBy("Product_Category_Name").agg(
    F.avg("Review_Score").alias("Average_Review_Score")
).orderBy(F.col("Average_Review_Score").desc())

result_2.show(truncate=False)
raise()

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