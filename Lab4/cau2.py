from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

spark = SparkSession.builder.appName("DS200_Lab4").getOrCreate()

data_path = "data"

orders = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Orders.csv")
customers = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Customer_List.csv")
order_items = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Order_Items.csv")
products = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Products.csv")
reviews = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Order_Reviews.csv")

# 1. Tính tổng số đơn hàng
total_orders = orders.count()

# 2. Tính số lượng khách hàng duy nhất
unique_customers = customers.select(countDistinct("Customer_Trx_ID")).collect()[0][0]

# 3. Tính số lượng người bán duy nhất
unique_sellers = order_items.select(countDistinct("Seller_ID")).collect()[0][0]

# In kết quả
print(f"Tổng số đơn hàng:        {total_orders}")
print(f"Số lượng khách hàng:     {unique_customers}")
print(f"Số lượng người bán:      {unique_sellers}")
