from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col


spark = SparkSession.builder.appName("DS200_Lab4").getOrCreate()


data_path = "data"


orders = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Orders.csv")
customers = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Customer_List.csv")

# BƯỚC 1: JOIN hai bảng Orders và Customers
orders_with_country = orders.join(
    customers.select("Customer_Trx_ID", "Customer_Country"),
    on="Customer_Trx_ID",
    how="inner"
)

# BƯỚC 2: Nhóm theo quốc gia (GROUP BY) và đếm số đơn hàng
order_count_by_country = orders_with_country.groupBy("Customer_Country").agg(
    count("Order_ID").alias("Number_of_Orders")
)

# BƯỚC 3: Sắp xếp theo số lượng đơn hàng giảm dần
order_count_by_country_sorted = order_count_by_country.orderBy(
    col("Number_of_Orders").desc()
)

# BƯỚC 4: Hiển thị kết quả
# Sử dụng show(truncate=False) để hiển thị toàn bộ dữ liệu không cắt bớt
print("\n")
order_count_by_country_sorted.show(truncate=False)

