from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("DS200_Lab4").getOrCreate()

# Đường dẫn dữ liệu
data_path = "data"

# Đọc Orders
orders = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Orders.csv")

# Tên cột thời gian mua có định dạng '2023-11-18 19:28'
purchase_time_col = "Order_Purchase_Timestamp"


# Chuyển cột về kiểu timestamp với format rõ ràng
orders = orders.withColumn(
    "_purchase_ts",
    F.to_timestamp(F.col(purchase_time_col), "yyyy-MM-dd HH:mm")
)

# Loại bỏ những dòng không parse được hoặc NULL
orders = orders.filter(F.col("_purchase_ts").isNotNull())

# Tạo cột year và month từ cột timestamp
orders = orders.withColumn("year", F.year(F.col("_purchase_ts")))
orders = orders.withColumn("month", F.month(F.col("_purchase_ts")))

# Đếm số dòng cho mỗi nhóm year, month
# Dùng count("*") vì mỗi dòng trong Orders là một đơn hàng
result = (
    orders.groupBy("year", "month")
    .agg(F.count(F.lit(1)).alias("num_rows"))
    .orderBy(F.asc("year"), F.desc("month"))
)

print("\nKẾT QUẢ CÂU 4: Nhóm đơn hàng theo năm tăng dần, tháng giảm dần")
result.show(200, truncate=False)

