from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("DS200_Lab4").getOrCreate()

# Đường dẫn dữ liệu
data_path = "data"

# Đọc file Order_Reviews.csv
reviews = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Order_Reviews.csv")

clean_reviews = reviews.filter(
    (F.col("Review_Score").isNotNull()) &
    (F.col("Review_Score").rlike("^[0-9]+$"))
)
# Kiểm tra cột Review_Score tồn tại
score_col = "Review_Score"

# Chuyển cột Review_Score sang kiểu số nguyên an toàn trước khi so sánh
reviews = reviews.withColumn(score_col, F.col(score_col).cast("int"))

# Tính điểm trung bình của Review_Score
average_score = clean_reviews.agg(F.avg(score_col).alias("avg_review_score")).collect()[0][0]

print("\nKẾT QUẢ CÂU 5: Review_Score")
if average_score is not None:
    print(f"Điểm đánh giá trung bình (Review_Score 1-5): {average_score:.2f}")
else:
    print("Không có dữ liệu hợp lệ để tính điểm trung bình.")

# Nhóm theo Review_Score và đếm số lượng dòng
score_distribution = (
    clean_reviews.groupBy(score_col)
    .agg(F.count(F.lit(1)).alias("count"))
    .orderBy(F.col(score_col).asc())
)

print("\nSố lượng đánh giá theo từng mức Review_Score:")
score_distribution.show(truncate=False)
