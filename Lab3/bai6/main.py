from pyspark import SparkContext, SparkConf
from datetime import datetime

sc = SparkContext.getOrCreate()

# Hàm chuyển timestamp Unix -> year
def get_year_from_timestamp(ts):
    return datetime.fromtimestamp(int(ts)).year


# Mỗi dòng: UserID, MovieID, Rating, Timestamp
ratings_rdd = sc.textFile("hdfs://127.0.0.1:9000/ratings")

# Output sẽ vẫn là raw string
print("Step 1 - ratings_rdd:")
print(ratings_rdd.take(5))


# 2. Parse thành (Year, (rating, 1))
def parse_rating_year(line):
    parts = line.split(",")
    rating = float(parts[2])
    timestamp = parts[3]
    year = get_year_from_timestamp(timestamp)
    return (year, (rating, 1))

year_rating_rdd = ratings_rdd.map(parse_rating_year)

# Output:
# (2020, (4.5, 1))
print("\nStep 2 - year_rating_rdd:")
print(year_rating_rdd.take(5))


# 3. Reduce theo năm
# Kết quả: (Year, (total_rating, total_count))
def reduce_sum_count(a, b):
    return (a[0] + b[0], a[1] + b[1])

year_sum_count_rdd = year_rating_rdd.reduceByKey(reduce_sum_count)

# Output:
# (2020, (120.5, 31))
print("\nStep 3 - year_sum_count_rdd:")
print(year_sum_count_rdd.take(10))


# 4. Tính avg rating theo năm
# Kết quả: (Year, total_count, avg_rating)
def final_format(row):
    year, (total_rating, total_count) = row
    avg_rating = total_rating / total_count
    return (year, total_count, avg_rating)

year_avg_rdd = year_sum_count_rdd.map(final_format).sortBy(lambda x: x[0])

print("\nFinal result - (Year, total_count, avg_rating):")
for row in year_avg_rdd.collect():
    print(row)

sc.stop()