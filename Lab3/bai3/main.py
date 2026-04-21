from pyspark import SparkContext,


sc = SparkContext.getOrCreate()

# =========================
# 1. Đọc users.txt
# Schema: UserID, Gender, Age, Occupation, Zip-code
# Mục tiêu: lấy map UserID -> Gender
# =========================
def parse_user_gender(line):
    parts = line.split(",")
    user_id = parts[0]
    gender = parts[1]
    return (user_id, gender)

users_rdd = sc.textFile("hdfs://127.0.0.1:9000/users")
users_gender_rdd = users_rdd.map(parse_user_gender)

# Output mỗi dòng sẽ dạng:
# ('1', 'M')
# ('2', 'F')
print("Step 1 - users_gender_rdd:")
print(users_gender_rdd.take(5))


# =========================
# 2. Đọc ratings_1.txt
# Schema: UserID, MovieID, Rating, Timestamp
# Mục tiêu: lấy map UserID -> (MovieID, Rating)
# =========================
def parse_rating(line):
    parts = line.split(",")
    user_id = parts[0]
    movie_id = parts[1]
    rating = float(parts[2])
    return (user_id, (movie_id, rating))

ratings_rdd = sc.textFile("hdfs://127.0.0.1:9000/ratings")
ratings_parsed_rdd = ratings_rdd.map(parse_rating)

# Output mỗi dòng sẽ dạng:
# ('7', ('1020', 4.5))
# ('23', ('1015', 3.5))
print("\nStep 2 - ratings_parsed_rdd:")
print(ratings_parsed_rdd.take(5))


# =========================
# 3. Join users với ratings theo UserID
# Kết quả: UserID -> (Gender, (MovieID, Rating))
# =========================
joined_user_rating_rdd = users_gender_rdd.join(ratings_parsed_rdd)

# Output mỗi dòng sẽ dạng:
# ('7', ('M', ('1020', 4.5)))
print("\nStep 3 - joined_user_rating_rdd:")
print(joined_user_rating_rdd.take(5))


# =========================
# 4. Chuyển key sang (MovieID, Gender)
# để sau đó tính avg rating theo từng phim và từng giới tính
# Kết quả: ((MovieID, Gender), (rating, 1))
# =========================
def to_movie_gender_key(row):
    user_id, (gender, (movie_id, rating)) = row
    return ((movie_id, gender), (rating, 1))

movie_gender_rating_rdd = joined_user_rating_rdd.map(to_movie_gender_key)

# Output mỗi dòng sẽ dạng:
# (('1020', 'M'), (4.5, 1))
print("\nStep 4 - movie_gender_rating_rdd:")
print(movie_gender_rating_rdd.take(5))


# =========================
# 5. Reduce để tính tổng điểm và số lượt rating
# Kết quả: ((MovieID, Gender), (total_rating, total_count))
# =========================
def reduce_sum_count(a, b):
    return (a[0] + b[0], a[1] + b[1])

movie_gender_sum_count_rdd = movie_gender_rating_rdd.reduceByKey(reduce_sum_count)

# Output mỗi dòng sẽ dạng:
# (('1020', 'M'), (18.5, 5))
print("\nStep 5 - movie_gender_sum_count_rdd:")
print(movie_gender_sum_count_rdd.take(5))


# =========================
# 6. Tính trung bình
# Kết quả: ((MovieID, Gender), avg_rating)
# =========================
def compute_avg(row):
    key, (total_rating, total_count) = row
    return (key, total_rating / total_count)

movie_gender_avg_rdd = movie_gender_sum_count_rdd.map(compute_avg)

# Output mỗi dòng sẽ dạng:
# (('1020', 'M'), 3.7)
print("\nStep 6 - movie_gender_avg_rdd:")
for row in movie_gender_avg_rdd.take(20):
    print(row)

sc.stop()