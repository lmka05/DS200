from pyspark import SparkContext

sc = SparkContext.getOrCreate()


# Hàm phân nhóm tuổi
def get_age_group(age):
    age = int(age)
    if age < 18:
        return "<18"
    elif age <= 25:
        return "18-25"
    elif age <= 35:
        return "26-35"
    elif age <= 45:
        return "36-45"
    elif age <= 55:
        return "46-55"
    else:
        return "56+"


# 1. Đọc users.txt
# Mục tiêu: UserID -> AgeGroup
def parse_user_age_group(line):
    parts = line.split(",")
    user_id = parts[0]
    age = parts[2]
    age_group = get_age_group(age)
    return (user_id, age_group)

users_rdd = sc.textFile("hdfs://127.0.0.1:9000/users")
users_age_group_rdd = users_rdd.map(parse_user_age_group)

# Output mỗi dòng sẽ dạng:
# ('1', '26-35')
# ('2', '36-45')
print("Step 1 - users_age_group_rdd:")
print(users_age_group_rdd.take(5))


# 2. Đọc ratings_1.txt
# Mục tiêu: UserID -> (MovieID, Rating)
def parse_rating(line):
    parts = line.split(",")
    user_id = parts[0]
    movie_id = parts[1]
    rating = float(parts[2])
    return (user_id, (movie_id, rating))

ratings_rdd = sc.textFile("hdfs://127.0.0.1:9000/ratings")
ratings_parsed_rdd = ratings_rdd.map(parse_rating)

# Output:
# ('7', ('1020', 4.5))
print("\nStep 2 - ratings_parsed_rdd:")
print(ratings_parsed_rdd.take(5))


# 3. Join theo UserID
# Kết quả: UserID -> (AgeGroup, (MovieID, Rating))
joined_rdd = users_age_group_rdd.join(ratings_parsed_rdd)

# Output:
# ('7', ('26-35', ('1020', 4.5)))
print("\nStep 3 - joined_rdd:")
print(joined_rdd.take(5))


# 4. Chuyển key sang (MovieID, AgeGroup)
# Kết quả: ((MovieID, AgeGroup), (rating, 1))
def to_movie_agegroup_key(row):
    user_id, (age_group, (movie_id, rating)) = row
    return ((movie_id, age_group), (rating, 1))

movie_agegroup_rating_rdd = joined_rdd.map(to_movie_agegroup_key)

# Output:
# (('1020', '26-35'), (4.5, 1))
print("\nStep 4 - movie_agegroup_rating_rdd:")
print(movie_agegroup_rating_rdd.take(5))


# 5. Reduce theo key
# Kết quả: ((MovieID, AgeGroup), (total_rating, total_count))
def reduce_sum_count(a, b):
    return (a[0] + b[0], a[1] + b[1])

movie_agegroup_sum_count_rdd = movie_agegroup_rating_rdd.reduceByKey(reduce_sum_count)

print("\nStep 5 - movie_agegroup_sum_count_rdd:")
print(movie_agegroup_sum_count_rdd.take(5))


# 6. Tính trung bình
# Kết quả: ((MovieID, AgeGroup), avg_rating)
def compute_avg(row):
    key, (total_rating, total_count) = row
    return (key, total_rating / total_count)

movie_agegroup_avg_rdd = movie_agegroup_sum_count_rdd.map(compute_avg)

print("\nStep 6 - movie_agegroup_avg_rdd:")
for row in movie_agegroup_avg_rdd.take(20):
    print(row)

sc.stop()