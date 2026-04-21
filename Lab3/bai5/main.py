from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Bai5_Rating_Theo_NgheNghiep").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf=conf)

# 1. Đọc users.txt
# Mục tiêu: UserID -> OccupationID
def parse_user_occupation(line):
    parts = line.split(",")
    user_id = parts[0]
    occupation_id = parts[3]
    return (user_id, occupation_id)

users_rdd = sc.textFile("hdfs://127.0.0.1:9000/users")
users_occ_rdd = users_rdd.map(parse_user_occupation)

# Output:
# ('1', '3')
# ('2', '7')
print("Step 1 - users_occ_rdd:")
print(users_occ_rdd.take(5))


# 2. Đọc ratings_1.txt
# Mục tiêu: UserID -> Rating
def parse_rating(line):
    parts = line.split(",")
    user_id = parts[0]
    rating = float(parts[2])
    return (user_id, rating)

ratings_rdd = sc.textFile("hdfs://127.0.0.1:9000/ratings")
ratings_parsed_rdd = ratings_rdd.map(parse_rating)

# Output:
# ('7', 4.5)
print("\nStep 2 - ratings_parsed_rdd:")
print(ratings_parsed_rdd.take(5))


# 3. Join users với ratings theo UserID
# Kết quả: UserID -> (OccupationID, Rating)
joined_rdd = users_occ_rdd.join(ratings_parsed_rdd)

# Output:
# ('7', ('3', 4.5))
print("\nStep 3 - joined_rdd:")
print(joined_rdd.take(5))


# 4. Chuyển sang key là OccupationID
# Kết quả: (OccupationID, (rating, 1))
def to_occ_rating(row):
    user_id, (occupation_id, rating) = row
    return (occupation_id, (rating, 1))

occ_rating_rdd = joined_rdd.map(to_occ_rating)

# Output:
# ('3', (4.5, 1))
print("\nStep 4 - occ_rating_rdd:")
print(occ_rating_rdd.take(5))


# 5. Reduce để tính tổng điểm và tổng lượt rating
# Kết quả: (OccupationID, (total_rating, total_count))
def reduce_sum_count(a, b):

    return (a[0] + b[0], a[1] + b[1])

occ_sum_count_rdd = occ_rating_rdd.reduceByKey(reduce_sum_count)

print("\nStep 5 - occ_sum_count_rdd:")
print(occ_sum_count_rdd.take(5))


# 6. Đọc occupation.txt
# Mục tiêu: OccupationID -> OccupationName
def parse_occupation(line):
    parts = line.split(",")
    occ_id = parts[0]
    occ_name = parts[1]
    return (occ_id, occ_name)

occupation_rdd = sc.textFile("hdfs://127.0.0.1:9000/occupation")
occupation_map_rdd = occupation_rdd.map(parse_occupation)

# Output:
# ('1', 'Programmer')
print("\nStep 6 - occupation_map_rdd:")
print(occupation_map_rdd.take(5))


# 7. Join để gắn tên nghề nghiệp
# Kết quả: OccupationID -> (OccupationName, (total_rating, total_count))
joined_occ_rdd = occupation_map_rdd.join(occ_sum_count_rdd)

print("\nStep 7 - joined_occ_rdd:")
print(joined_occ_rdd.take(5))


# 8. Tính avg rating
# Kết quả cuối: (OccupationName, total_count, avg_rating)
def final_format(row):
    occ_id, (occ_name, (total_rating, total_count)) = row
    avg_rating = total_rating / total_count
    return (occ_name, total_count, avg_rating)

final_rdd = joined_occ_rdd.map(final_format)

print("\nFinal result - (OccupationName, total_count, avg_rating):")
for row in final_rdd.take(20):
    print(row)

sc.stop()