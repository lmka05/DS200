# import pyspark
# from pyspark import SparkContext

# sc = SparkContext.getOrCreate()

# def keep_id_rating(line):
#     _, movie_id, rating, _ = line.split(",")
#     return movie_id, (float(rating),1)

# def keep_id_genres(line):
#     movie_id, _, genres = line.split(",")
#     genres = genres.split("|")
#     return movie_id, genres

# def get_total_ratings_count(value_1, value_2):
#     total_rating, total_count = value_1
#     rating, count = value_2
#     return total_rating + rating, total_count+count

# def get_avg(row):
#     id,(total_rating, total_count) = row            
#     return id, total_rating/total_count

# def keep_genres_id(row):
#     id, (genres, (total_count, avg_rating)) = row
#     return genres, (total_count, avg_rating)

# def keep_genre_rating_avg(row):
#     movie_id, (genres, rating) = row
#     return [(genre, (rating,1)) for genre in genres]

# def keep_genre_rating(line):
#     _, movie_id, rating, _ = line.split(",")
#     return movie_id, (float(rating),1)

# ratings_rdd = sc.textFile("hdfs://127.0.0.1:9000/ratings")
# ratings_rdd = ratings_rdd.map(keep_id_rating) 
# ratings_rdd = ratings_rdd.reduceByKey(get_total_ratings_count) # movieid -> (total_rating, total_count)
# ratings_rdd = ratings_rdd.map(get_avg) # movieid -> avg_rating

# movies_rdd = sc.textFile("hdfs://127.0.0.1:9000/movies")
# movies_rdd = movies_rdd.map(keep_id_genres)# movieid -> genres

# movies_genres_ratings_rdd = movies_rdd.join(ratings_rdd) # (movie_id, ([genres], avg_rating))
# genre_ratings = movies_genres_ratings_rdd.flatMap(keep_genre_rating_avg) # genre -> (rating, 1)

# genre_ratings = genre_ratings.reduceByKey(get_total_ratings_count) # genre -> (total_rating, total_count)

# genre_avg_ratings = genre_ratings.map(get_avg)
# print("Average rating for each genre:")
# for row in genre_avg_ratings.collect():
#     print(row)

# """
# movies : movieid -> genres
# ratings : movieid -> rating
# 1. Tính trung bình rating của mỗi phim
# """

import pyspark
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

def keep_id_rating(line):
    _, movie_id, rating, _ = line.split(",")
    return (movie_id, (float(rating), 1))

def keep_id_genres(line):
    movie_id, _, genres = line.split(",")
    return (movie_id, genres.split("|"))

def get_total_ratings_count(v1, v2):
    total_rating1, total_count1 = v1
    total_rating2, total_count2 = v2
    return (total_rating1 + total_rating2, total_count1 + total_count2)

def get_avg(row):
    key, (total_rating, total_count) = row
    return (key, total_rating / total_count)

def keep_genre_rating(row):
    movie_id, (genres, avg_rating) = row
    return [(genre, (avg_rating, 1)) for genre in genres]

ratings_rdd = sc.textFile("hdfs://127.0.0.1:9000/ratings")
ratings_rdd = ratings_rdd.map(keep_id_rating)
ratings_rdd = ratings_rdd.reduceByKey(get_total_ratings_count)
ratings_rdd = ratings_rdd.map(get_avg)   # (movie_id, avg_rating)

movies_rdd = sc.textFile("hdfs://127.0.0.1:9000/movies")
movies_rdd = movies_rdd.map(keep_id_genres)   # (movie_id, [genres])

movies_genres_ratings_rdd = movies_rdd.join(ratings_rdd)        
# (movie_id, ([genres], avg_rating))

genre_ratings = movies_genres_ratings_rdd.flatMap(keep_genre_rating)
# (genre, (avg_rating, 1))

genre_ratings = genre_ratings.reduceByKey(get_total_ratings_count)
# (genre, (total_rating, total_count))

genre_avg_ratings = genre_ratings.map(get_avg)
# (genre, avg_rating)

print("Average rating for each genre:")
for row in genre_avg_ratings.collect():
    print(row)