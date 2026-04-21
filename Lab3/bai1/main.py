import pyspark
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
movies_rdd = sc.textFile("hdfs://127.0.0.1:9000/movies")

def keep_id_title(line):
    movie_id, title, _ = line.split(",")
    return movie_id, title

movies_rdd = movies_rdd.map(keep_id_title)
print("Movies RDD:")
print(movies_rdd.collect())

ratings_rdd = sc.textFile("hdfs://127.0.0.1:9000/ratings")

def keep_id_rating(line):
    _, movie_id, rating, _ = line.split(",")
    return movie_id, (float(rating),1)  

ratings_rdd = ratings_rdd.map(keep_id_rating)

def get_total_ratings_count(value_1, value_2):
    total_rating, total_count = value_1
    rating, count = value_2
    return total_rating + rating, total_count+count

ratings_rdd = ratings_rdd.reduceByKey(get_total_ratings_count)

ratings_rdd = ratings_rdd.filter(lambda x: x[1][1] > 5) 


def get_avg(row):
    id,(total_rating, total_count) = row            
    return id, (total_count, total_rating/total_count)

avg_ratings_rdd = ratings_rdd.map(get_avg)
max_avg_rating = avg_ratings_rdd.reduce(lambda x, y: x if x[1][1] > y[1][1] else y)
max_avg_rating = sc.parallelize([max_avg_rating])
result = movies_rdd.join(max_avg_rating)
print("Movie with the highest average rating:")
print(result.collect())