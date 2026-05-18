from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DS200_Lab4").getOrCreate()

data_path = "data"

orders = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Orders.csv")
customers = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Customer_List.csv")
order_items = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Order_Items.csv")
products = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Products.csv")
reviews = spark.read.option('delimiter', ';').option("header", True).option("inferSchema", True).csv(f"{data_path}/Order_Reviews.csv")

for name, df in [
    ("orders", orders),
    ("customers", customers),
    ("order_items", order_items),
    ("products", products),
    ("reviews", reviews),
]:
    print(f"== {name} ==")
    df.printSchema()
    df.show(5, truncate=False)
