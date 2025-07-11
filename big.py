from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark Session
spark = SparkSession.builder.appName("COVID Bed Analysis").getOrCreate()

# Load CSV file
df = spark.read.csv("COVID.csv", header=True, inferSchema=True)

# Print schema
print("Dataset Schema:")
df.printSchema()

# Show top records
print("Top 5 Records:")
df.select(
    col("`Name of the District`"),
    col("`Total Vacant beds under CHC and CDH as on 15.06.2022`")
).show(5)

# Select and sort top 10 districts by vacant beds
vacant_summary = df.select(
    col("`Name of the District`"),
    col("`Total Vacant beds under CHC and CDH as on 15.06.2022`").alias("TotalVacantBeds")
)

top10_vacant = vacant_summary.orderBy(col("TotalVacantBeds").desc())
print("Top 10 Districts by Total Vacant Beds:")
top10_vacant.show(10)

# Stop Spark session
spark.stop()