from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType
import pyspark.sql.types as T

# Start Spark session
spark = SparkSession.builder \
    .appName('NasdaqAnalysis') \
    .getOrCreate()

file_path = 'data/nasdaq.csv'

# Read as text because it is space-delimited
lines = spark.read.text(file_path)

# Replace multiple spaces with commas
normalized = lines.select(
    F.regexp_replace(F.trim(F.col("value")), r"\s+", ",").alias("csv")
)

# Split into columns
cols = ["DateTime", "Open", "High", "Low", "Close", "Volume", "TickVolume"]
df = normalized.select([F.split(F.col('csv'), ",").getItem(i).alias(name) for i, name in enumerate(cols)])

# Remove header row
df = df.filter(F.col("DateTime") != "DateTime")

# Parse datetime with two formats
from pyspark.sql import functions as F

df = df.withColumn(
    "DateTime",
    F.coalesce(
        F.try_to_timestamp(F.col("DateTime"), F.lit("yyyy.MM.dd HH:mm:ss")),
        F.try_to_timestamp(F.col("DateTime"), F.lit("yyyy.MM.dd"))
    )
)

# Drop rows we couldn't parse
df = df.filter(F.col("DateTime").isNotNull())

# Cast numeric columns
df.filter(~F.col("Open").rlike("^[0-9.]+$")).show(5, truncate=False)
for c in ["Open","High","Low","Close"]:
    df = df.withColumn(c, F.when(F.col(c).rlike(r"^[0-9]+(\.[0-9]+)?$"), F.col(c).cast(T.DoubleType())))
for c in ["Volume","TickVolume"]:
    df = df.withColumn(c, F.when(F.col(c).rlike(r"^[0-9]+$"), F.col(c).cast(T.LongType())))

# Add month column
df = df.withColumn("month", F.date_format(F.col("DateTime"), "yyyy.MM"))

# Show schema and sample
print('Schema:')
df.printSchema()
print('First 5 rows:')
df.show(5, truncate=False)

# Filter example
df_filtered = df.filter(F.col('Volume') > 1000000)

# Show max/min close
print('Highest and lowest Close price:')
df.select(F.max('Close').alias('max_close'), F.min('Close').alias('min_close')).show()

# Stop Spark
spark.stop()