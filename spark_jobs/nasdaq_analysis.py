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
# floats/decimals
for c in ["Open", "High", "Low", "Close", "Volume"]:
    df = df.withColumn(
        c,
        F.when(F.col(c).rlike(r"^[0-9]+(\.[0-9]+)?$"), F.col(c).cast(T.DoubleType()))
         .otherwise(F.lit(None).cast(T.DoubleType()))
    )
# integers
df = df.withColumn(
    "TickVolume",
    F.when(F.col("TickVolume").rlike(r"^[0-9]+$"), F.col("TickVolume").cast(T.LongType()))
     .otherwise(F.lit(None).cast(T.LongType()))
)

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

# --- Save cleaned DataFrame to Parquet ---
parquet_path = 'output/nasdaq_clean.parquet'
df.write.mode('overwrite').parquet(parquet_path)
print(f'Data saved to {parquet_path}')

#Load the Parquet file back
df_parquet = spark.read.parquet(parquet_path)
print('Data loaded back from Parquet:')
df_parquet.show(5, truncate=False)

#Compare outputs to be sure that data matches
print(f'Original count: {df.count()}, Reloaded count: {df_parquet.count()}')

#Calculate monthly average Close price
monthly_avg = (
    df_parquet
    .groupBy('month')
    .agg(F.round(F.avg('Close'), 2). alias('avg_close'))
    .orderBy('month')
)
print('Monthly average close price:')
monthly_avg.show(truncate=False)

#TOP 5 months by total trading volumne
top_months_volume = (
    df_parquet
    .groupBy('month')
    .agg(F.sum('Volume').alias('total_volume'))
    .orderBy(F.desc('total_volume'))
    .limit(5)
)

print(f'Top 5 months by total trading volume:')
top_months_volume.show(truncate=False)


# Stop Spark
spark.stop()