from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    sum,
    avg,
    round
)

# Start Spark
spark = (
    SparkSession.builder
    .appName("LargeScaleDataProcessing")
    .getOrCreate()
)

# Sample sales data
data = [
    (1001, "Laptop", "Electronics", 1, 60000),
    (1002, "Phone", "Electronics", 2, 25000),
    (1003, "Headphones", "Audio", 3, 2000),
    (1004, "Keyboard", "Accessories", 1, 1500),
    (1005, "Mouse", "Accessories", 2, 800),
    (1006, "Monitor", "Electronics", 2, 15000),
    (1007, "Speaker", "Audio", 1, 3500),
    (1008, "Webcam", "Accessories", 2, 4500),
    (1009, "Tablet", "Electronics", 1, 30000),
    (1010, "Microphone", "Audio", 2, 5000)
]

columns = [
    "Order_ID",
    "Product",
    "Category",
    "Quantity",
    "Price"
]

sales_df = spark.createDataFrame(data, columns)

# Transformation
sales_df = sales_df.withColumn(
    "Total_Sales",
    col("Quantity") * col("Price")
)

# Data-quality filtering
clean_df = sales_df.filter(
    (col("Quantity") > 0) &
    (col("Price") > 0)
)

# Create temporary SQL view
clean_df.createOrReplaceTempView("sales")

# Spark SQL analysis
category_revenue = spark.sql("""
    SELECT
        Category,
        COUNT(Order_ID) AS Total_Orders,
        SUM(Quantity) AS Units_Sold,
        SUM(Total_Sales) AS Total_Revenue,
        ROUND(AVG(Total_Sales), 2) AS Average_Order_Value
    FROM sales
    GROUP BY Category
    ORDER BY Total_Revenue DESC
""")

category_revenue.show()

# Save as Parquet
clean_df.write.mode("overwrite").parquet(
    "sales_processed_parquet"
)

print("Pipeline completed successfully!")

spark.stop()
