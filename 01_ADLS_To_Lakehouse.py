#!/usr/bin/env python
# coding: utf-8

# ## 01_ADLS_To_Lakehouse
# 
# New notebook

# # <mark> 01_Access and extract the data from storage account ADLSGen2 </mark>
# 
# 
# 

# In[3]:


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import to_date

# Define schema with string type for date initially
transaction_schema = StructType([
    StructField("Transaction_ID", IntegerType(), nullable=False),
    StructField("Date", StringType(), nullable=False),
    StructField("Region", StringType(), nullable=False),
    StructField("Product", StringType(), nullable=False),
    StructField("Quantity", IntegerType(), nullable=False),
    StructField("Unit_Price", DoubleType(), nullable=False),
    StructField("Total_Amount", DoubleType(), nullable=False),
    StructField("Currency", StringType(), nullable=False),
    StructField("Customer_ID", StringType(), nullable=False)
])



# In[4]:


# ADLS Gen2 configuration
storage_account_name = "Your account info"
container_name = "Your account info"
relative_path = "raw/sales_data.csv"


# In[5]:


# ADLS Gen2 file path
file_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{relative_path}"


# In[6]:


# Read CSV file with the defined schema
df = spark.read \
    .option("header", "true") \
    .schema(transaction_schema) \
    .csv(file_path)

# Show the data
# display(df)


# In[7]:


from pyspark.sql.functions import col


# In[8]:


# Convert date to proper format
df = df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))


# In[7]:


## df.printSchema()


# # <mark>02_Data Cleaning & Transformation </mark>
# 
# 

# In[9]:


# Check for duplicates
print(f"Total rows: {df.count()}")
print(f"Unique Transaction IDs: {df.select('Transaction_ID').distinct().count()}")


# In[38]:


# Check for nulls
from pyspark.sql.functions import col, sum as spark_sum
df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()


# In[10]:


from pyspark.sql.functions import create_map, lit
from pyspark.sql.functions import round, bround


# In[11]:


from pyspark.sql.functions import create_map, lit, col, round

exchange_rates = {
    'GBP': 1.25,
    'EUR': 1.10,
    'AUD': 0.68,
    'JPY': 0.0075,
    'INR': 0.012,
    'USD': 1.0
}

# Create the map expression by flattening the dictionary items
exchange_rate_expr = create_map(*[lit(x) for pair in exchange_rates.items() for x in pair])

# Add converted amount column
df = df.withColumn(
    "Total_Amount_USD", 
    round(col("Total_Amount") * exchange_rate_expr[col("Currency")], 2)
)


# In[48]:


## Data Enrichment

# Add profit margin (assuming 15%)
df = df.withColumn("Profit_Margin", lit(0.15))
df = df.withColumn("Profit_Amount_USD", round(col("Total_Amount_USD") * col("Profit_Margin"), 2))


# In[49]:


# Add sales per unit
df = df.withColumn("Sales_Per_Unit", round(col("Total_Amount_USD") / col("Quantity"), 2))


# In[50]:


from pyspark.sql.functions import col, date_format, quarter


# In[51]:


# Extract month and quarter
df = df.withColumn("Month", date_format(col("Date"), "MMM-yy")) \
       .withColumn("Quarter", quarter(col("Date")))


# In[16]:


## display(df);


# In[52]:


from pyspark.sql.functions import col, sum, count, avg, round
from pyspark.sql.types import DoubleType


# # <mark> 03_Data Aggregations</mark>

# In[53]:


### Monthly Sales Summary
monthly_sales = df.groupBy("Month") \
    .agg(
        round(sum("Total_Amount_USD"), 2).alias("Total_Sales_USD"),
        count("*").alias("Transaction_Count"),
        round(avg("Total_Amount_USD"), 2).alias("Avg_Sale_Amount")
    ) \
    .orderBy("Month")


# In[54]:


display(monthly_sales)


# In[19]:


## display(monthly_sales)


# In[55]:


### Quarterly Sales Summary
quarterly_sales = df.groupBy("Quarter") \
    .agg(
        round(sum("Total_Amount_USD"), 2).alias("Total_Sales_USD"),
        count("*").alias("Transaction_Count"),
        round(avg("Total_Amount_USD"), 2).alias("Avg_Sale_Amount")
    ) \
    .orderBy("Quarter")


# In[21]:


## display(quarterly_sales)


# In[56]:


### Regional Performance
region_performance = df.groupBy("Region") \
    .agg(
        round(sum("Total_Amount_USD"), 2).alias("Total_Sales_USD"),
        count("*").alias("Transaction_Count")
    ) \
    .withColumn(
        "%_of_Total",
        round(col("Total_Sales_USD") / df.select(sum("Total_Amount_USD")).first()[0] * 100, 1)
    ) \
    .orderBy(col("Total_Sales_USD").desc())


# In[23]:


display(region_performance)


# # <mark>04_Data Loading To LakeHouse</mark>

# In[57]:


lakehouse_name = "Your account info_LH"
table_name = "sales_trans_tb"

# Write DataFrame to Lakehouse as Delta table
(df.write
   .format("delta")
   .mode("overwrite")  # Options: "overwrite", "append", "ignore", "error"
   .save(f"Tables/{table_name}"))

print(f"Data successfully written to Lakehouse table: {table_name}")


# In[58]:


table_name = "monthly_sales"


# In[59]:


# Write DataFrame to Lakehouse as Delta table
(monthly_sales.write
   .format("delta")
   .mode("overwrite")  # Options: "overwrite", "append", "ignore", "error"
   .save(f"Tables/{table_name}"))

print(f"Data successfully written to Lakehouse table: {table_name}")


# In[62]:


table_name = "quarterly_sales"


# In[63]:


# Write DataFrame to Lakehouse as Delta table
(quarterly_sales.write
   .format("delta")
   .mode("overwrite")  # Options: "overwrite", "append", "ignore", "error"
   .save(f"Tables/{table_name}"))

print(f"Data successfully written to Lakehouse table: {table_name}")


# In[64]:


table_name = "region_performance"


# In[65]:


# Write DataFrame to Lakehouse as Delta table
(region_performance.write
   .format("delta")
   .mode("overwrite")  # Options: "overwrite", "append", "ignore", "error"
   .save(f"Tables/{table_name}"))

print(f"Data successfully written to Lakehouse table: {table_name}")

