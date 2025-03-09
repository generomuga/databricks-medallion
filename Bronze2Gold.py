# Databricks notebook source
orders_path = '/FileStore/input/bronze/orders.csv'
orders_schema = StructType([
    StructField('ORDER_ID', IntegerType(), False),
    StructField('ORDER_DATETIME', StringType(), False),
    StructField('CUSTOMER_ID', IntegerType(), False),
    StructField('ORDER_STATUS', StringType(), False),
    StructField('STORE_ID', IntegerType(), False)
])
orders = spark.read.csv(orders_path, schema=orders_schema, header=True)

# COMMAND ----------

orders.display()

# COMMAND ----------

orders = orders.select(
    'ORDER_ID', 
    to_timestamp(orders['ORDER_DATETIME'],"dd-MMM-yy kk.mm.ss.SS").alias("ORDER_TIMESTAMP"), 
    'CUSTOMER_ID', 
    'ORDER_STATUS', 
    'STORE_ID'
)
orders.display()

# COMMAND ----------

orders = orders.filter(orders['ORDER_STATUS'] == 'COMPLETE')

# COMMAND ----------

orders.display()

# COMMAND ----------

from pyspark.sql.types import DoubleType

stores_path = '/FileStore/input/bronze/stores.csv'
stores_schema = StructType([
    StructField('STORE_ID', IntegerType(), False),
    StructField('STORE_NAME', StringType(), False),
    StructField('WEB_ADDRESS', StringType(), False),
    StructField('LATITUDE', DoubleType(), False),
    StructField('LONGITUDE', DoubleType(), False)
])
stores = spark.read.csv(stores_path, schema=stores_schema, header=True)

# COMMAND ----------

stores.display()

# COMMAND ----------

orders = orders.join(stores, orders['STORE_ID'] == stores['STORE_ID'], 'left').select("ORDER_ID", "ORDER_TIMESTAMP", "CUSTOMER_ID", "STORE_NAME")

# COMMAND ----------

orders.display()

# COMMAND ----------

orders.write.parquet("/FileStore/input/silver/orders.parquet", mode="overwrite")

# COMMAND ----------

orders_item_path = '/FileStore/input/bronze/order_items.csv'
orders_item_schema = StructType([
    StructField('ORDER_ID', IntegerType(), False),
    StructField('LINE_ITEM_ID', StringType(), False),
    StructField('PRODUCT_ID', IntegerType(), False),
    StructField('UNIT_PRICE', DoubleType(), False),
    StructField('QUANTITY', IntegerType(), False)
])
order_items = spark.read.csv(orders_item_path, schema=orders_item_schema, header=True)


# COMMAND ----------

order_items.display()

# COMMAND ----------

order_items = order_items.drop('LINE_ITEM_ID')

# COMMAND ----------

order_items.write.parquet("/FileStore/input/silver/order_items.parquet", mode="overwrite")

# COMMAND ----------

products_path = '/FileStore/input/bronze/products.csv'
products_schema = StructType([
    StructField('PRODUCT_ID', IntegerType(), False),
    StructField('PRODUCT_NAME', StringType(), False),
    StructField('UNIT_PRICE', DoubleType(), False)
])
products = spark.read.csv(products_path, schema=products_schema, header=True)

# COMMAND ----------

products.display()

# COMMAND ----------

products.write.parquet("/FileStore/input/silver/products.parquet", mode="overwrite")

# COMMAND ----------

customers_path = '/FileStore/input/bronze/customers.csv'
customers_schema = StructType([
    StructField('CUSTOMER_ID', IntegerType(), False),
    StructField('FULL_NAME', StringType(), False),
    StructField('EMAIL_ADDRESS', StringType(), False),
])
customers = spark.read.csv(customers_path, schema=customers_schema, header=True)

# COMMAND ----------

customers.display()

# COMMAND ----------

customers.write.parquet("/FileStore/input/silver/customers.parquet", mode="overwrite")

# COMMAND ----------

orders_silver_path = '/FileStore/input/silver/orders.parquet'
orders_silver = spark.read.parquet(orders_silver_path)

# COMMAND ----------

orders_silver.display()

# COMMAND ----------

orders = spark.read.parquet("/FileStore/input/silver/orders.parquet")
order_items = spark.read.parquet("/FileStore/input/silver/order_items.parquet")
products = spark.read.parquet("/FileStore/input/silver/products.parquet")
customers = spark.read.parquet("/FileStore/input/silver/customers.parquet")


# COMMAND ----------

orders.display()

# COMMAND ----------

from pyspark.sql.functions import to_date
order_details = orders.select(
    'ORDER_ID',
    to_date('ORDER_TIMESTAMP').alias('DATE'),
    'CUSTOMER_ID',
    'STORE_NAME'
)

# COMMAND ----------

order_details.display()

# COMMAND ----------

order_details.display()

# COMMAND ----------

order_details = order_details.join(order_items, order_items['order_id'] == order_details['order_id'], 'left').\
    select(order_details['ORDER_ID'], order_details['DATE'], order_details['CUSTOMER_ID'], order_details['STORE_NAME'], order_items['UNIT_PRICE'].alias('ITEM_UNIT_PRICE'), order_items['QUANTITY'])

# COMMAND ----------

order_details.display()

# COMMAND ----------

from pyspark.sql.functions import round

order_details = order_details.withColumn('TOTAL_SALES_AMOUNT', order_details['ITEM_UNIT_PRICE'] * order_details['QUANTITY'])

# COMMAND ----------

order_details.display()

# COMMAND ----------

order_details = order_details.\
    groupBy('ORDER_ID', 'DATE', 'CUSTOMER_ID', 'STORE_NAME'). \
        sum('TOTAL_SALES_AMOUNT'). \
            withColumnRenamed('sum(TOTAL_SALES_AMOUNT)', 'TOTAL_ORDER_AMOUNT')

# COMMAND ----------

order_details.display()

# COMMAND ----------

order_details = order_details.withColumn('TOTAL_ORDER_AMOUNT', round(order_details['TOTAL_ORDER_AMOUNT'], 2))

# COMMAND ----------

order_details.display()

# COMMAND ----------

orders.write.parquet("/FileStore/input/gold/order_details.parquet", mode="overwrite")

# COMMAND ----------

from pyspark.sql.functions import date_format
sales_with_month = order_details.withColumn('MONTH_YEAR',date_format(order_details['DATE'], 'yyyy-MM'))

# COMMAND ----------

sales_with_month.display()

# COMMAND ----------

from pyspark.sql.functions import round

month_sales = sales_with_month.groupBy('MONTH_YEAR').sum('TOTAL_ORDER_AMOUNT').\
    withColumn('TOTAL_SALES', round('sum(TOTAL_ORDER_AMOUNT)', 2)).sort(sales_with_month['MONTH_YEAR'].desc()).\
        select('MONTH_YEAR', 'TOTAL_SALES')

# COMMAND ----------

month_sales.display()

# COMMAND ----------

month_sales.write.parquet("/FileStore/input/gold/month_sales.parquet", mode="overwrite")

# COMMAND ----------

store_sales_with_month = sales_with_month.groupBy('MONTH_YEAR', 'STORE_NAME').sum('TOTAL_ORDER_AMOUNT').\
    withColumn('TOTAL_SALES', round('sum(TOTAL_ORDER_AMOUNT)', 2)).sort(sales_with_month['MONTH_YEAR'].desc()). \
        select('MONTH_YEAR', 'STORE_NAME', 'TOTAL_SALES')

# COMMAND ----------

sales_with_month.display()

# COMMAND ----------

store_sales_with_month.write.parquet("/FileStore/input/gold/store_monthly_sales.parquet", mode="overwrite")

# COMMAND ----------

display(order_details)

# COMMAND ----------

