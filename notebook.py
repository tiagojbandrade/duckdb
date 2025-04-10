# %%
import pandas as pd
import glob
import time
import duckdb

conn = duckdb.connect() # create an in-memory database
# %%

# with pandas - Print the first 10 rows
cur_time = time.time()
df = pd.concat([pd.read_csv(f) for f in glob.glob('dataset/*.csv')])
print(f"time: {(time.time() - cur_time)}")
print(df.head(10))
# %%

# with duckdb - Print the first 10 rows
cur_time = time.time()
df = conn.execute("""
	SELECT *
	FROM 'dataset/*.csv'
	LIMIT 10
""").df()
print(f"time: {(time.time() - cur_time)}")
print(df)
# %%

#Checking the types of the columns
df = conn.execute("""
	SELECT *
	FROM 'dataset/*.csv'
""").df()
conn.register("df_view", df)
conn.execute("DESCRIBE df_view").df() # doesn't work if you don't register df as a virtual table
# %%

#Counting rows
conn.execute("SELECT COUNT(*) FROM df_view").df()
# %%

#Drop nulls
df.isnull().sum()
df = df.dropna(how='all')

# Notice we use df and not df_view
# With DuckDB you can run SQL queries on top of Pandas dataframes
conn.execute("SELECT COUNT(*) FROM df").df()

# %%

#Where clause
conn.execute("""SELECT * FROM df WHERE "Order ID"='295665'""").df()

# %%
# Limpa a vírgula da coluna "Price"
df["Price"] = df["Price"].str.replace(',', '').astype(float)
# %%
#Create a table and load the data
conn.execute("""
CREATE OR REPLACE TABLE sales AS
	SELECT
		"Order ID"::INTEGER AS order_id,
		Product AS product,
		"Quantity Ordered"::INTEGER AS quantity,
		"Price"::DECIMAL AS price,
		"Order Date"::DATE AS order_date,
		"Purchase Address" AS purchase_address
	FROM df
	WHERE
		TRY_CAST("Order ID" AS INTEGER) NOTNULL
""")

# %%
#FROM-first clause
conn.execute("FROM sales").df()

# %%
#Exclude
conn.execute("""
	SELECT 
		* EXCLUDE (product, order_date, purchase_address)
	FROM sales
	""").df()

# %%
#The Columns Expression
conn.execute("""
	SELECT 
		MIN(COLUMNS(* EXCLUDE (product, order_date, purchase_address))) 
	FROM sales
	""").df()

# %%
#Create a VIEW
conn.execute("""
	CREATE OR REPLACE VIEW aggregated_sales AS
	SELECT
		order_id,
		COUNT(1) as nb_orders,
		MONTH(order_date) as month,
		str_split(purchase_address, ',')[2] AS city,
		SUM(quantity * price) AS revenue
	FROM sales
	GROUP BY ALL
""")

# %%
