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