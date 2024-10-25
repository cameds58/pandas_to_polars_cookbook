# %%
import pandas as pd
import polars as pl
import os

# %%
# Parsing Unix timestamps
# It's not obvious how to deal with Unix timestamps in pandas -- it took me quite a while to figure this out. The file we're using here is a popularity-contest file of packages.

# Read it, and remove the last row
# Assuming 'data/gdp_data.csv' is in a 'data' folder within the current working directory
file_path = os.path.join("data", "gdp_data.csv")

with open(file_path, 'r') as f:
    lines = f.readlines()

# Step 2: Remove the last row (assuming it's unnecessary)
lines = lines[:-1]

# Step 3: Split each line into columns based on spaces (handling multiple spaces)
parsed_data = [line.split() for line in lines]

# Step 4: Determine the maximum number of columns in the data
max_columns = max(len(row) for row in parsed_data)

# Step 5: Normalize all rows to have the same number of columns (fill with None if shorter)
normalized_data = [row + [None] * (max_columns - len(row)) for row in parsed_data]

# Step 6: Create a Polars DataFrame from the normalized data
# Provide column names manually (based on your pandas code)
column_names = ["atime", "ctime", "package-name", "mru-program", "tag"][:max_columns]

popcon = pl.DataFrame(normalized_data, schema=column_names)

# Step 7: Display the first 5 rows to ensure the data is correct
print(popcon.head(5))


# %%
# The magical part about parsing timestamps in pandas is that numpy datetimes are already stored as Unix timestamps. So all we need to do is tell pandas that these integers are actually datetimes -- it doesn't need to do any conversion at all.
# We need to convert these to ints to start:
# Convert the 'atime' and 'ctime' columns to integers
# Filter out rows where 'atime' or 'ctime' contain non-numeric values
# Replace non-numeric values with None in 'atime' and 'ctime'
popcon = popcon.slice(1)  # Removes the first row by slicing the DataFrame

# Step 2: Convert the 'atime' and 'ctime' columns to integers
popcon = popcon.with_columns([
    pl.col("atime").cast(pl.Int64),  # Cast 'atime' to Int64
    pl.col("ctime").cast(pl.Int64)   # Cast 'ctime' to Int64
])

# Step 3: Display the updated DataFrame
print(popcon.head())


# %%
# Every numpy array and pandas series has a dtype -- this is usually `int64`, `float64`, or `object`. Some of the time types available are `datetime64[s]`, `datetime64[ms]`, and `datetime64[us]`. There are also `timedelta` types, similarly.
# We can use the `pd.to_datetime` function to convert our integer timestamps into datetimes. This is a constant-time operation -- we're not actually changing any of the data, just how pandas thinks about it.
# Step 1: Multiply 'atime' and 'ctime' by 1000 to convert seconds to milliseconds
popcon = popcon.with_columns([
    (pl.col("atime") * 1000).alias("atime_ms"),  # Convert 'atime' from seconds to milliseconds
    (pl.col("ctime") * 1000).alias("ctime_ms")   # Convert 'ctime' from seconds to milliseconds
])

# Step 2: Cast the multiplied columns to Datetime using milliseconds ('ms')
popcon = popcon.with_columns([
    pl.col("atime_ms").cast(pl.Datetime("ms")),  # Cast 'atime_ms' to datetime with milliseconds
    pl.col("ctime_ms").cast(pl.Datetime("ms"))   # Cast 'ctime_ms' to datetime with milliseconds
])

# Step 3: Drop the old 'atime' and 'ctime' columns if you no longer need them
popcon = popcon.drop(["atime", "ctime"])

# Step 4: Rename the new columns to 'atime' and 'ctime'
popcon = popcon.rename({
    "atime_ms": "atime",
    "ctime_ms": "ctime"
})

# Step 5: Display the first few rows to verify the datetime conversion
print(popcon.head())

# %%
# Now suppose we want to look at all packages that aren't libraries.

# First, I want to get rid of everything with timestamp 0. Notice how we can just use a string in this comparison, even though it's actually a timestamp on the inside? That is because pandas is amazing.
# Step 1: Filter out rows with 'atime' less than or equal to "1970-01-01"
# Since 'atime' is a datetime, we will compare it with the datetime equivalent of '1970-01-01'.
popcon = popcon.filter(pl.col("atime") > pl.datetime(1970, 1, 1))

# Step 2: Filter rows where the 'package-name' does NOT contain 'lib'
nonlibraries = popcon.filter(~pl.col("package-name").str.contains("lib"))

# Step 3: Sort by 'ctime' in descending order and display the top 10 rows
nonlibraries_sorted = nonlibraries.sort("ctime", descending=True).head(10)

# Display the sorted, filtered data
print(nonlibraries_sorted)



# The whole message here is that if you have a timestamp in seconds or milliseconds or nanoseconds, then you can just "cast" it to a `'datetime64[the-right-thing]'` and pandas/numpy will take care of the rest.
