# %%
# The usual preamble
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import numpy as np

# Make the graphs a bit prettier, and bigger
plt.style.use("ggplot")
plt.rcParams["figure.figsize"] = (15, 5)
plt.rcParams["font.family"] = "sans-serif"


# %%
# One of the main problems with messy data is: how do you know if it's messy or not?
# We're going to use the NYC 311 service request dataset again here, since it's big and a bit unwieldy.
dtypes = {
    "Incident Zip": pl.Utf8,  # Set as string
    "Request Date": pl.Date,  # Set as Date type
    "Number of Requests": pl.Int32  # Set as 32-bit integer
}
requests = pl.read_csv(r"C:/Users\psingh\Desktop\pandas_to_polars_cookbook\data\311-service-requests.csv", dtypes=dtypes)
requests.head()

# TODO: load the data with Polars


# %%
# How to know if your data is messy?
# We're going to look at a few columns here. I know already that there are some problems with the zip code, so let's look at that first.

# To get a sense for whether a column has problems, I usually use `.unique()` to look at all its values. If it's a numeric column, I'll instead plot a histogram to get a sense of the distribution.

# When we look at the unique values in "Incident Zip", it quickly becomes clear that this is a mess.

# Some of the problems:

# * Some have been parsed as strings, and some as floats
# * There are `nan`s
# * Some of the zip codes are `29616-0759` or `83`
# * There are some N/A values that pandas didn't recognize, like 'N/A' and 'NO CLUE'

# What we can do:

# * Normalize 'N/A' and 'NO CLUE' into regular nan values
# * Look at what's up with the 83, and decide what to do
# * Make everything strings

requests.select(pl.col("Incident Zip").unique())

# TODO: what's the Polars command for this?

# %%
# Fixing the nan values and string/float confusion
# We can pass a `na_values` option to `pd.read_csv` to clean this up a little bit. We can also specify that the type of Incident Zip is a string, not a float.
null_values = ["NO CLUE", "N/A", "0"]
requests = pl.read_csv(
    r"C:/Users\psingh\Desktop\pandas_to_polars_cookbook\data\311-service-requests.csv", null_values=null_values, dtypes=dtypes
)
requests.select(pl.col("Incident Zip").unique())

# TODO: please implement this with Polars


# %%
# What's up with the dashes?
rows_with_dashes = requests.filter(pl.col("Incident Zip").str.contains("-"))
num_rows_with_dashes = rows_with_dashes.shape[0]  # Equivalent to len in Pandas
print(f"Number of rows with dashes: {num_rows_with_dashes}")
# Display the rows that contain dashes
print(rows_with_dashes)

# TODO: please implement this with Polars


# %%
# I thought these were missing data and originally deleted them like this:
# `requests['Incident Zip'][rows_with_dashes] = np.nan`
# But then 9-digit zip codes are normal. Let's look at all the zip codes with more than 5 digits, make sure they're okay, and then truncate them.
# Find all zip codes with more than 5 digits
long_zip_codes = requests.filter(pl.col("Incident Zip").str.len_chars() > 5)

# Get unique zip codes with more than 5 digits
unique_long_zips = long_zip_codes.select(pl.col("Incident Zip").unique())
print("Unique long zip codes:", unique_long_zips)

# Truncate 'Incident Zip' to the first 5 characters
requests = requests.with_columns(
    pl.col("Incident Zip").str.slice(0, 5)
)

# View the updated DataFrame
print(requests.head())

# TODO: please implement this with Polars


# %%
#  I'm still concerned about the 00000 zip codes, though: let's look at that.
# Assuming requests is your Polars DataFrame

# 1. Filter the rows where "Incident Zip" is "00000"
zero_zips = requests.filter(pl.col("Incident Zip") == "00000")

# 2. Replace "00000" zip codes with null (None in Python)
requests = requests.with_columns(
    pl.when(pl.col("Incident Zip") == "00000")
    .then(None)  # Replace "00000" with None (equivalent to np.nan in Pandas)
    .otherwise(pl.col("Incident Zip"))
    .alias("Incident Zip")  # Ensure to update the same column
)

# 3. Display the rows where "Incident Zip" was "00000"
print(zero_zips)

# 4. Verify that the "00000" values have been replaced with null
print(requests.filter(pl.col("Incident Zip").is_null()))

# TODO: please implement this with Polars


# %%
# Great. Let's see where we are now:
# Get unique values of 'Incident Zip'
unique_zips = requests.select(pl.col("Incident Zip").unique())
print("Unique Zip Codes:", unique_zips)

# Convert all values to strings, handling null (NaN) values by replacing them with "NaN"
# We use the `fill_null` method to handle missing values
requests = requests.with_columns(
    pl.col("Incident Zip").fill_null("NaN").cast(pl.Utf8).alias("Incident Zip")
)

# Get unique zip codes again after handling nulls and converting to string
unique_zips_after = requests.select(pl.col("Incident Zip").unique())

# Convert to a list and sort
unique_zips_sorted = unique_zips_after.to_series().sort()
print("Sorted Unique Zip Codes:", unique_zips_sorted)

# Amazing! This is much cleaner.

# TODO: please implement this with Polars


# %%
# There's something a bit weird here, though -- I looked up 77056 on Google maps, and that's in Texas.
# Let's take a closer look:
# Extract the 'Incident Zip' column
# Filter for zip codes that start with '0' or '1'
is_close = requests.filter(
    pl.col("Incident Zip").str.starts_with("0") | pl.col("Incident Zip").str.starts_with("1")
)

# Filter for zip codes that don't start with '0' or '1' and are not null
is_far = requests.filter(
    (~(pl.col("Incident Zip").str.starts_with("0") | pl.col("Incident Zip").str.starts_with("1")))
    & pl.col("Incident Zip").is_not_null()
)

# Display the filtered zip codes that don't start with '0' or '1'
print(is_far.select(["Incident Zip"]))

# TODO: please implement this with Polars


# %%
# Select the relevant columns and sort by 'Incident Zip'
filtered_sorted = (
    is_far
    .select(["Incident Zip", "Descriptor", "City"])  # Select the required columns
    .sort("Incident Zip")  # Sort by 'Incident Zip'
)

# Display the sorted and filtered DataFrame
print(filtered_sorted)

# TODO: please implement this with Polars


# %%
# Filtering by zip code is probably a bad way to handle this -- we should really be looking at the city instead.
# Convert 'City' to uppercase
requests = requests.with_columns(pl.col("City").str.to_uppercase().alias("City"))

# Use `value_counts()` directly on the 'City' column
city_counts = requests.select(pl.col("City").value_counts())

# Display the result
print(city_counts)

# It looks like these are legitimate complaints, so we'll just leave them alone.

# TODO: please implement this with Polars


# %%
# Let's turn this analysis into a function putting it all together:
# Define the values that should be treated as null
na_values = ["NO CLUE", "N/A", "0"]

# Define a function to fix zip codes
def fix_zip_codes(df):
    # Truncate all 'Incident Zip' values to 5 characters
    df = df.with_columns(pl.col("Incident Zip").str.slice(0, 5))

    # Set '00000' zip codes to null
    df = df.with_columns(
        pl.when(pl.col("Incident Zip") == "00000")
        .then(None)
        .otherwise(pl.col("Incident Zip"))
        .alias("Incident Zip")
    )

    return df

# Apply the zip code fixing function
requests = fix_zip_codes(requests)

# Get the unique values of 'Incident Zip'
unique_zips = requests.select(pl.col("Incident Zip").unique())

# Display the result
print(unique_zips)

# TODO: please implement this with Polars

# %%
