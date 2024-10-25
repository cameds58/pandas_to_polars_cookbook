# %%
import pandas as pd
import matplotlib.pyplot as plt
import polars as pl

# %%
# We're going to use a new dataset here, to demonstrate how to deal with larger datasets. This is a subset of the of 311 service requests from [NYC Open Data](https://nycopendata.socrata.com/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9).
# because of mixed types we specify dtype to prevent any errors
complaints = pd.read_csv("../data/311-service-requests.csv", dtype="unicode")
complaints.head()

# %%
# TODO: rewrite the above using the polars library and call the data frame pl_complaints
# Hint: we need the dtype argument reading all columns in as strings above in Pandas due to the zip code column containing NaNs as "NA" and some zip codes containing a dash like 1234-456
# you cannot exactly do the same in Polars but you can read about some other solutions here:
# see a discussion about dtype argument here: https://github.com/pola-rs/polars/issues/8230

# in polars default datatype as string
pl_complaints = pl.read_csv("../data/311-service-requests.csv", infer_schema_length=0)

# %%
# Selecting columns:
complaints["Complaint Type"]

# %%
# TODO: rewrite the above using the polars library

pl_complaints["Complaint Type"]
# %%
# Get the first 5 rows of a dataframe
complaints[:5]

# %%
# TODO: rewrite the above using the polars library
pl_complaints[:5]
# %%
# Combine these to get the first 5 rows of a column:
complaints["Complaint Type"][:5]

# %%
# TODO: rewrite the above using the polars library
pl_complaints["Complaint Type"][:5]

# %%
# Selecting multiple columns
complaints[["Complaint Type", "Borough"]]

# %%
# TODO: rewrite the above using the polars library
pl_complaints[["Complaint Type", "Borough"]]

# %%
# What's the most common complaint type?
complaint_counts = complaints["Complaint Type"].value_counts()
complaint_counts[:10]

# %%
# TODO: rewrite the above using the polars library
pl_complaints_count = pl_complaints["Complaint Type"].value_counts().sort("count", descending=True)
top10 = pl_complaints_count[:10]
# %%
# Plot the top 10 most common complaints
complaint_counts[:10].plot(kind="bar")
plt.title("Top 10 Complaint Types")
plt.xlabel("Complaint Type")
plt.ylabel("Count")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()

# %%
# TODO: please do the same with Polars
top_10_type = top10["Complaint Type"].to_list()
top_10_count = top10["count"].to_list()
plt.bar(top_10_type, top_10_count)
plt.title("Top 10 Complaint Types in polars")
plt.xlabel("Complaint Type")
plt.ylabel("Count")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()


# %%
