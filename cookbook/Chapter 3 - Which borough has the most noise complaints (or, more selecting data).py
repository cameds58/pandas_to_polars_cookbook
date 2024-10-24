import polars as pl
import matplotlib.pyplot as plt

# To display more columns in Polars
pd.set_option("display.width", 5000)
pd.set_option("display.max_columns", 60)

# Load the data 
pl_complaints = pl.read_csv("../data/311-service-requests.csv", dtype={"Complaint Type": pl.Utf8, "Borough": pl.Utf8})

# 3.1 Selecting only noise complaints
# Checking the first 5 rows of the data
pl_complaints.head(5)

# To get noise complaints with Complaint Type as "Noise - Street/Sidewalk"
noise_complaints = pl_complaints.filter(pl_complaints["Complaint Type"] == "Noise - Street/Sidewalk")
noise_complaints.head(3)

# Combining more than one condition
is_noise = pl_complaints["Complaint Type"] == "Noise - Street/Sidewalk"
in_brooklyn = pl_complaints["Borough"] == "BROOKLYN"
brooklyn_noise_complaints = pl_complaints.filter(is_noise & in_brooklyn)
brooklyn_noise_complaints.head(5)

# Selecting only specific columns from the data
brooklyn_noise_complaints.select(
    ["Complaint Type", "Borough", "Created Date", "Descriptor"]
).head(10)

# 3.3 So, which borough has the most noise complaints?
noise_complaints = pl_complaints.filter(pl_complaints["Complaint Type"] == "Noise - Street/Sidewalk")
noise_complaints_count = noise_complaints.groupby("Borough").count()

# Counting total complaints by borough
total_complaints_count = pl_complaints.groupby("Borough").count()

# Calculate the ratio of noise complaints to total complaints for each borough
ratios = noise_complaints_count.with_column(
    (pl.col("count") / total_complaints_count["count"]).alias("ratio")
)

# Plot the results
ratios_df = ratios.to_pandas()  # Convert Polars DataFrame to Pandas
ratios_df.set_index("Borough")["ratio"].plot(kind="bar")
plt.title("Noise Complaints by Borough (Normalized)")
plt.xlabel("Borough")
plt.ylabel("Ratio of Noise Complaints to Total Complaints")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()