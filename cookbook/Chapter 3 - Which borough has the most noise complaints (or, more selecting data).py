import polars as pl
import matplotlib.pyplot as plt

# Load the data
pl_complaints = pl.read_csv(
    "../data/311-service-requests.csv",
    dtype={"Complaint Type": pl.Utf8, "Borough": pl.Utf8}
)

# 3.1 Selecting only noise complaints
# Checking the first 5 rows
print(pl_complaints.head(5))

# To get noise complaints with Complaint Type as "Noise - Street/Sidewalk"
noise_complaints = pl_complaints.filter(pl_complaints["Complaint Type"] == "Noise - Street/Sidewalk")
print(noise_complaints.head(3))

# Combining more than one condition
is_noise = pl_complaints["Complaint Type"] == "Noise - Street/Sidewalk"
in_brooklyn = pl_complaints["Borough"] == "BROOKLYN"
brooklyn_noise_complaints = pl_complaints.filter(is_noise & in_brooklyn)
print(brooklyn_noise_complaints.head(5))

# Selecting only specific columns from the data
print(brooklyn_noise_complaints.select(
    ["Complaint Type", "Borough", "Created Date", "Descriptor"]
).head(10))

# 3.3 So, which borough has the most noise complaints?
noise_complaints = pl_complaints.filter(pl_complaints["Complaint Type"] == "Noise - Street/Sidewalk")
noise_complaints_count = noise_complaints.groupby("Borough").count()

# Counting total complaints by borough
total_complaints_count = pl_complaints.groupby("Borough").count()

# Calculate the ratio of noise complaints to total complaints for each borough
# First, join noise_complaints_count and total_complaints_count on the "Borough" column
ratios = noise_complaints_count.join(total_complaints_count, on="Borough", suffix="_total")
ratios = ratios.with_column((pl.col("count") / pl.col("count_total")).alias("ratio"))

# Plot the results
# Convert the Polars DataFrame to Pandas for compatibility with Matplotlib
ratios_df = ratios.select(["Borough", "ratio"]).to_pandas()
ratios_df.set_index("Borough")["ratio"].plot(kind="bar")

# Add plot labels and title
plt.title("Noise Complaints by Borough (Normalized)")
plt.xlabel("Borough")
plt.ylabel("Ratio of Noise Complaints to Total Complaints")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()