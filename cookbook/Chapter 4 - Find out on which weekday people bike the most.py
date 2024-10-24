import polars as pl
import matplotlib.pyplot as plt

# Set matplotlib styles
plt.style.use("ggplot")
plt.rcParams["figure.figsize"] = (15, 5)
plt.rcParams["font.family"] = "sans-serif"

# %% Load the data
bikes = pl.read_csv(
    "../data/bikes.csv",
    sep=";",
    encoding="latin1",
    parse_dates=True,
)

# Convert 'Date' column to a Date type and set as index
bikes = bikes.with_column(pl.col("Date").str.strptime(pl.Date, "%d/%m/%Y"))

# Select the Berri bike path data
bikes = bikes.select([pl.col("Date"), pl.col("Berri 1")])

# Set 'Date' as the index
bikes = bikes.set_sorted("Date")

# %% Plot Berri 1 data using Matplotlib
plt.plot(bikes["Date"], bikes["Berri 1"])
plt.title("Berri 1 Bike Path Usage Over Time")
plt.xlabel("Date")
plt.ylabel("Number of Cyclists")
plt.show()

# %% Create a Polars dataframe with just the Berri bike path
pl_berri_bikes = bikes.select(["Date", "Berri 1"])

# %% Add weekday column
# Extract the weekday from the 'Date' column (0 = Monday, 6 = Sunday)
pl_berri_bikes = pl_berri_bikes.with_columns(
    pl.col("Date").dt.weekday().alias("weekday")
)

# %% Group by weekday and sum
weekday_counts = pl_berri_bikes.groupby("weekday").agg(pl.sum("Berri 1"))

# %% Rename index
weekday_map = {
    0: "Monday", 1: "Tuesday", 2: "Wednesday", 
    3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday"
}

# Map weekday numbers to their names
weekday_counts = weekday_counts.with_column(
    pl.col("weekday").apply(lambda x: weekday_map[x]).alias("weekday_name")
)

# %% Plot results
plt.bar(weekday_counts["weekday_name"], weekday_counts["Berri 1"])
plt.title("Total Cyclists by Weekday")
plt.xlabel("Weekday")
plt.ylabel("Number of Cyclists")
plt.xticks(rotation=45)
plt.show()

# %% Final message
print("Analysis complete!")