# %%
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import numpy as np

plt.style.use("ggplot")
plt.rcParams["figure.figsize"] = (15, 3)
plt.rcParams["font.family"] = "sans-serif"


# %%
# We saw earlier that pandas is really good at dealing with dates. It is also amazing with strings! We're going to go back to our weather data from Chapter 5, here.
weather_2012 = pd.read_csv(
    "../data/weather_2012.csv", parse_dates=True, index_col="date_time"
)
weather_2012[:5]

# %%
# Load the data using polars and call the data frame pl_wather_2012
pl_weather_2012 = pl.read_csv("../data/weather_2012.csv", try_parse_dates = True)
pl_weather_2012.head()

# %%
# You'll see that the 'Weather' column has a text description of the weather that was going on each hour. We'll assume it's snowing if the text description contains "Snow".
# Pandas provides vectorized string functions, to make it easy to operate on columns containing text. There are some great examples: "http://pandas.pydata.org/pandas-docs/stable/basics.html#vectorized-string-methods" in the documentation.
weather_description = weather_2012["weather"]
is_snowing = weather_description.str.contains("Snow")

# Let's plot when it snowed and when it did not:
is_snowing = is_snowing.astype(float)
is_snowing.plot()
plt.show()

# %%
# The same with polars
pl_is_snowing = pl_weather_2012.select(
    pl.col("date_time"),
    pl.col("weather").str.contains("Snow").cast(pl.Float32))
plt.plot(pl_is_snowing["weather"])

# %%
# If we wanted the median temperature each month, we could use the `resample()` method like this:
weather_2012["temperature_c"].resample("M").apply(np.median).plot(kind="bar")
plt.show()

# Unsurprisingly, July and August are the warmest.

# %%
# Polars aggregate monthly temperature median
pl_temperature_median = pl_weather_2012.group_by_dynamic("date_time", every="1mo", period="1mo").agg(
    pl.col("temperature_c").median()
)
plt.bar(pl_temperature_median['date_time'], pl_temperature_median['temperature_c'], width=15)

# %%
# So we can think of snowiness as being a bunch of 1s and 0s instead of `True`s and `False`s:
is_snowing.astype(float)[:10]

# and then use `resample` to find the percentage of time it was snowing each month
is_snowing.astype(float).resample("M").apply(np.mean).plot(kind="bar")
plt.show()

# So now we know! In 2012, December was the snowiest month. Also, this graph suggests something that I feel -- it starts snowing pretty abruptly in November, and then tapers off slowly and takes a long time to stop, with the last snow usually being in April or May.

# %%

# Polars agg mean of snowness
pl_snowing_monthly_mean = pl_is_snowing.group_by_dynamic("date_time", every="1mo", period="1mo").agg(
    pl.col("weather").mean()
)
plt.bar(pl_snowing_monthly_mean['date_time'], pl_snowing_monthly_mean['weather'], width=15)


