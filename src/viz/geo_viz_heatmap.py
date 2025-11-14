import pandas as pd
import numpy as np
import folium
from folium.plugins import HeatMap

df = pd.read_parquet(r"D:\podyplomowe\PROJEKTY\GeoTelecom_Data_Pipeline\data\analytics\geo_points")

df = df[df["country"].str.lower() == "poland"].copy()

# GRID
GRID_SIZE = 0.01   # ~1 km resolution

df["lat_bin"] = (df["latitude"] // GRID_SIZE) * GRID_SIZE
df["lon_bin"] = (df["longitude"] // GRID_SIZE) * GRID_SIZE

grid = df.groupby(["lat_bin", "lon_bin"]).size().reset_index(name="count")

# Weighting
grid["weight"] = np.log1p(grid["count"])
grid["weight"] = grid["weight"] / grid["weight"].max()

# Prepare heatmap data
heat_data = grid[["lat_bin", "lon_bin", "weight"]].values.tolist()

# Map center
mean_lat = df["latitude"].mean()
mean_lon = df["longitude"].mean()

m = folium.Map(location=[mean_lat, mean_lon], zoom_start=6, tiles="CartoDB positron")

HeatMap(
    heat_data,
    radius=25,   # higher detail
    blur=30,
    max_zoom=8,
    min_opacity=0.3
).add_to(m)

m.save("cell_towers_pl_heatmap.html")
print("Saved cell_towers_pl_heatmap.html")