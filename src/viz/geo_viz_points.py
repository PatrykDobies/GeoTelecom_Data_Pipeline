import pandas as pd
import folium
import glob

df = pd.read_parquet(r"D:\GeoTelecom_Data_Pipeline\data\analytics\geo_points")

df_poland = df[df["country"].str.lower() == "poland"]

# geographical center
mean_lat = df_poland["latitude"].mean()
mean_lon = df_poland["longitude"].mean()

# creating map
m = folium.Map(location=[mean_lat, mean_lon], zoom_start=2, tiles="CartoDB positron")

# adding points
for _, row in df_poland.iterrows():
    folium.CircleMarker(
        location=[row["latitude"], row["longitude"]],
        radius=max(2, min(row["range_m"] / 10000, 10)),
        color=None,
        fill=True,
        fill_opacity=0.6,
        fill_color="blue" if row["avg_signal"] < -90 else "green",
        popup=f"""
        <b>Country:</b> {row['country']}<br>
        <b>Network:</b> {row['network_name']}<br>
        <b>Signal:</b> {row['avg_signal']}<br>
        <b>Range (m):</b> {row['range_m']}
        """,
    ).add_to(m)

m.save("cell_towers_pl_map.html")
print("Map saved as cell_towers_pl_map.html")
