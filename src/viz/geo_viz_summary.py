import pandas as pd
import plotly.express as px
import plotly.io as pio
import pycountry

geo_summary = pd.read_parquet(r"D:\podyplomowe\PROJEKTY\GeoTelecom_Data_Pipeline\data\analytics\geo_summary/")
operator_summary = pd.read_parquet(r"D:\podyplomowe\PROJEKTY\GeoTelecom_Data_Pipeline\data\analytics\operator_summary/")
geo_points = pd.read_parquet(r"D:\podyplomowe\PROJEKTY\GeoTelecom_Data_Pipeline\data\analytics\geo_points")

# ISO3 helper
def country_to_iso3(name):
    try:
        return pycountry.countries.lookup(name).alpha_3
    except:
        return None

geo_summary["iso3"] = geo_summary["country"].apply(country_to_iso3)

# Choropleth map
fig_choro = px.choropleth(
    geo_summary,
    locations="iso3",
    color="tower_count",
    hover_name="country",
    color_continuous_scale="Viridis",
    title="Number of towers per country"
)

# Bar chart - towers per country
fig_bar_towers = px.bar(
    geo_summary.sort_values("tower_count", ascending=False),
    x="tower_count",
    y="country",
    orientation="h",
    title="Number of towers per country",
    height=3000
)

# Bar chart - avg tower range per country
fig_bar_range = px.bar(
    geo_summary.sort_values("avg_range_m", ascending=False),
    x="avg_range_m",
    y="country",
    orientation="h",
    title="Average tower range per country",
    height=3000
)

# Top operators (global)
op_global = operator_summary.groupby("network_name")["tower_count"].sum().reset_index().sort_values("tower_count", ascending=False)
fig_op_global = px.bar(
    op_global,
    x="tower_count",
    y="network_name",
    orientation="h",
    title="Top operators worldwide by number of towers",
    height=3000
)

# Export all plots to dashboard
figs = [
    fig_choro,
    fig_bar_towers,
    fig_bar_range,
    fig_op_global,
]

with open("geotelecom_dashboard.html", "w", encoding="utf-8") as f:
    for fig in figs:
        f.write(pio.to_html(fig, full_html=False, include_plotlyjs='cdn'))
        f.write("<hr>")

print("Dashboard saved as geotelecom_dashboard.html")