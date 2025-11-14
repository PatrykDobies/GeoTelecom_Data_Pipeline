# GeoTelecom_Data_Pipeline
Cell Towers Worldwide: ETL, AWS Glue &amp; Data Visualization

---

## Project Description

This project demonstrates the ETL and analysis of worldwide cell towers data.  
It consists of three main stages:

1. **ETL in AWS Glue**  
   - Combine CSV files of towers from different continents  
   - Clean data (remove duplicates, handle missing values)  
   - Standardize column names and convert data types  
   - Save cleaned data in Parquet format to S3

2. **Aggregations**  
   - `geo_summary` – statistics per country/continent  
   - `operator_summary` – statistics per operator/country  
   - `geo_points` – detailed tower information with timestamps

3. **Data Visualization (locally in Python)**  
   - Point map of towers (Folium)  
   - Heatmap of tower density (Folium)  
   - HTML dashboard (Plotly) including:  
     - Choropleth map  
     - Bar charts (number of towers, average range, top operators)

---

## Repository Structure

GeoTelecom_Data_Pipeline/
│
├─ src/
│ ├─ glue_jobs/
│ └─ viz/
├─ data/ (sample)
├─ README.md
├─ .gitignore

---

Full datasets are stored on S3 or Kaggle and are not included in the repository.
AWS Glue jobs can be run directly in the AWS Console or locally using PySpark.
All visualizations are saved as interactive HTML files.