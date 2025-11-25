# GeoTelecom-Data-Pipeline
Cell Towers Worldwide: ETL, AWS Glue &amp; Data Visualization

## Kaggle dataset:
https://www.kaggle.com/datasets/zakariaeyoussefi/cell-towers-worldwide-location-data-by-continent/data

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
<img width="1918" height="907" alt="towers" src="https://github.com/user-attachments/assets/26f4c859-e328-4a43-8274-ed65f4bc50d5" />



   - Heatmap of tower density (Folium)
<img width="1916" height="907" alt="heatmap" src="https://github.com/user-attachments/assets/2e77c4cd-774b-4422-9d71-e3801a1057a6" />

<img width="1917" height="908" alt="heatmap_waw" src="https://github.com/user-attachments/assets/430bdf6a-f129-4195-9216-373afb4bc4b9" />



   - HTML dashboard (Plotly) including:  
     - Choropleth map
<img width="1900" height="892" alt="number-of-towers" src="https://github.com/user-attachments/assets/b4d881a8-bad8-41ff-ab83-3588d8174263" />



     - Bar charts (number of towers, average range, top operators)
<img width="1877" height="897" alt="number-of-towers-2" src="https://github.com/user-attachments/assets/b86573ef-9dcb-4462-bb01-60790721564a" />

---

Full datasets are are not included in the repository.
AWS Glue jobs can be run directly in the AWS Console or locally using PySpark.
