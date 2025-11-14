# GeoTelecom Data Pipeline - Step-by-Step Guide

This document provides step-by-step instructions to replicate the project.

## **1. Download Raw Data**
https://www.kaggle.com/datasets/zakariaeyoussefi/cell-towers-worldwide-location-data-by-continent/data

## **2. Set up AWS S3 Bucket**
- Create bucket: `geotelecom-data`
- Folders inside bucket:
  - `raw/` → for raw CSVs
  - `processed/` → cleaned and unified Parquet files
  - `analytics/` → aggregated datasets

## **3. Run Glue ETL Job**
- Job name: `WorldCellTowers_ETL`
- Input: raw CSV files from S3
- Output: cleaned Parquet files in `processed/`
- Steps include:
  1. Reading CSVs
  2. Renaming columns
  3. Removing duplicates and missing coordinates
  4. Casting column types
  5. Saving as Parquet
 
## **4. Optional: Tests / Validation in Athena
- Connect Athena to S3 bucket: `geotelecom-data/processed/`
- Example queries:
  
  a. Count towers per country:
     
     SELECT country, COUNT(*) AS tower_count
     FROM etl_world_towers_parquet
     GROUP BY country
     ORDER BY tower_count DESC;
     
  b. Check for missing coordinates:
  
     SELECT *
     FROM etl_world_towers_parquet
     WHERE latitude IS NULL OR longitude IS NULL;
  
  c. Validate timestamps:
    
     SELECT MIN(created_ts), MAX(updated_ts)
     FROM etl_world_towers_parquet;
 
- These tests ensure data quality before visualization or further analysis.

## **5. Run Glue Aggregation Spark Jobs**
- Job name: `WorldCellTowers_GeoAnalytics`
- Aggregations:
  - `geo_summary` → towers per country and continent
  - `operator_summary` → towers per operator
  - `geo_points` → tower coordinates
- Output folder: `analytics/` in S3

## **6. Visualize Data Locally**
1. Set up Python environment:
- Run visualization scripts:
python src/geo_viz_summary.py
python src/geo_viz_heatmap.py
python src/geo_viz_points.py

Output: interactive HTML dashboards in project folder


