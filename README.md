# 📊 US Census Data Analysis with Python 🐍

This repository provides Python scripts for downloading and analyzing US Census data. It includes handy functions to retrieve data from the Census API, compute population-based averages, and returns both the raw and processed data as pandas DataFrames. 

## 📚 Functions

- `download_census_data(variables, area, api_key)`: 🌐 Downloads data from the Census API.

- `compute_averages(df, variables)`: 🧮 Calculates averages of the specified variables using shares of population by race.

- `process_census_data(variables, area, api_key)`: 🔄 Main function that combines the above functions.

## ▶️ Usage

Replace `"B02001_002E", "B02001_003E"` with your variables, `"county"` with your area, and `"YOUR_API_KEY"` with your actual Census API key:

```python
original_df, averages_df = process_census_data(["B02001_002E", "B02001_003E"], "county", "YOUR_API_KEY")
```

## 📦 Requirements

Requires Python 3, and these Python libraries:

- requests
- pandas

Install with pip:

```shell
pip install requests pandas
```

## ⚠️ Disclaimer

This is a basic example, and should serve as a starting point for data analysis projects.
