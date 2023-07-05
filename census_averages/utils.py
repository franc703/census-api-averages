import requests
import ast
import os
import glob
import pandas as pd
import geopandas as gpd
from zipfile import ZipFile
from tqdm import tqdm


race_code_map = {
    "white": "B02001_002E",
    "black": "B02001_003E",
    "native": "B02001_004E",
    "asian": "B02001_005E",
}


def compute_weights(df, races, level):
    for race in races:
        total_population_race = df[race_code_map[race]].sum()
        df[f"weight_{race}_{level}"] = df[race_code_map[race]] / total_population_race
    return df


def compute_weighted_averages(df, races, level):
    weighted_averages_df = pd.DataFrame()
    for race in races:
        weight_column = f"weight_{race}_{level}"
        weighted_averages_df[race] = df[weight_column] * df[race_code_map[race]]
    return weighted_averages_df


def download_acs_data(
    variables,
    user_key,
    summary=None,
    decennial=False,
    host="https://api.census.gov/data/",
    year="2019",
    dataset_acronym="acs5",
    g="?get=",
    location="block group",
    state="27",
    county="*",
    tract="*",
    block_group="*",
):
    variables = ",".join(variables)
    location_key_map = {
        "block group": f"&for=block%20group:{block_group}&in=state:{state}%20county:{county}%20tract:{tract}",
        "tract": f"&for=tract:{tract}&in=state:{state}%20county:{county}",
        "county": f"&for=county:{county}&in=state:{state}",
        "state": f"&for=state:{state}",
    }

    query_url = f"{host}{year}/{dataset_acronym}{g}{variables}&key={user_key}{location_key_map[location]}"
    response = requests.get(query_url)
    raw_string = response.text
    cleaned_string = raw_string.replace("\n", "")
    list_object = ast.literal_eval(cleaned_string)
    df = pd.DataFrame(list_object[1:], columns=list_object[0])
    return df


def download_shapefile(year: str, state: str, dest_dir: str):
    geographies = ["state", "county", "tract", "bg"]

    for geography in geographies:
        zip_path = os.path.join(dest_dir, f"tl_{year}_{geography}.zip")

        if not os.path.exists(zip_path):
            if geography in ["tract", "bg"]:
                url = f"https://www2.census.gov/geo/tiger/TIGER{year}/{geography.upper()}/tl_{year}_{state}_{geography}.zip"
            else:
                url = f"https://www2.census.gov/geo/tiger/TIGER{year}/{geography.upper()}/tl_{year}_us_{geography}.zip"

            response = requests.get(url, stream=True)

            if response.status_code == 200:
                with open(zip_path, "wb") as file:
                    for chunk in tqdm(
                        response.iter_content(chunk_size=1024),
                        desc=f"Downloading {geography} data",
                    ):
                        if chunk:
                            file.write(chunk)

                with ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(dest_dir)
            else:
                print(f"Error downloading file from the URL: {url}")
        else:
            print(f"File already exists: {zip_path}")


def merge_areas(
    data_dir: str, year: str, areas_level: str = "bg", save_file: bool = True
):
    areas_mapping = {
        "bg": "block group",
        "tract": "tract",
        "county": "county",
        "state": "state",
    }

    outcome_file = os.path.join(data_dir, "outcome_geocoded.csv")

    outcome_df = pd.read_csv(outcome_file)
    outcome_gdf = gpd.GeoDataFrame(
        outcome_df,
        geometry=gpd.points_from_xy(outcome_df.longitude, outcome_df.latitude),
        crs="EPSG:4326",
    )

    shapefile_path = glob.glob(
        os.path.join(data_dir, f"tl_{year}_*_{areas_level}.shp")
    )[0]
    areas_gdf = gpd.read_file(shapefile_path)

    outcome_gdf = gpd.sjoin(outcome_gdf, areas_gdf, how="left", op="within")

    if areas_level == "bg":
        outcome_gdf = outcome_gdf.rename(
            columns={
                "STATEFP": "state_fips",
                "COUNTYFP": "county_fips",
                "TRACTCE": "tract_fips",
                "BLKGRPCE": "block_group_fips",
                "NAMELSAD": "block_group_name",
            }
        )
    elif areas_level == "tract":
        outcome_gdf = outcome_gdf.rename(
            columns={
                "STATEFP": "state_fips",
                "COUNTYFP": "county_fips",
                "TRACTCE": "tract_fips",
                "NAMELSAD": "tract_name",
            }
        )
    elif areas_level == "county":
        outcome_gdf = outcome_gdf.rename(
            columns={
                "STATEFP": "state_fips",
                "COUNTYFP": "county_fips",
                "NAMELSAD": "county_name",
            }
        )
    else:  # areas_level == "state"
        outcome_gdf = outcome_gdf.rename(
            columns={"STATEFP": "state_fips", "NAMELSAD": "state_name"}
        )

    # Save the merged dataframe as a new CSV file if save_file is True
    if save_file:
        output_file = os.path.join(
            data_dir, f"outcome_geocoded_{areas_mapping[areas_level]}.csv"
        )
        outcome_gdf.to_csv(output_file, index=False)

    return outcome_gdf
