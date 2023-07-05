import click
import pandas as pd
import utils
from tabulate import tabulate


@click.command()
@click.option("--year", default=2019, help="The year for which to download data.")
@click.option("--state", default="27", help="The state code.")
@click.option("--county", default="*", help="The county code.")
@click.option("--tract", default="*", help="The tract code.")
@click.option("--block_group", default="*", help="The block group code.")
@click.option("--user_key", prompt=True, help="The Census API Key.")
@click.option(
    "--variables",
    default=["B02001_002E", "B02001_003E", "B02001_004E", "B02001_005E"],
    multiple=True,
    help="The list of variables to extract.",
)
@click.option("--outcome_data_path", default=None, help="The path to the outcome data.")
@click.option(
    "--aggregate_level",
    default="tract",
    help="The level at which to aggregate the data.",
)
@click.option(
    "--average_data_path", default=None, help="The path to save the average data."
)
@click.option(
    "--display_results",
    is_flag=True,
    help="Flag to indicate whether to display the results.",
)
@click.option("--shapefile_path", default=None, help="The path to the shapefiles.")
@click.option(
    "--data_dir",
    default=None,
    help="The directory where the provider locations data and the shapefiles are located.",
)
def main(
    year,
    state,
    county,
    tract,
    block_group,
    user_key,
    variables,
    outcome_data_path,
    aggregate_level,
    average_data_path,
    display_results,
    shapefile_path,
    data_dir,
):
    # Download ACS data
    acs_data = download_acs_data(
        variables,
        user_key,
        year=year,
        state=state,
        county=county,
        tract=tract,
        block_group=block_group,
    )

    # Load outcome data
    outcome_data = pd.read_csv(outcome_data_path)

    # Merge the areas with the outcome data
    merged_data = merge_areas(data_dir, shapefile_path, outcome_data, year)

    # Compute weights
    weights = compute_weights(acs_data, aggregate_level)

    # Merge outcome data and weights
    merged_data = pd.merge(
        outcome_data,
        weights,
        on=["state", "county", "tract", "block_group"],
        how="left",
    )

    # Compute weighted averages
    average_data = compute_weighted_averages(merged_data, aggregate_level)

    # Save average data
    average_data.to_csv(average_data_path, index=False)

    # Display results
    if display_results:
        print(tabulate(average_data, headers="keys", tablefmt="psql"))


if __name__ == "__main__":
    main()
