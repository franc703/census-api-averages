import pandas as pd
import requests
from io import BytesIO


def _download_ruca_codes() -> pd.DataFrame:
    """
    Downloads the RUCA codes data from the web.

    Returns:
        pandas.DataFrame: A DataFrame containing the RUCA codes data.
    """
    # The actual link to the data file
    data_link = (
        "https://www.ers.usda.gov/webdocs/DataFiles/53241/ruca2010revised.xlsx?v=7676.8"
    )
    response = requests.get(data_link, timeout=10)  # Add a timeout of 10 seconds
    response.raise_for_status()  # Raise an error if the GET request was not successful

    # Convert the response content to a BytesIO object and load it into a DataFrame
    file = BytesIO(response.content)
    df = pd.read_excel(file)

    return df


def download_census_data(
    api_key: str,
    year: int,
    dataset: str,
    variables: list,
    geography: str = "county",
    state: str = "*",
    county: str = "*",
    tract: str = "*",
    block_group: str = "*",
    ruca: bool = False,
):
    """
    Download data from the US Census Bureau API.
    The geography can be 'county', 'tract', 'block group', or 'RUCA'.
    If 'RUCA' is chosen, the function will download the RUCA codes and merge them with the census data.
    """
    # The base URL for the API
    url = f"https://api.census.gov/data/{year}/{dataset}"

    # The variables to download
    get_vars = ",".join(variables)

    # Construct the API URL
    data_url = f"{url}?get={get_vars}&for={geography}:*&in=state:{state}%20county:{county}&key={api_key}"
    if tract != "*":
        data_url += f"%20tract:{tract}"
    if block_group != "*":
        data_url += f"%20block%20group:{block_group}"

    # Make the API request
    response = requests.get(data_url, timeout=10)  # Added timeout argument

    # Convert the response to JSON
    data = response.json()

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])

    # If RUCA is chosen, download the RUCA codes and merge them with the census data
    if ruca:
        ruca_codes = _download_ruca_codes()
        df = df.merge(ruca_codes, how="left", on="GEO_ID")

    return df


def compute_weighted_averages(
    df: pd.DataFrame, variables: list, population_var: str, group_vars: list
) -> pd.DataFrame:
    """
    Compute weighted averages of the given variables based on population.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame with the census data.
    variables : list of str
        The variables for which to compute averages.
    population_var : str
        The population variable to use for weighting.
    group_vars : list of str
        The variables to group by when computing averages.

    Returns
    -------
    df : pd.DataFrame
        The DataFrame with the computed weighted averages.
    """
    # Ensure there are no missing values
    df.fillna(0, inplace=True)

    df[population_var] = df[population_var].astype(float)

    for var in variables:
        df[var] = df[var].astype(float) * df[population_var]

    df = df.groupby(group_vars).sum()

    for var in variables:
        df[f"average_{var}"] = df[var] / df[population_var]

    return df.reset_index()


def process_census_data(
    api_key: str,
    year: int,
    dataset: str,
    variables: list,
    geography: str,
    population_var: str,
    group_vars: list,
    ruca: bool = False,
):
    """
    Download and process census data to compute weighted averages of the given variables.

    Parameters
    ----------
    api_key : str
        The API key to use for the Census API.
    year : int
        The year for which to download data.
    dataset : str
        The dataset from which to download data.
    variables : list of str
        The variables for which to download data and compute averages.
    geography : str
        The geography for which to download data.
    population_var : str
        The population variable to use for weighting.
    group_vars : list of str
        The variables to group by when computing averages.
    ruca : bool, optional
        Whether to use RUCA codes to classify areas. The default is `False`.

    Returns
    -------
    avg_df : pd.DataFrame
        The DataFrame with the computed weighted averages.
    """
    df = download_census_data(api_key, year, dataset, variables, geography, ruca=ruca)

    # Ensure the required columns exist in the DataFrame
    assert set(variables).issubset(
        df.columns
    ), "Some variables are not in the DataFrame"
    assert population_var in df.columns, f"'{population_var}' is not in the DataFrame"
    assert set(group_vars).issubset(
        df.columns
    ), "Some group variables are not in the DataFrame"

    avg_df = compute_weighted_averages(df, variables, population_var, group_vars)

    return avg_df
