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


def compute_averages(
    df: pd.DataFrame, variables: list, race: str, area: str, ruca: bool = False
) -> pd.DataFrame:
    """
    Compute weighted averages of the given variables by race and area.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame with the census data.
    variables : list of str
        The variables for which to compute averages.
    race : str
        The race for which to compute averages.
    area : str
        The area for which to compute averages. If `ruca` is `True`, this should be either 'urban' or 'rural'.
    ruca : bool, optional
        Whether to use RUCA codes to classify areas as 'urban' or 'rural'. The default is `False`.

    Returns
    -------
    df : pd.DataFrame
        The DataFrame with the computed averages.

    Note
    ----
    If `ruca` is `True`, the function first converts the RUCA codes to 'urban' or 'rural'
    (based on the classification that codes 1-3 are metropolitan (urban) and 4-10 are non-metropolitan (rural)),
    and then filters the DataFrame based on race and the RUCA classification.
    """
    if not ruca:
        df = df[(df["race"] == race) & (df["area"] == area)]
    else:
        df["ruca"] = df["ruca"].apply(
            lambda x: "urban" if int(x) in range(1, 4) else "rural"
        )
        df = df[(df["race"] == race) & (df["ruca"] == area)]

    for var in variables:
        df[var] = df[var].astype(float)  # Ensure the variables are treated as numbers

    df["total_population"] = df[variables].sum(axis=1)

    for var in variables:
        df[f"average_{var}"] = df[var] / df["total_population"]

    return df


def process_census_data(
    variables: list,
    year: int,
    dataset: str,
    area: str,
    api_key: str,
    race: str,
    census_area: str,
    ruca: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Download and process census data to compute weighted averages of the given variables by race and area.

    Parameters
    ----------
    variables : list of str
        The variables for which to download data and compute averages.
    area : str
        The area for which to download data. This should be a valid geography type for the Census API.
    api_key : str
        The API key to use for the Census API.
    race : str
        The race for which to compute averages.
    census_area : str
        The specific census area for which to compute averages. If `ruca` is `True`, this should be either 'urban' or 'rural'.
    ruca : bool, optional
        Whether to use RUCA codes to classify areas as 'urban' or 'rural'. The default is `False`.

    Returns
    -------
    df : pd.DataFrame
        The DataFrame with the downloaded census data.
    avg_df : pd.DataFrame
        The DataFrame with the computed averages.

    """
    df = download_census_data(
        api_key, year, dataset, variables, geography=area, ruca=ruca
    )
    avg_df = compute_averages(df, variables, race, census_area, ruca=ruca)
    return df, avg_df
