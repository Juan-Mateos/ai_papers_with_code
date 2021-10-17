# Data getters
import gzip

import numpy as np
import pandas as pd


def make_year(x):
    """Extracts year from a datetime.datetime object"""

    return x.year if pd.isnull(x) is False else np.nan


def read_parse(file_name):
    """Reads, decompresses and parses a pwc file"""
    with gzip.open(f"{DATA_PATH}/{file_name}", "rb") as f:
        file_content = f.read()

    return json.loads(file_content)


def parse_date_string(x, _format="%Y-%m-%d"):

    return (
        datetime.datetime.strptime(x, "%Y-%m-%d") if pd.isnull(x) is False else np.nan
    )


def make_month_year(x):

    return datetime.datetime(x.year, x.month, 1) if pd.isnull(x) is False else np.nan


def make_empty_list_na(df, variables):
    """Remove empty lists with np.nans in a dataframe"""

    df_ = df.copy()
    for v in variables:

        df_[v] = df[v].apply(lambda x: x if len(x) > 0 else np.nan)

    return df_


def get_pwc_papers():
    """Get papers table"""
    # Read and parse the data
    paper_json = read_parse("papers-with-abstracts.json.gz")

    # Fix missing values
    paper_df = pd.DataFrame(paper_json)
    paper_df_clean = make_empty_list_na(paper_df, ["tasks", "methods"]).replace(
        {None: np.nan, "": np.nan}
    )

    paper_df_clean["date"] = paper_df_clean["date"].apply(
        lambda x: parse_date_string(x)
    )
    paper_df_clean["month_year"] = paper_df_clean["date"].apply(
        lambda x: make_month_year(x)
    )
    paper_df_clean["year"] = paper_df_clean["date"].apply(lambda x: make_year(x))

    return paper_df_clean


def get_pwc_code_lookup():
    """Get papers to code lookup"""

    paper_code_table = read_parse("links-between-papers-and-code.json.gz")

    pc_df = pd.DataFrame(paper_code_table).replace({None: np.nan})

    return pc_df


def get_pwc_methods():
    """Get methods"""

    method_json = read_parse("methods.json.gz")

    method_df = pd.DataFrame(method_json).replace({None: np.nan})

    return method_df


def get_pwc_data():
    """Get data"""

    data_json = read_parse("datasets.json.gz")

    data_df = pd.DataFrame(data_json)

    data_df_clean = make_empty_list_na(
        data_df, ["languages", "tasks", "modalities", "data_loaders"]
    ).replace({None: np.nan})

    data_df_clean["date"] = data_df_clean["introduced_date"].apply(
        lambda x: parse_date_string(x)
    )

    data_df_clean["month_year"] = data_df_clean["date"].apply(
        lambda x: make_month_year(x)
    )

    data_df_clean["year"] = data_df_clean["date"].apply(lambda x: make_year(x))

    return data_df_clean
