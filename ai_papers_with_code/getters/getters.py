# Data getters
import datetime
import gzip
import json
import logging
import os

import numpy as np
import pandas as pd

from ai_papers_with_code import PROJECT_DIR

DATA_PATH = f"{PROJECT_DIR}/inputs/data"

# Papers with code scripts
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


# arXiv scripts


def fix_carnegie(institute):
    """Fixes a bad match with carnegie mellon"""
    logging.info("fixing bad carnegie match")

    carnegie = institute.query("name=='Carnegie Mellon University Australia'")
    no_carnegie = institute.query("name!='Carnegie Mellon University Australia'")

    carnegie_fixed = []

    for _id, r in carnegie.iterrows():

        row = r.copy()
        row["institute_id"] = "carnegie_fixed"
        row["grid_id"] = "carnegie_fixed"
        row["name"] = "Carnegie Mellon University"
        row["lat"] = np.nan
        row["lng"] = np.nan
        row["city"] = "Pittsburgh"
        row["country"] = "USA"
        row["country_code"] = "US"

        carnegie_fixed.append(row)

    carnegie_fixed_df = pd.DataFrame(carnegie_fixed)

    return pd.concat([no_carnegie, carnegie_fixed_df]).reset_index(drop=True)


def add_author(art_data, art, org="deepmind"):
    """Adds a new institutional author to an article list of contributing institutes"""

    if org == "deepmind":

        values = [
            art,
            "extra_deepmind",
            np.nan,
            np.nan,
            "extra_deepmind",
            "DeepMind",
            np.nan,
            np.nan,
            "London",
            "United Kingdom",
            "GB",
            "Company",
            "UKI",
            "UKI3",
            "UKI32",
        ]

    else:

        values = [
            art,
            "extra_openai",
            np.nan,
            np.nan,
            "extra_openai",
            "OpenAI",
            np.nan,
            np.nan,
            "San Francisco",
            "USA",
            "US",
            "Not-profit",
            np.nan,
            np.nan,
            np.nan,
        ]

    return art_data.append(pd.Series(values, index=art_data.columns), ignore_index=True)


def make_institute_article_table():
    """Processes the institute data to incorporate deepmind and openai"""

    institute = fix_carnegie(
        pd.read_csv(
            f"{PROJECT_DIR}/inputs/data/arxiv_institutes.csv", dtype={"article_id": str}
        )
    )

    ai_ids = set(get_arxiv_papers().query("is_ai==True")["article_id"])

    # We want one article x institution
    institute["name"] = institute["name"].apply(lambda x: x.split(" (")[0].strip())
    institute_deduped = institute.drop_duplicates(["article_id", "name"])

    with open(f"{PROJECT_DIR}/inputs/data/scraped_arxiv_ids.json", "r") as infile:
        arxiv_ids = json.load(infile)

    dm_papers = set(
        [key.split("/")[-1] for key, value in arxiv_ids.items() if value == "DeepMind"]
    )
    oai_papers = set(
        [key.split("/")[-1] for key, value in arxiv_ids.items() if value == "OpenAI"]
    )

    logging.info("Updating deepmind")

    institute_dm = institute_deduped.loc[
        institute_deduped["article_id"].isin(dm_papers)
    ]
    revised_rows = []

    # We loop over articles. If there is a google author we replace it with deepmind, otherwise we add deepmind
    for _id in set(institute_dm["article_id"]):

        art_data = institute_dm.query(f"article_id == '{_id}'")

        if "Google" in list(art_data["name"]):

            art_data_ = art_data.query("name!='Google'")

            revised_rows.append(add_author(art_data, _id, "deepmind"))
        else:
            revised_rows.append(add_author(art_data, _id, "deepmind"))

    institute_dm_revised = pd.concat(revised_rows)

    institute_dm_update = pd.concat(
        [
            institute_deduped.loc[~institute_deduped["article_id"].isin(dm_papers)],
            institute_dm_revised,
        ]
    ).reset_index(drop=True)

    revised_rows = []

    logging.info("Updating OpenAI")

    institute_oai = institute_dm_update.loc[
        institute_dm_update["article_id"].isin(oai_papers)
    ]

    # We loop over articles and add OpenAI as participant institution
    for _id in set(institute_oai["article_id"]):
        art_data = institute_oai.query(f"article_id == '{_id}'")

        revised_rows.append(add_author(art_data, _id, "openai"))

    institute_oai_revised = pd.concat(revised_rows)

    institute_final = pd.concat(
        [
            institute_dm_update.loc[
                ~institute_dm_update["article_id"].isin(oai_papers)
            ],
            institute_oai_revised,
        ]
    ).reset_index(drop=True)

    institute_final["is_ai"] = institute_final["article_id"].isin(ai_ids)

    return institute_final


def get_arxiv_papers():

    return pd.read_csv(
        f"{PROJECT_DIR}/inputs/data/arxiv_ai_papers.csv", dtype={"article_id": str}
    )


def get_arxiv_institutes(processed=True):

    if processed is True:

        proc_path = f"{PROJECT_DIR}/inputs/data/arxiv_institute_processed.csv"

        if os.path.exists(proc_path) is True:
            return pd.read_csv(proc_path, dtype={"article_id": str})

        else:
            logging.info("Processing and saving article - institutes")
            art_inst_proc = make_institute_article_table().drop(
                axis=1, labels=["grid_id"]
            )
            art_inst_proc.to_csv(proc_path, index_label=False)
            return art_inst_proc
    else:
        return pd.read_csv(
            f"{PROJECT_DIR}/inputs/data/arxiv_institutes.csv", dtype={"article_id": str}
        )
