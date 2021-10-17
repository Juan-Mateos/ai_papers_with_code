import logging

import requests

from ai_papers_with_code import PROJECT_DIR

DATA_PATH = f"{PROJECT_DIR}/inputs/data"


def fetch_pwc():
    """
    Fetches the pwc data
    """
    for url in [
        "https://production-media.paperswithcode.com/about/papers-with-abstracts.json.gz",
        "https://production-media.paperswithcode.com/about/links-between-papers-and-code.json.gz",
        "https://production-media.paperswithcode.com/about/evaluation-tables.json.gz",
        "https://production-media.paperswithcode.com/about/methods.json.gz",
        "https://production-media.paperswithcode.com/about/datasets.json.gz",
    ]:
        logging.info(f"Fetching and saving url {url}")
        fetch_save(url)


def fetch_save(url):
    """Fetch and save a URL"""

    name = url.split("/")[-1]
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(f"{DATA_PATH}/{name}", "wb") as f:
            f.write(response.raw.read())
    else:
        logging.info(f"Failed {url} download")


if __name__ == "__main__":
    fetch_pwc()
