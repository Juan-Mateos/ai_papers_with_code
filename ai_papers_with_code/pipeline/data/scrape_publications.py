import json
import logging
import os
import re
import yaml
from time import sleep

import requests
from bs4 import BeautifulSoup
from ai_papers_with_code import PROJECT_DIR

DM_URL = "https://deepmind.com/api/search/?content_type=research&filters=%7B%22collection%22:%5B%22Publications%22%5D%7D&page={t}&pagelen=21&q=&sort=relevance"
N_PAGES = 80
OAI_URL = "https://openai.com/papers/"
FILE_PATH = f"{PROJECT_DIR}/inputs/data/scraped_arxiv_ids.json"


def webscraping():
    if os.path.exists(FILE_PATH) is True:
        logging.info("Already collected data")

    else:
        logging.info("Scraping deepmind publications")

        # Collect the DeepMind arXiv papers

        dm_arx = []

        for p in range(1, N_PAGES):

            sleep(2)

            dm_page = requests.get(DM_URL.format(t=p))
            content = (dm_page.content).decode()
            parsed = json.loads(content[6:])

            for r in parsed["results"]:
                if "download" in r.keys():
                    url = r["download"]
                    if "arxiv" in str(url):
                        # Change links to pdfs to links to abstracts
                        if url[-4:] == ".pdf":  # Removes pdf format
                            url = url[:-4]
                        url = re.sub("pdf", "abs", url)
                        dm_arx.append(url)

        # Collect OpenAI papers
        logging.info("Scraping openai publications")

        # Download and parse the data
        oai = BeautifulSoup(requests.get(OAI_URL).content)

        # Unique links in the page that contain arXiv
        oai_arx = set(
            [x for x in [x.get("href") for x in oai.find_all("a")] if "arxiv" in x]
        )

        # Conclude
        dm_lu = {x: "DeepMind" for x in dm_arx}
        oai_lu = {x: "OpenAI" for x in oai_arx}

        combined = {**dm_lu, **oai_lu}

        with open(FILE_PATH, "w") as outfile:
            json.dump(combined, outfile)


if __name__ == "__main__":

    webscraping()
