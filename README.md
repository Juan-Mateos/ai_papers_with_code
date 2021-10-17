# ai_papers_with_code

## Setup

- Meet the data science cookiecutter [requirements](http://nestauk.github.io/ds-cookiecutter/quickstart), in brief:
  - Install: `git-crypt` and `conda`
  - Have a Nesta AWS account configured with `awscli`
- Run `make install` to configure the development environment:
  - Setup the conda environment
  - Configure pre-commit
  - Configure metaflow to use AWS

## Features

### Papers with code

#### Fetch data

Run `python ai_papers_with_code/pipeline/data/fetch_pwc_data.py` to fetch the papers with code datasets.

The data is saved in `inputs/data`.

### Fetch arXiv data

Run `python ai_papers_with_code/pipeline/data/fetch_arxiv.py` to fetch the arXiv tables from S3.

The data is saved in `inputs/data`

NB this requires AWS credentials

TODO: Make this available to anyone

Run `python ai_papers_with_code/pipeline/data/scrape_publications.py` to scrape arXiv publications from DeepMind and OpenAI's websites

### Read data

Use the getters in `ai_papers_with_code/getters/getters.py` to get papers with code tables.

## Contributor guidelines

[Technical and working style guidelines](https://github.com/nestauk/ds-cookiecutter/blob/master/GUIDELINES.md)

---

<small><p>Project based on <a target="_blank" href="https://github.com/nestauk/ds-cookiecutter">Nesta's data science project template</a>
(<a href="http://nestauk.github.io/ds-cookiecutter">Read the docs here</a>).
</small>
