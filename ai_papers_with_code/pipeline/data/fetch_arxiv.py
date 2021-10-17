# Fetch arxiv data from S3

from boto3.session import Session
import dotenv
import logging
import os

from ai_papers_with_code import PROJECT_DIR

dotenv.load_dotenv(f"{PROJECT_DIR}/.env")
DATA_PATH = f"{PROJECT_DIR}/inputs/data"

key = os.getenv("aws_access_key_id")
secret_key = os.getenv("aws_secret_access_key")


def main():
    session = Session(aws_access_key_id=key, aws_secret_access_key=secret_key)
    s3 = session.resource("s3")

    pwc_bucket = s3.Bucket("ai-papers-with-code")

    for s3_file in pwc_bucket.objects.all():
        logging.info(f"downloading {s3_file}")

        with open(f"{PROJECT_DIR}/{s3_file}", "wb") as out:
            pwc_bucket.download_fileobj(s3_file, out)


if name == "__main__":
    main()
