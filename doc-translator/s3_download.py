import json
import jsonlines
import logging
import os
import pathlib
import subprocess
from subprocess import PIPE
import traceback

import lxml
from lxml.html.clean import clean_html

from helper import calc_time
import s3util

formatter = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(format=formatter)
logger = logging.getLogger("translator")
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

BUCKET = os.environ.get("BUCKET")
PREFIX = os.environ.get("PREFIX")
TIMESTAMP = os.environ.get("TIMESTAMP")
INPUT_PREFIX = PREFIX + "/" + TIMESTAMP + "/"


def filter_data(target_path: str):
    """
    Filter and normalize AWS document data
    """
    jsonl_path = pathlib.Path(target_path)
    if jsonl_path.is_dir() is not True:
        logger.error("{} is not directory".format(target_path))
        return []

    doc_list = []

    for jsonl in jsonl_path.glob("*.jsonl"):
        logger.info("Processing {}".format(jsonl.absolute()))
        with jsonlines.open(jsonl.absolute()) as reader:
            for data in reader:
                url = data["url"]
                logger.info("  Start {}".format(url))

                # Filter by URL
                if data["status"] != 200:
                    logger.warning("No 200")
                    continue
                if "apireference" in url.lower():
                    logger.warning("API Reference")
                    continue
                if "/cli/" in url.lower():
                    logger.warning("AWS CLI")
                    continue
                if "/code-samples/" in url.lower():
                    logger.warning("code samples")
                    continue

                try:
                    h = lxml.html.fromstring(data["html"])
                    title = h.cssselect("title")[0].text

                    product = None
                    guide = None
                    try:
                        for meta in h.cssselect("meta"):
                            if meta.get("name") == "product":
                                product = meta.get("content")
                            if meta.get("name") == "guide":
                                guide = meta.get("content")
                    except Exception:
                        pass

                    # Normalize by clean_html
                    # https://lxml.de/lxmlhtml.html#cleaning-up-html
                    content = clean_html(h).text_content()
                    content = "".join([line.strip()
                                       for line in content.splitlines()])

                    doc_list.append(
                        {
                            "url": url,
                            "product": product,
                            "guide": guide,
                            "title": title,
                            "content": content,
                            "last_modified": data["last_modified"],
                            "crawled_at": data["crawled_at"],
                        }
                    )

                except Exception:
                    logger.exception(traceback.format_exc())
                    logger.warning("  Skipping this URL... {}".format(url))
                    continue

    return doc_list


def download_input_from_s3():
    """
    Download gzip files to /tmp/INPUT_PREFIX
    """
    succeeded = s3util.download_dir(
        bucket=BUCKET, prefix=INPUT_PREFIX, local="/tmp")
    if succeeded is False:
        logger.error("S3 Download failed")
        return 1
    logger.info("S3 download complete")

    # gunzip /tmp/INPUT_PREFIX/*.gz
    try:
        subprocess.run(
            "gunzip /tmp/{}/*.gz".format(INPUT_PREFIX),
            shell=True,
            stdout=PIPE,
            stderr=PIPE,
            text=True,
        )
    except Exception:
        trace = traceback.format_exc()
        logger.error("Error while gunzip /tmp/{}/*.gz".format(INPUT_PREFIX))
        logger.exception(trace)
        return 1

    # Read /tmp/INPUT_PREFIX jsonline and filter and append list
    filtered_data = filter_data("/tmp/{}".format(INPUT_PREFIX))
    logger.info("Document size: {}".format(len(filtered_data)))

    return filtered_data


@calc_time
def main():
    """
    Main logic
    """
    if BUCKET is None or PREFIX is None or TIMESTAMP is None:
        logger.error("BUCKET or PREFIX or TIMESTAMP is None")
        return 1

    logger.info("BUCKET: {}".format(BUCKET))
    logger.info("PREFIX: {}".format(PREFIX))
    logger.info("TIMESTAMP: {}".format(TIMESTAMP))

    # Download from S3
    filtered_data = download_input_from_s3()

    # Store in local
    with jsonlines.open("./filtered_rawdata.jsonl", mode="w") as writer:
        writer.write_all(filtered_data)

if __name__ == "__main__":
    main()
