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

from helper import calc_time, url_to_path
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
                            "raw_html": data["html"],
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
    Download gzip files to /tmp/INPUT_PREFIX/raw
    """
    succeeded = s3util.download_dir(
        bucket=BUCKET, prefix=INPUT_PREFIX, local="/tmp")
    if succeeded is False:
        logger.error("S3 Download failed")
        return 1
    logger.info("S3 download complete")

    # gunzip /tmp/INPUT_PREFIX/raw/*.gz
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


def upload_rawdata_to_s3(filtered_data):
    """
    Upload to S3

    S3://BUCKET/PREFIX/merged/filtered_rawdata_TIMESTAMP.jsonl.gz
    """
    try:
        filtered_data_bytes = "\n".join([json.dumps(d) for d in filtered_data]).encode(
            "utf-8"
        )
        key = "{}{}/filtered_rawdata_{}.jsonl.gz".format(
            INPUT_PREFIX, "merged", TIMESTAMP)
        s3util.upload_file_with_gzip(BUCKET, key, filtered_data_bytes)
    except Exception as e:
        logger.exception("Error while s3 upload", exc_info=e)


def upload_per_html_to_s3(filtered_data):
    for data in filtered_data:
        obj_path = url_to_path(data["url"])
        key = "{}{}/{}/{}".format(INPUT_PREFIX,
                                  "amazon-translate", "en", obj_path)
        try:
            raw_html_bytes = data["raw_html"].encode("utf-8")
            s3util.upload_file(BUCKET, key, raw_html_bytes)
        except Exception as e:
            logger.exception(
                "Error while s3 upload : s3://{}/{}".format(BUCKET, key), exc_info=e)


@calc_time
def main():
    """
    Main logic

    Summary:
        1. Download from S3 and filter data
        2. Store merged raw jsonl to local storage
        3. Upload merged raw jsonl to S3 (filtered_rawdata.jsonl)
        4. Upload original HTML per URL to S3
    """
    if BUCKET is None or PREFIX is None or TIMESTAMP is None:
        logger.error("BUCKET or PREFIX or TIMESTAMP is None")
        return 1

    logger.info("BUCKET: {}".format(BUCKET))
    logger.info("PREFIX: {}".format(PREFIX))
    logger.info("TIMESTAMP: {}".format(TIMESTAMP))

    # Download from S3
    filtered_data = download_input_from_s3()

    # Store merged raw jsonl to local storage
    # with jsonlines.open("./filtered_rawdata_{}.jsonl".format(TIMESTAMP), mode="w") as writer:
    #     writer.write_all(filtered_data)

    # Upload merged raw jsonl to S3 (filtered_rawdata.jsonl)
    upload_rawdata_to_s3(filtered_data)
    logger.info("s3 upload complete : merged raw jsonl")

    # Upload original HTML per URL to S3
    upload_per_html_to_s3(filtered_data)

    logger.info("Finished")


if __name__ == "__main__":
    main()
