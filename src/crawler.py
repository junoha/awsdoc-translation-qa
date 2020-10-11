import asyncio
from datetime import datetime, timezone
import json
import logging
import os
import traceback

import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import aiohttp
import xml.etree.ElementTree as ET
import jsonlines

import s3util
from helper import calc_time, to_isoformat, is_ok_url, filter_data

# requests retry backoff config
s = requests.Session()
retries = Retry(total=5, backoff_factor=1,
                status_forcelist=[500, 502, 503, 504])
s.mount("https://", HTTPAdapter(max_retries=retries))
s.mount("http://", HTTPAdapter(max_retries=retries))

formatter = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(format=formatter)
logger = logging.getLogger("crawler")
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

ROOT_SITEMAP_URL = "https://docs.aws.amazon.com/sitemap_index.xml"
# yyyymmddhhmmss (UTC)
TIMESTAMP = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
BUCKET = os.environ.get("BUCKET")
PREFIX_BASE = os.environ.get("PREFIX")
PREFIX = PREFIX_BASE + "/" + TIMESTAMP
SEMAPHORE = int(os.environ.get("SEMAPHORE", 30))


async def fetch(url, session):
    """
    Get HTML
    """
    await asyncio.sleep(2)
    doc_json = {}

    try:
        logger.debug("  GET -> {}".format(url))

        # Get ja docs if existing
        url_ja = url.replace(".com/", ".com/ja_jp/")

        response = await session.get(url)
        response_ja = await session.get(url_ja)
        crawled_at = datetime.now(timezone.utc).replace(
            microsecond=0).isoformat()

        if response.status == 200 and response_ja.status == 200:
            doc_json = {
                "crawled_at": crawled_at,
                "url": url,
                "status": response.status,
                "last_modified": to_isoformat(response.headers["Last-Modified"]),
                "etag": response.headers["Etag"],
                "html": await response.text("utf-8"),
                "url_ja": url_ja,
                "status_ja": response_ja.status,
                "last_modified_ja": to_isoformat(response_ja.headers["Last-Modified"]),
                "etag_ja": response_ja.headers["Etag"],
                "html_ja": await response_ja.text("utf-8"),
                "message": None,
            }
    except Exception:
        trace = traceback.format_exc()
        logger.error("Error while GET {}".format(url))
        logger.exception(trace)
        doc_json = {
            "crawled_at": crawled_at,
            "url": url,
            "status": None,
            "last_modified": None,
            "etag": None,
            "html": None,
            "url_ja": url_ja,
            "status_ja": None,
            "last_modified_ja": None,
            "etag_ja": None,
            "html_ja": None,
            "message": trace,
        }
    finally:
        return doc_json


async def burst_fetch(url, session, sem):
    """
    Get HTML with semaphore
    """
    logger.debug("=== burst_fetch")
    async with sem:
        logger.debug("==== call fetch")
        return await fetch(url, session)


async def get_doc_by_service(urls):
    """
    Get documents by service
    """
    tasks = []
    sem = asyncio.Semaphore(SEMAPHORE)
    async with aiohttp.ClientSession() as session:
        for url in urls:
            task = burst_fetch(url, session, sem)
            tasks.append(task)

        return await asyncio.wait(tasks)


def upload_rawdata_to_s3(filtered_data):
    """
    Upload to S3

    S3://BUCKET/PREFIX/merged/filtered_rawdata_TIMESTAMP.jsonl.gz
    """
    try:
        filtered_data_bytes = "\n".join([json.dumps(d) for d in filtered_data]).encode(
            "utf-8"
        )
        key = "{}/{}/filtered_rawdata_{}.jsonl.gz".format(
            PREFIX, "merged", TIMESTAMP)
        s3util.upload_file_with_gzip(BUCKET, key, filtered_data_bytes)
    except Exception as e:
        logger.exception("Error while s3 upload", exc_info=e)


def get_all_docs(sitemap_urls):
    """
    Get all AWS documents
    """
    remain_count = len(sitemap_urls)
    data_list = []

    for service_sitemap_url in sitemap_urls:
        logger.info(
            "({0}/{1}) {2}".format(remain_count,
                                   len(sitemap_urls), service_sitemap_url)
        )
        if not is_ok_url(service_sitemap_url):
            logger.info("Skipping this sitemap.xml")
            remain_count -= 1
            continue

        service_sitemap = None
        try:
            # Some sitemap redirects directory to one document url so stop redirect
            service_sitemap = s.get(service_sitemap_url, allow_redirects=False)
        except Exception:
            trace = traceback.format_exc()
            logger.error("Error while GET {}".format(service_sitemap_url))
            logger.exception(trace)
            continue

        if service_sitemap.status_code != 200:
            logger.warning(
                "failed to get this sitemap.xml due to {}({})".format(
                    service_sitemap.status_code, service_sitemap.reason
                )
            )
            continue

        service_root = ET.fromstring(service_sitemap.text.encode("utf-8"))
        service_urls = [child[0].text.strip() for child in service_root]

        # crawl only docs.aws.amazon.com
        filtered_service_urls = list(
            filter(lambda url: "docs.aws.amazon.com" in url, service_urls)
        )
        if len(filtered_service_urls) == 0:
            logger.info("No doc in docs.aws.amazon.com. skipping...")
            remain_count -= 1
            continue

        logger.info("Documents in service: {}".format(
            len(filtered_service_urls)))

        # Get HTMLs in parallel by asyncio
        done, _ = asyncio.run(get_doc_by_service(filtered_service_urls))
        for d in done:
            data_list.append(d.result())

        remain_count -= 1

    # Filter crawled data
    logger.info("Filtering all documets")
    filtered_data_list = filter_data(data_list)
    logger.info("Document size(All)     : {}".format(len(data_list)))
    logger.info("Document size(Filtered): {}".format(len(filtered_data_list)))

    return filtered_data_list


@calc_time
def main():
    """
    Main logic
    """
    logger.info("TIMESTAMP: {}".format(TIMESTAMP))
    logger.info("BUCKET: {}".format(BUCKET))
    logger.info("PREFIX: {}".format(PREFIX_BASE))
    logger.info("SEMAPHORE: {}".format(SEMAPHORE))

    root_sitemap = s.get(ROOT_SITEMAP_URL)
    root = ET.fromstring(root_sitemap.text.encode("utf-8"))
    service_sitemap_urls = [child[0].text.strip() for child in root]

    logger.info("Number of sitemap.xml: {}".format(
        len(service_sitemap_urls)))

    # Get and filter all documents
    filtered_data_list = get_all_docs(service_sitemap_urls)

    # Upload merged jsonl
    if (len(filtered_data_list) > 0):
        logger.info("Uploading to S3")
        upload_rawdata_to_s3(filtered_data_list)
    else:
        logger.info("skip upload to s3")

    logger.info("Done")


if __name__ == "__main__":
    main()
